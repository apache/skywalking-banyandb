// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/metrics"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// repairLockStripes is the number of locks a shard spreads its entity repairs over.
const repairLockStripes = 64

// repairStripe is a mutex whose acquisition can be abandoned when the waiter's context ends.
// Its zero value is unlocked and ready to use.
type repairStripe struct {
	sem  chan struct{}
	once sync.Once
}

// lock acquires the stripe, or returns the context's error if it ends first.
func (r *repairStripe) lock(ctx context.Context) error {
	r.once.Do(func() { r.sem = make(chan struct{}, 1) })
	// Do not take a free stripe once the context has already ended.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case r.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// unlock releases a stripe acquired by lock.
func (r *repairStripe) unlock() {
	<-r.sem
}

const (
	shardTemplate = "shard-%d"
	sourceField   = "_source"
	groupField    = "_group"
	nameField     = index.IndexModeName
	entityID      = "_entity_id"
	deleteField   = "_deleted"
	shaValueField = "_sha_value"
	// unindexedSourceTag names a Property tag kept out of the inverted index.
	// Schema properties carry their full JSON spec in it, a large unique term
	// that dominated dictionary build and merge time while nothing filters
	// on it; the value is still stored with the property and stays sortable.
	unindexedSourceTag = "source"
)

var (
	errNativeStoreNotConfigured = errors.New("native property store is not configured")

	sourceFieldKey   = index.FieldKey{TagName: sourceField}
	entityFieldKey   = index.FieldKey{TagName: entityID}
	groupFieldKey    = index.FieldKey{TagName: groupField}
	nameFieldKey     = index.FieldKey{TagName: nameField}
	deletedFieldKey  = index.FieldKey{TagName: deleteField}
	shaValueFieldKey = index.FieldKey{TagName: shaValueField}
)

type shard struct {
	nativeStore *nativePropertyStore
	l           *logger.Logger
	repairState *repair
	// testBeforeNativeRead, when set, is called once, synchronously, at the
	// start of a native read (every caller of nativeRead: searchNative's query,
	// entityRevisions' read for repair, and buildDeleteFromTimeDocuments's
	// lookup) before touching s.nativeStore. It exists only
	// because native reads (native.Owner.Acquire) are deliberately
	// lock-free -- unlike Update/Repair's write path, which
	// db_lifetime_test.go blocks through a real, production seam
	// (native.OwnerOptions.Persist, combined with nativePropertyStore.wait)
	// and needs no hook here at all -- so there is no existing seam that
	// makes an in-flight read observably slow. See db_lifetime_test.go's own
	// package doc comment for the full reasoning.
	testBeforeNativeRead func()
	location             string
	group                string
	// repairLocks serialize repairs of one entity, striped by entity: repair reads the
	// entity's revisions and then writes, and two concurrent repairs of the entity would
	// otherwise both decide from the same read. Update and delete do not take them.
	repairLocks        [repairLockStripes]repairStripe
	expireToDeleteSec  int64
	id                 common.ShardID
	waitForPersistence bool
}

func (s *shard) close() error {
	if s.nativeStore != nil {
		return s.nativeStore.close()
	}
	return nil
}

func (db *database) newShard(
	ctx context.Context,
	group string,
	id common.ShardID,
	_ int64,
	deleteExpireSec int64,
	repairBaseDir string,
	repairTreeSlotCount int,
) (*shard, error) {
	location := path.Join(db.location, group, fmt.Sprintf(shardTemplate, int(id)))
	sName := "shard" + strconv.Itoa(int(id))
	si := &shard{
		id:                 id,
		group:              group,
		l:                  logger.Fetch(ctx, sName),
		location:           location,
		expireToDeleteSec:  deleteExpireSec,
		waitForPersistence: db.indexConfig.WaitForPersistence,
	}
	batchWaitSec := db.indexConfig.BatchWaitSec
	metricsFactory := db.omr.With(db.metricsScope.ConstLabels(meter.LabelPairs{"group": group, "shard": sName}))
	nativeMetrics := metrics.NewMetrics(metricsFactory)
	// NewOwner is a synchronous constructor; the store's Batch path carries
	// request contexts after construction.
	wait := si.waitForPersistence || batchWaitSec <= 0
	persistInterval := time.Duration(batchWaitSec) * time.Second
	var err error
	//nolint:contextcheck // constructor has no context-bearing API
	if si.nativeStore, err = newNativePropertyStore(location, db.nativeLease, wait, persistInterval, func(count, size int64) {
		nativeMetrics.ObserveNative(count, size)
	}, si.prepareNativeMerge); err != nil {
		return nil, err
	}
	repairBaseDir = path.Join(repairBaseDir, group, sName)
	si.repairState = newRepair(location, repairBaseDir, logger.Fetch(ctx, fmt.Sprintf("repair%d", id)),
		metricsFactory, repairBatchSearchSize, repairTreeSlotCount, db.repairScheduler)
	return si, nil
}

func (s *shard) prepareNativeMerge(ctx context.Context, document native.MergeDocument) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var deleteTime int64
	if err := document.StoredFields(func(name string, value []byte) bool {
		if name == deleteField {
			if len(value) != 8 {
				deleteTime = -1
				return false
			}
			deleteTime = convert.BytesToInt64(value)
		}
		return true
	}); err != nil {
		return false, err
	}
	if deleteTime == -1 {
		return false, fmt.Errorf("invalid deletion timestamp: %w", native.ErrCorrupt)
	}
	if deleteTime <= 0 {
		return false, nil
	}
	return int64(time.Since(time.Unix(0, deleteTime)).Seconds()) >= s.expireToDeleteSec, nil
}

func (s *shard) update(ctx context.Context, id []byte, property *propertyv1.Property) error {
	document, err := s.buildUpdateDocument(id, property, 0)
	if err != nil {
		return fmt.Errorf("build update document failure: %w", err)
	}
	return s.updateDocuments(ctx, index.Documents{*document})
}

func (s *shard) buildUpdateDocument(id []byte, property *propertyv1.Property, deleteTime int64) (*index.Document, error) {
	pj, err := protojson.Marshal(property)
	if err != nil {
		return nil, err
	}
	sourceField := index.NewBytesField(sourceFieldKey, pj)
	sourceField.NoSort = true
	sourceField.Store = true
	entityField := index.NewBytesField(entityFieldKey, []byte(property.Id))
	entityField.Index = true
	groupField := index.NewBytesField(groupFieldKey, []byte(property.Metadata.Group))
	groupField.Index = true
	nameField := index.NewBytesField(nameFieldKey, []byte(property.Metadata.Name))
	nameField.Index = true

	doc := index.Document{
		EntityValues: id,
		Fields:       []index.Field{entityField, groupField, nameField, sourceField},
		Timestamp:    property.Metadata.ModRevision,
	}
	var tv []byte
	for _, t := range property.Tags {
		tv, err = pbv1.MarshalTagValue(t.Value)
		if err != nil {
			return nil, err
		}
		tagField := index.NewBytesField(index.FieldKey{IndexRuleID: uint32(convert.HashStr(t.Key))}, tv)
		tagField.Index = t.Key != unindexedSourceTag
		tagField.NoSort = false
		doc.Fields = append(doc.Fields, tagField)
	}

	if deleteTime > 0 {
		deleteField := index.NewBytesField(deletedFieldKey, convert.Int64ToBytes(deleteTime))
		deleteField.Store = true
		deleteField.NoSort = true
		doc.Fields = append(doc.Fields, deleteField)
	}

	shaVal, err := s.repairState.buildShaValue(pj, deleteTime)
	if err != nil {
		return nil, fmt.Errorf("building sha value failure: %w", err)
	}
	shaValueField := index.NewBytesField(shaValueFieldKey, convert.StringToBytes(shaVal))
	shaValueField.Store = true
	shaValueField.NoSort = true
	doc.Fields = append(doc.Fields, shaValueField)
	return &doc, nil
}

func (s *shard) delete(ctx context.Context, docID [][]byte) error {
	return s.deleteFromTime(ctx, docID, time.Now())
}

func (s *shard) deleteFromTime(ctx context.Context, docID [][]byte, delTime time.Time) error {
	if delTime.IsZero() {
		delTime = time.Now()
	}
	removeDocList, err := s.buildDeleteFromTimeDocuments(ctx, docID, delTime.UnixNano())
	if err != nil {
		return err
	}
	return s.updateDocuments(ctx, removeDocList)
}

func (s *shard) buildDeleteFromTimeDocuments(ctx context.Context, docID [][]byte, deleteTime int64) ([]index.Document, error) {
	store, err := s.nativeRead()
	if err != nil {
		return nil, err
	}
	existing, err := store.lookup(ctx, docID)
	if err != nil {
		return nil, fmt.Errorf("lookup existing documents failure: %w", err)
	}
	return s.buildDeleteDocuments(existing, deleteTime)
}

// buildDeleteDocuments rebuilds each property from its stored source as a tombstone at deleteTime.
func (s *shard) buildDeleteDocuments(existing []*queryProperty, deleteTime int64) ([]index.Document, error) {
	removeDocList := make([]index.Document, 0, len(existing))
	for _, property := range existing {
		p := &propertyv1.Property{}
		if err := protojson.Unmarshal(property.source, p); err != nil {
			return nil, fmt.Errorf("unmarshal property failure: %w", err)
		}
		document, err := s.buildUpdateDocument(GetPropertyID(p), p, deleteTime)
		if err != nil {
			return nil, fmt.Errorf("build delete document failure: %w", err)
		}
		removeDocList = append(removeDocList, *document)
	}
	return removeDocList, nil
}

func (s *shard) updateDocuments(ctx context.Context, docs index.Documents) error {
	if len(docs) == 0 {
		return nil
	}
	err := s.nativeStore.batch(ctx, docs, nil)
	if err == nil && s.repairState != nil && s.repairState.scheduler != nil {
		s.repairState.scheduler.documentUpdatesNotify()
	}
	return err
}

// nativeRead starts a native read: it calls the testBeforeNativeRead hook and returns the
// store, or an error when the shard has none.
func (s *shard) nativeRead() (*nativePropertyStore, error) {
	if s.testBeforeNativeRead != nil {
		s.testBeforeNativeRead()
	}
	if s.nativeStore == nil {
		return nil, errNativeStoreNotConfigured
	}
	return s.nativeStore, nil
}

func (s *shard) searchNative(ctx context.Context, request *propertyv1.QueryRequest, order *propertyv1.QueryOrder, limit int) ([]*queryProperty, error) {
	store, err := s.nativeRead()
	if err != nil {
		return nil, err
	}
	return store.query(ctx, request, order, limit)
}

// entityRevisions returns the newest stored revision and the live revisions of the entity
// request selects, in one native read.
func (s *shard) entityRevisions(ctx context.Context, request *propertyv1.QueryRequest) (newest *queryProperty, live []*queryProperty, err error) {
	store, err := s.nativeRead()
	if err != nil {
		return nil, nil, err
	}
	return store.entityRevisions(ctx, request)
}

func (s *shard) repair(ctx context.Context, id []byte, property *propertyv1.Property, deleteTime int64) (updated bool, selfNewer *queryProperty, err error) {
	stripe := &s.repairLocks[convert.HashStr(GetEntity(property))%repairLockStripes]
	if err = stripe.lock(ctx); err != nil {
		return false, nil, fmt.Errorf("wait for the repair lock: %w", err)
	}
	defer stripe.unlock()
	start := time.Now()
	var (
		readElapsed        time.Duration
		buildDeleteElapsed time.Duration
		updateElapsed      time.Duration
		liveCount          int
		deleteCount        int
	)
	defer func() {
		elapsed := time.Since(start)
		switch {
		case err != nil:
			s.l.Warn().Int64("elapsed_ms", elapsed.Milliseconds()).Str("group", property.Metadata.Group).
				Str("name", property.Metadata.Name).Str("id", property.Id).Err(err).
				Int64("read_ms", readElapsed.Milliseconds()).
				Int64("build_delete_ms", buildDeleteElapsed.Milliseconds()).
				Int64("update_ms", updateElapsed.Milliseconds()).
				Int("live_count", liveCount).
				Int("delete_count", deleteCount).
				Msg("property repair failed")
		case elapsed >= 5*time.Second:
			s.l.Warn().Int64("elapsed_ms", elapsed.Milliseconds()).Str("group", property.Metadata.Group).
				Str("name", property.Metadata.Name).Str("id", property.Id).
				Int64("read_ms", readElapsed.Milliseconds()).
				Int64("build_delete_ms", buildDeleteElapsed.Milliseconds()).
				Int64("update_ms", updateElapsed.Milliseconds()).
				Int("live_count", liveCount).
				Int("delete_count", deleteCount).
				Msg("slow property repair")
		}
	}()
	readStart := time.Now()
	newest, live, err := s.entityRevisions(ctx, &propertyv1.QueryRequest{
		Groups: []string{property.Metadata.Group}, Name: property.Metadata.Name, Ids: []string{property.Id},
	})
	readElapsed = time.Since(readStart)
	if err != nil {
		return false, nil, fmt.Errorf("read entity revisions failed: %w", err)
	}
	liveCount = len(live)
	// no stored revision: insert the repaired document as it is.
	if newest == nil {
		var doc *index.Document
		doc, err = s.buildUpdateDocument(id, property, deleteTime)
		if err != nil {
			return false, nil, fmt.Errorf("build update document failed: %w", err)
		}
		updateStart := time.Now()
		err = s.updateDocuments(ctx, index.Documents{*doc})
		updateElapsed = time.Since(updateStart)
		if err != nil {
			return false, nil, fmt.Errorf("update document failed: %w", err)
		}
		return true, nil, nil
	}

	// if the lastest property in shard is bigger than the repaired property,
	// then the repaired process should be stopped.
	if newest.timestamp > property.Metadata.ModRevision ||
		newest.timestamp == property.Metadata.ModRevision && !sameRevisionIncomingWins(newest.deleteTime, deleteTime) {
		return false, newest, nil
	}

	propertyID := GetPropertyID(property)
	// build_delete_ms times only building the tombstones in memory: the live revisions were
	// read with the entity's other revisions, in read_ms.
	buildDeleteStart := time.Now()
	deletedDocuments, err := s.buildDeleteDocuments(s.buildNotDeletedList(live, propertyID), time.Now().UnixNano())
	buildDeleteElapsed = time.Since(buildDeleteStart)
	if err != nil {
		return false, nil, fmt.Errorf("build delete older documents failed: %w", err)
	}
	deleteCount = len(deletedDocuments)
	// update the property to mark it as delete
	updateDoc, err := s.buildUpdateDocument(propertyID, property, deleteTime)
	if err != nil {
		return false, nil, fmt.Errorf("build repair document failure: %w", err)
	}
	result := make([]index.Document, 0, len(deletedDocuments)+1)
	result = append(result, deletedDocuments...)
	result = append(result, *updateDoc)
	updateStart := time.Now()
	err = s.updateDocuments(ctx, result)
	updateElapsed = time.Since(updateStart)
	if err != nil {
		return false, nil, fmt.Errorf("update documents failed: %w", err)
	}
	return true, nil, nil
}

// sameRevisionIncomingWins reports whether an incoming document must replace the local one
// that has the same ModRevision. A tombstone beats a live document, and between two
// tombstones the smaller deleteTime wins, so every replica converges regardless of order.
func sameRevisionIncomingWins(localDeleteTime, incomingDeleteTime int64) bool {
	if incomingDeleteTime <= 0 {
		return false
	}
	return localDeleteTime <= 0 || incomingDeleteTime < localDeleteTime
}

// buildNotDeletedList collects the live documents to be re-tombstoned, skipping excludeID,
// which is overwritten by the repaired document itself.
func (s *shard) buildNotDeletedList(properties []*queryProperty, excludeID []byte) []*queryProperty {
	result := make([]*queryProperty, 0, len(properties))
	for _, p := range properties {
		if p.deleteTime > 0 || bytes.Equal(p.id, excludeID) {
			// If the property is already deleted, ignore it.
			continue
		}
		result = append(result, p)
	}
	return result
}
