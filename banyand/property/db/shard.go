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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	segment "github.com/blugelabs/bluge_segment_api"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	shardTemplate  = "shard-%d"
	sourceField    = "_source"
	groupField     = "_group"
	nameField      = index.IndexModeName
	entityID       = "_entity_id"
	deleteField    = "_deleted"
	idField        = "_id"
	timestampField = "_timestamp"
	shaValueField  = "_sha_value"
	// unindexedSourceTag names a Property tag kept out of the inverted index.
	// Schema properties carry their full JSON spec in it, a large unique term
	// that dominated dictionary build and merge time while nothing filters
	// on it; the value is still stored with the property and stays sortable.
	unindexedSourceTag = "source"
)

var (
	sourceFieldKey    = index.FieldKey{TagName: sourceField}
	entityFieldKey    = index.FieldKey{TagName: entityID}
	groupFieldKey     = index.FieldKey{TagName: groupField}
	nameFieldKey      = index.FieldKey{TagName: nameField}
	deletedFieldKey   = index.FieldKey{TagName: deleteField}
	idFieldKey        = index.FieldKey{TagName: idField}
	timestampFieldKey = index.FieldKey{TagName: timestampField}
	shaValueFieldKey  = index.FieldKey{TagName: shaValueField}
	projection        = []index.FieldKey{idFieldKey, timestampFieldKey, sourceFieldKey, deletedFieldKey}
)

type shard struct {
	store              index.SeriesStore
	nativeStore        *nativePropertyStore
	l                  *logger.Logger
	repairState        *repair
	location           string
	group              string
	expireToDeleteSec  int64
	id                 common.ShardID
	waitForPersistence bool
}

func (s *shard) close() error {
	if s.nativeStore != nil {
		return s.nativeStore.close()
	}
	if s.store != nil {
		return s.store.Close()
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
	var err error
	if db.indexConfig.NativeWriter {
		nativeMetrics := inverted.NewMetrics(metricsFactory)
		// NewOwner is a synchronous constructor; the store's Batch path carries
		// request contexts after construction.
		wait := si.waitForPersistence || batchWaitSec <= 0
		persistInterval := time.Duration(batchWaitSec) * time.Second
		//nolint:contextcheck // constructor has no context-bearing API
		if si.nativeStore, err = newNativePropertyStore(location, db.nativeLease, wait, persistInterval, func(count, size int64) {
			nativeMetrics.ObserveNative(count, size)
		}, si.prepareNativeMerge); err != nil {
			return nil, err
		}
	} else {
		opts := inverted.StoreOpts{
			Path:                 location,
			Logger:               si.l,
			Metrics:              inverted.NewMetrics(metricsFactory),
			BatchWaitSec:         batchWaitSec,
			PrepareMergeCallback: si.prepareForMerge,
		}
		if si.store, err = inverted.NewStore(opts); err != nil {
			return nil, err
		}
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
	if s.nativeStore != nil {
		existing, err := s.nativeStore.lookup(ctx, docID)
		if err != nil {
			return nil, fmt.Errorf("lookup existing documents failure: %w", err)
		}
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
	// search the original documents by docID
	seriesMatchers := make([]index.SeriesMatcher, 0, len(docID))
	for _, id := range docID {
		seriesMatchers = append(seriesMatchers, index.SeriesMatcher{
			Match: id,
			Type:  index.SeriesMatcherTypeExact,
		})
	}
	if len(seriesMatchers) == 0 {
		return nil, nil
	}
	iq, err := s.store.BuildQuery(seriesMatchers, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("build property query failure: %w", err)
	}
	exisingDocList, err := s.search(ctx, iq, nil, len(docID))
	if err != nil {
		return nil, fmt.Errorf("search existing documents failure: %w", err)
	}
	removeDocList := make([]index.Document, 0, len(exisingDocList))
	for _, property := range exisingDocList {
		p := &propertyv1.Property{}
		if err := protojson.Unmarshal(property.source, p); err != nil {
			return nil, fmt.Errorf("unmarshal property failure: %w", err)
		}
		// update the property to mark it as delete
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
	if s.nativeStore != nil {
		err := s.nativeStore.batch(ctx, docs, nil)
		if err == nil && s.repairState != nil && s.repairState.scheduler != nil {
			s.repairState.scheduler.documentUpdatesNotify()
		}
		return err
	}
	if s.waitForPersistence {
		var updateErr, persistentError error
		wg := sync.WaitGroup{}
		wg.Add(1)
		updateErr = s.store.UpdateSeriesBatch(index.Batch{
			Documents: docs,
			PersistentCallback: func(err error) {
				persistentError = err
				wg.Done()
			},
		})
		if updateErr != nil {
			return updateErr
		}
		wg.Wait()
		if persistentError != nil {
			return fmt.Errorf("persistent failure: %w", persistentError)
		}
	} else {
		updateErr := s.store.UpdateSeriesBatch(index.Batch{
			Documents: docs,
		})
		if updateErr != nil {
			return updateErr
		}
	}
	if s.repairState.scheduler != nil {
		s.repairState.scheduler.documentUpdatesNotify()
	}
	return nil
}

func (s *shard) searchNative(ctx context.Context, request *propertyv1.QueryRequest, order *propertyv1.QueryOrder, limit int) ([]*queryProperty, error) {
	if s.nativeStore == nil {
		return nil, errors.New("native property store is not configured")
	}
	return s.nativeStore.query(ctx, request, order, limit)
}

func (s *shard) search(ctx context.Context, q index.Query, orderBy *propertyv1.QueryOrder, limit int,
) (data []*queryProperty, err error) {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "property.search")
		span.Tagf("query", "%s", q.String())
		span.Tagf("shard", "%d", s.id)
		if orderBy != nil {
			span.Tagf("order", "%s(%s)", orderBy.TagName, orderBy.Sort)
		}
		defer func() {
			if data != nil {
				span.Tagf("matched", "%d", len(data))
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	if orderBy == nil {
		ss, searchErr := s.store.Search(ctx, projection, q, limit)
		if searchErr != nil {
			return nil, searchErr
		}

		if len(ss) == 0 {
			return nil, nil
		}
		data = make([]*queryProperty, 0)
		for _, s := range ss {
			bytes := s.Fields[sourceField]
			if chargeErr := query.Charge(ctx, uint64(len(bytes))+128); chargeErr != nil {
				return nil, chargeErr
			}
			var deleteTime int64
			if s.Fields[deleteField] != nil {
				deleteTime = convert.BytesToInt64(s.Fields[deleteField])
			}
			data = append(data, &queryProperty{
				id:         s.Key.EntityValues,
				timestamp:  s.Timestamp,
				source:     bytes,
				deleteTime: deleteTime,
			})
		}
		return data, nil
	}
	order := &index.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{
				Id: uint32(convert.HashStr(orderBy.TagName)),
			},
		},
		Sort: orderBy.Sort,
		Type: index.OrderByTypeIndex,
	}
	if limit <= 0 {
		// SeriesSort uses this as its page size, not the final query limit.
		// One is the smallest non-zero page and still lets the iterator drain
		// every matching page without asking the index to allocate MaxInt hits.
		limit = 1
	}
	iter, err := s.store.SeriesSort(ctx, q, order, limit, projection)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	data = make([]*queryProperty, 0, queryCapacity(limit))
	for iter.Next() {
		val := iter.Val()
		if chargeErr := query.Charge(ctx, uint64(len(val.Values[sourceField]))+uint64(len(val.SortedValue))+128); chargeErr != nil {
			return nil, chargeErr
		}

		var deleteTime int64
		if val.Values[deleteField] != nil {
			deleteTime = convert.BytesToInt64(val.Values[deleteField])
		}

		var sortedValue []byte
		if len(val.SortedValue) > 0 {
			sortedValue = bytes.Clone(val.SortedValue)
		}

		data = append(data, &queryProperty{
			id:          val.EntityValues,
			timestamp:   val.Timestamp,
			source:      val.Values[sourceField],
			sortedValue: sortedValue,
			deleteTime:  deleteTime,
		})
	}
	return data, nil
}

func (s *shard) repair(ctx context.Context, id []byte, property *propertyv1.Property, deleteTime int64) (updated bool, selfNewer *queryProperty, err error) {
	start := time.Now()
	var (
		search1Elapsed     time.Duration
		deletePhaseElapsed time.Duration
		updateElapsed      time.Duration
		olderCount         int
		deleteCount        int
	)
	defer func() {
		elapsed := time.Since(start)
		switch {
		case err != nil:
			s.l.Warn().Int64("elapsed_ms", elapsed.Milliseconds()).Str("group", property.Metadata.Group).
				Str("name", property.Metadata.Name).Str("id", property.Id).Err(err).
				Int64("search1_ms", search1Elapsed.Milliseconds()).
				Int64("delete_phase_ms", deletePhaseElapsed.Milliseconds()).
				Int64("update_ms", updateElapsed.Milliseconds()).
				Int("older_count", olderCount).
				Int("delete_count", deleteCount).
				Msg("property repair failed")
		case elapsed >= 5*time.Second:
			s.l.Warn().Int64("elapsed_ms", elapsed.Milliseconds()).Str("group", property.Metadata.Group).
				Str("name", property.Metadata.Name).Str("id", property.Id).
				Int64("search1_ms", search1Elapsed.Milliseconds()).
				Int64("delete_phase_ms", deletePhaseElapsed.Milliseconds()).
				Int64("update_ms", updateElapsed.Milliseconds()).
				Int("older_count", olderCount).
				Int("delete_count", deleteCount).
				Msg("slow property repair")
		}
	}()
	search1Start := time.Now()
	var olderProperties []*queryProperty
	if s.nativeStore != nil {
		olderProperties, err = s.searchNative(ctx, &propertyv1.QueryRequest{
			Groups: []string{property.Metadata.Group}, Name: property.Metadata.Name, Ids: []string{property.Id},
		}, nil, 100)
	} else {
		iq, buildErr := inverted.BuildPropertyQuery(&propertyv1.QueryRequest{
			Groups: []string{property.Metadata.Group}, Name: property.Metadata.Name, Ids: []string{property.Id},
		}, groupField, entityID)
		if buildErr != nil {
			return false, nil, fmt.Errorf("build property query failure: %w", buildErr)
		}
		olderProperties, err = s.search(ctx, iq, nil, 100)
	}
	search1Elapsed = time.Since(search1Start)
	if err != nil {
		return false, nil, fmt.Errorf("query older properties failed: %w", err)
	}
	olderCount = len(olderProperties)
	sort.Sort(queryPropertySlice(olderProperties))
	// if there no older properties, we can insert the latest document.
	if len(olderProperties) == 0 {
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
	if (olderProperties[len(olderProperties)-1].timestamp > property.Metadata.ModRevision) ||
		olderProperties[len(olderProperties)-1].timestamp == property.Metadata.ModRevision &&
			olderProperties[len(olderProperties)-1].deleteTime == deleteTime {
		return false, olderProperties[len(olderProperties)-1], nil
	}

	docIDList := s.buildNotDeletedDocIDList(olderProperties)
	deletePhaseStart := time.Now()
	deletedDocuments, err := s.buildDeleteFromTimeDocuments(ctx, docIDList, time.Now().UnixNano())
	deletePhaseElapsed = time.Since(deletePhaseStart)
	if err != nil {
		return false, nil, fmt.Errorf("build delete older documents failed: %w", err)
	}
	deleteCount = len(deletedDocuments)
	// update the property to mark it as delete
	updateID := id
	if s.nativeStore != nil {
		updateID = GetPropertyID(property)
	}
	updateDoc, err := s.buildUpdateDocument(updateID, property, deleteTime)
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

func (s *shard) buildNotDeletedDocIDList(properties []*queryProperty) [][]byte {
	docIDList := make([][]byte, 0, len(properties))
	for _, p := range properties {
		if p.deleteTime > 0 {
			// If the property is already deleted, ignore it.
			continue
		}
		docIDList = append(docIDList, p.id)
	}
	return docIDList
}

func (s *shard) prepareForMerge(src []*roaring.Bitmap, segments []segment.Segment, _ uint64) (dest []*roaring.Bitmap, err error) {
	if len(segments) == 0 || len(src) == 0 || len(segments) != len(src) {
		return src, nil
	}
	for segID, seg := range segments {
		// The legacy engine's drop-set entries point at segment-owned bitmaps that concurrent searches
		// read via Snapshot.PostingsIterator. Mutating the original aliases would race against
		// roaring.AndNot in those readers. We take a private copy at most once per segment —
		// only on the first expired doc — so segments with no expirations cost nothing and
		// segments with many expirations clone exactly once instead of per-docID.
		privateOwned := false
		var docID uint64
		for ; docID < seg.Count(); docID++ {
			var deleteTime int64
			err = seg.VisitStoredFields(docID, func(field string, value []byte) bool {
				if field == deleteField {
					deleteTime = convert.BytesToInt64(value)
				}
				return true
			})
			if err != nil {
				return src, fmt.Errorf("visit stored field failure: %w", err)
			}

			if deleteTime <= 0 || int64(time.Since(time.Unix(0, deleteTime)).Seconds()) < s.expireToDeleteSec {
				continue
			}

			if !privateOwned {
				if src[segID] == nil {
					src[segID] = roaring.New()
				} else {
					src[segID] = src[segID].Clone()
				}
				privateOwned = true
			}

			src[segID].Add(uint32(docID))
		}
	}
	return src, nil
}

type queryPropertySlice []*queryProperty

func (q queryPropertySlice) Len() int {
	return len(q)
}

func (q queryPropertySlice) Less(i, j int) bool {
	return q[i].timestamp < q[j].timestamp
}

func (q queryPropertySlice) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}
