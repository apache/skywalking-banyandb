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

package property

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	segment "github.com/blugelabs/bluge_segment_api"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
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
	store       index.SeriesStore
	l           *logger.Logger
	repairState *repair
	location    string
	id          common.ShardID

	expireToDeleteSec int64
}

func (s *shard) close() error {
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (db *database) newShard(
	ctx context.Context,
	id common.ShardID,
	_ int64,
	deleteExpireSec int64,
	repairBaseDir string,
	repairTreeSlotCount int,
) (*shard, error) {
	location := path.Join(db.location, fmt.Sprintf(shardTemplate, int(id)))
	sName := "shard" + strconv.Itoa(int(id))
	si := &shard{
		id:                id,
		l:                 logger.Fetch(ctx, sName),
		location:          location,
		expireToDeleteSec: deleteExpireSec,
	}
	metricsFactory := db.omr.With(propertyScope.ConstLabels(meter.LabelPairs{"shard": sName}))
	opts := inverted.StoreOpts{
		Path:                 location,
		Logger:               si.l,
		Metrics:              inverted.NewMetrics(metricsFactory),
		BatchWaitSec:         0,
		PrepareMergeCallback: si.prepareForMerge,
	}
	var err error
	if si.store, err = inverted.NewStore(opts); err != nil {
		return nil, err
	}
	repairBaseDir = path.Join(repairBaseDir, sName)
	si.repairState = newRepair(location, repairBaseDir, logger.Fetch(ctx, fmt.Sprintf("repair%d", id)),
		metricsFactory, repairBatchSearchSize, repairTreeSlotCount, db.repairScheduler)
	return si, nil
}

func (s *shard) update(id []byte, property *propertyv1.Property) error {
	document, err := s.buildUpdateDocument(id, property, 0)
	if err != nil {
		return fmt.Errorf("build update document failure: %w", err)
	}
	return s.updateDocuments(index.Documents{*document})
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
		tagField.Index = true
		tagField.NoSort = true
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
	return s.deleteFromTime(ctx, docID, time.Now().UnixNano())
}

func (s *shard) deleteFromTime(ctx context.Context, docID [][]byte, deleteTime int64) error {
	removeDocList, err := s.buildDeleteFromTimeDocuments(ctx, docID, deleteTime)
	if err != nil {
		return err
	}
	return s.updateDocuments(removeDocList)
}

func (s *shard) buildDeleteFromTimeDocuments(ctx context.Context, docID [][]byte, deleteTime int64) ([]index.Document, error) {
	// search the original documents by docID
	seriesMatchers := make([]index.SeriesMatcher, 0, len(docID))
	for _, id := range docID {
		seriesMatchers = append(seriesMatchers, index.SeriesMatcher{
			Match: id,
			Type:  index.SeriesMatcherTypeExact,
		})
	}
	iq, err := s.store.BuildQuery(seriesMatchers, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("build property query failure: %w", err)
	}
	exisingDocList, err := s.search(ctx, iq, len(docID))
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

func (s *shard) updateDocuments(docs index.Documents) error {
	if len(docs) == 0 {
		return nil
	}
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
	if s.repairState.scheduler != nil {
		s.repairState.scheduler.documentUpdatesNotify()
	}
	return nil
}

func (s *shard) search(ctx context.Context, indexQuery index.Query, limit int,
) (data []*queryProperty, err error) {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "property.search")
		span.Tagf("query", "%s", indexQuery.String())
		span.Tagf("shard", "%d", s.id)
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
	ss, err := s.store.Search(ctx, projection, indexQuery, limit)
	if err != nil {
		return nil, err
	}
	if len(ss) == 0 {
		return nil, nil
	}
	data = make([]*queryProperty, 0, len(ss))
	for _, s := range ss {
		bytes := s.Fields[sourceField]
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

func (s *shard) repair(ctx context.Context, id []byte, property *propertyv1.Property, deleteTime int64) (updated bool, selfNewer *queryProperty, err error) {
	iq, err := inverted.BuildPropertyQuery(&propertyv1.QueryRequest{
		Groups: []string{property.Metadata.Group},
		Name:   property.Metadata.Name,
		Ids:    []string{property.Id},
	}, groupField, entityID)
	if err != nil {
		return false, nil, fmt.Errorf("build property query failure: %w", err)
	}
	olderProperties, err := s.search(ctx, iq, 100)
	if err != nil {
		return false, nil, fmt.Errorf("query older properties failed: %w", err)
	}
	sort.Sort(queryPropertySlice(olderProperties))
	// if there no older properties, we can insert the latest document.
	if len(olderProperties) == 0 {
		var doc *index.Document
		doc, err = s.buildUpdateDocument(id, property, deleteTime)
		if err != nil {
			return false, nil, fmt.Errorf("build update document failed: %w", err)
		}
		err = s.updateDocuments(index.Documents{*doc})
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
	deletedDocuments, err := s.buildDeleteFromTimeDocuments(ctx, docIDList, time.Now().UnixNano())
	if err != nil {
		return false, nil, fmt.Errorf("build delete older documents failed: %w", err)
	}
	// update the property to mark it as delete
	updateDoc, err := s.buildUpdateDocument(id, property, deleteTime)
	if err != nil {
		return false, nil, fmt.Errorf("build repair document failure: %w", err)
	}
	result := make([]index.Document, 0, len(deletedDocuments)+1)
	result = append(result, deletedDocuments...)
	result = append(result, *updateDoc)
	err = s.updateDocuments(result)
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

			if src[segID] == nil {
				src[segID] = roaring.New()
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
