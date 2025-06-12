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
	"strconv"
	"time"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	shardTemplate = "shard-%d"
	sourceField   = "_source"
	groupField    = "_group"
	nameField     = index.IndexModeName
	entityID      = "_entity_id"
	deleteField   = "_deleted"
)

var (
	sourceFieldKey  = index.FieldKey{TagName: sourceField}
	entityFieldKey  = index.FieldKey{TagName: entityID}
	groupFieldKey   = index.FieldKey{TagName: groupField}
	nameFieldKey    = index.FieldKey{TagName: nameField}
	deletedFieldKey = index.FieldKey{TagName: deleteField}
	projection      = []index.FieldKey{sourceFieldKey, deletedFieldKey}
)

type shard struct {
	store    index.SeriesStore
	l        *logger.Logger
	location string
	id       common.ShardID
}

func (s *shard) close() error {
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (db *database) newShard(ctx context.Context, id common.ShardID, flushTimeoutSeconds int64,
) (*shard, error) {
	location := path.Join(db.location, fmt.Sprintf(shardTemplate, int(id)))
	sName := "shard" + strconv.Itoa(int(id))
	si := &shard{
		id:       id,
		l:        logger.Fetch(ctx, sName),
		location: location,
	}
	opts := inverted.StoreOpts{
		Path:         location,
		Logger:       si.l,
		Metrics:      inverted.NewMetrics(db.omr.With(propertyScope.ConstLabels(meter.LabelPairs{"shard": sName}))),
		BatchWaitSec: flushTimeoutSeconds,
	}
	var err error
	if si.store, err = inverted.NewStore(opts); err != nil {
		return nil, err
	}
	return si, nil
}

func (s *shard) update(id []byte, property *propertyv1.Property) error {
	document, err := s.buildUpdateDocument(id, property)
	if err != nil {
		return fmt.Errorf("build update document failure: %v", err)
	}
	return s.store.UpdateSeriesBatch(index.Batch{
		Documents: index.Documents{*document},
	})
}

func (s *shard) buildUpdateDocument(id []byte, property *propertyv1.Property) (*index.Document, error) {
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
	}
	for _, t := range property.Tags {
		tv, err := pbv1.MarshalTagValue(t.Value)
		if err != nil {
			return nil, err
		}
		tagField := index.NewBytesField(index.FieldKey{IndexRuleID: uint32(convert.HashStr(t.Key))}, tv)
		tagField.Index = true
		tagField.NoSort = true
		doc.Fields = append(doc.Fields, tagField)
	}
	return &doc, nil
}

func (s *shard) delete(ctx context.Context, docID [][]byte) error {
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
		return fmt.Errorf("build property query failure: %v", err)
	}
	exisingDocList, err := s.search(ctx, iq, len(docID))
	if err != nil {
		return fmt.Errorf("search existing documents failure: %v", err)
	}
	removeDocList := make(index.Documents, 0, len(exisingDocList))
	for _, property := range exisingDocList {
		p := &propertyv1.Property{}
		if err := protojson.Unmarshal(property.source, p); err != nil {
			return fmt.Errorf("unmarshal property failure: %v", err)
		}
		// update the property metadata to mark it as deleted
		p.Metadata.ModRevision = time.Now().UnixNano()
		document, err := s.buildUpdateDocument(GetPropertyID(p), p)
		if err != nil {
			return fmt.Errorf("build delete document failure: %v", err)
		}
		// mark the document as deleted
		document.Deleted = true
		removeDocList = append(removeDocList, *document)
	}
	return s.store.UpdateSeriesBatch(index.Batch{
		Documents: removeDocList,
	})
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
		deleted := convert.BytesToBool(s.Fields[deleteField])
		data = append(data, &queryProperty{
			source:  bytes,
			deleted: deleted,
		})
	}
	return data, nil
}
