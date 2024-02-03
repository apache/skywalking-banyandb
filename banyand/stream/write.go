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

package stream

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type writeCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpWriteCallback(l *logger.Logger, schemaRepo *schemaRepo) bus.MessageListener {
	return &writeCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (w *writeCallback) handle(dst map[string]*elementsInGroup, writeEvent *streamv1.InternalWriteRequest) (map[string]*elementsInGroup, error) {
	req := writeEvent.Request
	t := req.Element.Timestamp.AsTime().Local()
	if err := timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := uint64(t.UnixNano())

	gn := req.Metadata.Group
	tsdb, err := w.schemaRepo.loadTSDB(gn)
	if err != nil {
		return nil, fmt.Errorf("cannot load tsdb for group %s: %w", gn, err)
	}
	eg, ok := dst[gn]
	if !ok {
		eg = &elementsInGroup{
			tsdb:   tsdb,
			tables: make([]*elementsInTable, 0),
		}
		dst[gn] = eg
	}

	var et *elementsInTable
	for i := range eg.tables {
		if eg.tables[i].timeRange.Contains(ts) {
			et = eg.tables[i]
			break
		}
	}
	shardID := common.ShardID(writeEvent.ShardId)
	if et == nil {
		tstb, err := tsdb.CreateTSTableIfNotExist(shardID, t)
		if err != nil {
			return nil, fmt.Errorf("cannot create ts table: %w", err)
		}
		et = &elementsInTable{
			timeRange: tstb.GetTimeRange(),
			tsTable:   tstb,
		}
		eg.tables = append(eg.tables, et)
	}
	et.elements.timestamps = append(et.elements.timestamps, int64(ts))
	et.elements.elementIDs = append(et.elements.elementIDs, writeEvent.Request.Element.GetElementId())
	stm, ok := w.schemaRepo.loadStream(writeEvent.GetRequest().GetMetadata())
	if !ok {
		return nil, fmt.Errorf("cannot find stream definition: %s", writeEvent.GetRequest().GetMetadata())
	}
	fLen := len(req.Element.GetTagFamilies())
	if fLen < 1 {
		return nil, fmt.Errorf("%s has no tag family", req.Metadata)
	}
	if fLen > len(stm.schema.GetTagFamilies()) {
		return nil, fmt.Errorf("%s has more tag families than expected", req.Metadata)
	}
	series := &pbv1.Series{
		Subject:      req.Metadata.Name,
		EntityValues: writeEvent.EntityValues,
	}
	if err := series.Marshal(); err != nil {
		return nil, fmt.Errorf("cannot marshal series: %w", err)
	}
	et.elements.seriesIDs = append(et.elements.seriesIDs, series.ID)

	tagFamilies := make([]tagValues, len(stm.schema.TagFamilies))
	tagFamiliesForIndexWrite := make([]tagValues, len(stm.schema.TagFamilies))
	entityMap := make(map[string]bool)
	et.elements.tagFamilies = append(et.elements.tagFamilies, tagFamilies)
	for _, entity := range stm.GetSchema().GetEntity().GetTagNames() {
		entityMap[entity] = true
	}
	for i := range stm.GetSchema().GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.Element.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.Element.TagFamilies[i]
		}
		tagFamilySpec := stm.GetSchema().GetTagFamilies()[i]
		tagFamilies[i].tag = tagFamilySpec.Name
		for j := range tagFamilySpec.Tags {
			var tagValue *modelv1.TagValue
			if tagFamily == pbv1.NullTagFamily || len(tagFamily.Tags) <= j {
				tagValue = pbv1.NullTagValue
			} else {
				tagValue = tagFamily.Tags[j]
			}
			tagFamiliesForIndexWrite[i].values = append(tagFamiliesForIndexWrite[i].values, encodeTagValue(
				tagFamilySpec.Tags[j].Name,
				tagFamilySpec.Tags[j].Type,
				tagValue,
			))
			if tagFamilySpec.Tags[j].IndexedOnly || entityMap[tagFamilySpec.Tags[j].Name] {
				continue
			}
			tagFamilies[i].values = append(tagFamilies[i].values, encodeTagValue(
				tagFamilySpec.Tags[j].Name,
				tagFamilySpec.Tags[j].Type,
				tagValue,
			))
		}
	}

	var fields []index.Field
	for _, indexRule := range stm.indexRuleLocators {
		tv := getIndexValue(indexRule, tagFamiliesForIndexWrite)
		if tv == nil {
			continue
		}
		if tv.value != nil {
			fields = append(fields, index.Field{
				Key: index.FieldKey{
					IndexRuleID: indexRule.Rule.GetMetadata().GetId(),
					Analyzer:    indexRule.Rule.Analyzer,
					SeriesID:    series.ID,
				},
				Term: tv.value,
			})
			continue
		}
		for _, val := range tv.valueArr {
			rule := indexRule.Rule
			fields = append(fields, index.Field{
				Key: index.FieldKey{
					IndexRuleID: rule.GetMetadata().GetId(),
					Analyzer:    rule.Analyzer,
					SeriesID:    series.ID,
				},
				Term: val,
			})
		}
	}

	et.docs = append(et.docs, index.Document{
		DocID:  ts,
		Fields: fields,
	})

	eg.docs = append(eg.docs, index.Document{
		DocID:        uint64(series.ID),
		EntityValues: series.Buffer,
	})
	return dst, nil
}

func (w *writeCallback) Rev(message bus.Message) (resp bus.Message) {
	events, ok := message.Data().([]any)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	if len(events) < 1 {
		w.l.Warn().Msg("empty event")
		return
	}
	groups := make(map[string]*elementsInGroup)
	for i := range events {
		var writeEvent *streamv1.InternalWriteRequest
		switch e := events[i].(type) {
		case *streamv1.InternalWriteRequest:
			writeEvent = e
		case *anypb.Any:
			writeEvent = &streamv1.InternalWriteRequest{}
			if err := e.UnmarshalTo(writeEvent); err != nil {
				w.l.Error().Err(err).RawJSON("written", logger.Proto(e)).Msg("fail to unmarshal event")
				continue
			}
		default:
			w.l.Warn().Msg("invalid event data type")
			continue
		}
		var err error
		if groups, err = w.handle(groups, writeEvent); err != nil {
			w.l.Error().Err(err).Msg("cannot handle write event")
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			es := g.tables[j]
			es.tsTable.Table().mustAddElements(&es.elements)
			index := es.tsTable.Table().Index()
			if err := index.Write(es.docs); err != nil {
				w.l.Error().Err(err).Msg("cannot write element index")
			}
			es.tsTable.DecRef()
		}
		if err := g.tsdb.IndexDB().Write(g.docs); err != nil {
			w.l.Error().Err(err).Msg("cannot write series index")
		}
	}
	return
}

func encodeTagValue(name string, tagType databasev1.TagType, tagVal *modelv1.TagValue) *tagValue {
	tv := &tagValue{tag: name}
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		tv.valueType = pbv1.ValueTypeInt64
		if tagVal.GetInt() != nil {
			tv.value = convert.Int64ToBytes(tagVal.GetInt().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_STRING:
		tv.valueType = pbv1.ValueTypeStr
		if tagVal.GetStr() != nil {
			tv.value = []byte(tagVal.GetStr().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		tv.valueType = pbv1.ValueTypeBinaryData
		if tagVal.GetBinaryData() != nil {
			tv.value = bytes.Clone(tagVal.GetBinaryData())
		}
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		tv.valueType = pbv1.ValueTypeInt64Arr
		if tagVal.GetIntArray() == nil {
			return tv
		}
		tv.valueArr = make([][]byte, len(tagVal.GetIntArray().Value))
		for i := range tagVal.GetIntArray().Value {
			tv.valueArr[i] = convert.Int64ToBytes(tagVal.GetIntArray().Value[i])
		}
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		tv.valueType = pbv1.ValueTypeStrArr
		if tagVal.GetStrArray() == nil {
			return tv
		}
		tv.valueArr = make([][]byte, len(tagVal.GetStrArray().Value))
		for i := range tagVal.GetStrArray().Value {
			tv.valueArr[i] = []byte(tagVal.GetStrArray().Value[i])
		}
	default:
		logger.Panicf("unsupported tag value type: %T", tagVal.GetValue())
	}
	return tv
}

func getIndexValue(ruleIndex *partition.IndexRuleLocator, tagFamilies []tagValues) *tagValue {
	if len(ruleIndex.TagIndices) != 1 {
		logger.Panicf("the index rule %s(%v) didn't support composited tags",
			ruleIndex.Rule.Metadata.Name, ruleIndex.Rule.Tags)
	}
	tIndex := ruleIndex.TagIndices[0]
	if tIndex.FamilyOffset >= len(tagFamilies) {
		return nil
	}
	tf := tagFamilies[tIndex.FamilyOffset]
	if tIndex.TagOffset >= len(tf.values) {
		return nil
	}
	return tf.values[tIndex.TagOffset]
}
