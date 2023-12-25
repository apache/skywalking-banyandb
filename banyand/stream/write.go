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

func (w *writeCallback) handle(dst map[string]*dataPointsInGroup, writeEvent *streamv1.InternalWriteRequest) (map[string]*dataPointsInGroup, error) {
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
	dpg, ok := dst[gn]
	if !ok {
		dpg = &dataPointsInGroup{
			tsdb:   tsdb,
			tables: make([]*dataPointsInTable, 0),
		}
		dst[gn] = dpg
	}

	var dpt *dataPointsInTable
	for i := range dpg.tables {
		if dpg.tables[i].timeRange.Contains(ts) {
			dpt = dpg.tables[i]
			break
		}
	}
	shardID := common.ShardID(writeEvent.ShardId)
	if dpt == nil {
		tstb, err := tsdb.CreateTSTableIfNotExist(shardID, t)
		if err != nil {
			return nil, fmt.Errorf("cannot create ts table: %w", err)
		}
		dpt = &dataPointsInTable{
			timeRange: tstb.GetTimeRange(),
			tsTable:   tstb,
		}
		dpg.tables = append(dpg.tables, dpt)
	}
	dpt.dataPoints.timestamps = append(dpt.dataPoints.timestamps, int64(ts))
	dpt.dataPoints.elementIDs = append(dpt.dataPoints.elementIDs, writeEvent.Request.Element.GetElementId())
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
	dpt.dataPoints.seriesIDs = append(dpt.dataPoints.seriesIDs, series.ID)

	tagFamilies := make([]nameValues, len(stm.schema.TagFamilies))
	for i := range stm.GetSchema().GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.Element.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.Element.TagFamilies[i]
		}
		tagFamilySpec := stm.GetSchema().GetTagFamilies()[i]
		tagFamilies[i].name = tagFamilySpec.Name
		for j := range tagFamilySpec.Tags {
			var tagValue *modelv1.TagValue
			if tagFamily == pbv1.NullTagFamily || len(tagFamily.Tags) <= j {
				tagValue = pbv1.NullTagValue
			} else {
				tagValue = tagFamily.Tags[j]
			}
			tagFamilies[i].values = append(tagFamilies[i].values, encodeTagValue(
				tagFamilySpec.Tags[j].Name,
				tagFamilySpec.Tags[j].Type,
				tagValue,
			))
		}
	}
	dpt.dataPoints.tagFamilies = append(dpt.dataPoints.tagFamilies, tagFamilies)

	var fields []index.Field
	for _, ruleIndex := range stm.indexRuleLocators {
		nv := getIndexValue(ruleIndex, tagFamilies)
		if nv == nil {
			continue
		}
		if nv.value != nil {
			fields = append(fields, index.Field{
				Key: index.FieldKey{
					IndexRuleID: ruleIndex.Rule.GetMetadata().GetId(),
					Analyzer:    ruleIndex.Rule.Analyzer,
				},
				Term: nv.value,
			})
			continue
		}
		for _, val := range nv.valueArr {
			rule := ruleIndex.Rule
			fields = append(fields, index.Field{
				Key: index.FieldKey{
					IndexRuleID: rule.GetMetadata().GetId(),
					Analyzer:    rule.Analyzer,
				},
				Term: val,
			})
		}
	}

	dpg.docs = append(dpg.docs, index.Document{
		DocID:        uint64(series.ID),
		EntityValues: series.Buffer,
		Fields:       fields,
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
	groups := make(map[string]*dataPointsInGroup)
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
			dps := g.tables[j]
			dps.tsTable.Table().mustAddDataPoints(&dps.dataPoints)
			dps.tsTable.DecRef()
		}
		if err := g.tsdb.IndexDB().Write(g.docs); err != nil {
			w.l.Error().Err(err).Msg("cannot write index")
		}
	}
	return
}

func encodeTagValue(name string, tagType databasev1.TagType, tagValue *modelv1.TagValue) *nameValue {
	nv := &nameValue{name: name}
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		nv.valueType = pbv1.ValueTypeInt64
		if tagValue.GetInt() != nil {
			nv.value = convert.Int64ToBytes(tagValue.GetInt().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_STRING:
		nv.valueType = pbv1.ValueTypeStr
		if tagValue.GetStr() != nil {
			nv.value = []byte(tagValue.GetStr().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		nv.valueType = pbv1.ValueTypeBinaryData
		if tagValue.GetBinaryData() != nil {
			nv.value = bytes.Clone(tagValue.GetBinaryData())
		}
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		nv.valueType = pbv1.ValueTypeInt64Arr
		if tagValue.GetIntArray() == nil {
			return nv
		}
		nv.valueArr = make([][]byte, len(tagValue.GetIntArray().Value))
		for i := range tagValue.GetIntArray().Value {
			nv.valueArr[i] = convert.Int64ToBytes(tagValue.GetIntArray().Value[i])
		}
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		nv.valueType = pbv1.ValueTypeStrArr
		if tagValue.GetStrArray() == nil {
			return nv
		}
		nv.valueArr = make([][]byte, len(tagValue.GetStrArray().Value))
		for i := range tagValue.GetStrArray().Value {
			nv.valueArr[i] = []byte(tagValue.GetStrArray().Value[i])
		}
	default:
		logger.Panicf("unsupported tag value type: %T", tagValue.GetValue())
	}
	return nv
}

func getIndexValue(ruleIndex *partition.IndexRuleLocator, tagFamilies []nameValues) *nameValue {
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
