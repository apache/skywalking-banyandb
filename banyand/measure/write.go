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

package measure

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
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

func (w *writeCallback) handle(dst map[string]*dataPointsInGroup, writeEvent *measurev1.InternalWriteRequest) (map[string]*dataPointsInGroup, error) {
	req := writeEvent.Request
	t := req.DataPoint.Timestamp.AsTime().Local()
	if err := timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %s", err)
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
	dps := dpt.dataPoints
	dps.timestamps = append(dps.timestamps, int64(ts))
	stm, ok := w.schemaRepo.loadMeasure(writeEvent.GetRequest().GetMetadata())
	if !ok {
		return nil, fmt.Errorf("cannot find measure definition: %s", writeEvent.GetRequest().GetMetadata())
	}
	fLen := len(req.DataPoint.GetTagFamilies())
	if fLen < 1 {
		return nil, fmt.Errorf("%s has no tag family", req.Metadata)
	}
	if fLen > len(stm.schema.GetTagFamilies()) {
		return nil, fmt.Errorf("%s has more tag families than expected", req.Metadata)
	}
	series, err := tsdb.Register(shardID, &pbv1.Series{
		Subject:      req.Metadata.Name,
		EntityValues: writeEvent.EntityValues[1:],
	})
	if err != nil {
		return nil, fmt.Errorf("cannot register series: %w", err)
	}
	dps.seriesIDs = append(dps.seriesIDs, series.ID)
	field := nameValues{}
	for i := range stm.GetSchema().GetFields() {
		var v *modelv1.FieldValue
		if len(req.DataPoint.Fields) <= i {
			v = pbv1.NullFieldValue
		} else {
			v = req.DataPoint.Fields[i]
		}
		field.values = append(field.values, encodeFieldValue(
			stm.GetSchema().GetFields()[i].GetName(),
			stm.GetSchema().GetFields()[i].FieldType,
			v,
		))
	}
	dps.fields = append(dps.fields, field)
	tagFamilies := make([]nameValues, len(req.DataPoint.TagFamilies))
	for i := range stm.GetSchema().GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.DataPoint.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.DataPoint.TagFamilies[i]
		}
		tf := nameValues{}
		tagFamilySpec := stm.GetSchema().GetTagFamilies()[i]
		for j := range tagFamilySpec.Tags {
			var tagValue *modelv1.TagValue
			if tagFamily == pbv1.NullTagFamily || len(tagFamily.Tags) <= j {
				tagValue = pbv1.NullTagValue
			} else {
				tagValue = tagFamily.Tags[j]
			}
			tf.values = append(tf.values, encodeTagValue(
				tagFamilySpec.Tags[j].Name,
				tagFamilySpec.Tags[j].Type,
				tagValue,
			))
		}
	}
	dps.tagFamilies = append(dps.tagFamilies, tagFamilies)

	if stm.processorManager != nil {
		stm.processorManager.onMeasureWrite(&measurev1.InternalWriteRequest{
			Request: &measurev1.WriteRequest{
				Metadata:  stm.GetSchema().Metadata,
				DataPoint: req.DataPoint,
				MessageId: uint64(time.Now().UnixNano()),
			},
			EntityValues: writeEvent.EntityValues[1:],
		})
	}
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
	if len(fields) > 0 {
		dpg.docs = append(dpg.docs, index.Document{
			DocID:  uint64(series.ID),
			Fields: fields,
		})
	}
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
		var err error
		if groups, err = w.handle(groups, events[i].(*measurev1.InternalWriteRequest)); err != nil {
			w.l.Error().Err(err).Msg("cannot handle write event")
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			dps := g.tables[j]
			dps.tsTable.Table().mustAddDataPoints(dps.dataPoints)
			dps.tsTable.DecRef()
		}
		g.tsdb.IndexDB().Write(g.docs)
	}
	return
}

func familyIdentity(name string, flag []byte) []byte {
	return bytes.Join([][]byte{tsdb.Hash([]byte(name)), flag}, nil)
}

func encodeFieldValue(name string, fieldType databasev1.FieldType, fieldValue *modelv1.FieldValue) *nameValue {
	nv := &nameValue{name: name}
	switch fieldType {
	case databasev1.FieldType_FIELD_TYPE_INT:
		nv.valueType = pbv1.ValueTypeInt64
		if fieldValue.GetInt() != nil {
			nv.value = convert.Int64ToBytes(fieldValue.GetInt().GetValue())
		}
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		nv.valueType = pbv1.ValueTypeFloat64
		if fieldValue.GetFloat() != nil {
			nv.value = convert.Float64ToBytes(fieldValue.GetFloat().GetValue())
		}
	case databasev1.FieldType_FIELD_TYPE_STRING:
		nv.valueType = pbv1.ValueTypeStr
		if fieldValue.GetStr() != nil {
			nv.value = []byte(fieldValue.GetStr().GetValue())
		}
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		nv.valueType = pbv1.ValueTypeBinaryData
		if fieldValue.GetBinaryData() != nil {
			nv.value = bytes.Clone(fieldValue.GetBinaryData())
		}
	default:
		logger.Panicf("unsupported field value type: %T", fieldValue.GetValue())
	}
	return nv
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
