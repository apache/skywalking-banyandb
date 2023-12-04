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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
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
	series, err := tsdb.Register(shardID, &storage.Series{
		Subject:      req.Metadata.Name,
		EntityValues: writeEvent.EntityValues,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot register series: %w", err)
	}
	dps.seriesIDs = append(dps.seriesIDs, series.ID)
	field := nameValues{}
	for i2 := range req.DataPoint.Fields {
		field.values = append(field.values, encodeFieldValue(
			stm.GetSchema().GetFields()[i2].GetName(),
			req.DataPoint.Fields[i2],
		))
	}
	dps.fields = append(dps.fields, field)
	tagFamilies := make([]nameValues, len(req.DataPoint.TagFamilies))
	for i := range req.DataPoint.TagFamilies {
		tagFamily := req.DataPoint.TagFamilies[i]
		tf := nameValues{}
		tagFamilySpec := stm.GetSchema().GetTagFamilies()[i]
		for j := range tagFamily.Tags {
			tf.values = append(tf.values, encodeTagValue(
				tagFamilySpec.Tags[j].Name,
				tagFamily.Tags[j],
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
			dps.tsTable.Table().mustAddRows(dps.dataPoints)
			dps.tsTable.DecRef()
		}
		g.tsdb.IndexDB().Write(g.docs)
	}
	return
}

func familyIdentity(name string, flag []byte) []byte {
	return bytes.Join([][]byte{tsdb.Hash([]byte(name)), flag}, nil)
}

func encodeFieldValue(name string, fieldValue *modelv1.FieldValue) *nameValue {
	nv := &nameValue{name: name}
	switch fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		nv.valueType = storage.ValueTypeInt64
		nv.value = convert.Int64ToBytes(fieldValue.GetInt().GetValue())
	case *modelv1.FieldValue_Float:
		nv.valueType = storage.ValueTypeFloat64
		nv.value = convert.Float64ToBytes(fieldValue.GetFloat().GetValue())
	case *modelv1.FieldValue_Str:
		nv.valueType = storage.ValueTypeStr
		nv.value = []byte(fieldValue.GetStr().GetValue())
	case *modelv1.FieldValue_BinaryData:
		nv.valueType = storage.ValueTypeBinaryData
		nv.value = bytes.Clone(fieldValue.GetBinaryData())
	}
	return nv
}

func encodeTagValue(name string, tagValue *modelv1.TagValue) *nameValue {
	nv := &nameValue{name: name}
	switch tagValue.GetValue().(type) {
	case *modelv1.TagValue_Int:
		nv.valueType = storage.ValueTypeInt64
		nv.value = convert.Int64ToBytes(tagValue.GetInt().GetValue())
	case *modelv1.TagValue_Str:
		nv.valueType = storage.ValueTypeStr
		nv.value = []byte(tagValue.GetStr().GetValue())
	case *modelv1.TagValue_BinaryData:
		nv.valueType = storage.ValueTypeBinaryData
		nv.value = bytes.Clone(tagValue.GetBinaryData())
	case *modelv1.TagValue_IntArray:
		nv.valueArr = make([][]byte, len(tagValue.GetIntArray().Value))
		for i := range tagValue.GetIntArray().Value {
			nv.valueArr[i] = convert.Int64ToBytes(tagValue.GetIntArray().Value[i])
		}
	case *modelv1.TagValue_StrArray:
		nv.valueArr = make([][]byte, len(tagValue.GetStrArray().Value))
		for i := range tagValue.GetStrArray().Value {
			nv.valueArr[i] = []byte(tagValue.GetStrArray().Value[i])
		}
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
