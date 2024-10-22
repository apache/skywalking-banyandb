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
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := t.UnixNano()

	gn := req.Metadata.Group
	tsdb, err := w.schemaRepo.loadTSDB(gn)
	if err != nil {
		return nil, fmt.Errorf("cannot load tsdb for group %s: %w", gn, err)
	}
	dpg, ok := dst[gn]
	if !ok {
		dpg = &dataPointsInGroup{
			tsdb:     tsdb,
			tables:   make([]*dataPointsInTable, 0),
			segments: make([]storage.Segment[*tsTable, option], 0),
		}
		dst[gn] = dpg
	}
	if dpg.latestTS < ts {
		dpg.latestTS = ts
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
		if dpt, err = w.newDpt(tsdb, dpg, t, ts, shardID); err != nil {
			return nil, fmt.Errorf("cannot create data points in table: %w", err)
		}
	}
	dpt.dataPoints.timestamps = append(dpt.dataPoints.timestamps, ts)
	dpt.dataPoints.versions = append(dpt.dataPoints.versions, req.DataPoint.Version)
	stm, ok := w.schemaRepo.loadMeasure(req.GetMetadata())
	if !ok {
		return nil, fmt.Errorf("cannot find measure definition: %s", req.GetMetadata())
	}
	fLen := len(req.DataPoint.GetTagFamilies())
	if fLen < 1 {
		return nil, fmt.Errorf("%s has no tag family", req.Metadata)
	}
	if fLen > len(stm.schema.GetTagFamilies()) {
		return nil, fmt.Errorf("%s has more tag families than %s", req.Metadata, stm.schema)
	}
	series := &pbv1.Series{
		Subject:      req.Metadata.Name,
		EntityValues: writeEvent.EntityValues,
	}
	if err := series.Marshal(); err != nil {
		return nil, fmt.Errorf("cannot marshal series: %w", err)
	}
	dpt.dataPoints.seriesIDs = append(dpt.dataPoints.seriesIDs, series.ID)
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
	dpt.dataPoints.fields = append(dpt.dataPoints.fields, field)
	tagFamilies := make([]nameValues, 0, len(stm.schema.TagFamilies))
	if len(stm.indexRuleLocators.TagFamilyTRule) != len(stm.GetSchema().GetTagFamilies()) {
		logger.Panicf("metadata crashed, tag family rule length %d, tag family length %d",
			len(stm.indexRuleLocators.TagFamilyTRule), len(stm.GetSchema().GetTagFamilies()))
	}
	var fields []index.Field
	for i := range stm.GetSchema().GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.DataPoint.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.DataPoint.TagFamilies[i]
		}
		tfr := stm.indexRuleLocators.TagFamilyTRule[i]
		tagFamilySpec := stm.GetSchema().GetTagFamilies()[i]
		tf := nameValues{
			name: tagFamilySpec.Name,
		}
		for j := range tagFamilySpec.Tags {
			var tagValue *modelv1.TagValue
			if tagFamily == pbv1.NullTagFamily || len(tagFamily.Tags) <= j {
				tagValue = pbv1.NullTagValue
			} else {
				tagValue = tagFamily.Tags[j]
			}

			t := tagFamilySpec.Tags[j]
			encodeTagValue := encodeTagValue(
				t.Name,
				t.Type,
				tagValue)
			if r, ok := tfr[t.Name]; ok {
				if encodeTagValue.value != nil {
					fields = append(fields, index.Field{
						Key: index.FieldKey{
							IndexRuleID: r.GetMetadata().GetId(),
							Analyzer:    r.Analyzer,
						},
						Term:  encodeTagValue.value,
						Store: true,
					})
				} else {
					for _, val := range encodeTagValue.valueArr {
						fields = append(fields, index.Field{
							Key: index.FieldKey{
								IndexRuleID: r.GetMetadata().GetId(),
								Analyzer:    r.Analyzer,
							},
							Term:  val,
							Store: true,
						})
					}
				}
				continue
			}
			_, isEntity := stm.indexRuleLocators.EntitySet[t.Name]
			if tagFamilySpec.Tags[j].IndexedOnly || isEntity {
				continue
			}
			tf.values = append(tf.values, encodeTagValue)
		}
		if len(tf.values) > 0 {
			tagFamilies = append(tagFamilies, tf)
		}
	}
	dpt.dataPoints.tagFamilies = append(dpt.dataPoints.tagFamilies, tagFamilies)

	if stm.processorManager != nil {
		stm.processorManager.onMeasureWrite(uint64(series.ID), &measurev1.InternalWriteRequest{
			Request: &measurev1.WriteRequest{
				Metadata:  stm.GetSchema().Metadata,
				DataPoint: req.DataPoint,
				MessageId: uint64(time.Now().UnixNano()),
			},
			EntityValues: writeEvent.EntityValues,
		})
	}

	dpg.docs = append(dpg.docs, index.Document{
		DocID:        uint64(series.ID),
		EntityValues: series.Buffer,
		Fields:       fields,
	})
	return dst, nil
}

func (w *writeCallback) newDpt(tsdb storage.TSDB[*tsTable, option], dpg *dataPointsInGroup, t time.Time, ts int64, shardID common.ShardID) (*dataPointsInTable, error) {
	var segment storage.Segment[*tsTable, option]
	for _, seg := range dpg.segments {
		if seg.GetTimeRange().Contains(ts) {
			segment = seg
		}
	}
	if segment == nil {
		var err error
		segment, err = tsdb.CreateSegmentIfNotExist(t)
		if err != nil {
			return nil, fmt.Errorf("cannot create segment: %w", err)
		}
		dpg.segments = append(dpg.segments, segment)
	}
	tstb, err := segment.CreateTSTableIfNotExist(shardID)
	if err != nil {
		return nil, fmt.Errorf("cannot create ts table: %w", err)
	}
	dpt := &dataPointsInTable{
		timeRange: segment.GetTimeRange(),
		tsTable:   tstb,
	}
	dpg.tables = append(dpg.tables, dpt)
	return dpt, nil
}

func (w *writeCallback) Rev(_ context.Context, message bus.Message) (resp bus.Message) {
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
		var writeEvent *measurev1.InternalWriteRequest
		switch e := events[i].(type) {
		case *measurev1.InternalWriteRequest:
			writeEvent = e
		case *anypb.Any:
			writeEvent = &measurev1.InternalWriteRequest{}
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
			w.l.Error().Err(err).RawJSON("written", logger.Proto(writeEvent)).Msg("cannot handle write event")
			groups = make(map[string]*dataPointsInGroup)
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			dps := g.tables[j]
			dps.tsTable.mustAddDataPoints(&dps.dataPoints)
		}
		for _, segment := range g.segments {
			if err := segment.IndexDB().Write(g.docs); err != nil {
				w.l.Error().Err(err).Msg("cannot write index")
			}
			segment.DecRef()
		}
		g.tsdb.Tick(g.latestTS)
	}
	return
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
