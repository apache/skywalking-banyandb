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

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var subjectField = index.FieldKey{TagName: index.IndexModeName}

type writeCallback struct {
	l                   *logger.Logger
	schemaRepo          *schemaRepo
	maxDiskUsagePercent int
}

func setUpWriteCallback(l *logger.Logger, schemaRepo *schemaRepo, maxDiskUsagePercent int) bus.MessageListener {
	if maxDiskUsagePercent > 100 {
		maxDiskUsagePercent = 100
	}
	return &writeCallback{
		l:                   l,
		schemaRepo:          schemaRepo,
		maxDiskUsagePercent: maxDiskUsagePercent,
	}
}

func (w *writeCallback) CheckHealth() *common.Error {
	if w.maxDiskUsagePercent < 1 {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "measure is readonly because \"measure-max-disk-usage-percent\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(w.schemaRepo.path)
	if diskPercent < w.maxDiskUsagePercent {
		return nil
	}
	w.l.Warn().Int("maxPercent", w.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
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
			tsdb:            tsdb,
			tables:          make([]*dataPointsInTable, 0),
			segments:        make([]storage.Segment[*tsTable, option], 0),
			metadataDocMap:  make(map[uint64]int),
			indexModeDocMap: make(map[uint64]int),
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
	is := stm.indexSchema.Load().(indexSchema)
	if len(is.indexRuleLocators.TagFamilyTRule) != len(stm.GetSchema().GetTagFamilies()) {
		logger.Panicf("metadata crashed, tag family rule length %d, tag family length %d",
			len(is.indexRuleLocators.TagFamilyTRule), len(stm.GetSchema().GetTagFamilies()))
	}

	shardID := common.ShardID(writeEvent.ShardId)
	if dpt == nil {
		if dpt, err = w.newDpt(tsdb, dpg, t, ts, shardID, stm.schema.IndexMode); err != nil {
			return nil, fmt.Errorf("cannot create data points in table: %w", err)
		}
	}

	series := &pbv1.Series{
		Subject:      req.Metadata.Name,
		EntityValues: writeEvent.EntityValues,
	}
	if err := series.Marshal(); err != nil {
		return nil, fmt.Errorf("cannot marshal series: %w", err)
	}

	if stm.schema.IndexMode {
		fields := handleIndexMode(stm.schema, req, is.indexRuleLocators)
		fields = w.appendEntityTagsToIndexFields(fields, stm, series)
		doc := index.Document{
			DocID:        uint64(series.ID),
			EntityValues: series.Buffer,
			Fields:       fields,
			Version:      req.DataPoint.Version,
			Timestamp:    ts,
		}

		if pos, exists := dpg.indexModeDocMap[doc.DocID]; exists {
			dpg.indexModeDocs[pos] = doc
		} else {
			dpg.indexModeDocMap[doc.DocID] = len(dpg.indexModeDocs)
			dpg.indexModeDocs = append(dpg.indexModeDocs, doc)
		}
		return dst, nil
	}

	fields := appendDataPoints(dpt, ts, series.ID, stm.GetSchema(), req, is.indexRuleLocators)

	doc := index.Document{
		DocID:        uint64(series.ID),
		EntityValues: series.Buffer,
		Fields:       fields,
	}

	if pos, exists := dpg.metadataDocMap[doc.DocID]; exists {
		dpg.metadataDocs[pos] = doc
	} else {
		dpg.metadataDocMap[doc.DocID] = len(dpg.metadataDocs)
		dpg.metadataDocs = append(dpg.metadataDocs, doc)
	}
	return dst, nil
}

func appendDataPoints(dest *dataPointsInTable, ts int64, sid common.SeriesID, schema *databasev1.Measure,
	req *measurev1.WriteRequest, locator partition.IndexRuleLocator,
) []index.Field {
	tagFamily, fields := handleTagFamily(schema, req, locator)
	if dest.dataPoints == nil {
		dest.dataPoints = generateDataPoints()
		dest.dataPoints.reset()
	}
	dataPoints := dest.dataPoints
	dataPoints.tagFamilies = append(dataPoints.tagFamilies, tagFamily)
	dataPoints.timestamps = append(dataPoints.timestamps, ts)
	dataPoints.versions = append(dataPoints.versions, req.DataPoint.Version)
	dataPoints.seriesIDs = append(dataPoints.seriesIDs, sid)

	field := nameValues{}
	for i := range schema.GetFields() {
		var v *modelv1.FieldValue
		if len(req.DataPoint.Fields) <= i {
			v = pbv1.NullFieldValue
		} else {
			v = req.DataPoint.Fields[i]
		}
		field.values = append(field.values, encodeFieldValue(
			schema.GetFields()[i].GetName(),
			schema.GetFields()[i].FieldType,
			v,
		))
	}
	dataPoints.fields = append(dataPoints.fields, field)

	dest.dataPoints = dataPoints
	return fields
}

func (w *writeCallback) newDpt(tsdb storage.TSDB[*tsTable, option], dpg *dataPointsInGroup,
	t time.Time, ts int64, shardID common.ShardID, indexMode bool,
) (*dataPointsInTable, error) {
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
	if indexMode {
		return &dataPointsInTable{
			timeRange: segment.GetTimeRange(),
		}, nil
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

func handleTagFamily(schema *databasev1.Measure, req *measurev1.WriteRequest, locator partition.IndexRuleLocator) ([]nameValues, []index.Field) {
	tagFamilies := make([]nameValues, 0, len(schema.TagFamilies))

	var fields []index.Field
	for i := range schema.GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.DataPoint.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.DataPoint.TagFamilies[i]
		}
		tfr := locator.TagFamilyTRule[i]
		tagFamilySpec := schema.GetTagFamilies()[i]
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
			r, ok := tfr[t.Name]
			if ok {
				fieldKey := index.FieldKey{}
				fieldKey.IndexRuleID = r.GetMetadata().GetId()
				fieldKey.Analyzer = r.Analyzer
				if encodeTagValue.value != nil {
					f := index.NewBytesField(fieldKey, encodeTagValue.value)
					f.Store = true
					f.Index = true
					f.NoSort = r.GetNoSort()
					fields = append(fields, f)
				} else {
					for _, val := range encodeTagValue.valueArr {
						f := index.NewBytesField(fieldKey, val)
						f.Store = true
						f.Index = true
						f.NoSort = r.GetNoSort()
						fields = append(fields, f)
					}
				}
				releaseNameValue(encodeTagValue)
				continue
			}
			_, isEntity := locator.EntitySet[t.Name]
			if isEntity {
				releaseNameValue(encodeTagValue)
				continue
			}
			tf.values = append(tf.values, encodeTagValue)
		}
		if len(tf.values) > 0 {
			tagFamilies = append(tagFamilies, tf)
		}
	}
	return tagFamilies, fields
}

func handleIndexMode(schema *databasev1.Measure, req *measurev1.WriteRequest, locator partition.IndexRuleLocator) []index.Field {
	var fields []index.Field
	for i := range schema.GetTagFamilies() {
		var tagFamily *modelv1.TagFamilyForWrite
		if len(req.DataPoint.TagFamilies) <= i {
			tagFamily = pbv1.NullTagFamily
		} else {
			tagFamily = req.DataPoint.TagFamilies[i]
		}
		tfr := locator.TagFamilyTRule[i]
		tagFamilySpec := schema.GetTagFamilies()[i]
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
			r, toIndex := tfr[t.Name]
			fieldKey := index.FieldKey{}
			if toIndex {
				fieldKey.IndexRuleID = r.GetMetadata().GetId()
				fieldKey.Analyzer = r.Analyzer
			} else {
				fieldKey.TagName = t.Name
			}
			if encodeTagValue.value != nil {
				f := index.NewBytesField(fieldKey, encodeTagValue.value)
				f.Store = true
				f.Index = toIndex
				f.NoSort = r.GetNoSort()
				fields = append(fields, f)
			} else {
				for _, val := range encodeTagValue.valueArr {
					f := index.NewBytesField(fieldKey, val)
					f.Store = true
					f.Index = toIndex
					f.NoSort = r.GetNoSort()
					fields = append(fields, f)
				}
			}
			releaseNameValue(encodeTagValue)
		}
	}
	return fields
}

func (w *writeCallback) appendEntityTagsToIndexFields(fields []index.Field, stm *measure, series *pbv1.Series) []index.Field {
	f := index.NewStringField(subjectField, series.Subject)
	f.Index = true
	f.NoSort = true
	fields = append(fields, f)
	is := stm.indexSchema.Load().(indexSchema)
	for i := range stm.schema.Entity.TagNames {
		if _, exists := is.indexTagMap[stm.schema.Entity.TagNames[i]]; exists {
			continue
		}
		tagName := stm.schema.Entity.TagNames[i]
		var t *databasev1.TagSpec
		for j := range stm.schema.TagFamilies {
			for k := range stm.schema.TagFamilies[j].Tags {
				if stm.schema.TagFamilies[j].Tags[k].Name == tagName {
					t = stm.schema.TagFamilies[j].Tags[k]
				}
			}
		}

		encodeTagValue := encodeTagValue(
			t.Name,
			t.Type,
			series.EntityValues[i])
		if encodeTagValue.value != nil {
			f = index.NewBytesField(index.FieldKey{TagName: index.IndexModeEntityTagPrefix + t.Name}, encodeTagValue.value)
			f.Index = true
			f.NoSort = true
			fields = append(fields, f)
		}
		releaseNameValue(encodeTagValue)
	}
	return fields
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
		case []byte:
			writeEvent = &measurev1.InternalWriteRequest{}
			if err := proto.Unmarshal(e, writeEvent); err != nil {
				w.l.Error().Err(err).RawJSON("written", e).Msg("fail to unmarshal event")
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
			if dps.tsTable != nil {
				dps.tsTable.mustAddDataPoints(dps.dataPoints)
			}
			if dps.dataPoints != nil {
				releaseDataPoints(dps.dataPoints)
			}
		}
		for _, segment := range g.segments {
			if len(g.metadataDocs) > 0 {
				if err := segment.IndexDB().Insert(g.metadataDocs); err != nil {
					w.l.Error().Err(err).Msg("cannot write metadata")
				}
			}
			if len(g.indexModeDocs) > 0 {
				if err := segment.IndexDB().Update(g.indexModeDocs); err != nil {
					w.l.Error().Err(err).Msg("cannot write index")
				}
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
	nv := generateNameValue()
	nv.name = name
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		nv.valueType = pbv1.ValueTypeInt64
		if tagValue.GetInt() != nil {
			nv.value = convert.Int64ToBytes(tagValue.GetInt().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_STRING:
		nv.valueType = pbv1.ValueTypeStr
		if tagValue.GetStr() != nil {
			nv.value = convert.StringToBytes(tagValue.GetStr().GetValue())
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
