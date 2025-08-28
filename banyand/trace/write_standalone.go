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

package trace

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

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
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "trace is readonly because \"trace-max-disk-usage-percent\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(w.schemaRepo.path)
	if diskPercent < w.maxDiskUsagePercent {
		return nil
	}
	w.l.Warn().Int("maxPercent", w.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
}

func (w *writeCallback) handle(dst map[string]*tracesInGroup, writeEvent *tracev1.InternalWriteRequest,
) (map[string]*tracesInGroup, error) {
	stm, ok := w.schemaRepo.loadTrace(writeEvent.GetRequest().GetMetadata())
	if !ok {
		return nil, fmt.Errorf("cannot find trace definition: %s", writeEvent.GetRequest().GetMetadata())
	}
	idx, err := getTagIndex(stm, stm.schema.TimestampTagName)
	if err != nil {
		return nil, err
	}
	t := writeEvent.Request.Tags[idx].GetTimestamp().AsTime().Local()
	if err = timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := t.UnixNano()
	eg, err := w.prepareTracesInGroup(dst, writeEvent, ts)
	if err != nil {
		return nil, err
	}
	et, err := w.prepareTracesInTable(eg, writeEvent, ts)
	if err != nil {
		return nil, err
	}
	err = processTraces(w.schemaRepo, et.traces, writeEvent)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (w *writeCallback) prepareTracesInGroup(dst map[string]*tracesInGroup, writeEvent *tracev1.InternalWriteRequest, ts int64) (*tracesInGroup, error) {
	gn := writeEvent.Request.Metadata.Group
	tsdb, err := w.schemaRepo.loadTSDB(gn)
	if err != nil {
		return nil, fmt.Errorf("cannot load tsdb for group %s: %w", gn, err)
	}

	eg, ok := dst[gn]
	if !ok {
		eg = &tracesInGroup{
			tsdb:     tsdb,
			tables:   make([]*tracesInTable, 0),
			segments: make([]storage.Segment[*tsTable, option], 0),
		}
		dst[gn] = eg
	}
	if eg.latestTS < ts {
		eg.latestTS = ts
	}
	return eg, nil
}

func (w *writeCallback) prepareTracesInTable(eg *tracesInGroup, writeEvent *tracev1.InternalWriteRequest, ts int64) (*tracesInTable, error) {
	var et *tracesInTable
	for i := range eg.tables {
		if eg.tables[i].timeRange.Contains(ts) {
			et = eg.tables[i]
			break
		}
	}

	if et == nil {
		var segment storage.Segment[*tsTable, option]
		for _, seg := range eg.segments {
			if seg.GetTimeRange().Contains(ts) {
				segment = seg
				break
			}
		}
		if segment == nil {
			var err error
			segment, err = eg.tsdb.CreateSegmentIfNotExist(time.Unix(0, ts))
			if err != nil {
				return nil, fmt.Errorf("cannot create segment: %w", err)
			}
			eg.segments = append(eg.segments, segment)
		}

		shardID := common.ShardID(writeEvent.ShardId)
		tstb, err := segment.CreateTSTableIfNotExist(shardID)
		if err != nil {
			return nil, fmt.Errorf("cannot create ts table: %w", err)
		}

		et = &tracesInTable{
			timeRange: segment.GetTimeRange(),
			tsTable:   tstb,
			traces:    generateTraces(),
			segment:   segment,
		}
		et.traces.reset()
		eg.tables = append(eg.tables, et)
	}
	return et, nil
}

func processTraces(schemaRepo *schemaRepo, traces *traces, writeEvent *tracev1.InternalWriteRequest) error {
	req := writeEvent.Request
	stm, ok := schemaRepo.loadTrace(req.GetMetadata())
	if !ok {
		return fmt.Errorf("cannot find trace definition: %s", req.GetMetadata())
	}

	idx, err := getTagIndex(stm, stm.schema.TraceIdTagName)
	if err != nil {
		return err
	}
	traces.traceIDs = append(traces.traceIDs, req.Tags[idx].GetStr().GetValue())
	traces.spans = append(traces.spans, req.Span)

	tLen := len(req.GetTags())
	if tLen < 1 {
		return fmt.Errorf("%s has no tag family", req)
	}
	if tLen > len(stm.schema.GetTags()) {
		return fmt.Errorf("%s has more tag than %s", req.Metadata, stm.schema)
	}

	is := stm.indexSchema.Load().(indexSchema)
	if len(is.indexRuleLocators) > len(stm.GetSchema().GetTags()) {
		return fmt.Errorf("metadata crashed, tag rule length %d, tag length %d",
			len(is.indexRuleLocators), len(stm.GetSchema().GetTags()))
	}

	tags := make([]*tagValue, 0, len(stm.schema.Tags))
	tagSpecs := stm.GetSchema().GetTags()
	for i := range tagSpecs {
		tagSpec := tagSpecs[i]
		if tagSpec.Name == stm.schema.TraceIdTagName {
			continue
		}
		if tagSpec.Name == stm.schema.TimestampTagName {
			traces.timestamps = append(traces.timestamps, req.Tags[i].GetTimestamp().AsTime().UnixNano())
		}

		var tagValue *modelv1.TagValue
		if len(req.Tags) <= i {
			tagValue = pbv1.NullTagValue
		} else {
			tagValue = req.Tags[i]
		}
		tv := encodeTagValue(tagSpec.Name, tagSpec.Type, tagValue)
		tags = append(tags, tv)
	}
	traces.tags = append(traces.tags, tags)
	return nil
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
	groups := make(map[string]*tracesInGroup)
	for i := range events {
		var writeEvent *tracev1.InternalWriteRequest
		switch e := events[i].(type) {
		case *tracev1.InternalWriteRequest:
			writeEvent = e
		case []byte:
			writeEvent = &tracev1.InternalWriteRequest{}
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
			w.l.Error().Err(err).Msg("cannot handle write event")
			groups = make(map[string]*tracesInGroup)
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			es := g.tables[j]
			es.tsTable.mustAddTraces(es.traces)
			releaseTraces(es.traces)
		}
		if len(g.segments) > 0 {
			for _, segment := range g.segments {
				segment.DecRef()
			}
		}
		g.tsdb.Tick(g.latestTS)
	}
	return
}

func encodeTagValue(name string, tagType databasev1.TagType, tagVal *modelv1.TagValue) *tagValue {
	tv := generateTagValue()
	tv.tag = name
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		tv.valueType = pbv1.ValueTypeInt64
		if tagVal.GetInt() != nil {
			tv.value = convert.Int64ToBytes(tagVal.GetInt().GetValue())
		}
	case databasev1.TagType_TAG_TYPE_STRING:
		tv.valueType = pbv1.ValueTypeStr
		if tagVal.GetStr() != nil {
			tv.value = convert.StringToBytes(tagVal.GetStr().GetValue())
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
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		tv.valueType = pbv1.ValueTypeTimestamp
		if tagVal.GetTimestamp() != nil {
			// Convert timestamp to 64-bit nanoseconds since epoch for efficient storage
			ts := tagVal.GetTimestamp()
			epochNanos := ts.Seconds*1e9 + int64(ts.Nanos)
			tv.value = convert.Int64ToBytes(epochNanos)
		}
	default:
		logger.Panicf("unsupported tag value type: %T", tagVal.GetValue())
	}
	return tv
}

func getTagIndex(trace *trace, name string) (int, error) {
	for i, tag := range trace.schema.Tags {
		if tag.Name == name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("tag %s not found in trace %s", name, trace.name)
}
