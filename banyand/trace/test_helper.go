// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// Timestamp marks a DumpTag value as a ValueType=Timestamp tag carrying nanos
// since the epoch (trace stores its per-span time in such a tag).
type Timestamp int64

// DumpTag describes one (flat) tag of a trace span for the dump test builder.
// Value is a Go-native value: string, int64, []string, []int64, []byte or
// Timestamp.
type DumpTag struct {
	Value any
	Name  string
}

// DumpRow is one trace span for BuildPartForDump. SeriesID is derived from the
// tags by the parser, so it is not specified here.
type DumpRow struct {
	TraceID   string
	SpanID    string
	Span      []byte
	Tags      []DumpTag
	Timestamp int64
}

// BuildPartForDump writes spans into a trace part at root/<partID>, returning the
// part directory, the rows for verification and a cleanup func.
func BuildPartForDump(tmpPath string, fileSystem fs.FileSystem, partID uint64, rows []DumpRow) (string, []DumpRow, func()) {
	ts := &traces{}
	for i := range rows {
		r := &rows[i]
		ts.traceIDs = append(ts.traceIDs, r.TraceID)
		ts.timestamps = append(ts.timestamps, r.Timestamp)
		ts.spanIDs = append(ts.spanIDs, r.SpanID)
		ts.spans = append(ts.spans, r.Span)
		ts.tags = append(ts.tags, buildDumpTags(r.Tags))
	}

	mp := &memPart{}
	mp.mustInitFromTraces(ts)
	path := partPath(tmpPath, partID)
	mp.mustFlush(fileSystem, path)

	return path, rows, func() {}
}

// StandardDumpRows returns the canonical trace fixture: three spans across two
// trace IDs, each carrying string / int64 / timestamp / string-array tags plus a
// binary span payload.
func StandardDumpRows() []DumpRow {
	now := time.Now().UnixNano()
	return []DumpRow{
		{
			TraceID:   "test-trace-1",
			SpanID:    "span-1",
			Span:      []byte("span-data-1-with-content"),
			Timestamp: now,
			Tags: []DumpTag{
				{Name: "service.name", Value: "test-service"},
				{Name: "http.status", Value: int64(200)},
				{Name: "timestamp", Value: Timestamp(now)},
				{Name: "tags", Value: []string{"tag1", "tag2"}},
				{Name: "duration", Value: int64(1234567)},
			},
		},
		{
			TraceID:   "test-trace-1",
			SpanID:    "span-2",
			Span:      []byte("span-data-2-with-content"),
			Timestamp: now + 1000,
			Tags: []DumpTag{
				{Name: "service.name", Value: "test-service"},
				{Name: "http.status", Value: int64(404)},
				{Name: "timestamp", Value: Timestamp(now + 1000)},
				{Name: "tags", Value: []string{"tag3", "tag4"}},
				{Name: "duration", Value: int64(9876543)},
			},
		},
		{
			TraceID:   "test-trace-2",
			SpanID:    "span-3",
			Span:      []byte("span-data-3-with-content"),
			Timestamp: now + 2000,
			Tags: []DumpTag{
				{Name: "service.name", Value: "another-service"},
				{Name: "http.status", Value: int64(500)},
				{Name: "timestamp", Value: Timestamp(now + 2000)},
				{Name: "tags", Value: []string{"tag5"}},
				{Name: "duration", Value: int64(5555555)},
			},
		},
	}
}

// EntityDumpRows returns one span per entity, each carrying a single meta.name
// string tag with a distinct traceID and spanID.
func EntityDumpRows(entities []string) []DumpRow {
	base := time.Now().UnixNano()
	rows := make([]DumpRow, 0, len(entities))
	for i, entity := range entities {
		rows = append(rows, DumpRow{
			TraceID:   fmt.Sprintf("trace-%d", i),
			SpanID:    fmt.Sprintf("span-%d", i),
			Span:      []byte("span-data-" + entity),
			Timestamp: base + int64(i),
			Tags:      []DumpTag{{Name: "meta.name", Value: entity}},
		})
	}
	return rows
}

func buildDumpTags(tags []DumpTag) []*tagValue {
	out := make([]*tagValue, 0, len(tags))
	for _, tg := range tags {
		out = append(out, encodeDumpTag(tg.Name, tg.Value))
	}
	return out
}

func encodeDumpTag(name string, value any) *tagValue {
	switch v := value.(type) {
	case string:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_STRING, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}})
	case int64:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_INT, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}})
	case Timestamp:
		tv := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(0, int64(v)))}}
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_TIMESTAMP, tv)
	case []string:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_STRING_ARRAY, &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: v}}})
	case []int64:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_INT_ARRAY, &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v}}})
	case []byte:
		return encodeTagValue(name, databasev1.TagType_TAG_TYPE_DATA_BINARY, &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: v}})
	default:
		panic(fmt.Sprintf("unsupported dump tag value type %T", value))
	}
}
