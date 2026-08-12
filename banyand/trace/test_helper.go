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

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// Timestamp marks a DumpTag value as a ValueType=Timestamp tag carrying nanos
// since the epoch (trace stores its per-span time in such a tag).
type Timestamp = EncodedTimestamp

// DumpTag describes one (flat) tag of a trace span for the dump test builder.
// Value is a Go-native value: string, int64, []string, []int64, []byte or
// Timestamp. RawValue and ValueType preserve an already-marshaled storage value.
type DumpTag = PartEncoderTag

// DumpRow is one trace span for BuildPartForDump. IndexSeries is ignored by the
// core encoder and carries source index-series identity for fixture generation.
type DumpRow = PartEncoderRow

// BuildPartForDump writes spans into a trace part at root/<partID>, returning the
// part directory, the rows for verification and a cleanup func.
func BuildPartForDump(tmpPath string, fileSystem fs.FileSystem, partID uint64, rows []DumpRow) (string, []DumpRow, func()) {
	path, cleanup := EncodePart(tmpPath, fileSystem, partID, rows)
	return path, rows, cleanup
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
