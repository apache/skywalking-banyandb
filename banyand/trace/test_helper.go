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
	"time"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// CreateTestPartForDump creates a test trace part for testing the dump tool.
// It returns the part path (directory) and cleanup function.
func CreateTestPartForDump(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	now := time.Now().UnixNano()

	// Create test traces with various tag types
	traces := &traces{
		traceIDs:   []string{"test-trace-1", "test-trace-1", "test-trace-2"},
		timestamps: []int64{now, now + 1000, now + 2000},
		spanIDs:    []string{"span-1", "span-2", "span-3"},
		spans: [][]byte{
			[]byte("span-data-1-with-content"),
			[]byte("span-data-2-with-content"),
			[]byte("span-data-3-with-content"),
		},
		tags: [][]*tagValue{
			{
				{tag: "service.name", valueType: pbv1.ValueTypeStr, value: []byte("test-service")},
				{tag: "http.status", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(200)},
				{tag: "timestamp", valueType: pbv1.ValueTypeTimestamp, value: convert.Int64ToBytes(now)},
				{tag: "tags", valueType: pbv1.ValueTypeStrArr, valueArr: [][]byte{[]byte("tag1"), []byte("tag2")}},
				{tag: "duration", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1234567)},
			},
			{
				{tag: "service.name", valueType: pbv1.ValueTypeStr, value: []byte("test-service")},
				{tag: "http.status", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(404)},
				{tag: "timestamp", valueType: pbv1.ValueTypeTimestamp, value: convert.Int64ToBytes(now + 1000)},
				{tag: "tags", valueType: pbv1.ValueTypeStrArr, valueArr: [][]byte{[]byte("tag3"), []byte("tag4")}},
				{tag: "duration", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(9876543)},
			},
			{
				{tag: "service.name", valueType: pbv1.ValueTypeStr, value: []byte("another-service")},
				{tag: "http.status", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(500)},
				{tag: "timestamp", valueType: pbv1.ValueTypeTimestamp, value: convert.Int64ToBytes(now + 2000)},
				{tag: "tags", valueType: pbv1.ValueTypeStrArr, valueArr: [][]byte{[]byte("tag5")}},
				{tag: "duration", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(5555555)},
			},
		},
	}

	// Create memPart and flush
	mp := &memPart{}
	mp.mustInitFromTraces(traces)

	epoch := uint64(12345)
	path := partPath(tmpPath, epoch)
	mp.mustFlush(fileSystem, path)

	cleanup := func() {
		// Cleanup is handled by the caller's test.Space cleanup
	}

	return path, cleanup
}
