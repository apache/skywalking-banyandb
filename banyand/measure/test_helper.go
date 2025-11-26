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

package measure

import (
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// CreateTestPartForDump creates a test measure part for testing the dump tool.
// It takes a temporary path and a file system as input, generates test data points with various tag and field types,
// creates a memory part, flushes it to disk, and returns the path to the created part directory.
// Parameters:
//
//	tmpPath:    the base directory where the part will be created.
//	fileSystem: the file system to use for writing the part.
//
// Returns:
//
//	The path to the created part directory and a cleanup function.
func CreateTestPartForDump(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	now := time.Now().UnixNano()

	// Create test data points with various tag and field types
	dps := &dataPoints{
		seriesIDs:  []common.SeriesID{1, 2, 3},
		timestamps: []int64{now, now + 1000, now + 2000},
		versions:   []int64{1, 2, 3},
		tagFamilies: [][]nameValues{
			{
				{
					name: "arrTag",
					values: []*nameValue{
						{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
						{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
					},
				},
				{
					name: "singleTag",
					values: []*nameValue{
						{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("test-value"), valueArr: nil},
						{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(100), valueArr: nil},
					},
				},
			},
			{
				{
					name: "singleTag",
					values: []*nameValue{
						{name: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
						{name: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
					},
				},
			},
			{}, // empty tagFamilies for seriesID 3
		},
		fields: []nameValues{
			{
				name: "intField",
				values: []*nameValue{
					{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1000), valueArr: nil},
				},
			},
			{
				name: "floatField",
				values: []*nameValue{
					{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(3.14), valueArr: nil},
				},
			},
			{
				name: "intField",
				values: []*nameValue{
					{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(2000), valueArr: nil},
				},
			},
		},
	}

	// Create memPart and flush
	mp := generateMemPart()
	mp.mustInitFromDataPoints(dps)

	epoch := uint64(12345)
	path := partPath(tmpPath, epoch)
	mp.mustFlush(fileSystem, path)

	cleanup := func() {
		// Cleanup is handled by the caller's test.Space cleanup
		releaseMemPart(mp)
	}

	return path, cleanup
}
