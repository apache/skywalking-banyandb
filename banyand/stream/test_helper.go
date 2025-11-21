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

package stream

import (
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// CreateTestPartForDump creates a test stream part for testing the dump tool.
// It returns the part path (directory) and cleanup function.
func CreateTestPartForDump(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	now := time.Now().UnixNano()

	// Create test elements with various tag types
	es := &elements{
		seriesIDs:  []common.SeriesID{1, 2, 3},
		timestamps: []int64{now, now + 1000, now + 2000},
		elementIDs: []uint64{11, 21, 31},
		tagFamilies: [][]tagValues{
			{
				{
					tag: "arrTag",
					values: []*tagValue{
						{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
						{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
					},
				},
				{
					tag: "singleTag",
					values: []*tagValue{
						{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("test-value"), valueArr: nil},
						{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(100), valueArr: nil},
					},
				},
			},
			{
				{
					tag: "singleTag",
					values: []*tagValue{
						{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
						{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
					},
				},
			},
			{}, // empty tagFamilies for seriesID 3
		},
	}

	// Create memPart and flush
	mp := generateMemPart()
	mp.mustInitFromElements(es)

	epoch := uint64(12345)
	path := partPath(tmpPath, epoch)
	mp.mustFlush(fileSystem, path)

	cleanup := func() {
		// Cleanup is handled by the caller's test.Space cleanup
	}

	return path, cleanup
}
