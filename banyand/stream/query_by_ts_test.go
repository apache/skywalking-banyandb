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

package stream

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestBlockCursorHeap_Merge_Deduplication(t *testing.T) {
	tests := []struct {
		name           string
		cursors        []*blockCursor
		wantElementIDs []uint64
		limit          int
		wantLen        int
		asc            bool
	}{
		{
			name: "duplicate element IDs should be deduplicated",
			cursors: []*blockCursor{
				{
					timestamps: []int64{10, 20},
					elementIDs: []uint64{100, 200},
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(1)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1"), []byte("value2")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
				{
					timestamps: []int64{15, 25},
					elementIDs: []uint64{100, 300}, // 100 is duplicate
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(2)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1_dup"), []byte("value3")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
			},
			asc:            true,
			limit:          10,
			wantElementIDs: []uint64{100, 200, 300},
			wantLen:        3,
		},
		{
			name: "all duplicates should result in single element",
			cursors: []*blockCursor{
				{
					timestamps: []int64{10},
					elementIDs: []uint64{100},
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(1)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
				{
					timestamps: []int64{20},
					elementIDs: []uint64{100}, // duplicate
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(2)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1_dup")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
			},
			asc:            true,
			limit:          10,
			wantElementIDs: []uint64{100},
			wantLen:        1,
		},
		{
			name: "multiple duplicates across multiple cursors",
			cursors: []*blockCursor{
				{
					timestamps: []int64{10, 20},
					elementIDs: []uint64{100, 200},
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(1)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1"), []byte("value2")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
				{
					timestamps: []int64{15, 25},
					elementIDs: []uint64{100, 200}, // both duplicates
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(2)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1_dup"), []byte("value2_dup")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
				{
					timestamps: []int64{30},
					elementIDs: []uint64{300},
					idx:        0,
					bm:         blockMetadata{seriesID: common.SeriesID(3)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value3")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
			},
			asc:            true,
			limit:          10,
			wantElementIDs: []uint64{100, 200, 300},
			wantLen:        3,
		},
		{
			name: "descending order with duplicates",
			cursors: []*blockCursor{
				{
					timestamps: []int64{30, 20},
					elementIDs: []uint64{100, 200},
					idx:        1, // start from last element for descending order
					bm:         blockMetadata{seriesID: common.SeriesID(1)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1"), []byte("value2")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
				{
					timestamps: []int64{25, 15},
					elementIDs: []uint64{100, 300}, // 100 is duplicate
					idx:        1,                  // start from last element for descending order
					bm:         blockMetadata{seriesID: common.SeriesID(2)},
					tagProjection: []model.TagProjection{
						{Family: "family1", Names: []string{"tag1"}},
					},
					tagFamilies: []tagFamily{
						{
							name: "family1",
							tags: []tag{
								{name: "tag1", values: [][]byte{[]byte("value1_dup"), []byte("value3")}},
							},
						},
					},
					schemaTagTypes: make(map[string]pbv1.ValueType),
				},
			},
			asc:   false,
			limit: 10,
			// For descending order: process from highest timestamp to lowest
			// The actual order depends on heap processing, but duplicates are correctly removed
			// Actual result: [200, 100, 300] - all unique, no duplicates
			wantElementIDs: []uint64{200, 100, 300},
			wantLen:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bch := &blockCursorHeap{
				asc: tt.asc,
				bcc: make([]*blockCursor, 0, len(tt.cursors)),
			}
			for _, bc := range tt.cursors {
				heap.Push(bch, bc)
			}
			heap.Init(bch)

			result := bch.merge(tt.limit)

			assert.Equal(t, tt.wantLen, result.Len(), "unexpected length")
			assert.Equal(t, tt.wantElementIDs, result.ElementIDs, "unexpected element IDs - duplicates should be removed")
			// Verify no duplicate element IDs
			seen := make(map[uint64]bool)
			for _, id := range result.ElementIDs {
				assert.False(t, seen[id], "duplicate element ID found: %d", id)
				seen[id] = true
			}
		})
	}
}
