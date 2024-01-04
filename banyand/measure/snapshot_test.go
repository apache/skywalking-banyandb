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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotGetParts(t *testing.T) {
	tests := []struct {
		snapshot *snapshot
		name     string
		dst      []*part
		expected []*part
		opts     queryOptions
		count    int
	}{
		{
			name: "Test with empty snapshot",
			snapshot: &snapshot{
				parts: []*partWrapper{},
			},
			dst: []*part{},
			opts: queryOptions{
				minTimestamp: 0,
				maxTimestamp: 10,
			},
			expected: []*part{},
			count:    0,
		},
		{
			name: "Test with non-empty snapshot and no matching parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 0,
							MaxTimestamp: 5,
						}},
					},
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 6,
							MaxTimestamp: 10,
						}},
					},
				},
			},
			dst: []*part{},
			opts: queryOptions{
				minTimestamp: 11,
				maxTimestamp: 15,
			},
			expected: []*part{},
			count:    0,
		},
		{
			name: "Test with non-empty snapshot and some matching parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 0,
							MaxTimestamp: 5,
						}},
					},
					{
						p: &part{partMetadata: partMetadata{
							MinTimestamp: 6,
							MaxTimestamp: 10,
						}},
					},
				},
			},
			dst: []*part{},
			opts: queryOptions{
				minTimestamp: 5,
				maxTimestamp: 10,
			},
			expected: []*part{
				{partMetadata: partMetadata{
					MinTimestamp: 0,
					MaxTimestamp: 5,
				}},
				{partMetadata: partMetadata{
					MinTimestamp: 6,
					MaxTimestamp: 10,
				}},
			},
			count: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, count := tt.snapshot.getParts(tt.dst, tt.opts)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.count, count)
		})
	}
}

func TestSnapshotCopyAllTo(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  snapshot
		expected  snapshot
		nextEpoch uint64
		closePrev bool
	}{
		{
			name: "Test with empty snapshot",
			snapshot: snapshot{
				parts: []*partWrapper{},
			},
			nextEpoch: 1,
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: nil,
			},
		},
		{
			name: "Test with non-empty snapshot",
			snapshot: snapshot{
				parts: []*partWrapper{
					{ref: 1},
					{ref: 2},
				},
			},
			nextEpoch: 1,
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{ref: 2},
					{ref: 3},
				},
			},
		},
		{
			name: "Test with closed previous snapshot",
			snapshot: snapshot{
				parts: []*partWrapper{
					{ref: 1},
					{ref: 2},
				},
			},
			nextEpoch: 1,
			closePrev: true,
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{ref: 1},
					{ref: 2},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.snapshot.copyAllTo(tt.nextEpoch)
			if tt.closePrev {
				tt.snapshot.decRef()
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSnapshotMerge(t *testing.T) {
	tests := []struct {
		snapshot  *snapshot
		nextParts map[uint64]*partWrapper
		name      string
		expected  snapshot
		nextEpoch uint64
		closePrev bool
	}{
		{
			name: "Test with empty snapshot and empty next parts",
			snapshot: &snapshot{
				parts: []*partWrapper{},
			},
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: nil,
			},
		},
		{
			name: "Test with non-empty snapshot and empty next parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 2},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 3},
				},
			},
		},
		{
			name: "Test with non-empty snapshot and non-empty next parts",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{
				2: {p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				3: {p: &part{partMetadata: partMetadata{ID: 3}}, ref: 1},
			},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 2},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				},
			},
		},
		{
			name: "Test with closed previous snapshot",
			snapshot: &snapshot{
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 2},
				},
			},
			closePrev: true,
			nextEpoch: 1,
			nextParts: map[uint64]*partWrapper{
				2: {p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				3: {p: &part{partMetadata: partMetadata{ID: 3}}, ref: 1},
			},
			expected: snapshot{
				epoch: 1,
				ref:   1,
				parts: []*partWrapper{
					{p: &part{partMetadata: partMetadata{ID: 1}}, ref: 1},
					{p: &part{partMetadata: partMetadata{ID: 2}}, ref: 1},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.snapshot.merge(tt.nextEpoch, tt.nextParts)
			if tt.closePrev {
				tt.snapshot.decRef()
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}
