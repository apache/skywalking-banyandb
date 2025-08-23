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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_dataBlock_reset(t *testing.T) {
	h := &dataBlock{
		offset: 1,
		size:   1,
	}

	h.reset()

	assert.Equal(t, uint64(0), h.offset)
	assert.Equal(t, uint64(0), h.size)
}

func Test_dataBlock_copyFrom(t *testing.T) {
	src := &dataBlock{
		offset: 1,
		size:   1,
	}

	dest := &dataBlock{
		offset: 2,
		size:   2,
	}

	dest.copyFrom(src)

	assert.Equal(t, src.offset, dest.offset)
	assert.Equal(t, src.size, dest.size)
}

func Test_dataBlock_marshal_unmarshal(t *testing.T) {
	original := &dataBlock{
		offset: 1,
		size:   1,
	}

	marshaled := original.marshal(nil)

	unmarshaled := &dataBlock{}

	_ = unmarshaled.unmarshal(marshaled)

	assert.Equal(t, original.offset, unmarshaled.offset)
	assert.Equal(t, original.size, unmarshaled.size)
}

func Test_timestampsMetadata_reset(t *testing.T) {
	tm := &timestampsMetadata{
		min: 1,
		max: 1,
	}

	tm.reset()

	assert.Equal(t, int64(0), tm.min)
	assert.Equal(t, int64(0), tm.max)
}

func Test_timestampsMetadata_copyFrom(t *testing.T) {
	src := &timestampsMetadata{
		min: 1,
		max: 1,
	}

	dest := &timestampsMetadata{
		min: 2,
		max: 2,
	}

	dest.copyFrom(src)

	assert.Equal(t, src.min, dest.min)
	assert.Equal(t, src.max, dest.max)
}

func Test_blockMetadata_marshal_unmarshal(t *testing.T) {
	testCases := []struct {
		original *blockMetadata
		name     string
	}{
		{
			name: "Zero values",
			original: &blockMetadata{
				traceID:                   "",
				uncompressedSpanSizeBytes: 0,
				count:                     0,
				timestamps:                timestampsMetadata{},
				spans:                     &dataBlock{},
				tags:                      make(map[string]*dataBlock),
			},
		},
		{
			name: "Non-zero values",
			original: &blockMetadata{
				traceID:                   "trace1",
				uncompressedSpanSizeBytes: 100,
				count:                     1,
				timestamps: timestampsMetadata{
					min: 1,
					max: 1,
				},
				spans: &dataBlock{
					offset: 10,
					size:   20,
				},
				tags: map[string]*dataBlock{
					"service_name": {
						offset: 1,
						size:   1,
					},
				},
			},
		},
		{
			name: "Multiple tags and metadata",
			original: &blockMetadata{
				traceID:                   "trace1",
				uncompressedSpanSizeBytes: 200,
				count:                     2,
				timestamps: timestampsMetadata{
					min: 2,
					max: 2,
				},
				spans: &dataBlock{
					offset: 30,
					size:   40,
				},
				tags: map[string]*dataBlock{
					"service_name": {
						offset: 2,
						size:   2,
					},
					"instance_name": {
						offset: 3,
						size:   3,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshaled := tc.original.marshal(nil, 6)

			unmarshaled := blockMetadata{
				tags: make(map[string]*dataBlock),
			}

			_, err := unmarshaled.unmarshal(marshaled, nil, 6)
			require.NoError(t, err)

			assert.Equal(t, tc.original.traceID, unmarshaled.traceID)
			assert.Equal(t, tc.original.uncompressedSpanSizeBytes, unmarshaled.uncompressedSpanSizeBytes)
			assert.Equal(t, tc.original.count, unmarshaled.count)
			assert.Equal(t, tc.original.tags, unmarshaled.tags)
		})
	}
}

func Test_unmarshalBlockMetadata(t *testing.T) {
	t.Run("unmarshal valid blockMetadata", func(t *testing.T) {
		original := []blockMetadata{
			{
				traceID: "trace1",
				spans: &dataBlock{
					offset: 10,
					size:   20,
				},
				timestamps: timestampsMetadata{
					min: 1,
					max: 1,
				},
				uncompressedSpanSizeBytes: 100,
				count:                     1,
			},
			{
				traceID: "trace2",
				spans: &dataBlock{
					offset: 30,
					size:   40,
				},
				timestamps: timestampsMetadata{
					min: 2,
					max: 2,
				},
				uncompressedSpanSizeBytes: 200,
				count:                     2,
			},
		}
		wanted := []blockMetadata{
			{
				traceID: "trace1",
				tagType: make(map[string]pbv1.ValueType),
				spans: &dataBlock{
					offset: 10,
					size:   20,
				},
				timestamps:                timestampsMetadata{},
				uncompressedSpanSizeBytes: 100,
				count:                     1,
			},
			{
				traceID: "trace2",
				tagType: make(map[string]pbv1.ValueType),
				spans: &dataBlock{
					offset: 30,
					size:   40,
				},
				timestamps:                timestampsMetadata{},
				uncompressedSpanSizeBytes: 200,
				count:                     2,
			},
		}

		var marshaled []byte
		for _, bm := range original {
			marshaled = bm.marshal(marshaled, 6)
		}

		tagType := make(map[string]pbv1.ValueType)
		unmarshaled, err := unmarshalBlockMetadata(nil, marshaled, tagType, 6)
		require.NoError(t, err)
		require.Equal(t, wanted, unmarshaled)
	})

	t.Run("unmarshal invalid blockMetadata", func(t *testing.T) {
		original := []blockMetadata{
			{
				traceID: "trace2",
				spans: &dataBlock{
					offset: 30,
					size:   40,
				},
				timestamps:                timestampsMetadata{},
				uncompressedSpanSizeBytes: 200,
				count:                     2,
			},
			{
				traceID: "trace1",
				spans: &dataBlock{
					offset: 10,
					size:   20,
				},
				timestamps:                timestampsMetadata{},
				uncompressedSpanSizeBytes: 100,
				count:                     1,
			},
		}

		var marshaled []byte
		for _, bm := range original {
			marshaled = bm.marshal(marshaled, 6)
		}

		tagType := make(map[string]pbv1.ValueType)
		_, err := unmarshalBlockMetadata(nil, marshaled, tagType, 6)
		require.Error(t, err)
	})
}
