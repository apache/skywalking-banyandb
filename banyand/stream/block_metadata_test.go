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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
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
		dataBlock: dataBlock{
			offset: 1,
			size:   1,
		},
		min:              1,
		max:              1,
		encodeType:       encoding.EncodeTypeConst,
		elementIDsOffset: 1,
	}

	tm.reset()

	assert.Equal(t, uint64(0), tm.dataBlock.offset)
	assert.Equal(t, uint64(0), tm.dataBlock.size)
	assert.Equal(t, int64(0), tm.min)
	assert.Equal(t, int64(0), tm.max)
	assert.Equal(t, uint64(0), tm.elementIDsOffset)
	assert.Equal(t, encoding.EncodeTypeUnknown, tm.encodeType)
}

func Test_timestampsMetadata_copyFrom(t *testing.T) {
	src := &timestampsMetadata{
		dataBlock: dataBlock{
			offset: 1,
			size:   1,
		},
		min:              1,
		max:              1,
		encodeType:       encoding.EncodeTypeConst,
		elementIDsOffset: 1,
	}

	dest := &timestampsMetadata{
		dataBlock: dataBlock{
			offset: 2,
			size:   2,
		},
		min:              2,
		max:              2,
		encodeType:       encoding.EncodeTypeDelta,
		elementIDsOffset: 2,
	}

	dest.copyFrom(src)

	assert.Equal(t, src.dataBlock.offset, dest.dataBlock.offset)
	assert.Equal(t, src.dataBlock.size, dest.dataBlock.size)
	assert.Equal(t, src.min, dest.min)
	assert.Equal(t, src.max, dest.max)
	assert.Equal(t, src.encodeType, dest.encodeType)
	assert.Equal(t, src.elementIDsOffset, dest.elementIDsOffset)
}

func Test_timestampsMetadata_marshal_unmarshal(t *testing.T) {
	original := &timestampsMetadata{
		dataBlock: dataBlock{
			offset: 1,
			size:   1,
		},
		min:              1,
		max:              1,
		encodeType:       encoding.EncodeTypeConst,
		elementIDsOffset: 1,
	}

	marshaled := original.marshal(nil)

	unmarshaled := &timestampsMetadata{}

	_ = unmarshaled.unmarshal(marshaled)

	assert.Equal(t, original.dataBlock.offset, unmarshaled.dataBlock.offset)
	assert.Equal(t, original.dataBlock.size, unmarshaled.dataBlock.size)
	assert.Equal(t, original.min, unmarshaled.min)
	assert.Equal(t, original.max, unmarshaled.max)
	assert.Equal(t, original.encodeType, unmarshaled.encodeType)
	assert.Equal(t, original.elementIDsOffset, unmarshaled.elementIDsOffset)
}

func Test_blockMetadata_marshal_unmarshal(t *testing.T) {
	testCases := []struct {
		original *blockMetadata
		name     string
	}{
		{
			name: "Zero values",
			original: &blockMetadata{
				seriesID:              common.SeriesID(0),
				uncompressedSizeBytes: 0,
				count:                 0,
				timestamps:            timestampsMetadata{},
				tagFamilies:           make(map[string]*dataBlock),
			},
		},
		{
			name: "Non-zero values",
			original: &blockMetadata{
				seriesID:              common.SeriesID(1),
				uncompressedSizeBytes: 1,
				count:                 1,
				timestamps: timestampsMetadata{
					dataBlock: dataBlock{
						offset: 1,
						size:   1,
					},
					min:        1,
					max:        1,
					encodeType: encoding.EncodeTypeConst,
				},
				tagFamilies: map[string]*dataBlock{
					"tag1": {
						offset: 1,
						size:   1,
					},
				},
			},
		},
		{
			name: "Multiple tagFamilies and tagMetadata",
			original: &blockMetadata{
				seriesID:              common.SeriesID(2),
				uncompressedSizeBytes: 2,
				count:                 2,
				timestamps: timestampsMetadata{
					dataBlock: dataBlock{
						offset: 2,
						size:   2,
					},
					min:        2,
					max:        2,
					encodeType: encoding.EncodeTypeConst,
				},
				tagFamilies: map[string]*dataBlock{
					"tag1": {
						offset: 2,
						size:   2,
					},
					"tag2": {
						offset: 3,
						size:   3,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshaled := tc.original.marshal(nil)

			unmarshaled := blockMetadata{
				tagFamilies: make(map[string]*dataBlock),
			}

			_, err := unmarshaled.unmarshal(marshaled)
			require.NoError(t, err)

			assert.Equal(t, tc.original.seriesID, unmarshaled.seriesID)
			assert.Equal(t, tc.original.uncompressedSizeBytes, unmarshaled.uncompressedSizeBytes)
			assert.Equal(t, tc.original.count, unmarshaled.count)
			assert.Equal(t, tc.original.timestamps, unmarshaled.timestamps)
			assert.Equal(t, tc.original.tagFamilies, unmarshaled.tagFamilies)
		})
	}
}

func Test_unmarshalBlockMetadata(t *testing.T) {
	t.Run("unmarshal valid blockMetadata", func(t *testing.T) {
		original := []blockMetadata{
			{
				seriesID: common.SeriesID(1),
				timestamps: timestampsMetadata{
					dataBlock: dataBlock{
						offset: 1,
						size:   1,
					},
					min:        1,
					max:        1,
					encodeType: encoding.EncodeTypeConst,
				},
			},
			{
				seriesID: common.SeriesID(2),
				timestamps: timestampsMetadata{
					dataBlock: dataBlock{
						offset: 2,
						size:   2,
					},
					min:        2,
					max:        2,
					encodeType: encoding.EncodeTypeConst,
				},
			},
		}

		var marshaled []byte
		for _, bm := range original {
			marshaled = bm.marshal(marshaled)
		}

		unmarshaled, err := unmarshalBlockMetadata(nil, marshaled)
		require.NoError(t, err)
		require.Equal(t, original, unmarshaled)
	})

	t.Run("unmarshal invalid blockMetadata", func(t *testing.T) {
		original := []blockMetadata{
			{
				seriesID: common.SeriesID(2),
				timestamps: timestampsMetadata{
					dataBlock: dataBlock{
						offset: 2,
						size:   2,
					},
					min:        2,
					max:        2,
					encodeType: encoding.EncodeTypeConst,
				},
			},
			{
				seriesID: common.SeriesID(1),
				timestamps: timestampsMetadata{
					dataBlock: dataBlock{
						offset: 1,
						size:   1,
					},
					min:        1,
					max:        1,
					encodeType: encoding.EncodeTypeConst,
				},
			},
		}

		var marshaled []byte
		for _, bm := range original {
			marshaled = bm.marshal(marshaled)
		}

		_, err := unmarshalBlockMetadata(nil, marshaled)
		require.Error(t, err)
	})
}
