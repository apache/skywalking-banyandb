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
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestMustInitFromElements(t *testing.T) {
	tests := []struct {
		es   *elements
		name string
		want partMetadata
	}{
		{
			name: "Test with empty elements",
			es: &elements{
				timestamps:  []int64{},
				elementIDs:  []uint64{},
				seriesIDs:   []common.SeriesID{},
				tagFamilies: make([][]tagValues, 0),
			},
			want: partMetadata{},
		},
		{
			name: "Test with one item in elements",
			es: &elements{
				timestamps: []int64{1},
				elementIDs: []uint64{0},
				seriesIDs:  []common.SeriesID{1},
				tagFamilies: [][]tagValues{
					{
						{
							"arrTag", []*tagValue{
								{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
							},
						},
					},
				},
			},
			want: partMetadata{
				BlocksCount:  1,
				MinTimestamp: 1,
				MaxTimestamp: 1,
				TotalCount:   1,
			},
		},
		{
			name: "Test with multiple items in elements",
			es:   es,
			want: partMetadata{
				BlocksCount:  3,
				MinTimestamp: 1,
				MaxTimestamp: 220,
				TotalCount:   6,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := &memPart{}
			mp.mustInitFromElements(tt.es)
			assert.Equal(t, tt.want.BlocksCount, mp.partMetadata.BlocksCount)
			assert.Equal(t, tt.want.MinTimestamp, mp.partMetadata.MinTimestamp)
			assert.Equal(t, tt.want.MaxTimestamp, mp.partMetadata.MaxTimestamp)
			assert.Equal(t, tt.want.TotalCount, mp.partMetadata.TotalCount)
			assert.Equal(t, len(mp.tagFamilies), len(mp.tagFamilyMetadata))
			tmpPath, defFn := test.Space(require.New(t))
			defer defFn()
			epoch := uint64(1)
			path := partPath(tmpPath, epoch)

			fileSystem := fs.NewLocalFileSystem()
			mp.mustFlush(fileSystem, path)
			p := mustOpenFilePart(epoch, tmpPath, fileSystem)
			defer p.close()
			assert.Equal(t, tt.want.BlocksCount, p.partMetadata.BlocksCount)
			assert.Equal(t, tt.want.MinTimestamp, p.partMetadata.MinTimestamp)
			assert.Equal(t, tt.want.MaxTimestamp, p.partMetadata.MaxTimestamp)
			assert.Equal(t, tt.want.TotalCount, p.partMetadata.TotalCount)
			if len(mp.tagFamilies) > 0 {
				for k := range mp.tagFamilies {
					_, ok := mp.tagFamilyMetadata[k]
					require.True(t, ok, "mp.tagFamilyMetadata %s not found", k)
					_, ok = p.tagFamilies[k]
					require.True(t, ok, "p.tagFamilies %s not found", k)
					_, ok = p.tagFamilyMetadata[k]
					require.True(t, ok, "p.tagFamilyMetadata %s not found", k)
				}
			}
		})
	}
}

var es = &elements{
	seriesIDs:  []common.SeriesID{1, 1, 2, 2, 3, 3},
	timestamps: []int64{1, 2, 8, 10, 100, 220},
	elementIDs: []uint64{0, 1, 2, 3, 4, 5},
	tagFamilies: [][]tagValues{
		{
			{
				tag: "arrTag", values: []*tagValue{
					{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
					{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
				},
			},
			{
				tag: "binaryTag", values: []*tagValue{
					{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
					{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
				},
			},
		},
		{
			{
				tag: "arrTag", values: []*tagValue{
					{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
					{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
				},
			},
			{
				tag: "binaryTag", values: []*tagValue{
					{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
				},
			},
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
					{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
				},
			},
		},
		{
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
				},
			},
		},
		{
			{
				tag: "singleTag", values: []*tagValue{
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag11"), valueArr: nil},
					{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag22"), valueArr: nil},
				},
			},
		},
		{},
		{}, // empty tagFamilies for seriesID 3
	},
}

func TestMustInitFromPart(t *testing.T) {
	// Step 1: Load the global variable "es" to a memPart
	originalMemPart := &memPart{}
	originalMemPart.mustInitFromElements(es)

	// Step 2: Flush the memPart
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	epoch := uint64(1)
	path := partPath(tmpPath, epoch)
	fileSystem := fs.NewLocalFileSystem()
	originalMemPart.mustFlush(fileSystem, path)

	// Step 3: Init a part from the files the memPart flushed
	part := mustOpenFilePart(epoch, tmpPath, fileSystem)
	defer part.close()

	// Step 4: Convert the part to a new memPart
	newMemPart := &memPart{}
	newMemPart.mustInitFromPart(part)

	// Step 5: Compare the new memPart with the original memPart
	// Compare part metadata
	assert.NotEqual(t, originalMemPart.partMetadata.ID, newMemPart.partMetadata.ID)
	assert.Equal(t, originalMemPart.partMetadata.BlocksCount, newMemPart.partMetadata.BlocksCount)
	assert.Equal(t, originalMemPart.partMetadata.MinTimestamp, newMemPart.partMetadata.MinTimestamp)
	assert.Equal(t, originalMemPart.partMetadata.MaxTimestamp, newMemPart.partMetadata.MaxTimestamp)
	assert.Equal(t, originalMemPart.partMetadata.TotalCount, newMemPart.partMetadata.TotalCount)

	// Compare primary data
	assert.Equal(t, originalMemPart.primary.Buf, newMemPart.primary.Buf)

	// Compare timestamps data
	assert.Equal(t, originalMemPart.timestamps.Buf, newMemPart.timestamps.Buf)

	// Compare meta data (primaryBlockMetadata)
	assert.Equal(t, originalMemPart.meta.Buf, newMemPart.meta.Buf)

	// Compare tag families
	assert.Equal(t, len(originalMemPart.tagFamilies), len(newMemPart.tagFamilies))
	for name, originalBuffer := range originalMemPart.tagFamilies {
		newBuffer, exists := newMemPart.tagFamilies[name]
		assert.True(t, exists, "Tag family %s not found in new memPart", name)
		assert.Equal(t, originalBuffer.Buf, newBuffer.Buf)
	}

	// Compare tag family metadata
	assert.Equal(t, len(originalMemPart.tagFamilyMetadata), len(newMemPart.tagFamilyMetadata))
	for name, originalBuffer := range originalMemPart.tagFamilyMetadata {
		newBuffer, exists := newMemPart.tagFamilyMetadata[name]
		assert.True(t, exists, "Tag family metadata %s not found in new memPart", name)
		assert.Equal(t, originalBuffer.Buf, newBuffer.Buf)
	}

	// Compare tag family filters
	assert.Equal(t, len(originalMemPart.tagFamilyFilter), len(newMemPart.tagFamilyFilter))
	for name, originalBuffer := range originalMemPart.tagFamilyFilter {
		newBuffer, exists := newMemPart.tagFamilyFilter[name]
		assert.True(t, exists, "Tag family filter %s not found in new memPart", name)
		assert.Equal(t, originalBuffer.Buf, newBuffer.Buf)
	}
}

func Test_memPart_Marshal_Unmarshal(t *testing.T) {
	// Create a test memPart with some data
	mp := generateMemPart()
	defer releaseMemPart(mp)

	// Initialize with test elements
	esData := &elements{
		timestamps: []int64{1, 2, 3},
		elementIDs: []uint64{100, 200, 300},
		seriesIDs:  []common.SeriesID{1, 1, 2},
		tagFamilies: [][]tagValues{
			{
				{
					tag: "service",
					values: []*tagValue{
						{tag: "name", valueType: pbv1.ValueTypeStr, value: []byte("test-service")},
					},
				},
			},
			{
				{
					tag: "service",
					values: []*tagValue{
						{tag: "name", valueType: pbv1.ValueTypeStr, value: []byte("test-service-2")},
					},
				},
			},
			{
				{
					tag: "instance",
					values: []*tagValue{
						{tag: "name", valueType: pbv1.ValueTypeStr, value: []byte("test-instance")},
					},
				},
			},
		},
	}

	mp.mustInitFromElements(esData)

	// Test marshal
	marshaled, err := mp.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, marshaled)

	// Test unmarshal
	mp2 := generateMemPart()
	defer releaseMemPart(mp2)

	err = mp2.Unmarshal(marshaled)
	require.NoError(t, err)

	// Verify the unmarshaled data matches the original
	require.Equal(t, mp.partMetadata.ID, mp2.partMetadata.ID)
	require.Equal(t, mp.partMetadata.TotalCount, mp2.partMetadata.TotalCount)
	require.Equal(t, mp.partMetadata.BlocksCount, mp2.partMetadata.BlocksCount)
	require.Equal(t, mp.partMetadata.MinTimestamp, mp2.partMetadata.MinTimestamp)
	require.Equal(t, mp.partMetadata.MaxTimestamp, mp2.partMetadata.MaxTimestamp)

	// Verify buffers
	require.Equal(t, mp.meta.Buf, mp2.meta.Buf)
	require.Equal(t, mp.primary.Buf, mp2.primary.Buf)
	require.Equal(t, mp.timestamps.Buf, mp2.timestamps.Buf)

	// Verify tag families
	require.Equal(t, len(mp.tagFamilies), len(mp2.tagFamilies))
	for name, tf := range mp.tagFamilies {
		tf2, exists := mp2.tagFamilies[name]
		require.True(t, exists)
		require.Equal(t, tf.Buf, tf2.Buf)
	}

	// Verify tag family metadata
	require.Equal(t, len(mp.tagFamilyMetadata), len(mp2.tagFamilyMetadata))
	for name, tfh := range mp.tagFamilyMetadata {
		tfh2, exists := mp2.tagFamilyMetadata[name]
		require.True(t, exists)
		require.Equal(t, tfh.Buf, tfh2.Buf)
	}

	// Verify tag family filter
	require.Equal(t, len(mp.tagFamilyFilter), len(mp2.tagFamilyFilter))
	for name, tff := range mp.tagFamilyFilter {
		tff2, exists := mp2.tagFamilyFilter[name]
		require.True(t, exists)
		require.Equal(t, tff.Buf, tff2.Buf)
	}
}

func Test_memPart_Marshal_Unmarshal_Empty(t *testing.T) {
	// Test with empty memPart
	mp := generateMemPart()
	defer releaseMemPart(mp)

	// Test marshal
	marshaled, err := mp.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, marshaled)

	// Test unmarshal
	mp2 := generateMemPart()
	defer releaseMemPart(mp2)

	err = mp2.Unmarshal(marshaled)
	require.NoError(t, err)

	// Verify the unmarshaled data matches the original
	// partMetadata.ID is not serialized, so always expect 0 after unmarshal
	require.Equal(t, uint64(0), mp2.partMetadata.ID)
	require.Equal(t, mp.partMetadata.TotalCount, mp2.partMetadata.TotalCount)
	require.Equal(t, mp.partMetadata.BlocksCount, mp2.partMetadata.BlocksCount)
	require.Equal(t, mp.partMetadata.MinTimestamp, mp2.partMetadata.MinTimestamp)
	require.Equal(t, mp.partMetadata.MaxTimestamp, mp2.partMetadata.MaxTimestamp)

	// Verify buffers are empty
	require.Empty(t, mp2.meta.Buf)
	require.Empty(t, mp2.primary.Buf)
	require.Empty(t, mp2.timestamps.Buf)
	require.Empty(t, mp2.tagFamilies)
	require.Empty(t, mp2.tagFamilyMetadata)
	require.Empty(t, mp2.tagFamilyFilter)
}

func Test_memPart_Marshal_Unmarshal_Simple(t *testing.T) {
	// Create a test memPart manually without using mustInitFromElements
	mp := generateMemPart()
	defer releaseMemPart(mp)

	// Set some basic metadata
	mp.partMetadata.ID = 123
	mp.partMetadata.TotalCount = 10
	mp.partMetadata.BlocksCount = 2
	mp.partMetadata.MinTimestamp = 1000
	mp.partMetadata.MaxTimestamp = 2000

	// Set some buffer data
	mp.meta.Buf = []byte("test meta data")
	mp.primary.Buf = []byte("test primary data")
	mp.timestamps.Buf = []byte("test timestamps data")

	// Create some tag families
	mp.tagFamilies = make(map[string]*bytes.Buffer)
	mp.tagFamilies["service"] = &bytes.Buffer{Buf: []byte("service data")}
	mp.tagFamilies["instance"] = &bytes.Buffer{Buf: []byte("instance data")}

	// Create some tag family metadata
	mp.tagFamilyMetadata = make(map[string]*bytes.Buffer)
	mp.tagFamilyMetadata["service"] = &bytes.Buffer{Buf: []byte("service metadata")}
	mp.tagFamilyMetadata["instance"] = &bytes.Buffer{Buf: []byte("instance metadata")}

	// Create some tag family filters
	mp.tagFamilyFilter = make(map[string]*bytes.Buffer)
	mp.tagFamilyFilter["service"] = &bytes.Buffer{Buf: []byte("service filter")}
	mp.tagFamilyFilter["instance"] = &bytes.Buffer{Buf: []byte("instance filter")}

	// Test marshal
	marshaled, err := mp.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, marshaled)

	// Test unmarshal
	mp2 := generateMemPart()
	defer releaseMemPart(mp2)

	err = mp2.Unmarshal(marshaled)
	require.NoError(t, err)

	// Verify the unmarshaled data matches the original
	require.NotEqual(t, mp.partMetadata.ID, mp2.partMetadata.ID)
	require.Equal(t, mp.partMetadata.TotalCount, mp2.partMetadata.TotalCount)
	require.Equal(t, mp.partMetadata.BlocksCount, mp2.partMetadata.BlocksCount)
	require.Equal(t, mp.partMetadata.MinTimestamp, mp2.partMetadata.MinTimestamp)
	require.Equal(t, mp.partMetadata.MaxTimestamp, mp2.partMetadata.MaxTimestamp)

	// Verify buffers
	require.Equal(t, mp.meta.Buf, mp2.meta.Buf)
	require.Equal(t, mp.primary.Buf, mp2.primary.Buf)
	require.Equal(t, mp.timestamps.Buf, mp2.timestamps.Buf)

	// Verify tag families
	require.Equal(t, len(mp.tagFamilies), len(mp2.tagFamilies))
	for name, tf := range mp.tagFamilies {
		tf2, exists := mp2.tagFamilies[name]
		require.True(t, exists)
		require.Equal(t, tf.Buf, tf2.Buf)
	}

	// Verify tag family metadata
	require.Equal(t, len(mp.tagFamilyMetadata), len(mp2.tagFamilyMetadata))
	for name, tfh := range mp.tagFamilyMetadata {
		tfh2, exists := mp2.tagFamilyMetadata[name]
		require.True(t, exists)
		require.Equal(t, tfh.Buf, tfh2.Buf)
	}

	// Verify tag family filter
	require.Equal(t, len(mp.tagFamilyFilter), len(mp2.tagFamilyFilter))
	for name, tff := range mp.tagFamilyFilter {
		tff2, exists := mp2.tagFamilyFilter[name]
		require.True(t, exists)
		require.Equal(t, tff.Buf, tff2.Buf)
	}
}

func Test_memPart_Marshal_Unmarshal_Minimal(t *testing.T) {
	// Create a minimal test memPart
	mp := generateMemPart()
	defer releaseMemPart(mp)

	// Set only basic metadata
	mp.partMetadata.ID = 123
	mp.partMetadata.TotalCount = 10

	// Set only basic buffer data
	mp.meta.Buf = []byte("test")
	mp.primary.Buf = []byte("test")
	mp.timestamps.Buf = []byte("test")

	// No tag families, metadata, or filters

	// Test marshal
	marshaled, err := mp.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, marshaled)

	// Debug: Check what was marshaled
	t.Logf("Marshaled data length: %d", len(marshaled))
	t.Logf("Original partMetadata ID: %d", mp.partMetadata.ID)

	// Test unmarshal
	mp2 := generateMemPart()
	defer releaseMemPart(mp2)

	err = mp2.Unmarshal(marshaled)
	require.NoError(t, err)

	// Debug: Check what was unmarshaled
	t.Logf("Unmarshaled partMetadata ID: %d", mp2.partMetadata.ID)

	// Verify the unmarshaled data matches the original
	// partMetadata.ID is not serialized, so always expect 0 after unmarshal
	require.Equal(t, uint64(0), mp2.partMetadata.ID)
	require.Equal(t, mp.partMetadata.TotalCount, mp2.partMetadata.TotalCount)

	// Verify buffers
	require.Equal(t, mp.meta.Buf, mp2.meta.Buf)
	require.Equal(t, mp.primary.Buf, mp2.primary.Buf)
	require.Equal(t, mp.timestamps.Buf, mp2.timestamps.Buf)

	// Verify maps are empty (not nil, but empty)
	require.Empty(t, mp2.tagFamilies)
	require.Empty(t, mp2.tagFamilyMetadata)
	require.Empty(t, mp2.tagFamilyFilter)
}
