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

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestMustInitFromTraces(t *testing.T) {
	tests := []struct {
		ts   *traces
		name string
		want partMetadata
	}{
		{
			name: "Test with empty traces",
			ts: &traces{
				traceIDs:   []string{},
				timestamps: []int64{},
				tags:       [][]*tagValue{},
				spans:      [][]byte{},
				spanIDs:    []string{},
			},
			want: partMetadata{},
		},
		{
			name: "Test with one item in traces",
			ts: &traces{
				traceIDs:   []string{"trace1"},
				timestamps: []int64{1},
				tags: [][]*tagValue{
					{
						{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
						{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
					},
				},
				spans:   [][]byte{[]byte("span1")},
				spanIDs: []string{"span1"},
			},
			want: partMetadata{
				BlocksCount:  1,
				MinTimestamp: 1,
				MaxTimestamp: 1,
				TotalCount:   1,
			},
		},
		{
			name: "Test with multiple items in traces",
			ts:   ts,
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
			mp.mustInitFromTraces(tt.ts)
			assert.Equal(t, tt.want.BlocksCount, mp.partMetadata.BlocksCount)
			assert.Equal(t, tt.want.MinTimestamp, mp.partMetadata.MinTimestamp)
			assert.Equal(t, tt.want.MaxTimestamp, mp.partMetadata.MaxTimestamp)
			assert.Equal(t, tt.want.TotalCount, mp.partMetadata.TotalCount)
			assert.Equal(t, len(mp.tags), len(mp.tagMetadata))
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
			if len(mp.tags) > 0 {
				for k := range mp.tags {
					_, ok := mp.tagMetadata[k]
					require.True(t, ok, "mp.tagMetadata %s not found", k)
					_, ok = p.tags[k]
					require.True(t, ok, "p.tags %s not found", k)
					_, ok = p.tagMetadata[k]
					require.True(t, ok, "p.tagMetadata %s not found", k)
				}
			}
		})
	}
}

var ts = &traces{
	traceIDs:   []string{"trace1", "trace1", "trace2", "trace2", "trace3", "trace3"},
	timestamps: []int64{1, 2, 8, 10, 100, 220},
	tags: [][]*tagValue{
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: nil},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag1"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: nil, valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: nil},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("tag2"), valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: nil, valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: nil},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: nil, valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: nil, valueArr: nil},
		},
		{
			{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: nil},
			{tag: "strTag", valueType: pbv1.ValueTypeStr, value: nil, valueArr: nil},
			{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: nil, valueArr: nil},
		},
	},
	spans: [][]byte{
		[]byte("span1"),
		[]byte("span2"),
		[]byte("span3"),
		[]byte("span4"),
		[]byte("span5"),
		[]byte("span6"),
	},
	spanIDs: []string{"span1", "span2", "span3", "span4", "span5", "span6"},
}

func TestMustInitFromPart(t *testing.T) {
	// Step 1: Load the global variable "ts" to a memPart
	originalMemPart := &memPart{}
	originalMemPart.mustInitFromTraces(ts)

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

	// Compare spans data
	assert.Equal(t, originalMemPart.spans.Buf, newMemPart.spans.Buf)

	// Compare meta data (primaryBlockMetadata)
	assert.Equal(t, originalMemPart.meta.Buf, newMemPart.meta.Buf)

	// Compare tags
	assert.Equal(t, len(originalMemPart.tags), len(newMemPart.tags))
	for name, originalBuffer := range originalMemPart.tags {
		newBuffer, exists := newMemPart.tags[name]
		assert.True(t, exists, "Tag %s not found in new memPart", name)
		assert.Equal(t, originalBuffer.Buf, newBuffer.Buf)
	}

	// Compare tag metadata
	assert.Equal(t, len(originalMemPart.tagMetadata), len(newMemPart.tagMetadata))
	for name, originalBuffer := range originalMemPart.tagMetadata {
		newBuffer, exists := newMemPart.tagMetadata[name]
		assert.True(t, exists, "Tag metadata %s not found in new memPart", name)
		assert.Equal(t, originalBuffer.Buf, newBuffer.Buf)
	}
}
