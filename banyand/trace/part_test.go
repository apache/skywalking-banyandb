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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestMustInitFromTraces(t *testing.T) {
	tests := []struct {
		ts                     *traces
		name                   string
		expectedTraceIDs       []string
		notExpectedTraceIDs    []string
		want                   partMetadata
		expectEmptyBloomFilter bool
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
			want:                   partMetadata{},
			expectedTraceIDs:       []string{},
			notExpectedTraceIDs:    []string{"trace1", "trace2"},
			expectEmptyBloomFilter: true,
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
			expectedTraceIDs:       []string{"trace1"},
			notExpectedTraceIDs:    []string{"trace2", "trace3", "trace0"},
			expectEmptyBloomFilter: false,
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
			expectedTraceIDs:       []string{"trace1", "trace2", "trace3"},
			notExpectedTraceIDs:    []string{"trace0", "trace4", "trace5", "nonexistent"},
			expectEmptyBloomFilter: false,
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

			// Verify bloom filter in memPart
			if tt.expectEmptyBloomFilter {
				assert.Nil(t, mp.traceIDFilter.filter, "Expected nil bloom filter for empty traces")
			} else {
				require.NotNil(t, mp.traceIDFilter.filter, "Expected non-nil bloom filter")
				// Verify expected trace IDs are in the filter
				for _, traceID := range tt.expectedTraceIDs {
					assert.True(t, mp.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)),
						"Expected trace ID %s to be in bloom filter", traceID)
				}
				// Verify not expected trace IDs are not in the filter (or might be due to false positives)
				for _, traceID := range tt.notExpectedTraceIDs {
					// Note: We can't assert false here due to potential false positives in bloom filters
					// But we log it for debugging purposes
					if mp.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
						t.Logf("Bloom filter returned potential false positive for trace ID: %s", traceID)
					}
				}
			}

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

			// Verify bloom filter in filePart after flush/load
			if tt.expectEmptyBloomFilter {
				assert.Nil(t, p.traceIDFilter.filter, "Expected nil bloom filter for empty traces after flush")
			} else {
				require.NotNil(t, p.traceIDFilter.filter, "Expected non-nil bloom filter after flush")
				// Verify expected trace IDs are in the filter
				for _, traceID := range tt.expectedTraceIDs {
					assert.True(t, p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)),
						"Expected trace ID %s to be in bloom filter after flush", traceID)
				}
				// Verify bloom filter properties
				assert.Equal(t, len(tt.expectedTraceIDs), p.traceIDFilter.filter.N(),
					"Bloom filter N should match number of unique trace IDs")
			}

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

func TestSeriesMetadataPersistence(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	epoch := uint64(12345)
	path := partPath(tmpPath, epoch)

	// Create a memPart with traces
	mp := generateMemPart()
	mp.mustInitFromTraces(ts)

	// Create sample series metadata using NewBytesField
	field1 := index.NewBytesField(index.FieldKey{
		IndexRuleID: 1,
		Analyzer:    "keyword",
		TagName:     "tag1",
	}, []byte("term1"))
	field2 := index.NewBytesField(index.FieldKey{
		IndexRuleID: 2,
		Analyzer:    "keyword",
		TagName:     "tag2",
	}, []byte("term2"))
	metadataDocs := index.Documents{
		{
			DocID:        1,
			EntityValues: []byte("entity1"),
			Fields:       []index.Field{field1},
		},
		{
			DocID:        2,
			EntityValues: []byte("entity2"),
			Fields:       []index.Field{field2},
		},
	}

	// Marshal series metadata
	seriesMetadataBytes, err := metadataDocs.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, seriesMetadataBytes)

	// Set series metadata in memPart
	_, err = mp.seriesMetadata.Write(seriesMetadataBytes)
	require.NoError(t, err)

	// Flush to disk
	mp.mustFlush(fileSystem, path)

	// Verify series metadata file exists by trying to read it
	seriesMetadataPath := filepath.Join(path, seriesMetadataFilename)
	readBytes, err := fileSystem.Read(seriesMetadataPath)
	require.NoError(t, err, "series metadata file should exist")
	assert.Equal(t, seriesMetadataBytes, readBytes, "series metadata content should match")

	// Open the part and verify series metadata is accessible
	p := mustOpenFilePart(epoch, tmpPath, fileSystem)
	defer p.close()

	// Verify series metadata reader is available
	assert.NotNil(t, p.seriesMetadata, "series metadata reader should be available")

	// Read and unmarshal series metadata using SequentialRead
	seqReader := p.seriesMetadata.SequentialRead()
	defer seqReader.Close()
	readMetadataBytes := make([]byte, 0)
	buf := make([]byte, 1024)
	for {
		var n int
		n, err = seqReader.Read(buf)
		if n == 0 {
			if err != nil {
				break
			}
			continue
		}
		if err != nil {
			break
		}
		readMetadataBytes = append(readMetadataBytes, buf[:n]...)
	}

	var readDocs index.Documents
	err = readDocs.Unmarshal(readMetadataBytes)
	require.NoError(t, err)
	assert.Equal(t, len(metadataDocs), len(readDocs), "number of documents should match")
	assert.Equal(t, metadataDocs[0].DocID, readDocs[0].DocID, "first document DocID should match")
	assert.Equal(t, metadataDocs[1].DocID, readDocs[1].DocID, "second document DocID should match")
}

func TestSeriesMetadataBackwardCompatibility(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	epoch := uint64(67890)
	path := partPath(tmpPath, epoch)

	// Create a memPart with traces but without series metadata
	mp := generateMemPart()
	mp.mustInitFromTraces(ts)
	// Don't set series metadata to simulate old parts

	// Flush to disk
	mp.mustFlush(fileSystem, path)

	// Verify series metadata file does not exist by trying to read it
	seriesMetadataPath := filepath.Join(path, seriesMetadataFilename)
	_, err := fileSystem.Read(seriesMetadataPath)
	assert.Error(t, err, "series metadata file should not exist for old parts")

	// Open the part - should work without series metadata (backward compatibility)
	p := mustOpenFilePart(epoch, tmpPath, fileSystem)
	defer p.close()

	// Verify part can be opened successfully
	assert.NotNil(t, p, "part should be opened successfully")
	assert.Nil(t, p.seriesMetadata, "series metadata reader should be nil for old parts")
}
