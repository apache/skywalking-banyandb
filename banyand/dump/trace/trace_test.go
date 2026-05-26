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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDecodeTracePartFormat verifies the library can parse the latest trace part
// format, decoding spans and tags block by block.
func TestDecodeTracePartFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err, "part directory should have valid hex name")

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "should be able to open part created by trace module")
	defer p.Close()

	assert.Equal(t, partID, p.partMetadata.ID)
	assert.Greater(t, p.partMetadata.TotalCount, uint64(0), "should have spans")
	assert.GreaterOrEqual(t, p.partMetadata.MaxTimestamp, p.partMetadata.MinTimestamp)

	// Tag types parsed from tag.type.
	assert.Equal(t, pbv1.ValueTypeStr, p.tagType["service.name"])
	assert.Equal(t, pbv1.ValueTypeInt64, p.tagType["http.status"])
	assert.Equal(t, pbv1.ValueTypeTimestamp, p.tagType["timestamp"])
	assert.Equal(t, pbv1.ValueTypeStrArr, p.tagType["tags"])

	decoder := &encoding.BytesBlockDecoder{}
	totalSpans := 0
	for blockIdx, pbm := range p.primaryBlockMetadata {
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		decompressed, decErr := zstd.Decompress(nil, primaryData)
		require.NoError(t, decErr, "should decompress primary data for primary block %d", blockIdx)

		blockMetadatas, parseErr := parseAllBlockMetadata(decompressed, p.tagType)
		require.NoError(t, parseErr, "should parse all block metadata from primary block %d", blockIdx)

		for _, bm := range blockMetadatas {
			spans, spanIDs, spanErr := readSpans(decoder, bm.spans, int(bm.count), p.spans)
			require.NoError(t, spanErr, "should read spans for trace %s", bm.traceID)
			assert.Len(t, spans, int(bm.count))
			assert.Len(t, spanIDs, int(bm.count))
			totalSpans += len(spans)

			for tagName, tagBlock := range bm.tags {
				tagValues, tagErr := readTagValues(decoder, tagBlock, tagName, int(bm.count), p.tagMetadata[tagName], p.tags[tagName], p.tagType[tagName])
				require.NoError(t, tagErr, "should read tag %s for trace %s", tagName, bm.traceID)
				assert.Len(t, tagValues, int(bm.count))
			}
		}
	}
	assert.Equal(t, int(p.partMetadata.TotalCount), totalSpans, "should have parsed all spans from metadata")
}

// TestTraceIteratorAllRows walks every span of the part and verifies, keyed by
// SpanID, the traceID, span payload and all five tags (str / int64 / timestamp /
// str-array) with their types, that Row.Timestamp is recovered from the
// ValueType=Timestamp tag, and that the iterator yields exactly TotalCount spans
// and is exhausted afterwards.
func TestTraceIteratorAllRows(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	// Row maps/bytes alias the decode buffer, so assert content inside the loop;
	// only scalar timestamps are kept to verify relative spacing afterwards.
	tsBySpan := map[string]int64{}
	count := 0
	for it.Next() {
		r := it.Row()
		count++
		tsBySpan[r.SpanID] = r.Timestamp
		require.Len(t, r.Tags, 5, "each span has five tags")
		assert.NotZero(t, r.SeriesID, "trace seriesID is derived from tags")
		assert.Nil(t, r.EntityValues, "no smeta.bin -> EntityValues nil")

		// Row.Timestamp is recovered from the ValueType=Timestamp "timestamp" tag.
		assert.Equal(t, pbv1.ValueTypeTimestamp, r.TagTypes["timestamp"])
		assert.Equal(t, r.Timestamp, convert.BytesToInt64(r.Tags["timestamp"]), "Row.Timestamp derives from the timestamp tag")
		assert.Equal(t, pbv1.ValueTypeStr, r.TagTypes["service.name"])
		assert.Equal(t, pbv1.ValueTypeInt64, r.TagTypes["http.status"])
		assert.Equal(t, pbv1.ValueTypeInt64, r.TagTypes["duration"])
		assert.Equal(t, pbv1.ValueTypeStrArr, r.TagTypes["tags"])

		service := dump.DecodeTagValue(r.TagTypes["service.name"], r.Tags["service.name"], nil).GetStr().GetValue()
		status := dump.DecodeTagValue(r.TagTypes["http.status"], r.Tags["http.status"], nil).GetInt().GetValue()
		duration := dump.DecodeTagValue(r.TagTypes["duration"], r.Tags["duration"], nil).GetInt().GetValue()
		tags := decodeStrArr(t, r.Tags["tags"])

		switch r.SpanID {
		case "span-1":
			assert.Equal(t, "test-trace-1", r.TraceID)
			assert.Equal(t, "span-data-1-with-content", string(r.Span))
			assert.Equal(t, "test-service", service)
			assert.Equal(t, int64(200), status)
			assert.Equal(t, int64(1234567), duration)
			assert.Equal(t, []string{"tag1", "tag2"}, tags)
		case "span-2":
			assert.Equal(t, "test-trace-1", r.TraceID)
			assert.Equal(t, "span-data-2-with-content", string(r.Span))
			assert.Equal(t, "test-service", service)
			assert.Equal(t, int64(404), status)
			assert.Equal(t, int64(9876543), duration)
			assert.Equal(t, []string{"tag3", "tag4"}, tags)
		case "span-3":
			assert.Equal(t, "test-trace-2", r.TraceID)
			assert.Equal(t, "span-data-3-with-content", string(r.Span))
			assert.Equal(t, "another-service", service)
			assert.Equal(t, int64(500), status)
			assert.Equal(t, int64(5555555), duration)
			assert.Equal(t, []string{"tag5"}, tags)
		default:
			t.Fatalf("unexpected spanID %q", r.SpanID)
		}
	}
	require.NoError(t, it.Err())

	assert.Equal(t, int(p.partMetadata.TotalCount), count, "iterator must yield every span")
	require.Len(t, tsBySpan, 3, "all three spans should be visited")
	assert.Equal(t, int64(1000), tsBySpan["span-2"]-tsBySpan["span-1"], "span-2 timestamp = base+1000")
	assert.Equal(t, int64(2000), tsBySpan["span-3"]-tsBySpan["span-1"], "span-3 timestamp = base+2000")
	assert.False(t, it.Next(), "iterator stays exhausted")
}

// nextVarArrayElem reads one element of a stored StrArr tag value (entity-delimited,
// escape-encoded), mirroring the production decoder. Returns the element and the
// remaining bytes.
func nextVarArrayElem(src []byte) (elem, rest []byte, ok bool) {
	if len(src) == 0 {
		return nil, nil, false
	}
	if src[0] == encoding.EntityDelimiter {
		return nil, src[1:], true
	}
	for len(src) > 0 {
		switch {
		case src[0] == encoding.Escape:
			if len(src) < 2 {
				return nil, nil, false
			}
			src = src[1:]
			elem = append(elem, src[0])
		case src[0] == encoding.EntityDelimiter:
			return elem, src[1:], true
		default:
			elem = append(elem, src[0])
		}
		src = src[1:]
	}
	return nil, nil, false
}

// decodeStrArr splits a stored StrArr tag value into its string elements.
func decodeStrArr(t *testing.T, raw []byte) []string {
	t.Helper()
	out := []string{}
	for len(raw) > 0 {
		elem, rest, ok := nextVarArrayElem(raw)
		require.True(t, ok, "malformed StrArr encoding")
		out = append(out, string(elem))
		raw = rest
	}
	return out
}

// TestTraceIteratorNoSeriesMeta verifies EntityValues is nil without smeta.bin.
func TestTraceIteratorNoSeriesMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()
	require.Nil(t, p.SeriesMap())

	it := p.Iterator()
	defer it.Close()
	for it.Next() {
		assert.Nil(t, it.Row().EntityValues, "EntityValues should be nil without smeta.bin")
	}
	require.NoError(t, it.Err())
}

// TestTraceSeriesMap verifies smeta.bin is parsed into the part-level series map.
func TestTraceSeriesMap(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, cleanup := createTestTracePartWithSeriesMetadata(tmpPath, fileSystem)
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	seriesMap := p.SeriesMap()
	require.NotNil(t, seriesMap, "series map should be parsed from smeta.bin")

	expectedSeriesID1 := common.SeriesID(convert.Hash([]byte("service.name=test-service")))
	expectedSeriesID2 := common.SeriesID(convert.Hash([]byte("service.name=another-service")))

	require.Contains(t, seriesMap, expectedSeriesID1)
	require.Contains(t, seriesMap, expectedSeriesID2)
	assert.Equal(t, "service.name=test-service", string(seriesMap[expectedSeriesID1]))
	assert.Equal(t, "service.name=another-service", string(seriesMap[expectedSeriesID2]))
}

func createTestTracePartWithSeriesMetadata(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())

	seriesMetadataPath := filepath.Join(partPath, "smeta.bin")
	docs := index.Documents{
		{
			DocID:        1,
			EntityValues: []byte("service.name=test-service"),
		},
		{
			DocID:        2,
			EntityValues: []byte("service.name=another-service"),
		},
	}

	seriesMetadataBytes, err := docs.Marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal series metadata documents: %v", err))
	}
	fs.MustFlush(fileSystem, seriesMetadataBytes, seriesMetadataPath, storage.FilePerm)

	return partPath, cleanup
}

// TestTraceOpenPartCorruptMetadata: corrupt metadata.json -> OpenPart error.
func TestTraceOpenPartCorruptMetadata(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(partPath, "metadata.json"), []byte("{not valid json"), 0o600))
	_, err = OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.Error(t, err, "OpenPart must fail on corrupt metadata.json")
}

// TestTraceOpenPartCorruptMeta: corrupt meta.bin -> OpenPart error.
func TestTraceOpenPartCorruptMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(partPath, "meta.bin"), []byte("not-zstd-garbage-data"), 0o600))
	_, err = OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.Error(t, err, "OpenPart must fail on corrupt meta.bin")
}

// TestTraceIteratorCorruptPrimary: corrupt primary.bin -> iterator terminal error.
func TestTraceIteratorCorruptPrimary(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	primaryPath := filepath.Join(partPath, "primary.bin")
	data, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	garbage := make([]byte, len(data))
	for i := range garbage {
		garbage[i] = 0xFF
	}
	require.NoError(t, os.WriteFile(primaryPath, garbage, 0o600))

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	assert.False(t, it.Next(), "Next must return false on corrupt primary block")
	require.Error(t, it.Err())
	assert.False(t, it.Next(), "iterator stays terminal")
}

// TestTraceDoubleClose: Close is safe to call multiple times.
func TestTraceDoubleClose(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	require.NoError(t, p.Close())
	assert.NotPanics(t, func() { _ = p.Close() }, "second Close must not panic")
}

// TestTraceIteratorCloseThenNext: Next after Close returns false, no panic.
func TestTraceIteratorCloseThenNext(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	require.NoError(t, it.Close())
	assert.False(t, it.Next(), "Next after Close must return false")
	assert.NotPanics(t, func() { _ = it.Close() })
}

// TestTraceDecodeSelfContained: DecodeTagValue must deep-copy so decoded protos
// survive buffer reuse across later Next() calls.
func TestTraceDecodeSelfContained(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagetrace.BuildPartForDump(tmpPath, fileSystem, 12345, storagetrace.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	require.True(t, it.Next(), "expected at least one span")
	first := it.Row()
	require.Contains(t, first.TagTypes, "service.name")
	savedTag := dump.DecodeTagValue(first.TagTypes["service.name"], first.Tags["service.name"], nil)
	savedValue := savedTag.GetStr().GetValue()
	require.NotEmpty(t, savedValue)

	for it.Next() {
		_ = it.Row()
	}
	require.NoError(t, it.Err())
	assert.Equal(t, savedValue, savedTag.GetStr().GetValue(), "DecodeTagValue must be self-contained across Next()")
}
