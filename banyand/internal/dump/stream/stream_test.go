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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	storagestream "github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDecodeStreamPartFormat verifies the library can parse the latest stream
// part format, decoding it block by block.
func TestDecodeStreamPartFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err, "part directory should have valid hex name")

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "should be able to open part created by stream module")
	defer p.Close()

	assert.Equal(t, partID, p.partMetadata.ID)
	assert.Greater(t, p.partMetadata.TotalCount, uint64(0), "should have elements")
	assert.Greater(t, p.partMetadata.BlocksCount, uint64(0), "should have at least 1 block")
	assert.GreaterOrEqual(t, p.partMetadata.MaxTimestamp, p.partMetadata.MinTimestamp)
	assert.Greater(t, len(p.primaryBlockMetadata), 0, "should have at least 1 primary block")

	decoder := &encoding.BytesBlockDecoder{}
	totalElements := 0

	for blockIdx, pbm := range p.primaryBlockMetadata {
		primaryData := make([]byte, pbm.Size)
		fs.MustReadData(p.primary, int64(pbm.Offset), primaryData)

		decompressed, decErr := zstd.Decompress(nil, primaryData)
		require.NoError(t, decErr, "should decompress primary data for primary block %d", blockIdx)

		blockMetadatas, parseErr := parseBlockMetadata(decompressed)
		require.NoError(t, parseErr, "should parse all block metadata from primary block %d", blockIdx)

		for _, bm := range blockMetadatas {
			timestamps, elementIDs, tsErr := readTimestamps(bm.timestamps, int(bm.count), p.timestamps, nil)
			require.NoError(t, tsErr, "should read timestamps/elementIDs for series %d", bm.seriesID)
			assert.Len(t, timestamps, int(bm.count), "should have correct number of timestamps")
			assert.Len(t, elementIDs, int(bm.count), "should have correct number of elementIDs")

			totalElements += len(timestamps)

			for _, ts := range timestamps {
				assert.GreaterOrEqual(t, ts, p.partMetadata.MinTimestamp, "timestamp should be >= min")
				assert.LessOrEqual(t, ts, p.partMetadata.MaxTimestamp, "timestamp should be <= max")
			}

			for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
				tagFamilyMetadataData := make([]byte, tagFamilyBlock.size)
				fs.MustReadData(p.tagFamilyMetadata[tagFamilyName], int64(tagFamilyBlock.offset), tagFamilyMetadataData)

				tagMetadatas, tmErr := parseTagFamilyMetadata(tagFamilyMetadataData)
				require.NoError(t, tmErr, "should parse tag family metadata %s for series %d", tagFamilyName, bm.seriesID)

				for _, tagMeta := range tagMetadatas {
					fullTagName := tagFamilyName + "." + tagMeta.name
					tagValues, tagErr := dump.ReadTagValues(decoder, tagMeta.dataBlock.offset, tagMeta.dataBlock.size,
						int(bm.count), p.tagFamilies[tagFamilyName], tagMeta.valueType, nil)
					require.NoError(t, tagErr, "should read tag %s for series %d", fullTagName, bm.seriesID)
					assert.Len(t, tagValues, int(bm.count), "tag %s should have value for each element", fullTagName)
				}
			}
		}
	}

	assert.Equal(t, int(p.partMetadata.TotalCount), totalElements, "should have parsed all elements from metadata")
}

// TestStreamIteratorAllRows walks every element of the part and verifies, per
// series, each tag's value and type (read == written by StandardDumpRows) plus
// the elementID and timestamp; it also asserts the iterator yields exactly
// TotalCount elements and is exhausted afterwards. Stream has no version/fields.
func TestStreamIteratorAllRows(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
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
	tsBySeries := map[common.SeriesID]int64{}
	count := 0
	for it.Next() {
		r := it.Row()
		count++
		tsBySeries[r.SeriesID] = r.Timestamp
		assert.GreaterOrEqual(t, r.Timestamp, p.partMetadata.MinTimestamp)
		assert.LessOrEqual(t, r.Timestamp, p.partMetadata.MaxTimestamp)
		assert.Nil(t, r.EntityValues, "no smeta.bin -> EntityValues nil")
		switch r.SeriesID {
		case 1:
			assert.Equal(t, uint64(11), r.ElementID)
			require.Len(t, r.Tags, 4)
			assert.Equal(t, pbv1.ValueTypeStrArr, r.TagTypes["arrTag.strArrTag"])
			assert.Equal(t, []string{"value1", "value2"}, decodeStrArr(t, r.Tags["arrTag.strArrTag"]))
			assert.Equal(t, pbv1.ValueTypeInt64Arr, r.TagTypes["arrTag.intArrTag"])
			assert.Equal(t, []int64{25, 30}, decodeInt64Arr(r.Tags["arrTag.intArrTag"]))
			assert.Equal(t, pbv1.ValueTypeStr, r.TagTypes["singleTag.strTag"])
			assert.Equal(t, "test-value", dump.DecodeTagValue(r.TagTypes["singleTag.strTag"], r.Tags["singleTag.strTag"], nil).GetStr().GetValue())
			assert.Equal(t, pbv1.ValueTypeInt64, r.TagTypes["singleTag.intTag"])
			assert.Equal(t, int64(100), dump.DecodeTagValue(r.TagTypes["singleTag.intTag"], r.Tags["singleTag.intTag"], nil).GetInt().GetValue())
		case 2:
			assert.Equal(t, uint64(21), r.ElementID)
			require.Len(t, r.Tags, 2)
			assert.Equal(t, pbv1.ValueTypeStr, r.TagTypes["singleTag.strTag1"])
			assert.Equal(t, "tag1", dump.DecodeTagValue(r.TagTypes["singleTag.strTag1"], r.Tags["singleTag.strTag1"], nil).GetStr().GetValue())
			assert.Equal(t, "tag2", dump.DecodeTagValue(r.TagTypes["singleTag.strTag2"], r.Tags["singleTag.strTag2"], nil).GetStr().GetValue())
		case 3:
			assert.Equal(t, uint64(31), r.ElementID)
			assert.Empty(t, r.Tags, "series 3 has no tag families")
		default:
			t.Fatalf("unexpected seriesID %d", r.SeriesID)
		}
	}
	require.NoError(t, it.Err())

	assert.Equal(t, int(p.partMetadata.TotalCount), count, "iterator must yield every element")
	require.Len(t, tsBySeries, 3, "all three series rows should be visited")
	assert.Equal(t, int64(1000), tsBySeries[2]-tsBySeries[1], "series 2 timestamp = base+1000")
	assert.Equal(t, int64(2000), tsBySeries[3]-tsBySeries[1], "series 3 timestamp = base+2000")
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

// decodeInt64Arr splits a stored Int64Arr tag value (consecutive 8-byte int64s).
func decodeInt64Arr(raw []byte) []int64 {
	out := []int64{}
	for i := 0; i+8 <= len(raw); i += 8 {
		out = append(out, convert.BytesToInt64(raw[i:i+8]))
	}
	return out
}

// TestStreamIteratorEntityValues verifies Row.EntityValues resolution when
// smeta.bin is present and block seriesIDs line up with hash(entity).
func TestStreamIteratorEntityValues(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	entities := []string{"service=a", "service=b", "service=c"}
	partPath, _, cleanup := storagestream.BuildEntityPartWithSeriesMeta(tmpPath, fileSystem, 0x3039, entities)
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	want := make(map[common.SeriesID]string, len(entities))
	for _, e := range entities {
		want[common.SeriesID(convert.Hash([]byte(e)))] = e
	}

	it := p.Iterator()
	defer it.Close()
	seen := map[common.SeriesID]bool{}
	for it.Next() {
		r := it.Row()
		require.NotEmpty(t, r.EntityValues, "EntityValues should be populated from smeta for series %d", r.SeriesID)
		assert.Equal(t, want[r.SeriesID], string(r.EntityValues))
		seen[r.SeriesID] = true
	}
	require.NoError(t, it.Err())
	assert.Len(t, seen, len(entities))
}

// TestStreamIteratorNoSeriesMeta verifies that, with no smeta.bin, EntityValues is nil.
func TestStreamIteratorNoSeriesMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 0x3039, storagestream.EntityDumpRows([]string{"service=a", "service=b"}))
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()
	require.Nil(t, p.SeriesMap(), "SeriesMap should be nil without smeta.bin")

	it := p.Iterator()
	defer it.Close()
	count := 0
	for it.Next() {
		assert.Nil(t, it.Row().EntityValues, "EntityValues should be nil without smeta.bin")
		count++
	}
	require.NoError(t, it.Err())
	assert.Equal(t, 2, count)
}

// TestStreamSeriesMap verifies smeta.bin is parsed into the part-level series map.
func TestStreamSeriesMap(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, cleanup := createTestStreamPartWithSeriesMetadata(tmpPath, fileSystem)
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	seriesMap := p.SeriesMap()
	require.NotNil(t, seriesMap, "series map should be parsed from smeta.bin")

	expectedSeriesID1 := common.SeriesID(convert.Hash([]byte("test=entity1")))
	expectedSeriesID2 := common.SeriesID(convert.Hash([]byte("test=entity2")))

	require.Contains(t, seriesMap, expectedSeriesID1)
	require.Contains(t, seriesMap, expectedSeriesID2)
	assert.Equal(t, "test=entity1", string(seriesMap[expectedSeriesID1]))
	assert.Equal(t, "test=entity2", string(seriesMap[expectedSeriesID2]))
}

func createTestStreamPartWithSeriesMetadata(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())

	seriesMetadataPath := filepath.Join(partPath, "smeta.bin")
	docs := index.Documents{
		{
			DocID:        1,
			EntityValues: []byte("test=entity1"),
		},
		{
			DocID:        2,
			EntityValues: []byte("test=entity2"),
		},
	}

	seriesMetadataBytes, err := docs.Marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal series metadata documents: %v", err))
	}
	fs.MustFlush(fileSystem, seriesMetadataBytes, seriesMetadataPath, storage.FilePerm)

	return partPath, cleanup
}

// TestStreamOpenPartCorruptMetadata: corrupt metadata.json -> OpenPart error.
func TestStreamOpenPartCorruptMetadata(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(partPath, "metadata.json"), []byte("{not valid json"), 0o600))
	_, err = OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.Error(t, err, "OpenPart must fail on corrupt metadata.json")
}

// TestStreamOpenPartCorruptMeta: corrupt meta.bin -> OpenPart error.
func TestStreamOpenPartCorruptMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(partPath, "meta.bin"), []byte("not-zstd-garbage-data"), 0o600))
	_, err = OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.Error(t, err, "OpenPart must fail on corrupt meta.bin")
}

// TestStreamIteratorCorruptPrimary: corrupt primary.bin -> iterator terminal error.
func TestStreamIteratorCorruptPrimary(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
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

// TestStreamDoubleClose: Close is safe to call multiple times.
func TestStreamDoubleClose(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	require.NoError(t, p.Close())
	assert.NotPanics(t, func() { _ = p.Close() }, "second Close must not panic")
}

// TestStreamIteratorCloseThenNext: Next after Close returns false, no panic.
func TestStreamIteratorCloseThenNext(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
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

// TestStreamDecodeSelfContained: DecodeTagValue must deep-copy so decoded protos
// survive buffer reuse across later Next() calls.
func TestStreamDecodeSelfContained(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagestream.BuildPartForDump(tmpPath, fileSystem, 12345, storagestream.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	require.True(t, it.Next(), "expected at least one element (series 1)")
	first := it.Row()
	require.Contains(t, first.TagTypes, "singleTag.strTag")
	savedTag := dump.DecodeTagValue(first.TagTypes["singleTag.strTag"], first.Tags["singleTag.strTag"], nil)

	for it.Next() {
		_ = it.Row()
	}
	require.NoError(t, it.Err())
	assert.Equal(t, "test-value", savedTag.GetStr().GetValue(), "DecodeTagValue must be self-contained across Next()")
}
