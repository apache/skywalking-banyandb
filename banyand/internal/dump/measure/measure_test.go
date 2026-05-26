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

package measure

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
	storagemeasure "github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDecodeMeasurePartFormat verifies the library can parse the latest measure
// part format. It creates a real part via the measure module's flush path and
// decodes it block by block.
func TestDecodeMeasurePartFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()

	partName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partName, 16, 64)
	require.NoError(t, err, "part directory should have valid hex name")

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "should be able to open part created by measure module")
	defer p.Close()

	assert.Equal(t, partID, p.partMetadata.ID)
	assert.Greater(t, p.partMetadata.TotalCount, uint64(0), "should have data points")
	assert.Greater(t, p.partMetadata.BlocksCount, uint64(0), "should have at least 1 block")
	assert.Greater(t, p.partMetadata.MinTimestamp, int64(0), "should have valid min timestamp")
	assert.GreaterOrEqual(t, p.partMetadata.MaxTimestamp, p.partMetadata.MinTimestamp)

	assert.Greater(t, len(p.primaryBlockMetadata), 0, "should have at least 1 primary block")

	decoder := &encoding.BytesBlockDecoder{}
	totalDataPoints := 0

	for blockIdx, pbm := range p.primaryBlockMetadata {
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		decompressed, decErr := zstd.Decompress(nil, primaryData)
		require.NoError(t, decErr, "should decompress primary data for primary block %d", blockIdx)

		blockMetadatas, parseErr := parseBlockMetadata(decompressed)
		require.NoError(t, parseErr, "should parse all block metadata from primary block %d", blockIdx)

		for _, bm := range blockMetadatas {
			timestamps, versions, tsErr := readTimestamps(bm.timestamps, int(bm.count), p.timestamps)
			require.NoError(t, tsErr, "should read timestamps/versions for series %d", bm.seriesID)
			assert.Len(t, timestamps, int(bm.count), "should have correct number of timestamps")
			assert.Len(t, versions, int(bm.count), "should have correct number of versions")

			totalDataPoints += len(timestamps)

			for _, ts := range timestamps {
				assert.GreaterOrEqual(t, ts, p.partMetadata.MinTimestamp, "timestamp should be >= min")
				assert.LessOrEqual(t, ts, p.partMetadata.MaxTimestamp, "timestamp should be <= max")
			}

			for _, colMeta := range bm.field.columns {
				fieldValues, fErr := readFieldValues(decoder, colMeta.dataBlock, colMeta.name, int(bm.count), p.fieldValues, colMeta.valueType)
				require.NoError(t, fErr, "should read field %s for series %d", colMeta.name, bm.seriesID)
				assert.Len(t, fieldValues, int(bm.count), "field %s should have value for each data point", colMeta.name)
			}

			for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
				tagFamilyMetadataData := make([]byte, tagFamilyBlock.size)
				fs.MustReadData(p.tagFamilyMetadata[tagFamilyName], int64(tagFamilyBlock.offset), tagFamilyMetadataData)

				var cfm columnFamilyMetadata
				_, cfmErr := cfm.unmarshal(tagFamilyMetadataData)
				require.NoError(t, cfmErr, "should parse tag family metadata %s for series %d", tagFamilyName, bm.seriesID)

				for _, colMeta := range cfm.columns {
					fullTagName := tagFamilyName + "." + colMeta.name
					tagValues, tErr := readTagValues(decoder, colMeta.dataBlock, fullTagName, int(bm.count), p.tagFamilies[tagFamilyName], colMeta.valueType)
					require.NoError(t, tErr, "should read tag %s for series %d", fullTagName, bm.seriesID)
					assert.Len(t, tagValues, int(bm.count), "tag %s should have value for each data point", fullTagName)
				}
			}
		}
	}

	assert.Equal(t, int(p.partMetadata.TotalCount), totalDataPoints, "should have parsed all data points from metadata")
}

// TestMeasureIteratorAllRows walks every row of the part and verifies, per series,
// each field's value and type (read == written by StandardDumpRows) plus the tags,
// timestamps and version; it also asserts the iterator yields exactly TotalCount
// rows and is exhausted afterwards.
func TestMeasureIteratorAllRows(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()

	// Row maps/bytes alias the decode buffer, so assert content inside the loop;
	// only scalar timestamps are kept to verify the relative spacing afterwards
	// (the absolute base is time.Now() and not knowable here).
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
			assert.Equal(t, int64(1), r.Version)
			require.Len(t, r.Tags, 4)
			// arrTag.strArrTag = ["value1","value2"] (StrArr)
			assert.Equal(t, pbv1.ValueTypeStrArr, r.TagTypes["arrTag.strArrTag"])
			assert.Equal(t, []string{"value1", "value2"}, decodeStrArr(t, r.Tags["arrTag.strArrTag"]))
			// arrTag.intArrTag = [25,30] (Int64Arr)
			assert.Equal(t, pbv1.ValueTypeInt64Arr, r.TagTypes["arrTag.intArrTag"])
			assert.Equal(t, []int64{25, 30}, decodeInt64Arr(r.Tags["arrTag.intArrTag"]))
			// singleTag.strTag = "test-value" (Str), singleTag.intTag = 100 (Int64)
			assert.Equal(t, pbv1.ValueTypeStr, r.TagTypes["singleTag.strTag"])
			assert.Equal(t, "test-value", dump.DecodeTagValue(r.TagTypes["singleTag.strTag"], r.Tags["singleTag.strTag"], nil).GetStr().GetValue())
			assert.Equal(t, pbv1.ValueTypeInt64, r.TagTypes["singleTag.intTag"])
			assert.Equal(t, int64(100), dump.DecodeTagValue(r.TagTypes["singleTag.intTag"], r.Tags["singleTag.intTag"], nil).GetInt().GetValue())
			// fields[0]: intField = 1000 (Int64)
			require.Len(t, r.Fields, 1)
			assert.Equal(t, pbv1.ValueTypeInt64, r.FieldTypes["intField"])
			assert.Equal(t, int64(1000), DecodeFieldValue(r.FieldTypes["intField"], r.Fields["intField"]).GetInt().GetValue())
		case 2:
			assert.Equal(t, int64(2), r.Version)
			require.Len(t, r.Tags, 2)
			assert.Equal(t, pbv1.ValueTypeStr, r.TagTypes["singleTag.strTag1"])
			assert.Equal(t, "tag1", dump.DecodeTagValue(r.TagTypes["singleTag.strTag1"], r.Tags["singleTag.strTag1"], nil).GetStr().GetValue())
			assert.Equal(t, "tag2", dump.DecodeTagValue(r.TagTypes["singleTag.strTag2"], r.Tags["singleTag.strTag2"], nil).GetStr().GetValue())
			// fields[1]: floatField = 3.14 (Float64) — only correct because FieldTypes is per-field
			require.Len(t, r.Fields, 1)
			assert.Equal(t, pbv1.ValueTypeFloat64, r.FieldTypes["floatField"])
			assert.InDelta(t, 3.14, DecodeFieldValue(r.FieldTypes["floatField"], r.Fields["floatField"]).GetFloat().GetValue(), 1e-9)
		case 3:
			assert.Equal(t, int64(3), r.Version)
			assert.Empty(t, r.Tags, "series 3 has no tag families")
			// fields[2]: intField = 2000 (Int64)
			require.Len(t, r.Fields, 1)
			assert.Equal(t, int64(2000), DecodeFieldValue(r.FieldTypes["intField"], r.Fields["intField"]).GetInt().GetValue())
		default:
			t.Fatalf("unexpected seriesID %d", r.SeriesID)
		}
	}
	require.NoError(t, it.Err())

	assert.Equal(t, int(p.partMetadata.TotalCount), count, "iterator must yield every data point")
	require.Len(t, tsBySeries, 3, "all three series rows should be visited")
	assert.Equal(t, int64(1000), tsBySeries[2]-tsBySeries[1], "series 2 timestamp = base+1000")
	assert.Equal(t, int64(2000), tsBySeries[3]-tsBySeries[1], "series 3 timestamp = base+2000")
	assert.False(t, it.Next(), "iterator stays exhausted")
}

// TestMeasureSeriesMap verifies smeta.bin is parsed into the part-level series map.
func TestMeasureSeriesMap(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	partPath, cleanup := createTestMeasurePartWithSeriesMetadata(tmpPath, fileSystem)
	defer cleanup()

	partName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partName, 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	seriesMap := p.SeriesMap()
	require.NotNil(t, seriesMap, "series map should be parsed from smeta.bin")

	expectedSeriesID1 := common.SeriesID(convert.Hash([]byte("service.name=test-service")))
	expectedSeriesID2 := common.SeriesID(convert.Hash([]byte("service.name=another-service")))

	require.Contains(t, seriesMap, expectedSeriesID1, "series map should contain first series")
	require.Contains(t, seriesMap, expectedSeriesID2, "series map should contain second series")
	assert.Equal(t, "service.name=test-service", string(seriesMap[expectedSeriesID1]))
	assert.Equal(t, "service.name=another-service", string(seriesMap[expectedSeriesID2]))
}

func createTestMeasurePartWithSeriesMetadata(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())

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

// TestMeasureOpenPartCorruptMetadata: corrupt metadata.json -> OpenPart error.
func TestMeasureOpenPartCorruptMetadata(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(partPath, "metadata.json"), []byte("{not valid json"), 0o600))
	_, err = OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.Error(t, err, "OpenPart must fail on corrupt metadata.json")
}

// TestMeasureOpenPartCorruptMeta: corrupt meta.bin (block metadata) -> OpenPart error.
func TestMeasureOpenPartCorruptMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(partPath, "meta.bin"), []byte("not-zstd-garbage-data"), 0o600))
	_, err = OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.Error(t, err, "OpenPart must fail on corrupt meta.bin")
}

// TestMeasureIteratorCorruptPrimary: corrupt primary.bin -> iterator terminal error.
func TestMeasureIteratorCorruptPrimary(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	// Overwrite primary.bin with same-length non-zstd garbage so offsets stay in range.
	primaryPath := filepath.Join(partPath, "primary.bin")
	data, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	garbage := make([]byte, len(data))
	for i := range garbage {
		garbage[i] = 0xFF
	}
	require.NoError(t, os.WriteFile(primaryPath, garbage, 0o600))

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err, "OpenPart does not decompress primary blocks, so it still succeeds")
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	assert.False(t, it.Next(), "Next must return false on corrupt primary block")
	require.Error(t, it.Err(), "Err must be set after corruption")
	assert.GreaterOrEqual(t, it.Position().BlockIdx, 0)
	assert.False(t, it.Next(), "iterator stays terminal")
}

// TestMeasureDoubleClose: Close is safe to call multiple times.
func TestMeasureDoubleClose(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	require.NoError(t, p.Close())
	assert.NotPanics(t, func() { _ = p.Close() }, "second Close must not panic")
}

// TestMeasureIteratorCloseThenNext: Next after Close returns false, no panic.
func TestMeasureIteratorCloseThenNext(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	require.NoError(t, it.Close())
	assert.False(t, it.Next(), "Next after Close must return false")
	assert.NotPanics(t, func() { _ = it.Close() }, "double iterator Close must not panic")
}

// TestMeasureDecodeSelfContained: DecodeTagValue/DecodeFieldValue must deep-copy so
// decoded protos survive buffer reuse across later Next() calls (Phase 2 batch publish).
func TestMeasureDecodeSelfContained(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, storagemeasure.StandardDumpRows())
	defer cleanup()
	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	require.True(t, it.Next(), "expected at least one row (series 1)")
	first := it.Row()
	require.Contains(t, first.TagTypes, "singleTag.strTag")
	require.Contains(t, first.FieldTypes, "intField")
	savedTag := dump.DecodeTagValue(first.TagTypes["singleTag.strTag"], first.Tags["singleTag.strTag"], nil)
	savedField := DecodeFieldValue(first.FieldTypes["intField"], first.Fields["intField"])

	// Drain the rest, reusing the block decode buffers.
	for it.Next() {
		_ = it.Row()
	}
	require.NoError(t, it.Err())

	assert.Equal(t, "test-value", savedTag.GetStr().GetValue(), "DecodeTagValue must be self-contained across Next()")
	assert.Equal(t, int64(1000), savedField.GetInt().GetValue(), "DecodeFieldValue must be self-contained across Next()")
}

// TestMeasureDecodeFieldValueBinaryDeepCopy: a binary field value must be
// deep-copied into a FieldValue_BinaryData (not aliased to the source buffer).
// DecodeTagValue's binary deep-copy is covered by the shared dump package test.
func TestMeasureDecodeFieldValueBinaryDeepCopy(t *testing.T) {
	raw := []byte{9, 8, 7}
	fv := DecodeFieldValue(pbv1.ValueTypeBinaryData, raw)
	require.NotNil(t, fv.GetBinaryData(), "binary field must decode to FieldValue_BinaryData")
	assert.Equal(t, []byte{9, 8, 7}, fv.GetBinaryData())
	raw[0] = 0xFF // mutate the source buffer
	assert.Equal(t, []byte{9, 8, 7}, fv.GetBinaryData(), "DecodeFieldValue must deep-copy binary data")
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

// TestMeasureIteratorPerFieldType is a regression test for the per-field type
// fix: a single block carrying both an int64 and a float64 field must decode each
// field with its OWN type. The pre-refactor decoder applied the first field
// column's type to every field, so a float64 field after an int64 field decoded
// as int64 (wrong value / nil GetFloat()).
func TestMeasureIteratorPerFieldType(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	rows := []storagemeasure.DumpRow{{
		SeriesID:  1,
		Timestamp: 1700000000000000000,
		Version:   1,
		Fields: []storagemeasure.DumpField{
			{Name: "intField", Value: int64(1000)},
			{Name: "floatField", Value: 3.14},
		},
	}}
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, rows)
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)
	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	it := p.Iterator()
	defer it.Close()
	require.True(t, it.Next())
	r := it.Row()

	require.Len(t, r.Fields, 2, "both fields must be present in the same block")
	// Each field keeps its own type — the bug applied columns[0]'s type to all.
	require.Equal(t, pbv1.ValueTypeInt64, r.FieldTypes["intField"])
	require.Equal(t, pbv1.ValueTypeFloat64, r.FieldTypes["floatField"])
	require.Equal(t, int64(1000), DecodeFieldValue(r.FieldTypes["intField"], r.Fields["intField"]).GetInt().GetValue())
	// Under the old bug the float field decoded with the int64 type of columns[0],
	// so GetFloat() would be nil and the value wrong.
	floatVal := DecodeFieldValue(r.FieldTypes["floatField"], r.Fields["floatField"])
	require.NotNil(t, floatVal.GetFloat(), "float field must decode as a float, not int64")
	require.InDelta(t, 3.14, floatVal.GetFloat().GetValue(), 1e-9)

	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

// TestMeasureIteratorEntityValues verifies that, when smeta.bin is present and the
// block seriesIDs line up with hash(entity), each row's EntityValues is populated
// with the correct entity. This exercises the production "Series" resolution path
// that the fixed-seriesID fixture cannot.
func TestMeasureIteratorEntityValues(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	entities := []string{"service=a", "service=b", "service=c"}
	partPath, _, cleanup := storagemeasure.BuildEntityPartWithSeriesMeta(tmpPath, fileSystem, 0x3039, entities)
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

// TestMeasureIteratorNoSeriesMeta verifies that, with no smeta.bin, EntityValues
// is nil (best-effort/optional smeta, matching the original CLI behavior).
func TestMeasureIteratorNoSeriesMeta(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	entities := []string{"service=a", "service=b"}
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 0x3039, storagemeasure.EntityDumpRows(entities))
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
	assert.Equal(t, len(entities), count)
}
