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
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	storagemeasure "github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// assertColumnarMatchesRow opens two independent readers on the same part — one
// driving the row-based Iterator, the other the columnar ColumnarIterator with
// reuse buffers (the path the migration uses) — and advances them in lockstep,
// asserting that every row decodes to identical data on both paths. Because the
// columnar block's byte slices alias the scratch decode buffer and are only valid
// until the next block boundary, each row is fully compared before either cursor
// advances. It returns the number of rows compared.
func assertColumnarMatchesRow(t *testing.T, p *PartReader) int {
	t.Helper()

	rowIt := p.Iterator()
	defer rowIt.Close()

	// SetReuseBuffers(true) is what the migration uses and what exercises the
	// scratch-buffer aliasing across primary blocks; keep it on.
	p.SetReuseBuffers(true)
	cur := p.ColumnarIterator()
	defer cur.Close()

	count := 0
	for rowIt.Next() {
		require.True(t, cur.Next(), "columnar cursor must yield a row for every row cursor row (row %d)", count)
		row := rowIt.Row()
		block := cur.Block()
		idx := cur.Index()
		require.NotNil(t, block, "columnar block must be non-nil at row %d", count)

		assert.Equal(t, row.SeriesID, block.SeriesID, "SeriesID mismatch at row %d", count)
		assert.Equal(t, row.Timestamp, block.Timestamp(idx), "Timestamp mismatch at row %d", count)
		assert.Equal(t, row.Version, block.Version(idx), "Version mismatch at row %d", count)
		assert.Equal(t, row.EntityValues, block.EntityValues, "EntityValues mismatch at row %d", count)

		assertTagsMatch(t, row.Tags, row.TagTypes, block, idx, count)
		assertFieldsMatch(t, row.Fields, row.FieldTypes, block, idx, count)

		count++
	}
	require.NoError(t, rowIt.Err(), "row iterator must not error")

	assert.False(t, cur.Next(), "columnar cursor must be exhausted when row cursor is")
	require.NoError(t, cur.Err(), "columnar cursor must not error")
	return count
}

// assertTagsMatch verifies the row's tags (raw bytes + value type) equal the
// columnar block's per-row tag columns under the same "family.tag" keys.
func assertTagsMatch(t *testing.T, tags map[string][]byte, tagTypes map[string]pbv1.ValueType,
	block *ColumnarBlock, idx, rowNum int,
) {
	t.Helper()
	assert.Equal(t, len(tags), len(block.TagCols), "tag column count mismatch at row %d", rowNum)
	for name, raw := range tags {
		col, ok := block.TagCols[name]
		require.True(t, ok, "columnar block missing tag %q at row %d", name, rowNum)
		require.Greater(t, len(col), idx, "tag %q column too short at row %d", name, rowNum)
		assert.Equal(t, raw, col[idx], "tag %q raw bytes mismatch at row %d", name, rowNum)
		assert.Equal(t, tagTypes[name], block.TagTypes[name], "tag %q value type mismatch at row %d", name, rowNum)
	}
}

// assertFieldsMatch verifies the row's fields (raw bytes + value type) equal the
// columnar block's per-row field columns under the same field-name keys.
func assertFieldsMatch(t *testing.T, fields map[string][]byte, fieldTypes map[string]pbv1.ValueType,
	block *ColumnarBlock, idx, rowNum int,
) {
	t.Helper()
	assert.Equal(t, len(fields), len(block.FieldCols), "field column count mismatch at row %d", rowNum)
	for name, raw := range fields {
		col, ok := block.FieldCols[name]
		require.True(t, ok, "columnar block missing field %q at row %d", name, rowNum)
		require.Greater(t, len(col), idx, "field %q column too short at row %d", name, rowNum)
		assert.Equal(t, raw, col[idx], "field %q raw bytes mismatch at row %d", name, rowNum)
		assert.Equal(t, fieldTypes[name], block.FieldTypes[name], "field %q value type mismatch at row %d", name, rowNum)
	}
}

// TestColumnarConsistencyStandardRows proves the columnar read path matches the
// row read path for the canonical StandardDumpRows fixture (three series with
// string/int64/string-array/int64-array tags across two families plus int64 and
// float64 fields, one series carrying no tags). Both readers are advanced in
// lockstep and every row's SeriesID, Timestamp, Version, EntityValues, tags and
// fields (raw bytes + value type) must be identical.
func TestColumnarConsistencyStandardRows(t *testing.T) {
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

	count := assertColumnarMatchesRow(t, p)
	assert.Equal(t, int(p.partMetadata.TotalCount), count, "must compare every data point in the part")
	assert.Equal(t, 3, count, "StandardDumpRows yields exactly three rows")
}

// TestColumnarConsistencyMultiPrimaryBlock proves the columnar path matches the
// row path across MULTIPLE primary blocks, which exercises the cursor's lazy
// per-primary-block parsing and scratch-buffer reuse (SetReuseBuffers(true)).
// It builds a part with many distinct series so the accumulated block metadata
// exceeds the writer's primary-block flush threshold, then asserts the part
// genuinely spans more than one primary block before comparing both paths in
// lockstep. If a single primary block held all series, the multi-block claim is
// reported via the assertion below rather than silently skipped.
func TestColumnarConsistencyMultiPrimaryBlock(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()
	rows := buildManySeriesRows(4000)
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 0x4242, rows)
	defer cleanup()

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)

	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	defer p.Close()

	require.Greater(t, len(p.primaryBlockMetadata), 1,
		"fixture must span more than one primary block to exercise scratch reuse; got %d", len(p.primaryBlockMetadata))

	count := assertColumnarMatchesRow(t, p)
	assert.Equal(t, int(p.partMetadata.TotalCount), count, "must compare every data point across all primary blocks")
	assert.Equal(t, len(rows), count, "must compare every generated row")
}

// buildManySeriesRows returns n single-data-point rows, each a distinct series
// (so each becomes its own block) carrying a string + int64 tag in one family and
// an int64 field. The large series count forces the writer to flush several
// primary blocks. SeriesIDs are assigned ascending so block ordering is stable.
func buildManySeriesRows(n int) []storagemeasure.DumpRow {
	base := time.Now().UnixNano()
	rows := make([]storagemeasure.DumpRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, storagemeasure.DumpRow{
			SeriesID:  common.SeriesID(i + 1),
			Timestamp: base + int64(i),
			Version:   int64(i + 1),
			Tags: []storagemeasure.DumpTag{
				{Family: "f", Name: "s", Value: "v" + strconv.Itoa(i)},
				{Family: "f", Name: "n", Value: int64(i)},
			},
			Fields: []storagemeasure.DumpField{{Name: "val", Value: int64(i * 10)}},
		})
	}
	return rows
}
