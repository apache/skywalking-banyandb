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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const borrowRows = 600

// borrowFixture is a cursor whose tag columns went through a real encode/decode
// round trip into the cursor's own decoder, so the decoded values alias the memory
// a query cursor owns, plus what each row should read back as.
type borrowFixture struct {
	bc     *blockCursor
	uniq   []string   // plain-encoded string tag, one distinct value per row
	dict   []string   // dictionary-encoded string tag; "" and null rows included
	dictOK []bool     // false where the dict row is null
	blob   [][]byte   // plain-encoded bytes tag
	arr    [][]string // string-array tag; nil for an empty array
}

func (f *borrowFixture) schema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_uniq", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_dict", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_blob", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_arr", Type: vectorized.ColumnTypeStrArray},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_int", Type: vectorized.ColumnTypeInt64},
	})
}

func newBorrowFixture(t *testing.T) *borrowFixture {
	t.Helper()
	f := &borrowFixture{bc: generateBlockCursor()}
	bc := f.bc
	bc.bm = blockMetadata{seriesID: common.SeriesID(9)}
	bc.shardID = 2
	bc.schemaTagTypes = map[string]pbv1.ValueType{
		"t_uniq": pbv1.ValueTypeStr, "t_dict": pbv1.ValueTypeStr, "t_blob": pbv1.ValueTypeBinaryData,
		"t_arr": pbv1.ValueTypeStrArr, "t_int": pbv1.ValueTypeInt64,
	}
	rawUniq := make([][]byte, borrowRows)
	rawDict := make([][]byte, borrowRows)
	rawBlob := make([][]byte, borrowRows)
	rawArr := make([][]byte, borrowRows)
	rawInt := make([][]byte, borrowRows)
	dictVals := []string{"alpha", "beta", "", "a-somewhat-longer-dictionary-value"}
	for i := range borrowRows {
		bc.timestamps = append(bc.timestamps, int64(i+1))
		bc.versions = append(bc.versions, 1)
		u := fmt.Sprintf("/api/v1/endpoint/%05d/%s", i, strings.Repeat("x", i%7))
		f.uniq = append(f.uniq, u)
		rawUniq[i] = []byte(u)

		switch {
		case i%11 == 0:
			f.dict = append(f.dict, "")
			f.dictOK = append(f.dictOK, false)
			rawDict[i] = nil
		default:
			d := dictVals[i%len(dictVals)]
			f.dict = append(f.dict, d)
			f.dictOK = append(f.dictOK, true)
			rawDict[i] = []byte(d)
		}

		blob := []byte(fmt.Sprintf("blob-%d-%s", i, strings.Repeat("\x01", i%5)))
		f.blob = append(f.blob, blob)
		rawBlob[i] = append([]byte(nil), blob...)

		var elems []string
		switch i % 4 {
		case 0:
			elems = []string{"one", fmt.Sprintf("two-%d", i)}
		case 1:
			elems = []string{"has|pipe", `back\slash`, "plain"}
		case 2:
			elems = nil
		case 3:
			elems = []string{""}
		}
		f.arr = append(f.arr, elems)
		var enc []byte
		for _, e := range elems {
			enc = marshalVarArray(enc, []byte(e))
		}
		if elems == nil {
			enc = []byte{}
		}
		rawArr[i] = enc
		rawInt[i] = []byte{0, 0, 0, 0, 0, 0, 0, byte(i)}
	}
	load := func(name string, vt pbv1.ValueType, values [][]byte) column {
		src := &column{name: name, valueType: vt, values: values}
		cm := &columnMetadata{}
		buf := &bytes.Buffer{}
		w := &writer{}
		w.init(buf)
		src.mustWriteTo(cm, w)
		dst := column{}
		dst.mustReadValues(&bc.columnValuesDecoder, buf, *cm, uint64(len(values)), true)
		return dst
	}
	bc.tagFamilies = []columnFamily{{name: "default", columns: []column{
		load("t_uniq", pbv1.ValueTypeStr, rawUniq),
		load("t_dict", pbv1.ValueTypeStr, rawDict),
		load("t_blob", pbv1.ValueTypeBinaryData, rawBlob),
		load("t_arr", pbv1.ValueTypeStrArr, rawArr),
		load("t_int", pbv1.ValueTypeInt64, rawInt),
	}}}
	return f
}

func (f *borrowFixture) requireBatch(t *testing.T, b *model.MeasureBatch, desc bool) {
	t.Helper()
	uniq := b.Tags[0].(*vectorized.TypedColumn[string])
	dict := b.Tags[1].(*vectorized.TypedColumn[string])
	blob := b.Tags[2].(*vectorized.TypedColumn[[]byte])
	arr := b.Tags[3].(*vectorized.TypedColumn[[]string])
	require.Equal(t, borrowRows, uniq.Len())
	for pos := range borrowRows {
		src := pos
		if desc {
			src = borrowRows - 1 - pos
		}
		require.Equal(t, f.uniq[src], uniq.Data()[pos], "uniq row %d", pos)
		require.Equal(t, !f.dictOK[src], dict.IsNull(pos), "dict null row %d", pos)
		if f.dictOK[src] {
			require.Equal(t, f.dict[src], dict.Data()[pos], "dict row %d", pos)
		}
		require.Equal(t, f.blob[src], blob.Data()[pos], "blob row %d", pos)
		require.Equal(t, f.arr[src], arr.Data()[pos], "arr row %d", pos)
	}
}

func TestBlockCursorRefcountLifecycle(t *testing.T) {
	verifyNoScanLeaks(t)
	bc := generateBlockCursor()
	require.EqualValues(t, 1, bc.refs.Load())
	bc.timestamps = append(bc.timestamps, 1, 2, 3)

	bc.Retain()
	require.EqualValues(t, 2, bc.refs.Load())
	releaseBlockCursor(bc)
	require.EqualValues(t, 1, bc.refs.Load())
	require.Len(t, bc.timestamps, 3, "a cursor with a reference left must not be reset")

	bc.Release()
	require.EqualValues(t, 0, bc.refs.Load())
	require.Empty(t, bc.timestamps, "the last release resets the cursor")

	require.Panics(t, func() { releaseBlockCursor(bc) }, "a release past the last reference is detected")
	require.EqualValues(t, 0, bc.refs.Load(), "a detected double release must not leave the count negative")
}

func TestBlockCursorPoisonsOnLastRelease(t *testing.T) {
	verifyNoScanLeaks(t)
	f := newBorrowFixture(t)
	view := f.bc.tagFamilies[0].columns[0].values[3]
	want := string(view)
	require.NotEmpty(t, want)

	f.bc.Retain()
	releaseBlockCursor(f.bc)
	require.Equal(t, want, string(view), "memory stays intact while a reference is held")

	releaseBlockCursor(f.bc)
	require.Equal(t, strings.Repeat("\xdd", len(want)), string(view),
		"memory is overwritten with the sentinel when the last reference goes")
}

func TestBorrowTagRowsMatchesCopy(t *testing.T) {
	verifyNoScanLeaks(t)
	for _, desc := range []bool{false, true} {
		t.Run(fmt.Sprintf("desc=%v", desc), func(t *testing.T) {
			f := newBorrowFixture(t)
			if desc {
				f.bc.idx = borrowRows - 1 // loadCursorsForBatch positions a descending cursor at its last row.
			}
			schema := f.schema()
			b := newMeasureBatchForSchema(schema, borrowRows)
			f.bc.copyAllToBatch(b, schema, nil, desc)
			f.requireBatch(t, b, desc)
			require.Equal(t, 1, b.Owners.Len())
			require.EqualValues(t, 2, f.bc.refs.Load(), "the batch pins the cursor once, however many columns borrow")

			// Release the query's reference: the batch keeps the cursor alive.
			releaseBlockCursor(f.bc)
			require.EqualValues(t, 1, f.bc.refs.Load())
			f.requireBatch(t, b, desc)

			// Capture header copies, then release the batch: the cursor is recycled and
			// poisoned, which a header kept past the release observes.
			uniq := b.Tags[0].(*vectorized.TypedColumn[string])
			dict := b.Tags[1].(*vectorized.TypedColumn[string])
			const pos = 3
			stale, staleDict := uniq.Data()[pos], dict.Data()[pos]
			wantStale, wantStaleDict := f.uniq[pos], f.dict[pos]
			if desc {
				wantStale, wantStaleDict = f.uniq[borrowRows-1-pos], f.dict[borrowRows-1-pos]
			}
			require.NotEmpty(t, wantStale)
			require.NotEmpty(t, wantStaleDict)
			b.Release()
			require.EqualValues(t, 0, f.bc.refs.Load())
			require.NotEqual(t, wantStale, stale, "plain-encoded cell must be poisoned after the last release")
			require.NotEqual(t, wantStaleDict, staleDict, "dictionary-encoded cell must be poisoned after the last release")
			require.Equal(t, strings.Repeat("\xdd", len(wantStale)), stale)
			require.Equal(t, strings.Repeat("\xdd", len(wantStaleDict)), staleDict)
		})
	}
}

func TestBorrowTagRowsSkippedForPassthroughAndNumeric(t *testing.T) {
	verifyNoScanLeaks(t)
	f := newBorrowFixture(t)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_uniq", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t_int", Type: vectorized.ColumnTypeInt64},
	})
	b := newMeasureBatchForSchema(schema, borrowRows)
	f.bc.copyAllToBatch(b, schema, nil, false)
	require.Zero(t, b.Owners.Len(), "passthrough and numeric columns copy, so nothing is pinned")
	require.EqualValues(t, 1, f.bc.refs.Load())
	b.Release()
	releaseBlockCursor(f.bc)
}

func TestBorrowTagRowsMergePathPinsEachCursorOnce(t *testing.T) {
	verifyNoScanLeaks(t)
	a, c := newBorrowFixture(t), newBorrowFixture(t)
	schema := a.schema()
	b := newMeasureBatchForSchema(schema, 8)
	for i := range 6 {
		src := a
		if i%2 == 1 {
			src = c
		}
		src.bc.idx = i
		src.bc.copyToBatch(b, schema, nil)
	}
	require.Equal(t, 2, b.Owners.Len())
	require.EqualValues(t, 2, a.bc.refs.Load())
	require.EqualValues(t, 2, c.bc.refs.Load())
	releaseBlockCursor(a.bc)
	releaseBlockCursor(c.bc)
	for i := range 6 {
		src := a
		if i%2 == 1 {
			src = c
		}
		require.Equal(t, src.uniq[i], b.Tags[0].(*vectorized.TypedColumn[string]).Data()[i])
		require.Equal(t, src.arr[i], b.Tags[3].(*vectorized.TypedColumn[[]string]).Data()[i])
	}
	b.Release()
	require.EqualValues(t, 0, a.bc.refs.Load())
	require.EqualValues(t, 0, c.bc.refs.Load())
}
