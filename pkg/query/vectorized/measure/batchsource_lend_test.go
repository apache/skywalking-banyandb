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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type lendOwner struct{ refs int }

func (o *lendOwner) Retain()  { o.refs++ }
func (o *lendOwner) Release() { o.refs-- }

func lendSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

// nativeBatch builds a pooled MeasureBatch of n rows of series sid whose field v
// is lent from backing (zero copy) and whose timestamps start at baseTS.
func nativeBatch(schema *vectorized.BatchSchema, sid common.SeriesID, baseTS int64, n int, owner vectorized.Owner) (*model.MeasureBatch, []int64) {
	mb := model.AcquireMeasureBatch(schema, n)
	values := make([]int64, n)
	for i := range n {
		mb.Timestamps = append(mb.Timestamps, baseTS+int64(i))
		mb.Versions = append(mb.Versions, 1)
		mb.ShardIDs = append(mb.ShardIDs, 3)
		mb.SeriesIDs = append(mb.SeriesIDs, sid)
		mb.Tags[0].(*vectorized.TypedColumn[string]).Append("svc")
		values[i] = baseTS*10 + int64(i)
	}
	mb.Fields[0].(*vectorized.TypedColumn[int64]).Borrow(values)
	if owner != nil {
		mb.Owners.Pin(owner)
	}
	return mb, values
}

func fieldIdx(t *testing.T, schema *vectorized.BatchSchema, name string) int {
	t.Helper()
	i, ok := schema.FieldIndex(name)
	require.True(t, ok)
	return i
}

func drain(t *testing.T, src *BatchSourceFromBatchResult) []*vectorized.RecordBatch {
	t.Helper()
	var out []*vectorized.RecordBatch
	for {
		b, err := src.NextBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			return out
		}
		out = append(out, b)
	}
}

func TestBatchSourcePassesALargeBatchThroughWithoutCopying(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 64)
	owner := &lendOwner{}
	const rows = 1000 // above model.SmallBatchRows and far above batchSize 64
	mb, values := nativeBatch(schema, 7, 100, rows, owner)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, 64)
	defer src.Close()

	batches := drain(t, src)
	require.Len(t, batches, 1, "the batch is neither split to batchSize nor copied")
	got := batches[0]
	require.True(t, got.Lent)
	require.Equal(t, rows, got.Len)
	require.Nil(t, got.Selection)

	field := got.Columns[fieldIdx(t, schema, "v")].(*vectorized.TypedColumn[int64])
	require.Same(t, &values[0], &field.Data()[0], "the numeric field column is the storage column, not a copy")
	require.True(t, field.Borrowed())
	ts := got.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	require.Equal(t, int64(100), ts.Data()[0])
	require.Equal(t, int64(100+rows-1), ts.Data()[rows-1])
	require.Equal(t, int64(7), got.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Data()[5])
	require.Equal(t, int64(3), got.Columns[schema.ShardIDIndex()].(*vectorized.TypedColumn[int64]).Data()[5])

	require.Equal(t, 1, owner.refs, "the lent batch holds the storage owner through the holder")
	pool.Put(got)
	require.Equal(t, 0, owner.refs, "releasing the lent batch releases the storage batch and its owners")
}

func TestBatchSourceKeepsStorageAliveUntilTheLastBorrowerReleases(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 64)
	owner := &lendOwner{}
	mb, _ := nativeBatch(schema, 1, 1, 400, owner)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, 64)
	defer src.Close()
	got := drain(t, src)[0]

	var elsewhere vectorized.Owners
	got.Owners.TransferTo(&elsewhere) // e.g. DrainPipelineToFrame hands a batch's owners to its output
	pool.Put(got)
	require.Equal(t, 1, owner.refs, "the transferred owner still keeps the storage batch")
	elsewhere.Release()
	require.Equal(t, 0, owner.refs)
}

func TestBatchSourcePassesAReversedSelectionThrough(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 64)
	mb, _ := nativeBatch(schema, 1, 1, 300, nil)
	mb.Selection = make([]uint16, 300)
	for i := range mb.Selection {
		mb.Selection[i] = uint16(299 - i)
	}
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, 64)
	defer src.Close()
	batches := drain(t, src)
	require.Len(t, batches, 1)
	defer batches[0].ReleaseOwners() // the lent batch pins the owner of the storage batch
	require.Equal(t, 300, batches[0].Len)
	require.Equal(t, 300, batches[0].ActiveLen())
	require.Equal(t, uint16(299), batches[0].Selection[0])
	require.Equal(t, uint16(0), batches[0].Selection[299])
	// Columns are in storage order; only the selection reverses them.
	require.Equal(t, int64(1), batches[0].Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()[0])
}

func TestBatchSourceCoalescesSmallBatchesAndKeepsRowOrder(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 512)
	small1, _ := nativeBatch(schema, 1, 0, 100, nil)
	small2, _ := nativeBatch(schema, 2, 1000, 100, nil)
	large, _ := nativeBatch(schema, 3, 2000, model.SmallBatchRows+44, nil)
	small3, _ := nativeBatch(schema, 4, 5000, 5, nil)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{small1, small2, large, small3}}, schema, pool, 512)
	defer src.Close()

	batches := drain(t, src)
	require.Len(t, batches, 3)
	require.False(t, batches[0].Lent, "batches below the threshold are copied together")
	require.Equal(t, 200, batches[0].Len)
	ts := batches[0].Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()
	require.Equal(t, int64(0), ts[0])
	require.Equal(t, int64(1000), ts[100], "the second small batch follows the first")
	require.True(t, batches[1].Lent, "a batch at the threshold is lent")
	require.Equal(t, model.SmallBatchRows+44, batches[1].Len)
	require.Equal(t, int64(2000), batches[1].Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()[0])
	require.False(t, batches[2].Lent)
	require.Equal(t, 5, batches[2].Len)
	for _, b := range batches {
		pool.Put(b)
	}
}

func TestBatchSourceLendsASmallBatchThatCarriesASelection(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 64)
	mb, values := nativeBatch(schema, 1, 10, 4, nil)
	mb.Selection = []uint16{3, 1}
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, 64)
	defer src.Close()
	// A selection marks a batch lendable even when small; the selection is kept.
	batches := drain(t, src)
	require.Len(t, batches, 1)
	defer batches[0].ReleaseOwners() // the lent batch pins the owner of the storage batch
	require.Equal(t, []uint16{3, 1}, batches[0].Selection)
	require.Equal(t, 2, batches[0].ActiveLen())
	require.Equal(t, values[3], batches[0].Columns[fieldIdx(t, schema, "v")].(*vectorized.TypedColumn[int64]).Data()[3])
}

func TestBatchSourceDoesNotLendOversizedBatches(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 100)
	const rows = 65536 + 10
	mb, _ := nativeBatch(schema, 1, 0, rows, nil)
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, 100)
	defer src.Close()
	total := 0
	for _, b := range drain(t, src) {
		require.False(t, b.Lent)
		require.LessOrEqual(t, b.Len, 100)
		total += b.Len
	}
	require.Equal(t, rows, total)
}

func TestBatchSourceCopySelectedRowsFollowsSelectionOrder(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 16)
	mb, values := nativeBatch(schema, 5, 100, 6, nil)
	mb.Selection = []uint16{5, 3, 0}
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{}, schema, pool, 16)
	out := pool.Get()
	require.NoError(t, src.copyRowsInto(out, mb, 1, 2)) // selection positions 1 and 2: rows 3 and 0
	out.Len += 2
	ts := out.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Data()
	require.Equal(t, []int64{103, 100}, ts)
	v := out.Columns[fieldIdx(t, schema, "v")].(*vectorized.TypedColumn[int64]).Data()
	require.Equal(t, []int64{values[3], values[0]}, v)
	mb.Release()
	pool.Put(out)
}

func TestBatchSourceLendsConstantTagsAndHints(t *testing.T) {
	schema := lendSchema()
	pool := vectorized.NewBatchPool(schema, 64)
	owner := &lendOwner{}
	const rows = 600
	mb, _ := nativeBatch(schema, 7, 100, rows, owner)
	// Replace the typed tag column by the constant a scan lends for an entity tag.
	vectorized.ReleaseColumn(mb.Tags[0])
	constTag, ok := mb.ConstFrom(vectorized.Materialize(vectorized.NewConstString("svc-a", 1)), 0, rows)
	require.True(t, ok)
	mb.Tags[0] = constTag
	mb.SeriesKnown = true
	mb.TimeOrder = vectorized.TimeOrderAsc
	src := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{mb}}, schema, pool, 64)
	defer src.Close()

	batches := drain(t, src)
	require.Len(t, batches, 1)
	got := batches[0]
	tag, isConst := vectorized.AsConst(got.Columns[4])
	require.True(t, isConst, "the lent batch keeps the constant column as it is")
	require.Equal(t, "svc-a", tag.Str())
	require.Equal(t, rows, tag.Len())
	require.Equal(t, vectorized.BatchHints{SeriesKnown: true, TimeOrder: vectorized.TimeOrderAsc}, got.Hints)
	runs, known := got.SeriesRuns(nil)
	require.True(t, known)
	require.Equal(t, []int{rows}, runs)
	pool.Put(got)
	require.Equal(t, 0, owner.refs)

	// A batch the source copies (it is small) is typed and carries no hint.
	small, _ := nativeBatch(schema, 7, 100, 10, owner)
	vectorized.ReleaseColumn(small.Tags[0])
	smallTag, ok := small.ConstFrom(vectorized.Materialize(vectorized.NewConstString("svc-a", 1)), 0, 10)
	require.True(t, ok)
	small.Tags[0] = smallTag
	small.SeriesKnown = true
	small.TimeOrder = vectorized.TimeOrderAsc
	copySrc := NewBatchSourceFromBatchResult(&fakeBatchResult{seq: []*model.MeasureBatch{small}}, schema, pool, 64)
	defer copySrc.Close()
	copied := drain(t, copySrc)
	require.Len(t, copied, 1)
	require.False(t, copied[0].Lent)
	copiedTag, ok := copied[0].Columns[4].(*vectorized.TypedColumn[string])
	require.True(t, ok, "copying expands the constant into an ordinary column")
	require.Equal(t, []string{"svc-a", "svc-a", "svc-a", "svc-a", "svc-a", "svc-a", "svc-a", "svc-a", "svc-a", "svc-a"}, copiedTag.Data())
	require.Equal(t, vectorized.BatchHints{}, copied[0].Hints)
	pool.Put(copied[0])
}
