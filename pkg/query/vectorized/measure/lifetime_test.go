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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

const lifetimePoison = 0xDD

// bufOwner is an Owner over a byte buffer that string, bytes and string-array
// cells borrow. When the last reference goes it overwrites the buffer, which is
// what a recycled block cursor does to its decoded values, so any borrowed cell
// still read afterwards is visibly wrong.
type bufOwner struct {
	buf  []byte
	refs int
}

func newBufOwner(size int) *bufOwner { return &bufOwner{buf: make([]byte, size), refs: 1} }

func (o *bufOwner) Retain() { o.refs++ }

func (o *bufOwner) Release() {
	o.refs--
	if o.refs < 0 {
		panic("bufOwner released more times than retained")
	}
	if o.refs == 0 {
		for i := range o.buf {
			o.buf[i] = lifetimePoison
		}
	}
}

// put copies s into the buffer at *off and returns the borrowed view.
func (o *bufOwner) put(off *int, s string) []byte {
	n := copy(o.buf[*off:], s)
	view := o.buf[*off : *off+n : *off+n]
	*off += n
	return view
}

// lifetimeSchema is [timestamp, svc string tag, blob bytes tag, arr string[] tag, v int64 field].
func lifetimeSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "blob", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "arr", Type: vectorized.ColumnTypeStrArray},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

// borrowedBatch builds a RecordBatch of rows rows whose string, bytes and
// string-array cells all borrow o's buffer, pinned on the batch. The creator's own
// reference on o is dropped, so the batch's pin is the only one keeping the buffer
// alive: releasing the batch's owners poisons it.
func borrowedBatch(schema *vectorized.BatchSchema, o *bufOwner, firstRow int) *vectorized.RecordBatch {
	const rows = 6
	b := vectorized.NewRecordBatch(schema, rows)
	off := 0
	for i := range rows {
		r := firstRow + i
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(r))
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(convert.BytesToString(o.put(&off, lifetimeSvc(r%3))))
		b.Columns[2].(*vectorized.TypedColumn[[]byte]).Append(o.put(&off, lifetimeBlob(r%3)))
		arr := []string{
			convert.BytesToString(o.put(&off, "e1-"+lifetimeSvc(r%3))),
			convert.BytesToString(o.put(&off, "e2-"+lifetimeSvc(r%3))),
		}
		b.Columns[3].(*vectorized.TypedColumn[[]string]).Append(arr)
		b.Columns[4].(*vectorized.TypedColumn[int64]).Append(int64(10 + r))
	}
	b.Len = rows
	b.Owners.Pin(o)
	o.Release()
	return b
}

func lifetimeSvc(k int) string  { return fmt.Sprintf("service-%d-with-a-long-enough-name", k) }
func lifetimeBlob(k int) string { return fmt.Sprintf("blob-payload-%d", k) }

// sliceSource is a PullOperator serving a fixed batch sequence.
type sliceSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	idx     int
}

func (s *sliceSource) Init(context.Context) error            { return nil }
func (s *sliceSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *sliceSource) Close() error                          { return nil }
func (s *sliceSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++
	return b, nil
}

func drainPipeline(t *testing.T, p *vectorized.Pipeline) []*vectorized.RecordBatch {
	t.Helper()
	var out []*vectorized.RecordBatch
	for {
		b, err := p.Next(context.Background())
		require.NoError(t, err)
		if b == nil {
			return out
		}
		out = append(out, b)
	}
}

// requireLifetimeRow checks that row i of b still carries the strings of source row r.
func requireLifetimeRow(t *testing.T, b *vectorized.RecordBatch, i, r int) {
	t.Helper()
	require.Equal(t, lifetimeSvc(r%3), b.Columns[1].(*vectorized.TypedColumn[string]).Data()[i])
	require.Equal(t, lifetimeBlob(r%3), string(b.Columns[2].(*vectorized.TypedColumn[[]byte]).Data()[i]))
	require.Equal(t, []string{"e1-" + lifetimeSvc(r%3), "e2-" + lifetimeSvc(r%3)},
		b.Columns[3].(*vectorized.TypedColumn[[]string]).Data()[i])
}

// TestBreakersCloneBorrowedCellsTheyRetain feeds batches whose cells borrow a
// buffer through a real pipeline into each breaker that keeps rows across
// batches. The pipeline releases the batch's owners as soon as Consume returns,
// which poisons the buffer; the breaker's output must still be intact, so every
// retained cell had to be cloned.
func TestBreakersCloneBorrowedCellsTheyRetain(t *testing.T) {
	schema := lifetimeSchema()
	tracker := func() *vectorized.MemoryTracker { return vectorized.NewMemoryTracker(1 << 30) }
	cases := map[string]func() vectorized.BreakerOperator{
		"groupby-first": func() vectorized.BreakerOperator {
			return NewBatchGroupByFirst(schema, []int{1}, vectorized.NewBatchPool(schema, 16), 16, tracker(), 64)
		},
		"top": func() vectorized.BreakerOperator { return NewBatchTop(schema, 4, 100, true, 16) },
	}
	for name, newOp := range cases {
		t.Run(name, func(t *testing.T) {
			o1, o2 := newBufOwner(4096), newBufOwner(4096)
			src := &sliceSource{schema: schema, batches: []*vectorized.RecordBatch{
				borrowedBatch(schema, o1, 0), borrowedBatch(schema, o2, 6),
			}}
			p, err := vectorized.NewPipelineBuilder().From(src).Break(newOp()).Build()
			require.NoError(t, err)
			require.NoError(t, p.Init(context.Background()))
			defer p.Close()

			out := drainPipeline(t, p)
			require.Zero(t, o1.refs, "the pipeline releases a consumed batch's owners")
			require.Zero(t, o2.refs)
			require.EqualValues(t, lifetimePoison, o1.buf[0], "the buffers were recycled before the output was read")

			rows := 0
			for _, b := range out {
				for i := range b.Len {
					ts := int(b.Columns[0].(*vectorized.TypedColumn[int64]).Data()[i])
					requireLifetimeRow(t, b, i, ts)
					rows++
				}
			}
			require.Positive(t, rows)
		})
	}
}

func TestAggregationClonesCarriedTagsAndKeys(t *testing.T) {
	schema := lifetimeSchema()
	o1, o2 := newBufOwner(4096), newBufOwner(4096)
	src := &sliceSource{schema: schema, batches: []*vectorized.RecordBatch{
		borrowedBatch(schema, o1, 0), borrowedBatch(schema, o2, 6),
	}}
	agg := NewBatchAggregation(schema, []int{1}, []AggSpec{{Func: AggSum, InputCol: 4, Output: "v"}},
		AggModeAll, 16, vectorized.NewMemoryTracker(1<<30), 64)
	p, err := vectorized.NewPipelineBuilder().From(src).Break(agg).Build()
	require.NoError(t, err)
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	out := drainPipeline(t, p)
	require.Zero(t, o1.refs)
	require.Zero(t, o2.refs)
	require.Len(t, out, 1)
	b := out[0]
	require.Equal(t, 3, b.Len, "three distinct services")
	for i := range b.Len {
		svc := b.Columns[0].(*vectorized.TypedColumn[string]).Data()[i]
		var k int
		_, scanErr := fmt.Sscanf(svc, "service-%d-with-a-long-enough-name", &k)
		require.NoError(t, scanErr, "key %q", svc)
		require.Equal(t, lifetimeBlob(k), string(b.Columns[1].(*vectorized.TypedColumn[[]byte]).Data()[i]))
		require.Equal(t, []string{"e1-" + lifetimeSvc(k), "e2-" + lifetimeSvc(k)},
			b.Columns[2].(*vectorized.TypedColumn[[]string]).Data()[i])
	}
}

func TestTimeBucketReleasesOwnersAndKeepsKeys(t *testing.T) {
	schema := bucketTestSchema()
	o := newBufOwner(256)
	b := vectorized.NewRecordBatch(schema, 4)
	off := 0
	for i, g := range []string{"alpha-service", "beta-service", "alpha-service", "beta-service"} {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(100 + i))
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(convert.BytesToString(o.put(&off, g)))
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(int64(i + 1))
	}
	b.Len = 4
	b.Owners.Pin(o)
	o.Release()

	upstream := &fakeBucketUpstream{schema: schema, batches: []*vectorized.RecordBatch{b}}
	bt := NewBatchTimeBucket(upstream, schema, []int{1}, 0, 1000, []AggSpec{{Func: AggSum, InputCol: 2, Output: "v"}},
		AggModeAll, 8, vectorized.NewMemoryTracker(1<<30), 0, true)
	require.NoError(t, bt.Init(context.Background()))
	defer bt.Close()
	got := sumByBucketAndTag(t, drainBucket(t, bt))
	require.Zero(t, o.refs, "the time-bucket operator releases the batch's owners after consuming it")
	require.Equal(t, map[string]int64{"alpha-service|0": 4, "beta-service|0": 6}, got)
}

// TestSerializeClonesBorrowedStrings checks the response edge: protos built from a
// batch survive the batch's owners being released, which the data node does before
// the protos are marshaled.
func TestSerializeClonesBorrowedStrings(t *testing.T) {
	schema := lifetimeSchema()
	o := newBufOwner(4096)
	b := borrowedBatch(schema, o, 0)
	dps := serializeBatchToProto(b, nil)
	b.ReleaseOwners()
	require.Zero(t, o.refs)
	require.EqualValues(t, lifetimePoison, o.buf[0])
	require.Len(t, dps, 6)
	for r, idp := range dps {
		tags := idp.GetDataPoint().GetTagFamilies()[0].GetTags()
		require.Equal(t, lifetimeSvc(r%3), tags[0].GetValue().GetStr().GetValue())
		require.Equal(t, lifetimeBlob(r%3), string(tags[1].GetValue().GetBinaryData()))
		require.Equal(t, []string{"e1-" + lifetimeSvc(r%3), "e2-" + lifetimeSvc(r%3)}, tags[2].GetValue().GetStrArray().GetValue())
	}
}

// TestDrainPipelineToFrameKeepsOwnersUntilEncoded drains batches that borrow a
// buffer into a frame. The coalescing batch holds their owners until the frame is
// encoded; releasing earlier would poison cells still waiting to be written.
func TestDrainPipelineToFrameKeepsOwnersUntilEncoded(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "blob", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	mk := func(o *bufOwner, first, rows int) *vectorized.RecordBatch {
		b := vectorized.NewRecordBatch(schema, rows)
		off := 0
		for i := range rows {
			r := first + i
			b.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(r))
			b.Columns[1].(*vectorized.TypedColumn[string]).Append(convert.BytesToString(o.put(&off, lifetimeSvc(r%3))))
			b.Columns[2].(*vectorized.TypedColumn[[]byte]).Append(o.put(&off, lifetimeBlob(r%3)))
			b.Columns[3].(*vectorized.TypedColumn[int64]).Append(int64(r))
		}
		b.Len = rows
		b.Owners.Pin(o)
		o.Release()
		return b
	}
	o1, o2 := newBufOwner(2048), newBufOwner(2048)
	src := &sliceSource{schema: schema, batches: []*vectorized.RecordBatch{mk(o1, 0, 5), mk(o2, 5, 5)}}
	p, err := vectorized.NewPipelineBuilder().From(src).Build()
	require.NoError(t, err)
	require.NoError(t, p.Init(context.Background()))
	defer p.Close()

	body, err := DrainPipelineToFrame(context.Background(), p, schema)
	require.NoError(t, err)
	require.Zero(t, o1.refs, "owners are released once the frame is encoded")
	require.Zero(t, o2.refs)

	decoded, err := frame.Decode(body)
	require.NoError(t, err)
	defer decoded.ReleaseOwners()
	require.Equal(t, 10, decoded.Len)
	for r := range 10 {
		require.Equal(t, lifetimeSvc(r%3), decoded.Columns[1].(*vectorized.TypedColumn[string]).Data()[r])
		require.Equal(t, lifetimeBlob(r%3), string(decoded.Columns[2].(*vectorized.TypedColumn[[]byte]).Data()[r]))
	}
}

// ownerBatchResult serves MeasureBatches whose string cells borrow owners.
type ownerBatchResult struct {
	batches []*model.MeasureBatch
	idx     int
}

func (r *ownerBatchResult) PullBatch(context.Context) (*model.MeasureBatch, error) {
	if r.idx >= len(r.batches) {
		return nil, nil
	}
	b := r.batches[r.idx]
	r.idx++
	return b, nil
}
func (r *ownerBatchResult) Release() {}

func borrowedMeasureBatch(schema *vectorized.BatchSchema, o *bufOwner, first, rows int) *model.MeasureBatch {
	mb := model.AcquireMeasureBatch(schema, rows)
	off := 0
	for i := range rows {
		r := first + i
		mb.Timestamps = append(mb.Timestamps, int64(r))
		mb.Versions = append(mb.Versions, 1)
		mb.SeriesIDs = append(mb.SeriesIDs, common.SeriesID(1))
		mb.ShardIDs = append(mb.ShardIDs, 0)
		mb.Tags[0].(*vectorized.TypedColumn[string]).Append(convert.BytesToString(o.put(&off, lifetimeSvc(r%3))))
		mb.Fields[0].(*vectorized.TypedColumn[int64]).Append(int64(r))
	}
	mb.Owners.Pin(o)
	o.Release()
	return mb
}

// TestBatchSourceTransfersOwnersAcrossRebatching re-batches two borrowed
// MeasureBatches into RecordBatches of a different size. Every RecordBatch must
// pin the owners of the rows it copied, and an owner must stay alive until the
// last RecordBatch that holds one of its rows is released.
func TestBatchSourceTransfersOwnersAcrossRebatching(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	o1, o2 := newBufOwner(1024), newBufOwner(1024)
	br := &ownerBatchResult{batches: []*model.MeasureBatch{
		borrowedMeasureBatch(schema, o1, 0, 5), borrowedMeasureBatch(schema, o2, 5, 5),
	}}
	pool := vectorized.NewBatchPool(schema, 4)
	src := NewBatchSourceFromBatchResult(br, schema, pool, 4) // 4-row batches straddle the 5-row sources
	require.NoError(t, src.Init(context.Background()))

	var got []*vectorized.RecordBatch
	for {
		b, err := src.NextBatch(context.Background())
		require.NoError(t, err)
		if b == nil {
			break
		}
		got = append(got, b)
	}
	require.Len(t, got, 3) // 4 + 4 + 2 rows
	require.Equal(t, 1, got[0].Owners.Len())
	require.Equal(t, 2, got[1].Owners.Len(), "rows 4..7 come from both sources")
	require.Equal(t, 1, got[2].Owners.Len())
	require.Positive(t, o1.refs)
	require.Positive(t, o2.refs)

	row := 0
	for _, b := range got {
		for i := range b.Len {
			require.Equal(t, lifetimeSvc(row%3), b.Columns[4].(*vectorized.TypedColumn[string]).Data()[i], "row %d", row)
			row++
		}
	}

	pool.Put(got[0])
	require.Positive(t, o1.refs, "o1 still backs rows held by the second batch")
	pool.Put(got[1])
	require.Zero(t, o1.refs, "the last batch holding o1 rows is gone")
	require.Positive(t, o2.refs, "o2 still backs the third batch")
	require.NoError(t, src.Close())
	pool.Put(got[2])
	require.Zero(t, o2.refs)
}
