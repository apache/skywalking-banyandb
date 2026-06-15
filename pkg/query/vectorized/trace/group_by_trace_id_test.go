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

package trace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// makePhase2Batch builds a Phase-2 batch with the given rows (traceID, key, span, spanID).
func makePhase2Batch(schema *vectorized.BatchSchema, rows []phase2Row) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(schema, len(rows))
	tidCol := Phase2TraceIDs(batch)
	keyCol := Phase2Keys(batch)
	spanCol := Phase2Spans(batch)
	spanIDCol := Phase2SpanIDs(batch)
	for _, row := range rows {
		tidCol.Append(row.traceID)
		keyCol.Append(row.key)
		spanCol.Append(row.span)
		spanIDCol.Append(row.spanID)
	}
	batch.Len = len(rows)
	return batch
}

type phase2Row struct {
	traceID string
	spanID  string
	span    []byte
	key     int64
}

func TestGroupByTraceID_Basic(t *testing.T) {
	schema := NewPhase2Schema(nil)
	order := []string{"trace-a", "trace-b", "trace-c"}
	op := NewGroupByTraceID(schema, order)
	ctx := context.Background()

	require.NoError(t, op.Init(ctx))

	// 6 rows interleaved across 3 traceIDs
	rows := []phase2Row{
		{traceID: "trace-a", key: 1, span: []byte("a1"), spanID: "s1"},
		{traceID: "trace-b", key: 2, span: []byte("b1"), spanID: "s2"},
		{traceID: "trace-c", key: 3, span: []byte("c1"), spanID: "s3"},
		{traceID: "trace-a", key: 1, span: []byte("a2"), spanID: "s4"},
		{traceID: "trace-b", key: 2, span: []byte("b2"), spanID: "s5"},
		{traceID: "trace-c", key: 3, span: []byte("c2"), spanID: "s6"},
	}
	batch := makePhase2Batch(schema, rows)
	require.NoError(t, op.Consume(ctx, batch))
	require.NoError(t, op.Finalize(ctx))

	// Should emit trace-a, trace-b, trace-c in order
	outA, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, outA)
	require.Equal(t, 2, outA.Len)
	require.Equal(t, []string{"trace-a", "trace-a"}, Phase2TraceIDs(outA).Data())
	require.Equal(t, [][]byte{[]byte("a1"), []byte("a2")}, Phase2Spans(outA).Data())

	outB, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, outB)
	require.Equal(t, 2, outB.Len)
	require.Equal(t, []string{"trace-b", "trace-b"}, Phase2TraceIDs(outB).Data())

	outC, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, outC)
	require.Equal(t, 2, outC.Len)
	require.Equal(t, []string{"trace-c", "trace-c"}, Phase2TraceIDs(outC).Data())

	done, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, done)

	require.NoError(t, op.Close())
}

func TestGroupByTraceID_SkipsUnknownTraceIDs(t *testing.T) {
	schema := NewPhase2Schema(nil)
	order := []string{"trace-a"}
	op := NewGroupByTraceID(schema, order)
	ctx := context.Background()

	require.NoError(t, op.Init(ctx))

	rows := []phase2Row{
		{traceID: "trace-a", key: 1, span: []byte("a1"), spanID: "s1"},
		{traceID: "trace-unknown", key: 9, span: []byte("u1"), spanID: "su"},
	}
	batch := makePhase2Batch(schema, rows)
	require.NoError(t, op.Consume(ctx, batch))
	require.NoError(t, op.Finalize(ctx))

	outA, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, outA)
	require.Equal(t, 1, outA.Len)
	require.Equal(t, []string{"trace-a"}, Phase2TraceIDs(outA).Data())

	done, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, done)

	require.NoError(t, op.Close())
}

// TestGroupByTraceID_DuplicateOrderEmitsOnce guards against the regression where
// a duplicate entry in traceIDsOrder caused GroupByTraceID to emit the same
// trace bucket twice (the fix is delete(g.buckets, traceID) after emission).
func TestGroupByTraceID_DuplicateOrderEmitsOnce(t *testing.T) {
	schema := NewPhase2Schema(nil)
	op := NewGroupByTraceID(schema, []string{"trace-a", "trace-a"})
	ctx := context.Background()

	require.NoError(t, op.Init(ctx))

	batch := makePhase2Batch(schema, []phase2Row{
		{traceID: "trace-a", key: 1, span: []byte("s1"), spanID: "id1"},
	})
	require.NoError(t, op.Consume(ctx, batch))
	require.NoError(t, op.Finalize(ctx))

	out, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, 1, out.Len)
	require.Equal(t, []string{"trace-a"}, Phase2TraceIDs(out).Data())

	done, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, done, "duplicate traceID in traceIDsOrder must not emit the bucket twice")

	require.NoError(t, op.Close())
}

func TestGroupByTraceID_SkipsEmptyGroups(t *testing.T) {
	schema := NewPhase2Schema(nil)
	// trace-b is in order but no rows will be sent for it
	order := []string{"trace-a", "trace-b", "trace-c"}
	op := NewGroupByTraceID(schema, order)
	ctx := context.Background()

	require.NoError(t, op.Init(ctx))

	rows := []phase2Row{
		{traceID: "trace-a", key: 1, span: []byte("a1"), spanID: "s1"},
		{traceID: "trace-c", key: 3, span: []byte("c1"), spanID: "s3"},
	}
	batch := makePhase2Batch(schema, rows)
	require.NoError(t, op.Consume(ctx, batch))
	require.NoError(t, op.Finalize(ctx))

	outA, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, outA)
	require.Equal(t, []string{"trace-a"}, Phase2TraceIDs(outA).Data())

	// trace-b is skipped — next is trace-c
	outC, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, outC)
	require.Equal(t, []string{"trace-c"}, Phase2TraceIDs(outC).Data())

	done, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, done)

	require.NoError(t, op.Close())
}
