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

	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// staticPhase2Source is a minimal PullOperator emitting pre-built Phase-2 batches for tests.
type staticPhase2Source struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	pos     int
}

func (s *staticPhase2Source) Init(context.Context) error            { return nil }
func (s *staticPhase2Source) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *staticPhase2Source) Close() error                          { return nil }
func (s *staticPhase2Source) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.pos]
	s.pos++
	return b, nil
}

func TestBuildStaticPhase1CarriesOnlyLimitedRows(t *testing.T) {
	plan, err := BuildStaticPhase1([]string{"trace-a", "trace-b", "trace-c"}, map[string]int64{"trace-c": 3}, 2, 10)
	require.NoError(t, err)
	require.NoError(t, plan.Pipeline.Init(context.Background()))

	batch, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []uint16{0, 1}, batch.Selection)
	require.Equal(t, []string{"trace-a", "trace-b"}, plan.Carry.Order())
	require.Equal(t, map[int64][]string{0: {"trace-a", "trace-b"}}, plan.Carry.TraceIDsByPart())

	done, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, done)
	require.NoError(t, plan.Pipeline.Close())
}

func TestBuildMergePhase1DecodesLimitsAndCarries(t *testing.T) {
	iter := newCountingIter([]*MergeItem{
		NewMergeItem(1, 10, 100, traceIDPayload("trace-a")),
		NewMergeItem(2, 20, 200, traceIDPayload("trace-b")),
	})
	plan, err := BuildMergePhase1([]itersort.Iterator[*MergeItem]{iter}, false, 1, 10)
	require.NoError(t, err)
	require.NoError(t, plan.Pipeline.Init(context.Background()))

	batch, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []uint16{0}, batch.Selection)
	require.Equal(t, []string{"trace-a"}, plan.Carry.Order())
	require.Equal(t, map[int64][]string{100: {"trace-a"}}, plan.Carry.TraceIDsByPart())
	require.Equal(t, map[string]int64{"trace-a": 1}, plan.Carry.Keys())

	done, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, done)
	require.NoError(t, plan.Pipeline.Close())
}

func TestBuildPhase2(t *testing.T) {
	schema := NewPhase2Schema(nil)
	// Build two batches: each batch is one cursor (one traceID, two spans)
	batchA := vectorized.NewRecordBatch(schema, 2)
	Phase2TraceIDs(batchA).Append("trace-a")
	Phase2TraceIDs(batchA).Append("trace-a")
	Phase2Keys(batchA).Append(int64(10))
	Phase2Keys(batchA).Append(int64(10))
	Phase2Spans(batchA).Append([]byte("spanA1"))
	Phase2Spans(batchA).Append([]byte("spanA2"))
	Phase2SpanIDs(batchA).Append("sA1")
	Phase2SpanIDs(batchA).Append("sA2")
	batchA.Len = 2

	batchB := vectorized.NewRecordBatch(schema, 2)
	Phase2TraceIDs(batchB).Append("trace-b")
	Phase2TraceIDs(batchB).Append("trace-b")
	Phase2Keys(batchB).Append(int64(20))
	Phase2Keys(batchB).Append(int64(20))
	Phase2Spans(batchB).Append([]byte("spanB1"))
	Phase2Spans(batchB).Append([]byte("spanB2"))
	Phase2SpanIDs(batchB).Append("sB1")
	Phase2SpanIDs(batchB).Append("sB2")
	batchB.Len = 2

	source := &staticPhase2Source{
		schema:  schema,
		batches: []*vectorized.RecordBatch{batchA, batchB},
	}

	traceIDsOrder := []string{"trace-a", "trace-b"}
	plan, buildErr := BuildPhase2(source, traceIDsOrder)
	require.NoError(t, buildErr)
	require.NoError(t, plan.Pipeline.Init(context.Background()))

	outA, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, outA)
	require.Equal(t, 2, outA.Len)
	require.Equal(t, []string{"trace-a", "trace-a"}, Phase2TraceIDs(outA).Data())
	require.Equal(t, [][]byte{[]byte("spanA1"), []byte("spanA2")}, Phase2Spans(outA).Data())

	outB, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, outB)
	require.Equal(t, 2, outB.Len)
	require.Equal(t, []string{"trace-b", "trace-b"}, Phase2TraceIDs(outB).Data())
	require.Equal(t, [][]byte{[]byte("spanB1"), []byte("spanB2")}, Phase2Spans(outB).Data())

	done, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.Nil(t, done)

	require.NoError(t, plan.Pipeline.Close())
}

func TestBuildStaticPhase1ZeroLimitIsUnbounded(t *testing.T) {
	plan, err := BuildStaticPhase1([]string{"trace-a", "trace-b"}, nil, 0, 10)
	require.NoError(t, err)
	require.NoError(t, plan.Pipeline.Init(context.Background()))
	batch, err := plan.Pipeline.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, []uint16{0, 1}, batch.Selection)
	require.Equal(t, []string{"trace-a", "trace-b"}, plan.Carry.Order())
	require.NoError(t, plan.Pipeline.Close())
}
