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

package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// scriptedSource serves decoded chunks and records how many were pulled.
type scriptedSource struct {
	err    error
	chunks [][]byte
	pulled int
}

func (s *scriptedSource) next(context.Context) (*vectorized.RecordBatch, error) {
	if s.pulled >= len(s.chunks) {
		return nil, s.err
	}
	body := s.chunks[s.pulled]
	s.pulled++
	return frame.Decode(body)
}

// newScriptedMerger primes every source like the plan does and builds the merge over them.
func newScriptedMerger(t *testing.T, spec distributedRowsSpec, sources ...*scriptedSource) *rowMerger {
	t.Helper()
	var inputs []rowStreamInput
	var decoded []decodedBatchSource
	for idx, src := range sources {
		first, err := src.next(t.Context())
		require.NoError(t, err)
		if first == nil {
			continue
		}
		inputs = append(inputs, rowStreamInput{src: src, first: first, source: idx})
		decoded = append(decoded, decodedBatchSource{batch: first, source: idx})
	}
	schema, err := unionDistributedRowFrameSchemas(decoded)
	require.NoError(t, err)
	merger, err := newRowMerger(t.Context(), inputs, schema, sameLayoutReshape(schema), spec)
	require.NoError(t, err)
	return merger
}

func chunk(t *testing.T, rows ...rowMergeOutput) []byte {
	t.Helper()
	return encodeDistributedRows(t, distributedRowsTestSchema(), rows...)
}

func TestRowMergerPullsNodeChunksOnlyAsFarAsTheOutputNeeds(t *testing.T) {
	pins := vectorized.OutstandingOwnerPins()
	nodeA := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 10, ver: 1, sid: 1, value: 1}, rowMergeOutput{ts: 20, ver: 1, sid: 1, value: 2}),
		chunk(t, rowMergeOutput{ts: 30, ver: 1, sid: 1, value: 3}),
		chunk(t, rowMergeOutput{ts: 40, ver: 1, sid: 1, value: 4}),
	}}
	nodeB := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 15, ver: 1, sid: 2, value: 5}),
		chunk(t, rowMergeOutput{ts: 35, ver: 1, sid: 2, value: 6}),
	}}
	merger := newScriptedMerger(t, distributedRowsSpec{BatchSize: 2}, nodeA, nodeB)
	first, err := merger.NextBatch(t.Context())
	require.NoError(t, err)
	require.NotNil(t, first)
	assertRowsEqual(t, collectDistributedRows([]*vectorized.RecordBatch{first}), []rowMergeOutput{
		{ts: 10, ver: 1, sid: 1, value: 1}, {ts: 15, ver: 1, sid: 2, value: 5},
	})
	// The merge looks one sort key ahead, so A's second chunk may be read; its third never is.
	require.Less(t, nodeA.pulled, 3, "the first output batch must not need A's last chunk")
	first.ReleaseOwners()
	var rest []*vectorized.RecordBatch
	for {
		batch, nextErr := merger.NextBatch(t.Context())
		require.NoError(t, nextErr)
		if batch == nil {
			break
		}
		rest = append(rest, batch)
	}
	assertRowsEqual(t, collectDistributedRows(rest), []rowMergeOutput{
		{ts: 20, ver: 1, sid: 1, value: 2},
		{ts: 30, ver: 1, sid: 1, value: 3},
		{ts: 35, ver: 1, sid: 2, value: 6},
		{ts: 40, ver: 1, sid: 1, value: 4},
	})
	releaseBatches(rest)
	require.NoError(t, merger.Close())
	require.Equal(t, pins, vectorized.OutstandingOwnerPins(), "every chunk must be released")
}

func TestRowMergerKeepsTheHighestVersionAcrossChunksAndNodes(t *testing.T) {
	nodeA := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 100}),
		chunk(t, rowMergeOutput{ts: 20, ver: 5, sid: 7, value: 201}, rowMergeOutput{ts: 20, ver: 1, sid: 8, value: 202}),
	}}
	nodeB := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 10, ver: 3, sid: 7, value: 300}),
		chunk(t, rowMergeOutput{ts: 20, ver: 2, sid: 7, value: 203}),
	}}
	merger := newScriptedMerger(t, distributedRowsSpec{BatchSize: 8}, nodeA, nodeB)
	batches, err := drainRowMerger(t.Context(), merger)
	require.NoError(t, err)
	t.Cleanup(func() { releaseBatches(batches) })
	got := collectDistributedRows(batches)
	require.Len(t, got, 3)
	require.Contains(t, got, rowMergeOutput{ts: 10, ver: 3, sid: 7, value: 300})
	require.Contains(t, got, rowMergeOutput{ts: 20, ver: 5, sid: 7, value: 201})
	require.Contains(t, got, rowMergeOutput{ts: 20, ver: 1, sid: 8, value: 202})
}

func TestRowMergerIndexModeSuppressesASeriesSeenInAnEarlierSortGroupAndChargesTheSeenSet(t *testing.T) {
	var charged uint64
	spec := distributedRowsSpec{
		BatchSize: 8, IndexMode: true,
		ChargeSeen: func(bytes uint64) error { charged += bytes; return nil },
	}
	nodeA := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 1}),
		chunk(t, rowMergeOutput{ts: 30, ver: 1, sid: 8, value: 3}),
	}}
	nodeB := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 20, ver: 1, sid: 7, value: 2}),
		chunk(t, rowMergeOutput{ts: 40, ver: 1, sid: 8, value: 4}),
	}}
	merger := newScriptedMerger(t, spec, nodeA, nodeB)
	batches, err := drainRowMerger(t.Context(), merger)
	require.NoError(t, err)
	t.Cleanup(func() { releaseBatches(batches) })
	assertRowsEqual(t, collectDistributedRows(batches), []rowMergeOutput{
		{ts: 10, ver: 1, sid: 7, value: 1}, {ts: 30, ver: 1, sid: 8, value: 3},
	})
	require.Equal(t, 2*seenSIDEntryBytes, charged, "each series the query emitted is charged once")
}

func TestRowMergerFailsWhenTheSeenSetExceedsTheLease(t *testing.T) {
	exhausted := errors.New("lease exhausted")
	spec := distributedRowsSpec{BatchSize: 8, IndexMode: true, ChargeSeen: func(uint64) error { return exhausted }}
	merger := newScriptedMerger(t, spec, &scriptedSource{chunks: [][]byte{chunk(t, rowMergeOutput{ts: 10, ver: 1, sid: 7, value: 1})}})
	_, err := drainRowMerger(t.Context(), merger)
	require.ErrorIs(t, err, exhausted)
}

func TestRowMergerFailedNodeFailsTheMergeAndReleasesEverything(t *testing.T) {
	pins := vectorized.OutstandingOwnerPins()
	broken := errors.New("node went away")
	nodeA := &scriptedSource{chunks: [][]byte{chunk(t, rowMergeOutput{ts: 10, ver: 1, sid: 1, value: 1})}, err: broken}
	nodeB := &scriptedSource{chunks: [][]byte{
		chunk(t, rowMergeOutput{ts: 5, ver: 1, sid: 2, value: 2}),
		chunk(t, rowMergeOutput{ts: 50, ver: 1, sid: 2, value: 3}),
	}}
	merger := newScriptedMerger(t, distributedRowsSpec{BatchSize: 1}, nodeA, nodeB)
	var err error
	for err == nil {
		var batch *vectorized.RecordBatch
		batch, err = merger.NextBatch(t.Context())
		if batch == nil && err == nil {
			t.Fatal("the merge ended cleanly although a node failed")
		}
		if batch != nil {
			batch.ReleaseOwners()
		}
	}
	require.ErrorIs(t, err, broken)
	require.NoError(t, merger.Close())
	require.Equal(t, pins, vectorized.OutstandingOwnerPins())
}

func TestRowMergerHonoursCancellation(t *testing.T) {
	pins := vectorized.OutstandingOwnerPins()
	nodeA := &scriptedSource{chunks: [][]byte{chunk(t, rowMergeOutput{ts: 10, ver: 1, sid: 1, value: 1})}}
	merger := newScriptedMerger(t, distributedRowsSpec{BatchSize: 1}, nodeA)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := merger.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, merger.Close())
	require.Equal(t, pins, vectorized.OutstandingOwnerPins())
}
