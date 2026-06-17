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

func TestSortedMergeMergesAscendingAndDeduplicates(t *testing.T) {
	op := NewSortedMerge([]itersort.Iterator[*MergeItem]{
		newCountingIter([]*MergeItem{
			NewMergeItem(1, 10, 100, []byte("a")),
			NewMergeItem(3, 10, 100, []byte("c")),
		}),
		newCountingIter([]*MergeItem{
			NewMergeItem(2, 20, 200, []byte("b")),
			NewMergeItem(4, 20, 200, []byte("c")),
		}),
	}, false, 10)
	require.NoError(t, op.Init(context.Background()))
	batch, err := op.NextBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, int64ColumnData(batch, phase1ColumnKey))
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, bytesColumnData(batch, phase1ColumnPayload))
	require.NoError(t, op.Close())
}

func TestSortedMergeMergesDescending(t *testing.T) {
	op := NewSortedMerge([]itersort.Iterator[*MergeItem]{
		newCountingIter([]*MergeItem{
			NewMergeItem(3, 10, 100, []byte("c")),
			NewMergeItem(1, 10, 100, []byte("a")),
		}),
		newCountingIter([]*MergeItem{
			NewMergeItem(4, 20, 200, []byte("d")),
			NewMergeItem(2, 20, 200, []byte("b")),
		}),
	}, true, 10)
	require.NoError(t, op.Init(context.Background()))
	batch, err := op.NextBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{4, 3, 2, 1}, int64ColumnData(batch, phase1ColumnKey))
	require.NoError(t, op.Close())
}

func TestSortedMergeIsDemandBoundedAndClosesSources(t *testing.T) {
	first := newCountingIter([]*MergeItem{
		NewMergeItem(1, 10, 100, []byte("a")),
		NewMergeItem(3, 10, 100, []byte("c")),
		NewMergeItem(5, 10, 100, []byte("e")),
	})
	second := newCountingIter([]*MergeItem{
		NewMergeItem(2, 20, 200, []byte("b")),
		NewMergeItem(4, 20, 200, []byte("d")),
		NewMergeItem(6, 20, 200, []byte("f")),
	})
	op := NewSortedMerge([]itersort.Iterator[*MergeItem]{first, second}, false, 2)
	require.NoError(t, op.Init(context.Background()))

	batch, err := op.NextBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, int64ColumnData(batch, phase1ColumnKey))
	require.Less(t, first.nextCalls+second.nextCalls, 6)

	require.NoError(t, op.Close())
	require.True(t, first.closed)
	require.True(t, second.closed)
}

func TestSortedMergePropagatesIteratorError(t *testing.T) {
	iter := NewSidxResponseIterator([]*SidxRowBatch{
		{
			Keys:    []int64{1},
			Data:    [][]byte{{0xff, 'x'}},
			SIDs:    []int64{10},
			PartIDs: []int64{100},
		},
	})
	op := NewSortedMerge([]itersort.Iterator[*MergeItem]{iter}, false, 2)
	require.NoError(t, op.Init(context.Background()))
	batch, err := op.NextBatch(context.Background())
	require.Nil(t, batch)
	require.Error(t, err)
	require.NoError(t, op.Close())
}

func int64ColumnData(batch *vectorized.RecordBatch, colIdx int) []int64 {
	return batch.Columns[colIdx].(*vectorized.TypedColumn[int64]).Data()
}

func bytesColumnData(batch *vectorized.RecordBatch, colIdx int) [][]byte {
	return batch.Columns[colIdx].(*vectorized.TypedColumn[[]byte]).Data()
}

type countingIter struct {
	items     []*MergeItem
	idx       int
	nextCalls int
	closed    bool
}

func newCountingIter(items []*MergeItem) *countingIter {
	return &countingIter{items: items, idx: -1}
}

func (i *countingIter) Next() bool {
	i.nextCalls++
	nextIdx := i.idx + 1
	if nextIdx >= len(i.items) {
		return false
	}
	i.idx = nextIdx
	return true
}

func (i *countingIter) Val() *MergeItem {
	return i.items[i.idx]
}

func (i *countingIter) Close() error {
	i.closed = true
	return nil
}
