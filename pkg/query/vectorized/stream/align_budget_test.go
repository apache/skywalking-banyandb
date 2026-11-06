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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestAlignedMergeKeepsUncappedBudgetBehavior(t *testing.T) {
	schema := tsSchema()
	first := buildBatch(schema, []testRow{{ts: 1, elemID: 1}})
	second := buildBatch(schema, []testRow{{ts: 2, elemID: 2}})
	third := buildBatch(schema, []testRow{{ts: 3, elemID: 3}})
	lease := &mergeBudgetLease{limit: vectorized.EstimatedRetainedBytes(first) + vectorized.EstimatedRetainedBytes(second)}
	ctx := query.WithBudgetLease(context.Background(), lease)
	aligned := &AlignedStreamBatches{Schema: schema, Batches: []*vectorized.RecordBatch{first, second, third}, sourceIdx: -1}
	got, err := aligned.Merge(ctx, false, 0, 1)
	require.ErrorIs(t, err, errTestMergeBudget, "the default entry point must preserve uncapped local behavior")
	require.Nil(t, got)
	require.Zero(t, lease.Used(), "failed pipeline Close refunds its retention")
}

func TestAlignedMergeCappedUsesTheClientWindow(t *testing.T) {
	schema := tsSchema()
	var batches []*vectorized.RecordBatch
	for id := uint64(1); id <= 3; id++ {
		batches = append(batches, buildBatch(schema, []testRow{{ts: int64(id), elemID: id}}))
	}
	lease := &mergeBudgetLease{limit: 2 * vectorized.EstimatedRetainedBytes(batches[0])}
	ctx := query.WithBudgetLease(context.Background(), lease)
	aligned := &AlignedStreamBatches{Schema: schema, Batches: batches, sourceIdx: -1}
	got, err := aligned.MergeCapped(ctx, false, 0, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 1, got[0].ActiveLen())
	require.Equal(t, uint64(1), ColumnToElementID(streamElementIDs(got[0]).Data()[0]))
	require.Zero(t, lease.Used())
	require.LessOrEqual(t, lease.peak, lease.limit)
}
