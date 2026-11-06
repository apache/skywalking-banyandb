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

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

func TestDistributedRebindPreservesColumnsAndSelection(t *testing.T) {
	one := buildNodeBatch(vstream.BuildStreamBatchSchema(nil, "", ""), []nodeRow{{ts: 1, elementID: 1}, {ts: 2, elementID: 2}})
	two := buildNodeBatch(vstream.BuildStreamBatchSchema(nil, "", ""), []nodeRow{{ts: 3, elementID: 3}})
	one.Selection = []uint16{1}
	lease := &distributedAccountingLease{}
	aligned, err := vstream.AlignStreamBatches(query.WithBudgetLease(context.Background(), lease), []*vectorized.RecordBatch{one, two}, true)
	require.NoError(t, err)
	require.Same(t, one, aligned.Batches[0])
	require.Same(t, two, aligned.Batches[1])
	require.Same(t, one.Schema, two.Schema)
	require.Empty(t, lease.charged, "schema rebinding must not charge a column copy")
	merged, err := aligned.Merge(context.Background(), false, 0, 2)
	require.NoError(t, err)
	elements, err := aligned.BuildElements(merged, nil)
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(2), hexElementID(3)}, elementIDSeq(elements))
}

func TestDistributedRebindOnlyChargesMerge(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	lease := &distributedAccountingLease{}
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "one", chunks: []any{projectedFrameChunk(t, []nodeRow{{ts: 1, elementID: 1}}, []string{"svc"})}},
		{name: "two", chunks: []any{projectedFrameChunk(t, []nodeRow{{ts: 2, elementID: 2}}, []string{"svc"})}},
	}}
	ctx := query.WithBudgetLease(context.Background(), lease)
	got, err := newBatchMergePlan(false, 0, 2).Execute(executor.WithDistributedExecutionContext(ctx, fake))
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(1), hexElementID(2)}, elementIDSeq(got))
	require.Len(t, lease.charged, 2, "only merge retention is charged")
	require.Zero(t, lease.Used(), "merge Close refunds its own charges")
}
