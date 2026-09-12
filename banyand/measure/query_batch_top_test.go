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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestApplyTopNOptionsOnlyForTopNSchema pins the gate that keeps an ordinary
// Top query on the batch path. PullBatch falls back to the row merge only when
// topNQueryOptions is set, and applyTopNOptions sets it for the internal
// _top_n_result measure alone. A client Top clause never reaches it: the
// analyzer routes Top through the vectorized BatchTop operator instead.
//
// Widening this gate to fire on a client Top clause would silently put ordinary
// multi-block Top queries back on the row merge, which is what this asserts against.
func TestApplyTopNOptionsOnlyForTopNSchema(t *testing.T) {
	tests := []struct {
		name        string
		measureName string
		wantSet     bool
	}{
		{name: "ordinary measure stays batch-native", measureName: "sw_metric", wantSet: false},
		{name: "empty name stays batch-native", measureName: "", wantSet: false},
		{name: "TopN pre-aggregation measure opts in", measureName: TopNSchemaName, wantSet: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result queryResult
			applyTopNOptions(model.MeasureQueryOptions{
				Name:          tt.measureName,
				Sort:          modelv1.Sort_SORT_DESC,
				Number:        10,
				TopNFieldType: 0,
			}, &result)
			if tt.wantSet {
				require.NotNil(t, result.topNQueryOptions, "the TopN pre-aggregation measure must opt into the row merge")
				return
			}
			require.Nil(t, result.topNQueryOptions,
				"measure %q must not opt into the row merge; a client Top clause goes through BatchTop", tt.measureName)
		})
	}
}

// TestPullBatchMultiBlockStaysBatchNative drives PullBatch over a query result
// spanning more than one block cursor and asserts it merges natively, returning
// every row in timestamp order without the row-path detour.
func TestPullBatchMultiBlockStaysBatchNative(t *testing.T) {
	h := setupBenchStorageHarness(t)

	var qr queryResult
	qr.ctx = context.TODO()
	// mergeBatch resolves tags from batchSchema.Columns and the block cursor; only the
	// row fallback reads tagProjection. Leaving it nil means a regression that routes
	// this query through qr.merge null-fills every tag cell, which the per-batch
	// assertion below rejects. Without this the test passes on either path.
	qr.tagProjection = nil
	qr.batchSchema = benchNativeSchema()
	qr.orderByTS = true
	qr.ascTS = true

	ti := &tstIter{}
	ti.init(h.parts, h.sids, h.queryOpts.minTimestamp, h.queryOpts.maxTimestamp)
	for ti.nextBlock() {
		bc := generateBlockCursor()
		p := ti.piHeap[0]
		opts := h.queryOpts
		opts.TagProjection = benchStorageProj
		opts.FieldProjection = benchStorageFieldProj
		bc.init(p.p, p.curBlock, opts)
		qr.data = append(qr.data, bc)
	}
	defer qr.Release()

	require.Nil(t, qr.topNQueryOptions, "an ordinary query must not carry TopN options")

	var rows int
	var lastTS int64
	var checkedMultiBlock bool
	for {
		batch, err := qr.PullBatch(context.TODO())
		require.NoError(t, err)
		if batch == nil {
			break
		}
		if !checkedMultiBlock {
			require.Greater(t, len(qr.data), 1,
				"the merge must run over more than one loaded cursor, not the single-block fast path")
			checkedMultiBlock = true
		}
		require.NotEmpty(t, batch.Timestamps, "a non-nil batch must carry rows")
		for _, ts := range batch.Timestamps {
			require.GreaterOrEqual(t, ts, lastTS, "ascending timestamp order must hold across the block merge")
			lastTS = ts
		}
		svc, ok := batch.Tags[0].(*vectorized.TypedColumn[string])
		require.True(t, ok, "the native schema must decode svc into a string column")
		for idx, value := range svc.Data() {
			require.False(t, svc.IsNull(idx), "the batch merge must fill svc from the block cursor")
			require.Equal(t, "alpha", value)
		}
		rows += batch.RowCount()
		batch.Release()
	}
	require.Equal(t, benchStorageSeries*benchStorageRowsPer, rows,
		"the native merge must return every row across all blocks")
}
