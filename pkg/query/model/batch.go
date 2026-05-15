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

package model

import (
	"context"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// MeasureBatchResult is the columnar counterpart to MeasureQueryResult.
// Implementations return a *MeasureBatch carrying parallel typed slices
// instead of the row-shaped *modelv1.TagValue / *modelv1.FieldValue cells.
//
// The contract is independent of MeasureQueryResult: a single underlying
// query may satisfy both interfaces (dual-emit) or only one. Callers that
// want the columnar path call PullBatch; callers that still want the row
// path call MeasureQueryResult.Pull. Release is shared semantics — the
// caller invokes it exactly once after iteration is complete; calling
// Release twice (e.g. once via each interface) is safe but unnecessary.
type MeasureBatchResult interface {
	// PullBatch returns the next batch from the underlying scan. It returns
	// (nil, nil) on EOF; (nil, err) on storage error. Once an error is
	// returned, subsequent calls must continue to return that same error
	// (sticky-error contract — matches MeasureQueryResult.Pull's
	// MeasureResult.Error semantics).
	PullBatch(ctx context.Context) (*MeasureBatch, error)

	// Release frees any resources held by the result. Idempotent.
	Release()
}

// MeasureBatch is a single columnar batch flowing out of the storage layer.
//
// Row count is the length of Timestamps. Versions, ShardIDs and SeriesIDs
// have the same length. Tags and Fields are parallel column slices whose
// indices correspond to Schema.Columns entries with RoleTag / RoleField
// respectively (the column at Tags[i] aligns with the i-th tag slot in
// Schema, in declaration order; same for Fields[i]).
//
// SeriesBoundaries records the exclusive end-of-series row indices within
// the batch. For example, a batch holding 100 rows of series A followed by
// 200 rows of series B reports SeriesBoundaries = [100, 300]. Empty (nil
// or zero-length) means "single series" — every row in the batch belongs
// to the same series.
type MeasureBatch struct {
	// Schema describes the column layout. Tags[i] / Fields[i] entries are
	// indexed via Schema.TagIndex / Schema.FieldIndex.
	Schema *vectorized.BatchSchema

	// Per-row metadata columns. Length equals the row count.
	Timestamps []int64
	Versions   []int64
	ShardIDs   []common.ShardID
	SeriesIDs  []common.SeriesID

	// Typed tag and field columns. Tags[i] corresponds to the i-th
	// RoleTag entry in Schema.Columns (in declaration order); same for
	// Fields. Each column reports the same Len() as len(Timestamps).
	Tags   []vectorized.Column
	Fields []vectorized.Column

	// SeriesBoundaries records exclusive end-of-series row indices for
	// multi-series batches. nil or empty when the batch contains a single
	// series.
	SeriesBoundaries []int
}

// RowCount returns the number of rows in the batch. It reads len(Timestamps);
// callers must keep the parallel column lengths in sync.
func (b *MeasureBatch) RowCount() int {
	if b == nil {
		return 0
	}
	return len(b.Timestamps)
}
