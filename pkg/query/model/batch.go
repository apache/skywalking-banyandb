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
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// measureBatchPool recycles *MeasureBatch shells together with their
// parallel metadata slices (Timestamps / Versions / ShardIDs / SeriesIDs).
// Tag and Field columns come from the per-type column pool exposed by the
// vectorized package; AcquireMeasureBatch wires them in on Get and Release
// returns them on Put.
var measureBatchPool = pool.Register[*MeasureBatch]("vectorized.measure-batch")

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

// AcquireMeasureBatch returns a recycled *MeasureBatch wired to schema with
// Tag / Field column slots pre-acquired from the per-type column pool. The
// parallel metadata slices are truncated to length 0 and grown to capacity
// if needed; columns inherit whatever capacity their pooled instance last
// held (at least capacity). Callers must call Release exactly once when
// done with the batch.
func AcquireMeasureBatch(schema *vectorized.BatchSchema, capacity int) *MeasureBatch {
	b := measureBatchPool.Get()
	if b == nil {
		b = &MeasureBatch{}
	}
	b.Schema = schema
	b.Timestamps = b.Timestamps[:0]
	b.Versions = b.Versions[:0]
	b.ShardIDs = b.ShardIDs[:0]
	b.SeriesIDs = b.SeriesIDs[:0]
	b.SeriesBoundaries = b.SeriesBoundaries[:0]
	if capacity > 0 {
		if cap(b.Timestamps) < capacity {
			b.Timestamps = make([]int64, 0, capacity)
		}
		if cap(b.Versions) < capacity {
			b.Versions = make([]int64, 0, capacity)
		}
		if cap(b.ShardIDs) < capacity {
			b.ShardIDs = make([]common.ShardID, 0, capacity)
		}
		if cap(b.SeriesIDs) < capacity {
			b.SeriesIDs = make([]common.SeriesID, 0, capacity)
		}
	}
	b.Tags = b.Tags[:0]
	b.Fields = b.Fields[:0]
	if schema != nil {
		for _, def := range schema.Columns {
			switch def.Role {
			case vectorized.RoleTag:
				b.Tags = append(b.Tags, vectorized.AcquireColumn(def.Type, capacity))
			case vectorized.RoleField:
				b.Fields = append(b.Fields, vectorized.AcquireColumn(def.Type, capacity))
			case vectorized.RoleTimestamp, vectorized.RoleVersion,
				vectorized.RoleSeriesID, vectorized.RoleShardID:
				// Metadata roles use the parallel slices on the batch.
			}
		}
	}
	return b
}

// Release returns the batch's Tag / Field columns to the per-type pool and
// the batch itself to the MeasureBatch pool. Parallel metadata slices keep
// their backing arrays (truncated to length 0) so the next Acquire can
// reuse them. Calling Release on a nil receiver is a no-op; calling Release
// twice on the same batch is a use-after-free and not supported.
func (b *MeasureBatch) Release() {
	if b == nil {
		return
	}
	for _, c := range b.Tags {
		vectorized.ReleaseColumn(c)
	}
	for _, c := range b.Fields {
		vectorized.ReleaseColumn(c)
	}
	b.Schema = nil
	b.Timestamps = b.Timestamps[:0]
	b.Versions = b.Versions[:0]
	b.ShardIDs = b.ShardIDs[:0]
	b.SeriesIDs = b.SeriesIDs[:0]
	b.Tags = b.Tags[:0]
	b.Fields = b.Fields[:0]
	b.SeriesBoundaries = b.SeriesBoundaries[:0]
	measureBatchPool.Put(b)
}
