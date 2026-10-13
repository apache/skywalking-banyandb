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

// SmallBatchRows is the row count below which a storage batch is copied into a
// larger owned batch instead of being passed along as it is: per-batch overhead
// (a RecordBatch, an owner set, one operator call) would otherwise dominate a
// batch of a few rows. Storage coalesces its small series windows to this
// threshold, and the scan source applies it to whatever storage hands over.
const SmallBatchRows = 256

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
// to the same series. A batch whose rows interleave series (a time-ordered merge)
// lists the end of every maximal run of one series, so a series can appear in
// several runs; SeriesIDs says which series a run is.
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

	// Selection, when non-nil, lists the row indices of the batch's columns in
	// the order the rows are to be emitted; rows not listed are not part of the
	// batch's result. nil means every row in storage order. A descending scan
	// emits one block as [n-1, ..., 0] over unreversed columns instead of copying
	// them backwards. RowCount still reports the column length: use ActiveRows for
	// the number of rows the consumer sees. Consumers must treat it as read-only.
	Selection []uint16

	// ownTimestamps and ownVersions keep the pooled buffers while Timestamps and
	// Versions are borrowed views of storage memory (see BorrowMetadata).
	ownTimestamps []int64
	ownVersions   []int64

	// Owners pins the storage memory that string, bytes and string-array cells
	// in Tags borrow without copying. Release drops the references. A consumer
	// that keeps a borrowed cell past Release must copy it first or pin the same
	// owners (vectorized.Owners.PinAll) before releasing the batch.
	Owners vectorized.Owners

	// consts is storage for the constant columns a producer puts in Tags, so a batch
	// that lends a series' entity tags allocates nothing for them; nConsts of them are
	// in use.
	consts [maxEmbeddedConsts]vectorized.ConstColumn

	nConsts int

	// SeriesKnown is set by a producer that knows exactly which series each row
	// belongs to: SeriesBoundaries is then exact, and empty means the batch is a
	// single series. Without it SeriesBoundaries says nothing.
	SeriesKnown bool

	// TimeOrder is the direction of time within each series run (a whole batch, for
	// a single series), in the order the batch's rows are read: TimeOrderAsc,
	// TimeOrderDesc, or TimeOrderUnknown when the producer does not promise one.
	TimeOrder vectorized.TimeOrder

	// metaBorrowed is set while Timestamps and Versions are borrowed views.
	metaBorrowed bool
}

// maxEmbeddedConsts is how many constant columns a MeasureBatch carries in its own
// storage; further ones come from the column pool.
const maxEmbeddedConsts = 4

// ConstFrom returns a constant column of n rows holding the value of row of src (see
// vectorized.AcquireConstFrom), in storage the batch owns when it has room. The
// caller puts it in Tags, which Release gives back; it reports false, having built
// nothing, when src cannot be constant.
func (b *MeasureBatch) ConstFrom(src vectorized.Column, row, n int) (*vectorized.ConstColumn, bool) {
	if b.nConsts < len(b.consts) {
		c := &b.consts[b.nConsts]
		if !vectorized.FillEmbeddedConstFrom(c, src, row, n) {
			return nil, false
		}
		b.nConsts++
		return c, true
	}
	return vectorized.AcquireConstFrom(src, row, n)
}

// RowCount returns the number of rows in the batch. It reads len(Timestamps);
// callers must keep the parallel column lengths in sync.
func (b *MeasureBatch) RowCount() int {
	if b == nil {
		return 0
	}
	return len(b.Timestamps)
}

// ActiveRows returns the number of rows a consumer sees: len(Selection) when a
// selection is set, RowCount otherwise.
func (b *MeasureBatch) ActiveRows() int {
	if b == nil {
		return 0
	}
	if b.Selection != nil {
		return len(b.Selection)
	}
	return len(b.Timestamps)
}

// BorrowMetadata makes Timestamps and Versions zero-copy views of timestamps and
// versions, which must have equal length and stay valid and unmodified until
// Release; the caller pins whatever owns them. The slices are capacity-limited,
// so an append reallocates instead of writing into the owner's memory. Release
// drops the views and keeps the batch's own pooled buffers.
func (b *MeasureBatch) BorrowMetadata(timestamps, versions []int64) {
	if !b.metaBorrowed {
		b.ownTimestamps = b.Timestamps[:0]
		b.ownVersions = b.Versions[:0]
		b.metaBorrowed = true
	}
	b.Timestamps = timestamps[:len(timestamps):len(timestamps)]
	b.Versions = versions[:len(versions):len(versions)]
}

// MetadataBorrowed reports whether Timestamps and Versions are borrowed views.
func (b *MeasureBatch) MetadataBorrowed() bool { return b.metaBorrowed }

// AcquireMeasureBatch returns a recycled *MeasureBatch wired to schema with
// Tag / Field column slots pre-acquired from the per-type column pool. The
// parallel metadata slices are truncated to length 0 and grown to capacity
// if needed; columns inherit whatever capacity their pooled instance last
// held (at least capacity). Callers must call Release exactly once when
// done with the batch.
func AcquireMeasureBatch(schema *vectorized.BatchSchema, capacity int) *MeasureBatch {
	return acquireMeasureBatch(schema, capacity, true, false)
}

// AcquireBorrowingMeasureBatch is AcquireMeasureBatch for a batch whose
// Timestamps and Versions will be set with BorrowMetadata: it does not size
// those two slices, only ShardIDs, SeriesIDs and the columns.
func AcquireBorrowingMeasureBatch(schema *vectorized.BatchSchema, capacity int) *MeasureBatch {
	return acquireMeasureBatch(schema, capacity, false, false)
}

// AcquireConstEntityMeasureBatch is AcquireBorrowingMeasureBatch for a batch whose
// entity tag columns are about to be replaced by constants (see ConstFrom): their
// placeholder columns are acquired empty instead of sized to capacity rows, so a pool
// miss does not allocate a column's worth of memory that is released unused.
func AcquireConstEntityMeasureBatch(schema *vectorized.BatchSchema, capacity int) *MeasureBatch {
	return acquireMeasureBatch(schema, capacity, false, true)
}

func acquireMeasureBatch(schema *vectorized.BatchSchema, capacity int, sizeMetadata, emptyEntityTags bool) *MeasureBatch {
	b := measureBatchPool.Get()
	if b == nil {
		b = &MeasureBatch{}
	}
	b.Schema = schema
	b.Selection = nil
	b.Timestamps = b.Timestamps[:0]
	b.Versions = b.Versions[:0]
	b.ShardIDs = b.ShardIDs[:0]
	b.SeriesIDs = b.SeriesIDs[:0]
	b.SeriesBoundaries = b.SeriesBoundaries[:0]
	b.SeriesKnown = false
	b.TimeOrder = vectorized.TimeOrderUnknown
	b.nConsts = 0
	if capacity > 0 {
		if sizeMetadata && cap(b.Timestamps) < capacity {
			b.Timestamps = make([]int64, 0, capacity)
		}
		if sizeMetadata && cap(b.Versions) < capacity {
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
				tagCapacity := capacity
				if emptyEntityTags && def.Entity {
					tagCapacity = 0
				}
				b.Tags = append(b.Tags, vectorized.AcquireColumn(def.Type, tagCapacity))
			case vectorized.RoleField:
				b.Fields = append(b.Fields, vectorized.AcquireColumn(def.Type, capacity))
			case vectorized.RoleTimestamp, vectorized.RoleVersion,
				vectorized.RoleSeriesID, vectorized.RoleShardID,
				vectorized.RoleElementID, vectorized.RoleOrderKey:
				// Metadata roles use the parallel slices on the batch. ElementID/OrderKey
				// are stream-only roles that never appear in a measure schema.
			}
		}
	}
	return b
}

// Release returns the batch's Tag / Field columns to the per-type pool and
// the batch itself to the MeasureBatch pool, and drops its owner references,
// after which no borrowed cell may be read. Parallel metadata slices keep
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
	b.Selection = nil
	b.SeriesKnown = false
	b.TimeOrder = vectorized.TimeOrderUnknown
	if b.metaBorrowed {
		// The views belong to the pinned owners; get the pooled buffers back.
		b.Timestamps = b.ownTimestamps[:0]
		b.Versions = b.ownVersions[:0]
		b.ownTimestamps = nil
		b.ownVersions = nil
		b.metaBorrowed = false
	}
	b.Timestamps = b.Timestamps[:0]
	b.Versions = b.Versions[:0]
	b.ShardIDs = b.ShardIDs[:0]
	b.SeriesIDs = b.SeriesIDs[:0]
	b.Tags = b.Tags[:0]
	b.Fields = b.Fields[:0]
	b.SeriesBoundaries = b.SeriesBoundaries[:0]
	b.Owners.Release()
	measureBatchPool.Put(b)
}
