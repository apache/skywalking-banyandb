// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	logicalstream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	indexOrderRuleID      = 1
	indexOrderSeriesCount = 4
	indexOrderTSCount     = 12
)

// parityEntities builds the per-series entity tag-value lists the fixture writer
// keys on (entity1..entityN), matching the entity index docs writeVecFixture adds.
func parityEntities(seriesCount int) [][]*modelv1.TagValue {
	entities := make([][]*modelv1.TagValue, 0, seriesCount)
	for i := 1; i <= seriesCount; i++ {
		entities = append(entities, []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(i)}}},
		})
	}
	return entities
}

// fullProjection is the two-tag projection the fixture populates on every element.
func fullProjection() []model.TagProjection {
	return []model.TagProjection{{
		Family: "benchmark-family",
		Names:  []string{"entity-tag", "filter-tag"},
	}}
}

// runVecPipeline drives the vec scan through the FULL M4 pipeline
// (merge → distinct → limit) exactly as localIndexScan.ExecuteVectorized does at
// the data-node standalone path, then materializes []*streamv1.Element via the
// batch egress. limitRows/offset are the M4 pipeline caps; pass limitRows=0 for
// "no cap" (drain everything). This is the vec oracle used by every parity case.
func runVecPipeline(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions,
	desc bool, offset, limitRows uint32,
) []*streamv1.Element {
	t.Helper()
	src, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	require.NotNil(t, src)
	schema := src.Schema()
	// Mirror ExecuteVectorized: the merge caps at maxElementSize (in-order top-N),
	// and the trailing Limit applies the client offset/limit slice.
	mergeCap := 0
	if sqo.MaxElementSize > 0 && sqo.MaxElementSize < math.MaxInt32 {
		mergeCap = sqo.MaxElementSize
	}
	pipeline, err := vstream.BuildStreamMergePipeline(
		&testVecSource{src: src, schema: schema}, schema, desc, offset, limitRows, vstream.DefaultConfig().BatchSize, mergeCap)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	var batches []*vectorized.RecordBatch
	for {
		batch, nextErr := pipeline.Next(ctx)
		require.NoError(t, nextErr)
		if batch == nil {
			break
		}
		batches = append(batches, batch)
	}
	elems, err := BuildElementsFromBatches(batches, sqo.TagProjection)
	require.NoError(t, err)
	require.NoError(t, pipeline.Close())
	return elems
}

// runRowPath materializes the row oracle's elements: s.Query drains one Pull into
// a StreamResult, then BuildElementsFromStreamResult converts to elements. The
// caller controls MaxElementSize via sqo.
func runRowPath(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions) []*streamv1.Element {
	t.Helper()
	rowRes, err := s.Query(ctx, sqo)
	require.NoError(t, err)
	require.NotNil(t, rowRes)
	defer rowRes.Release()
	rowElements, err := logicalstream.BuildElementsFromStreamResult(ctx, rowRes, sqo.TagProjection)
	require.NoError(t, err)
	return rowElements
}

// assertSetParity asserts the two element slices are the SAME set keyed by
// elementID with identical values (proto.Equal on tags + timestamp), plus the vec
// output is monotonically ordered by timestamp per asc/desc. This is the tie-break
// safe discipline the existing parity tests use: equal-timestamp rows have no
// cross-path-stable order, so only the set (not the per-index order) is compared.
func assertSetParity(t *testing.T, want, got []*streamv1.Element, desc bool) {
	t.Helper()
	require.Equal(t, len(want), len(got), "element-set size mismatch")
	wantByID := indexByElementID(t, want)
	gotByID := indexByElementID(t, got)
	require.Equal(t, len(wantByID), len(gotByID), "unique element-id set size mismatch")
	for id, wantElem := range wantByID {
		gotElem, ok := gotByID[id]
		require.True(t, ok, "vec path missing elementID present in row path: %s", id)
		assertElementsEqual(t, wantElem, gotElem)
	}
	assertMonotonicTS(t, got, desc)
}

// assertMonotonicTS asserts the elements are monotonically ordered by timestamp
// (non-decreasing for asc, non-increasing for desc).
func assertMonotonicTS(t *testing.T, elems []*streamv1.Element, desc bool) {
	t.Helper()
	for i := 1; i < len(elems); i++ {
		prev := elems[i-1].Timestamp.AsTime().UnixNano()
		cur := elems[i].Timestamp.AsTime().UnixNano()
		if desc {
			require.GreaterOrEqual(t, prev, cur, "output not desc-ordered by ts at %d", i)
		} else {
			require.LessOrEqual(t, prev, cur, "output not asc-ordered by ts at %d", i)
		}
	}
}

// assertExactParity asserts the two element slices are identical in ORDER and
// value. Only valid when the fixture has globally unique timestamps (seriesCount=1)
// so both paths produce the same deterministic total order with no ties.
func assertExactParity(t *testing.T, want, got []*streamv1.Element) {
	t.Helper()
	require.Equal(t, len(want), len(got), "element count mismatch")
	for i := range want {
		require.Equal(t, want[i].ElementId, got[i].ElementId, "elementID order mismatch at %d", i)
		assertElementsEqual(t, want[i], got[i])
	}
}

// TestQueryVectorized_Parity_Order runs the row path (oracle) and the full M4 vec
// pipeline over the SAME fixture for the vec-eligible no-order shapes: default ts
// order asc and desc. Comparison is set-based + ts-monotonic (tie-break safe)
// because the multi-series fixture stamps duplicate timestamps.
func TestQueryVectorized_Parity_Order(t *testing.T) {
	p := parameter{
		batchCount:     1,
		timestampCount: 20,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-parity-order",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	projection := fullProjection()
	ctx := context.Background()

	cases := []struct {
		order *index.OrderBy
		name  string
	}{
		{name: "no-order-asc", order: &index.OrderBy{Sort: modelv1.Sort_SORT_ASC}},
		{name: "no-order-desc", order: &index.OrderBy{Sort: modelv1.Sort_SORT_DESC}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desc := tc.order.Sort == modelv1.Sort_SORT_DESC
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  projection,
				Order:          tc.order,
				MaxElementSize: math.MaxInt32,
			}

			rowElements := runRowPath(ctx, t, s, sqo)
			require.NotEmpty(t, rowElements, "row path produced no elements; fixture is degenerate")

			vecElements := runVecPipeline(ctx, t, s, sqo, desc, 0, uint32(len(rowElements)*4))
			assertSetParity(t, rowElements, vecElements, desc)
		})
	}
}

// TestQueryVectorized_Parity_IndexOrder runs the row index-sort oracle and the vec
// OrderKey path over a DEDICATED index-order fixture (buildIndexOrderStream) for
// order-by-indexed-tag asc + desc. The shared generateData fixture cannot serve as
// an index-sort oracle (its sortable docs use DocID=timestamp, not the element id
// the row index-sort resolves against, and set no timestampField for the sort
// query's date range) — see the writer's doc comment. Comparison is set-based (the
// order key is the tag term; ties on it are path-specific) with tag-value parity.
func TestQueryVectorized_Parity_IndexOrder(t *testing.T) {
	// The index rule the dedicated fixture stamps its sortable docs with. Its single
	// tag "filter-tag" is the order key resolveOrderTag maps to the projection cell.
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "filter-idx", Id: indexOrderRuleID},
		Tags:     []string{"filter-tag"},
	}
	s, tr := buildIndexOrderStream(t)
	entities := parityEntities(indexOrderSeriesCount)
	projection := fullProjection()
	ctx := context.Background()

	cases := []struct {
		name string
		sort modelv1.Sort
	}{
		{name: "index-order-asc", sort: modelv1.Sort_SORT_ASC},
		{name: "index-order-desc", sort: modelv1.Sort_SORT_DESC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desc := tc.sort == modelv1.Sort_SORT_DESC
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  projection,
				Order:          &index.OrderBy{Index: indexRule, Sort: tc.sort},
				MaxElementSize: math.MaxInt32,
			}
			rowElements := runRowPath(ctx, t, s, sqo)
			require.NotEmpty(t, rowElements, "row index-sort produced no elements; fixture is degenerate")
			vecElements := runVecPipeline(ctx, t, s, sqo, desc, 0, uint32(len(rowElements)*4))

			// Order key is the tag term, not ts — ties on equal tag values order
			// path-specifically, so assert set parity (values + membership) only.
			require.Equal(t, len(rowElements), len(vecElements), "element-set size mismatch")
			rowByID := indexByElementID(t, rowElements)
			vecByID := indexByElementID(t, vecElements)
			require.Equal(t, len(rowByID), len(vecByID))
			for id, wantElem := range rowByID {
				gotElem, ok := vecByID[id]
				require.True(t, ok, "vec path missing elementID present in row path: %s", id)
				assertElementsEqual(t, wantElem, gotElem)
			}
		})
	}
}

// TestQueryVectorized_Parity_Projection covers projection subsets over the SAME
// fixture: full projection, a single-tag projection, and a projection of a tag
// absent from every element (exercising the NullTagValue fill on both paths).
func TestQueryVectorized_Parity_Projection(t *testing.T) {
	p := parameter{
		batchCount:     1,
		timestampCount: 20,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-parity-projection",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()

	cases := []struct {
		name       string
		projection []model.TagProjection
	}{
		{name: "full", projection: fullProjection()},
		{name: "single-tag", projection: []model.TagProjection{{Family: "benchmark-family", Names: []string{"filter-tag"}}}},
		{
			name: "absent-tag-nulltagvalue",
			// "missing-tag" is declared in the projection but written by no element,
			// so both egresses must NullTagValue-fill its column.
			projection: []model.TagProjection{{Family: "benchmark-family", Names: []string{"entity-tag", "missing-tag"}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  tc.projection,
				Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
				MaxElementSize: math.MaxInt32,
			}
			rowElements := runRowPath(ctx, t, s, sqo)
			require.NotEmpty(t, rowElements, "row path produced no elements; fixture is degenerate")
			vecElements := runVecPipeline(ctx, t, s, sqo, false, 0, uint32(len(rowElements)*4))
			assertSetParity(t, rowElements, vecElements, false)
		})
	}
}

// TestQueryVectorized_Parity_OffsetLimit covers the offset/limit egress split over
// a single-series fixture (globally UNIQUE timestamps), enabling exact order+value
// parity (no ties). The row oracle and vec pipeline both cap the scan at
// limit+offset and slice [offset:offset+limit], exactly as the *limit plan node
// does. Cases: limit-only, offset>0+limit, limit>result (all rows), limit<result
// (top-N).
func TestQueryVectorized_Parity_OffsetLimit(t *testing.T) {
	// seriesCount=1 => one element per timestamp => globally unique, totally
	// ordered timestamps => deterministic total order on BOTH paths => exact parity.
	p := parameter{
		batchCount:     1,
		timestampCount: 30,
		seriesCount:    1,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-parity-offsetlimit",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	projection := fullProjection()
	ctx := context.Background()

	const total = 30 // timestampCount*seriesCount, all in one batch/segment.

	cases := []struct {
		name   string
		offset uint32
		limit  uint32
	}{
		{name: "limit-only", offset: 0, limit: 10},
		{name: "offset-plus-limit", offset: 5, limit: 10},
		{name: "limit-larger-than-result", offset: 0, limit: total + 20},
		{name: "limit-smaller-than-result-topN", offset: 0, limit: 3},
		{name: "offset-into-tail", offset: total - 2, limit: 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// maxElementSize = limit+offset, mirroring PushDownMaxSize at
			// stream_analyzer.go:94 (the scan-level cap the *limit node feeds down).
			maxSize := int(tc.limit) + int(tc.offset)
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  projection,
				Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
				MaxElementSize: maxSize,
			}

			rowAll := runRowPath(ctx, t, s, sqo)
			// Apply the *limit node's egress slice to the row oracle (query.go/Query
			// does not itself apply offset/limit — that is the plan node's job).
			rowSliced := sliceOffsetLimit(rowAll, tc.offset, tc.limit)

			// The vec pipeline applies offset/limit inside NewLimit at egress, so pass
			// the SAME offset/limit; no post-slice needed.
			vecElements := runVecPipeline(ctx, t, s, sqo, false, tc.offset, tc.limit)

			// Unique timestamps => exact order+value parity is provable.
			assertExactParity(t, rowSliced, vecElements)
			assertMonotonicTS(t, vecElements, false)
		})
	}
}

// sliceOffsetLimit reproduces the *limit plan node's egress slice
// (allEntities[offset:offset+limit], clamped) so the row oracle can be compared to
// the vec pipeline's built-in Limit operator.
func sliceOffsetLimit(elems []*streamv1.Element, offset, limit uint32) []*streamv1.Element {
	off := int(offset)
	lim := int(limit)
	if len(elems) <= off {
		return []*streamv1.Element{}
	}
	end := off + lim
	if end > len(elems) {
		end = len(elems)
	}
	return elems[off:end]
}

// TestQueryVectorized_Parity_Boundary covers the degenerate result shapes: empty
// (query window with no data), single-element, and a >=2-block fixture so
// cross-block ordering is exercised. Each asserts set parity + ts monotonicity.
func TestQueryVectorized_Parity_Boundary(t *testing.T) {
	t.Run("empty-result", func(t *testing.T) {
		p := parameter{
			batchCount:     1,
			timestampCount: 20,
			seriesCount:    5,
			tagCardinality: 4,
			startTimestamp: 1,
			endTimestamp:   1000,
			scenario:       "vec-parity-empty",
		}
		s, tr := buildVecTestStream(t, p)
		entities := parityEntities(p.seriesCount)
		ctx := context.Background()

		// A time range entirely BEFORE the fixture window (which starts at ~now-1h):
		// SelectSegments still returns the recent segment but the scan yields no rows
		// in [tr.Start, tr.Start] shifted 100 years back, so both paths are empty.
		emptyStart := tr.Start.Add(-100 * 365 * 24 * time.Hour)
		emptyTR := timestamp.NewInclusiveTimeRange(emptyStart, tr.Start.Add(-time.Nanosecond))
		sqo := model.StreamQueryOptions{
			Name:           "benchmark",
			TimeRange:      &emptyTR,
			Entities:       entities,
			TagProjection:  fullProjection(),
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: math.MaxInt32,
		}
		rowElements := runRowPath(ctx, t, s, sqo)
		require.Empty(t, rowElements, "expected empty row result for a no-data window")
		// Pass a real (non-zero) limit so the emptiness comes from the no-data window,
		// not from Limit(0) emitting nothing by construction (which would make this a
		// tautology). A large limit lets any wrongly-returned row through and fail.
		vecElements := runVecPipeline(ctx, t, s, sqo, false, 0, math.MaxUint32)
		require.Empty(t, vecElements, "expected empty vec result for a no-data window")
	})

	t.Run("single-element", func(t *testing.T) {
		// seriesCount=1, timestampCount=1 => exactly one element.
		p := parameter{
			batchCount:     1,
			timestampCount: 1,
			seriesCount:    1,
			tagCardinality: 4,
			startTimestamp: 1,
			endTimestamp:   1000,
			scenario:       "vec-parity-single",
		}
		s, tr := buildVecTestStream(t, p)
		entities := parityEntities(p.seriesCount)
		ctx := context.Background()
		sqo := model.StreamQueryOptions{
			Name:           "benchmark",
			TimeRange:      &tr,
			Entities:       entities,
			TagProjection:  fullProjection(),
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: math.MaxInt32,
		}
		rowElements := runRowPath(ctx, t, s, sqo)
		require.Len(t, rowElements, 1, "expected exactly one row element")
		vecElements := runVecPipeline(ctx, t, s, sqo, false, 0, uint32(len(rowElements)*4))
		assertExactParity(t, rowElements, vecElements)
	})

	t.Run("multi-block-cross-block-order", func(t *testing.T) {
		// A block splits on series-id change (part.go:204), so seriesCount=3 forces
		// >=3 physical blocks within the single part; the vec SortedMerge must order
		// across those blocks identically to the row path's cross-block merge. The
		// element count (3*50=150) stays well under one scan batch round (32 blocks)
		// so the row oracle's ONE non-empty Pull covers the COMPLETE result — the
		// documented one-Pull limitation of BuildElementsFromStreamResult. Multiple
		// series share timestamps (ties), so parity is set-based + ts-monotonic, the
		// same tie-break-safe discipline the existing parity tests use.
		p := parameter{
			batchCount:     1,
			timestampCount: 50,
			seriesCount:    3,
			tagCardinality: 4,
			startTimestamp: 1,
			endTimestamp:   1000,
			scenario:       "vec-parity-multiblock",
		}
		s, tr := buildVecTestStream(t, p)
		entities := parityEntities(p.seriesCount)
		ctx := context.Background()
		sqo := model.StreamQueryOptions{
			Name:           "benchmark",
			TimeRange:      &tr,
			Entities:       entities,
			TagProjection:  fullProjection(),
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: math.MaxInt32,
		}
		rowElements := runRowPath(ctx, t, s, sqo)
		require.NotEmpty(t, rowElements)
		vecElements := runVecPipeline(ctx, t, s, sqo, false, 0, uint32(len(rowElements)*4))
		assertSetParity(t, rowElements, vecElements, false)

		// Same fixture, DESC, to exercise the desc cross-block merge direction.
		sqoDesc := sqo
		sqoDesc.Order = &index.OrderBy{Sort: modelv1.Sort_SORT_DESC}
		rowDesc := runRowPath(ctx, t, s, sqoDesc)
		require.NotEmpty(t, rowDesc)
		vecDesc := runVecPipeline(ctx, t, s, sqoDesc, true, 0, uint32(len(rowDesc)*4))
		assertSetParity(t, rowDesc, vecDesc, true)
	})
}

// TestQueryVectorized_Parity_Fallback_RowPathCorrect asserts that a shape M6 does
// NOT vectorize (VecExecutable declines) still returns correct elements via the
// row path. A tag-filter-wrapped scan (criteria tags needing row-side re-matching)
// is the canonical non-vectorizable shape: VecExecutable returns nil for it, so
// the engine runs Query. Here we assert the row path itself is correct on the
// fixture (the fallback destination), which is the guarantee that matters.
func TestQueryVectorized_Parity_Fallback_RowPathCorrect(t *testing.T) {
	p := parameter{
		batchCount:     1,
		timestampCount: 20,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-parity-fallback",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: math.MaxInt32,
	}
	rowElements := runRowPath(ctx, t, s, sqo)
	require.NotEmpty(t, rowElements, "fallback row path produced no elements; fixture is degenerate")
	// Every element carries the full projection with non-null values (the fixture
	// writes entity-tag + filter-tag on every element), and ts is asc-ordered.
	for _, e := range rowElements {
		require.Len(t, e.TagFamilies, 1)
		require.Equal(t, "benchmark-family", e.TagFamilies[0].Name)
		require.Len(t, e.TagFamilies[0].Tags, 2)
		require.NotEmpty(t, e.ElementId)
	}
	assertMonotonicTS(t, rowElements, false)

	// The decline contract itself: VecExecutable returns nil for a plan that is not
	// the vectorizable *limit→*localIndexScan shape, so the engine runs the row path.
	require.Nil(t, logicalstream.VecExecutable(nil), "VecExecutable must decline a non-limit plan")
}

// TestQueryVectorized_Parity_LargeSingleBlock is the C1 regression: a single
// block with MORE than DefaultBatchSize (1024) rows. Stream blocks are capped by
// uncompressed BYTES (2 MiB), not row count, so a block can hold >1024 rows; the
// scan must drain a cursor across multiple batchSize batches. Before the fix the
// scan emitted only the first batchSize rows of a cursor and silently dropped the
// rest, so the vec set would be a strict subset of the row set. seriesCount=1 =>
// one series => one physical block => globally unique, totally ordered timestamps
// => exact order+value parity is provable.
func TestQueryVectorized_Parity_LargeSingleBlock(t *testing.T) {
	const rows = 1500 // > DefaultBatchSize (1024), single series => single block.
	p := parameter{
		batchCount:     1,
		timestampCount: rows,
		seriesCount:    1,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   10000,
		scenario:       "vec-parity-large-single-block",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: math.MaxInt32,
	}
	rowElements := runRowPath(ctx, t, s, sqo)
	require.Len(t, rowElements, rows, "fixture must yield every row in one Pull")
	vecElements := runVecPipeline(ctx, t, s, sqo, false, 0, uint32(rows*4))
	// No row loss: the vec path must return the SAME 1500 rows in the same order.
	assertExactParity(t, rowElements, vecElements)
}

// TestQueryVectorized_Parity_TopN_Desc is the C2 regression for descending
// top-N: ORDER BY ts DESC with MaxElementSize < total must return the NEWEST N
// rows, not the oldest N. Before the fix the scan truncated the first N rows in
// storage (ascending) order before the merge, so a desc query returned the
// OLDEST N. seriesCount=1 => unique timestamps => exact parity against the row
// oracle (which caps AFTER its in-order desc heap merge).
func TestQueryVectorized_Parity_TopN_Desc(t *testing.T) {
	const total = 100
	const topN = 10
	p := parameter{
		batchCount:     1,
		timestampCount: total,
		seriesCount:    1,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   10000,
		scenario:       "vec-parity-topn-desc",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxElementSize: topN, // per-node cap < total => in-order top-N.
	}
	rowElements := runRowPath(ctx, t, s, sqo)
	require.Len(t, rowElements, topN, "row oracle must cap at MaxElementSize (newest N)")
	// The vec merge caps at MaxElementSize (topN) in DESC sort order; no client
	// offset/limit slice (limit=topN, offset=0) so egress keeps all N.
	vecElements := runVecPipeline(ctx, t, s, sqo, true, 0, uint32(topN))
	// Newest N kept, in desc order, byte-identical to the row oracle.
	assertExactParity(t, rowElements, vecElements)
	assertMonotonicTS(t, vecElements, true)
}

// TestQueryVectorized_Parity_TopN_MultiSeries_Asc is the C2 cross-series
// regression: an ASC query over MULTIPLE series with MaxElementSize < total.
// Blocks are seriesID-major, so a pre-merge storage-order cap would keep rows
// from the first series(es) only, not the globally-oldest N across all series.
// The cap must apply AFTER the cross-series merge (in ts order). Multiple series
// share timestamps (ties), so parity is the set of kept elementIDs + ts
// monotonicity, the same tie-break-safe discipline the other multi-series cases
// use.
func TestQueryVectorized_Parity_TopN_MultiSeries_Asc(t *testing.T) {
	p := parameter{
		batchCount:     1,
		timestampCount: 40,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   10000,
		scenario:       "vec-parity-topn-multiseries",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	const topN = 30 // < 40*5 = 200 total.
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: topN,
	}
	rowElements := runRowPath(ctx, t, s, sqo)
	require.Len(t, rowElements, topN, "row oracle must cap at MaxElementSize across series")
	vecElements := runVecPipeline(ctx, t, s, sqo, false, 0, uint32(topN))
	require.Len(t, vecElements, topN, "vec must keep exactly the in-order top-N across series")
	// The kept ts window must match the row oracle's (globally-oldest N): assert the
	// max kept ts on both paths is identical, so vec did not keep later rows the
	// oracle excluded. Ties at the boundary ts can pick different elementIDs, so the
	// boundary ts (not the exact id set) is the cross-path invariant.
	require.Equal(t, maxTS(rowElements), maxTS(vecElements),
		"vec kept a different ts window than the row oracle (wrong cross-series top-N)")
	assertMonotonicTS(t, vecElements, false)
}

// maxTS returns the largest timestamp (UnixNano) across the elements.
func maxTS(elems []*streamv1.Element) int64 {
	var m int64
	for _, e := range elems {
		if ts := e.Timestamp.AsTime().UnixNano(); ts > m {
			m = ts
		}
	}
	return m
}

// buildIndexOrderStream builds a stream over a DEDICATED index-order fixture whose
// sortable index docs are shaped for BOTH oracles:
//   - the ROW index-sort (query_by_idx.go) resolves elements by the sort doc's
//     DocID == elementID and filters the sort query by the seriesIDField and the
//     timestampField date range, so each doc sets DocID=elementID and Timestamp=ts.
//   - the VEC OrderKey path keys on the "filter-tag" tag value (resolveOrderTag),
//     so the element's filter-tag value equals the sortable field's term.
//
// The shared generateData fixture cannot serve as an index-sort oracle: it stamps
// DocID=timestamp (never matching an element id) and sets no Timestamp on its docs
// (so the sort query's date range matches nothing). Hence this dedicated writer.
//
// Sort values are assigned so index order deliberately DIFFERS from ts order (the
// per-series value decreases as ts increases), proving the test exercises the
// order-key path rather than accidentally matching timestamp order.
func buildIndexOrderStream(t *testing.T) (*stream, timestamp.TimeRange) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	base := time.Now().Add(-time.Hour).Truncate(time.Hour)

	elems := &elements{}
	var sortDocs index.Documents
	var minTS, maxTS int64
	// Precompute hashed series ids the entity index derives, so the block scanner's
	// index-resolved series id matches the element rows and the sort field key.
	sidByK := make(map[int]common.SeriesID, indexOrderSeriesCount)
	var entityDocs index.Documents
	for k := 1; k <= indexOrderSeriesCount; k++ {
		entity := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(k)}}},
		}
		series := &pbv1.Series{Subject: "benchmark", EntityValues: entity}
		require.NoError(t, series.Marshal())
		sidByK[k] = series.ID
		entityDocs = append(entityDocs, index.Document{DocID: uint64(series.ID), EntityValues: series.Buffer})
	}

	for k := 1; k <= indexOrderSeriesCount; k++ {
		sid := sidByK[k]
		for j := 1; j <= indexOrderTSCount; j++ {
			ts := base.UnixNano() + int64(j)*int64(time.Second)
			if minTS == 0 || ts < minTS {
				minTS = ts
			}
			if ts > maxTS {
				maxTS = ts
			}
			elementIDStr := strconv.Itoa(k) + "-" + strconv.Itoa(j)
			elementID := convert.HashStr(elementIDStr)
			// Sort value decreases as ts increases => index order != ts order. Zero-pad
			// so lexicographic term order equals numeric order.
			sortRank := indexOrderTSCount - j
			sortValue := filterTagValuePrefix + fmt.Sprintf("%03d", sortRank)

			elems.seriesIDs = append(elems.seriesIDs, sid)
			elems.timestamps = append(elems.timestamps, ts)
			elems.elementIDs = append(elems.elementIDs, elementID)
			elems.tagFamilies = append(elems.tagFamilies, []tagValues{{
				tag: "benchmark-family",
				values: []*tagValue{
					{tag: "entity-tag", value: []byte(entityTagValuePrefix + strconv.Itoa(k)), valueType: pbv1.ValueTypeStr},
					{tag: "filter-tag", value: []byte(sortValue), valueType: pbv1.ValueTypeStr},
				},
			}})

			sortDocs = append(sortDocs, index.Document{
				DocID:     elementID,
				Timestamp: ts,
				Fields: []index.Field{
					index.NewBytesField(index.FieldKey{IndexRuleID: indexOrderRuleID, SeriesID: sid}, []byte(sortValue)),
				},
			})
		}
	}

	db := openDatabase(t, t.TempDir())
	t.Cleanup(func() { _ = db.Close() })
	seg, err := db.CreateSegmentIfNotExist(time.Unix(0, elems.timestamps[0]))
	require.NoError(t, err)
	seg.IndexDB().Insert(entityDocs)
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	tst.mustAddElements(elems)
	tst.Index().Write(sortDocs)
	seg.DecRef()

	entity := &databasev1.Entity{TagNames: []string{"entity-tag"}}
	tagFamily := &databasev1.TagFamilySpec{
		Name: "benchmark-family",
		Tags: []*databasev1.TagSpec{
			{Name: "entity-tag", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "filter-tag", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}
	schema := &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Name: "benchmark", Group: "test"},
		Entity:      entity,
		TagFamilies: []*databasev1.TagFamilySpec{tagFamily},
	}
	s := &stream{
		schema:     schema,
		l:          logger.GetLogger("test-vec-idxorder"),
		pm:         protector.Nop{},
		vectorized: vstream.DefaultConfig(),
	}
	s.vectorized.Enabled = true
	s.name, s.group = "benchmark", "test"
	var is indexSchema
	is.parse(schema)
	s.indexSchema = atomic.Value{}
	s.indexSchema.Store(is)
	s.tsdb.Store(db)
	return s, timestamp.NewInclusiveTimeRange(time.Unix(0, minTS), time.Unix(0, maxTS))
}
