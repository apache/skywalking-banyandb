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
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
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

// buildVecTestStream constructs a stream over a real fixture part written by the
// benchmark fixture helpers (generateData / write), with the logger, protector,
// and index schema populated so both the row Query path and the vectorized scan
// path execute end-to-end.
func buildVecTestStream(t *testing.T, p parameter) (*stream, timestamp.TimeRange) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	esList, docsList, _ := generateData(p)
	// generateData stamps 1970-epoch timestamps, which a live TTL reclaims before
	// the query runs (SelectSegments returns nothing). Shift every element to a
	// recent segment and return the covering time range for the query.
	base := time.Now().Add(-time.Hour).Truncate(time.Hour)
	var minTS, maxTS int64
	for _, es := range esList {
		for i := range es.timestamps {
			es.timestamps[i] = base.UnixNano() + es.timestamps[i]
			if minTS == 0 || es.timestamps[i] < minTS {
				minTS = es.timestamps[i]
			}
			if es.timestamps[i] > maxTS {
				maxTS = es.timestamps[i]
			}
		}
	}
	tr := timestamp.NewInclusiveTimeRange(time.Unix(0, minTS), time.Unix(0, maxTS))
	db := writeVecFixture(t, p, esList, docsList)

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
		l:          logger.GetLogger("test-vec-stream"),
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
	return s, tr
}

// writeVecFixture mirrors the benchmark write helper but persists into a
// t.TempDir() that lives for the whole test, rather than a self-deleting
// test.Space. The benchmark's write() removes its temp dir on return, which is
// fine for a bench that never re-reads persisted parts but breaks a query test
// (and races the background flusher) — hence a dedicated writer here.
func writeVecFixture(t *testing.T, p parameter, esList []*elements, docsList []index.Documents) storage.TSDB[*tsTable, option] {
	db := openDatabase(t, t.TempDir())
	// Close the TSDB before t.TempDir() removal so the background flusher loop
	// stops writing into a directory that is about to be deleted.
	t.Cleanup(func() { _ = db.Close() })
	var docs index.Documents
	// sidByK maps the placeholder series id generateData stamps (common.SeriesID(k),
	// k=1..seriesCount) to the real hashed series id the entity index derives from
	// the marshaled series buffer. generateData writes the placeholder ids, which
	// the block scanner then fails to match against the index-resolved ids — so the
	// element rows must be re-stamped with the hashed ids before they are added.
	sidByK := make(map[common.SeriesID]common.SeriesID, p.seriesCount)
	for i := 1; i <= p.seriesCount; i++ {
		entity := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(i)}}},
		}
		series := &pbv1.Series{Subject: "benchmark", EntityValues: entity}
		require.NoError(t, series.Marshal())
		sidByK[common.SeriesID(i)] = series.ID
		docs = append(docs, index.Document{DocID: uint64(series.ID), EntityValues: series.Buffer})
	}
	for _, es := range esList {
		for i := range es.seriesIDs {
			if mapped, ok := sidByK[es.seriesIDs[i]]; ok {
				es.seriesIDs[i] = mapped
			}
		}
	}
	seg, err := db.CreateSegmentIfNotExist(time.Unix(0, esList[0].timestamps[0]))
	require.NoError(t, err)
	seg.IndexDB().Insert(docs)
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	for i := range esList {
		tst.mustAddElements(esList[i])
		tst.Index().Write(docsList[i])
	}
	seg.DecRef()
	return db
}

func indexByElementID(t *testing.T, elements []*streamv1.Element) map[string]*streamv1.Element {
	out := make(map[string]*streamv1.Element, len(elements))
	for _, e := range elements {
		_, exists := out[e.ElementId]
		require.False(t, exists, "unexpected duplicate elementID within a single path: %s", e.ElementId)
		out[e.ElementId] = e
	}
	return out
}

func assertElementsEqual(t *testing.T, want, got *streamv1.Element) {
	require.True(t, proto.Equal(want.Timestamp, got.Timestamp),
		"timestamp mismatch for %s: want %v got %v", want.ElementId, want.Timestamp, got.Timestamp)
	require.Equal(t, len(want.TagFamilies), len(got.TagFamilies), "tag family count mismatch for %s", want.ElementId)
	for i := range want.TagFamilies {
		wf, gf := want.TagFamilies[i], got.TagFamilies[i]
		require.Equal(t, wf.Name, gf.Name)
		require.Equal(t, len(wf.Tags), len(gf.Tags), "tag count mismatch in family %s", wf.Name)
		for j := range wf.Tags {
			require.Equal(t, wf.Tags[j].Key, gf.Tags[j].Key)
			require.True(t, proto.Equal(wf.Tags[j].Value, gf.Tags[j].Value),
				"tag value mismatch for %s.%s.%s", want.ElementId, wf.Name, wf.Tags[j].Key)
		}
	}
}

// TestQueryVectorized_NoOrder_MatchesRowPath scans a real fixture part with both
// the row path (Query -> BuildElementsFromStreamResult) and the vec path
// (queryVectorized -> BuildElementsFromVecBatches) for a no-order query with a
// two-tag projection, and asserts the produced element SETS are equal (keyed by
// elementID). Ordering/dedup across sources is M4; comparison is set-based.
func TestQueryVectorized_NoOrder_MatchesRowPath(t *testing.T) {
	// A single write batch keeps the fixture to one part / one disjoint block
	// group, so the row egress (BuildElementsFromStreamResult reads exactly one
	// non-empty Pull) and the drain-everything vec egress see the identical set.
	// Cross-part multi-Pull merge is a M4 operator concern, not an M3 scan one.
	p := parameter{
		batchCount:     1,
		timestampCount: 20,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-parity",
	}
	s, tr := buildVecTestStream(t, p)
	entities := make([][]*modelv1.TagValue, 0, p.seriesCount)
	for i := 1; i <= p.seriesCount; i++ {
		entities = append(entities, []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(i)}}},
		})
	}
	tagProjection := []model.TagProjection{{
		Family: "benchmark-family",
		Names:  []string{"entity-tag", "filter-tag"},
	}}
	// No-order query over the full fixture range (no inverted filter) so the
	// fixture yields a representative, non-degenerate set for the parity check.
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  tagProjection,
		MaxElementSize: math.MaxInt32,
	}

	ctx := context.Background()

	rowRes, err := s.Query(ctx, sqo)
	require.NoError(t, err)
	require.NotNil(t, rowRes)
	defer rowRes.Release()
	rowElements, err := logicalstream.BuildElementsFromStreamResult(ctx, rowRes, sqo.TagProjection)
	require.NoError(t, err)

	src, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	require.NotNil(t, src)
	defer src.Release()
	vecElements, err := BuildElementsFromVecBatches(ctx, src, sqo.TagProjection)
	require.NoError(t, err)

	require.NotEmpty(t, rowElements, "row path produced no elements; fixture is degenerate")

	rowByID := indexByElementID(t, rowElements)
	// The vec path does not dedup across cursors in M3; collapse to first-seen
	// per elementID so the SET comparison matches the row path's deduped set.
	vecByID := make(map[string]*streamv1.Element, len(vecElements))
	for _, e := range vecElements {
		if _, ok := vecByID[e.ElementId]; !ok {
			vecByID[e.ElementId] = e
		}
	}

	require.Equal(t, len(rowByID), len(vecByID), "element-set size mismatch between row and vec paths")
	for id, wantElem := range rowByID {
		gotElem, ok := vecByID[id]
		require.True(t, ok, "vec path missing elementID present in row path: %s", id)
		assertElementsEqual(t, wantElem, gotElem)
	}
}

// TestQueryVectorized_FullPipeline_MatchesRowPath is the M6 standalone-parity
// test: it drives the vec scan through the FULL M4 pipeline (merge → distinct →
// limit) and the batch egress (BuildElementsFromBatches), exactly as the data-node
// standalone processor path does, then asserts the produced []*streamv1.Element is
// identical (order + values) to the row path for representative queries.
func TestQueryVectorized_FullPipeline_MatchesRowPath(t *testing.T) {
	// A single write batch keeps the fixture to one part so the row egress
	// (BuildElementsFromStreamResult reads exactly one non-empty Pull) sees the
	// same complete set the drain-everything vec pipeline does. Cross-part
	// dedup/merge is covered by the M4 operator unit tests, not this end-to-end
	// engine parity check.
	p := parameter{
		batchCount:     1,
		timestampCount: 20,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-full-pipeline",
	}
	s, tr := buildVecTestStream(t, p)
	entities := make([][]*modelv1.TagValue, 0, p.seriesCount)
	for i := 1; i <= p.seriesCount; i++ {
		entities = append(entities, []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(i)}}},
		})
	}
	tagProjection := []model.TagProjection{{
		Family: "benchmark-family",
		Names:  []string{"entity-tag", "filter-tag"},
	}}

	cases := []struct {
		name string
		desc bool
	}{
		{name: "ts-order-asc", desc: false},
		{name: "ts-order-desc", desc: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			order := &index.OrderBy{Sort: modelv1.Sort_SORT_ASC}
			if tc.desc {
				order.Sort = modelv1.Sort_SORT_DESC
			}
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  tagProjection,
				Order:          order,
				MaxElementSize: math.MaxInt32,
			}

			ctx := context.Background()

			rowRes, err := s.Query(ctx, sqo)
			require.NoError(t, err)
			require.NotNil(t, rowRes)
			defer rowRes.Release()
			rowElements, err := logicalstream.BuildElementsFromStreamResult(ctx, rowRes, sqo.TagProjection)
			require.NoError(t, err)
			require.NotEmpty(t, rowElements, "row path produced no elements; fixture is degenerate")

			src, err := s.queryVectorized(ctx, sqo)
			require.NoError(t, err)
			require.NotNil(t, src)
			schema := src.Schema()
			pipeline, err := vstream.BuildStreamMergePipeline(
				&testVecSource{src: src, schema: schema}, schema, tc.desc, 0, uint32(len(rowElements)*4), vstream.DefaultConfig().BatchSize, 0)
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
			vecElements, err := BuildElementsFromBatches(batches, sqo.TagProjection)
			require.NoError(t, err)
			require.NoError(t, pipeline.Close())

			// Equal-timestamp rows have no cross-path-stable tie-break (the row
			// ts-order and the vec SortedMerge tie differently), so parity is
			// asserted as: (1) identical element SET keyed by elementID with
			// identical values, and (2) the vec output is monotonically ordered by
			// timestamp per asc/desc. This is the same set-based discipline the
			// no-order parity test uses.
			require.Equal(t, len(rowElements), len(vecElements), "element count mismatch")
			rowByID := indexByElementID(t, rowElements)
			vecByID := indexByElementID(t, vecElements)
			require.Equal(t, len(rowByID), len(vecByID), "element-set size mismatch")
			for id, wantElem := range rowByID {
				gotElem, ok := vecByID[id]
				require.True(t, ok, "vec path missing elementID present in row path: %s", id)
				assertElementsEqual(t, wantElem, gotElem)
			}
			for i := 1; i < len(vecElements); i++ {
				prev := vecElements[i-1].Timestamp.AsTime().UnixNano()
				cur := vecElements[i].Timestamp.AsTime().UnixNano()
				if tc.desc {
					require.GreaterOrEqual(t, prev, cur, "vec output not desc-ordered by ts at %d", i)
				} else {
					require.LessOrEqual(t, prev, cur, "vec output not asc-ordered by ts at %d", i)
				}
			}
		})
	}
}

// testVecSource adapts a vecScanSource to a vectorized.PullOperator so the test
// can drive the M4 pipeline directly, mirroring the production vecSourceOperator.
type testVecSource struct {
	src    vecScanSource
	schema *vectorized.BatchSchema
}

func (o *testVecSource) Init(context.Context) error            { return nil }
func (o *testVecSource) OutputSchema() *vectorized.BatchSchema { return o.schema }
func (o *testVecSource) Close() error                          { o.src.Release(); return nil }
func (o *testVecSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	return o.src.NextBatch(ctx)
}

// TestBuildElementsFromVecBatches_EmptySource verifies the egress helper returns
// no elements and no error for an exhausted source.
func TestBuildElementsFromVecBatches_EmptySource(t *testing.T) {
	elements, err := BuildElementsFromVecBatches(context.Background(), emptyVecScan{},
		[]model.TagProjection{{Family: "f", Names: []string{"a"}}})
	require.NoError(t, err)
	require.Empty(t, elements)
}

// TestQueryVectorized_MemoryBudget verifies the QueryMemoryMiB soft byte budget
// FAILS LOUD: a tiny budget processes the first block (first-block exception) but
// then returns ErrQueryMemoryBudgetExceeded when a later block would exceed the
// remaining budget, rather than silently truncating. This preserves vec==row
// parity (the row path errors under memory pressure via the protector). The
// multi-series fixture yields multiple blocks so the over-budget block is reached.
func TestQueryVectorized_MemoryBudget(t *testing.T) {
	p := parameter{
		batchCount:     1,
		timestampCount: 20,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   1000,
		scenario:       "vec-budget",
	}
	s, tr := buildVecTestStream(t, p)
	entities := make([][]*modelv1.TagValue, 0, p.seriesCount)
	for i := 1; i <= p.seriesCount; i++ {
		entities = append(entities, []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(i)}}},
		})
	}
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  []model.TagProjection{{Family: "benchmark-family", Names: []string{"entity-tag"}}},
		MaxElementSize: math.MaxInt32,
	}
	ctx := context.Background()

	// Unbounded scan (default 256 MiB budget): count every row.
	fullSrc, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	fullElements, err := BuildElementsFromVecBatches(ctx, fullSrc, sqo.TagProjection)
	require.NoError(t, err)
	require.Greater(t, len(fullElements), 1, "fixture must yield multiple blocks for the budget test")

	// Budget scan: force a 1-byte budget on the source so the first block decodes
	// (first-block exception) but the next over-budget block fails the query loud.
	budgetSrc, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	vs, ok := budgetSrc.(*streamVecScan)
	require.True(t, ok, "expected *streamVecScan")
	vs.mem = vectorized.NewMemoryTracker(1)
	_, err = BuildElementsFromVecBatches(ctx, budgetSrc, sqo.TagProjection)
	require.Error(t, err, "an over-budget block must fail loud, not silently truncate")
	require.ErrorIs(t, err, ErrQueryMemoryBudgetExceeded)
}
