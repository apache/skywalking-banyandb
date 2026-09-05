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
