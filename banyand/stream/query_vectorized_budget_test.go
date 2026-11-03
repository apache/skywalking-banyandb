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
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	budgetSegments        = 4
	budgetSeriesPerSeg    = 500
	budgetRowsPerSeries   = 20
	budgetPayloadBytes    = 512
	budgetLimit           = 20
	budgetRankModulus     = budgetSegments * budgetSeriesPerSeg * budgetRowsPerSeries
	budgetRankMultiplier  = 7919
	budgetResultBatchSize = blockScannerBatchSize
)

// budgetRow is one element of the multi-segment budget fixture.
type budgetRow struct {
	rank      string
	elementID uint64
	ts        int64
}

// lookupChargeMax separates the series lookup's per-series charges, a few hundred
// bytes each, from the block and merge-batch charges, which are tens of kilobytes.
const lookupChargeMax = 1024

// peakLease records the highest usage a query lease reaches while it is charged,
// and the small series-lookup charges, which the lookup keeps for the whole query.
type peakLease struct {
	query.BudgetLease
	peak   atomic.Uint64
	lookup atomic.Uint64
}

func (l *peakLease) Charge(bytes uint64) error {
	if err := l.BudgetLease.Charge(bytes); err != nil {
		return err
	}
	if bytes <= lookupChargeMax {
		l.lookup.Add(bytes)
	}
	for used := l.BudgetLease.Used(); ; {
		peak := l.peak.Load()
		if used <= peak || l.peak.CompareAndSwap(peak, used) {
			return nil
		}
	}
}

// buildBudgetStream writes budgetSegments one-day segments, each with
// budgetSeriesPerSeg series of budgetRowsPerSeries rows, so a scan has to read
// several times more block bytes than the per-query allowance. Timestamps and
// index-order ranks are unique and deliberately uncorrelated, so each order mode
// has a single correct answer.
func buildBudgetStream(t *testing.T) (*stream, timestamp.TimeRange, []budgetRow, [][]*modelv1.TagValue) {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	base := time.Now().Add(-time.Hour).Truncate(time.Hour)
	db := openDatabaseWithTTLDays(t, t.TempDir(), 2*budgetSegments)
	t.Cleanup(func() { _ = db.Close() })

	padding := strings.Repeat("x", budgetPayloadBytes)
	var rows []budgetRow
	var entities [][]*modelv1.TagValue
	var minTS, maxTS int64
	position := 0
	for segIdx := 0; segIdx < budgetSegments; segIdx++ {
		segBase := base.Add(-time.Duration(segIdx) * 24 * time.Hour).UnixNano()
		elems := &elements{}
		var entityDocs index.Documents
		for k := 0; k < budgetSeriesPerSeg; k++ {
			entityName := fmt.Sprintf("%s-%d-%d", entityTagValuePrefix, segIdx, k)
			series := &pbv1.Series{
				Subject:      "benchmark",
				EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityName}}}},
			}
			require.NoError(t, series.Marshal())
			entities = append(entities, series.EntityValues)
			entityDocs = append(entityDocs, index.Document{DocID: uint64(series.ID), EntityValues: series.Buffer})
			for j := 0; j < budgetRowsPerSeries; j++ {
				ts := segBase + int64(k*budgetRowsPerSeries+j)*int64(time.Millisecond)
				rank := fmt.Sprintf("%08d", position*budgetRankMultiplier%budgetRankModulus)
				elementID := convert.HashStr(fmt.Sprintf("budget-%d-%d-%d", segIdx, k, j))
				position++
				if minTS == 0 || ts < minTS {
					minTS = ts
				}
				maxTS = max(maxTS, ts)
				rows = append(rows, budgetRow{rank: rank, elementID: elementID, ts: ts})
				elems.seriesIDs = append(elems.seriesIDs, series.ID)
				elems.timestamps = append(elems.timestamps, ts)
				elems.elementIDs = append(elems.elementIDs, elementID)
				elems.tagFamilies = append(elems.tagFamilies, []tagValues{{
					tag: "benchmark-family",
					values: []*tagValue{
						{tag: entityTagName, value: []byte(entityName), valueType: pbv1.ValueTypeStr},
						{tag: "filter-tag", value: []byte(rank + padding), valueType: pbv1.ValueTypeStr},
					},
				}})
			}
		}
		seg, err := db.CreateSegmentIfNotExist(time.Unix(0, segBase))
		require.NoError(t, err)
		seg.IndexDB().Insert(entityDocs)
		tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
		require.NoError(t, err)
		tst.mustAddElements(elems)
		seg.DecRef()
	}

	schema := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "benchmark", Group: "test"},
		Entity:   &databasev1.Entity{TagNames: []string{entityTagName}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "benchmark-family",
			Tags: []*databasev1.TagSpec{
				{Name: entityTagName, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "filter-tag", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}
	s := &stream{
		schema:     schema,
		l:          logger.GetLogger("test-vec-budget"),
		pm:         protector.Nop{},
		vectorized: vstream.DefaultConfig(),
	}
	s.name, s.group = "benchmark", "test"
	var is indexSchema
	is.parse(schema)
	s.indexSchema.Store(is)
	s.tsdb.Store(db)
	return s, timestamp.NewInclusiveTimeRange(time.Unix(0, minTS), time.Unix(0, maxTS)), rows, entities
}

func budgetQuery(tr timestamp.TimeRange, entities [][]*modelv1.TagValue, order *index.OrderBy) model.StreamQueryOptions {
	return model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  []model.TagProjection{{Family: "benchmark-family", Names: []string{entityTagName, "filter-tag"}}},
		Order:          order,
		MaxElementSize: budgetLimit,
	}
}

// expectedBudgetIDs returns the element IDs a limit-20 query must return.
func expectedBudgetIDs(rows []budgetRow, byRank, desc bool) []string {
	sorted := append([]budgetRow(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		less := sorted[i].ts < sorted[j].ts
		if byRank {
			less = sorted[i].rank < sorted[j].rank
		}
		if desc {
			return !less
		}
		return less
	})
	ids := make([]string, 0, budgetLimit)
	for _, row := range sorted[:budgetLimit] {
		ids = append(ids, hex.EncodeToString(convert.Uint64ToBytes(row.elementID)))
	}
	return ids
}

// admitBudgetLease admits a query exactly like the data-node processor does, on a
// protector without a limit so the per-query allowance is the 8 MiB fallback.
func admitBudgetLease(t *testing.T) (context.Context, *peakLease, func()) {
	t.Helper()
	budget := protector.QueryBudgetFor(protector.Nop{})
	admitted, release, err := budget.AdmitRequestContext(context.Background(), budgetLimit, 0, 20)
	require.NoError(t, err)
	lease, ok := query.BudgetLeaseFromContext(admitted)
	require.True(t, ok)
	peak := &peakLease{BudgetLease: lease}
	return query.WithBudgetLease(admitted, peak), peak, release
}

func runBudgetPipeline(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions, desc bool) []string {
	t.Helper()
	src, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	schema := src.Schema()
	pipeline, err := vstream.BuildStreamMergePipeline(&testVecSource{src: src, schema: schema}, schema, desc,
		0, budgetLimit, vstream.DefaultConfig().BatchSize, sqo.MaxElementSize)
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
	ids := make([]string, 0, len(elems))
	for _, e := range elems {
		ids = append(ids, e.ElementId)
	}
	return ids
}

// TestQueryVectorized_LimitScanStaysWithinQueryBudget is the regression for
// apache/skywalking#14165: a limit-20 scan over far more block bytes than the
// per-query allowance used to fail with "query memory pool exhausted" because the
// scan charged every block it read and the merge charged every batch it consumed,
// and neither charge was ever returned.
func TestQueryVectorized_LimitScanStaysWithinQueryBudget(t *testing.T) {
	s, tr, rows, entities := buildBudgetStream(t)
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "filter-idx", Id: indexOrderRuleID},
		Tags:     []string{"filter-tag"},
	}
	for _, tc := range []struct {
		order  *index.OrderBy
		name   string
		byRank bool
		desc   bool
	}{
		{name: "time-asc", order: &index.OrderBy{Sort: modelv1.Sort_SORT_ASC}},
		{name: "time-desc", order: &index.OrderBy{Sort: modelv1.Sort_SORT_DESC}, desc: true},
		{name: "index-asc", order: &index.OrderBy{Index: indexRule, Sort: modelv1.Sort_SORT_ASC}, byRank: true},
		{name: "index-desc", order: &index.OrderBy{Index: indexRule, Sort: modelv1.Sort_SORT_DESC}, byRank: true, desc: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, lease, release := admitBudgetLease(t)
			defer release()
			base := lease.Used()
			require.NotZero(t, base)
			cursors := pool.AllRefsCount()["stream-blockCursor"]

			got := runBudgetPipeline(ctx, t, s, budgetQuery(tr, entities, tc.order), tc.desc)
			require.Equal(t, expectedBudgetIDs(rows, tc.byRank, tc.desc), got)

			// Every segment alone holds more block bytes than the allowance's
			// remainder after the fixed overhead, and all of them together hold
			// several times that, so a scan that kept its charges would fail.
			allowance := lease.Limit() - base
			total := uint64(budgetSegments * budgetSeriesPerSeg * budgetRowsPerSeries * budgetPayloadBytes)
			require.Greater(t, total, 2*allowance, "the fixture must hold much more than the allowance")
			// The scan holds at most one scanner result batch of blocks, and the merge
			// holds the batches that still back its retained rows.
			peakHeld := lease.peak.Load() - base
			t.Logf("%s: allowance=%d total_block_bytes~%d peak_held=%d limit=%d", tc.name, allowance, total, peakHeld, lease.Limit())
			perBlock := uint64(budgetRowsPerSeries * (budgetPayloadBytes + 64))
			// The merge pins whole batches through the few rows it keeps: at worst one
			// batch per kept row, each estimated at about its payload (tag value and
			// order key) plus a fixed overhead per value; the bound keeps a generous margin.
			perBatch := uint64(budgetRowsPerSeries * (16*budgetPayloadBytes + 1024))
			bound := lease.lookup.Load() + budgetResultBatchSize*perBlock + (budgetLimit+2)*perBatch
			require.LessOrEqual(t, peakHeld, bound, "the scan must hold a few blocks and the merge's retained rows, not the whole range")
			require.Less(t, peakHeld, total/2, "held bytes must stay far below what the scan reads")
			require.Equal(t, base+lease.lookup.Load(), lease.Used(),
				"every scan and merge charge must be refunded once the query is closed")
			require.Equal(t, cursors, pool.AllRefsCount()["stream-blockCursor"], "cursors leaked")
		})
	}
}

// scanLeakCheck snapshots what a scan can leak and returns a function that fails
// the test when any of it is still held after the scan was released.
func scanLeakCheck(t *testing.T, lease *peakLease) func(scan *streamVecScan) {
	t.Helper()
	g := gomega.NewWithT(t)
	goroutines := gleak.Goroutines()
	base := lease.Used()
	cursors := pool.AllRefsCount()["stream-blockCursor"]
	scanBatches := pool.AllRefsCount()["stream-blockScannerBatch"]
	return func(scan *streamVecScan) {
		t.Helper()
		require.Empty(t, scan.pending, "pending cursors left")
		require.Nil(t, scan.cur.bc, "current cursor left")
		require.Nil(t, scan.scanCh, "scanner still attached")
		require.Zero(t, scan.mem.Used(), "soft memory reservation left")
		require.Equal(t, base+lease.lookup.Load(), lease.Used(), "lease charge left")
		require.Equal(t, cursors, pool.AllRefsCount()["stream-blockCursor"], "cursors leaked")
		g.Eventually(func() int { return pool.AllRefsCount()["stream-blockScannerBatch"] - scanBatches },
			5*time.Second, 10*time.Millisecond).Should(gomega.BeZero(), "scanner result batches leaked")
		g.Eventually(gleak.Goroutines, 5*time.Second, 10*time.Millisecond).ShouldNot(gleak.HaveLeaked(goroutines))
	}
}

func openBudgetScan(ctx context.Context, t *testing.T, s *stream, tr timestamp.TimeRange, entities [][]*modelv1.TagValue) *streamVecScan {
	t.Helper()
	src, err := s.queryVectorized(ctx, budgetQuery(tr, entities, &index.OrderBy{Sort: modelv1.Sort_SORT_ASC}))
	require.NoError(t, err)
	scan, ok := src.(*streamVecScan)
	require.True(t, ok)
	// Turn the soft budget on so the reservations it makes are checked as well.
	scan.mem = vectorized.NewMemoryTracker(256 << 20)
	return scan
}

// TestQueryVectorized_ScanReleaseLeavesNothingHeld verifies that releasing a scan
// early, at any point of its progress, stops the scanner goroutine and returns every
// cursor, reservation and charge, however much the scanner had read ahead.
func TestQueryVectorized_ScanReleaseLeavesNothingHeld(t *testing.T) {
	s, tr, _, entities := buildBudgetStream(t)
	for _, batchesBeforeRelease := range []int{0, 1, 2, 40, 300} {
		t.Run(fmt.Sprintf("after-%d-batches", batchesBeforeRelease), func(t *testing.T) {
			ctx, lease, release := admitBudgetLease(t)
			defer release()
			check := scanLeakCheck(t, lease)
			scan := openBudgetScan(ctx, t, s, tr, entities)
			for i := 0; i < batchesBeforeRelease; i++ {
				batch, err := scan.NextBatch(ctx)
				require.NoError(t, err)
				require.NotNil(t, batch)
			}
			if batchesBeforeRelease > 0 {
				require.NotEmpty(t, scan.pending, "the scan should have decoded ahead of the consumer")
				require.NotNil(t, scan.scanCh, "the scanner should still be running")
			}
			scan.Release()
			scan.Release()
			check(scan)
		})
	}
}

// TestQueryVectorized_ScanCancellationLeavesNothingHeld verifies that canceling the
// query context in the middle of a scan fails the scan with the context error rather
// than ending it silently, and that releasing it afterwards leaves nothing held.
func TestQueryVectorized_ScanCancellationLeavesNothingHeld(t *testing.T) {
	s, tr, _, entities := buildBudgetStream(t)
	admitted, lease, release := admitBudgetLease(t)
	defer release()
	check := scanLeakCheck(t, lease)
	ctx, cancel := context.WithCancel(admitted)
	defer cancel()
	scan := openBudgetScan(ctx, t, s, tr, entities)
	for i := 0; i < 5; i++ {
		batch, err := scan.NextBatch(ctx)
		require.NoError(t, err)
		require.NotNil(t, batch)
	}
	cancel()
	var scanErr error
	for scanErr == nil {
		batch, err := scan.NextBatch(ctx)
		scanErr = err
		require.False(t, err == nil && batch == nil, "a canceled scan must not look exhausted")
	}
	require.ErrorIs(t, scanErr, context.Canceled)
	scan.Release()
	check(scan)
}

// TestQueryVectorized_ScanOfBlocksWithoutProjectedFamilyRefundsEverything covers blocks
// that decode to nothing: loadBlockCursor releases such a cursor itself, so the scan
// must give back only the budget it took for the block, and must not release it twice.
func TestQueryVectorized_ScanOfBlocksWithoutProjectedFamilyRefundsEverything(t *testing.T) {
	s, tr, _, entities := buildBudgetStream(t)
	ctx, lease, release := admitBudgetLease(t)
	defer release()
	check := scanLeakCheck(t, lease)
	sqo := budgetQuery(tr, entities, &index.OrderBy{Sort: modelv1.Sort_SORT_ASC})
	sqo.TagProjection = []model.TagProjection{{Family: "no-such-family", Names: []string{"missing"}}}
	src, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	scan, ok := src.(*streamVecScan)
	require.True(t, ok)
	scan.mem = vectorized.NewMemoryTracker(256 << 20)
	batch, err := scan.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, batch)
	scan.Release()
	check(scan)
}
