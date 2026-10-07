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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// NIDX-03 §13 performance benchmark. It measures the IndexDB operations the
// design's performance table names (write hot path, exact/prefix/wildcard
// lookup, index-order Search, external receive) over banyand/internal/storage's
// series index, built with newSeriesIndex exactly as a live segment would.
//
// Scale: nidx03BenchSeriesCount series, split into nidx03BenchGroups groups of
// nidx03BenchInstancesPerGroup instances each (default 1000x1000 = 1,000,000,
// per §13's own target; override with the NIDX03_BENCH_SERIES_COUNT env var
// to a smaller perfect square -- e.g. 40000 for a 200x200 grid -- if 1M proves
// impractical in a given environment). Fixture construction (bulk Insert) is
// unavoidably expensive at this scale; every benchmark pays it once in its own
// setup, outside the timed region, and it dominates `-count>1` wall time more
// than the operation under measurement -- this is an accepted cost of
// benchmarking at the design's stated scale, not a flaw in the harness.
//
// Porting to the 735e9ad2 (previous-release) tree changes exactly three
// functions (every Benchmark function and nidx03BenchFixture method below is
// otherwise unchanged, because 735e9ad2's seriesIndex.Insert/Search/
// EnableExternalSegments and IndexSearchOpts{Order, Projection} already have
// this shape):
//   - buildBenchSeriesIndex: its newSeriesIndex call drops the RootLease
//     argument, and metrics is *inverted.Metrics instead of *metrics.Metrics.
//   - nidx03BenchTakeFileSnapshot: 735e9ad2's IndexDB has no TakeFileSnapshot
//     method (added in NIDX-03 phase 2); the ported copy calls
//     si.store.TakeFileSnapshot(dir) instead.
//   - nidx03SettleCompaction: a no-op in the ported copy. Here it brings the
//     native owner to a quiescent state (tiered plan exhausted, root
//     durable) so no background merge or persist overlaps a timed region;
//     the retired bluge engine has no equivalent Compact call to drive.
//
// Methodology: every operation benchmark closes its fixture in a b.Cleanup
// (outside the timed region), runs the operation once untimed (reported as
// the first-op-ns metric, the one-time lazy per-segment initialization cost)
// before resetting the timer, and should be run with an explicit iteration
// count (-benchtime=Nx, N > 1) so each reported ns/op averages N warm
// operations instead of one cold sample.
const (
	nidx03BenchGroupsDefault            = 1000
	nidx03BenchInstancesPerGroupDefault = 1000
	nidx03BenchSubject                  = "nidx03_bench"
)

// nidx03BenchCounts resolves (groups, instancesPerGroup) from
// NIDX03_BENCH_SERIES_COUNT (a perfect square; default 1_000_000 ->
// 1000x1000), so a constrained environment can run this at a smaller, still
// square, scale without editing the file.
func nidx03BenchCounts(b *testing.B) (groups, perGroup int) {
	b.Helper()
	groups, perGroup = nidx03BenchGroupsDefault, nidx03BenchInstancesPerGroupDefault
	raw := os.Getenv("NIDX03_BENCH_SERIES_COUNT")
	if raw == "" {
		return groups, perGroup
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		b.Fatalf("NIDX03_BENCH_SERIES_COUNT=%q must be a positive integer", raw)
	}
	root := isqrt(n)
	if root*root != n {
		b.Fatalf("NIDX03_BENCH_SERIES_COUNT=%d must be a perfect square (groups == instancesPerGroup)", n)
	}
	return root, root
}

func isqrt(n int) int {
	if n <= 0 {
		return 0
	}
	x := n
	y := (x + 1) / 2
	for y < x {
		x = y
		y = (x + n/x) / 2
	}
	return x
}

// nidx03BenchFixture owns the on-disk series index every benchmark below
// opens fresh (via b.TempDir(), never shared across Benchmark functions, so
// one benchmark's writes never skew another's measurement) and the exact
// series identities it was built from, so operation benchmarks can look one
// up without recomputing the marshaled form on every b.N iteration.
type nidx03BenchFixture struct {
	si          *seriesIndex
	groupOf     func(i int) string
	instanceOf  func(i int) string
	seriesCount int
	groups      int
	perGroup    int
}

// buildBenchSeriesIndex is one of the two functions a 735e9ad2 port of this
// file changes: newSeriesIndex there takes no RootLease argument and metrics
// is *inverted.Metrics (the 735e9ad2 copy drops ", &testRootLease{}" from
// the newSeriesIndex call below). The other is nidx03BenchTakeFileSnapshot.
func buildBenchSeriesIndex(b *testing.B) (*seriesIndex, string) {
	b.Helper()
	root := b.TempDir()
	si, err := newSeriesIndex(context.Background(), root, 0, 0, nil, &testRootLease{})
	if err != nil {
		b.Fatal(err)
	}
	// Close runs as a cleanup, after the benchmark harness has stopped the
	// timer. A `defer si.Close()` in a Benchmark function runs before the
	// function returns, i.e. inside the timed region, and with -benchtime=1x
	// Close (the native owner's final drain, waiting out an in-flight
	// background merge) dominated every row's measurement. Callers must not
	// Close si themselves: the previous release's Close is not idempotent.
	b.Cleanup(func() { _ = si.Close() })
	return si, filepath.Join(root, "sidx")
}

// nidx03BenchTakeFileSnapshot is the second (and last) constructor-shaped
// difference a 735e9ad2 port of this file changes: IndexDB there (added in
// NIDX-03 phase 2, before this phase 3 change) has no TakeFileSnapshot
// method, so the ported copy opens si's package-private bluge-backed store
// field directly -- `return si.store.TakeFileSnapshot(dir)` -- instead of
// calling through seriesIndex.
func nidx03BenchTakeFileSnapshot(si *seriesIndex, dir string) error {
	return si.TakeFileSnapshot(dir)
}

// nidx03SettleCompaction brings the fixture to a quiescent, steady state
// before any timed region: it drives the owner's bounded-merge Compact until
// the tiered plan is a no-op (the root generation stops advancing), then
// waits until that root is durable. A plan that is a no-op on the current
// root leaves the owner's background maintenance goroutine nothing to merge
// either, and waiting for durability also waits out persistence's handle
// promotion, so neither a background merge nor a persist overlaps the timed
// region. A lost race with background maintenance (ErrStaleCompaction) or a
// concurrent collection (ErrPersistenceBusy) is retried, not treated as
// "settled".
func nidx03SettleCompaction(b *testing.B, si *seriesIndex, _ string) {
	b.Helper()
	ctx := context.Background()
	generation := func() uint64 {
		view, err := si.owner.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		g := view.Generation()
		_ = view.Close()
		return g
	}
	settled := false
	for i := 0; i < 1000 && !settled; i++ {
		before := generation()
		err := si.owner.Compact(ctx)
		switch {
		case errors.Is(err, native.ErrStaleCompaction), errors.Is(err, native.ErrPersistenceBusy):
			time.Sleep(10 * time.Millisecond)
		case err != nil:
			b.Fatal(err)
		default:
			settled = generation() == before
		}
	}
	if !settled {
		b.Fatal("series index compaction did not settle")
	}
	deadline := time.Now().Add(5 * time.Minute)
	for si.owner.DurableGeneration() < generation() {
		if time.Now().After(deadline) {
			b.Fatal("series index did not become durable")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Collect superseded segment files now rather than letting the
	// post-persist collection wake run inside a timed region.
	for i := 0; i < 100; i++ {
		err := si.owner.CollectGarbage(ctx)
		if !errors.Is(err, native.ErrPersistenceBusy) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// nidx03WarmUp runs op once outside the timed region, then resets the
// timer. It returns a function the caller must invoke after its timed loop
// to report that first call's latency as the "first-op-ns" metric (a metric
// reported before ResetTimer would be discarded by it). The first
// operation after a segment is published or promoted pays one-time lazy
// per-segment initialization (term dictionary load, bloom filter build,
// stored-document offset table), which is real but is a different quantity
// from the steady-state per-op cost the §13 rows compare.
func nidx03WarmUp(b *testing.B, op func()) (reportFirstOp func()) {
	b.Helper()
	start := time.Now()
	op()
	first := time.Since(start)
	b.ReportAllocs()
	b.ResetTimer()
	return func() { b.ReportMetric(float64(first.Nanoseconds()), "first-op-ns") }
}

func nidx03BenchSeries(group, instance string) *pbv1.Series {
	return &pbv1.Series{
		Subject: nidx03BenchSubject,
		EntityValues: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: instance}}},
		},
	}
}

const nidx03BenchScoreRuleID = 7

// buildNidx03BenchFixture inserts groups*perGroup series (each carrying an
// indexed, sortable "score" field so index-order Search has something to
// sort on) into a fresh series index, batching the writes so construction at
// 1M series completes in a practical amount of wall time.
func buildNidx03BenchFixture(b *testing.B) *nidx03BenchFixture {
	b.Helper()
	groups, perGroup := nidx03BenchCounts(b)
	si, sidxPath := buildBenchSeriesIndex(b)

	const batchSize = 5000
	var batch index.Documents
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := si.Insert(batch); err != nil {
			b.Fatal(err)
		}
		batch = batch[:0]
	}
	scoreKey := index.FieldKey{IndexRuleID: nidx03BenchScoreRuleID}
	n := 0
	for g := 0; g < groups; g++ {
		group := fmt.Sprintf("group-%06d", g)
		for i := 0; i < perGroup; i++ {
			instance := fmt.Sprintf("instance-%06d", i)
			series := nidx03BenchSeries(group, instance)
			if err := series.Marshal(); err != nil {
				b.Fatal(err)
			}
			score := index.NewBytesField(scoreKey, convert.Int64ToBytes(int64(n)))
			score.Store, score.Index = true, true
			batch = append(batch, index.Document{
				EntityValues: append([]byte(nil), series.Buffer...),
				Fields:       []index.Field{score},
				Timestamp:    int64(n + 1),
			})
			n++
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
	flush()
	nidx03SettleCompaction(b, si, sidxPath)
	return &nidx03BenchFixture{
		si:          si,
		groups:      groups,
		perGroup:    perGroup,
		seriesCount: n,
		groupOf:     func(i int) string { return fmt.Sprintf("group-%06d", i) },
		instanceOf:  func(i int) string { return fmt.Sprintf("instance-%06d", i) },
	}
}

// BenchmarkSeriesIndexInsertExisting measures the write hot path NIDX-03 §13
// names: Insert of series that already exist with every field the incoming
// document carries, which InsertIfAbsent admission must recognize and skip
// without encoding a new document.
func BenchmarkSeriesIndexInsertExisting(b *testing.B) {
	fixture := buildNidx03BenchFixture(b)

	const reinsertBatch = 100
	docs := make(index.Documents, 0, reinsertBatch)
	scoreKey := index.FieldKey{IndexRuleID: nidx03BenchScoreRuleID}
	for i := 0; i < reinsertBatch; i++ {
		series := nidx03BenchSeries(fixture.groupOf(0), fixture.instanceOf(i))
		if err := series.Marshal(); err != nil {
			b.Fatal(err)
		}
		score := index.NewBytesField(scoreKey, convert.Int64ToBytes(int64(i)))
		score.Store, score.Index = true, true
		docs = append(docs, index.Document{
			EntityValues: append([]byte(nil), series.Buffer...),
			Fields:       []index.Field{score},
			Timestamp:    int64(i + 1),
		})
	}

	insert := func() {
		if err := fixture.si.Insert(docs); err != nil {
			b.Fatal(err)
		}
	}
	reportFirstOp := nidx03WarmUp(b, insert)
	for i := 0; i < b.N; i++ {
		insert()
	}
	reportFirstOp()
}

// BenchmarkSeriesIndexLookupExact measures Search with a single exact `_id`
// series matcher, no order, no criteria -- Stream/Trace segment.Lookup's
// shape.
func BenchmarkSeriesIndexLookupExact(b *testing.B) {
	fixture := buildNidx03BenchFixture(b)
	ctx := context.Background()
	target := []*pbv1.Series{nidx03BenchSeries(fixture.groupOf(fixture.groups/2), fixture.instanceOf(fixture.perGroup/2))}

	lookup := func() {
		sd, _, err := fixture.si.Search(ctx, target, IndexSearchOpts{})
		if err != nil {
			b.Fatal(err)
		}
		if len(sd.SeriesList) != 1 {
			b.Fatalf("expected exactly one exact match, got %d", len(sd.SeriesList))
		}
	}
	reportFirstOp := nidx03WarmUp(b, lookup)
	for i := 0; i < b.N; i++ {
		lookup()
	}
	reportFirstOp()
}

// BenchmarkSeriesIndexLookupPrefix measures Search with a prefix series
// matcher (trailing AnyTagValue): one group's perGroup instances.
func BenchmarkSeriesIndexLookupPrefix(b *testing.B) {
	fixture := buildNidx03BenchFixture(b)
	ctx := context.Background()
	prefixQuery := &pbv1.Series{
		Subject: nidx03BenchSubject,
		EntityValues: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fixture.groupOf(fixture.groups / 2)}}},
			pbv1.AnyTagValue,
		},
	}

	lookup := func() {
		sd, _, err := fixture.si.Search(ctx, []*pbv1.Series{prefixQuery}, IndexSearchOpts{})
		if err != nil {
			b.Fatal(err)
		}
		if len(sd.SeriesList) != fixture.perGroup {
			b.Fatalf("expected %d prefix matches, got %d", fixture.perGroup, len(sd.SeriesList))
		}
	}
	reportFirstOp := nidx03WarmUp(b, lookup)
	for i := 0; i < b.N; i++ {
		lookup()
	}
	reportFirstOp()
}

// BenchmarkSeriesIndexLookupWildcard measures Search with a wildcard series
// matcher (leading AnyTagValue): one instance across every group.
func BenchmarkSeriesIndexLookupWildcard(b *testing.B) {
	fixture := buildNidx03BenchFixture(b)
	ctx := context.Background()
	wildcardQuery := &pbv1.Series{
		Subject: nidx03BenchSubject,
		EntityValues: []*modelv1.TagValue{
			pbv1.AnyTagValue,
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fixture.instanceOf(fixture.perGroup / 2)}}},
		},
	}

	lookup := func() {
		sd, _, err := fixture.si.Search(ctx, []*pbv1.Series{wildcardQuery}, IndexSearchOpts{})
		if err != nil {
			b.Fatal(err)
		}
		if len(sd.SeriesList) != fixture.groups {
			b.Fatalf("expected %d wildcard matches, got %d", fixture.groups, len(sd.SeriesList))
		}
	}
	reportFirstOp := nidx03WarmUp(b, lookup)
	for i := 0; i < b.N; i++ {
		lookup()
	}
	reportFirstOp()
}

// BenchmarkSeriesIndexSearchIndexOrder measures Search with an index-rule
// sort order over one group's instances -- the Measure index-rule sort path
// (view.SortHits / ProjectSortValue).
func BenchmarkSeriesIndexSearchIndexOrder(b *testing.B) {
	fixture := buildNidx03BenchFixture(b)
	ctx := context.Background()
	prefixQuery := &pbv1.Series{
		Subject: nidx03BenchSubject,
		EntityValues: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fixture.groupOf(fixture.groups / 2)}}},
			pbv1.AnyTagValue,
		},
	}
	order := &index.OrderBy{
		Type: index.OrderByTypeIndex,
		Sort: modelv1.Sort_SORT_ASC,
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Id: nidx03BenchScoreRuleID},
		},
	}

	search := func() {
		// PreloadSize is unused by the current engine's Search but is the
		// previous release's bluge sortIterator page size: a zero value
		// starves it to zero results there, so every benchmark in this file
		// sets it explicitly to keep both trees' call shape identical.
		sd, sortedValues, err := fixture.si.Search(ctx, []*pbv1.Series{prefixQuery}, IndexSearchOpts{Order: order, PreloadSize: fixture.perGroup})
		if err != nil {
			b.Fatal(err)
		}
		if len(sd.SeriesList) != fixture.perGroup || len(sortedValues) != fixture.perGroup {
			b.Fatalf("expected %d sorted matches, got %d series / %d sort values", fixture.perGroup, len(sd.SeriesList), len(sortedValues))
		}
	}
	reportFirstOp := nidx03WarmUp(b, search)
	for i := 0; i < b.N; i++ {
		search()
	}
	reportFirstOp()
}

// BenchmarkSeriesIndexExternalReceive measures EnableExternalSegments'
// StartSegment/WriteChunk/CompleteSegment cycle: the crash-safe raw segment
// receive path the lifecycle/*-series-sync receivers drive.
func BenchmarkSeriesIndexExternalReceive(b *testing.B) {
	fixture := buildNidx03BenchFixture(b)

	// Build one committed segment's bytes per iteration (plus one for the
	// warm-up), each from a fresh, separate series index under its own
	// subject, so every received segment is wholly new to fixture.si: the
	// common receive case this benchmark targets, not the keep-existing
	// dedup path index_external_segments_test.go already covers. Reusing
	// one payload would turn every iteration after the first into a fully
	// deduplicated receive once b.N > 1.
	payloads := make([][]byte, b.N+1)
	for i := range payloads {
		payloads[i] = nidx03BenchExternalSegmentPayload(b, i)
	}
	receive := func(payload []byte) {
		streamer, err := fixture.si.EnableExternalSegments()
		if err != nil {
			b.Fatal(err)
		}
		if err := streamer.StartSegment(); err != nil {
			b.Fatal(err)
		}
		if err := streamer.WriteChunk(payload); err != nil {
			b.Fatal(err)
		}
		if err := streamer.CompleteSegment(); err != nil {
			b.Fatal(err)
		}
	}
	reportFirstOp := nidx03WarmUp(b, func() { receive(payloads[b.N]) })
	for i := 0; i < b.N; i++ {
		receive(payloads[i])
	}
	reportFirstOp()
}

// nidx03BenchExternalSegmentPayload builds one committed segment file's raw
// bytes from a fresh, separate series index: 5000 documents under a subject
// (distinct per ordinal) no fixture series or other payload uses, so
// EnableExternalSegments admits every document as new.
func nidx03BenchExternalSegmentPayload(b *testing.B, ordinal int) []byte {
	b.Helper()
	const externalDocs = 5000
	// Closed by buildBenchSeriesIndex's cleanup; the previous release's
	// Close is not idempotent, so no second, explicit Close here.
	si, _ := buildBenchSeriesIndex(b)
	var docs index.Documents
	for i := 0; i < externalDocs; i++ {
		series := &pbv1.Series{
			Subject: fmt.Sprintf("nidx03_bench_external_%d", ordinal),
			EntityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("ext-%06d", i)}}},
			},
		}
		if err := series.Marshal(); err != nil {
			b.Fatal(err)
		}
		docs = append(docs, index.Document{EntityValues: append([]byte(nil), series.Buffer...), Timestamp: int64(i + 1)})
	}
	if err := si.Insert(docs); err != nil {
		b.Fatal(err)
	}
	segDir := b.TempDir()
	if err := nidx03BenchTakeFileSnapshot(si, segDir); err != nil {
		b.Fatal(err)
	}
	matches, globErr := filepath.Glob(filepath.Join(segDir, "*.seg"))
	if globErr != nil {
		b.Fatal(globErr)
	}
	if len(matches) != 1 {
		b.Fatalf("expected exactly one segment file in %s, found %d", segDir, len(matches))
	}
	data, readErr := os.ReadFile(matches[0])
	if readErr != nil {
		b.Fatal(readErr)
	}
	return data
}
