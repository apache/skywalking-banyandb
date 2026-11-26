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

package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// scriptedCounts answers the reads of a reconciler in turn, repeating the last one.
type scriptedCounts struct {
	errs map[int]error
	// onRead, when set, runs before the n-th read is answered.
	onRead func(n int)
	reads  []*Counts
	n      int
	mu     sync.Mutex
}

func (s *scriptedCounts) snapshot(context.Context) (*Counts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.n
	s.n++
	if s.onRead != nil {
		s.onRead(i)
	}
	if err := s.errs[i]; err != nil {
		return nil, err
	}
	return s.reads[min(i, len(s.reads)-1)], nil
}

func testReconciler(baseline *Counts, reads ...*Counts) (*reconciler, *scriptedCounts) {
	s := &scriptedCounts{reads: reads}
	r := newReconciler(baseline, s.snapshot)
	r.interval = time.Millisecond
	return r, s
}

var rk = targetKey{Catalog: stream, Group: "g", Stage: "hot", Node: "a", Shard: 0, Segment: "s1"}

func rq(k targetKey, rows uint64) reconcileQuery { return reconcileQuery{Key: k, ID: 1, Rows: rows} }

func TestReconcile_DeltaDecides(t *testing.T) {
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	cases := []struct {
		name   string
		actual int64
		want   reconcileOutcome
	}{
		{"delta 0: not landed", 10, reconcileNotLanded},
		{"delta n: landed", 14, reconcileLanded},
		{"other delta: undecidable", 12, reconcileUndecidable},
		{"negative delta: undecidable", 9, reconcileUndecidable},
	}
	for _, tc := range cases {
		r, _ := testReconciler(base, emptyCounts().set(stream, "g", "hot", "a", 0, "s1", tc.actual))
		d := r.decide(context.Background(), rq(rk, 4))
		assert.Equal(t, tc.want, d.Outcome, tc.name)
		assert.Equal(t, int64(10), d.Expected, tc.name)
		assert.Equal(t, tc.actual, d.Actual, tc.name)
		assert.True(t, d.HasActual, tc.name)
	}
}

func TestReconcile_LandedPartsRaiseTheExpectation(t *testing.T) {
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	r, _ := testReconciler(base, emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 17))
	r.recordLanded(rk, 3)
	other := rk
	other.Node = "b"
	r.recordLanded(other, 100)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileLanded, d.Outcome)
	assert.Equal(t, "13→17", d.String())
}

func TestReconcile_NewSegmentStartsFromZero(t *testing.T) {
	r, _ := testReconciler(emptyCounts(), emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 4))
	assert.Equal(t, reconcileLanded, r.decide(context.Background(), rq(rk, 4)).Outcome)
}

func TestReconcile_MissingFromThePlanIsUndecidable(t *testing.T) {
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	segmentGone := emptyCounts().set(stream, "g", "hot", "a", 0, "s2", 10)
	r, _ := testReconciler(base, segmentGone)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "no segment s1")
	assert.Equal(t, "10→?", d.String())

	nodeGone := emptyCounts().set(stream, "g", "hot", "b", 0, "s1", 10)
	r, _ = testReconciler(base, nodeGone)
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "node a reports no shard 0")

	unreachable := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 14)
	unreachable.Unreachable = []string{"a"}
	r, _ = testReconciler(base, unreachable)
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "node a did not answer the Plan")

	noBaseline := emptyCounts()
	noBaseline.Unreachable = []string{"a"}
	r, _ = testReconciler(noBaseline, emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 14))
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "did not answer the baseline Plan")
}

func TestReconcile_MissingKeyWithNothingExpectedIsNotLanded(t *testing.T) {
	// The inventory skips a shard or a segment without parts: with no rows expected there, the
	// node answering without the key holds nothing, so the part did not land.
	otherSegment := emptyCounts().set(stream, "g", "hot", "a", 0, "s2", 7)
	r, _ := testReconciler(otherSegment, otherSegment)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileNotLanded, d.Outcome, d.Reason)
	assert.Equal(t, "0→0", d.String())

	r, _ = testReconciler(emptyCounts(), emptyCounts())
	assert.Equal(t, reconcileNotLanded, r.decide(context.Background(), rq(rk, 4)).Outcome, "no shard of the node at all")

	// Rows recorded as landed raise the expectation: their key missing is undecidable.
	r, _ = testReconciler(emptyCounts(), otherSegment)
	r.recordLanded(rk, 3)
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "no segment s1 in shard 0, although 3 row(s) are expected there")

	// A node that did not answer is never read as holding nothing.
	gone := emptyCounts()
	gone.Unreachable = []string{"a"}
	r, _ = testReconciler(emptyCounts(), gone)
	assert.Equal(t, reconcileUndecidable, r.decide(context.Background(), rq(rk, 4)).Outcome)
}

func TestReconcile_MergeSampledTraceGroupIsUndecidable(t *testing.T) {
	tk := rk
	tk.Catalog, tk.Group = commonv1.Catalog_CATALOG_TRACE, "tg"
	post := emptyCounts().set(commonv1.Catalog_CATALOG_TRACE, "tg", "hot", "a", 0, "s1", 4).set(stream, "g", "hot", "a", 0, "s1", 4)
	r, s := testReconciler(emptyCounts(), post)
	r.sampled = map[string]bool{"tg": true}
	got := r.decideAll(context.Background(), []reconcileQuery{rq(tk, 4), rq(rk, 4)})
	assert.Equal(t, reconcileUndecidable, got[0].Outcome)
	assert.Contains(t, got[0].Reason, "trace group tg samples spans while parts merge")
	assert.Equal(t, reconcileLanded, got[1].Outcome, "the other queries are still decided")
	assert.Equal(t, 2, s.n)

	r, s = testReconciler(emptyCounts(), post)
	r.sampled = map[string]bool{"tg": true}
	assert.Equal(t, reconcileUndecidable, r.decide(context.Background(), rq(tk, 4)).Outcome)
	assert.Zero(t, s.n, "a sampled group alone needs no read")
}

func TestMergeSampledGroups(t *testing.T) {
	sampler := &commonv1.Plugin{Name: "s", Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{Path: "s.so"}}}
	group := func(name string, catalog commonv1.Catalog, cfg *commonv1.TracePipelineConfig) *commonv1.Group {
		g := testGroup(name, catalog)
		g.Pipeline = cfg
		return g
	}
	merge := []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE}
	finalize := []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE}
	trace := commonv1.Catalog_CATALOG_TRACE
	got := mergeSampledGroups(map[string]*commonv1.Group{
		"merge":    group("merge", trace, &commonv1.TracePipelineConfig{Enabled: true, Plugins: []*commonv1.Plugin{sampler}, EnabledEvents: merge}),
		"finalize": group("finalize", trace, &commonv1.TracePipelineConfig{Enabled: true, Plugins: []*commonv1.Plugin{sampler}, EnabledEvents: finalize}),
		"disabled": group("disabled", trace, &commonv1.TracePipelineConfig{Enabled: false, Plugins: []*commonv1.Plugin{sampler}, EnabledEvents: merge}),
		"noevent":  group("noevent", trace, &commonv1.TracePipelineConfig{Enabled: true, Plugins: []*commonv1.Plugin{sampler}}),
		"nosample": group("nosample", trace, &commonv1.TracePipelineConfig{Enabled: true, EnabledEvents: merge}),
		"plain":    group("plain", trace, nil),
		"stream":   group("stream", stream, nil),
	})
	assert.Equal(t, map[string]bool{"merge": true, "finalize": true}, got)
}

func TestReconcile_WaitsForTwoAgreeingReads(t *testing.T) {
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	moving := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 12)
	stable := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 14)
	r, s := testReconciler(base, moving, stable, stable)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileLanded, d.Outcome, "the first, unstable read is not used")
	assert.Equal(t, 3, s.n)

	// A failed read breaks the pair.
	r, s = testReconciler(base, stable)
	s.errs = map[int]error{1: errors.New("plan down")}
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileLanded, d.Outcome)
	assert.Equal(t, 4, s.n)
}

func TestReconcile_GivesUpWhenCountsKeepMoving(t *testing.T) {
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	var reads []*Counts
	for i := range reconcileAttempts + 2 {
		reads = append(reads, emptyCounts().set(stream, "g", "hot", "a", 0, "s1", int64(10+i)))
	}
	r, s := testReconciler(base, reads...)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "did not read the same twice")
	assert.Equal(t, reconcileAttempts+1, s.n, "the reads are bounded")

	r, s = testReconciler(base, base)
	s.errs = map[int]error{}
	for i := range reconcileAttempts + 1 {
		s.errs[i] = errors.New("plan down")
	}
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "the Plan failed: plan down")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, _ = testReconciler(base, base)
	assert.Equal(t, reconcileUndecidable, r.decide(ctx, rq(rk, 4)).Outcome)
}

func TestReconcile_DecideAllUsesOneReadPair(t *testing.T) {
	b, c := rk, rk
	b.Node = "b"
	c.Node, c.Shard = "c", 1
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10).set(stream, "g", "hot", "b", 0, "s1", 10).set(stream, "g", "hot", "c", 1, "s1", 5)
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 14).set(stream, "g", "hot", "b", 0, "s1", 10).set(stream, "g", "hot", "c", 1, "s1", 6)
	r, s := testReconciler(base, post)
	got := r.decideAll(context.Background(), []reconcileQuery{rq(rk, 4), rq(b, 4), rq(c, 2)})
	require.Len(t, got, 3)
	assert.Equal(t, reconcileLanded, got[0].Outcome)
	assert.Equal(t, reconcileNotLanded, got[1].Outcome)
	assert.Equal(t, reconcileUndecidable, got[2].Outcome)
	assert.Equal(t, 2, s.n, "one pair of reads decides every query")
	assert.Empty(t, r.decideAll(context.Background(), nil))
}

func TestKeyLocks_SerialiseOneKey(t *testing.T) {
	var l keyLocks
	unlock := l.lock(rk)
	other := rk
	other.Shard = 1
	l.lock(other)() // another key is not blocked
	got := make(chan struct{})
	//panicdiag:allow-rawgo test waiter
	go func() {
		l.lock(rk)()
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("the second lock of a key must wait")
	case <-time.After(20 * time.Millisecond):
	}
	unlock()
	<-got
}

func TestPushesReconcilableParts(t *testing.T) {
	entry := func(c commonv1.Catalog, k WorkItemKind) Entry { return Entry{Item: WorkItem{Catalog: c, Kind: k}} }
	assert.True(t, pushesReconcilableParts([]Entry{entry(measure, ShardItem), entry(stream, ShardItem)}))
	assert.True(t, pushesReconcilableParts([]Entry{entry(commonv1.Catalog_CATALOG_TRACE, ShardItem)}))
	assert.False(t, pushesReconcilableParts([]Entry{entry(measure, ShardItem), entry(stream, SegmentItem), entry(commonv1.Catalog_CATALOG_PROPERTY, PropertyItem)}))
	assert.False(t, pushesReconcilableParts(nil))
}

func TestReconcile_MissingKeyWhileTheShardGrewElsewhereIsUndecidable(t *testing.T) {
	// A data node puts an imported part into an existing segment covering its time, whatever its
	// suffix: the part's own key missing while the shard grew elsewhere may be the part.
	grewElsewhere := emptyCounts().set(stream, "g", "hot", "a", 0, "legacy", 4)
	r, _ := testReconciler(emptyCounts(), grewElsewhere)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "may have landed in another segment covering its time")
}

func TestReconcile_SeveralPartsAtOneKeyAreDecidedTogether(t *testing.T) {
	base := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	p1, p2 := rq(rk, 4), rq(rk, 4)
	p2.ID = 2
	for _, tc := range []struct {
		name   string
		actual int64
		want   reconcileOutcome
	}{
		{name: "neither landed", actual: 10, want: reconcileNotLanded},
		{name: "both landed", actual: 18, want: reconcileLanded},
		// One of the two landed: the same rows cannot tell which.
		{name: "one landed", actual: 14, want: reconcileUndecidable},
	} {
		r, _ := testReconciler(base, emptyCounts().set(stream, "g", "hot", "a", 0, "s1", tc.actual))
		ds := r.decideAll(context.Background(), []reconcileQuery{p1, p2})
		for _, d := range ds {
			assert.Equal(t, tc.want, d.Outcome, tc.name)
		}
		if tc.want == reconcileUndecidable {
			assert.Contains(t, ds[0].Reason, "2 ambiguous parts at this key", tc.name)
		}
	}
}

func TestReconcile_TaintedKeyIsUndecidableWithoutARead(t *testing.T) {
	r, s := testReconciler(emptyCounts(), emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 4).set(stream, "g", "hot", "b", 0, "s1", 4))
	r.taint(rk)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "an earlier payload at this key could not be reconciled")
	assert.Zero(t, s.n, "no Plan is read")
	other := rk
	other.Node = "b"
	assert.Equal(t, reconcileLanded, r.decide(context.Background(), rq(other, 4)).Outcome, "only the tainted key")
}

func TestReconcile_NodeNotAskedIsNotReadAsEmpty(t *testing.T) {
	// A node missing from the liaison's registry is in neither list of the Plan.
	notAsked := emptyCounts()
	notAsked.Answered = []string{"b"}
	r, _ := testReconciler(emptyCounts(), notAsked)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "node a was not asked by the Plan")

	baseline := emptyCounts()
	baseline.Answered = []string{"b"}
	r, s := testReconciler(baseline, emptyCounts())
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "not asked by the baseline Plan")
	assert.Zero(t, s.n, "a node the baseline did not count is undecidable without a read")
}

func TestReconcile_KeysAreDecidedOnceEachIsStable(t *testing.T) {
	other := rk
	other.Node = "b"
	moving := func(b int64) *Counts {
		return emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 4).set(stream, "g", "hot", "b", 0, "s1", b)
	}
	// a is stable from the first two reads, b only from the third and fourth.
	r, s := testReconciler(emptyCounts(), moving(1), moving(2), moving(4), moving(4))
	ds := r.decideAll(context.Background(), []reconcileQuery{rq(rk, 4), rq(other, 4)})
	assert.Equal(t, reconcileLanded, ds[0].Outcome)
	assert.Equal(t, reconcileLanded, ds[1].Outcome, ds[1].Reason)
	assert.Equal(t, 4, s.n)

	// A node that keeps not answering is read again, and its reason is what is reported.
	gone := emptyCounts()
	gone.Unreachable = []string{"a"}
	r, s = testReconciler(emptyCounts(), gone)
	d := r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Equal(t, "node a did not answer the Plan", d.Reason)
	assert.Equal(t, reconcileAttempts+1, s.n)

	// A failed read followed by agreeing reads decides.
	r, s = testReconciler(emptyCounts(), moving(4))
	s.errs = map[int]error{0: context.DeadlineExceeded}
	assert.Equal(t, reconcileLanded, r.decide(context.Background(), rq(rk, 4)).Outcome)
}

// A part of another segment of the same shard pushed while the counts are read moves the shard's
// counts: a query that cannot be decided says so, and one with no such push does not.
func TestReconcile_ConcurrentPushOfTheShardIsNamed(t *testing.T) {
	other := rk
	other.Segment, other.Node = "s2", ""
	// The part's own key is missing while the shard grew in another segment.
	grewElsewhere := emptyCounts().set(stream, "g", "hot", "a", 0, "s2", 4)

	r, _ := testReconciler(emptyCounts(), grewElsewhere)
	done := r.pushing(other)
	d := r.decide(context.Background(), rq(rk, 4))
	done()
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "other parts of shard 0 were being pushed meanwhile (segment s2), which moves its counts; ")
	assert.Contains(t, d.Reason, "may have landed in another segment covering its time")

	// A push of the shard that started and ended while the counts were read counts too.
	var reads []*Counts
	for i := range reconcileAttempts + 2 {
		reads = append(reads, emptyCounts().set(stream, "g", "hot", "a", 0, "s1", int64(10+i)))
	}
	r, s := testReconciler(emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10), reads...)
	s.onRead = func(n int) {
		if n == 1 {
			r.pushing(other)()
		}
	}
	d = r.decide(context.Background(), rq(rk, 4))
	assert.Equal(t, reconcileUndecidable, d.Outcome)
	assert.Contains(t, d.Reason, "other parts of shard 0 were being pushed meanwhile, which moves its counts; the counts did not read the same twice")

	// The same segment, another shard or nothing pushed: no note.
	for _, k := range []targetKey{rk, {Catalog: stream, Group: "g", Stage: "hot", Shard: 1, Segment: "s2"}} {
		r, _ = testReconciler(emptyCounts(), grewElsewhere)
		done = r.pushing(k)
		d = r.decide(context.Background(), rq(rk, 4))
		done()
		assert.Equal(t, reconcileUndecidable, d.Outcome)
		assert.NotContains(t, d.Reason, "being pushed meanwhile", "%+v", k)
	}
}

const (
	streamShardKey = "stream/sg/hot/20261018/shard-0"
	nativeSegment  = "20261018"
)

// reconcilingOptions are nativePushOptions with a reconciler over baseline that reads the
// counts from li's Plan.
func reconcilingOptions(li *cluster.Liaison, baseline *Counts) PushOptions {
	opts := nativePushOptions()
	opts.reconciler = newReconciler(baseline, func(ctx context.Context) (*Counts, error) { return Snapshot(ctx, li.Conn, nil) })
	opts.reconciler.interval = time.Millisecond
	return opts
}

// d2FailsAfterCompletion answers the first part payload with d1 stored and d2 failed after its
// completion; a part payload pushed again lands on its retry nodes.
func d2FailsAfterCompletion(f *fakePartsLiaison) {
	f.answer = func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() != nativefmt.KindPart {
			return okTargets("d1", "d2"), nil
		}
		if len(p.init.GetRetryNodes()) > 0 {
			return okTargets(p.init.GetRetryNodes()...), nil
		}
		resp := okTargets("d1", "d2")
		resp.Targets[1].Success, resp.Targets[1].Error, resp.Targets[1].CompletionSent = false, "stream reset", true
		resp.Targets[1].FailureCause = transferv1.ImportPartsResponse_FAILURE_CAUSE_STREAM_BROKEN
		return resp, nil
	}
}

func partPayloads(f *fakePartsLiaison) []*recordedPayload {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*recordedPayload
	for _, p := range f.payloads {
		if p.init.GetKind() == nativefmt.KindPart {
			out = append(out, p)
		}
	}
	return out
}

// streamCounts is what the Plan counts in segment 20261018 of shard 0 of stream sg/hot on d1
// and d2.
func streamCounts(d1, d2 int64) *Counts {
	return emptyCounts().set(stream, "sg", "hot", "d1", 0, nativeSegment, d1).set(stream, "sg", "hot", "d2", 0, nativeSegment, d2)
}

// shardArtifact writes a native artifact of one shard item of catalog/group, segment 20261018,
// shard 0, from source node n1, whose container holds members; it is routed to targets.
func shardArtifact(t *testing.T, catalog commonv1.Catalog, group string, rows uint64, targets []string, members ...member) (*Artifact, Entry) {
	t.Helper()
	dir := t.TempDir()
	shard := writeContainer(t, dir, fmt.Sprintf("nodes/n1/%s/%s/seg-%s/shard-0", transfer.CatalogName(catalog), group, nativeSegment), members...)
	art := &Artifact{Dir: dir, Manifest: &manifest.Manifest{Format: manifest.FormatNative}, Schemas: NewSchemas()}
	item := WorkItem{
		Catalog: catalog, Group: group, Stage: "hot", Segment: nativeSegment, SegmentVersion: "1.5.0", Kind: ShardItem,
		Sources: []WorkSource{{Node: "n1", Files: []manifest.File{shard}, MinTimestamp: nativeSegTime + 1, MaxTimestamp: nativeSegTime + 2, Rows: rows}},
	}
	return art, Entry{Item: item, TargetStage: "hot", TargetNodes: targets, Copies: uint32(len(targets))}
}

func coreMeta(rows int) string {
	return fmt.Sprintf(`{"compressedSizeBytes":30,"uncompressedSizeBytes":60,"uncompressedSpanSizeBytes":60,"totalCount":%d,`+
		`"blocksCount":1,"minTimestamp":%d,"maxTimestamp":%d}`, rows, nativeSegTime+1, nativeSegTime+2)
}

func TestPushParts_ReconcileStreamPartLanded(t *testing.T) {
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{plan: func(int) *Counts { return streamCounts(14, 14) }}
	d2FailsAfterCompletion(f)
	li := f.serve(t)
	var log bytes.Buffer
	opts := reconcilingOptions(li, streamCounts(10, 10))
	opts.Log = &log
	res, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, entries, opts)
	require.NoError(t, err, log.String())
	assert.Len(t, res.Done, 2)
	assert.Len(t, partPayloads(f), 1, "a part the counts show landed is not sent again")
	assert.Contains(t, log.String(), "reconcile "+streamShardKey+" part 1 of n1 on d2: landed (10→14, part 4 rows)")
	assert.Equal(t, 2, f.planCalls, "two agreeing reads")
}

// A part that reads as not landed on a node whose answer failed after the completion is not
// judged in-run: the node may still be committing it, whether its stream broke or the liaison
// stopped waiting for it (the structured failure_cause, whatever the error text), or the liaison
// named no cause or one this client does not know. The run fails with the payload left to a
// resume, and the part is not sent again.
func TestPushParts_ReconcileStreamPartNotLandedIsLeftToResume(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		cause        transferv1.ImportPartsResponse_FailureCause
	}{
		{"broken connection", "the connection to the node broke after the completion", transferv1.ImportPartsResponse_FAILURE_CAUSE_STREAM_BROKEN},
		{"no answer", "the liaison stopped waiting for the node", transferv1.ImportPartsResponse_FAILURE_CAUSE_NO_ANSWER},
		{"unspecified", "the node failed after the completion", transferv1.ImportPartsResponse_FAILURE_CAUSE_UNSPECIFIED},
		{"unknown cause", "the node failed after the completion", transferv1.ImportPartsResponse_FailureCause(99)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortenPartRetries(t)
			art, entries := nativeArtifact(t)
			f := &fakePartsLiaison{plan: func(int) *Counts { return streamCounts(14, 10) }}
			d2FailsAfterCompletion(f)
			answer := f.answer
			f.answer = func(i int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
				resp, err := answer(i, p)
				if p.init.GetKind() == nativefmt.KindPart && len(p.init.GetRetryNodes()) == 0 {
					resp.Targets[1].FailureCause = tc.cause
				}
				return resp, err
			}
			li := f.serve(t)
			var log bytes.Buffer
			opts := reconcilingOptions(li, streamCounts(10, 10))
			opts.Log = &log
			res, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, entries, opts)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "reconciliation: d2 undecidable (10→10): "+tc.reason)
			assert.Contains(t, err.Error(), "a resume decides it")
			assert.Equal(t, []string{"stream/sg/hot/20261018/segment"}, res.Done)
			assert.Len(t, partPayloads(f), 1, "a part that may still be committed is not sent again in-run")
			assert.Contains(t, log.String(), "reconcile "+streamShardKey+" part 1 of n1 on d2: undecidable (10→10, part 4 rows): "+tc.reason)
		})
	}
}

func TestPushParts_ReconcileUndecidableFails(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{plan: func(int) *Counts { return streamCounts(14, 12) }}
	d2FailsAfterCompletion(f)
	li := f.serve(t)
	var log bytes.Buffer
	opts := reconcilingOptions(li, streamCounts(10, 10))
	opts.Log = &log
	res, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, entries, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "may be stored on them already")
	assert.Contains(t, err.Error(), "reconciliation: d2 undecidable (10→12): the rows grew by 2, neither 0 nor the part's 4")
	assert.Equal(t, []string{"stream/sg/hot/20261018/segment"}, res.Done)
	assert.Len(t, partPayloads(f), 1, "an undecidable part is not sent again")
	assert.Contains(t, log.String(), "reconcile "+streamShardKey+" part 1 of n1 on d2: undecidable (10→12, part 4 rows)")
	// The part may still land on d2: its key is tainted before the item releases it.
	assert.True(t, opts.reconciler.isTainted(partTargetKey(stream, "sg", "hot", nativeSegment, 0, "d2")))
	assert.False(t, opts.reconciler.isTainted(partTargetKey(stream, "sg", "hot", nativeSegment, 0, "d1")))
}

func TestPushParts_ReconcileTracePart(t *testing.T) {
	trace := commonv1.Catalog_CATALOG_TRACE
	art, e := shardArtifact(t, trace, "tg", 3, []string{"d1", "d2"},
		member{"0000000000000001/metadata.json", coreMeta(3)},
		member{"0000000000000001/spans.bin", "SPANS"},
		member{"0000000000000001/primary.bin", "TPRIMARY"},
		member{"0000000000000001.snp", `["0000000000000001"]`},
	)
	counts := func(d1, d2 int64) *Counts {
		return emptyCounts().set(trace, "tg", "hot", "d1", 0, nativeSegment, d1).set(trace, "tg", "hot", "d2", 0, nativeSegment, d2)
	}
	for _, tc := range []struct {
		post    *Counts
		outcome string
		parts   int
	}{
		{counts(3, 3), "landed (0→3, part 3 rows)", 1},
		{counts(3, 0), "undecidable (0→0, part 3 rows): the connection to the node broke after the completion", 1},
	} {
		shortenPartRetries(t)
		f := &fakePartsLiaison{plan: func(int) *Counts { return tc.post }}
		d2FailsAfterCompletion(f)
		li := f.serve(t)
		var log bytes.Buffer
		opts := reconcilingOptions(li, counts(0, 0))
		opts.Log = &log
		_, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, []Entry{e}, opts)
		if tc.parts == 1 && strings.HasPrefix(tc.outcome, "landed") {
			require.NoError(t, err, log.String())
		} else {
			require.Error(t, err, "a part that reads as not landed in-run is left to a resume")
		}
		assert.Contains(t, log.String(), "reconcile trace/tg/hot/20261018/shard-0 part 1 of n1 on d2: "+tc.outcome)
		assert.Len(t, partPayloads(f), tc.parts)
	}
}

func TestPushParts_MeasurePartFailedAfterCompletionIsResentWithoutReconciling(t *testing.T) {
	shortenPartRetries(t)
	art, e := shardArtifact(t, measure, "mg", 4, []string{"d1", "d2"},
		member{"0000000000000001/metadata.json", coreMeta(4)},
		member{"0000000000000001/primary.bin", "PRIMARY"},
		member{"0000000000000001/meta.bin", "META"},
		member{"0000000000000001/timestamps.bin", "TS"},
		member{"0000000000000001/fv.bin", "FV"},
		member{"0000000000000001.snp", `["0000000000000001"]`},
	)
	f := &fakePartsLiaison{plan: func(int) *Counts { return emptyCounts() }}
	d2FailsAfterCompletion(f)
	li := f.serve(t)
	var log bytes.Buffer
	opts := reconcilingOptions(li, emptyCounts())
	opts.Log = &log
	_, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, []Entry{e}, opts)
	require.NoError(t, err, log.String())
	parts := partPayloads(f)
	require.Len(t, parts, 2, "a measure part converges when pushed again")
	assert.Equal(t, []string{"d2"}, parts[1].init.GetRetryNodes())
	assert.Zero(t, f.planCalls, "a measure part is not reconciled")
	assert.NotContains(t, log.String(), "reconcile ")
}

func TestPushParts_ElementIndexFailedAfterCompletionStillFails(t *testing.T) {
	shortenPartRetries(t)
	art, entries := nativeArtifact(t)
	f := &fakePartsLiaison{plan: func(int) *Counts { return streamCounts(4, 4) }}
	f.answer = func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		resp := okTargets("d1", "d2")
		if p.init.GetKind() == nativefmt.KindElementIndex {
			resp.Targets[1].Success, resp.Targets[1].Error, resp.Targets[1].CompletionSent = false, "stream reset", true
		}
		return resp, nil
	}
	li := f.serve(t)
	_, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, entries, reconcilingOptions(li, streamCounts(0, 0)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KIND_ELEMENT_INDEX failed on d2 after the completion was sent")
	assert.Zero(t, f.planCalls, "an element index is not reconciled")
}

func TestPushParts_ItemsSharingATargetKeyAreSerialised(t *testing.T) {
	art, entries := nativeArtifact(t)
	hot := entries[0]
	// A warm source falling back to hot maps to the same target (segment, shard).
	warm := hot
	warm.Item.Stage = "warm"
	otherShard := hot
	otherShard.Item.Shard = 1
	var mu sync.Mutex
	inflight := map[uint32]int{}
	most := map[uint32]int{}
	peak, total := 0, 0
	f := &fakePartsLiaison{answer: func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		shard := p.init.GetShardId()
		mu.Lock()
		inflight[shard]++
		total++
		most[shard] = max(most[shard], inflight[shard])
		peak = max(peak, total)
		mu.Unlock()
		if shard == 1 {
			// The other shard is not held back by the key of shard 0: it is answered once a
			// payload of shard 0 is in flight with it.
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
				mu.Lock()
				busy := inflight[0] > 0
				mu.Unlock()
				if busy {
					break
				}
			}
		}
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		inflight[shard]--
		total--
		mu.Unlock()
		return okTargets("d1", "d2"), nil
	}}
	opts := nativePushOptions()
	opts.Parallelism = 3
	_, err := PushParts(context.Background(), []*cluster.Liaison{f.serve(t)}, art, []Entry{hot, warm, otherShard}, opts)
	require.NoError(t, err)
	assert.Equal(t, 1, most[0], "two items of one target key never push at the same time")
	assert.GreaterOrEqual(t, peak, 2, "items of other target keys push in parallel")
	assert.Len(t, f.payloads, 6)
}

func TestPushParts_MergeSampledTracePartIsNotReconciled(t *testing.T) {
	shortenPartRetries(t)
	trace := commonv1.Catalog_CATALOG_TRACE
	art, e := shardArtifact(t, trace, "tg", 3, []string{"d1", "d2"},
		member{"0000000000000001/metadata.json", coreMeta(3)},
		member{"0000000000000001/spans.bin", "SPANS"},
		member{"0000000000000001/primary.bin", "TPRIMARY"},
		member{"0000000000000001.snp", `["0000000000000001"]`},
	)
	f := &fakePartsLiaison{plan: func(int) *Counts { return emptyCounts() }}
	d2FailsAfterCompletion(f)
	li := f.serve(t)
	opts := reconcilingOptions(li, emptyCounts())
	opts.reconciler.sampled = map[string]bool{"tg": true}
	_, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, []Entry{e}, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trace group tg samples spans while parts merge")
	assert.Len(t, partPayloads(f), 1, "the part is not sent again")
	f.mu.Lock()
	assert.Zero(t, f.planCalls, "a sampled group is never counted")
	f.mu.Unlock()
}

// firstEntry is the first shard item of catalog among entries.
func firstEntry(t *testing.T, entries []Entry, catalog commonv1.Catalog) *Entry {
	t.Helper()
	for i := range entries {
		if entries[i].Item.Catalog == catalog && entries[i].Item.Kind == ShardItem {
			return &entries[i]
		}
	}
	t.Fatalf("no %s shard item", catalog)
	return nil
}

// A target that fails a part after its completion is decided by the failure cause the liaison
// names, not by the wording of its error. A refusal is a definite answer: the part is missing
// there and pushed again to it alone, without reconciling or reading a count. no_answer means
// the node may still be committing the part: it is not read as not landed, nor pushed again.
func TestPushParts_FailureCauseAfterTheCompletion(t *testing.T) {
	for name, tc := range map[string]struct {
		message string
		wantErr string
		parts   int
		cause   transferv1.ImportPartsResponse_FailureCause
	}{
		"refused": {cause: transferv1.ImportPartsResponse_FAILURE_CAUSE_REFUSED, message: "chunk 2 answered FORMAT_VERSION_MISMATCH: no", parts: 2},
		"no answer": {
			cause: transferv1.ImportPartsResponse_FAILURE_CAUSE_NO_ANSWER, message: "chunk 4: stalled", parts: 1,
			wantErr: "the liaison stopped waiting for the node, which may still be committing the part",
		},
	} {
		t.Run(name, func(t *testing.T) {
			shortenPartRetries(t)
			art, entries := nativeArtifact(t)
			f := &fakePartsLiaison{plan: func(int) *Counts { return streamCounts(14, 10) }}
			f.answer = func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
				if p.init.GetKind() != nativefmt.KindPart {
					return okTargets("d1", "d2"), nil
				}
				if len(p.init.GetRetryNodes()) > 0 {
					return okTargets(p.init.GetRetryNodes()...), nil
				}
				resp := okTargets("d1", "d2")
				resp.Targets[1].Success, resp.Targets[1].Error, resp.Targets[1].CompletionSent = false, tc.message, true
				resp.Targets[1].FailureCause = tc.cause
				return resp, nil
			}
			li := f.serve(t)
			var log bytes.Buffer
			opts := reconcilingOptions(li, streamCounts(10, 10))
			opts.Log = &log
			res, err := PushParts(context.Background(), []*cluster.Liaison{li}, art, entries, opts)
			parts := partPayloads(f)
			require.Len(t, parts, tc.parts, log.String())
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err, log.String())
			assert.Len(t, res.Done, 2)
			assert.Equal(t, []string{"d2"}, parts[1].init.GetRetryNodes(), "to the node that refused it only")
			assert.NotContains(t, log.String(), "reconcile ", "a refusal is not reconciled")
			f.mu.Lock()
			defer f.mu.Unlock()
			assert.Zero(t, f.planCalls, "no count is read for a refusal")
		})
	}
}

// answers records a refusal after the completion as missing, without a cause, and any other
// failure after the completion as ambiguous with the cause the liaison named; a value this
// client does not know is taken as unspecified.
func TestAnswers_FailureCauses(t *testing.T) {
	_, entries := nativeArtifact(t)
	e := firstEntry(t, entries, stream)
	init := &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart}
	target := func(node string, completionSent bool, cause transferv1.ImportPartsResponse_FailureCause) *transferv1.ImportPartsResponse_TargetResult {
		return &transferv1.ImportPartsResponse_TargetResult{Node: node, Error: "failed", CompletionSent: completionSent, FailureCause: cause}
	}
	resp := &transferv1.ImportPartsResponse{Targets: []*transferv1.ImportPartsResponse_TargetResult{
		target("d1", true, transferv1.ImportPartsResponse_FAILURE_CAUSE_REFUSED),
		target("d2", true, transferv1.ImportPartsResponse_FAILURE_CAUSE_NO_ANSWER),
		target("d3", true, transferv1.ImportPartsResponse_FAILURE_CAUSE_STREAM_BROKEN),
		target("d4", true, transferv1.ImportPartsResponse_FailureCause(42)),
		target("d5", false, transferv1.ImportPartsResponse_FAILURE_CAUSE_UNREACHABLE),
	}}
	p := &partPusher{opts: nativePushOptions()}
	rec := &payloadRecord{}
	a := p.answers(&e.Item, init, resp, []string{"d1", "d2", "d3", "d4", "d5"}, rec, false)
	assert.Equal(t, []string{"d1", "d2", "d3", "d4", "d5"}, a.failed)
	assert.Equal(t, []string{"d2", "d3", "d4"}, a.ambiguous)
	assert.Equal(t, []string{"d1", "d5"}, rec.Missing)
	assert.Equal(t, []string{"d2", "d3", "d4"}, rec.CompletionSent)
	assert.Equal(t, map[string]string{"d2": causeNoAnswer, "d3": causeStreamBroken}, rec.Causes)
	assert.True(t, causeMayCommitLater(rec.Causes["d2"]))
	assert.False(t, causeMayCommitLater(rec.Causes["d3"]))
	assert.False(t, causeMayCommitLater(rec.Causes["d4"]))
}

// The latest answer of a node replaces its cause: a node the liaison stopped waiting for that
// fails again before the completion, or stores the payload, keeps no cause; one that fails after
// the completion again keeps the cause of the latest answer only.
func TestPayloadRecord_CauseFollowsTheLatestAnswer(t *testing.T) {
	r := &payloadRecord{}
	r.answered("n1", false, true, causeNoAnswer)
	r.answered("n2", false, true, causeStreamBroken)
	assert.Equal(t, map[string]string{"n1": causeNoAnswer, "n2": causeStreamBroken}, r.Causes)
	r.answered("n1", false, false, causeUnreachable)
	assert.Equal(t, map[string]string{"n2": causeStreamBroken}, r.Causes, "a failure before the completion keeps no cause")
	assert.Equal(t, []string{"n1"}, r.Missing)
	r.answered("n1", false, true, causeNoAnswer)
	r.answered("n1", false, true, causeStreamBroken)
	assert.Equal(t, map[string]string{"n1": causeStreamBroken, "n2": causeStreamBroken}, r.Causes, "the second answer's cause replaces the first")
	r.answered("n1", false, true, "")
	assert.Equal(t, map[string]string{"n2": causeStreamBroken}, r.Causes, "an answer naming no cause drops the earlier one")
	assert.Equal(t, []string{"n1", "n2"}, r.CompletionSent)
	r.answered("n2", true, false, "")
	assert.Nil(t, r.Causes, "a stored payload keeps no cause")
}

// A payload record keeps its causes through the progress file, under "causes".
func TestPayloadRecord_CausesRoundTrip(t *testing.T) {
	r := &payloadRecord{
		Nodes: []string{"d1"}, Missing: []string{}, CompletionSent: []string{"d2", "d3"},
		Causes: map[string]string{"d2": causeNoAnswer, "d3": causeStreamBroken}, Rows: 4,
	}
	data, err := json.Marshal(r)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"causes":{"d2":"no_answer","d3":"stream_broken"}`)
	got := &payloadRecord{}
	require.NoError(t, json.Unmarshal(data, got))
	assert.Equal(t, r, got)
	assert.Equal(t, r, got.clone())

	data, err = json.Marshal(&payloadRecord{Nodes: []string{"d1"}, Missing: []string{}})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "causes", "a record without causes writes none")
}

// A stream part whose answer said the liaison stopped waiting for a node is written at once: a
// resume of a payload left in flight reads a node counted as not landed as not landed and pushes
// it again, while one recorded with no_answer is undecided, since the node may still be committing it.
func TestTracker_NoAnswerIsWrittenAtOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	art, entries := nativeArtifact(t)
	e := firstEntry(t, entries, commonv1.Catalog_CATALOG_STREAM)
	state := newProgress(art, testBindings())
	track, err := startTracker(context.Background(), path, state, 64, nil)
	require.NoError(t, err)
	ref := partRef("n0", 7, 3)
	require.NoError(t, track.payloadSending(0, e, ref, []string{"n1"}))
	rec := &payloadRecord{Rows: 3}
	rec.answered("n1", false, true, causeNoAnswer)
	require.NoError(t, track.payloadAnswered(0, e, ref, rec, false))
	onDisk := readState(t, path)
	require.NotNil(t, onDisk.Done[e.Item.Key()], "the answer is on disk without another write")
	got := onDisk.Done[e.Item.Key()].Parts[ref.id]
	require.NotNil(t, got)
	assert.Equal(t, map[string]string{"n1": causeNoAnswer}, got.Causes)
	assert.Empty(t, onDisk.Inflight)
	require.NoError(t, track.close())

	opts := defaultImport(t, t.TempDir())
	noAnswer := classify(onDisk, entries, &opts, nil, "")
	require.Len(t, noAnswer, 1)
	assert.Equal(t, actionReconcile, noAnswer[0].action)
	assert.True(t, causeMayCommitLater(noAnswer[0].causes["n1"]), "a resume does not read n1 as not landed")
	lost := newProgress(art, testBindings())
	lost.Inflight["0"] = inflightPayload{Key: e.Item.Key(), Source: "n0", Kind: kindName(nativefmt.KindPart), PartID: 7, Rows: 3, Nodes: []string{"n1"}}
	inflight := classify(lost, entries, &opts, nil, "")
	require.Len(t, inflight, 1)
	assert.Empty(t, inflight[0].causes, "had the answer been lost, n1 counted as not landed would be pushed again")
}

// A node the liaison stopped waiting for may have committed the part after the run ended: a
// resume does not read it as not landed.
func TestReconcileAmbiguous_NoAnswerNodeIsUndecidable(t *testing.T) {
	_, _, e := nativeShardEntry(t)
	rec, _ := testReconciler(emptyCounts(), emptyCounts())
	causes := map[string]string{"d1": causeStreamBroken, "d2": causeNoAnswer}
	a := &ambiguousPayload{entry: e, ref: partRef("n1", 1, 4), nodes: []string{"d1", "d2"}, causes: causes, action: actionReconcile}
	var log bytes.Buffer
	reconcileAmbiguous(context.Background(), &log, rec, []*ambiguousPayload{a})
	assert.Equal(t, []string{"d1"}, a.notLanded)
	assert.Equal(t, []string{"d2"}, a.undecided, log.String())
	assert.Contains(t, log.String(), "the liaison stopped waiting for the node")

	state := newProgress(csvArtifact(), testBindings())
	state.Done[e.Item.Key()] = &itemRecord{Parts: map[string]*payloadRecord{"n1/1": {
		CompletionSent: []string{"d1", "d2"}, Causes: map[string]string{"d1": causeStreamBroken, "d2": causeNoAnswer}, Rows: 4,
	}}}
	amb := classify(state, []Entry{*e}, &config.Import{}, nil, "")
	require.Len(t, amb, 1)
	assert.Equal(t, map[string]string{"d1": causeStreamBroken, "d2": causeNoAnswer}, amb[0].causes, "the progress file keeps why each node failed")
}

// A progress file naming a failure cause this client does not know (one a newer client wrote, or
// refused, which no record keeps) is taken as unspecified: the resume goes on and decides the
// payload by the counts, as it does for a liaison that named no cause.
func TestResumeCSV_UnknownFailureCauseIsTakenAsUnspecified(t *testing.T) {
	for _, cause := range []string{"timed_out", causeRefused} {
		t.Run(cause, func(t *testing.T) {
			f := newFakeLiaison()
			li := f.serve(t)
			opts := resumeImport(t, writeDataArtifact(t))
			interruptedCSV(t, f, li, opts, 5)
			path := ProgressPath(opts)
			editState(t, path, func(s *progressState) {
				s.Done["stream/x"] = &itemRecord{Parts: map[string]*payloadRecord{"n1/1": {
					Nodes: []string{"d1"}, CompletionSent: []string{"d2"}, Causes: map[string]string{"d2": cause}, Rows: 4,
				}}}
			})
			opts.Resume = true
			run := runImportEnv(context.Background(), f, li, opts, nil, nil)
			require.NoError(t, run.err, run.log)
			assert.NotContains(t, run.log, "unknown failure cause")
			assert.False(t, causeMayCommitLater(cause), "an unknown cause does not hold the counts back")
		})
	}
}
