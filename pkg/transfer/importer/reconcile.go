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
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

var (
	// reconcileSampleInterval is the wait between two Plan reads of a reconciliation: two reads
	// that agree this far apart show counts no commit or merge in progress is still changing.
	reconcileSampleInterval = 5 * time.Second
	// reconcileAttempts bounds the Plan reads after the first one: the counts must read the same
	// on two consecutive reads within them, or every part asked about is undecidable.
	reconcileAttempts = 4
)

// reconcileOutcome is what the counts of the target tell about an ambiguous part on a node.
type reconcileOutcome uint8

const (
	// reconcileUndecidable: the counts cannot tell whether the part landed.
	reconcileUndecidable reconcileOutcome = iota
	// reconcileNotLanded: the node does not hold the part; it is pushed to the node again.
	reconcileNotLanded
	// reconcileLanded: the node holds the part; it is done there.
	reconcileLanded
)

func (o reconcileOutcome) String() string {
	switch o {
	case reconcileLanded:
		return "landed"
	case reconcileNotLanded:
		return "not landed"
	default:
		return "undecidable"
	}
}

// targetKey is where a stream or trace part lands: one segment of one node's shard of a
// (catalog, group, target stage). A native import keeps the source's segment grid and shard
// number, so the source segment and shard of a part are its target ones.
type targetKey struct {
	Group   string
	Stage   string
	Node    string
	Segment string
	Catalog commonv1.Catalog
	Shard   uint32
}

func (k targetKey) countKey() CountKey {
	return CountKey{Catalog: k.Catalog, Group: k.Group, Stage: k.Stage, Node: k.Node, Shard: k.Shard}
}

// reconcileQuery asks whether part ID, of Rows rows, landed at Key.
type reconcileQuery struct {
	Key  targetKey
	ID   uint64
	Rows uint64
}

// reconcileDecision answers a reconcileQuery: Expected is the baseline plus the rows recorded
// as landed at the key, Actual what the Plan counts there (HasActual false when it counts
// nothing), Reason why an undecidable query could not be decided.
type reconcileDecision struct {
	Reason    string
	Expected  int64
	Actual    int64
	Outcome   reconcileOutcome
	HasActual bool
}

// String renders the counts of a decision: "<expected>→<actual>".
func (d reconcileDecision) String() string {
	actual := "?"
	if d.HasActual {
		actual = strconv.FormatInt(d.Actual, 10)
	}
	return fmt.Sprintf("%d→%s", d.Expected, actual)
}

// reconciler decides from the target's counts whether a stream or trace part whose push ended
// ambiguously (the completion was sent, the target failed) landed on a node: the rows a Plan
// counts at the part's target key minus those expected there (the baseline plus the parts
// recorded as landed) is 0 when it did not land and the part's rows when it did; anything else
// is undecidable. It relies on nothing else writing to the target groups meanwhile; several
// ambiguous parts at one key are decided only when all or none of them landed.
type reconciler struct {
	baseline *Counts
	snapshot func(ctx context.Context) (*Counts, error)
	landed   map[targetKey]int64
	// tainted are the keys whose counts are off by an unknown number of rows.
	tainted map[targetKey]bool
	// sampled names the trace groups whose sampling pipeline may drop spans in a merge: their
	// counts do not keep the rows of a part, so their parts are never reconciled.
	sampled map[string]bool
	// pushes are the parts being pushed per shard of a (catalog, group, target stage), by segment:
	// the counts of a shard move while another segment of it takes a part.
	pushes   map[shardKey]*shardPushes
	interval time.Duration
	attempts int
	mu       sync.Mutex
}

// shardKey is one shard of a (catalog, group, target stage), on any node.
type shardKey struct {
	Group   string
	Stage   string
	Catalog commonv1.Catalog
	Shard   uint32
}

func shardKeyOf(k targetKey) shardKey {
	return shardKey{Catalog: k.Catalog, Group: k.Group, Stage: k.Stage, Shard: k.Shard}
}

// shardPushes counts the work items of a shard being pushed per segment, and every one started.
type shardPushes struct {
	active  map[string]int
	started map[string]uint64
}

// newReconciler builds a reconciler over the counts of the target before the push; snapshot
// reads the counts now.
func newReconciler(baseline *Counts, snapshot func(ctx context.Context) (*Counts, error)) *reconciler {
	return &reconciler{
		baseline: baseline, snapshot: snapshot, landed: map[targetKey]int64{}, tainted: map[targetKey]bool{}, sampled: map[string]bool{},
		pushes: map[shardKey]*shardPushes{}, interval: reconcileSampleInterval, attempts: reconcileAttempts,
	}
}

// mergeSampledGroups names the trace groups of groups whose native sampling pipeline may drop
// spans while parts merge: an enabled pipeline with a sampler and the MERGE or FINALIZE event.
func mergeSampledGroups(groups map[string]*commonv1.Group) map[string]bool {
	out := map[string]bool{}
	for name, g := range groups {
		cfg := g.GetPipeline()
		if g.GetCatalog() != commonv1.Catalog_CATALOG_TRACE || !validate.PipelineHasSampler(cfg) {
			continue
		}
		if slices.Contains(cfg.GetEnabledEvents(), commonv1.PipelineEvent_PIPELINE_EVENT_MERGE) ||
			slices.Contains(cfg.GetEnabledEvents(), commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE) {
			out[name] = true
		}
	}
	return out
}

// samplesSpans tells whether the counts of k cannot be trusted to keep a part's rows: a trace
// group sampling spans in merges.
func (r *reconciler) samplesSpans(k targetKey) bool {
	return k.Catalog == commonv1.Catalog_CATALOG_TRACE && r.sampled[k.Group]
}

// recordLanded records that a part of rows rows landed at k.
func (r *reconciler) recordLanded(k targetKey, rows uint64) {
	r.mu.Lock()
	r.landed[k] += int64(rows)
	r.mu.Unlock()
}

// pushing records that a work item pushes parts at k (its node left out) until the returned
// func is called.
func (r *reconciler) pushing(k targetKey) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	sp := r.pushes[shardKeyOf(k)]
	if sp == nil {
		sp = &shardPushes{active: map[string]int{}, started: map[string]uint64{}}
		r.pushes[shardKeyOf(k)] = sp
	}
	sp.active[k.Segment]++
	sp.started[k.Segment]++
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		sp.active[k.Segment]--
	}
}

// otherPushes is how many work items were started so far at the shard of k in another segment,
// and the segments being pushed now.
func (r *reconciler) otherPushes(k targetKey) (started uint64, active []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sp := r.pushes[shardKeyOf(k)]
	if sp == nil {
		return 0, nil
	}
	for seg, n := range sp.started {
		if seg != k.Segment {
			started += n
		}
	}
	for seg, n := range sp.active {
		if seg != k.Segment && n > 0 {
			active = append(active, seg)
		}
	}
	slices.Sort(active)
	return started, active
}

// concurrentNote says, for an undecidable reason of k, that other parts of its shard were being
// pushed while the counts were read, which moves them; "" when none were.
func (r *reconciler) concurrentNote(k targetKey, startedBefore uint64, activeBefore []string) string {
	started, active := r.otherPushes(k)
	segs := transfer.UniqueSorted(append(slices.Clone(activeBefore), active...))
	if started == startedBefore && len(segs) == 0 {
		return ""
	}
	what := "other parts of shard " + strconv.FormatUint(uint64(k.Shard), 10) + " were being pushed meanwhile"
	if len(segs) > 0 {
		what += " (segment " + strings.Join(segs, ", ") + ")"
	}
	return what + ", which moves its counts; "
}

// decide tells whether one part landed at its key.
func (r *reconciler) decide(ctx context.Context, q reconcileQuery) reconcileDecision {
	return r.decideAll(ctx, []reconcileQuery{q})[0]
}

// taint marks a key whose counts carry an unknown offset: a payload there was pushed again or
// left out although it may have landed, so no later part at the key can be decided from them.
func (r *reconciler) taint(k targetKey) {
	r.mu.Lock()
	r.tainted[k] = true
	r.mu.Unlock()
}

func (r *reconciler) isTainted(k targetKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tainted[k]
}

// decideAll decides every query from the Plan reads: the queries of one target key are decided
// together once the key counts the same on two consecutive reads, a sample interval apart,
// and the reads go on, up to attempts after the first, for the keys still moving. A key whose
// counts cannot be trusted (a sampled trace group, a tainted key, a node the baseline did not
// count) is undecidable without a read; so is every key left when the reads fail, keep moving
// or ctx is canceled.
func (r *reconciler) decideAll(ctx context.Context, queries []reconcileQuery) []reconcileDecision {
	ds := &decisionSet{
		r: r, out: make([]reconcileDecision, len(queries)), decided: make([]bool, len(queries)),
		byKey: map[targetKey][]int{}, before: map[targetKey]pushActivity{},
	}
	for i, q := range queries {
		if reason := r.untrusted(q.Key); reason != "" {
			ds.out[i], ds.decided[i] = reconcileDecision{Outcome: reconcileUndecidable, Expected: r.expected(q.Key), Reason: reason}, true
			continue
		}
		if ds.byKey[q.Key] == nil {
			ds.keys = append(ds.keys, q.Key)
		}
		ds.byKey[q.Key] = append(ds.byKey[q.Key], i)
	}
	for _, k := range ds.keys {
		started, active := r.otherPushes(k)
		ds.before[k] = pushActivity{active: active, started: started}
	}
	var prev *Counts
	var lastErr error
	for read := 0; read <= r.attempts && len(ds.left()) > 0; read++ {
		if read > 0 {
			if err := transfer.SleepCtx(ctx, r.interval); err != nil {
				for _, k := range ds.left() {
					ds.undecidable(k, fmt.Sprintf("interrupted: %v", err))
				}
				return ds.out
			}
		}
		cur, err := r.snapshot(ctx)
		if err != nil {
			lastErr, prev = err, nil
			continue
		}
		lastErr = nil
		if prev != nil {
			ds.settle(prev, cur, queries)
		}
		prev = cur
	}
	for _, k := range ds.left() {
		switch {
		case lastErr != nil:
			ds.undecidable(k, fmt.Sprintf("the Plan failed: %v", lastErr))
		case prev != nil && sampleReason(prev, k) != "":
			ds.undecidable(k, sampleReason(prev, k))
		default:
			ds.undecidable(k, ds.note(k, fmt.Sprintf("the counts did not read the same twice in %d reads %s apart", r.attempts+1, r.interval)))
		}
	}
	return ds.out
}

// pushActivity is what the other pushes of a key's shard were when decideAll began.
type pushActivity struct {
	active  []string
	started uint64
}

// decisionSet is what decideAll has decided so far: the decision of every query, the queries of
// each target key and the pushes of the key's shard going on when the reads began.
type decisionSet struct {
	r       *reconciler
	byKey   map[targetKey][]int
	before  map[targetKey]pushActivity
	out     []reconcileDecision
	decided []bool
	keys    []targetKey
}

// left are the keys with queries not decided yet.
func (ds *decisionSet) left() []targetKey {
	var l []targetKey
	for _, k := range ds.keys {
		if !ds.decided[ds.byKey[k][0]] {
			l = append(l, k)
		}
	}
	return l
}

// undecidable decides every query at k as undecidable for reason.
func (ds *decisionSet) undecidable(k targetKey, reason string) {
	for _, i := range ds.byKey[k] {
		ds.out[i], ds.decided[i] = reconcileDecision{Outcome: reconcileUndecidable, Expected: ds.r.expected(k), Reason: reason}, true
	}
}

// note prefixes the reason a key's counts did not settle with the pushes of its shard meanwhile.
func (ds *decisionSet) note(k targetKey, reason string) string {
	return ds.r.concurrentNote(k, ds.before[k].started, ds.before[k].active) + reason
}

// settle decides the queries of every undecided key whose counts read the same in prev and cur.
func (ds *decisionSet) settle(prev, cur *Counts, queries []reconcileQuery) {
	for _, k := range ds.left() {
		if !stableAt(prev, cur, k) {
			continue
		}
		ds.r.evaluateKey(cur, k, queries, ds.byKey[k], ds.out, ds.decided)
		for _, i := range ds.byKey[k] {
			if ds.out[i].Outcome == reconcileUndecidable && ds.out[i].Reason != "" {
				ds.out[i].Reason = ds.note(k, ds.out[i].Reason)
			}
		}
	}
}

// untrusted says why the counts of k cannot decide a part there, "" when they can.
func (r *reconciler) untrusted(k targetKey) string {
	switch {
	case r.samplesSpans(k):
		return "trace group " + k.Group + " samples spans while parts merge, so its counts do not keep a part's rows"
	case r.isTainted(k):
		return "an earlier payload at this key could not be reconciled, so its counts are off by an unknown number of rows"
	case slices.Contains(r.baseline.Unreachable, k.Node):
		return "node " + k.Node + " did not answer the baseline Plan"
	case r.baseline.Answered != nil && !slices.Contains(r.baseline.Answered, k.Node):
		return "node " + k.Node + " was not asked by the baseline Plan: the liaison did not know it"
	}
	return ""
}

func (r *reconciler) expected(k targetKey) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.baseline.Shards[k.countKey()][k.Segment] + r.landed[k]
}

// expectedShard is what the node's shard of k is expected to count over all its segments.
func (r *reconciler) expectedShard(k targetKey) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, rows := range r.baseline.Shards[k.countKey()] {
		n += rows
	}
	for lk, rows := range r.landed {
		if lk.countKey() == k.countKey() {
			n += rows
		}
	}
	return n
}

// evaluateKey decides the queries idx, all at key k, from a stable read of the counts. One
// query is landed when the rows grew by its rows and not landed when they did not grow; several
// queries at one key are decided only when the growth tells them all apart: none grew, or all
// of them. A key the node does not report although it answered holds nothing: the inventory
// skips a shard or a segment without parts, so it reads as 0 rows when none were expected there
// and the node's shard counts no more rows elsewhere; it is undecidable otherwise (retention or
// a foreign delete removed rows, or the part landed in a segment of another suffix that covers
// its time).
func (r *reconciler) evaluateKey(c *Counts, k targetKey, queries []reconcileQuery, idx []int, out []reconcileDecision, decided []bool) {
	expected := r.expected(k)
	var sum uint64
	for _, i := range idx {
		sum += queries[i].Rows
	}
	decide := func(outcome reconcileOutcome, actual int64, hasActual bool, reason string) {
		for _, i := range idx {
			out[i] = reconcileDecision{Outcome: outcome, Expected: expected, Actual: actual, HasActual: hasActual, Reason: reason}
			decided[i] = true
		}
	}
	actual, missing, reason := sample(c, k)
	if reason != "" {
		decide(reconcileUndecidable, 0, false, reason)
		return
	}
	if missing != "" {
		if expected > 0 {
			decide(reconcileUndecidable, 0, false, fmt.Sprintf("%s, although %d row(s) are expected there", missing, expected))
			return
		}
		if shard, want := shardRows(c, k), r.expectedShard(k); shard != want {
			decide(reconcileUndecidable, 0, false, fmt.Sprintf("%s, but the shard counts %d row(s) where %d are expected: the part may have landed in "+
				"another segment covering its time", missing, shard, want))
			return
		}
	}
	grew := actual - expected
	switch {
	case grew == 0:
		decide(reconcileNotLanded, actual, true, "")
	case grew == int64(sum):
		decide(reconcileLanded, actual, true, "")
	case len(idx) > 1:
		decide(reconcileUndecidable, actual, true, fmt.Sprintf("%d ambiguous parts at this key: the rows grew by %d, neither 0 nor their %d together",
			len(idx), grew, sum))
	default:
		decide(reconcileUndecidable, actual, true, fmt.Sprintf("the rows grew by %d, neither 0 nor the part's %d", grew, sum))
	}
}

// sample reads the rows a Plan counts at k. missing says why the node, which answered, reports
// nothing at k (n is then 0); reason says why the read cannot count k at all.
func sample(c *Counts, k targetKey) (n int64, missing, reason string) {
	if reason = sampleReason(c, k); reason != "" {
		return 0, "", reason
	}
	segs, ok := c.Shards[k.countKey()]
	if !ok {
		return 0, fmt.Sprintf("node %s reports no shard %d of %s %s/%s", k.Node, k.Shard, transfer.CatalogName(k.Catalog), k.Group, k.Stage), ""
	}
	n, ok = segs[k.Segment]
	if !ok {
		return 0, fmt.Sprintf("node %s reports no segment %s in shard %d", k.Node, k.Segment, k.Shard), ""
	}
	return n, "", ""
}

// sampleReason says why a read cannot count k at all: the node did not answer it, or was not
// asked; "" when it can.
func sampleReason(c *Counts, k targetKey) string {
	if slices.Contains(c.Unreachable, k.Node) {
		return "node " + k.Node + " did not answer the Plan"
	}
	if c.Answered != nil && !slices.Contains(c.Answered, k.Node) {
		return "node " + k.Node + " was not asked by the Plan: the liaison does not know it"
	}
	return ""
}

// shardRows sums the rows the node's shard of k counts over all its segments.
func shardRows(c *Counts, k targetKey) int64 {
	var n int64
	for _, rows := range c.Shards[k.countKey()] {
		n += rows
	}
	return n
}

// stableAt reports whether two reads count k the same, and both could count it; when k is
// missing, the node's whole shard must count the same too.
func stableAt(a, b *Counts, k targetKey) bool {
	na, ma, ra := sample(a, k)
	nb, mb, rb := sample(b, k)
	return ra == "" && rb == "" && na == nb && ma == mb && (ma == "" || shardRows(a, k) == shardRows(b, k))
}

// reconcilable tells whether an ambiguous payload of catalog and kind is reconciled from the
// counts: a stream or trace part, whose rows a merge keeps.
func reconcilable(catalog commonv1.Catalog, kind nativefmt.Kind) bool {
	return kind == nativefmt.KindPart && storesDuplicates(catalog)
}

// storesDuplicates tells whether rows of catalog pushed twice are stored twice: stream and trace
// rows are, a measure point or a property pushed again overwrites the one there.
func storesDuplicates(catalog commonv1.Catalog) bool {
	return catalog == commonv1.Catalog_CATALOG_STREAM || catalog == commonv1.Catalog_CATALOG_TRACE
}

// keyLocks serializes the work items that map to the same target (catalog, group, target
// stage, segment, shard), so within a run at most one part per target key is in flight.
type keyLocks struct {
	locks map[targetKey]*sync.Mutex
	mu    sync.Mutex
}

// lock locks k and returns its unlock.
func (l *keyLocks) lock(k targetKey) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[targetKey]*sync.Mutex{}
	}
	m := l.locks[k]
	if m == nil {
		m = &sync.Mutex{}
		l.locks[k] = m
	}
	l.mu.Unlock()
	m.Lock()
	return m.Unlock
}
