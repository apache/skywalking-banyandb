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
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

var (
	// verifyPollInterval is the wait between two Plan polls: the default flush interval of
	// measure and property (5s), so two polls are at least a measure flush apart.
	verifyPollInterval = 5 * time.Second
	// verifyPollGrace is how long the last poll may run past --verify-timeout before it is cut off.
	verifyPollGrace = 5 * time.Second
)

// CountKey keys a count of the target: one node's shard of a (catalog, group, stage). Stage
// is the lower-case stage ("hot" for the default) and "" for property; Shard is 0 for the
// segment-level (index-mode) documents.
type CountKey struct {
	Group   string
	Stage   string
	Node    string
	Catalog commonv1.Catalog
	Shard   uint32
}

// Counts is what a Plan of the imported groups counted on the target.
type Counts struct {
	// Shards are the rows (total_count) of every segment, keyed by segment suffix.
	Shards map[CountKey]map[string]int64
	// Segments are the segment-level (index-mode) documents of every segment.
	Segments map[CountKey]map[string]int64
	// Unreachable are the nodes the Plan could not count.
	Unreachable []string
}

// CountsFromPlan folds a Plan's frames into Counts.
func CountsFromPlan(result *cluster.PlanResult) *Counts {
	c := &Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}}
	c.Unreachable = slices.Clone(result.UnreachableNodes)
	for _, f := range result.Frames {
		node := f.GetNodeId()
		stage := normalizeStage(f.GetStage())
		for _, u := range f.GetUnits() {
			if u.GetProperty() != nil {
				continue
			}
			seg := u.GetSegment()
			unit := seg.GetUnit()
			base := CountKey{Catalog: unit.GetCatalog(), Group: unit.GetGroup(), Stage: stage, Node: node}
			if sl := seg.GetSegmentLevel(); sl != nil {
				addCount(c.Segments, base, unit.GetSegmentSuffix(), int64(sl.GetDocCount()))
			}
			for _, s := range seg.GetShards() {
				k := base
				k.Shard = s.GetShardId()
				addCount(c.Shards, k, unit.GetSegmentSuffix(), int64(s.GetTotalCount()))
			}
		}
	}
	return c
}

func addCount[K comparable](m map[K]map[string]int64, k K, sub string, n int64) {
	if m[k] == nil {
		m[k] = map[string]int64{}
	}
	m[k][sub] += n
}

// Snapshot counts the given groups on the target with one Plan without a session.
func Snapshot(ctx context.Context, conn *grpc.ClientConn, selectors []*transferv1.Selector) (*Counts, error) {
	result, err := cluster.RunPlan(ctx, conn, selectors)
	if err != nil {
		return nil, err
	}
	return CountsFromPlan(result), nil
}

// VerifySelectors narrows a Plan to the groups the entries import.
func VerifySelectors(entries []Entry) []*transferv1.Selector {
	groups := map[string]commonv1.Catalog{}
	for _, e := range entries {
		groups[e.Item.Group] = e.Item.Catalog
	}
	return groupSelectors(groups)
}

// segmentDeltas sums post − pre over the segments present in post for every key; a segment
// pre has and post lacks was removed by retention and is left out on both sides, with a
// warning.
func segmentDeltas(pre, post map[CountKey]map[string]int64) (map[CountKey]int64, []string) {
	out := map[CountKey]int64{}
	for k, segs := range post {
		for seg, n := range segs {
			out[k] += n - pre[k][seg]
		}
	}
	var warns []string
	for k, segs := range pre {
		for seg := range segs {
			if _, ok := post[k][seg]; !ok {
				warns = append(warns, fmt.Sprintf("segment %s of %s %s/%s on %s shard %d disappeared (retention?); it is left out of the check",
					seg, transfer.CatalogName(k.Catalog), k.Group, k.Stage, k.Node, k.Shard))
			}
		}
	}
	return out, warns
}

// VerifyReport is the outcome of --verify: checks that could not complete and count mismatches,
// either of which fails the import (exit 4), and warnings.
type VerifyReport struct {
	Incomplete []string
	Mismatches []string
	Warnings   []string
	// progress describes, per (catalog, group, stage) not reconciled yet, how far its counts
	// are, for the waiting line of a poll.
	progress []string
	// measures are the measure (group, stage)s with the rows all their copies count.
	measures []measureProgress
	// Elapsed is how long verify took.
	Elapsed time.Duration
	// measureTotal sums the measure row deltas, to see them settle between two polls.
	measureTotal int64
	// pending marks mismatches a later poll may still resolve: stream/trace growth that does not
	// add up yet, and measure rows that no node, or not every target node, counts yet.
	pending bool
	// noBaseline marks a target node missing from the baseline Plan: no later poll can count it.
	noBaseline bool
}

type measureProgress struct {
	label   string
	counted int64
	pending bool
}

// Failed reports whether verification fails the import.
func (r *VerifyReport) Failed() bool { return len(r.Incomplete) > 0 || len(r.Mismatches) > 0 }

// evaluate applies the verification checks to one pair of snapshots, given where the import
// sent the rows (routes) and how many (pushed).
func evaluate(pre, post *Counts, routes map[RouteKey]*transferv1.TargetsResponse, pushed map[PushKey]uint64) *VerifyReport {
	r := &VerifyReport{}
	shardDelta, warns := segmentDeltas(pre.Shards, post.Shards)
	r.Warnings = append(r.Warnings, warns...)
	segDelta, warns := segmentDeltas(pre.Segments, post.Segments)
	r.Warnings = append(r.Warnings, warns...)

	rowsPushed := map[streamKey]uint64{}
	segPushed := map[streamKey]uint64{}
	routeNodes := map[string]bool{}
	for k, n := range pushed {
		rk := k.streamKey
		switch {
		case k.catalog == commonv1.Catalog_CATALOG_PROPERTY:
			// A property keeps each entity's latest revision, so its documents need not
			// match the revisions sent; it is not counted.
			rk.stage = transfer.DefaultStageName
		case k.Kind == SegmentItem && k.catalog == commonv1.Catalog_CATALOG_MEASURE:
			segPushed[rk] += n
		default:
			rowsPushed[rk] += n
		}
		for _, node := range routeOf(routes, rk.group, rk.stage).nodes() {
			routeNodes[node] = true
		}
	}
	// A node missing from the baseline has no pre counts, so all it holds would read as growth:
	// its (group, stage) checks are left out, the node is reported as not counted.
	noBaseline := map[string]bool{}
	for _, n := range transfer.UniqueSorted(slices.Concat(pre.Unreachable, post.Unreachable)) {
		if !routeNodes[n] {
			continue
		}
		if slices.Contains(pre.Unreachable, n) {
			noBaseline[n] = true
			r.noBaseline = true
			r.Incomplete = append(r.Incomplete, fmt.Sprintf("target node %s did not answer the baseline Plan, so its rows cannot be counted", n))
			continue
		}
		r.Incomplete = append(r.Incomplete, fmt.Sprintf("target node %s did not answer the Plan, so its rows cannot be counted", n))
	}
	for _, rk := range sortedRowKeys(rowsPushed) {
		route := routeOf(routes, rk.group, rk.stage)
		if slices.ContainsFunc(route.nodes(), func(n string) bool { return noBaseline[n] }) {
			continue
		}
		perShard := map[uint32]map[string]int64{}
		for k, d := range shardDelta {
			if k.Catalog == rk.catalog && k.Group == rk.group && k.Stage == rk.stage && d != 0 {
				addCount(perShard, k.Shard, k.Node, d)
			}
		}
		label := fmt.Sprintf("%s %s/%s", transfer.CatalogName(rk.catalog), rk.group, rk.stage)
		if rk.catalog == commonv1.Catalog_CATALOG_MEASURE {
			r.evaluateMeasure(label, rowsPushed[rk], route, perShard, preRows(pre.Shards, rk))
			continue
		}
		r.evaluateRows(label, rowsPushed[rk], route, perShard)
	}
	for _, rk := range sortedRowKeys(segPushed) {
		r.reportSegments(rk, segPushed[rk], routeOf(routes, rk.group, rk.stage), segDelta, pre.Segments)
	}
	return r
}

// reportSegments warns about a target node of an index-mode measure that gained no segment
// documents although rows were sent and it held none before; one document per series and
// segment, so no count is checked, and a node that held documents may have had the rows
// replayed onto them.
func (r *VerifyReport) reportSegments(rk streamKey, pushed uint64, route routeView, segDelta map[CountKey]int64, pre map[CountKey]map[string]int64) {
	if pushed == 0 {
		return
	}
	for _, n := range route.nodes() {
		k := CountKey{Catalog: rk.catalog, Group: rk.group, Stage: rk.stage, Node: n}
		var held int64
		for _, docs := range pre[k] {
			held += docs
		}
		if segDelta[k] == 0 && held == 0 {
			r.Warnings = append(r.Warnings, fmt.Sprintf("index-mode measure %s/%s: %d row(s) sent, but node %s reports no new segment documents",
				rk.group, rk.stage, pushed, n))
		}
	}
}

// evaluateRows checks a stream or trace (group, stage): every shard that grew grew on exactly
// its target nodes by the same amount, and the shards together grew by the rows sent.
func (r *VerifyReport) evaluateRows(label string, pushed uint64, route routeView, perShard map[uint32]map[string]int64) {
	var sum, counted int64
	mismatches := len(r.Mismatches)
	defer func() {
		if len(r.Mismatches) > mismatches {
			r.progress = append(r.progress, fmt.Sprintf("%s %d/%d rows", label, counted, pushed))
		}
	}()
	for _, shard := range slices.Sorted(maps.Keys(perShard)) {
		deltas := perShard[shard]
		targets := route.targets(shard)
		counted += minGrowth(deltas, targets)
		var want int64
		ok := len(targets) > 0
		for _, n := range targets {
			d := deltas[n]
			if d <= 0 || (want != 0 && d != want) {
				ok = false
			}
			if want == 0 {
				want = d
			}
		}
		for n, d := range deltas {
			if !slices.Contains(targets, n) || d < 0 {
				ok = false
			}
		}
		if !ok {
			r.pending = true
			r.Mismatches = append(r.Mismatches, fmt.Sprintf("%s shard %d: rows grew by %s, expected the same growth on exactly [%s]",
				label, shard, formatDeltas(deltas), strings.Join(targets, " ")))
			continue
		}
		sum += want
	}
	if sum != int64(pushed) {
		r.pending = true
		r.Mismatches = append(r.Mismatches, fmt.Sprintf("%s: rows grew by %d per copy, %d were sent", label, sum, pushed))
		return
	}
}

// minGrowth is the growth a shard shows on every one of its target nodes.
func minGrowth(deltas map[string]int64, targets []string) int64 {
	if len(targets) == 0 {
		return 0
	}
	least := deltas[targets[0]]
	for _, n := range targets[1:] {
		least = min(least, deltas[n])
	}
	return max(least, 0)
}

// preRows sums the baseline rows of a (catalog, group, stage) per shard and node.
func preRows(pre map[CountKey]map[string]int64, rk streamKey) map[uint32]map[string]int64 {
	out := map[uint32]map[string]int64{}
	for k, segs := range pre {
		if k.Catalog != rk.catalog || k.Group != rk.group || k.Stage != rk.stage {
			continue
		}
		for _, n := range segs {
			addCount(out, k.Shard, k.Node, n)
		}
	}
	return out
}

// evaluateMeasure checks a measure (group, stage) once its counts settled: rows sent show up,
// no more than rows × copies appear, and a shard that grew grew on every target node. Where
// the target already held rows, no growth is only a warning: replayed points merge into the
// ones already there.
func (r *VerifyReport) evaluateMeasure(label string, pushed uint64, route routeView, perShard, before map[uint32]map[string]int64) {
	var total int64
	mismatches := len(r.Mismatches)
	defer func() {
		r.measures = append(r.measures, measureProgress{label: label, counted: total, pending: len(r.Mismatches) > mismatches})
	}()
	copies := 1
	for _, shard := range slices.Sorted(maps.Keys(perShard)) {
		deltas := perShard[shard]
		targets := route.targets(shard)
		copies = max(copies, len(targets))
		grew := false
		for n, d := range deltas {
			total += d
			if d < 0 {
				r.Warnings = append(r.Warnings, fmt.Sprintf("%s shard %d: node %s counts %d fewer rows (merged duplicates)", label, shard, n, -d))
			}
			if d > 0 {
				grew = true
			}
		}
		if !grew {
			continue
		}
		for _, n := range targets {
			if deltas[n] > 0 {
				continue
			}
			if held := before[shard][n]; held > 0 {
				r.Warnings = append(r.Warnings, fmt.Sprintf("%s shard %d: target node %s did not grow, but already held %d row(s) (replayed points merged into existing ones)",
					label, shard, n, held))
				continue
			}
			r.pending = true
			r.Mismatches = append(r.Mismatches, fmt.Sprintf("%s shard %d: target node %s did not grow (%s)", label, shard, n, formatDeltas(deltas)))
		}
	}
	r.measureTotal += total
	var held int64
	for _, nodes := range before {
		for _, n := range nodes {
			held += n
		}
	}
	switch {
	case pushed > 0 && total <= 0 && held > 0:
		r.Warnings = append(r.Warnings, fmt.Sprintf("%s: %d row(s) sent, no net growth; the target already held %d row(s), so the points may have been replays that merged",
			label, pushed, held))
	case pushed > 0 && total <= 0:
		r.pending = true
		r.Mismatches = append(r.Mismatches, fmt.Sprintf("%s: %d row(s) sent, but no node counts new rows", label, pushed))
	case total > int64(pushed)*int64(copies):
		r.Mismatches = append(r.Mismatches, fmt.Sprintf("%s: nodes count %d new rows, more than %d sent × %d copies", label, total, pushed, copies))
	}
}

func formatDeltas(deltas map[string]int64) string {
	parts := make([]string, 0, len(deltas))
	for _, n := range slices.Sorted(maps.Keys(deltas)) {
		parts = append(parts, fmt.Sprintf("%s=%+d", n, deltas[n]))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func sortedRowKeys(m map[streamKey]uint64) []streamKey {
	return slices.SortedFunc(maps.Keys(m), func(a, b streamKey) int {
		return cmp.Or(cmp.Compare(transfer.CatalogName(a.catalog), transfer.CatalogName(b.catalog)), cmp.Compare(a.group, b.group), cmp.Compare(a.stage, b.stage))
	})
}

// VerifyOptions configure Verify.
type VerifyOptions struct {
	// Log receives the poll notes and the report.
	Log io.Writer
	// Progress, on a terminal, shows what verify waits for in a live line instead of a note
	// per poll.
	Progress *output.Progress
	// Timeout is --verify-timeout.
	Timeout time.Duration
}

// verifyLine is the live line of a verify in progress.
type verifyLine struct {
	start   time.Time
	p       *output.Progress
	state   string
	timeout time.Duration
}

// LiveLine renders what verify waits for. The caller holds the progress locked.
func (l *verifyLine) LiveLine(now time.Time) string {
	// The times come first: a long state is cut at the terminal's width.
	return fmt.Sprintf("%s verify  %s / %s  %s", l.p.Spinner(), output.FormatElapsed(now.Sub(l.start)), output.FormatElapsed(l.timeout), l.state)
}

// Verify polls a Plan of the imported groups until the counts reconcile with what was sent
// or the timeout elapses, then reports in three categories. Stream and trace
// shards must grow by the same amount on exactly their target nodes and sum to the rows
// sent; measures must first settle across two polls at least a flush interval apart, then
// grow, by no more than rows × copies, on every target node of a shard that grew; index-mode
// measures and properties are only reported. Polls are verifyPollInterval (the 5s flush
// interval) apart, and every poll that does not end the check logs a waiting line. The target
// must not take other writes meanwhile.
func Verify(ctx context.Context, conn *grpc.ClientConn, pre *Counts, plan *Plan, routes map[RouteKey]*transferv1.TargetsResponse,
	pushed map[PushKey]uint64, opts VerifyOptions,
) *VerifyReport {
	if opts.Log == nil {
		opts.Log = io.Discard
	}
	selectors := VerifySelectors(plan.Entries)
	start := time.Now()
	deadline := start.Add(opts.Timeout)
	var line *verifyLine
	if opts.Progress != nil && opts.Progress.Terminal() {
		line = &verifyLine{start: start, p: opts.Progress, state: "waiting for the target to flush", timeout: opts.Timeout}
		opts.Progress.Add(line)
		defer opts.Progress.Remove(line, "")
	}
	report := func(rep *VerifyReport) *VerifyReport {
		rep.Elapsed = time.Since(start)
		return rep
	}
	var last *VerifyReport
	var lastErr error
	var prevTotal int64
	polls := 0
	// A measure settles only on a second poll with the same total, so the first poll comes
	// at most halfway to the deadline: a timeout at or below the poll interval still fits two.
	nextWait := func() time.Duration {
		wait := min(verifyPollInterval, max(time.Until(deadline), 0))
		if polls == 0 {
			wait = min(wait, opts.Timeout/2)
		}
		return wait
	}
	for {
		timer := time.NewTimer(nextWait())
		select {
		case <-ctx.Done():
			timer.Stop()
			return report(finishReport(last, lastErr, fmt.Errorf("interrupted: %w", context.Cause(ctx))))
		case <-timer.C:
		}
		// A Plan that hangs must not hold the import past --verify-timeout (plus a grace).
		pollCtx, cancel := context.WithDeadline(ctx, deadline.Add(verifyPollGrace))
		post, err := Snapshot(pollCtx, conn, selectors)
		cancel()
		expired := !time.Now().Before(deadline)
		if err != nil {
			lastErr = err
			fmt.Fprintf(opts.Log, "verify: Plan failed: %v\n", err)
			if expired {
				return report(finishReport(last, lastErr, nil))
			}
			continue
		}
		lastErr = nil
		rep := evaluate(pre, post, routes, pushed)
		settled := len(rep.measures) == 0 || (polls > 0 && rep.measureTotal == prevTotal)
		prevTotal = rep.measureTotal
		polls++
		last = rep
		// A node missing from the baseline stays uncounted, so there is nothing to wait for.
		if (!rep.pending && settled && len(rep.Incomplete) == 0) || expired || rep.noBaseline {
			if !settled && !rep.noBaseline {
				rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("measure row counts did not settle within --verify-timeout %s", opts.Timeout))
			}
			return report(finishReport(rep, nil, nil))
		}
		if line != nil {
			state := "waiting for the target to flush: " + rep.waiting(settled)
			opts.Progress.Update(func() string {
				line.state = state
				return ""
			})
			opts.Progress.Redraw()
			continue
		}
		fmt.Fprintf(opts.Log, "verify: waiting for the target to flush (%s elapsed, timeout %s): %s; next poll in %s\n",
			time.Since(start).Round(time.Second), opts.Timeout, rep.waiting(settled),
			nextWait().Round(time.Second))
	}
}

// waiting summarizes what a poll still waits for: the (catalog, group, stage)s not reconciled
// yet, the measures whose counts have not settled, and the nodes that did not answer.
func (r *VerifyReport) waiting(settled bool) string {
	parts := slices.Clone(r.progress)
	for _, m := range r.measures {
		if !settled {
			parts = append(parts, fmt.Sprintf("%s settling (%d rows counted)", m.label, m.counted))
			continue
		}
		if m.pending {
			parts = append(parts, fmt.Sprintf("%s %d rows counted", m.label, m.counted))
		}
	}
	parts = append(parts, r.Incomplete...)
	if len(parts) == 0 {
		return "counts not final"
	}
	return strings.Join(parts, ", ")
}

// finishReport completes a report with any failure to poll.
func finishReport(rep *VerifyReport, pollErr, stopErr error) *VerifyReport {
	if rep == nil {
		rep = &VerifyReport{}
	}
	if pollErr != nil {
		rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("the last Plan poll failed: %v", pollErr))
	}
	if stopErr != nil {
		rep.Incomplete = append(rep.Incomplete, stopErr.Error())
	}
	return rep
}

// Render writes the conclusion of the report and every problem it found.
func (r *VerifyReport) Render(w io.Writer) {
	took := ""
	if r.Elapsed > 0 {
		took = " in " + output.FormatElapsed(r.Elapsed)
	}
	if r.Failed() {
		fmt.Fprintf(w, "VERIFY    FAILED%s (exit %d)\n", took, exitcode.ExitVerify)
	} else {
		fmt.Fprintf(w, "VERIFY    ok%s\n", took)
	}
	for _, l := range r.Incomplete {
		fmt.Fprintf(w, "  - not checked: %s\n", l)
	}
	for _, l := range r.Mismatches {
		fmt.Fprintf(w, "  - %s\n", l)
	}
	for _, l := range transfer.UniqueSorted(r.Warnings) {
		fmt.Fprintf(w, "  - warning: %s\n", l)
	}
}
