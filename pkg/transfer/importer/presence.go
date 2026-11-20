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
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// presenceExamples bounds the examples a refusal of the data check names.
const presenceExamples = 5

// countTarget counts the given groups on the target; a test replaces it.
var countTarget = Snapshot

// doneItem is a work item the progress file records rows of as landed: where they landed per
// target node, or only that some did (nodes nil).
type doneItem struct {
	entry  *Entry
	landed map[string]uint64
	rows   uint64
}

// checkPresence checks that the target still holds what the progress file at path records as
// imported, before a resumed push and before a finished import is reported as done: a target
// that was reset, or another cluster with the same node names, holds none of it. The schema
// comparison of first must not CREATE (or miss) any resource of a group the import wrote rows
// into, and one Plan of those groups must count rows wherever the progress file records rows as
// landed (missingData): a native stream or trace item per data node, shard and segment; any other
// shard item per shard and segment, summing the replicas, when its target stage has the source's
// shard number (rows keep their shard then), else per (group, stage), which log says; a property
// item's group must hold documents. Segments past the retention of their target stage are left
// out. A violation, and a target segment whose suffix does not parse as a time, exit
// exitcode.ExitResume; a place no answering node holds rows in while a node that may hold them
// did not answer exits exitcode.ExitRuntime (run again); nothing is written.
func checkPresence(ctx context.Context, log io.Writer, conn *grpc.ClientConn, art *Artifact, path string, state *progressState, first *decision,
	now time.Time,
) error {
	groups := recordedGroups(state, first.plan)
	if len(groups) == 0 {
		return nil
	}
	if lacking := lackingSchema(first.diff, groups); len(lacking) > 0 {
		return exitcode.Exit(exitcode.ExitResume, "the progress file %s records data this import wrote, but the target lacks %s; "+
			"the target looks reset or is another cluster, so nothing was written; %s", path, lackingList(lacking), otherTargetHint)
	}
	done := doneItems(state, first.plan, art.Native())
	if len(done) == 0 {
		return nil
	}
	counts, err := countTarget(ctx, conn, groupSelectors(groups))
	if err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "count the target to check the data the progress file %s records as imported: %w; nothing was written", path, err)
	}
	check, err := missingData(art, first.state, counts, done, now)
	if err != nil {
		return exitcode.Exit(exitcode.ExitResume, "%w; the progress file %s cannot be checked against the target, so nothing was written; %s",
			err, path, otherTargetHint)
	}
	for _, l := range check.coarse {
		notePresence(log, l)
	}
	for _, l := range check.sampled {
		notePresence(log, l)
	}
	if missing := check.missing; len(missing) > 0 {
		return exitcode.Exit(exitcode.ExitResume, "the progress file %s records data this import wrote that the target does not hold "+
			"(%d place(s)):\n  %s\nthe target looks reset or is another cluster, so nothing was written; %s",
			path, len(missing), examples(missing), otherTargetHint)
	}
	if unchecked := check.unchecked; len(unchecked) > 0 {
		return exitcode.Exit(exitcode.ExitRuntime, "could not check the data the progress file %s records as imported: node(s) %s did not answer the count, "+
			"and no node that answered holds it (%d place(s)):\n  %s\nnothing was written; run again once the node(s) are reachable",
			path, strings.Join(check.blind(), " "), len(unchecked), examples(unchecked))
	}
	return nil
}

// lackingList names up to presenceExamples of the resources a target lacks, saying how many more
// there are: a reset target lacks thousands.
func lackingList(lacking []string) string {
	shown := strings.Join(lacking[:min(len(lacking), presenceExamples)], ", ")
	if len(lacking) > presenceExamples {
		shown += fmt.Sprintf(" and %d more", len(lacking)-presenceExamples)
	}
	return shown
}

// examples renders up to presenceExamples of places, one per line, saying how many more there are.
func examples(places []string) string {
	shown := places[:min(len(places), presenceExamples)]
	more := ""
	if len(places) > len(shown) {
		more = fmt.Sprintf("\n  ... and %d more", len(places)-len(shown))
	}
	return strings.Join(shown, "\n  ") + more
}

// recordedGroups are the groups of the work items of plan (planned or refused now) the progress
// file records as complete or as holding a landed payload or batch. An item without rows, such as a
// segment-level series index, counts: its group's schema must be in the target all the same.
func recordedGroups(state *progressState, plan *Plan) map[string]commonv1.Catalog {
	byKey := knownItems(state, plan.Entries, plan.Rejected)
	groups := map[string]commonv1.Catalog{}
	for key, r := range state.Done {
		e := byKey[key]
		if r == nil || e == nil {
			continue
		}
		if r.Complete || len(landedNodes(r)) > 0 || (r.Cursor != nil && r.Cursor.Rows > 0) {
			groups[e.Item.Group] = e.Item.Catalog
		}
	}
	return groups
}

// doneItems are the work items of plan (planned or refused now) the progress file records rows of
// as landed, in key order, each at the target stage knownItems gives it. Of a native artifact
// only the rows of the parts recorded as landed count: an item complete without any, every part
// of it left out by skip, holds nothing to check. The exception is a complete segment item with
// rows, an index-mode one: its rows are the documents of its series index, which no part counts,
// and all of them count as landed.
func doneItems(state *progressState, plan *Plan, native bool) []doneItem {
	byKey := knownItems(state, plan.Entries, plan.Rejected)
	var out []doneItem
	for _, key := range slices.Sorted(maps.Keys(state.Done)) {
		r, e := state.Done[key], byKey[key]
		if r == nil || e == nil {
			continue
		}
		entry := *e
		d := doneItem{entry: &entry, landed: landedNodes(r)}
		switch {
		case len(d.landed) > 0:
			for _, n := range d.landed {
				d.rows += n
			}
		case r.Cursor != nil && r.Cursor.Rows > 0:
			d.rows = r.Cursor.Rows
		case !native && r.Complete && r.Parts == nil && r.Landed == nil:
			// A complete CSV item keeps no cursor: all its rows landed.
			d.rows = entry.Item.Rows()
		case native && r.Complete && entry.Item.Kind == SegmentItem:
			d.rows = entry.Item.Rows()
		}
		if d.rows > 0 {
			out = append(out, d)
		}
	}
	return out
}

// landedNodes are the rows of the native parts of r that landed, per target node: Landed of a
// complete item, else the parts that are done.
func landedNodes(r *itemRecord) map[string]uint64 {
	if len(r.Landed) > 0 {
		return r.Landed
	}
	out := map[string]uint64{}
	for _, p := range r.Parts {
		if p.Rows == 0 {
			continue
		}
		for _, n := range p.Nodes {
			out[n] += p.Rows
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// lackingSchema names the resources of groups the schema comparison would create, or the target
// lacks for an artifact without a schema.
func lackingSchema(diff *SchemaDiff, groups map[string]commonv1.Catalog) []string {
	if diff == nil {
		return nil
	}
	var out []string
	for i := range diff.Actions {
		a := &diff.Actions[i]
		if _, ok := groups[a.Group]; !ok {
			continue
		}
		if a.Disposition == DispositionCreate || a.Disposition == DispositionMissing {
			out = append(out, a.ID())
		}
	}
	return out
}

// presenceCheck is what missingData found: the places the target holds none of the rows the
// progress file records there; the places that hold none on the nodes that answered, while a
// node that may hold them did not (unchecked, with those nodes in blindNodes); the places of
// trace groups that sample spans in merges that hold none, which merges may have dropped
// (sampled); and the (catalog, group, stage)s checked as a whole although their items are shard
// items, since the target stage's shard number differs from the source's (coarse).
type presenceCheck struct {
	blindNodes map[string]bool
	missing    []string
	unchecked  []string
	sampled    []string
	coarse     []string
}

// blind names the nodes the unchecked places wait for, sorted.
func (c *presenceCheck) blind() []string {
	return slices.Sorted(maps.Keys(c.blindNodes))
}

// report files a place that holds none of the rows recorded there: unchecked when some of the
// nodes blind may hold them, else in the sampled notes for a trace group that samples spans in
// merges, else missing.
func (c *presenceCheck) report(place string, blind []string, sampled bool) {
	switch {
	case len(blind) > 0:
		if c.blindNodes == nil {
			c.blindNodes = map[string]bool{}
		}
		for _, n := range blind {
			c.blindNodes[n] = true
		}
		c.unchecked = append(c.unchecked, fmt.Sprintf("%s; not counted: %s", place, strings.Join(blind, " ")))
	case sampled:
		c.sampled = append(c.sampled, place+"; not refused: the group samples spans in merges, which may have dropped them")
	default:
		c.missing = append(c.missing, place)
	}
}

// missingData names every place the progress file records rows of done as landed that counts
// does not hold rows in. A native stream or trace item is checked per target node and shard;
// any other shard item per shard, summing the nodes, when the shard number of its target stage is
// the source's, else per (catalog, group, stage), as is a segment item; only the segments that
// overlap the item's rows and are not past the retention of the target stage count. A property
// item's group must hold some documents. An item all of whose rows are past the retention is not
// checked. A native item's node the count did not reach is checked per shard over the nodes it
// reached. A place no answering node holds rows in, while a node that may hold them (a route node
// of it, or a node the count reports unreachable) was not counted, is unchecked, not missing; a
// trace group that samples spans in merges is only noted. A target segment whose suffix does not
// parse as a time of the stage's interval is an error.
func missingData(art *Artifact, target *TargetState, counts *Counts, done []doneItem, now time.Time) (presenceCheck, error) {
	var out presenceCheck
	coarse := map[string]bool{}
	sampled := mergeSampledGroups(target.Groups)
	idx := indexCounts(counts)
	for _, d := range done {
		item := &d.entry.Item
		stage := normalizeStage(d.entry.TargetStage)
		route := routeOf(target.Routes, item.Group, stage)
		if item.Catalog == commonv1.Catalog_CATALOG_PROPERTY {
			if counts.Properties[item.Group] == 0 {
				out.report(fmt.Sprintf("property %s: %d row(s) imported (%s), the target holds no documents of the group", item.Group, d.rows, item.Key()),
					uncountedOf(counts, nil), false)
			}
			continue
		}
		w := segmentWindow(art, target, d, now)
		if w.expired {
			continue
		}
		samples := item.Catalog == commonv1.Catalog_CATALOG_TRACE && sampled[item.Group]
		perNode := art.Native() && item.Kind == ShardItem &&
			(item.Catalog == commonv1.Catalog_CATALOG_STREAM || item.Catalog == commonv1.Catalog_CATALOG_TRACE) &&
			len(d.landed) > 0
		if perNode {
			if err := out.checkNodes(idx, d, stage, route, w, samples); err != nil {
				return out, err
			}
			continue
		}
		if item.Kind == ShardItem {
			src, dst := shardNums(art, target, item, d.entry.TargetStage)
			if src != 0 && src == dst {
				held, err := idx.holdsShard(item, stage, w)
				if err != nil {
					return out, err
				}
				if !held {
					out.report(fmt.Sprintf("%s %s stage %s shard %d: %d row(s) imported (%s), no node of the target holds any in %s",
						item.CatalogName(), item.Group, stage, item.Shard, d.rows, item.Key(), w.describe()),
						uncountedOf(counts, route.targets(item.Shard)), samples)
				}
				continue
			}
			if label := fmt.Sprintf("%s %s stage %s", item.CatalogName(), item.Group, stage); !coarse[label] {
				coarse[label] = true
				why := fmt.Sprintf("its target stage has %d shard(s), the source stage %d, so the rows were routed to other shards", dst, src)
				if src == 0 || dst == 0 {
					why = "the shard number of its source or target stage is not known"
				}
				out.coarse = append(out.coarse, fmt.Sprintf("the data check of %s is per (group, stage), not per shard: %s", label, why))
			}
		}
		held, err := idx.holdsAny(item.Catalog, item.Group, stage, w)
		if err != nil {
			return out, err
		}
		if !held {
			out.report(fmt.Sprintf("%s %s stage %s: %d row(s) imported (%s), the target holds none in %s",
				item.CatalogName(), item.Group, stage, d.rows, item.Key(), w.describe()), uncountedOf(counts, route.nodes()), samples)
		}
	}
	return out, nil
}

// checkNodes checks a native stream or trace item on every node its rows landed on. A node the
// count did not reach (unreachable, or no longer in the cluster) is checked per shard over the
// nodes it reached instead: some must hold rows of the shard, else the place is unchecked when
// the node, or another that may hold the shard, is unreachable, and missing when it is gone.
func (c *presenceCheck) checkNodes(idx *countIndex, d doneItem, stage string, route routeView, w segmentFilter, samples bool) error {
	counts := idx.counts
	item := &d.entry.Item
	var uncounted []string
	for _, node := range slices.Sorted(maps.Keys(d.landed)) {
		if d.landed[node] == 0 {
			continue
		}
		if !counts.counted(node) {
			uncounted = append(uncounted, node)
			continue
		}
		k := CountKey{Catalog: item.Catalog, Group: item.Group, Stage: stage, Node: node, Shard: item.Shard}
		held, err := w.holds(counts.Shards[k], k)
		if err != nil {
			return err
		}
		if !held {
			c.report(fmt.Sprintf("%s %s stage %s shard %d on %s: %d row(s) imported (%s), the target holds none in %s",
				item.CatalogName(), item.Group, stage, item.Shard, node, d.landed[node], item.Key(), w.describe()), nil, samples)
		}
	}
	if len(uncounted) == 0 {
		return nil
	}
	held, err := idx.holdsShard(item, stage, w)
	if err != nil || held {
		return err
	}
	var rows uint64
	var blind []string
	for _, n := range uncounted {
		rows += d.landed[n]
		if slices.Contains(counts.Unreachable, n) {
			blind = append(blind, n)
		}
	}
	blind = transfer.UniqueSorted(append(blind, uncountedOf(counts, route.targets(item.Shard))...))
	c.report(fmt.Sprintf("%s %s stage %s shard %d: %d row(s) imported (%s) on %s, which the count did not reach, and no node it reached holds any in %s",
		item.CatalogName(), item.Group, stage, item.Shard, rows, item.Key(), strings.Join(uncounted, " "), w.describe()), blind, samples)
	return nil
}

// uncountedOf names the nodes of candidates the count did not reach; without candidates (no
// route tells which nodes may hold the rows), the nodes it reports unreachable.
func uncountedOf(counts *Counts, candidates []string) []string {
	if len(candidates) == 0 {
		return transfer.UniqueSorted(counts.Unreachable)
	}
	var out []string
	for _, n := range candidates {
		if !counts.counted(n) {
			out = append(out, n)
		}
	}
	return transfer.UniqueSorted(out)
}

// shardNums are the shard numbers of the source stage of item and of its target stage, 0 when
// not known.
func shardNums(art *Artifact, target *TargetState, item *WorkItem, targetStage string) (src, dst uint32) {
	if art != nil && art.Manifest != nil {
		if normalizeStage(item.Stage) == transfer.DefaultStageName {
			if st, ok := sourceGroupDefault(art.Manifest, item.Group); ok {
				src = st.ShardNum
			}
		} else if st, ok := sourceStage(art.Manifest, item.Group, item.Stage); ok {
			src = st.ShardNum
		}
	}
	ro := target.Groups[item.Group].GetResourceOpts()
	dst = ro.GetShardNum()
	if i := stageIndex(ro, targetStage); i >= 0 && !target.Standalone && normalizeStage(targetStage) != transfer.DefaultStageName {
		dst = ro.GetStages()[i].GetShardNum()
	}
	if r, ok := target.Routes[NewRouteKey(item.Group, targetStage)]; ok && r.GetShardNum() != 0 {
		dst = r.GetShardNum()
	}
	return src, dst
}

// countIndex buckets the keys of the shard and segment counts of counts by (catalog, group, stage),
// the shard keys by shard too, each bucket in compareCountKeys order: a done item looks up its
// place instead of going over every count.
type countIndex struct {
	counts   *Counts
	shards   map[streamKey][]CountKey
	segments map[streamKey][]CountKey
	byShard  map[shardPlace][]CountKey
}

// shardPlace is one shard of a (catalog, group, stage).
type shardPlace struct {
	streamKey
	shard uint32
}

func indexCounts(counts *Counts) *countIndex {
	idx := &countIndex{counts: counts, shards: map[streamKey][]CountKey{}, segments: map[streamKey][]CountKey{}, byShard: map[shardPlace][]CountKey{}}
	for _, k := range slices.SortedFunc(maps.Keys(counts.Shards), compareCountKeys) {
		sk := streamKey{catalog: k.Catalog, group: k.Group, stage: k.Stage}
		idx.shards[sk] = append(idx.shards[sk], k)
		place := shardPlace{streamKey: sk, shard: k.Shard}
		idx.byShard[place] = append(idx.byShard[place], k)
	}
	for _, k := range slices.SortedFunc(maps.Keys(counts.Segments), compareCountKeys) {
		sk := streamKey{catalog: k.Catalog, group: k.Group, stage: k.Stage}
		idx.segments[sk] = append(idx.segments[sk], k)
	}
	return idx
}

// holdsShard tells whether some counted node holds rows of the item's shard of (catalog, group,
// stage) in a segment w accepts: the replicas are summed, only to tell that the shard holds rows.
func (idx *countIndex) holdsShard(item *WorkItem, stage string, w segmentFilter) (bool, error) {
	sum := map[string]int64{}
	place := shardPlace{streamKey: streamKey{catalog: item.Catalog, group: item.Group, stage: stage}, shard: item.Shard}
	for _, k := range idx.byShard[place] {
		if !idx.counts.counted(k.Node) {
			continue
		}
		segs := idx.counts.Shards[k]
		if _, err := w.holds(segs, k); err != nil {
			return false, err
		}
		for suffix, n := range segs {
			sum[suffix] += n
		}
	}
	return w.holds(sum, CountKey{Catalog: item.Catalog, Group: item.Group, Stage: stage, Shard: item.Shard})
}

// holdsAny tells whether some node holds rows of (catalog, group, stage) in a segment w accepts,
// in its shards or its segment-level documents.
func (idx *countIndex) holdsAny(catalog commonv1.Catalog, group, stage string, w segmentFilter) (bool, error) {
	held := false
	sk := streamKey{catalog: catalog, group: group, stage: stage}
	for _, m := range []struct {
		counts map[CountKey]map[string]int64
		keys   []CountKey
	}{{idx.counts.Shards, idx.shards[sk]}, {idx.counts.Segments, idx.segments[sk]}} {
		for _, k := range m.keys {
			ok, err := w.holds(m.counts[k], k)
			if err != nil {
				return false, err
			}
			held = held || ok
		}
	}
	return held, nil
}

// compareCountKeys orders count keys, for a stable first error.
func compareCountKeys(a, b CountKey) int {
	return cmp.Or(cmp.Compare(a.Catalog, b.Catalog), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Stage, b.Stage),
		cmp.Compare(a.Node, b.Node), cmp.Compare(a.Shard, b.Shard))
}

// segmentFilter accepts the target segments that may hold an item's rows: those overlapping
// [from, to] and ending after deadline. A zero bound does not filter; without an interval
// every segment is accepted.
type segmentFilter struct {
	from, to, deadline time.Time
	loc                *time.Location
	interval           *commonv1.IntervalRule
	expired            bool
}

// segmentWindow is the filter of the target segments of a done item, and whether every row of it
// is past the retention of its target stage, by the TTL gate's rule.
func segmentWindow(art *Artifact, target *TargetState, d doneItem, now time.Time) segmentFilter {
	item := &d.entry.Item
	nodes := slices.Collect(maps.Keys(d.landed))
	w := segmentFilter{loc: gridLocation(Inputs{Artifact: art, Target: target}, item, nodes)}
	ro := target.Groups[item.Group].GetResourceOpts()
	stage := d.entry.TargetStage
	if target.Standalone {
		stage = ""
	}
	w.interval = stageSegmentInterval(ro, stage)
	newest, err := newestTime(item)
	if err == nil && newest != 0 {
		w.to = time.Unix(0, newest).In(w.loc)
	}
	if oldest := oldestTime(item); oldest != 0 {
		w.from = time.Unix(0, oldest).In(w.loc)
	}
	if ro.GetTtl() == nil {
		return w
	}
	ttl, err := CumulativeTTL(ro, stage)
	if err != nil {
		return w
	}
	w.deadline = now.Add(-ttl)
	if !w.to.IsZero() && targetSegmentEnd(w.to, w.interval).Before(w.deadline) {
		w.expired = true
	}
	return w
}

// oldestTime is the oldest row time of an item, 0 when not known.
func oldestTime(item *WorkItem) int64 {
	var oldest int64
	for _, s := range item.Sources {
		if s.MinTimestamp != 0 && (oldest == 0 || s.MinTimestamp < oldest) {
			oldest = s.MinTimestamp
		}
	}
	if oldest == 0 {
		oldest = item.RowsMinTimestamp
	}
	return oldest
}

// holds tells whether segs, rows by segment suffix, of the count key k has rows in a segment the
// filter accepts. A suffix that does not parse as a time of the interval is an error naming k: it
// cannot be placed on the grid, and matching it loosely could take another import's rows for this
// one's.
func (w segmentFilter) holds(segs map[string]int64, k CountKey) (bool, error) {
	held := false
	for _, suffix := range slices.Sorted(maps.Keys(segs)) {
		ok, err := w.accepts(suffix)
		if err != nil {
			where := fmt.Sprintf("%s group %s stage %s", transfer.CatalogName(k.Catalog), k.Group, k.Stage)
			if k.Node != "" {
				where += " on node " + k.Node
			}
			return false, fmt.Errorf("the target reports segment %q of %s, whose suffix is not a time of the stage's %s segment interval: %w",
				suffix, where, output.IntervalLabel(w.interval), err)
		}
		held = held || (ok && segs[suffix] > 0)
	}
	return held, nil
}

func (w segmentFilter) accepts(suffix string) (bool, error) {
	if w.interval.GetNum() == 0 {
		return true, nil
	}
	start, err := parseSegmentSuffix(suffix, w.interval.GetUnit(), w.loc)
	if err != nil {
		return false, err
	}
	end := targetSegmentEnd(start, w.interval)
	if !w.deadline.IsZero() && end.Before(w.deadline) {
		return false, nil
	}
	if !w.to.IsZero() && start.After(w.to) {
		return false, nil
	}
	return w.from.IsZero() || end.After(w.from), nil
}

// describe renders the segments the filter accepts.
func (w segmentFilter) describe() string {
	if w.from.IsZero() || w.to.IsZero() {
		return "any segment"
	}
	return fmt.Sprintf("the segments of %s .. %s", w.from.UTC().Format(time.RFC3339), w.to.UTC().Format(time.RFC3339))
}

// targetNames renders the liaisons and data nodes a progress file records.
func targetNames(b importBindings) string {
	nodes := transfer.UniqueSorted(b.TargetNodes)
	return fmt.Sprintf("liaisons %s, data nodes %s", cmp.Or(strings.Join(b.Liaisons, " "), "-"), cmp.Or(strings.Join(nodes, " "), "-"))
}
