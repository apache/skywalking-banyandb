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
	"io"
	"maps"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

const (
	stream  = commonv1.Catalog_CATALOG_STREAM
	measure = commonv1.Catalog_CATALOG_MEASURE
)

// twoShardRoute is a route of two shards on two nodes with two copies each.
func twoShardRoute(group, stage string) map[RouteKey]*transferv1.TargetsResponse {
	return map[RouteKey]*transferv1.TargetsResponse{NewRouteKey(group, stage): {
		Group: group, Stage: stage, ShardNum: 2, Copies: 2,
		Shards: []*transferv1.TargetsResponse_Shard{{ShardId: 0, NodeIds: []string{"a", "b"}}, {ShardId: 1, NodeIds: []string{"b", "a"}}},
	}}
}

func emptyCounts() *Counts {
	return &Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}}
}

func (c *Counts) set(catalog commonv1.Catalog, group, stage, node string, shard uint32, segment string, n int64) *Counts {
	addCount(c.Shards, CountKey{Catalog: catalog, Group: group, Stage: stage, Node: node, Shard: shard}, segment, n)
	return c
}

func pushKey(catalog commonv1.Catalog, group, stage string, kind WorkItemKind) PushKey {
	return PushKey{streamKey: streamKey{catalog: catalog, group: group, stage: stage}, Kind: kind}
}

func pushedRows(catalog commonv1.Catalog, group, stage string, n uint64) map[PushKey]uint64 {
	return map[PushKey]uint64{pushKey(catalog, group, stage, ShardItem): n}
}

func TestVerifyStreamCountsEveryCopy(t *testing.T) {
	pre := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 100).set(stream, "g", "hot", "b", 0, "s1", 100)
	post := emptyCounts().
		set(stream, "g", "hot", "a", 0, "s1", 103).set(stream, "g", "hot", "b", 0, "s1", 103).
		set(stream, "g", "hot", "a", 1, "s2", 2).set(stream, "g", "hot", "b", 1, "s2", 2)
	r := evaluate(pre, post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 5))
	assert.False(t, r.Failed(), r.Mismatches)
	assert.False(t, r.pending)
}

func TestVerifyStreamMissingCopyIsAMismatch(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3)
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3))
	assert.True(t, r.Failed())
	assert.True(t, r.pending)
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "stream g/hot shard 0")
}

func TestVerifyStreamSumMustEqualPushedRows(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3).set(stream, "g", "hot", "b", 0, "s1", 3)
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 4))
	require.Len(t, r.Mismatches, 1)
	assert.Contains(t, r.Mismatches[0], "rows grew by 3 per copy, 4 were sent")
}

func TestVerifyStreamGrowthOnAForeignNodeIsAMismatch(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3).set(stream, "g", "hot", "b", 0, "s1", 3).set(stream, "g", "hot", "c", 0, "s1", 3)
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3))
	assert.True(t, r.Failed())
}

func TestVerifyLeavesOutSegmentsRemovedByRetention(t *testing.T) {
	pre := emptyCounts().set(stream, "g", "hot", "a", 0, "old", 50).set(stream, "g", "hot", "b", 0, "old", 50)
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "new", 2).set(stream, "g", "hot", "b", 0, "new", 2)
	r := evaluate(pre, post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 2))
	assert.False(t, r.Failed(), r.Mismatches)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "segment old")
}

func TestVerifyUnreachableTargetNodeIsIncomplete(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3).set(stream, "g", "hot", "b", 0, "s1", 3)
	post.Unreachable = []string{"b", "z"}
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3))
	require.Len(t, r.Incomplete, 1)
	assert.Contains(t, r.Incomplete[0], "target node b")
}

func TestVerifyMeasureBounds(t *testing.T) {
	routes := twoShardRoute("m", "warm")
	ok := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 4).set(measure, "m", "warm", "b", 0, "s", 3)
	r := evaluate(emptyCounts(), ok, routes, pushedRows(measure, "m", "warm", 4))
	assert.False(t, r.Failed(), r.Mismatches)
	assert.NotEmpty(t, r.measures)
	assert.Equal(t, int64(7), r.measureTotal)

	tooMany := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 5).set(measure, "m", "warm", "b", 0, "s", 5)
	r = evaluate(emptyCounts(), tooMany, routes, pushedRows(measure, "m", "warm", 4))
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "more than 4 sent × 2 copies")

	oneCopy := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 4)
	r = evaluate(emptyCounts(), oneCopy, routes, pushedRows(measure, "m", "warm", 4))
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "target node b did not grow")

	none := evaluate(emptyCounts(), emptyCounts(), routes, pushedRows(measure, "m", "warm", 4))
	assert.Contains(t, strings.Join(none.Mismatches, "\n"), "no node counts new rows")

	merged := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 1).set(measure, "m", "warm", "b", 0, "s", 1).set(measure, "m", "warm", "a", 1, "s", -2)
	r = evaluate(emptyCounts(), merged, routes, pushedRows(measure, "m", "warm", 4))
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "fewer rows")
}

func TestVerifyIndexModeAndPropertyAreNotCounted(t *testing.T) {
	routes := twoShardRoute("m", "hot")
	routes[NewRouteKey("p", "hot")] = &transferv1.TargetsResponse{Shards: []*transferv1.TargetsResponse_Shard{{NodeIds: []string{"a"}}}}
	post := emptyCounts()
	addCount(post.Segments, CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: "a"}, "s", 3)
	r := evaluate(emptyCounts(), post, routes, map[PushKey]uint64{
		pushKey(measure, "m", "hot", SegmentItem):                         10,
		pushKey(commonv1.Catalog_CATALOG_PROPERTY, "p", "", PropertyItem): 4,
	})
	assert.False(t, r.Failed())
	assert.Equal(t, []string{"index-mode measure m/hot: 10 row(s) sent, but node b reports no new segment documents"}, r.Warnings,
		"only the node of the index-mode measure that gained nothing is reported; the property is not counted")
}

func TestVerifyIndexModeReplayOntoHeldDocumentsIsNotAWarning(t *testing.T) {
	routes := twoShardRoute("m", "hot")
	pre := emptyCounts()
	for _, node := range []string{"a", "b"} {
		addCount(pre.Segments, CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: node}, "s", 3)
	}
	post := emptyCounts()
	for _, node := range []string{"a", "b"} {
		addCount(post.Segments, CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: node}, "s", 3)
	}
	r := evaluate(pre, post, routes, map[PushKey]uint64{pushKey(measure, "m", "hot", SegmentItem): 10})
	assert.False(t, r.Failed())
	assert.Empty(t, r.Warnings, "documents upserted onto the ones a node held gain nothing")
}

func TestVerifyReportRenderShowsOnlyTheConclusionAndTheProblems(t *testing.T) {
	var ok strings.Builder
	(&VerifyReport{}).Render(&ok)
	assert.Equal(t, "VERIFY    ok\n", ok.String())

	var warned strings.Builder
	(&VerifyReport{Warnings: []string{"w2", "w1", "w2"}}).Render(&warned)
	assert.Equal(t, "VERIFY    ok\n  - warning: w1\n  - warning: w2\n", warned.String())

	var failed strings.Builder
	(&VerifyReport{
		Incomplete: []string{"target node n1 did not answer the Plan"}, Mismatches: []string{"stream g/hot: rows grew by 1 per copy, 2 were sent"},
		Warnings: []string{"w"},
	}).Render(&failed)
	assert.Equal(t, "VERIFY    FAILED (exit 4)\n"+
		"  - not checked: target node n1 did not answer the Plan\n"+
		"  - stream g/hot: rows grew by 1 per copy, 2 were sent\n"+
		"  - warning: w\n", failed.String())
}

func TestCountsFromPlanMapsStages(t *testing.T) {
	result := &cluster.PlanResult{
		Frames: []*transferv1.UnitFrame{{NodeId: "n1", Stage: "Warm", Units: []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Segment{
			Segment: &transferv1.SegmentInventory{
				Unit:         &transferv1.SegmentUnit{Catalog: stream, Group: "g", SegmentSuffix: "20261016"},
				Shards:       []*transferv1.ShardStat{{ShardId: 1, TotalCount: 7}},
				SegmentLevel: &transferv1.SidxStat{DocCount: 2},
			},
		}}}}},
	}
	c := CountsFromPlan(result)
	assert.Equal(t, int64(7), c.Shards[CountKey{Catalog: stream, Group: "g", Stage: "warm", Node: "n1", Shard: 1}]["20261016"])
	assert.Equal(t, int64(2), c.Segments[CountKey{Catalog: stream, Group: "g", Stage: "warm", Node: "n1"}]["20261016"])
}

func TestVerifyMeasureWithoutGrowthKeepsPolling(t *testing.T) {
	routes := twoShardRoute("m", "hot")
	none := evaluate(emptyCounts(), emptyCounts(), routes, pushedRows(measure, "m", "hot", 4))
	assert.True(t, none.pending, "a flush may still make the rows visible")

	oneCopy := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 4)
	r := evaluate(emptyCounts(), oneCopy, routes, pushedRows(measure, "m", "hot", 4))
	assert.True(t, r.pending, "the other replica may still flush")

	tooMany := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 5).set(measure, "m", "hot", "b", 0, "s", 5)
	r = evaluate(emptyCounts(), tooMany, routes, pushedRows(measure, "m", "hot", 4))
	assert.True(t, r.Failed())
	assert.False(t, r.pending, "more flushes cannot reduce the count")
}

func TestVerifyMeasureReplayIntoExistingRowsOnlyWarns(t *testing.T) {
	routes := twoShardRoute("m", "hot")
	pre := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 4).set(measure, "m", "hot", "b", 0, "s", 4)
	same := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 4).set(measure, "m", "hot", "b", 0, "s", 4)
	r := evaluate(pre, same, routes, pushedRows(measure, "m", "hot", 4))
	assert.False(t, r.Failed(), r.Mismatches)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "no net growth")

	oneMerged := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 6).set(measure, "m", "hot", "b", 0, "s", 4)
	r = evaluate(pre, oneMerged, routes, pushedRows(measure, "m", "hot", 4))
	assert.False(t, r.Failed(), r.Mismatches)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "target node b did not grow, but already held 4 row(s)")
}

func TestVerifyNodeMissingFromTheBaselineIsIncomplete(t *testing.T) {
	pre := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 10)
	pre.Unreachable = []string{"b"}
	post := emptyCounts().
		set(stream, "g", "hot", "a", 0, "s1", 13).set(stream, "g", "hot", "b", 0, "s1", 113).
		set(measure, "m", "hot", "b", 0, "s1", 500)
	routes := twoShardRoute("g", "hot")
	maps.Copy(routes, twoShardRoute("m", "hot"))
	pushed := pushedRows(stream, "g", "hot", 3)
	pushed[pushKey(measure, "m", "hot", ShardItem)] = 4
	r := evaluate(pre, post, routes, pushed)
	require.Len(t, r.Incomplete, 1)
	assert.Contains(t, r.Incomplete[0], "target node b did not answer the baseline Plan")
	assert.Empty(t, r.Mismatches, "the node's existing rows must not read as growth")
	assert.True(t, r.noBaseline)
	assert.True(t, r.Failed())
}

// blockingPlan is an ExportService whose Plan never answers until its caller gives up.
type blockingPlan struct {
	transferv1.UnimplementedExportServiceServer
}

func (blockingPlan) Plan(_ *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestVerifyPollIsBoundedByTheTimeout(t *testing.T) {
	base, grace := verifyPollInterval, verifyPollGrace
	verifyPollInterval, verifyPollGrace = 10*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { verifyPollInterval, verifyPollGrace = base, grace })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	transferv1.RegisterExportServiceServer(srv, blockingPlan{})
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g", Catalog: stream}}}}
	start := time.Now()
	rep := Verify(context.Background(), conn, emptyCounts(), plan, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3),
		VerifyOptions{Timeout: 200 * time.Millisecond})
	assert.Less(t, time.Since(start), 5*time.Second, "a hung Plan must not hold Verify")
	require.NotEmpty(t, rep.Incomplete)
	assert.Contains(t, strings.Join(rep.Incomplete, "\n"), "the last Plan poll failed")
}

func TestVerifyWaitsForLateMeasureFlushes(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	f.nodes = []string{"a", "b"}
	li := f.serve(t)
	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "m", Catalog: measure}}}}
	//panicdiag:allow-rawgo test flush
	go func() {
		time.Sleep(150 * time.Millisecond)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.counts[CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: "a", Shard: 0}] = 4
		f.counts[CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: "b", Shard: 0}] = 4
	}()
	rep := Verify(context.Background(), li.Conn, emptyCounts(), plan, twoShardRoute("m", "hot"), pushedRows(measure, "m", "hot", 4),
		VerifyOptions{Timeout: 5 * time.Second})
	assert.False(t, rep.Failed(), "%v %v", rep.Incomplete, rep.Mismatches)
}

func TestVerifyMeasureSettlesWithinATimeoutBelowThePollInterval(t *testing.T) {
	base := verifyPollInterval
	verifyPollInterval = 2 * time.Second
	t.Cleanup(func() { verifyPollInterval = base })
	f := newFakeLiaison()
	f.nodes = []string{"a", "b"}
	f.counts[CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: "a", Shard: 0}] = 4
	f.counts[CountKey{Catalog: measure, Group: "m", Stage: "hot", Node: "b", Shard: 0}] = 4
	li := f.serve(t)
	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "m", Catalog: measure}}}}
	rep := Verify(context.Background(), li.Conn, emptyCounts(), plan, twoShardRoute("m", "hot"), pushedRows(measure, "m", "hot", 4),
		VerifyOptions{Timeout: 100 * time.Millisecond})
	assert.False(t, rep.Failed(), "%v %v", rep.Incomplete, rep.Mismatches)
}

func TestVerifyLogsAWaitingLineOnEveryPoll(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	f.nodes = []string{"a", "b"}
	li := f.serve(t)
	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g", Catalog: stream}}}}
	//panicdiag:allow-rawgo test flush
	go func() {
		time.Sleep(150 * time.Millisecond)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.counts[CountKey{Catalog: stream, Group: "g", Stage: "hot", Node: "a", Shard: 0}] = 3
		f.counts[CountKey{Catalog: stream, Group: "g", Stage: "hot", Node: "b", Shard: 0}] = 3
	}()
	var log strings.Builder
	rep := Verify(context.Background(), li.Conn, emptyCounts(), plan, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3),
		VerifyOptions{Timeout: 5 * time.Second, Log: &log})
	assert.False(t, rep.Failed(), "%v %v", rep.Incomplete, rep.Mismatches)
	lines := strings.Split(strings.TrimSpace(log.String()), "\n")
	require.NotEmpty(t, lines)
	line := regexp.MustCompile(`^verify: waiting for the target to flush \(\d+s elapsed, timeout 5s\): stream g/hot 0/3 rows; next poll in 0s$`)
	for _, l := range lines {
		assert.Regexp(t, line, l)
	}
}

func TestVerifyOnATerminalShowsALiveLineInsteadOfNotes(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	f.nodes = []string{"a", "b"}
	li := f.serve(t)
	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g", Catalog: stream}}}}
	//panicdiag:allow-rawgo test flush
	go func() {
		time.Sleep(150 * time.Millisecond)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.counts[CountKey{Catalog: stream, Group: "g", Stage: "hot", Node: "a", Shard: 0}] = 3
		f.counts[CountKey{Catalog: stream, Group: "g", Stage: "hot", Node: "b", Shard: 0}] = 3
	}()
	var out strings.Builder
	prog := output.NewProgress(&out, func() (int, int) { return 200, 50 }, "")
	rep := Verify(context.Background(), li.Conn, emptyCounts(), plan, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3),
		VerifyOptions{Timeout: 5 * time.Second, Log: prog.Wrap(&out), Progress: prog})
	prog.Close()
	assert.False(t, rep.Failed(), "%v %v", rep.Incomplete, rep.Mismatches)
	assert.Positive(t, rep.Elapsed)
	assert.NotContains(t, out.String(), "verify: waiting", "a terminal shows the live line, not a note per poll")
	assert.Regexp(t, `verify  \S+ / 5\.0s  waiting for the target to flush: stream g/hot 0/3 rows`, out.String())
}

func TestVerifyLineRendersStateAndTimes(t *testing.T) {
	prog := output.NewProgress(io.Discard, func() (int, int) { return 200, 50 }, "")
	start := time.Unix(1_800_000_000, 0)
	l := &verifyLine{start: start, p: prog, state: "waiting for the target to flush: stream g/hot 2/3 rows", timeout: 2 * time.Minute}
	assert.Regexp(t, `^\S verify  5\.0s / 2m0s  waiting for the target to flush: stream g/hot 2/3 rows$`, l.LiveLine(start.Add(5*time.Second)))
}

func TestVerifyReportRenderNamesHowLongItTook(t *testing.T) {
	var ok strings.Builder
	(&VerifyReport{Elapsed: 10 * time.Second}).Render(&ok)
	assert.Equal(t, "VERIFY    ok in 10.0s\n", ok.String())
	var failed strings.Builder
	(&VerifyReport{Elapsed: 10 * time.Second, Mismatches: []string{"m"}}).Render(&failed)
	assert.Equal(t, "VERIFY    FAILED in 10.0s (exit 4)\n  - m\n", failed.String())
}

func TestVerifyWaitingSummary(t *testing.T) {
	routes := twoShardRoute("g", "hot")
	maps.Copy(routes, twoShardRoute("m", "hot"))
	pushed := pushedRows(stream, "g", "hot", 3)
	pushed[pushKey(measure, "m", "hot", ShardItem)] = 4
	post := emptyCounts().
		set(stream, "g", "hot", "a", 0, "s1", 2).set(stream, "g", "hot", "b", 0, "s1", 2).
		set(measure, "m", "hot", "a", 0, "s1", 4)
	r := evaluate(emptyCounts(), post, routes, pushed)
	assert.Equal(t, "stream g/hot 2/3 rows, measure m/hot settling (4 rows counted)", r.waiting(false))
	assert.Equal(t, "stream g/hot 2/3 rows, measure m/hot 4 rows counted", r.waiting(true), "node b of shard 0 did not grow yet")

	post.set(stream, "g", "hot", "a", 1, "s1", 1).set(stream, "g", "hot", "b", 1, "s1", 1).set(measure, "m", "hot", "b", 0, "s1", 4)
	r = evaluate(emptyCounts(), post, routes, pushed)
	assert.Equal(t, "measure m/hot settling (8 rows counted)", r.waiting(false))
	assert.Equal(t, "counts not final", r.waiting(true))
}
