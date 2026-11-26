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
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
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

// verifyingImport is the data fixture's import with --verify and fast polls.
func verifyingImport(t *testing.T) config.Import {
	t.Helper()
	fastPolls(t)
	opts := defaultImport(t, writeDataArtifact(t))
	opts.Verify = true
	opts.VerifyTimeout = 300 * time.Millisecond
	return opts
}

// runExplicit runs an import whose --verify was given on the command line.
func runExplicit(t *testing.T, f *fakeLiaison, li *cluster.Liaison, opts config.Import) importRun {
	t.Helper()
	var out, log bytes.Buffer
	res, err := RunImport(context.Background(), Env{
		Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
		Now: func() time.Time { return dataNow }, VerifyExplicit: true,
	}, opts)
	return importRun{res: res, err: err, out: out.String(), log: log.String()}
}

// routeTo routes every shard of targets to its nodes.
func routeTo(targets map[uint32][]string) routeView {
	resp := &transferv1.TargetsResponse{}
	for shard, nodes := range targets {
		resp.Shards = append(resp.Shards, &transferv1.TargetsResponse_Shard{ShardId: shard, NodeIds: nodes})
	}
	return routeView{r: resp}
}

func TestVerifyStreamCountsEveryCopy(t *testing.T) {
	pre := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 100).set(stream, "g", "hot", "b", 0, "s1", 100)
	post := emptyCounts().
		set(stream, "g", "hot", "a", 0, "s1", 103).set(stream, "g", "hot", "b", 0, "s1", 103).
		set(stream, "g", "hot", "a", 1, "s2", 2).set(stream, "g", "hot", "b", 1, "s2", 2)
	r := evaluate(pre, post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 5), nil, nil)
	assert.False(t, r.Failed(), r.Mismatches)
	assert.False(t, r.pending)
}

func TestVerifyStreamMissingCopyIsAMismatch(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3)
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3), nil, nil)
	assert.True(t, r.Failed())
	assert.True(t, r.pending)
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "stream g/hot shard 0")
}

func TestVerifyStreamSumMustEqualPushedRows(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3).set(stream, "g", "hot", "b", 0, "s1", 3)
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 4), nil, nil)
	require.Len(t, r.Mismatches, 1)
	assert.Contains(t, r.Mismatches[0], "rows grew by 3 per copy, 4 were sent")
}

func TestVerifyStreamGrowthOnAForeignNodeIsAMismatch(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3).set(stream, "g", "hot", "b", 0, "s1", 3).set(stream, "g", "hot", "c", 0, "s1", 3)
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3), nil, nil)
	assert.True(t, r.Failed())
}

func TestVerifyLeavesOutSegmentsRemovedByRetention(t *testing.T) {
	pre := emptyCounts().set(stream, "g", "hot", "a", 0, "old", 50).set(stream, "g", "hot", "b", 0, "old", 50)
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "new", 2).set(stream, "g", "hot", "b", 0, "new", 2)
	r := evaluate(pre, post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 2), nil, nil)
	assert.False(t, r.Failed(), r.Mismatches)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "segment old")
}

func TestVerifyUnreachableTargetNodeIsIncomplete(t *testing.T) {
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s1", 3).set(stream, "g", "hot", "b", 0, "s1", 3)
	post.Unreachable = []string{"b", "z"}
	r := evaluate(emptyCounts(), post, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3), nil, nil)
	require.Len(t, r.Incomplete, 1)
	assert.Contains(t, r.Incomplete[0], "target node b")
}

func TestVerifyMeasureBounds(t *testing.T) {
	routes := twoShardRoute("m", "warm")
	ok := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 4).set(measure, "m", "warm", "b", 0, "s", 3)
	r := evaluate(emptyCounts(), ok, routes, pushedRows(measure, "m", "warm", 4), nil, nil)
	assert.False(t, r.Failed(), r.Mismatches)
	assert.NotEmpty(t, r.measures)
	assert.Equal(t, int64(7), r.measureTotal)

	tooMany := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 5).set(measure, "m", "warm", "b", 0, "s", 5)
	r = evaluate(emptyCounts(), tooMany, routes, pushedRows(measure, "m", "warm", 4), nil, nil)
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "more than 4 sent × 2 copies")

	oneCopy := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 4)
	r = evaluate(emptyCounts(), oneCopy, routes, pushedRows(measure, "m", "warm", 4), nil, nil)
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "target node b did not grow")

	none := evaluate(emptyCounts(), emptyCounts(), routes, pushedRows(measure, "m", "warm", 4), nil, nil)
	assert.Contains(t, strings.Join(none.Mismatches, "\n"), "no node counts new rows")

	merged := emptyCounts().set(measure, "m", "warm", "a", 0, "s", 1).set(measure, "m", "warm", "b", 0, "s", 1).set(measure, "m", "warm", "a", 1, "s", -2)
	r = evaluate(emptyCounts(), merged, routes, pushedRows(measure, "m", "warm", 4), nil, nil)
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
	}, nil, nil)
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
	r := evaluate(pre, post, routes, map[PushKey]uint64{pushKey(measure, "m", "hot", SegmentItem): 10}, nil, nil)
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
	none := evaluate(emptyCounts(), emptyCounts(), routes, pushedRows(measure, "m", "hot", 4), nil, nil)
	assert.True(t, none.pending, "a flush may still make the rows visible")

	oneCopy := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 4)
	r := evaluate(emptyCounts(), oneCopy, routes, pushedRows(measure, "m", "hot", 4), nil, nil)
	assert.True(t, r.pending, "the other replica may still flush")

	tooMany := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 5).set(measure, "m", "hot", "b", 0, "s", 5)
	r = evaluate(emptyCounts(), tooMany, routes, pushedRows(measure, "m", "hot", 4), nil, nil)
	assert.True(t, r.Failed())
	assert.False(t, r.pending, "more flushes cannot reduce the count")
}

func TestVerifyMeasureReplayIntoExistingRowsOnlyWarns(t *testing.T) {
	routes := twoShardRoute("m", "hot")
	pre := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 4).set(measure, "m", "hot", "b", 0, "s", 4)
	same := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 4).set(measure, "m", "hot", "b", 0, "s", 4)
	r := evaluate(pre, same, routes, pushedRows(measure, "m", "hot", 4), nil, nil)
	assert.False(t, r.Failed(), r.Mismatches)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "no net growth")

	oneMerged := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 6).set(measure, "m", "hot", "b", 0, "s", 4)
	r = evaluate(pre, oneMerged, routes, pushedRows(measure, "m", "hot", 4), nil, nil)
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
	r := evaluate(pre, post, routes, pushed, nil, nil)
	require.Len(t, r.Incomplete, 1)
	assert.Contains(t, r.Incomplete[0], "target node b was not counted by the baseline Plan")
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
	r := evaluate(emptyCounts(), post, routes, pushed, nil, nil)
	assert.Equal(t, "stream g/hot 2/3 rows, measure m/hot settling (4 rows counted)", r.waiting(false))
	assert.Equal(t, "stream g/hot 2/3 rows, measure m/hot 4 rows counted", r.waiting(true), "node b of shard 0 did not grow yet")

	post.set(stream, "g", "hot", "a", 1, "s1", 1).set(stream, "g", "hot", "b", 1, "s1", 1).set(measure, "m", "hot", "b", 0, "s1", 4)
	r = evaluate(emptyCounts(), post, routes, pushed, nil, nil)
	assert.Equal(t, "measure m/hot settling (8 rows counted)", r.waiting(false))
	assert.Equal(t, "counts not final", r.waiting(true))
}

func TestVerifyReportsAMergeSampledTraceGroupAsNotChecked(t *testing.T) {
	fastPolls(t)
	trace := commonv1.Catalog_CATALOG_TRACE
	f := newFakeLiaison()
	f.nodes = []string{"a", "b"}
	// The sampler dropped spans: the trace group holds fewer rows than were sent. The stream
	// group next to it is complete.
	f.counts[CountKey{Catalog: trace, Group: "tg", Stage: "hot", Node: "a", Shard: 0}] = 1
	f.counts[CountKey{Catalog: trace, Group: "tg", Stage: "hot", Node: "b", Shard: 0}] = 1
	f.counts[CountKey{Catalog: stream, Group: "g", Stage: "hot", Node: "a", Shard: 0}] = 3
	f.counts[CountKey{Catalog: stream, Group: "g", Stage: "hot", Node: "b", Shard: 0}] = 3
	li := f.serve(t)
	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "tg", Catalog: trace}}, {Item: WorkItem{Group: "g", Catalog: stream}}}}
	routes := twoShardRoute("g", "hot")
	maps.Copy(routes, twoShardRoute("tg", "hot"))
	pushed := pushedRows(stream, "g", "hot", 3)
	maps.Copy(pushed, pushedRows(trace, "tg", "hot", 3))

	compared := Verify(context.Background(), li.Conn, emptyCounts(), plan, routes, pushed, VerifyOptions{Timeout: 200 * time.Millisecond})
	require.True(t, compared.Failed(), "without the sampler the dropped spans are a mismatch")

	rep := Verify(context.Background(), li.Conn, emptyCounts(), plan, routes, pushed,
		VerifyOptions{Timeout: 5 * time.Second, Sampled: map[string]bool{"tg": true}})
	assert.False(t, rep.Failed(), "%v %v", rep.Incomplete, rep.Mismatches)
	require.Len(t, rep.Partial, 1)
	assert.Contains(t, rep.Partial[0], "trace tg/hot checked partially")
	var out strings.Builder
	rep.Render(&out)
	assert.Regexp(t, `^VERIFY    ok \(1 group\(s\) not fully checked\) in \S+\n  - trace tg/hot checked partially: `, out.String())
}

func TestVerifyMeasureToleratesPointsPushedAgain(t *testing.T) {
	routes := twoShardRoute("m", "hot")
	tol := map[streamKey]*verifyTolerance{{catalog: measure, group: "m", stage: "hot"}: {rows: 2, payloads: []string{"m rows after f record 2"}}}
	// 4 points sent, 2 of them sent again: until a merge, both versions are counted.
	twice := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 6).set(measure, "m", "hot", "b", 0, "s", 6)
	r := evaluate(emptyCounts(), twice, routes, pushedRows(measure, "m", "hot", 4), tol, nil)
	assert.False(t, r.Failed(), r.Mismatches)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "within the 2 row(s) the resumed import pushed again, which are counted twice until a merge: m rows after f record 2")

	beyond := emptyCounts().set(measure, "m", "hot", "a", 0, "s", 7).set(measure, "m", "hot", "b", 0, "s", 7)
	r = evaluate(emptyCounts(), beyond, routes, pushedRows(measure, "m", "hot", 4), tol, nil)
	assert.Contains(t, strings.Join(r.Mismatches, "\n"), "more than 4 sent × 2 copies")
}

// verify counts a node only where the Plan says it answered.
func TestVerify_NodesThePlanDidNotCount(t *testing.T) {
	routes := twoShardRoute("g", "hot")
	pushed := pushedRows(stream, "g", "hot", 3)
	pre := emptyCounts()
	pre.Answered = []string{"a"}
	post := emptyCounts().set(stream, "g", "hot", "a", 0, "s", 3).set(stream, "g", "hot", "b", 0, "s", 3)
	r := evaluate(pre, post, routes, pushed, nil, nil)
	require.True(t, r.noBaseline)
	assert.Contains(t, strings.Join(r.Incomplete, "\n"), "target node b was not counted by the baseline Plan")

	pre = emptyCounts().set(stream, "g", "hot", "b", 0, "s", 5)
	post = emptyCounts().set(stream, "g", "hot", "a", 0, "s", 3)
	post.Answered = []string{"a"}
	r = evaluate(pre, post, routes, pushed, nil, nil)
	assert.Contains(t, strings.Join(r.Incomplete, "\n"), "target node b did not answer the Plan")
	assert.NotContains(t, strings.Join(r.Warnings, "\n"), "disappeared", "a node not counted lost no segment")

	data, err := json.Marshal(recordCounts(&Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}, Answered: []string{}}))
	require.NoError(t, err)
	var back recordedCounts
	require.NoError(t, json.Unmarshal(data, &back))
	c, err := back.counts()
	require.NoError(t, err)
	assert.NotNil(t, c.Answered, "no node answered is kept apart from not known")
}

// A trace group sampling spans in merges: a merge may drop spans the target held, so a node may
// shrink; only growth on the target nodes counts, every shard is checked in full, and no growth
// with a shrinking node or a segment gone is not a mismatch.
func TestVerifyPartial_ShrinkAndGone(t *testing.T) {
	routes := twoShardRoute("tg", "hot")
	r := &VerifyReport{}
	r.evaluatePartial("trace tg/hot", 3, routeOf(routes, "tg", "hot"), map[uint32]map[string]int64{0: {"a": 2, "b": -5}}, nil, false)
	assert.Empty(t, r.Mismatches, "growth on a target node with another shrinking is growth")

	r = &VerifyReport{}
	r.evaluatePartial("trace tg/hot", 3, routeOf(routes, "tg", "hot"), map[uint32]map[string]int64{0: {"a": -1, "b": -1}}, nil, false)
	assert.Empty(t, r.Mismatches)
	require.Len(t, r.Warnings, 1)
	assert.Contains(t, r.Warnings[0], "merges dropped spans the target held")

	r = &VerifyReport{}
	r.evaluatePartial("trace tg/hot", 3, routeOf(routes, "tg", "hot"), map[uint32]map[string]int64{}, nil, true)
	assert.Empty(t, r.Mismatches)
	assert.NotEmpty(t, r.Warnings)

	// A node outside the targets that shrank is no growth there; one that grew is, whatever the
	// order the deltas are read in.
	off := map[RouteKey]*transferv1.TargetsResponse{NewRouteKey("tg", "hot"): {
		Group: "tg", Stage: "hot", ShardNum: 1, Copies: 1, Shards: []*transferv1.TargetsResponse_Shard{{ShardId: 0, NodeIds: []string{"a"}}},
	}}
	for range 20 {
		r = &VerifyReport{}
		r.evaluatePartial("trace tg/hot", 3, routeOf(off, "tg", "hot"), map[uint32]map[string]int64{0: {"a": 2, "b": -1, "c": 1, "d": 1}}, nil, false)
		require.Len(t, r.Mismatches, 1, "one line per shard")
		assert.Contains(t, r.Mismatches[0], "but only [a] are its target nodes")
	}
}

func TestVerifyPartial_MergeSampledTraceGroup(t *testing.T) {
	trace := commonv1.Catalog_CATALOG_TRACE
	routes := twoShardRoute("tg", "hot")
	pushed := pushedRows(trace, "tg", "hot", 3)
	sampled := map[string]bool{"tg": true}
	for _, tc := range []struct {
		post     *Counts
		name     string
		mismatch string
	}{
		{name: "fewer rows than sent", post: emptyCounts().set(trace, "tg", "hot", "a", 0, "s", 1).set(trace, "tg", "hot", "b", 0, "s", 1)},
		{name: "one copy grows", post: emptyCounts().set(trace, "tg", "hot", "a", 0, "s", 1)},
		{name: "no growth", post: emptyCounts(), mismatch: "3 row(s) sent, but no target node counts new rows"},
		{
			name: "more than sent × copies", post: emptyCounts().set(trace, "tg", "hot", "a", 0, "s", 4).set(trace, "tg", "hot", "b", 0, "s", 4),
			mismatch: "nodes count 8 new rows, more than 3 sent × 2 copies",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := evaluate(emptyCounts(), tc.post, routes, pushed, nil, sampled)
			require.Len(t, r.Partial, 1)
			if tc.mismatch == "" {
				assert.False(t, r.Failed(), "%v", r.Mismatches)
				return
			}
			require.Len(t, r.Mismatches, 1)
			assert.Contains(t, r.Mismatches[0], tc.mismatch)
		})
	}
	off := map[RouteKey]*transferv1.TargetsResponse{NewRouteKey("tg", "hot"): {
		Group: "tg", Stage: "hot", ShardNum: 1, Copies: 1, Shards: []*transferv1.TargetsResponse_Shard{{ShardId: 0, NodeIds: []string{"a"}}},
	}}
	r := evaluate(emptyCounts(), emptyCounts().set(trace, "tg", "hot", "b", 0, "s", 2), off, pushed, nil, sampled)
	require.NotEmpty(t, r.Mismatches)
	assert.Contains(t, r.Mismatches[0], "but only [a] are its target nodes")
}

func TestEvaluateRows_Tolerance(t *testing.T) {
	for _, tc := range []struct {
		perShard map[uint32]map[string]int64
		name     string
		wantWarn string
		pushed   uint64
		allowed  uint64
		mismatch bool
		warn     bool
	}{
		{name: "exact", perShard: map[uint32]map[string]int64{0: {"a": 5}}, pushed: 5},
		{
			name: "every copy pushed again", perShard: map[uint32]map[string]int64{0: {"a": 7}}, pushed: 5, allowed: 2, warn: true,
			wantWarn: "stream g/hot: rows grew by 7 per copy, 5 were sent;",
		},
		{name: "beyond the tolerance", perShard: map[uint32]map[string]int64{0: {"a": 8}}, pushed: 5, allowed: 2, mismatch: true},
		{name: "fewer rows than sent", perShard: map[uint32]map[string]int64{0: {"a": 4}}, pushed: 5, allowed: 2, mismatch: true},
		// 2 more on every copy and 2 more again on one: 4 above the rows sent on that copy.
		{name: "both budgets add up", perShard: map[uint32]map[string]int64{0: {"a": 7, "b": 9}}, pushed: 5, allowed: 2, mismatch: true},
		{
			name: "one copy pushed again", perShard: map[uint32]map[string]int64{0: {"a": 5, "b": 7}}, pushed: 5, allowed: 2, warn: true,
			wantWarn: "stream g/hot: rows grew by 5 per copy (2 more on some copies), 5 were sent;",
		},
	} {
		targets := map[uint32][]string{}
		for shard, d := range tc.perShard {
			for n := range d {
				targets[shard] = append(targets[shard], n)
			}
		}
		r := &VerifyReport{}
		var tol *verifyTolerance
		if tc.allowed > 0 {
			tol = &verifyTolerance{rows: tc.allowed, payloads: []string{"p"}}
		}
		r.evaluateRows("stream g/hot", tc.pushed, routeTo(targets), tc.perShard, tol, false)
		assert.Equal(t, tc.mismatch, len(r.Mismatches) > 0, "%s: %v", tc.name, r.Mismatches)
		assert.Equal(t, tc.warn, len(r.Warnings) > 0, "%s: %v", tc.name, r.Warnings)
		if tc.wantWarn != "" {
			assert.Contains(t, strings.Join(r.Warnings, "\n"), tc.wantWarn, tc.name)
		}
	}
}

// A segment that disappeared on the target may have held rows sent into it: fewer rows than sent,
// on every copy or on some only, is not checked, not a mismatch.
func TestEvaluateRows_GoneSegmentIsIncompleteNotAMismatch(t *testing.T) {
	routes := map[RouteKey]*transferv1.TargetsResponse{NewRouteKey("g", "hot"): {
		Group: "g", Stage: "hot", ShardNum: 1, Copies: 2, Shards: []*transferv1.TargetsResponse_Shard{{ShardId: 0, NodeIds: []string{"a", "b"}}},
	}}
	pushed := pushedRows(stream, "g", "hot", 5)
	for _, tc := range []struct {
		post *Counts
		name string
	}{
		{name: "every copy", post: emptyCounts().set(stream, "g", "hot", "a", 0, "new", 3).set(stream, "g", "hot", "b", 0, "new", 3)},
		{name: "one copy", post: emptyCounts().set(stream, "g", "hot", "a", 0, "new", 3).set(stream, "g", "hot", "b", 0, "new", 5).
			set(stream, "g", "hot", "b", 0, "old", 10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pre := emptyCounts().set(stream, "g", "hot", "a", 0, "old", 10).set(stream, "g", "hot", "b", 0, "old", 10)
			rep := evaluate(pre, tc.post, routes, pushed, nil, nil)
			assert.Empty(t, rep.Mismatches)
			require.NotEmpty(t, rep.Incomplete)
			for _, l := range rep.Incomplete {
				assert.Contains(t, l, "disappeared meanwhile (retention?)")
			}
			assert.NotEmpty(t, rep.Warnings, "the segment that disappeared is named")
		})
	}
}

// Rows on a node outside the shard's targets are a mismatch even when a segment disappeared.
func TestEvaluateRows_GoneSegmentKeepsTheRouteCheck(t *testing.T) {
	rep := &VerifyReport{}
	rep.evaluateRows("stream g/hot", 5, routeTo(map[uint32][]string{0: {"a"}}), map[uint32]map[string]int64{0: {"a": 3, "c": 2}}, nil, true)
	require.Len(t, rep.Mismatches, 1)
	assert.Contains(t, rep.Mismatches[0], "expected the same growth on exactly [a]")
}

// A baseline no check needs may fail: the import goes on, records that it has none, and a
// verify forced later says so.
func TestRunImport_BaselineNotNeededIsAWarning(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	f.unreachable = []string{"data-a"}
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = false
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	assert.Contains(t, run.log, "WARN the baseline Plan of the target did not count target node(s) data-a; neither --verify nor a reconciliation needs it")
	assert.NotContains(t, run.log, "no data was sent", "the import goes on, so the warning does not say nothing was sent")
	state := readState(t, ProgressPath(opts))
	assert.True(t, state.NoBaseline)
	assert.Nil(t, state.BaselineAt)

	f.unreachable = nil
	editState(t, ProgressPath(opts), func(s *progressState) { s.SkippedPayloads = []string{"x"} })
	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitVerify, again.code(), again.log)
	assert.Contains(t, again.err.Error(), "the first run recorded no baseline")
	assert.Contains(t, again.err.Error(), "--restart imports it again with a new baseline")
}

// Ctrl-C during verify ends the import with exit 3: the report says it was interrupted, not that
// verify failed with exit 4.
func TestVerifyInterruptedIsNotRenderedAsAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plan := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g", Catalog: stream}}}}
	rep := Verify(ctx, nil, emptyCounts(), plan, twoShardRoute("g", "hot"), pushedRows(stream, "g", "hot", 3), VerifyOptions{Timeout: time.Minute})
	var out strings.Builder
	rep.Render(&out)
	assert.True(t, strings.HasPrefix(out.String(), "VERIFY    interrupted in "), out.String())
	assert.NotContains(t, out.String(), "exit 4")
}

// A verify a run could not do because the first run recorded no baseline ends it with exit 4, and
// the same command run again is not asked to verify: the verify cannot pass, so none is pending, and
// the finished import says why it did not verify.
func TestRunImport_VerifyWithoutABaselineIsNotPending(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	f.unreachable = []string{"data-a"}
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = false
	interruptedCSV(t, f, li, opts, 5)
	assert.True(t, readState(t, ProgressPath(opts)).NoBaseline)

	f.unreachable = nil
	opts.Verify, opts.Resume = true, true
	first := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitVerify, first.code(), first.log)
	assert.ErrorContains(t, first.err, "verify is not possible")
	state := readState(t, ProgressPath(opts))
	require.NotNil(t, state.FinishedAt)
	assert.False(t, state.VerifyPending, "a verify without a baseline never passes, so it is not kept pending")

	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "already imported into this cluster")
	assert.Contains(t, again.log, "NOTE verify skipped: the first run recorded no baseline")
}
