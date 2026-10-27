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

package exporter

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestRunPlan_CollectsFramesAndDropsUnreachableNodes(t *testing.T) {
	f := newFakeLiaison()
	f.planFrames = []*transferv1.PlanResponse{
		unitFrame("data-a:17912", "", "m", 100, 200, 5),
		unitFrame("data-b:17912", "warm", "m", 150, 250, 6), // data-b failed after its frame was relayed
	}
	f.summary = &transferv1.PlanSummary{AnsweredNodes: []string{"data-a:17912"}, UnreachableNodes: []string{"data-b:17912"}}
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := cluster.RunPlan(ctx, conn, []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_MEASURE}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Frames) != 1 || res.Frames[0].GetNodeId() != "data-a:17912" {
		t.Fatalf("frames of an unreachable node must be dropped, got %+v", res.Frames)
	}
	if res.Frames[0].GetStage() != transfer.DefaultStageName {
		t.Fatalf("stage must be normalized, got %q", res.Frames[0].GetStage())
	}
	if !slices.Equal(res.UnreachableNodes, []string{"data-b:17912"}) || !slices.Equal(res.AnsweredNodes, []string{"data-a:17912"}) {
		t.Fatalf("summary = answered %v, unreachable %v", res.AnsweredNodes, res.UnreachableNodes)
	}
}

// TestRunPlan_MergesSegmentPiecesAcrossFrames covers a data node that split a large segment
// into pieces with disjoint shards over several frames, interleaved with another node's
// frames: the result must hold the segment as one unit, as if it had arrived whole.
func TestRunPlan_MergesSegmentPiecesAcrossFrames(t *testing.T) {
	whole := unitFrame("data-a:17912", "warm", "m", 1, 2, 1)
	seg := segmentOf(whole)
	seg.Unit.ShardIds = []uint32{0, 1, 2}
	for _, id := range []uint32{1, 2} {
		seg.Shards = append(seg.Shards, &transferv1.ShardStat{ShardId: id, MinTimestamp: 3, MaxTimestamp: 4, TotalCount: 2, PartsCount: 1})
	}
	piece := func(level *transferv1.SidxStat, shards ...*transferv1.ShardStat) *transferv1.PlanResponse {
		p := &transferv1.SegmentInventory{
			Unit:           &transferv1.SegmentUnit{Catalog: seg.Unit.Catalog, Group: seg.Unit.Group, SegmentSuffix: seg.Unit.SegmentSuffix},
			SegmentVersion: seg.SegmentVersion,
			SegmentLevel:   level,
			Shards:         shards,
		}
		for _, sh := range shards {
			p.Unit.ShardIds = append(p.Unit.ShardIds, sh.ShardId)
		}
		return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{
			NodeId: "data-a:17912", Stage: "warm", Units: []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Segment{Segment: p}}},
		}}}
	}
	other := unitFrame("data-b:17912", "warm", "m", 1, 2, 1)
	f := newFakeLiaison()
	f.planFrames = []*transferv1.PlanResponse{
		piece(nil, seg.Shards[2]), other, piece(seg.SegmentLevel, seg.Shards[0]), piece(nil, seg.Shards[1]),
	}
	f.summary = &transferv1.PlanSummary{AnsweredNodes: []string{"data-a:17912", "data-b:17912"}}
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := cluster.RunPlan(ctx, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Frames) != 2 || !proto.Equal(res.Frames[0], whole.GetUnits()) || !proto.Equal(res.Frames[1], other.GetUnits()) {
		t.Fatalf("the pieces must merge into the whole segment, got %+v", res.Frames)
	}
	rep := BuildReport(&cluster.ClusterInfo{}, res)
	if len(rep.Rows) != 2 || rep.Rows[0].Segments != 1 || rep.Rows[0].Shards != 3 || len(rep.MultiSource) != 1 {
		t.Fatalf("a merged segment counts once with no extra sources: %+v %+v", rep.Rows, rep.MultiSource)
	}
}

// A shard split into pieces with disjoint parts, across frames and next to other segment
// pieces in one frame, merges back into one ShardStat whose parts keep their order and whose
// aggregates come from the first piece only; the report counts it exactly like the whole shard.
func TestRunPlan_MergesShardPiecesAcrossFrames(t *testing.T) {
	const node = "data-a:17912"
	parts := func(from, to uint64) []*transferv1.PartStat {
		var ps []*transferv1.PartStat
		for id := from; id <= to; id++ {
			ps = append(ps, &transferv1.PartStat{Id: id, MinTimestamp: int64(100 + id), MaxTimestamp: int64(200 + id), TotalCount: id})
		}
		return ps
	}
	shard := func(id uint32, ps []*transferv1.PartStat) *transferv1.ShardStat {
		s := &transferv1.ShardStat{ShardId: id, PartsCount: uint32(len(ps)), EstimatedCompressedBytes: 64 * uint64(len(ps)), EstimatedUncompressedBytes: 256 * uint64(len(ps))}
		for _, p := range ps {
			s.TotalCount += p.TotalCount
			transfer.MergeRange(&s.MinTimestamp, &s.MaxTimestamp, p.MinTimestamp, p.MaxTimestamp)
		}
		s.Parts = ps
		return s
	}
	// Shard 0 (whose id encodes as nothing) and shard 1 are split; shard 2 travels whole.
	shards := []*transferv1.ShardStat{shard(0, parts(1, 3)), shard(1, parts(11, 15)), shard(2, parts(21, 22))}
	whole := &transferv1.SegmentInventory{
		Unit:           &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "m", SegmentSuffix: "20260930", ShardIds: []uint32{0, 1, 2}},
		SegmentVersion: "1.5.0",
		SegmentLevel:   &transferv1.SidxStat{EstimatedBytes: 10},
		Shards:         shards,
	}
	head := func(s *transferv1.ShardStat, n int) *transferv1.ShardStat {
		h := proto.Clone(s).(*transferv1.ShardStat)
		h.Parts = h.Parts[:n]
		return h
	}
	rest := func(s *transferv1.ShardStat, from, to int) *transferv1.ShardStat {
		return &transferv1.ShardStat{ShardId: s.ShardId, Parts: proto.Clone(s).(*transferv1.ShardStat).Parts[from:to]}
	}
	piece := func(level *transferv1.SidxStat, ss ...*transferv1.ShardStat) *transferv1.UnitInventory {
		p := &transferv1.SegmentInventory{
			Unit:           &transferv1.SegmentUnit{Catalog: whole.Unit.Catalog, Group: whole.Unit.Group, SegmentSuffix: whole.Unit.SegmentSuffix},
			SegmentVersion: whole.SegmentVersion,
			SegmentLevel:   level,
			Shards:         ss,
		}
		for _, sh := range ss {
			p.Unit.ShardIds = append(p.Unit.ShardIds, sh.ShardId)
		}
		return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: p}}
	}
	frame := func(units ...*transferv1.UnitInventory) *transferv1.PlanResponse {
		return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{NodeId: node, Stage: "warm", Units: units}}}
	}
	other := unitFrame("data-b:17912", "warm", "m", 101, 203, 6)
	f := newFakeLiaison()
	f.planFrames = []*transferv1.PlanResponse{
		frame(piece(proto.Clone(whole.SegmentLevel).(*transferv1.SidxStat), head(shards[0], 2), head(shards[1], 2))),
		other,
		frame(piece(nil, rest(shards[0], 2, 3)), piece(nil, rest(shards[1], 2, 3))),
		frame(piece(nil, rest(shards[1], 3, 5), proto.Clone(shards[2]).(*transferv1.ShardStat))),
	}
	f.summary = &transferv1.PlanSummary{AnsweredNodes: []string{node, "data-b:17912"}}
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := cluster.RunPlan(ctx, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Frames) != 2 || len(res.Frames[0].GetUnits()) != 1 || !proto.Equal(res.Frames[0].GetUnits()[0].GetSegment(), whole) ||
		!proto.Equal(res.Frames[1], other.GetUnits()) {
		t.Fatalf("the shard pieces must merge into the whole segment, got %+v", res.Frames)
	}
	got := BuildReport(&cluster.ClusterInfo{}, res)
	want := BuildReport(&cluster.ClusterInfo{}, &cluster.PlanResult{
		Frames: []*transferv1.UnitFrame{
			{NodeId: node, Stage: "warm", Units: []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Segment{Segment: whole}}}},
			other.GetUnits(),
		},
		AnsweredNodes: res.AnsweredNodes,
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("a split shard must report like the whole shard:\n got %+v\nwant %+v", got, want)
	}
	if got.Rows[0].Shards != 3 || got.Rows[0].Parts != 10 || len(got.MultiSource) != 1 {
		t.Fatalf("a split shard counts once and adds no source: %+v %+v", got.Rows, got.MultiSource)
	}
}

func TestRunPlan_RejectsProtocolViolations(t *testing.T) {
	created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: "feedfeedfeedfeedfeedfeedfeedfeed"}}}
	summary := summaryFrame(&transferv1.PlanSummary{AnsweredNodes: []string{"data-a:17912"}})
	for name, tc := range map[string]struct {
		want   string
		frames []*transferv1.PlanResponse
	}{
		"created frame in a dry run": {frames: []*transferv1.PlanResponse{created, summary}, want: "unexpected frame *v1.PlanResponse_Created"},
		"second summary":             {frames: []*transferv1.PlanResponse{summary, summary}, want: "after the summary"},
		"units after the summary":    {frames: []*transferv1.PlanResponse{summary, unitFrame("data-a:17912", "", "m", 1, 2, 1)}, want: "after the summary"},
		"units from an unlisted node": {
			frames: []*transferv1.PlanResponse{unitFrame("data-z:17912", "", "m", 1, 2, 1), summary},
			want:   "neither answered nor unreachable",
		},
		"segment without its unit": {
			frames: []*transferv1.PlanResponse{{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{
				NodeId: "data-a:17912",
				Units:  []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{}}}},
			}}}, summary},
			want: "segment inventory without its unit",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeLiaison()
			f.skipSummary = true
			f.planFrames = tc.frames
			conn := dial(t, serveLiaison(t, f))
			_, err := cluster.RunPlan(context.Background(), conn, nil)
			var exitErr *exitcode.ExitError
			if !errors.As(err, &exitErr) || exitErr.Code != exitcode.ExitRuntime || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want exit 3 mentioning %q, got %v", tc.want, err)
			}
		})
	}
}

func TestRunPlan_ServerWithoutExportService(t *testing.T) {
	f := newFakeLiaison()
	f.noExport = true
	conn := dial(t, serveLiaison(t, f))
	_, err := cluster.RunPlan(context.Background(), conn, nil)
	if err == nil || !strings.Contains(err.Error(), "does not support data export (needs 0.12+)") {
		t.Fatalf("an older server must get the upgrade hint, got %v", err)
	}
	if code := exitcode.ExitCodeFor(err); code != exitcode.ExitPreflight {
		t.Fatalf("UNIMPLEMENTED is a preflight rejection, got exit %d", code)
	}
}

func TestRunPlan_RejectsUnstampedFrames(t *testing.T) {
	f := newFakeLiaison()
	f.planFrames = []*transferv1.PlanResponse{unitFrame("", "", "m", 1, 2, 1)}
	conn := dial(t, serveLiaison(t, f))
	if _, err := cluster.RunPlan(context.Background(), conn, nil); err == nil || !strings.Contains(err.Error(), "node_id") {
		t.Fatalf("a units frame without node_id must be rejected, got %v", err)
	}
}

func TestValidateSelectors(t *testing.T) {
	groups := []*commonv1.Group{
		{Metadata: &commonv1.Metadata{Name: "sw_metric"}, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		{Metadata: &commonv1.Metadata{Name: "sw_record"}, Catalog: commonv1.Catalog_CATALOG_STREAM},
	}
	ok := []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{"sw_metric"}}, {Catalog: commonv1.Catalog_CATALOG_STREAM}}
	if err := ValidateSelectors(groups, ok); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSelectors(groups, []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"sw_metric"}}}); err == nil {
		t.Fatal("a group in the wrong catalog must be rejected")
	}
	if err := ValidateSelectors(groups, []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"nope"}}}); err == nil {
		t.Fatal("an unknown group must be rejected")
	}
}

func TestCollectPlanFrames_StreamEndsWithoutSummaryFrame(t *testing.T) {
	f := newFakeLiaison()
	f.skipSummary = true
	f.planFrames = []*transferv1.PlanResponse{
		unitFrame("data-a:17912", "hot", "g1", 100, 200, 5),
	}
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := cluster.RunPlan(ctx, conn, nil)
	var exitErr *exitcode.ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != exitcode.ExitRuntime || !strings.Contains(err.Error(), "without a summary frame") {
		t.Fatalf("a stream without the summary frame leaves the coverage unknown and must fail with exit 3, got %v", err)
	}
}
