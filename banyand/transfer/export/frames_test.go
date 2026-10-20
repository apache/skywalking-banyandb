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

package export

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// receiveLimit is the message size the liaison and the export client accept.
const receiveLimit = 32 << 20

// syntheticSegments builds segments*shards*parts of inventory for one stream group.
func syntheticSegments(segments, shards, parts int) []*transferv1.UnitInventory {
	units := make([]*transferv1.UnitInventory, 0, segments)
	for s := 0; s < segments; s++ {
		seg := &transferv1.SegmentInventory{
			Unit: &transferv1.SegmentUnit{
				Catalog:       commonv1.Catalog_CATALOG_STREAM,
				Group:         "sw_record",
				SegmentSuffix: fmt.Sprintf("2026%04d", s),
			},
			SegmentVersion: "1.5.0",
			SegmentLevel:   &transferv1.SidxStat{EstimatedBytes: 7},
		}
		for sh := 0; sh < shards; sh++ {
			stat := &transferv1.ShardStat{ShardId: uint32(sh), PartsCount: uint32(parts), MinTimestamp: 1_788_739_200_000_000_000}
			for p := 0; p < parts; p++ {
				stat.Parts = append(stat.Parts, &transferv1.PartStat{
					Id: uint64(p + 1), MinTimestamp: 1_788_739_200_000_000_000, MaxTimestamp: 1_788_825_599_000_000_000, TotalCount: 1000,
				})
			}
			seg.Unit.ShardIds = append(seg.Unit.ShardIds, uint32(sh))
			seg.Shards = append(seg.Shards, stat)
		}
		units = append(units, &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: seg}})
	}
	return units
}

// assertReassembles checks that frames stay within budget and that merging their units by
// (segment, shard) gives back exactly units: each segment-level index counted once, no segment
// piece listing a shard twice, and the pieces of a split shard holding disjoint parts in order
// with the shard's aggregates on the first piece only. It returns the largest number of pieces
// any shard was split into.
func assertReassembles(t *testing.T, units []*transferv1.UnitInventory, frames []*transferv1.PlanResponse, budget int) int {
	t.Helper()
	type key struct {
		suffix string
		shard  uint32
	}
	got := map[key][]*transferv1.ShardStat{}
	segLevels := map[string]int{}
	for i, f := range frames {
		if size := proto.Size(f); size > budget {
			t.Fatalf("frame %d is %d bytes, over the %d byte budget", i, size, budget)
		}
		if f.GetUnits().GetStage() != "warm" {
			t.Fatalf("frame %d lost the stage", i)
		}
		for _, u := range f.GetUnits().GetUnits() {
			seg := u.GetSegment()
			if len(seg.GetUnit().GetShardIds()) != len(seg.GetShards()) {
				t.Fatalf("a piece's shard_ids must list exactly its shards")
			}
			if seg.GetSegmentLevel() != nil {
				segLevels[seg.GetUnit().GetSegmentSuffix()]++
			}
			inPiece := map[uint32]bool{}
			for j, sh := range seg.GetShards() {
				if seg.GetUnit().GetShardIds()[j] != sh.GetShardId() {
					t.Fatalf("shard_ids must follow the shards")
				}
				if inPiece[sh.GetShardId()] {
					t.Fatalf("a segment piece of %s lists shard %d twice", seg.GetUnit().GetSegmentSuffix(), sh.GetShardId())
				}
				inPiece[sh.GetShardId()] = true
				k := key{seg.GetUnit().GetSegmentSuffix(), sh.GetShardId()}
				got[k] = append(got[k], sh)
			}
		}
	}
	want, maxPieces := 0, 0
	for _, u := range units {
		seg := u.GetSegment()
		suffix := seg.GetUnit().GetSegmentSuffix()
		if segLevels[suffix] != 1 {
			t.Fatalf("segment %s must carry its segment-level index exactly once, got %d", suffix, segLevels[suffix])
		}
		for _, sh := range seg.GetShards() {
			want++
			pieces := got[key{suffix, sh.GetShardId()}]
			if len(pieces) == 0 {
				t.Fatalf("shard %d of %s is missing", sh.GetShardId(), suffix)
			}
			maxPieces = max(maxPieces, len(pieces))
			merged := proto.Clone(pieces[0]).(*transferv1.ShardStat)
			for _, p := range pieces[1:] {
				if !proto.Equal(p, &transferv1.ShardStat{ShardId: p.GetShardId(), Parts: p.GetParts()}) {
					t.Fatalf("a continuation piece of shard %d of %s must carry only shard_id and parts: %v", sh.GetShardId(), suffix, p)
				}
				if len(p.GetParts()) == 0 {
					t.Fatalf("a continuation piece of shard %d of %s carries no parts", sh.GetShardId(), suffix)
				}
				merged.Parts = append(merged.Parts, p.GetParts()...)
			}
			if !proto.Equal(merged, sh) {
				t.Fatalf("shard %d of %s did not survive the split: got %d parts, want %d",
					sh.GetShardId(), suffix, len(merged.GetParts()), len(sh.GetParts()))
			}
		}
	}
	if len(got) != want {
		t.Fatalf("want %d shards, got %d", want, len(got))
	}
	return maxPieces
}

func TestSplitUnitFrames_GroupOverTheReceiveLimit(t *testing.T) {
	// 365 day segments of 128 shards with 32 parts each encode to about 40 MiB in one frame.
	units := syntheticSegments(365, 128, 32)
	whole := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{Stage: "warm", Units: units}}}
	if size := proto.Size(whole); size <= receiveLimit {
		t.Fatalf("the fixture must exceed the receive limit in aggregate, got %d bytes", size)
	}
	frames := splitUnitFrames("warm", units, planFrameBudget)
	if len(frames) < 2 {
		t.Fatalf("want several frames, got %d", len(frames))
	}
	assertReassembles(t, units, frames, planFrameBudget)
}

func TestSplitUnitFrames_SplitsASegmentLargerThanTheBudget(t *testing.T) {
	units := syntheticSegments(3, 16, 8)
	budget := proto.Size(units[0]) / 3
	frames := splitUnitFrames("warm", units, budget)
	if len(frames) < 9 {
		t.Fatalf("each segment must be split into several pieces, got %d frames", len(frames))
	}
	assertReassembles(t, units, frames, budget)
}

func TestSplitUnitFrames_SmallGroupIsOneFrame(t *testing.T) {
	units := syntheticSegments(3, 2, 2)
	frames := splitUnitFrames("warm", units, planFrameBudget)
	if len(frames) != 1 || !proto.Equal(frames[0].GetUnits(), &transferv1.UnitFrame{Stage: "warm", Units: units}) {
		t.Fatalf("a small group must travel unchanged in one frame, got %d frames", len(frames))
	}
	if frames := splitUnitFrames("warm", nil, planFrameBudget); len(frames) != 0 {
		t.Fatalf("a group without units has no frame, got %d", len(frames))
	}
}

func TestSplitUnitFrames_OneShardOverTheReceiveLimit(t *testing.T) {
	// One segment with one shard of 1.2 million parts encodes to about 35 MB in one frame.
	units := syntheticSegments(1, 1, 1_200_000)
	whole := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{Stage: "warm", Units: units}}}
	if size := proto.Size(whole); size <= receiveLimit {
		t.Fatalf("the fixture must exceed the receive limit with a single shard, got %d bytes", size)
	}
	frames := splitUnitFrames("warm", units, planFrameBudget)
	if pieces := assertReassembles(t, units, frames, planFrameBudget); pieces < receiveLimit/planFrameBudget {
		t.Fatalf("the shard must be split into at least %d pieces, got %d", receiveLimit/planFrameBudget, pieces)
	}
}

// varyShards gives every part distinct timestamps and counts and makes the shard aggregates
// match, so a reordered, lost or duplicated part changes the reassembled shard.
func varyShards(units []*transferv1.UnitInventory) {
	for _, u := range units {
		for _, sh := range u.GetSegment().GetShards() {
			sh.MinTimestamp, sh.MaxTimestamp, sh.TotalCount = 0, 0, 0
			sh.EstimatedCompressedBytes, sh.EstimatedUncompressedBytes = uint64(len(sh.Parts))*512, uint64(len(sh.Parts))*4096
			sh.PartsCount = uint32(len(sh.Parts))
			for i, p := range sh.Parts {
				p.MinTimestamp = 1_788_739_200_000_000_000 + int64(i)*1_000_000_000
				p.MaxTimestamp = p.MinTimestamp + 500_000_000
				p.TotalCount = uint64(i%97 + 1)
				if sh.MinTimestamp == 0 || p.MinTimestamp < sh.MinTimestamp {
					sh.MinTimestamp = p.MinTimestamp
				}
				sh.MaxTimestamp = max(sh.MaxTimestamp, p.MaxTimestamp)
				sh.TotalCount += p.TotalCount
			}
		}
	}
}

func TestSplitUnitFrames_SplitsAShardLargerThanTheBudget(t *testing.T) {
	units := syntheticSegments(3, 4, 4)
	// Shard 0 (whose id encodes as nothing) and shard 2 of the middle segment are oversized;
	// the other shards stay whole and keep sharing pieces.
	for _, id := range []int{0, 2} {
		sh := units[1].GetSegment().GetShards()[id]
		for p := len(sh.Parts); p < 400; p++ {
			sh.Parts = append(sh.Parts, &transferv1.PartStat{Id: uint64(p + 1)})
		}
	}
	varyShards(units)
	budget := 2048
	if size := proto.Size(units[1].GetSegment().GetShards()[0]); size <= budget {
		t.Fatalf("the oversized shard must exceed the budget, got %d bytes", size)
	}
	frames := splitUnitFrames("warm", units, budget)
	if pieces := assertReassembles(t, units, frames, budget); pieces < 4 {
		t.Fatalf("the oversized shards must be split into several pieces, got %d", pieces)
	}
}

// Every shape and budget, from a budget that barely fits one part next to the piece header to
// one that holds whole segments, must reassemble without losing, duplicating or reordering a
// part and without a frame over the budget.
func TestSplitUnitFrames_ShardSplitBudgetSweep(t *testing.T) {
	shapes := map[string]func() []*transferv1.UnitInventory{
		"one shard": func() []*transferv1.UnitInventory { return syntheticSegments(1, 1, 300) },
		"one oversized shard among many": func() []*transferv1.UnitInventory {
			units := syntheticSegments(2, 6, 3)
			sh := units[0].GetSegment().GetShards()[5]
			for p := len(sh.Parts); p < 250; p++ {
				sh.Parts = append(sh.Parts, &transferv1.PartStat{Id: uint64(p + 1)})
			}
			return units
		},
		"every shard oversized": func() []*transferv1.UnitInventory { return syntheticSegments(2, 3, 120) },
		"shard without parts": func() []*transferv1.UnitInventory {
			units := syntheticSegments(1, 3, 200)
			units[0].GetSegment().GetShards()[1].Parts = nil
			return units
		},
	}
	for name, build := range shapes {
		for budget := 256; budget <= 16<<10; budget += 61 {
			units := build()
			varyShards(units)
			frames := splitUnitFrames("warm", units, budget)
			t.Run(fmt.Sprintf("%s/%d", name, budget), func(t *testing.T) {
				assertReassembles(t, units, frames, budget)
			})
		}
	}
}

func TestSplitShard_FirstPieceCarriesTheAggregates(t *testing.T) {
	units := syntheticSegments(1, 1, 50)
	varyShards(units)
	shard := units[0].GetSegment().GetShards()[0]
	shard.ShardId = 7
	pieces := splitShard(shard, 300)
	if len(pieces) < 3 {
		t.Fatalf("want several pieces, got %d", len(pieces))
	}
	head := proto.Clone(shard).(*transferv1.ShardStat)
	head.Parts = pieces[0].GetParts()
	if !proto.Equal(pieces[0], head) {
		t.Fatalf("the first piece must carry every aggregate: %v", pieces[0])
	}
	var parts []*transferv1.PartStat
	for i, p := range pieces {
		if size := entrySize(proto.Size(p)); size > 300 {
			t.Fatalf("piece %d is %d bytes, over the budget", i, size)
		}
		if i > 0 && !proto.Equal(p, &transferv1.ShardStat{ShardId: 7, Parts: p.GetParts()}) {
			t.Fatalf("piece %d must carry only shard_id and parts: %v", i, p)
		}
		parts = append(parts, p.GetParts()...)
	}
	if len(parts) != len(shard.GetParts()) {
		t.Fatalf("want %d parts, got %d", len(shard.GetParts()), len(parts))
	}
	for i := range parts {
		if parts[i] != shard.GetParts()[i] {
			t.Fatalf("part %d is lost or out of order", i)
		}
	}
}
