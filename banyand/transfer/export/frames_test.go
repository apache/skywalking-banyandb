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
// (segment, shard) gives back exactly units, with each segment-level index counted once.
func assertReassembles(t *testing.T, units []*transferv1.UnitInventory, frames []*transferv1.PlanResponse, budget int) {
	t.Helper()
	type key struct {
		suffix string
		shard  uint32
	}
	got := map[key]*transferv1.ShardStat{}
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
			for j, sh := range seg.GetShards() {
				if seg.GetUnit().GetShardIds()[j] != sh.GetShardId() {
					t.Fatalf("shard_ids must follow the shards")
				}
				k := key{seg.GetUnit().GetSegmentSuffix(), sh.GetShardId()}
				if _, dup := got[k]; dup {
					t.Fatalf("shard %v is sent twice", k)
				}
				got[k] = sh
			}
		}
	}
	want := 0
	for _, u := range units {
		seg := u.GetSegment()
		if segLevels[seg.GetUnit().GetSegmentSuffix()] != 1 {
			t.Fatalf("segment %s must carry its segment-level index exactly once, got %d",
				seg.GetUnit().GetSegmentSuffix(), segLevels[seg.GetUnit().GetSegmentSuffix()])
		}
		for _, sh := range seg.GetShards() {
			want++
			if !proto.Equal(got[key{seg.GetUnit().GetSegmentSuffix(), sh.GetShardId()}], sh) {
				t.Fatalf("shard %d of %s did not survive the split", sh.GetShardId(), seg.GetUnit().GetSegmentSuffix())
			}
		}
	}
	if len(got) != want {
		t.Fatalf("want %d shards, got %d", want, len(got))
	}
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
