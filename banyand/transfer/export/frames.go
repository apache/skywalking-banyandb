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
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// planFrameBudget bounds the encoded size of one units frame, far below the 32 MiB the
// liaison and the export client accept, so a group of any size is inventoried. It is a
// variable only so tests can shrink it.
var planFrameBudget = 4 << 20

// pieceSlack covers the length prefixes that grow while shards are added to a segment piece.
const pieceSlack = 32

// splitUnitFrames packs the units of one group into frames of at most budget bytes, in order.
// A segment too large for one frame is split into pieces with disjoint shards (see
// splitSegment); a single shard is never split, so a shard larger than the budget travels
// alone in an oversized frame.
func splitUnitFrames(stage string, units []*transferv1.UnitInventory, budget int) []*transferv1.PlanResponse {
	base := proto.Size(&transferv1.UnitFrame{Stage: stage}) + pieceSlack
	var frames []*transferv1.PlanResponse
	var current []*transferv1.UnitInventory
	size := base
	flush := func() {
		if len(current) == 0 {
			return
		}
		frames = append(frames, &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{
			Units: &transferv1.UnitFrame{Stage: stage, Units: current},
		}})
		current, size = nil, base
	}
	add := func(u *transferv1.UnitInventory, n int) {
		if size+n > budget {
			flush()
		}
		current = append(current, u)
		size += n
	}
	for _, u := range units {
		n := entrySize(proto.Size(u))
		if seg := u.GetSegment(); n > budget-base && len(seg.GetShards()) > 1 {
			splitSegment(seg, budget-base, add)
			continue
		}
		add(u, n)
	}
	flush()
	return frames
}

// splitSegment passes seg to add as pieces of whole shards, each with an upper bound of its
// encoded size, filling a piece up to budget bytes. Every piece repeats the segment's key and
// version with the shard_ids of its own shards; only the first carries segment_level, so
// summing the pieces counts the segment-level index once.
func splitSegment(seg *transferv1.SegmentInventory, budget int, add func(u *transferv1.UnitInventory, n int)) {
	var piece *transferv1.SegmentInventory
	size := 0
	emit := func() {
		if piece != nil {
			add(&transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: piece}}, size)
		}
	}
	for _, shard := range seg.GetShards() {
		n := entrySize(proto.Size(shard)) + protowire.SizeVarint(uint64(shard.GetShardId()))
		if piece == nil || (len(piece.Shards) > 0 && size+n > budget) {
			first := piece == nil
			emit()
			piece = &transferv1.SegmentInventory{
				Unit: &transferv1.SegmentUnit{
					Catalog:       seg.GetUnit().GetCatalog(),
					Group:         seg.GetUnit().GetGroup(),
					SegmentSuffix: seg.GetUnit().GetSegmentSuffix(),
				},
				SegmentVersion: seg.GetSegmentVersion(),
			}
			if first {
				piece.SegmentLevel = seg.GetSegmentLevel()
			}
			size = entrySize(proto.Size(piece)) + pieceSlack
		}
		piece.Unit.ShardIds = append(piece.Unit.ShardIds, shard.GetShardId())
		piece.Shards = append(piece.Shards, shard)
		size += n
	}
	emit()
}

// entrySize is the encoded size of a length-delimited field with a one-byte tag (every
// field number used here is below 16) whose payload is n bytes.
func entrySize(n int) int {
	return 1 + protowire.SizeBytes(n)
}
