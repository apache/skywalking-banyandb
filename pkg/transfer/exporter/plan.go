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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// DefaultStageName is the name the CLI and manifest give the engine's unnamed default tier.
const DefaultStageName = "hot"

// PlanResult is everything one dry-run Plan call returned. Frames holds the `units` frames
// with node_id stamped and the stage normalized; the node lists come from the `summary` frame.
type PlanResult struct {
	Frames           []*transferv1.UnitFrame
	UnreachableNodes []string
	AnsweredNodes    []string
}

// RunPlan performs the single server-streaming live-directory Plan call of a dry run.
func RunPlan(ctx context.Context, conn *grpc.ClientConn, selectors []*transferv1.Selector) (*PlanResult, error) {
	stream, err := transferv1.NewExportServiceClient(conn).Plan(ctx, &transferv1.PlanRequest{Selectors: selectors})
	if err != nil {
		return nil, fmt.Errorf("plan: %w", unsupported(err))
	}
	return collectPlanFrames(stream)
}

// unsupported adds an upgrade hint to the UNIMPLEMENTED answer of a server without
// ExportService; the gRPC status stays reachable for ExitCodeFor.
func unsupported(err error) error {
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("the server does not support data export (needs 0.12+): %w", err)
	}
	return err
}

// collectPlanFrames drains a dry-run Plan stream. `units` frames are kept, the single
// `summary` frame closes the stream and is folded into the result, and frames from nodes
// the liaison reported unreachable are dropped so a half-answered node counts as a
// coverage gap, not as a source; a frame from a node the summary lists as neither answered nor
// unreachable breaks the protocol. A `created` frame (the client never asks for a session), a
// second summary or any frame after it breaks the protocol; a stream that ends without the
// summary is an error too: without it the coverage is unknown.
func collectPlanFrames(stream transferv1.ExportService_PlanClient) (*PlanResult, error) {
	result := &PlanResult{}
	sawSummary := false
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, fmt.Errorf("plan stream: %w", unsupported(recvErr))
		}
		if sawSummary {
			return nil, Exit(ExitRuntime, "plan stream: unexpected %T frame after the summary frame", frame.GetFrame())
		}
		var units *transferv1.UnitFrame
		switch f := frame.GetFrame().(type) {
		case *transferv1.PlanResponse_Summary:
			sawSummary = true
			result.UnreachableNodes = f.Summary.GetUnreachableNodes()
			result.AnsweredNodes = f.Summary.GetAnsweredNodes()
			continue
		case *transferv1.PlanResponse_Units:
			units = f.Units
		default:
			return nil, Exit(ExitRuntime, "plan stream: unexpected frame %T in a dry run", frame.GetFrame())
		}
		if units.GetNodeId() == "" {
			return nil, errors.New("plan frame without node_id; the liaison must stamp every units frame")
		}
		units.Stage = NormalizeStage(units.GetStage())
		result.Frames = append(result.Frames, units)
	}
	if !sawSummary {
		return nil, Exit(ExitRuntime, "plan stream ended without a summary frame")
	}
	unreachable := make(map[string]struct{}, len(result.UnreachableNodes))
	for _, n := range result.UnreachableNodes {
		unreachable[n] = struct{}{}
	}
	answered := make(map[string]struct{}, len(result.AnsweredNodes))
	for _, n := range result.AnsweredNodes {
		answered[n] = struct{}{}
	}
	kept := result.Frames[:0]
	for _, f := range result.Frames {
		if _, dropped := unreachable[f.GetNodeId()]; dropped {
			continue
		}
		if _, ok := answered[f.GetNodeId()]; !ok {
			return nil, Exit(ExitRuntime, "plan stream: units frame from node %s, which the summary lists as neither answered nor unreachable", f.GetNodeId())
		}
		kept = append(kept, f)
	}
	result.Frames = mergeSegmentPieces(kept)
	result.UnreachableNodes = uniqueSorted(result.UnreachableNodes)
	result.AnsweredNodes = uniqueSorted(result.AnsweredNodes)
	return result, nil
}

// mergeSegmentPieces folds the pieces of a segment a data node split across frames back into
// its first piece, so every consumer sees one unit per (node, segment) and one ShardStat per
// shard. Only one segment piece carries segment_level; a shard split into pieces has disjoint
// parts in each and its aggregates on only one (see mergeShardPieces). A frame left without
// units is dropped.
func mergeSegmentPieces(frames []*transferv1.UnitFrame) []*transferv1.UnitFrame {
	first := map[string]*transferv1.SegmentInventory{}
	kept := frames[:0]
	for _, f := range frames {
		units := f.Units[:0]
		for _, u := range f.GetUnits() {
			seg := u.GetSegment()
			if seg == nil {
				units = append(units, u)
				continue
			}
			key := f.GetNodeId() + "\x00" + unitCatalog(u).String() + "\x00" + seg.GetUnit().GetGroup() + "\x00" + seg.GetUnit().GetSegmentSuffix()
			head, ok := first[key]
			if !ok {
				first[key] = seg
				units = append(units, u)
				continue
			}
			head.Unit.ShardIds = append(head.Unit.ShardIds, seg.GetUnit().GetShardIds()...)
			head.Shards = append(head.Shards, seg.GetShards()...)
			if head.SegmentLevel == nil {
				head.SegmentLevel = seg.GetSegmentLevel()
			}
		}
		if f.Units = units; len(units) > 0 {
			kept = append(kept, f)
		}
	}
	for _, seg := range first {
		seg.Unit.ShardIds = slices.Compact(slices.Sorted(slices.Values(seg.Unit.ShardIds)))
		seg.Shards = mergeShardPieces(seg.Shards)
	}
	return kept
}

// mergeShardPieces sorts shards by id and folds the pieces of each shard into one, keeping
// the parts in arrival order. Only one piece of a split shard carries its aggregates and the
// others leave them zero, so adding them up counts the shard's statistics once.
func mergeShardPieces(shards []*transferv1.ShardStat) []*transferv1.ShardStat {
	slices.SortStableFunc(shards, func(a, b *transferv1.ShardStat) int { return cmp.Compare(a.GetShardId(), b.GetShardId()) })
	merged := shards[:0]
	for _, s := range shards {
		last := len(merged) - 1
		if last < 0 || merged[last].GetShardId() != s.GetShardId() {
			merged = append(merged, s)
			continue
		}
		dst := merged[last]
		dst.Parts = append(dst.Parts, s.GetParts()...)
		dst.PartsCount += s.GetPartsCount()
		dst.TotalCount += s.GetTotalCount()
		dst.EstimatedCompressedBytes += s.GetEstimatedCompressedBytes()
		dst.EstimatedUncompressedBytes += s.GetEstimatedUncompressedBytes()
		mergeRange(&dst.MinTimestamp, &dst.MaxTimestamp, s.GetMinTimestamp(), s.GetMaxTimestamp())
	}
	return merged
}

// unitGroup is the group a unit belongs to, whichever kind it is.
func unitGroup(u *transferv1.UnitInventory) string {
	if p := u.GetProperty(); p != nil {
		return p.GetGroup()
	}
	return u.GetSegment().GetUnit().GetGroup()
}

// unitCatalog is the catalog of a unit: PROPERTY for a property inventory, the segment
// unit's catalog otherwise.
func unitCatalog(u *transferv1.UnitInventory) commonv1.Catalog {
	if u.GetProperty() != nil {
		return commonv1.Catalog_CATALOG_PROPERTY
	}
	return u.GetSegment().GetUnit().GetCatalog()
}

// uniqueSorted returns the distinct values in ascending order, or nil for no values.
func uniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := slices.Clone(values)
	slices.Sort(out)
	return slices.Compact(out)
}

// NormalizeStage maps the engine's empty default tier to DefaultStageName.
func NormalizeStage(stage string) string {
	if stage == "" {
		return DefaultStageName
	}
	return stage
}
