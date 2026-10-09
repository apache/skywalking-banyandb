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
	result.Frames = kept
	result.UnreachableNodes = uniqueSorted(result.UnreachableNodes)
	result.AnsweredNodes = uniqueSorted(result.AnsweredNodes)
	return result, nil
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
