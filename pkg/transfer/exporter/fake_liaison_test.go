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
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// fakeLiaison is a real TCP gRPC server standing in for a liaison: it answers the
// preflight (health, current node, cluster state), the group registry and the two
// ExportService methods the dry-run client drives (Plan without a session, Sessions release).
type fakeLiaison struct {
	databasev1.UnimplementedNodeQueryServiceServer
	databasev1.UnimplementedClusterStateServiceServer
	databasev1.UnimplementedGroupRegistryServiceServer
	transferv1.UnimplementedExportServiceServer
	self                *databasev1.Node
	requireUser         string
	releaseFrames       []*transferv1.SessionsResponse // ACTION_RELEASE answer; nil releases on every data node
	summary             *transferv1.PlanSummary        // the dry-run summary; nil names every data node answered
	groups              []*commonv1.Group
	planFrames          []*transferv1.PlanResponse
	sessionCalls        []*transferv1.SessionsRequest
	dataNodes           []*databasev1.Node
	mu                  sync.Mutex
	failGetClusterState bool
	skipSummary         bool
	healthAuth          bool // the health check also requires requireUser
	noExport            bool // no ExportService, like a server older than 0.12
	hangDescribe        bool // GetCurrentNode never answers until the caller gives up
	failList            bool // the group registry answers UNAVAILABLE
}

func newFakeLiaison() *fakeLiaison {
	return &fakeLiaison{
		self: &databasev1.Node{
			Metadata: &commonv1.Metadata{Name: "liaison-1:17912"},
			Roles:    []databasev1.Role{databasev1.Role_ROLE_LIAISON},
		},
		dataNodes: []*databasev1.Node{
			{Metadata: &commonv1.Metadata{Name: "data-a:17912"}, Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}},
			{Metadata: &commonv1.Metadata{Name: "data-b:17912"}, Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}},
		},
	}
}

func (f *fakeLiaison) checkAuth(ctx context.Context) error {
	if f.requireUser == "" {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get("username"); len(got) != 1 || got[0] != f.requireUser {
		return status.Error(codes.Unauthenticated, "missing or wrong username metadata")
	}
	return nil
}

func (f *fakeLiaison) GetCurrentNode(ctx context.Context, _ *databasev1.GetCurrentNodeRequest) (*databasev1.GetCurrentNodeResponse, error) {
	if err := f.checkAuth(ctx); err != nil {
		return nil, err
	}
	if f.hangDescribe {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &databasev1.GetCurrentNodeResponse{Node: f.self}, nil
}

func (f *fakeLiaison) GetClusterState(ctx context.Context, _ *databasev1.GetClusterStateRequest) (*databasev1.GetClusterStateResponse, error) {
	if err := f.checkAuth(ctx); err != nil {
		return nil, err
	}
	if f.failGetClusterState {
		return nil, status.Error(codes.Unavailable, "cluster state unavailable (injected failure)")
	}
	tables := map[string]*databasev1.RouteTable{}
	if len(f.dataNodes) > 0 {
		tables["tire2"] = &databasev1.RouteTable{Registered: f.dataNodes}
	}
	return &databasev1.GetClusterStateResponse{RouteTables: tables}, nil
}

func (f *fakeLiaison) List(ctx context.Context, _ *databasev1.GroupRegistryServiceListRequest) (*databasev1.GroupRegistryServiceListResponse, error) {
	if err := f.checkAuth(ctx); err != nil {
		return nil, err
	}
	if f.failList {
		return nil, status.Error(codes.Unavailable, "registry unavailable (injected failure)")
	}
	return &databasev1.GroupRegistryServiceListResponse{Group: f.groups}, nil
}

// Sessions answers like the liaison fan-out: it records the request and streams releaseFrames,
// or one done frame per data node when unset.
func (f *fakeLiaison) Sessions(req *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	if err := f.checkAuth(stream.Context()); err != nil {
		return err
	}
	f.mu.Lock()
	f.sessionCalls = append(f.sessionCalls, req)
	frames := f.releaseFrames
	f.mu.Unlock()
	if frames == nil {
		for _, n := range f.dataNodes {
			frames = append(frames, doneFrame(n.GetMetadata().GetName()))
		}
	}
	for _, fr := range frames {
		if err := stream.Send(fr); err != nil {
			return err
		}
	}
	return nil
}

// Plan streams planFrames and then, unless skipSummary is set, exactly one summary frame
// like the liaison's dry run: summary when set, otherwise every data node answered.
func (f *fakeLiaison) Plan(_ *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	if err := f.checkAuth(stream.Context()); err != nil {
		return err
	}
	f.mu.Lock()
	summary := f.summary
	if summary == nil {
		summary = &transferv1.PlanSummary{}
		for _, n := range f.dataNodes {
			summary.AnsweredNodes = append(summary.AnsweredNodes, n.GetMetadata().GetName())
		}
	}
	frames := f.planFrames
	skipSummary := f.skipSummary
	f.mu.Unlock()
	for _, fr := range frames {
		if err := stream.Send(fr); err != nil {
			return err
		}
	}
	if skipSummary {
		return nil
	}
	return stream.Send(summaryFrame(summary))
}

// authHealth is a health server that checks the fake's credentials first.
type authHealth struct {
	*health.Server
	f *fakeLiaison
}

func (h authHealth) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if err := h.f.checkAuth(ctx); err != nil {
		return nil, err
	}
	return h.Server.Check(ctx, req)
}

func (f *fakeLiaison) lastSessions() *transferv1.SessionsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sessionCalls) == 0 {
		return nil
	}
	return f.sessionCalls[len(f.sessionCalls)-1]
}

// serveLiaison starts the fake on 127.0.0.1:0 and returns its address.
func serveLiaison(t *testing.T, f *fakeLiaison) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	databasev1.RegisterNodeQueryServiceServer(srv, f)
	databasev1.RegisterClusterStateServiceServer(srv, f)
	databasev1.RegisterGroupRegistryServiceServer(srv, f)
	if !f.noExport {
		transferv1.RegisterExportServiceServer(srv, f)
	}
	if f.healthAuth {
		grpc_health_v1.RegisterHealthServer(srv, authHealth{Server: health.NewServer(), f: f})
	} else {
		grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	}
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// unitFrame is a `units` frame with one measure segment unit of one shard.
func unitFrame(node, stage, group string, minTS, maxTS int64, rows uint64) *transferv1.PlanResponse {
	seg := &transferv1.SegmentInventory{
		Unit:           &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: group, SegmentSuffix: "20260930", ShardIds: []uint32{0}},
		SegmentVersion: "1.5.0",
		SegmentLevel:   &transferv1.SidxStat{EstimatedBytes: 10},
		Shards: []*transferv1.ShardStat{{
			ShardId: 0, MinTimestamp: minTS, MaxTimestamp: maxTS, TotalCount: rows, PartsCount: 1,
			EstimatedCompressedBytes: 100, EstimatedUncompressedBytes: 300,
		}},
	}
	return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{
		NodeId: node, Stage: stage, Units: []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Segment{Segment: seg}}},
	}}}
}

// segmentOf is the first segment unit of a unitFrame, for tests that tweak it.
func segmentOf(frame *transferv1.PlanResponse) *transferv1.SegmentInventory {
	return frame.GetUnits().GetUnits()[0].GetSegment()
}

// unitFrames unwraps `units` frames into the UnitFrame list a PlanResult carries.
func unitFrames(frames ...*transferv1.PlanResponse) []*transferv1.UnitFrame {
	out := make([]*transferv1.UnitFrame, 0, len(frames))
	for _, f := range frames {
		out = append(out, f.GetUnits())
	}
	return out
}

func summaryFrame(s *transferv1.PlanSummary) *transferv1.PlanResponse {
	return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Summary{Summary: s}}
}

func doneFrame(node string) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{NodeId: node, Outcome: &transferv1.SessionsResponse_Done{Done: &transferv1.Ack{}}}
}

func noneFrame(node string) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{NodeId: node, Outcome: &transferv1.SessionsResponse_None{None: &transferv1.Ack{}}}
}

func errorFrame(node, msg string) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{NodeId: node, Outcome: &transferv1.SessionsResponse_Error{Error: msg}}
}
