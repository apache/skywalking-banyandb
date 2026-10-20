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
	"hash/crc32"
	"io"
	"maps"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// fakeLiaison is a real TCP gRPC server standing in for a liaison: it answers the
// preflight (health, current node, cluster state), the schema registries and ExportService.
// Its create follows the data-node rule: any occupant in listed is refused with
// ALREADY_EXISTS unless the request preempts, in which case every one of them is removed and
// reported in preempted_session_ids. Export runs exportScript per stream.
type fakeLiaison struct {
	databasev1.UnimplementedNodeQueryServiceServer
	transferv1.UnimplementedExportServiceServer
	databasev1.UnimplementedGroupRegistryServiceServer
	databasev1.UnimplementedClusterStateServiceServer
	exportUnits map[string][]*transferv1.ExportRequest // target node -> unit frames received
	canceled    map[string]bool                        // target node -> its stream was canceled
	summary     *transferv1.PlanSummary                // the plan summary; nil names every data node answered
	self        *databasev1.Node
	// exportScript answers one Export stream after init; nil uses defaultExport.
	exportScript        func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error
	sessionID           string
	requireUser         string
	exportInits         []*transferv1.ExportRequest_Init
	groups              []*commonv1.Group
	dataNodes           []*databasev1.Node
	sessionCalls        []*transferv1.SessionsRequest
	planFrames          []*transferv1.PlanResponse
	listed              []*transferv1.SessionsResponse    // ACTION_LIST answer; nil answers none on every data node
	onCreateAttempt     func(f *fakeLiaison, attempt int) // called, under the lock, as a create attempt starts
	released            []string
	plans               []*transferv1.PlanRequest
	hbFrames            []*transferv1.SessionsResponse // ACTION_HEARTBEAT answer; nil answers done on every data node
	releaseFrames       []*transferv1.SessionsResponse // ACTION_RELEASE answer; nil releases on every data node
	schema              fakeSchema
	createAttempts      int
	failCreateTimes     int
	mu                  sync.Mutex
	hbCode              codes.Code
	failAfterFirstFrame bool
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
		sessionID: "feedfeed",
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

// Sessions answers like the liaison fan-out: ACTION_LIST streams listed (or none per data
// node), ACTION_HEARTBEAT fails with hbCode or streams hbFrames (or done per data node), and
// ACTION_RELEASE records the id and streams releaseFrames (or done per data node).
func (f *fakeLiaison) Sessions(req *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	if err := f.checkAuth(stream.Context()); err != nil {
		return err
	}
	f.mu.Lock()
	f.sessionCalls = append(f.sessionCalls, req)
	var frames []*transferv1.SessionsResponse
	perNode := doneFrame
	switch req.GetAction() {
	case transferv1.SessionsRequest_ACTION_LIST:
		frames, perNode = f.listed, noneFrame
	case transferv1.SessionsRequest_ACTION_HEARTBEAT:
		frames = f.hbFrames
	case transferv1.SessionsRequest_ACTION_RELEASE:
		f.released = append(f.released, req.GetSessionId())
		frames = f.releaseFrames
	}
	hbCode := f.hbCode
	f.mu.Unlock()
	if req.GetAction() == transferv1.SessionsRequest_ACTION_HEARTBEAT && hbCode != codes.OK {
		return status.Errorf(hbCode, "data node data-b:17912: export session %s (injected)", req.GetSessionId())
	}
	if frames == nil {
		for _, n := range f.dataNodes {
			frames = append(frames, perNode(n.GetMetadata().GetName()))
		}
	}
	for _, fr := range frames {
		if err := stream.Send(fr); err != nil {
			return err
		}
	}
	return nil
}

// Plan streams planFrames and then, unless skipSummary is set, exactly one summary frame:
// summary when set, otherwise every data node answered. A create first announces the
// session id like the liaison, then applies the occupant rule.
func (f *fakeLiaison) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	if err := f.checkAuth(stream.Context()); err != nil {
		return err
	}
	f.mu.Lock()
	f.plans = append(f.plans, req)
	summary := f.summary
	if summary == nil {
		summary = &transferv1.PlanSummary{}
		for _, n := range f.dataNodes {
			summary.AnsweredNodes = append(summary.AnsweredNodes, n.GetMetadata().GetName())
		}
	}
	var refused []string
	create := req.GetCreate() != nil
	if create {
		f.createAttempts++
		if f.onCreateAttempt != nil {
			f.onCreateAttempt(f, f.createAttempts)
		}
		if f.createAttempts <= f.failCreateTimes {
			attempt := f.createAttempts
			f.mu.Unlock()
			return status.Errorf(codes.Unavailable, "data node data-b:17912 did not reach (injected failure #%d)", attempt)
		}
		summary = proto.Clone(summary).(*transferv1.PlanSummary)
		for _, fr := range f.listed {
			if l := fr.GetSession(); l != nil {
				if req.GetCreate().GetPreempt() {
					summary.PreemptedSessionIds = append(summary.PreemptedSessionIds, l.GetSessionId())
				} else {
					refused = append(refused, l.GetSessionId())
				}
			}
		}
	}
	frames := f.planFrames
	skipSummary := f.skipSummary
	failAfterFirst := f.failAfterFirstFrame
	id := f.sessionID
	f.mu.Unlock()
	if create {
		if err := stream.Send(&transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: id}}}); err != nil {
			return err
		}
		if len(refused) > 0 {
			return status.Errorf(codes.AlreadyExists, "data node data-a:17912: export session %v already exists on this node", refused)
		}
		if failAfterFirst {
			return status.Error(codes.Internal, "data node data-b:17912: snapshot failed")
		}
	}
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

// Export reads init, records it and hands the stream to exportScript (or defaultExport).
func (f *fakeLiaison) Export(stream transferv1.ExportService_ExportServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "the first export frame must be init")
	}
	f.mu.Lock()
	f.exportInits = append(f.exportInits, init)
	script := f.exportScript
	f.mu.Unlock()
	if script == nil {
		script = defaultExport
	}
	err = script(f, init, stream)
	if stream.Context().Err() != nil {
		f.mu.Lock()
		if f.canceled == nil {
			f.canceled = map[string]bool{}
		}
		f.canceled[init.GetTargetNode()] = true
		f.mu.Unlock()
	}
	return err
}

// recvUnit receives the next unit frame and records it; io.EOF ends the stream cleanly.
func (f *fakeLiaison) recvUnit(init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) (*transferv1.ExportRequest, error) {
	req, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.exportUnits == nil {
		f.exportUnits = map[string][]*transferv1.ExportRequest{}
	}
	f.exportUnits[init.GetTargetNode()] = append(f.exportUnits[init.GetTargetNode()], req)
	f.mu.Unlock()
	return req, nil
}

// fakeFile is one file a scripted unit streams.
type fakeFile struct {
	rows     map[string]uint64
	revs     map[string]int64
	path     string
	content  []byte
	shard    uint32
	segLevel bool
	// badCRC announces a wrong CRC-32.
	badCRC bool
}

// filesFor is the default content of a unit: one CSV file per shard (or one segment-level
// file for an index-mode unit), whose rows and schema revisions name the group's first
// resource in the fake schema.
func (f *fakeLiaison) filesFor(req *transferv1.ExportRequest) []fakeFile {
	var catalog, group, seg string
	var shards []uint32
	if p := req.GetProperty(); p != nil {
		catalog, group, shards = "property", p.GetGroup(), p.GetShardIds()
	} else {
		u := req.GetSegment()
		catalog, group, seg, shards = transfer.CatalogName(u.GetCatalog()), u.GetGroup(), u.GetSegmentSuffix(), u.GetShardIds()
	}
	name, revs := f.schema.first(group)
	var out []fakeFile
	for _, sh := range shards {
		p := fmt.Sprintf("%s/%s/shard-%d-00001.csv", catalog, csvfmt.EscapePathComponent(group), sh)
		if seg != "" {
			p = fmt.Sprintf("%s/%s/seg-%s/shard-%d-00001.csv", catalog, csvfmt.EscapePathComponent(group), seg, sh)
		}
		out = append(out, fakeFile{
			path: p, shard: sh, content: []byte(fmt.Sprintf("_name,v\n%s,%d\n%s,%d\n", name, sh, name, sh+1)),
			rows: map[string]uint64{name: 2}, revs: maps.Clone(revs),
		})
	}
	return out
}

// defaultExport answers ready, then streams filesFor of every unit, in two chunks per file.
func defaultExport(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
	if err := sendReady(init, stream); err != nil {
		return err
	}
	for {
		req, err := f.recvUnit(init, stream)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = sendUnit(stream, f.filesFor(req), nil); err != nil {
			return err
		}
	}
}

func sendReady(_ *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
	return stream.Send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Ready{Ready: &transferv1.Ready{}}})
}

// sendUnit streams unit_started (two parts per file), the files (each content split into two
// chunks, the first reporting one row written) and unit_finished with warnings.
func sendUnit(stream transferv1.ExportService_ExportServer, files []fakeFile, warnings []string) error {
	send := func(r *transferv1.ExportResponse) error { return stream.Send(r) }
	started := &transferv1.UnitStarted{TotalShards: uint32(len(files)), TotalParts: uint32(2 * len(files))}
	if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitStarted{UnitStarted: started}}); err != nil {
		return err
	}
	for i, ff := range files {
		if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{
			RelativePath: ff.path, ShardId: ff.shard, SegmentLevel: ff.segLevel, Seq: uint32(i + 1),
		}}}); err != nil {
			return err
		}
		half := len(ff.content) / 2
		for i, part := range [][]byte{ff.content[:half], ff.content[half:]} {
			if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: part, Rows: uint64(i + 1)}}}); err != nil {
				return err
			}
		}
		crc := crc32.ChecksumIEEE(ff.content)
		if ff.badCRC {
			crc++
		}
		if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileFinished{FileFinished: &transferv1.FileFinished{
			Bytes: uint64(len(ff.content)), Crc32: crc, RowCounts: ff.rows, SchemaModRevisions: ff.revs,
		}}}); err != nil {
			return err
		}
	}
	return send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitFinished{UnitFinished: &transferv1.UnitFinished{Warnings: warnings}}})
}

func (f *fakeLiaison) lastPlan() *transferv1.PlanRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.plans) == 0 {
		return nil
	}
	return f.plans[len(f.plans)-1]
}

func (f *fakeLiaison) countSessions(action transferv1.SessionsRequest_Action) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.sessionCalls {
		if r.GetAction() == action {
			n++
		}
	}
	return n
}

func (f *fakeLiaison) releasedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.released...)
}

// fakeSchema is the content of the fake schema registries.
type fakeSchema struct {
	listCalls  []string // "<kind>/<group>" per List call
	streams    []*databasev1.Stream
	measures   []*databasev1.Measure
	traces     []*databasev1.Trace
	properties []*databasev1.Property
	rules      []*databasev1.IndexRule
	bindings   []*databasev1.IndexRuleBinding
	topNs      []*databasev1.TopNAggregation
	mu         sync.Mutex
}

func (s *fakeSchema) record(kind, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls = append(s.listCalls, kind+"/"+group)
}

func (s *fakeSchema) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.listCalls...)
}

// first returns the group's first resource in the fake schema and the mod revision of
// every resource of the group, which a node reports for each file it encodes.
func (s *fakeSchema) first(group string) (string, map[string]int64) {
	var mds []*commonv1.Metadata
	for _, r := range s.streams {
		mds = append(mds, r.GetMetadata())
	}
	for _, r := range s.measures {
		mds = append(mds, r.GetMetadata())
	}
	for _, r := range s.traces {
		mds = append(mds, r.GetMetadata())
	}
	for _, r := range s.properties {
		mds = append(mds, r.GetMetadata())
	}
	name, revs := "unknown", map[string]int64{}
	for _, md := range mds {
		if md.GetGroup() != group || md.GetName() == csvfmt.TopNResultMeasure {
			continue
		}
		if len(revs) == 0 {
			name = md.GetName()
		}
		revs[md.GetName()] = md.GetModRevision()
	}
	return name, revs
}

func inGroup[T interface{ GetMetadata() *commonv1.Metadata }](items []T, group string) []T {
	var out []T
	for _, it := range items {
		if it.GetMetadata().GetGroup() == group {
			out = append(out, it)
		}
	}
	return out
}

type (
	fakeStreamRegistry struct {
		databasev1.UnimplementedStreamRegistryServiceServer
		s *fakeSchema
	}
	fakeMeasureRegistry struct {
		databasev1.UnimplementedMeasureRegistryServiceServer
		s *fakeSchema
	}
	fakeTraceRegistry struct {
		databasev1.UnimplementedTraceRegistryServiceServer
		s *fakeSchema
	}
	fakePropertyRegistry struct {
		databasev1.UnimplementedPropertyRegistryServiceServer
		s *fakeSchema
	}
	fakeIndexRuleRegistry struct {
		databasev1.UnimplementedIndexRuleRegistryServiceServer
		s *fakeSchema
	}
	fakeBindingRegistry struct {
		databasev1.UnimplementedIndexRuleBindingRegistryServiceServer
		s *fakeSchema
	}
	fakeTopNRegistry struct {
		databasev1.UnimplementedTopNAggregationRegistryServiceServer
		s *fakeSchema
	}
)

func (r fakeStreamRegistry) List(_ context.Context, req *databasev1.StreamRegistryServiceListRequest) (*databasev1.StreamRegistryServiceListResponse, error) {
	r.s.record("stream", req.GetGroup())
	return &databasev1.StreamRegistryServiceListResponse{Stream: inGroup(r.s.streams, req.GetGroup())}, nil
}

func (r fakeMeasureRegistry) List(_ context.Context, req *databasev1.MeasureRegistryServiceListRequest) (*databasev1.MeasureRegistryServiceListResponse, error) {
	r.s.record("measure", req.GetGroup())
	return &databasev1.MeasureRegistryServiceListResponse{Measure: inGroup(r.s.measures, req.GetGroup())}, nil
}

func (r fakeTraceRegistry) List(_ context.Context, req *databasev1.TraceRegistryServiceListRequest) (*databasev1.TraceRegistryServiceListResponse, error) {
	r.s.record("trace", req.GetGroup())
	return &databasev1.TraceRegistryServiceListResponse{Trace: inGroup(r.s.traces, req.GetGroup())}, nil
}

func (r fakePropertyRegistry) List(_ context.Context, req *databasev1.PropertyRegistryServiceListRequest) (*databasev1.PropertyRegistryServiceListResponse, error) {
	r.s.record("property", req.GetGroup())
	return &databasev1.PropertyRegistryServiceListResponse{Properties: inGroup(r.s.properties, req.GetGroup())}, nil
}

func (r fakeIndexRuleRegistry) List(_ context.Context, req *databasev1.IndexRuleRegistryServiceListRequest) (*databasev1.IndexRuleRegistryServiceListResponse, error) {
	r.s.record("indexRule", req.GetGroup())
	return &databasev1.IndexRuleRegistryServiceListResponse{IndexRule: inGroup(r.s.rules, req.GetGroup())}, nil
}

func (r fakeBindingRegistry) List(_ context.Context, req *databasev1.IndexRuleBindingRegistryServiceListRequest) (
	*databasev1.IndexRuleBindingRegistryServiceListResponse, error,
) {
	r.s.record("indexRuleBinding", req.GetGroup())
	return &databasev1.IndexRuleBindingRegistryServiceListResponse{IndexRuleBinding: inGroup(r.s.bindings, req.GetGroup())}, nil
}

func (r fakeTopNRegistry) List(_ context.Context, req *databasev1.TopNAggregationRegistryServiceListRequest) (
	*databasev1.TopNAggregationRegistryServiceListResponse, error,
) {
	r.s.record("topNAggregation", req.GetGroup())
	return &databasev1.TopNAggregationRegistryServiceListResponse{TopNAggregation: inGroup(r.s.topNs, req.GetGroup())}, nil
}

func registerSchemaRegistries(srv *grpc.Server, f *fakeLiaison) {
	s := &f.schema
	databasev1.RegisterStreamRegistryServiceServer(srv, fakeStreamRegistry{s: s})
	databasev1.RegisterMeasureRegistryServiceServer(srv, fakeMeasureRegistry{s: s})
	databasev1.RegisterTraceRegistryServiceServer(srv, fakeTraceRegistry{s: s})
	databasev1.RegisterPropertyRegistryServiceServer(srv, fakePropertyRegistry{s: s})
	databasev1.RegisterIndexRuleRegistryServiceServer(srv, fakeIndexRuleRegistry{s: s})
	databasev1.RegisterIndexRuleBindingRegistryServiceServer(srv, fakeBindingRegistry{s: s})
	databasev1.RegisterTopNAggregationRegistryServiceServer(srv, fakeTopNRegistry{s: s})
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
	registerSchemaRegistries(srv, f)
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
