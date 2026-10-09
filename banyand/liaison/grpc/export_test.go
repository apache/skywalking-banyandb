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

package grpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer"
)

const (
	testSessionFeedID = "feed"
	testSessionOldID  = "01d1"
	testLocalNodeAddr = "127.0.0.1:17912"
	testNodeA         = "data-a"
	testNodeB         = "data-b"
)

// fakeDataNode is an in-memory ExportService data node: it serves one Plan frame per
// configured group, holds at most one session, and records what the liaison asked of it.
type fakeDataNode struct {
	transferv1.UnimplementedExportServiceServer
	expired       map[string]bool
	lastCreate    *transferv1.PlanRequest
	createErr     error                    // every CreateSession Plan fails with this status
	sessionsErr   error                    // every Sessions call fails with this status
	extraFrame    *transferv1.PlanResponse // sent upstream before the unit frames
	started       chan struct{}
	failAfterCh   <-chan struct{} // with failAfter, Plan fails only once this is closed
	occupant      string
	actionErr     string // every Sessions action answers this error frame instead
	groups        []string
	released      []string
	frameDelay    time.Duration
	startOnce     sync.Once
	mu            sync.Mutex
	closing       bool // every Plan answers Canceled, as a connection closed by the pool does
	stall         bool // Plan sends nothing and blocks until the liaison gives up
	stallSessions bool // Sessions sends nothing and blocks until the liaison gives up
	failAfter     bool // Plan relays every unit frame and then fails as Unavailable
}

func (f *fakeDataNode) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	if f.started != nil {
		f.startOnce.Do(func() { close(f.started) })
	}
	if f.stall {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	f.mu.Lock()
	if f.closing {
		f.mu.Unlock()
		return status.Error(codes.Canceled, "grpc: the client connection is closing")
	}
	if req.GetCreate() != nil {
		f.lastCreate = req
		if f.createErr != nil {
			f.mu.Unlock()
			return f.createErr
		}
		if f.occupant != "" && !req.GetCreate().GetPreempt() {
			occupant := f.occupant
			f.mu.Unlock()
			return status.Errorf(codes.AlreadyExists, "export session %s already exists on this node", occupant)
		}
	}
	if id := req.GetRead().GetId(); req.GetRead() != nil {
		if f.expired[id] {
			f.mu.Unlock()
			return status.Errorf(codes.FailedPrecondition, "export session %s expired", id)
		}
		if f.occupant != id {
			f.mu.Unlock()
			return status.Errorf(codes.NotFound, "export session %s not found on this node", id)
		}
	}
	var preempted []string
	if req.GetCreate() != nil {
		if f.occupant != "" {
			preempted = []string{f.occupant}
		}
		f.occupant = req.GetCreate().GetId()
	}
	f.mu.Unlock()
	if f.extraFrame != nil {
		if err := stream.Send(f.extraFrame); err != nil {
			return err
		}
	}
	for i, g := range f.groups {
		if i > 0 {
			time.Sleep(f.frameDelay)
		}
		if err := stream.Send(unitsFrame(g)); err != nil {
			return err
		}
	}
	if f.failAfter {
		if f.failAfterCh != nil {
			select {
			case <-f.failAfterCh:
			case <-stream.Context().Done():
			}
		}
		return status.Error(codes.Unavailable, "transport is closing")
	}
	if req.GetCreate() != nil {
		// A real node closes its stream with this only when it removed something; sending it
		// always proves the liaison folds it instead of relaying it.
		return stream.Send(summaryFrame(&transferv1.PlanSummary{PreemptedSessionIds: preempted}))
	}
	return nil
}

// Sessions mirrors the data node's handler: ACTION_LIST answers the occupant, ACTION_HEARTBEAT
// answers an error frame for a session this node does not hold and fails for an expired one,
// ACTION_RELEASE clears the occupant and answers done, or none when it holds another session
// or nothing. With actionErr every action answers that
// error frame instead.
func (f *fakeDataNode) Sessions(req *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	if f.stallSessions {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sessionsErr != nil {
		return f.sessionsErr
	}
	id := req.GetSessionId()
	if f.actionErr != "" {
		return stream.Send(errorOutcome(f.actionErr))
	}
	switch req.GetAction() {
	case transferv1.SessionsRequest_ACTION_LIST:
		resp := noneOutcome()
		if f.occupant != "" {
			resp = sessionOutcome(f.occupant)
		}
		return stream.Send(resp)
	case transferv1.SessionsRequest_ACTION_HEARTBEAT:
		if f.expired[id] {
			return status.Errorf(codes.FailedPrecondition, "export session %s expired", id)
		}
		if f.occupant != id {
			return stream.Send(errorOutcome("export session " + id + " not found on this node"))
		}
		return stream.Send(doneOutcome())
	case transferv1.SessionsRequest_ACTION_RELEASE:
		f.released = append(f.released, id)
		if f.occupant != id {
			return stream.Send(&transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_None{None: &transferv1.Ack{}}})
		}
		f.occupant = ""
		return stream.Send(doneOutcome())
	default:
		return status.Errorf(codes.InvalidArgument, "unknown action %s", req.GetAction())
	}
}

func (f *fakeDataNode) releasedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.released...)
}

func (f *fakeDataNode) createRequest() *transferv1.PlanRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastCreate
}

// serveFake serves node over an in-memory connection; a nil node serves no ExportService at
// all, as a data node of an older version does.
func serveFake(t *testing.T, node transferv1.ExportServiceServer) *grpclib.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpclib.NewServer()
	if node != nil {
		transferv1.RegisterExportServiceServer(srv, node)
	}
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpclib.NewClient("passthrough:///bufconn",
		grpclib.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpclib.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// deadConn is a connection whose every RPC fails with Unavailable.
func deadConn(t *testing.T) *grpclib.ClientConn {
	t.Helper()
	conn, err := grpclib.NewClient("passthrough:///dead",
		grpclib.WithContextDialer(func(context.Context, string) (net.Conn, error) { return nil, errors.New("connection refused") }),
		grpclib.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type fakeRouteTable struct{ nodes []string }

func (f fakeRouteTable) GetRouteTable() *databasev1.RouteTable {
	rt := &databasev1.RouteTable{}
	for _, n := range f.nodes {
		rt.Registered = append(rt.Registered, &databasev1.Node{Metadata: &commonv1.Metadata{Name: n}})
	}
	return rt
}

// newFanOut builds an exportService over the given fake nodes plus optional dead nodes,
// whose connections refuse every RPC.
func newFanOut(t *testing.T, nodes map[string]*fakeDataNode, dead ...string) *exportService {
	t.Helper()
	return newFanOutWith(t, nodes, fanOutExtras{dead: dead})
}

// fanOutExtras are the registered data nodes that are not fake ExportService servers.
type fanOutExtras struct {
	wrap     func(node string, c transferv1.ExportServiceClient) transferv1.ExportServiceClient // wraps a fake node's client
	dead     []string                                                                           // every RPC fails with Unavailable
	noClient []string                                                                           // the pool cannot even hand out a client
	old      []string                                                                           // the node serves no ExportService: every RPC answers Unimplemented
}

func newFanOutWith(t *testing.T, nodes map[string]*fakeDataNode, extras fanOutExtras) *exportService {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := queue.NewMockClient(ctrl)
	var names []string
	for name, node := range nodes {
		c := transferv1.NewExportServiceClient(serveFake(t, node))
		if extras.wrap != nil {
			c = extras.wrap(name, c)
		}
		client.EXPECT().NewExportClient(name).Return(c, nil).AnyTimes()
		names = append(names, name)
	}
	for _, name := range extras.dead {
		conn := deadConn(t)
		client.EXPECT().NewExportClient(name).Return(transferv1.NewExportServiceClient(conn), nil).AnyTimes()
		names = append(names, name)
	}
	for _, name := range extras.noClient {
		client.EXPECT().NewExportClient(name).Return(nil, errors.New("node "+name+" is not in the pool")).AnyTimes()
		names = append(names, name)
	}
	for _, name := range extras.old {
		conn := serveFake(t, nil)
		client.EXPECT().NewExportClient(name).Return(transferv1.NewExportServiceClient(conn), nil).AnyTimes()
		names = append(names, name)
	}
	sort.Strings(names)
	svc := newExportService(client, fakeRouteTable{nodes: names}, nil, logger.GetLogger("export-test"))
	svc.newSessionID = func() string { return testSessionFeedID }
	return svc
}

// recordingStream captures what the liaison sends downstream.
type recordingStream struct {
	grpclib.ServerStream
	ctx    context.Context
	frames []*transferv1.PlanResponse
}

func (r *recordingStream) Context() context.Context { return r.ctx }
func (r *recordingStream) Send(f *transferv1.PlanResponse) error {
	r.frames = append(r.frames, f)
	return nil
}

type recordingSessionsStream struct {
	grpclib.ServerStream
	ctx    context.Context
	frames []*transferv1.SessionsResponse
}

func (r *recordingSessionsStream) Context() context.Context { return r.ctx }
func (r *recordingSessionsStream) Send(f *transferv1.SessionsResponse) error {
	r.frames = append(r.frames, f)
	return nil
}

func listReq() *transferv1.SessionsRequest {
	return &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST}
}

func heartbeatReq(id string) *transferv1.SessionsRequest {
	return &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: id}
}

func releaseReq(id string) *transferv1.SessionsRequest {
	return &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id}
}

// sessions runs one Sessions call against svc and returns its frames.
func sessions(t *testing.T, svc *exportService, req *transferv1.SessionsRequest) []*transferv1.SessionsResponse {
	t.Helper()
	rs := &recordingSessionsStream{ctx: context.Background()}
	if err := svc.Sessions(req, rs); err != nil {
		t.Fatalf("Sessions(%+v) = %v", req, err)
	}
	return rs.frames
}

// errorsByNode maps every frame's node to its error text, "" for a done frame. Any other
// outcome fails the test: heartbeat and release answer nothing else.
func errorsByNode(t *testing.T, frames []*transferv1.SessionsResponse) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, f := range frames {
		switch o := f.GetOutcome().(type) {
		case *transferv1.SessionsResponse_Done, *transferv1.SessionsResponse_None:
			// none: a release on a node that did not hold the session, still a success
			out[f.GetNodeId()] = ""
		case *transferv1.SessionsResponse_Error:
			out[f.GetNodeId()] = o.Error
		default:
			t.Fatalf("heartbeat and release frames are done, none or error, got %+v", f)
		}
	}
	return out
}

// lastSummary returns the final frame of a Plan stream, failing unless it is the summary.
func lastSummary(t *testing.T, frames []*transferv1.PlanResponse) *transferv1.PlanSummary {
	t.Helper()
	last := frames[len(frames)-1]
	if last.GetSummary() == nil {
		t.Fatalf("the last frame must be the summary, got %+v", last)
	}
	return last.GetSummary()
}

func summaryFrame(s *transferv1.PlanSummary) *transferv1.PlanResponse {
	return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Summary{Summary: s}}
}

// unitsFrame is the units frame a data node emits for one stream group (node_id empty).
func unitsFrame(group string) *transferv1.PlanResponse {
	return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{Units: []*transferv1.UnitInventory{{
		Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
			Unit: &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: group, SegmentSuffix: "20260928", ShardIds: []uint32{0}},
		}},
	}}}}}
}

func sessionOutcome(id string) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Session{Session: &transferv1.SessionLease{SessionId: id}}}
}

func noneOutcome() *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_None{None: &transferv1.Ack{}}}
}

func doneOutcome() *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Done{Done: &transferv1.Ack{}}}
}

func TestExportPlan_FanOutStampsNodeAndReportsUnreachable(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {groups: []string{"g1", "g2"}},
		testNodeB: {groups: []string{"g1"}},
	}, "data-dead")
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatal(err)
	}
	last := lastSummary(t, stream.frames)
	if got := last.UnreachableNodes; len(got) != 1 || got[0] != "data-dead" {
		t.Fatalf("unreachable = %v", got)
	}
	if got := last.AnsweredNodes; len(got) != 2 || got[0] != testNodeA || got[1] != testNodeB {
		t.Fatalf("answered = %v", got)
	}
	byNode := map[string]int{}
	for _, f := range stream.frames[:len(stream.frames)-1] {
		if f.GetUnits() == nil {
			t.Fatalf("every frame before the summary is a units frame: %+v", f)
		}
		if f.GetUnits().GetNodeId() == "" {
			t.Fatalf("every units frame must be stamped with node_id: %+v", f)
		}
		byNode[f.GetUnits().GetNodeId()]++
	}
	if byNode[testNodeA] != 2 || byNode[testNodeB] != 1 {
		t.Fatalf("frames per node = %v, want a:2 b:1", byNode)
	}
}

func TestExportPlan_NodeErrorIsFatalNotUnreachable(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {groups: []string{"g"}, occupant: "abcd"},
		testNodeB: {expired: map[string]bool{"abcd": true}},
	})
	err := svc.Plan(transfer.ReadRequest("abcd"), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), testNodeB) {
		t.Fatalf("an expired session must fail the whole call naming the node, got %v", err)
	}
}

func TestExportPlan_StandaloneUsesLocalPlanner(t *testing.T) {
	local := &fakeLocalPlanner{frames: 2, preempted: []string{"old"}}
	svc := newLocalExportService(local, testLocalNodeAddr)
	svc.newSessionID = func() string { return testSessionFeedID }

	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.frames) != 3 || stream.frames[0].GetUnits().GetNodeId() != testLocalNodeAddr || stream.frames[1].GetUnits().GetNodeId() != testLocalNodeAddr {
		t.Fatalf("local frames must be stamped with the liaison's own node id: %+v", stream.frames)
	}
	if got := lastSummary(t, stream.frames).AnsweredNodes; len(got) != 1 || got[0] != testLocalNodeAddr {
		t.Fatalf("answered = %v", got)
	}

	created := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(transfer.CreateRequest(nil, false), created); err != nil {
		t.Fatal(err)
	}
	assertAnnouncesSession(t, created.frames[0])
	if local.lastReq.GetCreate().GetId() != testSessionFeedID {
		t.Fatalf("local create must pass the announced id to the planner, got %q", local.lastReq.GetCreate().GetId())
	}
	if got := lastSummary(t, created.frames).PreemptedSessionIds; len(got) != 1 || got[0] != "old" {
		t.Fatalf("preempted ids must move to the final frame: %+v", lastSummary(t, created.frames))
	}
	for _, f := range created.frames[1 : len(created.frames)-1] {
		if f.GetUnits() == nil {
			t.Fatalf("the planner's summary frame must not be forwarded: %+v", f)
		}
	}
}

// fakeLocalPlanner is a standalone process's own ExportService, which the local pipeline
// serves in-process.
type fakeLocalPlanner struct {
	transferv1.UnimplementedExportServiceServer
	listErr      error  // ACTION_LIST fails with this status, as the data node's handler would
	listFrameErr string // ACTION_LIST answers an error frame with this text
	releaseErr   error  // ACTION_RELEASE reports this in the frame's error
	planErr      error
	lastReq      *transferv1.PlanRequest
	released     string
	preempted    []string
	frames       int
	stall        bool // emit nothing and block until the liaison gives up
}

func (f *fakeLocalPlanner) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	f.lastReq = req
	if f.stall {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if f.planErr != nil {
		return f.planErr
	}
	for i := 0; i < f.frames; i++ {
		if err := stream.Send(unitsFrame("g")); err != nil {
			return err
		}
	}
	if req.GetCreate() != nil {
		return stream.Send(summaryFrame(&transferv1.PlanSummary{PreemptedSessionIds: f.preempted}))
	}
	return nil
}

func (f *fakeLocalPlanner) Sessions(req *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	frame := doneOutcome()
	switch req.GetAction() {
	case transferv1.SessionsRequest_ACTION_LIST:
		if f.listErr != nil {
			return f.listErr
		}
		frame = sessionOutcome("local")
		if f.listFrameErr != "" {
			frame = errorOutcome(f.listFrameErr)
		}
	case transferv1.SessionsRequest_ACTION_RELEASE:
		f.released = req.GetSessionId()
		if f.releaseErr != nil {
			frame = errorOutcome(f.releaseErr.Error())
		}
	default:
	}
	return stream.Send(frame)
}

// newLocalExportService is a standalone liaison: no tire2 route table, and a local pipeline
// that serves planner in-process as the node named self.
func newLocalExportService(planner transferv1.ExportServiceServer, self string) *exportService {
	q := queue.Local()
	q.SetExportServer(planner)
	return newExportService(q, nil, func() string { return self }, logger.GetLogger("export-test"))
}

func TestExportPlan_NoDataNodesIsFailedPrecondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc := newExportService(queue.NewMockClient(ctrl), fakeRouteTable{}, nil, logger.GetLogger("export-test"))
	err := svc.Plan(&transferv1.PlanRequest{}, &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

func TestExportPlan_InvalidCombinations(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {}})
	for _, req := range []*transferv1.PlanRequest{
		{Session: &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{Id: "abcd"}}}, // the liaison generates the id
		{Session: &transferv1.PlanRequest_Read{Read: &transferv1.ReadSession{}}},                 // needs the session id
		{Session: &transferv1.PlanRequest_Read{Read: &transferv1.ReadSession{Id: "NOT-HEX"}}},
	} {
		if err := svc.Plan(req, &recordingStream{ctx: context.Background()}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%+v must be InvalidArgument, got %v", req, err)
		}
	}
}

func TestExportPlan_CreateSessionFirstFrameAndFanOut(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}}, &fakeDataNode{groups: []string{"g"}}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(transfer.CreateRequest(nil, false), stream); err != nil {
		t.Fatal(err)
	}
	assertAnnouncesSession(t, stream.frames[0])
	if a.createRequest().GetCreate().GetId() != testSessionFeedID || b.createRequest().GetCreate().GetId() != testSessionFeedID {
		t.Fatal("every node must receive the same generated session id with CreateSession")
	}
	last := lastSummary(t, stream.frames)
	if len(last.UnreachableNodes) != 0 || len(last.AnsweredNodes) != 2 {
		t.Fatalf("create mode never degrades: %+v", last)
	}
	for _, f := range stream.frames[1 : len(stream.frames)-1] {
		if f.GetUnits() == nil {
			t.Fatalf("node summary frames must not be forwarded: %+v", f)
		}
	}
}

func TestExportPlan_CreateSessionRollsBackOnOccupant(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}}, &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), testSessionOldID) {
		t.Fatalf("want AlreadyExists naming the occupant, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("liaison must release the new session on the node that accepted it, got %v", got)
	}
	if b.occupant != testSessionOldID {
		t.Fatal("the occupant must survive a failed attempt")
	}
}

// A node whose connection the pool closed mid-call (the liaison evicting a node that just
// went down) answers Canceled; a live Plan degrades it to unreachable like any other transport
// failure, while a CreateSession Plan stays strict and rolls back.
func TestExportPlan_ClosingConnectionIsUnreachable(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}}, &fakeDataNode{closing: true}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	rs := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, rs); err != nil {
		t.Fatalf("a closing upstream connection must not fail a live plan: %v", err)
	}
	last := lastSummary(t, rs.frames)
	if got := last.GetUnreachableNodes(); len(got) != 1 || got[0] != testNodeB {
		t.Fatalf("unreachable = %v, want [data-b]", got)
	}
	if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA {
		t.Fatalf("answered = %v, want [data-a]", got)
	}

	rs = &recordingStream{ctx: context.Background()}
	err := svc.Plan(transfer.CreateRequest(nil, false), rs)
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), testNodeB) {
		t.Fatalf("CreateSession must stay strict and report transport failures as Unavailable, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("the accepted node must be rolled back, got %v", got)
	}
}

func TestExportPlan_CreateSessionUnreachableIsFatal(t *testing.T) {
	a := &fakeDataNode{groups: []string{"g"}}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a}, "data-dead")
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 {
		t.Fatalf("the accepted node must be rolled back, got %v", got)
	}
	if !strings.Contains(err.Error(), "snapshots released") || strings.Contains(err.Error(), "releasing it failed") {
		t.Fatalf("a node whose Plan never opened holds nothing and must not be reported as leaked: %v", err)
	}
}

func TestExportPlan_PreemptedIdsAggregated(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}, &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(transfer.CreateRequest(nil, true), stream); err != nil {
		t.Fatal(err)
	}
	if got := lastSummary(t, stream.frames).PreemptedSessionIds; len(got) != 1 || got[0] != testSessionOldID {
		t.Fatalf("preempted ids must be deduplicated across nodes: %v", got)
	}
	assertAnnouncesSession(t, stream.frames[0])
}

// assertAnnouncesSession checks that a CreateSession first frame is the created frame
// carrying the generated id.
func assertAnnouncesSession(t *testing.T, first *transferv1.PlanResponse) {
	t.Helper()
	if first.GetCreated() == nil || first.GetCreated().GetSessionId() != testSessionFeedID {
		t.Fatalf("first frame must be the created frame announcing the session: %+v", first)
	}
}

func TestExportSessions_ListAndReleaseFanOut(t *testing.T) {
	a, b := &fakeDataNode{occupant: testSessionOldID}, &fakeDataNode{}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	listed := sessions(t, svc, listReq())
	if len(listed) != 2 || listed[0].NodeId != testNodeA || listed[1].NodeId != testNodeB {
		t.Fatalf("one list frame per node in node order, got %+v", listed)
	}
	if listed[0].GetSession().GetSessionId() != testSessionOldID || listed[1].GetNone() == nil {
		t.Fatalf("list frames are session or none: %+v", listed)
	}
	released := sessions(t, svc, releaseReq(testSessionOldID))
	if got := errorsByNode(t, released); len(got) != 2 || got[testNodeA] != "" || got[testNodeB] != "" {
		t.Fatalf("release frames = %+v", released)
	}
	if a.releasedIDs()[0] != testSessionOldID || b.releasedIDs()[0] != testSessionOldID {
		t.Fatal("release must reach every node")
	}
}

func TestExportSessions_InvalidRequests(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {}})
	for _, req := range []*transferv1.SessionsRequest{
		{},                              // the action is required
		heartbeatReq(""),                // heartbeat needs the session id
		releaseReq(""),                  // release needs the session id
		releaseReq("NOT-HEX"),           // the id rule
		{Action: 99, SessionId: "abcd"}, // an unknown action
	} {
		if err := svc.Sessions(req, &recordingSessionsStream{ctx: context.Background()}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%+v must be InvalidArgument, got %v", req, err)
		}
	}
	// ACTION_LIST ignores the id.
	if frames := sessions(t, svc, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST, SessionId: "abcd"}); len(frames) != 1 {
		t.Fatalf("list frames = %+v", frames)
	}
}

func TestExportSessions_ListUnreachableFails(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {}}, "data-dead")
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "data-dead") {
		t.Fatalf("want Unavailable naming data-dead, got %v", err)
	}
}

// A release relays each node's own outcome: done where the node held the session, none where
// it did not, so the client can tell an unknown session from a released one.
func TestExportSessions_ReleaseRelaysDoneAndNone(t *testing.T) {
	a := &fakeDataNode{occupant: testSessionOldID}
	b := &fakeDataNode{}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	frames := sessions(t, svc, releaseReq(testSessionOldID))
	got := map[string]string{}
	for _, f := range frames {
		switch f.GetOutcome().(type) {
		case *transferv1.SessionsResponse_Done:
			got[f.GetNodeId()] = "done"
		case *transferv1.SessionsResponse_None:
			got[f.GetNodeId()] = "none"
		default:
			t.Fatalf("unexpected release frame %+v", f)
		}
	}
	if len(got) != 2 || got[testNodeA] != "done" || got[testNodeB] != "none" {
		t.Fatalf("want done on the holder and none elsewhere, got %v", got)
	}
}

func TestExportSessions_ReleaseReportsFailedNodeInFrame(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {}}, "data-dead")
	got := errorsByNode(t, sessions(t, svc, releaseReq(testSessionOldID)))
	if len(got) != 2 || got[testNodeA] != "" || got["data-dead"] == "" {
		t.Fatalf("an unreachable node must be reported in its own frame, not fail the release: %v", got)
	}
}

// ACTION_HEARTBEAT: a node that does not hold the session or cannot be reached answers an
// error frame and the call succeeds, so the client reports the gap and the next tick
// retries; an expired session fails the call, which stops the heartbeat.
func TestExportSessions_HeartbeatShapes(t *testing.T) {
	const id = "abcd01ef"
	a, b := &fakeDataNode{occupant: id}, &fakeDataNode{occupant: id}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b}, "data-dead")
	got := errorsByNode(t, sessions(t, svc, heartbeatReq(id)))
	if len(got) != 3 || got[testNodeA] != "" || got[testNodeB] != "" || got["data-dead"] == "" {
		t.Fatalf("heartbeat frames = %v, want an error only for the unreachable node", got)
	}

	b.mu.Lock()
	b.occupant = ""
	b.mu.Unlock()
	got = errorsByNode(t, sessions(t, svc, heartbeatReq(id)))
	if got[testNodeA] != "" || !strings.Contains(got[testNodeB], "not found on this node") {
		t.Fatalf("a node that lost the session answers an error frame, got %v", got)
	}

	b.expired = map[string]bool{id: true}
	err := svc.Sessions(heartbeatReq(id), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "data node data-b: export session "+id+" expired") {
		t.Fatalf("an expired session must fail the heartbeat naming the node, got %v", err)
	}
}

func TestExportLocal_ListAndRelease(t *testing.T) {
	local := &fakeLocalPlanner{}
	svc := newLocalExportService(local, roleLabelSelf)
	listed := sessions(t, svc, listReq())
	if len(listed) != 1 || listed[0].NodeId != roleLabelSelf || listed[0].GetSession().GetSessionId() != "local" {
		t.Fatalf("local list = %+v", listed)
	}
	released := sessions(t, svc, releaseReq("abcd"))
	if len(released) != 1 || released[0].NodeId != roleLabelSelf || released[0].GetDone() == nil || local.released != "abcd" {
		t.Fatalf("local release = %+v, released %q", released, local.released)
	}
}

// erroringStream lets the first Send succeed and fails every subsequent one, simulating a
// client that disconnects after receiving the first frame.
type erroringStream struct {
	grpclib.ServerStream
	ctx     context.Context
	sendErr error
	frames  []*transferv1.PlanResponse
	count   int
	mu      sync.Mutex
}

func (r *erroringStream) Context() context.Context { return r.ctx }
func (r *erroringStream) Send(f *transferv1.PlanResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count > 0 {
		return r.sendErr
	}
	r.count++
	r.frames = append(r.frames, f)
	return nil
}

func TestExportPlan_DownstreamSendFailureAbortsAndDoesNotHang(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {groups: []string{"g1", "g2"}},
		testNodeB: {groups: []string{"g1"}},
	})
	stream := &erroringStream{ctx: context.Background(), sendErr: status.Error(codes.Unavailable, "client disconnected")}
	done := make(chan error, 1)
	//panicdiag:allow-rawgo deadline guard for goroutine-leak detection
	go func() { done <- svc.Plan(&transferv1.PlanRequest{}, stream) }()
	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "client disconnected") {
			t.Fatalf("Plan must return the downstream Send error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Plan did not return within deadline; possible goroutine leak")
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.frames) != 1 || stream.frames[0].GetUnits() == nil {
		t.Fatalf("only the first units frame may reach the client and no summary, got %+v", stream.frames)
	}
}

func TestExportPlan_CreateSessionPartialNodeFailureRollsBack(t *testing.T) {
	a := &fakeDataNode{groups: []string{"g"}}
	b := &fakeDataNode{createErr: status.Error(codes.Internal, "snapshot failed")}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want codes.Internal when a data node fails CreateSession, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("session must be rolled back on the node that accepted it, got %v", got)
	}
	a.mu.Lock()
	occ := a.occupant
	a.mu.Unlock()
	if occ != "" {
		t.Fatalf("node A occupant must be cleared after rollback, got %q", occ)
	}
}

func TestExportLocal_ErrorPropagation(t *testing.T) {
	// A local release failure is reported like the fan-out does: an error frame, no RPC error.
	svc1 := newLocalExportService(&fakeLocalPlanner{releaseErr: errors.New("disk full")}, roleLabelSelf)
	released := sessions(t, svc1, releaseReq("abcd"))
	if len(released) != 1 || released[0].NodeId != roleLabelSelf || released[0].GetError() != "disk full" {
		t.Fatalf("local release failure must surface in the liaison's own frame, got %+v", released)
	}

	// A local list error keeps the data node's code and gains the node prefix.
	svc2 := newLocalExportService(&fakeLocalPlanner{listErr: status.Error(codes.Internal, "list export sessions: io error")}, roleLabelSelf)
	err := svc2.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node "+roleLabelSelf+": list export sessions: io error") {
		t.Fatalf("a local list error must keep codes.Internal and name the node, got %v", err)
	}

	// A non-create Plan error keeps its code and gains the node prefix, as the fan-out does.
	svc3 := newLocalExportService(&fakeLocalPlanner{planErr: status.Error(codes.FailedPrecondition, "export session abcd expired")}, roleLabelSelf)
	err = svc3.Plan(transfer.ReadRequest("abcd"), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "data node "+roleLabelSelf+": export session abcd expired") {
		t.Fatalf("local plan error must keep its code and name the node, got %v", err)
	}
}

// The standalone path is the fan-out itself: a NotFound from the process's own ExportService degrades
// the liaison itself to unreachable_nodes for ReadSession, and stays fatal (with the session
// rolled back) for CreateSession.
func TestExportLocal_NotFoundDegradesUnlessStrict(t *testing.T) {
	local := &fakeLocalPlanner{planErr: status.Error(codes.NotFound, "export session abcd not found on this node")}
	svc := newLocalExportService(local, roleLabelSelf)
	svc.newSessionID = func() string { return testSessionFeedID }
	{
		req := transfer.ReadRequest("abcd")
		stream := &recordingStream{ctx: context.Background()}
		if err := svc.Plan(req, stream); err != nil {
			t.Fatalf("%+v: NotFound must not fail a non-strict local plan: %v", req, err)
		}
		if len(stream.frames) != 1 {
			t.Fatalf("%+v: want only the summary frame, got %+v", req, stream.frames)
		}
		last := lastSummary(t, stream.frames)
		if got := last.GetUnreachableNodes(); len(got) != 1 || got[0] != roleLabelSelf {
			t.Fatalf("%+v: unreachable = %v, want [%s]", req, got, roleLabelSelf)
		}
		if len(last.GetAnsweredNodes()) != 0 {
			t.Fatalf("%+v: the degraded summary must carry no answered nodes: %+v", req, last)
		}
	}
	if local.released != "" {
		t.Fatalf("a degraded read must not release anything, released %q", local.released)
	}

	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), "create export session "+testSessionFeedID+" aborted") {
		t.Fatalf("create mode must keep NotFound fatal and roll back, got %v", err)
	}
	if local.released != testSessionFeedID {
		t.Fatalf("the failed create must be rolled back, released %q", local.released)
	}
}

func TestExportLocal_PlanCreateSessionRollsBackOnPlanError(t *testing.T) {
	local := &fakeLocalPlanner{planErr: status.Error(codes.Internal, "snapshot failed")}
	svc := newLocalExportService(local, roleLabelSelf)
	svc.newSessionID = func() string { return testSessionFeedID }

	stream := &recordingStream{ctx: context.Background()}
	err := svc.Plan(transfer.CreateRequest(nil, false), stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("the abort must keep the planner's code, got %v", err)
	}
	if len(stream.frames) < 1 || stream.frames[0].GetCreated().GetSessionId() != testSessionFeedID {
		t.Fatalf("first frame must announce the session id before any error, got %+v", stream.frames)
	}
	if local.released != testSessionFeedID {
		t.Fatalf("a failed local create must release the announced session id; got %q, want %q", local.released, testSessionFeedID)
	}
	want := "create export session " + testSessionFeedID + " aborted, snapshots released: data node " + roleLabelSelf + ": snapshot failed"
	if err.Error() != "rpc error: code = Internal desc = "+want {
		t.Fatalf("local abort text must match the fan-out:\n got %v\nwant %s", err, want)
	}

	// When the local release fails too, the liaison itself is the leaked node.
	local.releaseErr = errors.New("disk full")
	err = svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	wantLeak := "releasing it failed on [" + roleLabelSelf + "], release session " + testSessionFeedID + " by hand"
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), wantLeak) {
		t.Fatalf("a failed local release must name the liaison as leaked, got %v", err)
	}
}

// A node that fails after relaying unit frames did not finish, so it belongs to
// unreachable_nodes and not to answered_nodes; the client drops its partial rows.
func TestExportPlan_NodeFailingAfterFramesIsUnreachable(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {groups: []string{"g"}},
		testNodeB: {groups: []string{"g"}, failAfter: true},
	})
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatalf("a transport failure after frames must degrade, not fail: %v", err)
	}
	last := lastSummary(t, stream.frames)
	if got := last.GetUnreachableNodes(); len(got) != 1 || got[0] != testNodeB {
		t.Fatalf("unreachable = %v, want [data-b]", got)
	}
	if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA {
		t.Fatalf("answered = %v, want [data-a]", got)
	}
	relayed := map[string]int{}
	for _, f := range stream.frames[:len(stream.frames)-1] {
		relayed[f.GetUnits().GetNodeId()]++
	}
	if relayed[testNodeA] != 1 || relayed[testNodeB] != 1 {
		t.Fatalf("the failed node's frames must still be relayed with its node_id, got %v", relayed)
	}
}

func shortenPlanNodeMaxWait(t *testing.T, window time.Duration) {
	t.Helper()
	saved := exportPlanNodeMaxWait
	exportPlanNodeMaxWait = window
	t.Cleanup(func() { exportPlanNodeMaxWait = saved })
}

// A node that stays silent past the Plan max wait is cut off by the liaison itself: a dry run
// or ReadSession reports it as unreachable, while a CreateSession fails with DeadlineExceeded
// and rolls back.
func TestExportPlan_MaxWaitDegradesUnlessStrict(t *testing.T) {
	shortenPlanNodeMaxWait(t, 500*time.Millisecond)
	a, b := &fakeDataNode{groups: []string{"g"}}, &fakeDataNode{stall: true}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatalf("a silent node must not fail a dry run: %v", err)
	}
	last := lastSummary(t, stream.frames)
	if got := last.GetUnreachableNodes(); len(got) != 1 || got[0] != testNodeB {
		t.Fatalf("unreachable = %v, want [data-b]", got)
	}
	if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA {
		t.Fatalf("answered = %v, want [data-a]", got)
	}

	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "data node data-b: no frame for 500ms") {
		t.Fatalf("want DeadlineExceeded naming the silent node, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("the create attempt must be rolled back on the node that answered, got %v", got)
	}

	local := newLocalExportService(&fakeLocalPlanner{stall: true}, roleLabelSelf)
	stream = &recordingStream{ctx: context.Background()}
	if err = local.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatalf("a silent local export service must not fail a dry run: %v", err)
	}
	if got := lastSummary(t, stream.frames).GetUnreachableNodes(); len(got) != 1 || got[0] != roleLabelSelf {
		t.Fatalf("the local export service is bound by the same Plan max wait, unreachable = %v", got)
	}
}

// The Plan max wait restarts with every frame: a node whose frames each arrive within it
// answers even when the whole plan takes longer than the window.
func TestExportPlan_MaxWaitResetsPerFrame(t *testing.T) {
	shortenPlanNodeMaxWait(t, 250*time.Millisecond)
	a := &fakeDataNode{groups: []string{"g1", "g2", "g3", "g4", "g5", "g6"}, frameDelay: 100 * time.Millisecond}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a})
	stream := &recordingStream{ctx: context.Background()}
	start := time.Now()
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 2*exportPlanNodeMaxWait {
		t.Fatalf("the plan must outlast two max waits to prove the reset, took %v", elapsed)
	}
	last := lastSummary(t, stream.frames)
	if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA || len(last.GetUnreachableNodes()) != 0 {
		t.Fatalf("a node streaming within the window must answer: %+v", last)
	}
	if len(stream.frames) != 7 {
		t.Fatalf("want 6 units frames and the summary, got %d frames", len(stream.frames))
	}
}

// slowSendStream blocks every Send for delay, as a slow downstream client does.
type slowSendStream struct {
	recordingStream
	delay time.Duration
}

func (r *slowSendStream) Send(f *transferv1.PlanResponse) error {
	time.Sleep(r.delay)
	return r.recordingStream.Send(f)
}

// The Plan max wait is paused while a frame waits on the downstream client, so a client slower
// than the window is never blamed on the node.
func TestExportPlan_SlowDownstreamIsNotUnreachable(t *testing.T) {
	shortenPlanNodeMaxWait(t, 200*time.Millisecond)
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {groups: []string{"g1", "g2"}}})
	stream := &slowSendStream{recordingStream: recordingStream{ctx: context.Background()}, delay: 500 * time.Millisecond}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatal(err)
	}
	last := lastSummary(t, stream.frames)
	if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA || len(last.GetUnreachableNodes()) != 0 {
		t.Fatalf("a slow client must not mark the node unreachable: %+v", last)
	}
	if len(stream.frames) != 3 {
		t.Fatalf("want 2 units frames and the summary, got %d frames", len(stream.frames))
	}
}

// eofSignalingClient closes eof once a Plan stream it opened ends, that is after the liaison
// handled every frame of it, summary included.
type eofSignalingClient struct {
	transferv1.ExportServiceClient
	eof chan struct{}
}

func (c eofSignalingClient) Plan(ctx context.Context, in *transferv1.PlanRequest,
	opts ...grpclib.CallOption,
) (grpclib.ServerStreamingClient[transferv1.PlanResponse], error) {
	up, err := c.ExportServiceClient.Plan(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return eofSignalingStream{ServerStreamingClient: up, eof: c.eof}, nil
}

type eofSignalingStream struct {
	grpclib.ServerStreamingClient[transferv1.PlanResponse]
	eof chan struct{}
}

func (s eofSignalingStream) Recv() (*transferv1.PlanResponse, error) {
	frame, err := s.ServerStreamingClient.Recv()
	if errors.Is(err, io.EOF) {
		close(s.eof)
	}
	return frame, err
}

// A create that fails after one node finished, preempting a session there, names the preempted
// session, which is gone even though the new one was rolled back, and warns that the node that
// did not finish may have removed sessions too. Node b fails only once the liaison handled node
// a's whole stream.
func TestExportPlan_CreateSessionAbortNamesPreempted(t *testing.T) {
	aDone := make(chan struct{})
	a := &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}
	b := &fakeDataNode{groups: []string{"g1"}, failAfter: true, failAfterCh: aDone}
	svc := newFanOutWith(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b}, fanOutExtras{
		wrap: func(node string, c transferv1.ExportServiceClient) transferv1.ExportServiceClient {
			if node != testNodeA {
				return c
			}
			return eofSignalingClient{ExportServiceClient: c, eof: aDone}
		},
	})
	err := svc.Plan(transfer.CreateRequest(nil, true), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "preempted sessions ["+testSessionOldID+"] were already removed") {
		t.Fatalf("want Unavailable naming the preempted session, got %v", err)
	}
	if !strings.Contains(err.Error(), "nodes that did not finish may also have removed preempted sessions, check with ACTION_LIST") {
		t.Fatalf("an unfinished node may have preempted sessions too, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("the failed create must be rolled back, got %v", got)
	}
}

// A node that never held the session answers NotFound: a coverage gap for a ReadSession Plan
// (the client reports it as unreachable), still fatal when creating.
func TestExportPlan_NotFoundDegradesUnlessStrict(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}, occupant: "abcd"}, &fakeDataNode{}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	{
		req := transfer.ReadRequest("abcd")
		stream := &recordingStream{ctx: context.Background()}
		if err := svc.Plan(req, stream); err != nil {
			t.Fatalf("%+v: NotFound must not fail a non-strict plan: %v", req, err)
		}
		last := lastSummary(t, stream.frames)
		if got := last.GetUnreachableNodes(); len(got) != 1 || got[0] != testNodeB {
			t.Fatalf("%+v: unreachable = %v, want [data-b]", req, got)
		}
		if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA {
			t.Fatalf("%+v: answered = %v, want [data-a]", req, got)
		}
	}

	a.mu.Lock()
	a.occupant = ""
	a.mu.Unlock()
	b.createErr = status.Error(codes.NotFound, "no such thing")
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), "data node data-b") {
		t.Fatalf("create mode must keep NotFound fatal, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("the accepted node must be rolled back, got %v", got)
	}
}

// Canceling the downstream stream mid-create releases the session on every node whose Plan
// stream was opened, and the error carries the context's own code rather than Unknown.
func TestExportPlan_CreateSessionCanceledDownstreamReleasesEveryNode(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}}, &fakeDataNode{stall: true, started: make(chan struct{})}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//panicdiag:allow-rawgo test driver cancels once the stalled node has opened its stream
	go func() {
		<-b.started
		cancel()
	}()
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: ctx})
	if status.Code(err) != codes.Canceled || !strings.Contains(err.Error(), "create export session "+testSessionFeedID+" aborted, snapshots released") {
		t.Fatalf("want Canceled with the abort text, got %v", err)
	}
	for name, node := range map[string]*fakeDataNode{testNodeA: a, testNodeB: b} {
		if got := node.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
			t.Fatalf("%s must be released after a canceled create, got %v", name, got)
		}
	}
}

func TestExportSessions_ListNodeErrorKeepsCode(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {},
		testNodeB: {sessionsErr: status.Error(codes.PermissionDenied, "not allowed")},
	})
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "data node data-b: not allowed") {
		t.Fatalf("a node that answers with an error must fail the list with its own code, got %v", err)
	}
}

// summaryFailingStream records every frame and fails the summary, as a client that drops
// right before the end of a CreateSession does.
type summaryFailingStream struct {
	recordingStream
	mu sync.Mutex
}

func (r *summaryFailingStream) Send(f *transferv1.PlanResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f.GetSummary() != nil {
		return status.Error(codes.Unavailable, "client went away")
	}
	return r.recordingStream.Send(f)
}

// Without the summary the client cannot use the session, so a failed final Send rolls the
// session back on every node like any other failure.
func TestExportPlan_CreateSessionSummarySendFailureRollsBack(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}}, &fakeDataNode{groups: []string{"g"}}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	err := svc.Plan(transfer.CreateRequest(nil, false), &summaryFailingStream{recordingStream: recordingStream{ctx: context.Background()}})
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "create export session "+testSessionFeedID+" aborted, snapshots released: client went away") {
		t.Fatalf("want Unavailable with the abort text, got %v", err)
	}
	for name, node := range map[string]*fakeDataNode{testNodeA: a, testNodeB: b} {
		if got := node.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
			t.Fatalf("%s must be released when the summary cannot be sent, got %v", name, got)
		}
	}
}

// A node the pool cannot hand a client for never saw the request: a dry run reports it as
// unreachable, and a failed create neither releases nor reports it as leaked even when other
// nodes already failed the call.
func TestExportPlan_NodeWithoutClient(t *testing.T) {
	a := &fakeDataNode{groups: []string{"g"}}
	svc := newFanOutWith(t, map[string]*fakeDataNode{testNodeA: a}, fanOutExtras{noClient: []string{"data-gone1", "data-gone2"}})
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatal(err)
	}
	last := lastSummary(t, stream.frames)
	if got := last.GetUnreachableNodes(); strings.Join(got, ",") != "data-gone1,data-gone2" {
		t.Fatalf("unreachable = %v, want both nodes without a client", got)
	}
	if got := last.GetAnsweredNodes(); len(got) != 1 || got[0] != testNodeA {
		t.Fatalf("answered = %v, want [data-a]", got)
	}

	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "aborted, snapshots released") || strings.Contains(err.Error(), "releasing it failed") {
		t.Fatalf("nodes that never got a client must not be reported as leaked, got %v", err)
	}
	if !strings.Contains(err.Error(), "is not in the pool") || strings.Count(err.Error(), "rpc error") != 1 {
		t.Fatalf("the pool error must appear once, without a nested status prefix, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("the accepted node must be rolled back, got %v", got)
	}
}

// A node that releases with an error is named in the abort as leaked.
func TestExportPlan_CreateSessionReleaseFailureIsLeaked(t *testing.T) {
	a := &fakeDataNode{groups: []string{"g"}, actionErr: "remove lease: busy"}
	b := &fakeDataNode{createErr: status.Error(codes.Internal, "snapshot failed")}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	wantLeak := "releasing it failed on [" + testNodeA + "], release session " + testSessionFeedID + " by hand"
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), wantLeak) {
		t.Fatalf("want Internal naming data-a as leaked, got %v", err)
	}

	got := errorsByNode(t, sessions(t, svc, releaseReq(testSessionFeedID)))
	if got[testNodeA] != "remove lease: busy" || got[testNodeB] != "" {
		t.Fatalf("release must answer the node's error frame, got %v", got)
	}
	got = errorsByNode(t, sessions(t, svc, heartbeatReq(testSessionFeedID)))
	if got[testNodeA] != "remove lease: busy" {
		t.Fatalf("heartbeat must answer the node's error frame, got %v", got)
	}
}

// A data node of an older version serves no ExportService and answers Unimplemented: a dry
// run reports it as unreachable, heartbeat and release answer an error frame for it, while
// list and create fail.
func TestExport_DataNodeWithoutExportService(t *testing.T) {
	a := &fakeDataNode{groups: []string{"g"}}
	svc := newFanOutWith(t, map[string]*fakeDataNode{testNodeA: a}, fanOutExtras{old: []string{"data-old"}})
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatalf("an old data node must not fail a dry run: %v", err)
	}
	last := lastSummary(t, stream.frames)
	if got := last.GetUnreachableNodes(); len(got) != 1 || got[0] != "data-old" {
		t.Fatalf("unreachable = %v, want [data-old]", got)
	}

	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "aborted, snapshots released: data node data-old does not support export (older version)") {
		t.Fatalf("create must fail with Unimplemented naming the old node and not treat it as leaked, got %v", err)
	}

	err = svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "data node data-old does not support export (older version)") {
		t.Fatalf("list must fail naming the old node, got %v", err)
	}
	for _, req := range []*transferv1.SessionsRequest{heartbeatReq(testSessionFeedID), releaseReq(testSessionFeedID)} {
		got := errorsByNode(t, sessions(t, svc, req))
		if !strings.Contains(got["data-old"], "does not support export") {
			t.Fatalf("%s must answer an error frame for the old node, got %v", req.GetAction(), got)
		}
	}
}

// An error frame in a node's ACTION_LIST answer must fail the list, not be hidden by `none`.
func TestExportSessions_ListUpstreamErrorFrameFails(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {},
		testNodeB: {actionErr: "read lease: io error"},
	})
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node data-b: read lease: io error") {
		t.Fatalf("want Internal naming data-b, got %v", err)
	}
}

// A node that never answers a Sessions call is cut off by the per-node timeouts: list fails
// as Unavailable, heartbeat and release answer an error frame for it.
func TestExportSessions_StalledNode(t *testing.T) {
	saved := exportSessionTimeout
	exportSessionTimeout = 200 * time.Millisecond
	t.Cleanup(func() { exportSessionTimeout = saved })
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {occupant: testSessionOldID},
		testNodeB: {stallSessions: true},
	})
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	want := "data nodes data-b did not answer within the 200ms sessions timeout and may be creating or releasing a session"
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), want) {
		t.Fatalf("want Unavailable naming data-b as slow, got %v", err)
	}
	for _, req := range []*transferv1.SessionsRequest{heartbeatReq(testSessionOldID), releaseReq(testSessionOldID)} {
		got := errorsByNode(t, sessions(t, svc, req))
		if len(got) != 2 || got[testNodeA] != "" || got[testNodeB] == "" {
			t.Fatalf("%s must answer an error frame only for the stalled node, got %v", req.GetAction(), got)
		}
	}
}

// A data node only streams units and summary frames; anything else is a broken node.
func TestExportPlan_UnexpectedUpstreamFrameIsInternal(t *testing.T) {
	created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: "abcd"}}}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {groups: []string{"g"}, extraFrame: created}})
	err := svc.Plan(&transferv1.PlanRequest{}, &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node data-a: unexpected plan frame") {
		t.Fatalf("want Internal naming the node, got %v", err)
	}
}

// Any heartbeat failure other than an unreachable node fails the call with the node's code.
func TestExportSessions_HeartbeatInternalFails(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {occupant: testSessionOldID},
		testNodeB: {sessionsErr: status.Error(codes.Internal, "read lease: io error")},
	})
	err := svc.Sessions(heartbeatReq(testSessionOldID), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node data-b: read lease: io error") {
		t.Fatalf("want Internal naming data-b, got %v", err)
	}
}

// A node the pool cannot hand a client for: list fails naming it, heartbeat and release answer
// an error frame for it.
func TestExportSessions_NodeWithoutClient(t *testing.T) {
	svc := newFanOutWith(t, map[string]*fakeDataNode{testNodeA: {occupant: testSessionOldID}}, fanOutExtras{noClient: []string{"data-gone"}})
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "did not reach data nodes data-gone") {
		t.Fatalf("want Unavailable naming data-gone, got %v", err)
	}
	for _, req := range []*transferv1.SessionsRequest{heartbeatReq(testSessionOldID), releaseReq(testSessionOldID)} {
		got := errorsByNode(t, sessions(t, svc, req))
		if len(got) != 2 || got[testNodeA] != "" || !strings.Contains(got["data-gone"], "is not in the pool") {
			t.Fatalf("%s must answer an error frame for data-gone, got %v", req.GetAction(), got)
		}
	}
}

func TestExportSessions_ListCanceled(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {}, testNodeB: {}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: ctx}); status.Code(err) != codes.Canceled {
		t.Fatalf("a canceled list must answer Canceled, got %v", err)
	}
}

func TestExportSessions_NoDataNodesIsFailedPrecondition(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc := newExportService(queue.NewMockClient(ctrl), fakeRouteTable{}, nil, logger.GetLogger("export-test"))
	if err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
}

// The local node fails a list on an error frame like a remote one.
func TestExportLocal_ListErrorFrameFails(t *testing.T) {
	svc := newLocalExportService(&fakeLocalPlanner{listFrameErr: "read lease: io error"}, roleLabelSelf)
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node "+roleLabelSelf+": read lease: io error") {
		t.Fatalf("want Internal naming the liaison, got %v", err)
	}
}

// emptySessionsNode answers every Sessions call with an end of stream and no frame.
type emptySessionsNode struct {
	transferv1.UnimplementedExportServiceServer
}

func (emptySessionsNode) Sessions(*transferv1.SessionsRequest, transferv1.ExportService_SessionsServer) error {
	return nil
}

// twoFramesSessionsNode answers every Sessions call with two frames.
type twoFramesSessionsNode struct {
	transferv1.UnimplementedExportServiceServer
}

func (twoFramesSessionsNode) Sessions(_ *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	if err := stream.Send(noneOutcome()); err != nil {
		return err
	}
	return stream.Send(sessionOutcome(testSessionOldID))
}

// A data node answers exactly one frame; a second one is a protocol violation.
func TestExportSessions_SecondFrameIsInternal(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := queue.NewMockClient(ctrl)
	client.EXPECT().NewExportClient(testNodeA).Return(transferv1.NewExportServiceClient(serveFake(t, twoFramesSessionsNode{})), nil).AnyTimes()
	svc := newExportService(client, fakeRouteTable{nodes: []string{testNodeA}}, nil, logger.GetLogger("export-test"))
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node data-a: more than one sessions frame in the stream") {
		t.Fatalf("want Internal naming data-a, got %v", err)
	}
}

func TestExportSessions_NoOutcomeIsInternal(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := queue.NewMockClient(ctrl)
	client.EXPECT().NewExportClient(testNodeA).Return(transferv1.NewExportServiceClient(serveFake(t, emptySessionsNode{})), nil).AnyTimes()
	svc := newExportService(client, fakeRouteTable{nodes: []string{testNodeA}}, nil, logger.GetLogger("export-test"))
	err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node data-a: no sessions outcome in the stream") {
		t.Fatalf("want Internal naming data-a, got %v", err)
	}
	if got := errorsByNode(t, sessions(t, svc, releaseReq(testSessionOldID))); !strings.Contains(got[testNodeA], "no sessions outcome") {
		t.Fatalf("release must answer an error frame for a node without an outcome, got %v", got)
	}
}

func TestExportLocal_Heartbeat(t *testing.T) {
	svc := newLocalExportService(&fakeLocalPlanner{}, roleLabelSelf)
	frames := sessions(t, svc, heartbeatReq("abcd"))
	if len(frames) != 1 || frames[0].NodeId != roleLabelSelf || frames[0].GetDone() == nil {
		t.Fatalf("local heartbeat = %+v", frames)
	}
}

func TestMethodPolicies_CoverExportService(t *testing.T) {
	var got []string
	for _, p := range GlobalMethodPolicies() {
		if strings.HasPrefix(p.FullMethod, "/banyandb.transfer.v1.ExportService/") {
			got = append(got, p.FullMethod)
		}
	}
	sort.Strings(got)
	want := []string{
		"/banyandb.transfer.v1.ExportService/Plan",
		"/banyandb.transfer.v1.ExportService/Sessions",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("export policies = %v, want %v", got, want)
	}
}

// When every node finished, the preempted list is complete and the abort carries no hint.
func TestExportPlan_CreateSessionAbortAfterEveryNodeFinishedHasNoHint(t *testing.T) {
	a, b := &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}, &fakeDataNode{groups: []string{"g"}}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	err := svc.Plan(transfer.CreateRequest(nil, true), &summaryFailingStream{recordingStream: recordingStream{ctx: context.Background()}})
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "preempted sessions ["+testSessionOldID+"] were already removed") {
		t.Fatalf("want Unavailable naming the preempted session, got %v", err)
	}
	if strings.Contains(err.Error(), "did not finish") {
		t.Fatalf("every node finished, so the preempted list is complete: %v", err)
	}
}

// A create that reached no node established nothing and must not claim snapshots were released.
func TestExportPlan_CreateSessionReachingNoNodeHasNothingToRelease(t *testing.T) {
	svc := newFanOutWith(t, nil, fanOutExtras{noClient: []string{"data-gone"}})
	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	want := "create export session " + testSessionFeedID + " aborted, nothing to release: data node data-gone: "
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), want) {
		t.Fatalf("want Unavailable saying nothing to release, got %v", err)
	}
}

// planFramesNode answers every Plan with the given frames.
type planFramesNode struct {
	transferv1.UnimplementedExportServiceServer
	frames []*transferv1.PlanResponse
}

func (n planFramesNode) Plan(_ *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	for _, f := range n.frames {
		if err := stream.Send(f); err != nil {
			return err
		}
	}
	return nil
}

// A data node's summary is its last frame: any frame after it, a second summary included,
// fails the node as Internal.
func TestExportPlan_FrameAfterSummaryIsInternal(t *testing.T) {
	for name, after := range map[string]*transferv1.PlanResponse{
		"units":   unitsFrame("g"),
		"summary": summaryFrame(&transferv1.PlanSummary{}),
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := queue.NewMockClient(ctrl)
			node := planFramesNode{frames: []*transferv1.PlanResponse{summaryFrame(&transferv1.PlanSummary{}), after}}
			client.EXPECT().NewExportClient(testNodeA).Return(transferv1.NewExportServiceClient(serveFake(t, node)), nil).AnyTimes()
			svc := newExportService(client, fakeRouteTable{nodes: []string{testNodeA}}, nil, logger.GetLogger("export-test"))
			err := svc.Plan(&transferv1.PlanRequest{}, &recordingStream{ctx: context.Background()})
			if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node data-a: frame after summary") {
				t.Fatalf("want Internal naming data-a, got %v", err)
			}
		})
	}
}

// A standalone pipeline without a registered ExportService is unsupported, like a data node
// predating it, not unreachable.
func TestExportLocal_WithoutExportServerIsUnsupported(t *testing.T) {
	svc := newExportService(queue.Local(), nil, func() string { return roleLabelSelf }, logger.GetLogger("export-test"))
	svc.newSessionID = func() string { return testSessionFeedID }
	stream := &recordingStream{ctx: context.Background()}
	if err := svc.Plan(&transferv1.PlanRequest{}, stream); err != nil {
		t.Fatalf("an unsupported node must not fail a dry run: %v", err)
	}
	if got := lastSummary(t, stream.frames).GetUnreachableNodes(); len(got) != 1 || got[0] != roleLabelSelf {
		t.Fatalf("unreachable = %v, want [%s]", got, roleLabelSelf)
	}

	err := svc.Plan(transfer.CreateRequest(nil, false), &recordingStream{ctx: context.Background()})
	want := "aborted, nothing to release: data node " + roleLabelSelf + " " + unsupportedExport
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), want) {
		t.Fatalf("create must fail as Unimplemented, got %v", err)
	}

	err = svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()})
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), unsupportedExport) {
		t.Fatalf("list must fail as Unimplemented, got %v", err)
	}
	got := errorsByNode(t, sessions(t, svc, heartbeatReq(testSessionFeedID)))
	if got[roleLabelSelf] != "data node "+unsupportedExport {
		t.Fatalf("heartbeat must answer the unsupported error frame, got %v", got)
	}
}
