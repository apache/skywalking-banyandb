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
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer"
)

const (
	testSessionFeedID = "feedfeedfeedfeedfeedfeedfeedfeed"
	testSessionOldID  = "01d101d101d101d101d101d101d101d1"
	testSessionAbcdID = "abcdabcdabcdabcdabcdabcdabcdabcd"
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
		testNodeA: {groups: []string{"g"}, occupant: testSessionAbcdID},
		testNodeB: {expired: map[string]bool{testSessionAbcdID: true}},
	})
	err := svc.Plan(transfer.ReadRequest(testSessionAbcdID), &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), testNodeB) {
		t.Fatalf("an expired session must fail the whole call naming the node, got %v", err)
	}
}

// A failed Plan leaves no goroutine behind: the sender of the downstream frames ends with
// the call, also when it was idle on an empty queue.
func TestExportPlan_FailedPlanLeavesNoGoroutine(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{
		testNodeA: {groups: []string{"g"}, occupant: testSessionAbcdID},
		testNodeB: {expired: map[string]bool{testSessionAbcdID: true}},
	})
	plan := func() {
		if err := svc.Plan(transfer.ReadRequest(testSessionAbcdID), &recordingStream{ctx: context.Background()}); err == nil {
			t.Fatal("an expired session must fail the call")
		}
	}
	plan() // warm up the connections
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		plan()
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines grew from %d to %d", before, after)
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

// A liaison that has not discovered any data node yet is unavailable, not a sign that a
// session is gone: a heartbeat must not take it for an expired session.
func TestExportPlan_NoDataNodesIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc := newExportService(queue.NewMockClient(ctrl), fakeRouteTable{}, nil, logger.GetLogger("export-test"))
	err := svc.Plan(&transferv1.PlanRequest{}, &recordingStream{ctx: context.Background()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
}

func TestExportPlan_InvalidCombinations(t *testing.T) {
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: {}})
	for _, req := range []*transferv1.PlanRequest{
		{Session: &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{Id: testSessionAbcdID}}}, // the liaison generates the id
		{Session: &transferv1.PlanRequest_Read{Read: &transferv1.ReadSession{}}},                            // needs the session id
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
		{},                    // the action is required
		heartbeatReq(""),      // heartbeat needs the session id
		releaseReq(""),        // release needs the session id
		releaseReq("NOT-HEX"), // the id rule
		{Action: 99, SessionId: testSessionAbcdID}, // an unknown action
	} {
		if err := svc.Sessions(req, &recordingSessionsStream{ctx: context.Background()}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%+v must be InvalidArgument, got %v", req, err)
		}
	}
	// ACTION_LIST ignores the id.
	if frames := sessions(t, svc, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST, SessionId: testSessionAbcdID}); len(frames) != 1 {
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
	const id = "abcd01efabcd01efabcd01efabcd01ef"
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
	released := sessions(t, svc, releaseReq(testSessionAbcdID))
	if len(released) != 1 || released[0].NodeId != roleLabelSelf || released[0].GetDone() == nil || local.released != testSessionAbcdID {
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
	released := sessions(t, svc1, releaseReq(testSessionAbcdID))
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
	err = svc3.Plan(transfer.ReadRequest(testSessionAbcdID), &recordingStream{ctx: context.Background()})
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
		req := transfer.ReadRequest(testSessionAbcdID)
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
	a, b := &fakeDataNode{groups: []string{"g"}, occupant: testSessionAbcdID}, &fakeDataNode{}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	{
		req := transfer.ReadRequest(testSessionAbcdID)
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
	created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: testSessionAbcdID}}}
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

func TestExportSessions_NoDataNodesIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc := newExportService(queue.NewMockClient(ctrl), fakeRouteTable{}, nil, logger.GetLogger("export-test"))
	if err := svc.Sessions(listReq(), &recordingSessionsStream{ctx: context.Background()}); status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
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
	frames := sessions(t, svc, heartbeatReq(testSessionAbcdID))
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
		"/banyandb.transfer.v1.ExportService/Export",
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

// stalledStream accepts the `created` frame of a CreateSession, then blocks every other Send
// until unblock is closed, as a client that stays connected but stops reading does. entered
// closes when the first Send blocks.
type stalledStream struct {
	grpclib.ServerStream
	ctx       context.Context
	entered   chan struct{}
	unblock   chan struct{}
	enterOnce sync.Once
}

func newStalledStream() *stalledStream {
	return &stalledStream{ctx: context.Background(), entered: make(chan struct{}), unblock: make(chan struct{})}
}

func (r *stalledStream) Context() context.Context { return r.ctx }
func (r *stalledStream) Send(f *transferv1.PlanResponse) error {
	if f.GetCreated() == nil {
		r.enterOnce.Do(func() { close(r.entered) })
		<-r.unblock
	}
	return nil
}

// fakeExportNode is a data node's Export handler: it answers init with ready and every unit
// with unit_started, one chunk carrying the unit's group, and unit_finished.
type fakeExportNode struct {
	transferv1.UnimplementedExportServiceServer
	failWith error // returned right after init, before ready
	exited   chan struct{}
	received []*transferv1.ExportRequest
	mu       sync.Mutex
	sawEOF   bool
}

func newFakeExportNode() *fakeExportNode { return &fakeExportNode{exited: make(chan struct{})} }

func (f *fakeExportNode) Export(stream transferv1.ExportService_ExportServer) error {
	defer close(f.exited)
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			f.mu.Lock()
			f.sawEOF = true
			f.mu.Unlock()
			return nil
		}
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.received = append(f.received, req)
		f.mu.Unlock()
		if req.GetInit() != nil {
			if f.failWith != nil {
				return f.failWith
			}
			if err = stream.Send(readyFrame()); err != nil {
				return err
			}
			continue
		}
		for _, frame := range unitAnswer(req.GetSegment().GetGroup()) {
			if err = stream.Send(frame); err != nil {
				return err
			}
		}
	}
}

func (f *fakeExportNode) snapshot() ([]*transferv1.ExportRequest, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.received), f.sawEOF
}

func readyFrame() *transferv1.ExportResponse {
	return &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Ready{Ready: &transferv1.Ready{}}}
}

func unitAnswer(group string) []*transferv1.ExportResponse {
	return []*transferv1.ExportResponse{
		{Frame: &transferv1.ExportResponse_UnitStarted{UnitStarted: &transferv1.UnitStarted{TotalShards: 2}}},
		{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: []byte(group)}}},
		{Frame: &transferv1.ExportResponse_UnitFinished{UnitFinished: &transferv1.UnitFinished{Warnings: []string{group}}}},
	}
}

func exportInit(target string) *transferv1.ExportRequest {
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Init_{Init: &transferv1.ExportRequest_Init{
		SessionId: testSessionFeedID, TargetNode: target, Format: transferv1.Format_FORMAT_CSV, MaxFileSize: 1 << 20,
		Compress: transferv1.Compress_COMPRESS_GZIP,
	}}}
}

func exportUnit(group string) *transferv1.ExportRequest {
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Segment{Segment: &transferv1.SegmentUnit{Group: group, SegmentSuffix: "20260907"}}}
}

// newExportRelay serves an exportService over the given data nodes and returns a client of
// the liaison itself.
func newExportRelay(t *testing.T, nodes map[string]transferv1.ExportServiceServer, extras fanOutExtras) transferv1.ExportServiceClient {
	t.Helper()
	_, client := newExportRelayService(t, nodes, extras)
	return client
}

// newExportRelayService is newExportRelay that also returns the liaison's service.
func newExportRelayService(t *testing.T, nodes map[string]transferv1.ExportServiceServer,
	extras fanOutExtras,
) (*exportService, transferv1.ExportServiceClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := queue.NewMockClient(ctrl)
	var names []string
	for name, node := range nodes {
		client.EXPECT().NewExportClient(name).Return(transferv1.NewExportServiceClient(serveFake(t, node)), nil).AnyTimes()
		names = append(names, name)
	}
	for _, name := range extras.dead {
		client.EXPECT().NewExportClient(name).Return(transferv1.NewExportServiceClient(deadConn(t)), nil).AnyTimes()
		names = append(names, name)
	}
	for _, name := range extras.noClient {
		client.EXPECT().NewExportClient(name).Return(nil, errors.New("node "+name+" is not in the pool")).AnyTimes()
		names = append(names, name)
	}
	for _, name := range extras.old {
		client.EXPECT().NewExportClient(name).Return(transferv1.NewExportServiceClient(serveFake(t, nil)), nil).AnyTimes()
		names = append(names, name)
	}
	sort.Strings(names)
	svc := newExportService(client, fakeRouteTable{nodes: names}, nil, logger.GetLogger("export-test"))
	return svc, transferv1.NewExportServiceClient(serveFake(t, svc))
}

// runExport sends frames, half-closes and collects every answer until the stream ends.
func runExport(t *testing.T, client transferv1.ExportServiceClient, frames ...*transferv1.ExportRequest) ([]*transferv1.ExportResponse, error) {
	t.Helper()
	stream, err := client.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		if err = stream.Send(f); err != nil {
			break // the stream ended; Recv reports why
		}
	}
	_ = stream.CloseSend()
	var got []*transferv1.ExportResponse
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return got, nil
		}
		if recvErr != nil {
			return got, recvErr
		}
		got = append(got, frame)
	}
}

func waitClosed(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

func assertExportFrames[T proto.Message](t *testing.T, got, want []T) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("frame %d changed: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestExportRelay_ForwardsFramesUnchangedAndHalfClose(t *testing.T) {
	nodeA, nodeB := newFakeExportNode(), newFakeExportNode()
	client := newExportRelay(t, map[string]transferv1.ExportServiceServer{testNodeA: nodeA, testNodeB: nodeB}, fanOutExtras{})
	sent := []*transferv1.ExportRequest{exportInit(testNodeB), exportUnit("g1"), exportUnit("g2")}
	got, err := runExport(t, client, sent...)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]*transferv1.ExportResponse{readyFrame()}, unitAnswer("g1")...)
	want = append(want, unitAnswer("g2")...)
	assertExportFrames(t, got, want)

	waitClosed(t, nodeB.exited, "the target node's handler must end")
	received, sawEOF := nodeB.snapshot()
	assertExportFrames(t, received, sent)
	if !sawEOF {
		t.Fatal("the client's half-close must reach the node as io.EOF")
	}
	if received, _ = nodeA.snapshot(); len(received) != 0 {
		t.Fatalf("only the target node may get the stream, %s got %v", testNodeA, received)
	}
}

func TestExportRelay_InvalidFirstFrame(t *testing.T) {
	node := newFakeExportNode()
	client := newExportRelay(t, map[string]transferv1.ExportServiceServer{testNodeA: node}, fanOutExtras{})
	badInit := exportInit(testNodeA)
	badInit.GetInit().SessionId = "NOT-HEX"
	noFormat := exportInit(testNodeA)
	noFormat.GetInit().Format = transferv1.Format_FORMAT_UNSPECIFIED
	for name, frames := range map[string][]*transferv1.ExportRequest{
		"no frame":       nil,
		"unit first":     {exportUnit("g")},
		"empty frame":    {{}},
		"bad session id": {badInit},
		"no format":      {noFormat},
		"no target":      {exportInit("")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := runExport(t, client, frames...); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("got %v, want InvalidArgument", err)
			}
		})
	}
	if received, _ := node.snapshot(); len(received) != 0 {
		t.Fatalf("an invalid stream must not reach the node, got %v", received)
	}
}

func TestExportRelay_UnknownTargetIsNotFound(t *testing.T) {
	client := newExportRelay(t, map[string]transferv1.ExportServiceServer{testNodeA: newFakeExportNode()}, fanOutExtras{})
	_, err := runExport(t, client, exportInit("data-x"))
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), `data node "data-x" is not registered`) {
		t.Fatalf("got %v, want NotFound naming the node", err)
	}
}

func TestExportRelay_UpstreamFailuresKeepTheirCode(t *testing.T) {
	client := newExportRelay(t, nil, fanOutExtras{dead: []string{"dead"}, noClient: []string{"gone"}, old: []string{"old"}})
	for target, want := range map[string]codes.Code{"dead": codes.Unavailable, "gone": codes.Unavailable, "old": codes.Unimplemented} {
		t.Run(target, func(t *testing.T) {
			if _, err := runExport(t, client, exportInit(target), exportUnit("g")); status.Code(err) != want {
				t.Fatalf("got %v, want %s", err, want)
			}
		})
	}
}

func TestExportRelay_LocalPipelineWithoutExportServerIsUnimplemented(t *testing.T) {
	svc := newExportService(queue.Local(), nil, func() string { return testLocalNodeAddr }, logger.GetLogger("export-test"))
	_, err := runExport(t, transferv1.NewExportServiceClient(serveFake(t, svc)), exportInit(testLocalNodeAddr))
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("got %v, want Unimplemented", err)
	}
}

func TestExportRelay_UpstreamErrorReachesClient(t *testing.T) {
	node := newFakeExportNode()
	node.failWith = status.Error(codes.FailedPrecondition, "export session feed expired")
	client := newExportRelay(t, map[string]transferv1.ExportServiceServer{testNodeA: node}, fanOutExtras{})
	got, err := runExport(t, client, exportInit(testNodeA), exportUnit("g"))
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "export session feed expired" {
		t.Fatalf("got %v, want the node's status unchanged", err)
	}
	if len(got) != 0 {
		t.Fatalf("no frame expected, got %v", got)
	}
}

func TestExportRelay_ClientCancelTearsDownUpstream(t *testing.T) {
	node := newFakeExportNode()
	client := newExportRelay(t, map[string]transferv1.ExportServiceServer{testNodeA: node}, fanOutExtras{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(exportInit(testNodeA)); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitClosed(t, node.exited, "canceling the client must end the node's handler")
	if _, sawEOF := node.snapshot(); sawEOF {
		t.Fatal("a canceled client must not look like a clean half-close to the node")
	}
}

func TestExportRelay_StandaloneUsesLocalPipeline(t *testing.T) {
	node := newFakeExportNode()
	q := queue.Local()
	q.SetExportServer(node)
	svc := newExportService(q, nil, func() string { return testLocalNodeAddr }, logger.GetLogger("export-test"))
	client := transferv1.NewExportServiceClient(serveFake(t, svc))

	if _, err := runExport(t, client, exportInit(testNodeA)); status.Code(err) != codes.NotFound {
		t.Fatalf("a standalone liaison only knows itself, got %v", err)
	}
	sent := []*transferv1.ExportRequest{exportInit(testLocalNodeAddr), exportUnit("g1")}
	got, err := runExport(t, client, sent...)
	if err != nil {
		t.Fatal(err)
	}
	assertExportFrames(t, got, append([]*transferv1.ExportResponse{readyFrame()}, unitAnswer("g1")...))
	waitClosed(t, node.exited, "the local handler must end")
	received, sawEOF := node.snapshot()
	assertExportFrames(t, received, sent)
	if !sawEOF {
		t.Fatal("the client's half-close must reach the local handler as io.EOF")
	}
}

func TestExportRelay_LeavesNoGoroutine(t *testing.T) {
	nodes := map[string]transferv1.ExportServiceServer{testNodeA: &fakeExportNode{}}
	client := newExportRelay(t, nodes, fanOutExtras{})
	run := func(cancelEarly bool) {
		nodes[testNodeA].(*fakeExportNode).exited = make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.Export(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.Send(exportInit(testNodeA)); err != nil {
			t.Fatal(err)
		}
		if _, err = stream.Recv(); err != nil {
			t.Fatal(err)
		}
		if !cancelEarly {
			_ = stream.CloseSend()
			if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("got %v, want io.EOF", err)
			}
		}
		cancel()
		waitClosed(t, nodes[testNodeA].(*fakeExportNode).exited, "the node's handler must end")
	}
	run(false) // warm up the connections
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		run(i%2 == 0)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
}

// chunkExportNode answers init with ready and then one chunk per size. With hold it then waits
// for the stream to end instead of returning.
type chunkExportNode struct {
	transferv1.UnimplementedExportServiceServer
	exited chan struct{}
	sizes  []int
	rows   uint64
	hold   bool
}

func (c *chunkExportNode) Export(stream transferv1.ExportService_ExportServer) error {
	defer close(c.exited)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(readyFrame()); err != nil {
		return err
	}
	for _, size := range c.sizes {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i)
		}
		chunk := &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: content, Rows: c.rows}}}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
	if c.hold {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return nil
}

// gateNode makes node open its Plan stream only once gate is closed.
func gateNode(node string, gate <-chan struct{}) fanOutExtras {
	return fanOutExtras{wrap: func(n string, c transferv1.ExportServiceClient) transferv1.ExportServiceClient {
		if n != node {
			return c
		}
		return gatedClient{ExportServiceClient: c, gate: gate}
	}}
}

// gatedClient opens a Plan stream only once gate is closed.
type gatedClient struct {
	transferv1.ExportServiceClient
	gate <-chan struct{}
}

func (c gatedClient) Plan(ctx context.Context, in *transferv1.PlanRequest,
	opts ...grpclib.CallOption,
) (grpclib.ServerStreamingClient[transferv1.PlanResponse], error) {
	select {
	case <-c.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.ExportServiceClient.Plan(ctx, in, opts...)
}

// planWithin runs Plan and fails the test when it does not return within 5s.
func planWithin(t *testing.T, svc *exportService, req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	t.Helper()
	done := make(chan error, 1)
	//panicdiag:allow-rawgo deadline guard for a Plan blocked on the downstream client
	go func() { done <- svc.Plan(req, stream) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Plan did not return while the downstream client stopped reading")
		return nil
	}
}

// A client that stays connected but stops reading must not hold up a failed create: node a
// created its snapshot and its inventory waits on the client when node b rejects the create,
// yet the liaison still fails the call and releases the session on a.
func TestExportPlan_CreateSessionBlockedDownstreamStillRollsBack(t *testing.T) {
	stream := newStalledStream()
	defer close(stream.unblock)
	a := &fakeDataNode{groups: []string{"g"}}
	b := &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}
	svc := newFanOutWith(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b}, gateNode(testNodeB, stream.entered))
	err := planWithin(t, svc, transfer.CreateRequest(nil, false), stream)
	if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), testSessionOldID) {
		t.Fatalf("want AlreadyExists naming the occupant, got %v", err)
	}
	if got := a.releasedIDs(); len(got) != 1 || got[0] != testSessionFeedID {
		t.Fatalf("the session must be released on the node that created it, got %v", got)
	}
}

// A dry run whose client stopped reading still fails promptly with another node's fatal error.
func TestExportPlan_BlockedDownstreamDoesNotHideNodeError(t *testing.T) {
	stream := newStalledStream()
	defer close(stream.unblock)
	a := &fakeDataNode{groups: []string{"g"}}
	b := &fakeDataNode{groups: []string{"g"}, extraFrame: &transferv1.PlanResponse{
		Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: testSessionFeedID}},
	}}
	svc := newFanOutWith(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b}, gateNode(testNodeB, stream.entered))
	err := planWithin(t, svc, &transferv1.PlanRequest{}, stream)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "data node "+testNodeB) {
		t.Fatalf("want node b's Internal error, got %v", err)
	}
}

// fanOutGoroutines counts the goroutines still running inside fanOutPlan, its plan sender
// included.
func fanOutGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "(*exportService).fanOutPlan") {
			count++
		}
	}
	return count
}

// A create that every node rejects before emitting any unit must not leave the plan sender
// waiting on an empty queue: repeated attempts would pile up goroutines holding the request
// and the stream.
func TestExportPlan_CreateSessionRejectedBeforeUnitsStopsTheSender(t *testing.T) {
	a := &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}
	b := &fakeDataNode{groups: []string{"g"}, occupant: testSessionOldID}
	svc := newFanOut(t, map[string]*fakeDataNode{testNodeA: a, testNodeB: b})
	const attempts = 10
	for i := 0; i < attempts; i++ {
		stream := &recordingStream{ctx: context.Background()}
		err := planWithin(t, svc, transfer.CreateRequest(nil, false), stream)
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("attempt %d: want AlreadyExists, got %v", i, err)
		}
		for _, f := range stream.frames {
			if f.GetUnits() != nil {
				t.Fatalf("attempt %d: a rejected create must not relay units, got %+v", i, f)
			}
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for fanOutGoroutines() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d fan-out goroutines still running after %d rejected creates", fanOutGoroutines(), attempts)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func rateInit(target string, bytesPerSecond uint64) *transferv1.ExportRequest {
	req := exportInit(target)
	req.GetInit().LiaisonBytesPerSecond = bytesPerSecond
	return req
}

func assertRelayRate(t *testing.T, r *relayLimiter, wantLimit rate.Limit) {
	t.Helper()
	if limit := r.limiter.Limit(); limit != wantLimit {
		t.Fatalf("got limit %v, want limit %v", limit, wantLimit)
	}
}

func TestRelayLimiter_LastWinsAndIdleIsUnlimited(t *testing.T) {
	r := newRelayLimiter()
	assertRelayRate(t, r, rate.Inf)

	releaseA := r.open(100)
	assertRelayRate(t, r, 100)
	if r.pieceSize() != 100 {
		t.Fatalf("a rate below one chunk per second relays pieces of one second's worth, got %d", r.pieceSize())
	}
	releaseShared := r.open(100)
	if r.pieceSize() != 50 {
		t.Fatalf("two relays share one second's worth, got %d", r.pieceSize())
	}
	releaseShared()
	releaseB := r.open(4 << 20)
	assertRelayRate(t, r, 4<<20)
	releaseC := r.open(0)
	assertRelayRate(t, r, rate.Inf) // 0 overrides an earlier limit
	releaseC()
	assertRelayRate(t, r, rate.Inf) // closing a stream does not restore an older value
	releaseD := r.open(1 << 40)
	assertRelayRate(t, r, 1<<40)
	releaseB()
	releaseD()
	assertRelayRate(t, r, 1<<40) // A is still active
	releaseA()
	assertRelayRate(t, r, rate.Inf)
	if r.pieceSize() != relayBurst {
		t.Fatalf("an idle relay goes back to whole chunks, got %d", r.pieceSize())
	}
}

func TestRelayLimiter_TakeLargerThanBurstWaits(t *testing.T) {
	r := newRelayLimiter()
	defer r.open(2 << 20)()
	start := time.Now()
	// The bucket starts full at one 1 MiB burst, so the other 2 MiB wait about one second.
	if err := r.wait(context.Background(), 3<<20); err != nil {
		t.Fatalf("a take larger than the burst must wait, not fail: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 750*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("waited %v, want about 1s", elapsed)
	}
}

// A relay opening with a lower rate shrinks the burst under a wait that sized its step for the
// old one: the wait takes smaller steps instead of failing.
func TestRelayLimiter_LowerRateDuringWaitTakesSmallerSteps(t *testing.T) {
	r := newRelayLimiter()
	defer r.open(8 << 20)()
	done := make(chan struct{})
	//panicdiag:allow-rawgo the test toggles the shared rate while waits run
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			// The last relay opened sets the rate: a low one shrinks the burst for a moment,
			// the high one restores it.
			r.open(64 << 10)()
			r.open(8 << 20)()
			time.Sleep(time.Millisecond)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for waits := 0; ; waits++ {
		if err := r.wait(ctx, 256<<10); err != nil {
			<-done
			t.Fatalf("wait %d failed while the rate changed: %v", waits, err)
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func TestRelayLimiter_UnlimitedDoesNotWait(t *testing.T) {
	r := newRelayLimiter()
	start := time.Now()
	if err := r.wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	defer r.open(0)()
	if err := r.wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("an unlimited relay must not wait, took %v", elapsed)
	}
}

func TestRelayLimiter_CanceledWaitReturnsCanceled(t *testing.T) {
	r := newRelayLimiter()
	defer r.open(1)()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if err := r.wait(ctx, 2<<20); status.Code(err) != codes.Canceled {
		t.Fatalf("got %v, want Canceled", err)
	}
}

func TestExportRelay_ThrottlesChunksAndResetsWhenIdle(t *testing.T) {
	node := &chunkExportNode{exited: make(chan struct{}), sizes: []int{3 << 19, 16}}
	svc, client := newExportRelayService(t, map[string]transferv1.ExportServiceServer{testNodeA: node}, fanOutExtras{})
	stream, err := client.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(rateInit(testNodeA, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	assertRelayRate(t, svc.relayRate, 1<<20)
	start := time.Now()
	_ = stream.CloseSend()
	var chunks int
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if frame.GetChunk() != nil {
			chunks++
		}
	}
	// 1.5 MiB at 1 MiB/s from a full 1 MiB bucket: the chunk larger than the burst goes out as
	// a 1 MiB piece at once and a 512 KiB piece about 500ms later, then the small chunk.
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("relayed in %v, want about 500ms", elapsed)
	}
	if chunks != 3 {
		t.Fatalf("got %d chunks, want 3", chunks)
	}
	waitClosed(t, node.exited, "the node's handler must end")
	waitRelayRate(t, svc.relayRate, rate.Inf)
}

// Below one chunk per second the relay splits a chunk into pieces of one second's worth: the
// client gets a frame about every second, with the bytes in order and the chunk's rows on
// every piece, instead of the whole chunk after a long wait.
func TestExportRelay_SplitsChunksBelowOneChunkPerSecond(t *testing.T) {
	const rate, size = 64 << 10, 256 << 10
	node := &chunkExportNode{exited: make(chan struct{}), sizes: []int{size}, rows: 7}
	_, client := newExportRelayService(t, map[string]transferv1.ExportServiceServer{testNodeA: node}, fanOutExtras{})
	stream, err := client.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(rateInit(testNodeA, rate)); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_ = stream.CloseSend()
	var content []byte
	var pieces int
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		chunk := frame.GetChunk()
		if chunk == nil {
			continue
		}
		if len(chunk.GetContent()) > rate || chunk.GetRows() != 7 {
			t.Fatalf("piece of %d bytes with %d rows, want at most %d bytes with 7 rows", len(chunk.GetContent()), chunk.GetRows(), rate)
		}
		content = append(content, chunk.GetContent()...)
		pieces++
	}
	// One second's worth goes out at once, the other three a second apart.
	if elapsed := time.Since(start); elapsed < 2500*time.Millisecond || elapsed > 10*time.Second {
		t.Fatalf("relayed in %v, want about 3s", elapsed)
	}
	if pieces != size/rate || len(content) != size {
		t.Fatalf("got %d pieces and %d bytes, want %d pieces and %d bytes", pieces, len(content), size/rate, size)
	}
	for i, b := range content {
		if b != byte(i) {
			t.Fatalf("byte %d is %d: the pieces must keep the chunk's bytes in order", i, b)
		}
	}
	waitClosed(t, node.exited, "the node's handler must end")
}

func TestExportRelay_CanceledClientWhileThrottledEndsRelay(t *testing.T) {
	node := &chunkExportNode{exited: make(chan struct{}), sizes: []int{2 << 20}, hold: true}
	svc, client := newExportRelayService(t, map[string]transferv1.ExportServiceServer{testNodeA: node}, fanOutExtras{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(rateInit(testNodeA, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	// At 1 byte/s the 2 MiB chunk goes out one byte a second: the first byte at once, the rest
	// would take weeks.
	time.AfterFunc(100*time.Millisecond, cancel)
	var received int
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			if status.Code(recvErr) != codes.Canceled {
				t.Fatalf("got %v, want Canceled", recvErr)
			}
			break
		}
		received += len(frame.GetChunk().GetContent())
	}
	if received > 1 {
		t.Fatalf("received %d bytes before the cancel, want at most the first one", received)
	}
	waitClosed(t, node.exited, "the relay must end and tear the node's stream down")
	// The handler returned: its relay is no longer counted.
	waitRelayRate(t, svc.relayRate, rate.Inf)
}

// waitRelayRate waits for the relay handler to return and the limiter to settle.
func waitRelayRate(t *testing.T, r *relayLimiter, wantLimit rate.Limit) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		limit := r.limiter.Limit()
		if limit == wantLimit {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("got limit %v, want limit %v", limit, wantLimit)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
