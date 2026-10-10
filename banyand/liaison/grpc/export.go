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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc/route"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// The export timeouts are variables so tests can shorten them.
var (
	// exportSessionTimeout bounds one data node's answer to any Sessions action (list,
	// heartbeat, release), including the release that undoes a half-created session. Every
	// action waits on the node's session lock, which a create holds while it snapshots and a
	// release holds while it removes a session, so a slow snapshot or a half-open connection
	// must not pin the call.
	exportSessionTimeout = 2 * time.Minute
	// exportPlanNodeMaxWait is the longest the liaison waits for a data node's next Plan frame
	// before giving the node up. It restarts with every frame and is paused while a frame waits
	// on the downstream client, so a healthy node streaming a large inventory is never cut off.
	// A dry run or ReadSession reports a node that keeps silent as unreachable; a CreateSession
	// fails with DeadlineExceeded. The wait for the first frame also covers the data node's
	// snapshot in a CreateSession, so a slower snapshot fails the create.
	exportPlanNodeMaxWait = 5 * time.Minute
)

// unsupportedExport describes a data node without ExportService (an older version during a
// rolling upgrade), which answers every export RPC with codes.Unimplemented.
const unsupportedExport = "does not support export (older version)"

// errPlanNodeMaxWait is the cancellation cause of an upstream Plan the liaison gave up on
// after exportPlanNodeMaxWait without a frame.
var errPlanNodeMaxWait = errors.New("export plan max wait exceeded")

// exportService is the liaison-side ExportService: a server-streaming proxy. Plan fans
// out to every registered data node, relays `units` frames 1:1 stamping node_id from the
// upstream connection, and closes with one `summary` frame; Sessions fans one lifecycle
// action out and answers one frame per node.
type exportService struct {
	transferv1.UnimplementedExportServiceServer
	tire2        queue.Client
	routeTable   route.TableProvider
	selfNode     func() string
	newSessionID func() string
	l            *logger.Logger
}

// newExportService fans out over the data nodes registered in routeTable. A standalone
// process has no tire2 route table: it is its own only data node, named by selfNode, and its
// local pipeline serves the ExportService in-process.
func newExportService(tire2 queue.Client, routeTable route.TableProvider, selfNode func() string, l *logger.Logger) *exportService {
	return &exportService{tire2: tire2, routeTable: routeTable, selfNode: selfNode, newSessionID: randomSessionID, l: l}
}

// randomSessionID returns 16 random bytes as lowercase hex, which satisfies the session id
// rule enforced by the proto and by every data node.
func randomSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		logger.Panicf("cannot generate export session id: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func (s *exportService) registeredDataNodes() []string {
	rt := s.routeTable.GetRouteTable()
	names := make([]string, 0, len(rt.GetRegistered()))
	for _, n := range rt.GetRegistered() {
		names = append(names, n.GetMetadata().GetName())
	}
	sort.Strings(names)
	return names
}

// planNode is one target of the export fan-out, answering with the shapes a data node's
// ExportService handlers produce.
type planNode interface {
	name() string
	// plan runs one Plan and hands every upstream frame to onFrame, returning its error as is.
	plan(ctx context.Context, req *transferv1.PlanRequest, onFrame func(*transferv1.PlanResponse) error) error
	// sessions runs one Sessions action and returns the node's answer (node_id unset).
	sessions(ctx context.Context, req *transferv1.SessionsRequest) (*transferv1.SessionsResponse, error)
}

// notOpenedError wraps a Plan failure that happened before the upstream stream existed and
// that the node cannot have seen, so a rollback must not target it.
type notOpenedError struct{ error }

func (e notOpenedError) Unwrap() error { return e.error }

// GRPCStatus keeps the wrapped status as is, so its message is not prefixed with the
// wrapper's own text.
func (e notOpenedError) GRPCStatus() *status.Status { return status.Convert(e.error) }

// remoteNode reaches a data node through the tire2 client: the connection pool in a
// cluster, the in-process local pipeline in a standalone process.
type remoteNode struct {
	tire2 queue.Client
	id    string
}

func (n remoteNode) name() string { return n.id }

// exportClient hands out the node's ExportService client. A pipeline without one (a local
// queue with no registered ExportService) is Unimplemented like a node predating it; any other
// failure means the node cannot be reached.
func (n remoteNode) exportClient() (transferv1.ExportServiceClient, error) {
	client, err := n.tire2.NewExportClient(n.id)
	if errors.Is(err, queue.ErrNotImplemented) {
		return nil, status.Error(codes.Unimplemented, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return client, nil
}

func (n remoteNode) plan(ctx context.Context, req *transferv1.PlanRequest, onFrame func(*transferv1.PlanResponse) error) error {
	client, err := n.exportClient()
	if err != nil {
		return notOpenedError{err}
	}
	up, err := client.Plan(ctx, req)
	if err != nil {
		// An open cut short by ctx may already have delivered the request; any other open
		// failure (the connection refused it) never reached the node. A node still connecting
		// when ctx ends is counted too, so its failed rollback may name it as leaked.
		if code := status.Code(err); ctx.Err() != nil && (code == codes.Canceled || code == codes.DeadlineExceeded) {
			return err
		}
		return notOpenedError{err}
	}
	for {
		frame, recvErr := up.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		if err = onFrame(frame); err != nil {
			return err
		}
	}
}

// sessions returns the one frame a data node answers a Sessions action with. A stream
// without an outcome, or with more than one frame, is Internal.
func (n remoteNode) sessions(ctx context.Context, req *transferv1.SessionsRequest) (*transferv1.SessionsResponse, error) {
	client, err := n.exportClient()
	if err != nil {
		return nil, err
	}
	up, err := client.Sessions(ctx, req)
	if err != nil {
		return nil, err
	}
	frame, err := up.Recv()
	if errors.Is(err, io.EOF) || (err == nil && frame.GetOutcome() == nil) {
		return nil, status.Error(codes.Internal, "no sessions outcome in the stream")
	}
	if err != nil {
		return nil, err
	}
	if _, err = up.Recv(); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Internal, "more than one sessions frame in the stream")
	}
	return frame, nil
}

// errorOutcome is a Sessions frame whose node did not complete the action.
func errorOutcome(msg string) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Error{Error: msg}}
}

// failedOutcome reports whether a Sessions frame carries the `error` outcome.
func failedOutcome(f *transferv1.SessionsResponse) bool {
	_, failed := f.GetOutcome().(*transferv1.SessionsResponse_Error)
	return failed
}

// planNodes is the fan-out target set: every registered data node or, in a standalone
// process, the process itself. Empty when a cluster has no data node registered.
func (s *exportService) planNodes() []planNode {
	if s.routeTable == nil {
		return []planNode{remoteNode{tire2: s.tire2, id: s.selfNode()}}
	}
	names := s.registeredDataNodes()
	nodes := make([]planNode, 0, len(names))
	for _, name := range names {
		nodes = append(nodes, remoteNode{tire2: s.tire2, id: name})
	}
	return nodes
}

// toStatus keeps a status error as it is and maps a raw context error to Canceled or
// DeadlineExceeded, so a downstream disconnect never surfaces as codes.Unknown.
func toStatus(err error) *status.Status {
	if st, ok := status.FromError(err); ok {
		return st
	}
	return status.FromContextError(err)
}

// nodeErrKind is how the export fan-outs classify a data node's failure; each action applies
// its own policy to the kinds.
type nodeErrKind int

const (
	nodeErrOther       nodeErrKind = iota
	nodeErrTransport               // the node was not reached or did not answer (failover codes)
	nodeErrUnsupported             // the node predates ExportService
	nodeErrNotFound                // the node does not hold the session
)

// classifyNodeErr returns the kind of a data node's failure and its status.
func classifyNodeErr(err error) (nodeErrKind, *status.Status) {
	st := toStatus(err)
	switch {
	case grpchelper.IsFailoverError(err):
		return nodeErrTransport, st
	case st.Code() == codes.Unimplemented:
		return nodeErrUnsupported, st
	case st.Code() == codes.NotFound:
		return nodeErrNotFound, st
	}
	return nodeErrOther, st
}

// unsupportedNodeError fails a call on a data node without ExportService.
func unsupportedNodeError(node string) error {
	return status.Errorf(codes.Unimplemented, "data node %s %s", node, unsupportedExport)
}

// nodeError reports a data node's failure under the given code, prefixed with the node.
func nodeError(code codes.Code, node, msg string) error {
	return status.Errorf(code, "data node %s: %s", node, msg)
}

// createAborted is the error of a CreateSession Plan that did not complete: the cause,
// whether the half-created session was released (or no node got the request, so there was
// nothing to release), which nodes still hold it when releasing failed, and the sessions the
// nodes that finished reported as preempted, which are gone either way. The preempted list
// covers only those nodes: with preempt set, a node that did not finish may have removed
// sessions too, so unfinishedPreempt adds a hint to check with ACTION_LIST.
func createAborted(id string, cause error, nothingEstablished bool, leaked, preempted []string, unfinishedPreempt bool) error {
	st := status.Convert(cause)
	outcome := "snapshots released"
	if nothingEstablished {
		outcome = "nothing to release"
	}
	msg := fmt.Sprintf("create export session %s aborted, %s: %s", id, outcome, st.Message())
	if len(leaked) > 0 {
		msg = fmt.Sprintf("create export session %s aborted: %s; releasing it failed on %v, release session %s by hand",
			id, st.Message(), leaked, id)
	}
	if len(preempted) > 0 {
		msg += fmt.Sprintf("; preempted sessions %v were already removed", preempted)
	}
	if unfinishedPreempt {
		msg += "; nodes that did not finish may also have removed preempted sessions, check with ACTION_LIST"
	}
	return status.Error(st.Code(), msg)
}

// Plan implements transferv1.ExportServiceServer.
func (s *exportService) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	if err := req.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateSession(req); err != nil {
		return err
	}
	nodes := s.planNodes()
	if len(nodes) == 0 {
		return status.Error(codes.FailedPrecondition, "this liaison has not discovered any data node")
	}
	if req.GetCreate() != nil {
		return s.planCreateSession(req, stream, nodes)
	}
	summary, _, _, err := s.fanOutPlan(stream.Context(), req, nodes, stream.Send)
	if err != nil {
		return err
	}
	return stream.Send(summary)
}

// validateSession applies the rule ValidateAll cannot express: a CreateSession must come
// without an id, because the liaison generates it. No session is a dry run and ReadSession's
// id is already validated by ValidateAll.
func validateSession(req *transferv1.PlanRequest) error {
	if req.GetCreate().GetId() != "" {
		return status.Error(codes.InvalidArgument, "create session must not carry an id; the liaison generates it")
	}
	return nil
}

// sessionID is the id of the session req drives, empty for a dry run.
func sessionID(req *transferv1.PlanRequest) string {
	if c := req.GetCreate(); c != nil {
		return c.GetId()
	}
	return req.GetRead().GetId()
}

// withSessionID clones req and stamps the generated id into its CreateSession.
func withSessionID(req *transferv1.PlanRequest, id string) *transferv1.PlanRequest {
	upstream := proto.Clone(req).(*transferv1.PlanRequest)
	upstream.GetCreate().Id = id
	return upstream
}

// planCreateSession generates the cluster-wide session id, announces it in the first
// frame, then fans out strictly: any node that fails or does not answer aborts the whole
// attempt and the session is released on every node that opened a Plan stream before the
// error is returned.
func (s *exportService) planCreateSession(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer, nodes []planNode) error {
	id := s.newSessionID()
	created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: id}}}
	if err := stream.Send(created); err != nil {
		return err
	}
	summary, established, unfinished, err := s.fanOutPlan(stream.Context(), withSessionID(req, id), nodes, stream.Send)
	if err == nil {
		err = stream.Send(summary)
	}
	if err != nil {
		// Without the summary the client cannot use the session, so it is undone like any
		// other failure.
		leaked := s.releaseOn(context.WithoutCancel(stream.Context()), id, established)
		return createAborted(id, toStatus(err).Err(), len(established) == 0, leaked,
			summary.GetSummary().GetPreemptedSessionIds(), req.GetCreate().GetPreempt() && unfinished)
	}
	return nil
}

// fanOutPlan runs one upstream Plan per node and relays `units` frames through send
// (serialized). Node `summary` frames are folded into the returned final summary,
// and the nodes that may have received the request are returned so a rollback targets only
// them and not the nodes that were unreachable before any Plan reached them. unfinished
// reports whether some node closed without its summary. The error, if any, is a status error;
// the summary then carries only the preempted session ids of the nodes that finished, so an
// aborted create can still name them. A node's summary must be its last frame: anything after
// it fails the node as Internal.
// CreateSession is strict: every failure is fatal, transport failures as Unavailable and a
// node silent for exportPlanNodeMaxWait as DeadlineExceeded. Otherwise transport failures, the
// max wait, NotFound (the node does not hold the session) and Unimplemented (the node
// predates ExportService) degrade the node to unreachable_nodes and any other status is
// returned as-is.
func (s *exportService) fanOutPlan(
	ctx context.Context, req *transferv1.PlanRequest, nodes []planNode, send func(*transferv1.PlanResponse) error,
) (*transferv1.PlanResponse, []planNode, bool, error) {
	strict := req.GetCreate() != nil
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sendMu sync.Mutex
	sendLocked := func(frame *transferv1.PlanResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return send(frame)
	}
	var mu sync.Mutex
	var established []planNode
	var finished int
	var unreachable, answered, preempted []string
	markUnreachable := func(node string) {
		mu.Lock()
		unreachable = append(unreachable, node)
		mu.Unlock()
	}
	nodeFailed := func(node string, err error) error {
		kind, st := classifyNodeErr(err)
		if strict {
			switch kind {
			case nodeErrTransport:
				return nodeError(codes.Unavailable, node, st.Message())
			case nodeErrUnsupported:
				return unsupportedNodeError(node)
			default:
				return nodeError(st.Code(), node, st.Message())
			}
		}
		switch kind {
		case nodeErrTransport:
			s.l.Warn().Err(err).Str("node", node).Msg("data node unreachable during export plan; continuing with the remaining nodes")
		case nodeErrNotFound:
			s.l.Warn().Err(err).Str("node", node).Str("session", sessionID(req)).
				Msg("data node does not hold the export session; reporting it as unreachable")
		case nodeErrUnsupported:
			s.l.Warn().Err(err).Str("node", node).Msg("data node " + unsupportedExport + "; reporting it as unreachable")
		default:
			return nodeError(st.Code(), node, st.Message())
		}
		markUnreachable(node)
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, n := range nodes {
		g.Go(func() error {
			upCtx, cancelUp := context.WithCancelCause(gctx)
			defer cancelUp(nil)
			wait := time.AfterFunc(exportPlanNodeMaxWait, func() { cancelUp(errPlanNodeMaxWait) })
			defer wait.Stop()
			var sendErr error
			var summarized bool
			err := n.plan(upCtx, req, func(frame *transferv1.PlanResponse) error {
				// The window restarts after every frame and is paused while the frame waits
				// on the downstream client, so a slow client is never blamed on the node.
				wait.Stop()
				defer wait.Reset(exportPlanNodeMaxWait)
				if summarized {
					return status.Error(codes.Internal, "frame after summary")
				}
				switch f := frame.GetFrame().(type) {
				case *transferv1.PlanResponse_Summary:
					summarized = true
					mu.Lock()
					finished++
					preempted = append(preempted, f.Summary.GetPreemptedSessionIds()...)
					mu.Unlock()
					return nil
				case *transferv1.PlanResponse_Units:
					f.Units.NodeId = n.name()
					sendErr = sendLocked(frame)
					return sendErr
				default:
					return status.Errorf(codes.Internal, "unexpected plan frame %T", frame.GetFrame())
				}
			})
			wait.Stop()
			timedOut := errors.Is(context.Cause(upCtx), errPlanNodeMaxWait)
			var notOpened notOpenedError
			if !errors.As(err, &notOpened) && status.Code(err) != codes.Unimplemented {
				// The node may have received the request, so a rollback must cover it. A node
				// without ExportService cannot hold a session.
				mu.Lock()
				established = append(established, n)
				mu.Unlock()
			}
			switch {
			case err == nil:
				mu.Lock()
				answered = append(answered, n.name())
				mu.Unlock()
				return nil
			case sendErr != nil:
				return sendErr
			case gctx.Err() != nil:
				return gctx.Err() // another node or the client already failed the call
			case timedOut && strict:
				return nodeError(codes.DeadlineExceeded, n.name(), "no frame for "+exportPlanNodeMaxWait.String())
			case timedOut:
				s.l.Warn().Str("node", n.name()).Msg("no frame for " + exportPlanNodeMaxWait.String() +
					" during export plan; reporting the data node as unreachable")
				markUnreachable(n.name())
				return nil
			}
			return nodeFailed(n.name(), err)
		})
	}
	err := g.Wait()
	summary := &transferv1.PlanSummary{PreemptedSessionIds: slices.Compact(slices.Sorted(slices.Values(preempted)))}
	frame := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Summary{Summary: summary}}
	unfinished := finished < len(nodes)
	if err != nil {
		return frame, established, unfinished, toStatus(err).Err()
	}
	sort.Strings(unreachable)
	sort.Strings(answered)
	summary.UnreachableNodes, summary.AnsweredNodes = unreachable, answered
	return frame, established, unfinished, nil
}

// releaseOn drops the session on the given nodes and returns the ones that failed, sorted.
func (s *exportService) releaseOn(ctx context.Context, id string, nodes []planNode) []string {
	req := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id}
	frames, _ := s.fanOutSessions(ctx, req, nodes) // ACTION_RELEASE reports every failure in a frame
	var failed []string
	for _, f := range frames {
		if failedOutcome(f) {
			failed = append(failed, f.GetNodeId())
		}
	}
	return failed
}

// Sessions implements transferv1.ExportServiceServer: fan the action out and answer one
// frame per data node, stamped with node_id, in node order.
func (s *exportService) Sessions(req *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	if err := req.ValidateAll(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetAction() != transferv1.SessionsRequest_ACTION_LIST && req.GetSessionId() == "" {
		return status.Errorf(codes.InvalidArgument, "action %s requires the session id", req.GetAction())
	}
	nodes := s.planNodes()
	if len(nodes) == 0 {
		return status.Error(codes.FailedPrecondition, "this liaison has not discovered any data node")
	}
	frames, err := s.fanOutSessions(stream.Context(), req, nodes)
	if err != nil {
		return err
	}
	for _, f := range frames {
		if err = stream.Send(f); err != nil {
			return err
		}
	}
	return nil
}

// fanOutSessions runs one Sessions action on every node and returns their frames sorted by
// node. How a node's failure is handled depends on the action. ACTION_LIST is strict: a node
// that does not answer fails the call as Unavailable, because a client cannot tell a clean
// node from a silent one, and a node that answers with an error (including Unimplemented from
// a node without ExportService) fails it with that node's code, or Internal for an `error`
// frame. ACTION_HEARTBEAT reports an
// unreachable node or one without ExportService as an `error` frame (the next heartbeat
// retries) and fails the call with any other error, so an expired session stops the
// client's heartbeat. ACTION_RELEASE never fails the call for a node: every failure is
// reported as an `error` frame so a partially released session stays visible.
func (s *exportService) fanOutSessions(ctx context.Context, req *transferv1.SessionsRequest, nodes []planNode) ([]*transferv1.SessionsResponse, error) {
	release := req.GetAction() == transferv1.SessionsRequest_ACTION_RELEASE
	var mu sync.Mutex
	list := req.GetAction() == transferv1.SessionsRequest_ACTION_LIST
	var frames []*transferv1.SessionsResponse
	var unreached, slow []string
	g, gctx := errgroup.WithContext(ctx)
	for _, n := range nodes {
		g.Go(func() error {
			nodeCtx, cancel := context.WithTimeout(gctx, exportSessionTimeout)
			defer cancel()
			frame, err := n.sessions(nodeCtx, req)
			if err != nil {
				if !release && gctx.Err() != nil {
					return nil // another node or the client already failed the call
				}
				kind, st := classifyNodeErr(err)
				switch {
				case kind == nodeErrUnsupported && list:
					return unsupportedNodeError(n.name())
				case kind == nodeErrUnsupported:
					frame = errorOutcome("data node " + unsupportedExport)
				case release || (kind == nodeErrTransport && req.GetAction() == transferv1.SessionsRequest_ACTION_HEARTBEAT):
					frame = errorOutcome(st.Message())
				case kind == nodeErrTransport:
					s.l.Warn().Err(err).Str("node", n.name()).Msg("data node did not answer the export session list")
					mu.Lock()
					if errors.Is(nodeCtx.Err(), context.DeadlineExceeded) {
						slow = append(slow, n.name())
					} else {
						unreached = append(unreached, n.name())
					}
					mu.Unlock()
					return nil
				default:
					return nodeError(st.Code(), n.name(), st.Message())
				}
			}
			if list && failedOutcome(frame) {
				return nodeError(codes.Internal, n.name(), frame.GetError())
			}
			frame.NodeId = n.name()
			if failedOutcome(frame) {
				s.l.Warn().Str("node", n.name()).Str("session", req.GetSessionId()).Stringer("action", req.GetAction()).
					Str("error", frame.GetError()).Msg("export session action failed on a data node")
			}
			mu.Lock()
			frames = append(frames, frame)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil && !release {
		return nil, toStatus(err).Err()
	}
	var gaps []string
	if len(unreached) > 0 {
		sort.Strings(unreached)
		gaps = append(gaps, "export session list did not reach data nodes "+strings.Join(unreached, ", "))
	}
	if len(slow) > 0 {
		sort.Strings(slow)
		gaps = append(gaps, fmt.Sprintf("data nodes %s did not answer within the %s sessions timeout and may be creating or releasing a session",
			strings.Join(slow, ", "), exportSessionTimeout))
	}
	if len(gaps) > 0 {
		return nil, status.Error(codes.Unavailable, strings.Join(gaps, "; "))
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].GetNodeId() < frames[j].GetNodeId() })
	return frames, nil
}
