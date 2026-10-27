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
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

const (
	// HeartbeatInterval is fixed. A heartbeat keeps the lease fresh (the data
	// node sweeps a session only once it expired) and records when the holder was last seen,
	// which is what an operator refused by that session judges from before preempting it;
	// the data node itself takes no liveness decision.
	HeartbeatInterval = 5 * time.Minute
	// createRetries bounds the jittered retry of a create that hit a transient failure.
	createRetries = 3
	// sessionsCallTimeout bounds one heartbeat or list call.
	sessionsCallTimeout = 30 * time.Second
)

// createRetryBase is the base back-off for create retries; every attempt adds jitter. Tests
// shorten it.
var createRetryBase = 2 * time.Second

// SessionOptions configure CreateSession.
type SessionOptions struct {
	// OnCreated is called with the session id as soon as the liaison announces it, before
	// the inventory arrives, so the caller can print it right away.
	OnCreated func(id string)
	// Confirm, when set and Preempt is not, is asked whether to take over the sessions that
	// refused the create, given their rendered list; on yes the create is made once more
	// with Preempt, on no the refusal only says how to take them over or release them.
	Confirm func(occupants string) bool
	// Preempt removes every session the data nodes hold and creates ours.
	Preempt bool
}

// Session is a created export session as seen by the client.
type Session struct {
	ID        string
	Preempted []string
}

// Occupant is one listed session with the nodes that hold it.
type Occupant struct {
	Lease *transferv1.SessionLease
	Nodes []string
}

// Classification is the merged view of the listed sessions. Nobody decides liveness: the
// client renders each occupant with its last heartbeat so the operator can judge whether to
// preempt it.
type Classification struct {
	Occupants []Occupant
	// Split is true when more than one session id exists or a session is held by only
	// some of the answering nodes: a half-won create that must be released in full.
	Split bool
}

// String renders the occupants for error and warning text.
func (c Classification) String() string {
	var b strings.Builder
	for _, o := range c.Occupants {
		fmt.Fprintf(&b, "session %s started %s, last heartbeat %s, catalogs %v, on nodes %v\n",
			o.Lease.GetSessionId(), unixNano(o.Lease.GetStartedAt()), unixNano(o.Lease.GetLastHeartbeatAt()),
			o.Lease.GetCatalogs(), o.Nodes)
	}
	if c.Split {
		b.WriteString("the sessions are split across nodes (a half-won create)\n")
	}
	return b.String()
}

func unixNano(ns int64) string {
	if ns == 0 {
		return "unknown"
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}

// ListSessions collects the ACTION_LIST frames, one per data node.
func ListSessions(ctx context.Context, conn *grpc.ClientConn) ([]*transferv1.SessionsResponse, error) {
	frames, err := sessions(ctx, conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST})
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	return frames, nil
}

// sessions runs one Sessions call and drains its frames, one per data node.
func sessions(ctx context.Context, conn *grpc.ClientConn, req *transferv1.SessionsRequest) ([]*transferv1.SessionsResponse, error) {
	stream, err := transferv1.NewExportServiceClient(conn).Sessions(ctx, req)
	if err != nil {
		return nil, cluster.Unsupported(err)
	}
	var out []*transferv1.SessionsResponse
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return out, nil
		}
		if recvErr != nil {
			return nil, cluster.Unsupported(recvErr)
		}
		out = append(out, frame)
	}
}

// failedNodes returns the nodes whose frame carries an error, with the errors themselves.
func failedNodes(frames []*transferv1.SessionsResponse) (nodes, errs []string) {
	for _, f := range frames {
		if _, isErr := f.GetOutcome().(*transferv1.SessionsResponse_Error); isErr {
			nodes = append(nodes, f.GetNodeId())
			errs = append(errs, f.GetNodeId()+": "+f.GetError())
		}
	}
	return nodes, errs
}

// ClassifySessions merges per-node list frames into occupants, one per session id, and
// flags a split. The copy with the newest heartbeat represents the session: a heartbeat
// that reached only some nodes must not make it look older than it is.
func ClassifySessions(frames []*transferv1.SessionsResponse) Classification {
	byID := map[string]*Occupant{}
	var nodes []string
	for _, f := range frames {
		if !slices.Contains(nodes, f.GetNodeId()) {
			nodes = append(nodes, f.GetNodeId())
		}
		l := f.GetSession()
		if l == nil {
			continue
		}
		o, ok := byID[l.GetSessionId()]
		if !ok {
			o = &Occupant{Lease: l}
			byID[l.GetSessionId()] = o
		} else if l.GetLastHeartbeatAt() > o.Lease.GetLastHeartbeatAt() {
			o.Lease = l
		}
		if !slices.Contains(o.Nodes, f.GetNodeId()) {
			o.Nodes = append(o.Nodes, f.GetNodeId())
		}
	}
	c := Classification{Split: len(byID) > 1}
	for _, o := range byID {
		sort.Strings(o.Nodes)
		if len(o.Nodes) != len(nodes) {
			c.Split = true
		}
		c.Occupants = append(c.Occupants, *o)
	}
	sort.Slice(c.Occupants, func(i, j int) bool { return c.Occupants[i].Lease.GetSessionId() < c.Occupants[j].Lease.GetSessionId() })
	return c
}

// CreateSession runs Plan(create{preempt}) and collects the snapshot inventory. A data node
// holding any other session refuses with ALREADY_EXISTS unless opts.Preempt is set; the
// refusal is rendered with the occupants the nodes list and exits 2.
// The liaison rolls a failed create back itself, so a failure leaves no session behind.
// Transient failures are retried with jitter; a failure after the session id was announced
// exits 3 because snapshots may have been taken in between.
func CreateSession(ctx context.Context, conn *grpc.ClientConn, selectors []*transferv1.Selector, opts SessionOptions) (*Session, *cluster.PlanResult, error) {
	req := &transferv1.PlanRequest{
		Selectors: selectors,
		Session:   &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{Preempt: opts.Preempt}},
	}
	var errs []error
	var last error
	var announced []string
retry:
	for attempt := 1; attempt <= createRetries; attempt++ {
		sess, result, err := createOnce(ctx, conn, req, opts.OnCreated)
		if err == nil {
			return sess, result, nil
		}
		errs = append(errs, err)
		last = err
		if status.Code(err) == codes.AlreadyExists {
			occupants, refusal := refusedByOccupant(ctx, conn, errs, announced)
			if occupants == nil || opts.Confirm == nil || opts.Preempt {
				return nil, nil, refusal
			}
			if !opts.Confirm(occupants.String()) {
				return nil, nil, declinedTakeover(occupants)
			}
			opts.Preempt, opts.Confirm = true, nil
			return CreateSession(ctx, conn, selectors, opts)
		}
		if sess != nil {
			announced = append(announced, sess.ID)
		}
		if !retryableCreate(err) || attempt == createRetries {
			break
		}
		delay := createRetryBase*time.Duration(attempt) + time.Duration(rand.Int64N(int64(createRetryBase))) //nolint:gosec // back-off jitter, not a secret
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			last = ctx.Err()
			errs = append(errs, last)
			break retry
		case <-timer.C:
		}
	}
	code := createExitCode(len(announced) > 0, last)
	if code == exitcode.ExitRuntime && len(announced) > 0 {
		// The liaison rolls back a create that fails on its side, but one the client gave up
		// on after the id was announced may stand: name how to release it.
		hints := make([]string, len(announced))
		for i, id := range announced {
			hints[i] = "bydbctl data release-session --id " + id
		}
		return nil, nil, exitcode.Exit(code, "create export session: %w; a session that was announced may still be held, release it with: %s",
			errors.Join(errs...), strings.Join(hints, "; "))
	}
	return nil, nil, exitcode.Exit(code, "create export session: %w", errors.Join(errs...))
}

// createExitCode follows exitcode.ExitCodeFor while no attempt got past the first frame, and is
// exitcode.ExitRuntime afterwards; a data node without data export is a preflight refusal either
// way, since the liaison rolls the half-created session back.
func createExitCode(started bool, last error) int {
	if started && status.Code(last) != codes.Unimplemented {
		return exitcode.ExitRuntime
	}
	return exitcode.ExitCodeFor(last)
}

// refusedByOccupant lists the sessions once so the refusal names the occupant(s) with
// their last heartbeat: the operator, not the node, decides whether they are abandoned. An
// occupant that an earlier attempt of this run announced was not rolled back by the liaison
// (each attempt's own is): it is this run's to release. occupants are the sessions the
// operator may take over, nil when there are none to offer.
func refusedByOccupant(ctx context.Context, conn *grpc.ClientConn, errs []error, announced []string) (occupants *Classification, refusal error) {
	listCtx, cancel := context.WithTimeout(ctx, sessionsCallTimeout)
	defer cancel()
	frames, err := ListSessions(listCtx, conn)
	own := announced
	if err == nil {
		own = nil
		for _, f := range frames {
			if id := f.GetSession().GetSessionId(); slices.Contains(announced, id) && !slices.Contains(own, id) {
				own = append(own, id)
			}
		}
	}
	if len(own) > 0 {
		hints := make([]string, len(own))
		for i, id := range own {
			hints[i] = "bydbctl data release-session --id " + id
		}
		return nil, exitcode.Exit(exitcode.ExitRuntime, "create export session: %w; an earlier attempt announced a session that may still be held, release it with: %s",
			errors.Join(errs...), strings.Join(hints, "; "))
	}
	if err != nil {
		errs = append(errs, err)
		return nil, exitcode.Exit(exitcode.ExitPreflight, "another export session is held; give --preempt to take it over: %w", errors.Join(errs...))
	}
	c := ClassifySessions(frames)
	return &c, exitcode.Exit(exitcode.ExitPreflight, "another export session is held; give --preempt to take it over "+
		"(check its last heartbeat before you do):\n%s%w", c, errors.Join(errs...))
}

// declinedTakeover is the refusal after the operator chose not to take the occupants over,
// which were listed with the question: it names how to take them over or release them.
func declinedTakeover(occupants *Classification) error {
	hints := make([]string, 0, len(occupants.Occupants))
	for _, o := range occupants.Occupants {
		hints = append(hints, "bydbctl data release-session --id "+o.Lease.GetSessionId())
	}
	return exitcode.Exit(exitcode.ExitPreflight, "the export session(s) above are left as they are; give --preempt to take them over, or release them with: %s",
		strings.Join(hints, "; "))
}

func retryableCreate(err error) bool {
	switch status.Code(err) {
	case codes.Internal, codes.Unavailable, codes.Aborted:
		return true
	default:
		return false
	}
}

// createOnce runs one create attempt. The returned Session is non-nil as soon as the first
// frame announced the id, even on failure, so the caller knows snapshots may have existed.
func createOnce(ctx context.Context, conn *grpc.ClientConn, req *transferv1.PlanRequest, onCreated func(string)) (*Session, *cluster.PlanResult, error) {
	planCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := transferv1.NewExportServiceClient(conn).Plan(planCtx, req)
	if err != nil {
		return nil, nil, cluster.Unsupported(err)
	}
	first, err := stream.Recv()
	if err != nil {
		return nil, nil, cluster.Unsupported(err)
	}
	created := first.GetCreated()
	if created.GetSessionId() == "" {
		return nil, nil, exitcode.Exit(exitcode.ExitRuntime, "the first Plan frame of a create must announce the session id, got %T", first.GetFrame())
	}
	sess := &Session{ID: created.GetSessionId()}
	if onCreated != nil {
		onCreated(sess.ID)
	}
	result, err := cluster.CollectPlanFrames(stream)
	if err != nil {
		return sess, nil, fmt.Errorf("session %s: %w", sess.ID, err)
	}
	if len(result.UnreachableNodes) > 0 {
		// A create is strict on the liaison; a summary naming unreachable nodes would leave
		// their data out, so the session is given back rather than used.
		err = fmt.Errorf("session %s: data nodes %v did not take the snapshot", sess.ID, result.UnreachableNodes)
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), sessionsCallTimeout)
		defer releaseCancel()
		released, releaseErr := ReleaseSession(releaseCtx, conn, sess.ID)
		switch {
		case releaseErr != nil:
			err = errors.Join(err, releaseErr)
		case len(released.Failed) > 0:
			nodes := make([]string, 0, len(released.Failed))
			for _, f := range released.Failed {
				nodes = append(nodes, f.GetNodeId())
			}
			err = errors.Join(err, fmt.Errorf("export session %s could not be released on %v; release it with: bydbctl data release-session --id %s",
				sess.ID, nodes, sess.ID))
		}
		return sess, nil, err
	}
	sess.Preempted = result.PreemptedSessionIDs
	return sess, result, nil
}

// heartbeatInterval is HeartbeatInterval, shortened by tests.
var heartbeatInterval = HeartbeatInterval

// StartHeartbeat sends ACTION_HEARTBEAT for the session every interval until stop is called
// (0 means HeartbeatInterval). A transport failure or an error frame is a WARN line on warn, not fatal:
// the lease outlives several missed heartbeats, so the next tick retries. FAILED_PRECONDITION
// means the session expired or its lease is unreadable on some node; it is sent on lost once
// and the heartbeat stops.
func StartHeartbeat(ctx context.Context, conn *grpc.ClientConn, sessionID string, interval time.Duration, l *logger.Logger, warn io.Writer,
) (stop func(), lost <-chan error) {
	if interval <= 0 {
		interval = heartbeatInterval
	}
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	lostCh := make(chan error, 1)
	run.Go(hbCtx, "export-heartbeat", l, func(ctx context.Context) {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := HeartbeatOnce(ctx, conn, sessionID)
				switch {
				case err == nil, ctx.Err() != nil:
				case heartbeatLost(err):
					lostCh <- fmt.Errorf("export session %s lost: %w", sessionID, err)
					return
				default:
					fmt.Fprintf(warn, "WARN export session %s heartbeat failed, retrying: %v\n", sessionID, err)
				}
			}
		}
	})
	return func() {
		cancel()
		<-done
	}, lostCh
}

func heartbeatLost(err error) bool { return status.Code(err) == codes.FailedPrecondition }

// HeartbeatOnce sends one ACTION_HEARTBEAT and fails if any data node did not take it: a
// node that was unreachable or no longer holds the session answers an error frame, which is
// a coverage gap; an expired session fails the call itself with FAILED_PRECONDITION.
func HeartbeatOnce(ctx context.Context, conn *grpc.ClientConn, sessionID string) error {
	ctx, cancel := context.WithTimeout(ctx, sessionsCallTimeout)
	defer cancel()
	frames, err := sessions(ctx, conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: sessionID})
	if err != nil {
		return err
	}
	if nodes, errs := failedNodes(frames); len(nodes) > 0 {
		return fmt.Errorf("heartbeat did not reach %v: %s", nodes, strings.Join(errs, "; "))
	}
	return nil
}

// ReleaseResult sorts the data nodes by their answer to a release.
type ReleaseResult struct {
	// Released are the nodes that held the session and deleted it.
	Released []string
	// NotHeld are the nodes that did not hold the session; nothing was deleted there.
	NotHeld []string
	// Failed are the error frames, one per node that could not release; an unexpected outcome becomes one.
	Failed []*transferv1.SessionsResponse
}

// ReleaseSession asks every data node to drop the session snapshot and sorts their answers.
func ReleaseSession(ctx context.Context, conn *grpc.ClientConn, sessionID string) (*ReleaseResult, error) {
	req := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: sessionID}
	stream, err := transferv1.NewExportServiceClient(conn).Sessions(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("release session %s: %w", sessionID, cluster.Unsupported(err))
	}
	result := &ReleaseResult{}
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return result, nil
		}
		if recvErr != nil {
			return nil, fmt.Errorf("release session %s: %w", sessionID, cluster.Unsupported(recvErr))
		}
		switch outcome := frame.GetOutcome().(type) {
		case *transferv1.SessionsResponse_Done:
			result.Released = append(result.Released, frame.GetNodeId())
		case *transferv1.SessionsResponse_None:
			result.NotHeld = append(result.NotHeld, frame.GetNodeId())
		case *transferv1.SessionsResponse_Error:
			result.Failed = append(result.Failed, frame)
		default:
			// A release answers done, none or error; anything else is a protocol error for that node.
			result.Failed = append(result.Failed, &transferv1.SessionsResponse{
				NodeId:  frame.GetNodeId(),
				Outcome: &transferv1.SessionsResponse_Error{Error: fmt.Sprintf("unexpected release outcome %T", outcome)},
			})
		}
	}
}
