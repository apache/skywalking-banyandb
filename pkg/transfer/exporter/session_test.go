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
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

func TestReleaseSession(t *testing.T) {
	f := newFakeLiaison()
	conn := dial(t, serveLiaison(t, f))
	result, err := ReleaseSession(context.Background(), conn, "abcdabcdabcdabcdabcdabcdabcdabcd")
	if err != nil || len(result.Failed) != 0 || len(result.NotHeld) != 0 || len(result.Released) == 0 {
		t.Fatalf("release = %+v %v", result, err)
	}
	if r := f.lastSessions(); r.GetAction() != transferv1.SessionsRequest_ACTION_RELEASE || r.GetSessionId() != "abcdabcdabcdabcdabcdabcdabcdabcd" {
		t.Fatalf("release must send ACTION_RELEASE for the session: %+v", r)
	}
}

// Every node's answer is sorted: done is released, none did not hold the session, and an
// error frame is a node the release did not reach.
func TestReleaseSession_SortsNodesByOutcome(t *testing.T) {
	f := newFakeLiaison()
	f.releaseFrames = []*transferv1.SessionsResponse{
		doneFrame("data-a:17912"),
		noneFrame("data-d:17912"),
		errorFrame("data-b:17912", "remove lease: busy"),
		errorFrame("data-c:17912", "connection refused"),
	}
	conn := dial(t, serveLiaison(t, f))
	result, err := ReleaseSession(context.Background(), conn, "abcdabcdabcdabcdabcdabcdabcdabcd")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Released) != 1 || result.Released[0] != "data-a:17912" {
		t.Fatalf("done is a released node, got %v", result.Released)
	}
	if len(result.NotHeld) != 1 || result.NotHeld[0] != "data-d:17912" {
		t.Fatalf("none is a node that did not hold the session, got %v", result.NotHeld)
	}
	failed := result.Failed
	if len(failed) != 2 || failed[0].GetNodeId() != "data-b:17912" || failed[1].GetNodeId() != "data-c:17912" {
		t.Fatalf("a node whose frame carries an error is a failed node, got %v", failed)
	}
	if failed[0].GetError() != "remove lease: busy" {
		t.Fatalf("every failed frame keeps its reason, got %v", failed[0])
	}
}

// A frame without a release outcome (no outcome, or a LIST session) is never counted as
// released: it fails that node with a synthesized error frame.
func TestReleaseSession_UnexpectedOutcomeFails(t *testing.T) {
	f := newFakeLiaison()
	f.releaseFrames = []*transferv1.SessionsResponse{
		doneFrame("node-1"),
		{NodeId: "node-2"},
		{NodeId: "node-3", Outcome: &transferv1.SessionsResponse_Session{Session: &transferv1.SessionLease{}}},
	}
	conn := dial(t, serveLiaison(t, f))
	result, err := ReleaseSession(context.Background(), conn, "abcdabcdabcdabcdabcdabcdabcdabcd")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Released) != 1 || result.Released[0] != "node-1" || len(result.NotHeld) != 0 {
		t.Fatalf("only done is released, got %+v", result)
	}
	if len(result.Failed) != 2 || result.Failed[0].GetNodeId() != "node-2" || result.Failed[1].GetNodeId() != "node-3" {
		t.Fatalf("an unexpected outcome fails its node, got %v", result.Failed)
	}
	if !strings.Contains(result.Failed[0].GetError(), "unexpected release outcome <nil>") ||
		!strings.Contains(result.Failed[1].GetError(), "SessionsResponse_Session") {
		t.Fatalf("the synthesized frame names the outcome, got %v", result.Failed)
	}
}

func TestExitCodeFor(t *testing.T) {
	cases := map[int]error{
		exitcode.ExitUsage:     status.Error(codes.InvalidArgument, "bad id"),
		exitcode.ExitPreflight: status.Error(codes.PermissionDenied, "no"),
		exitcode.ExitRuntime:   status.Error(codes.Unavailable, "down"),
	}
	for want, err := range cases {
		if got := exitcode.ExitCodeFor(err); got != want {
			t.Fatalf("exitcode.ExitCodeFor(%v) = %d, want %d", err, got, want)
		}
	}
	for _, code := range []codes.Code{codes.Unauthenticated, codes.FailedPrecondition, codes.NotFound, codes.Unimplemented} {
		if got := exitcode.ExitCodeFor(status.Error(code, "x")); got != exitcode.ExitPreflight {
			t.Fatalf("exitcode.ExitCodeFor(%v) = %d, want %d", code, got, exitcode.ExitPreflight)
		}
	}
	if exitcode.ExitCodeFor(errors.New("plain")) != exitcode.ExitRuntime || exitcode.ExitCodeFor(exitcode.Exit(exitcode.ExitUsage, "u")) != exitcode.ExitUsage {
		t.Fatal("a plain error is a runtime failure; an exitcode.ExitError keeps its code")
	}
}

func leaseAt(id string, beatAgo time.Duration, now time.Time) *transferv1.SessionLease {
	return &transferv1.SessionLease{SessionId: id, LastHeartbeatAt: now.Add(-beatAgo).UnixNano(), ExpiresAt: now.Add(time.Hour).UnixNano()}
}

func sessionFrame(node string, l *transferv1.SessionLease) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{NodeId: node, Outcome: &transferv1.SessionsResponse_Session{Session: l}}
}

// listedOn lists session id, last seen a minute ago, on both data nodes.
func listedOn(id string) []*transferv1.SessionsResponse {
	l := leaseAt(id, time.Minute, time.Now())
	return []*transferv1.SessionsResponse{sessionFrame("data-a:17912", l), sessionFrame("data-b:17912", l)}
}

func shortenRetries(t *testing.T) {
	t.Helper()
	orig := createRetryBase
	createRetryBase = time.Millisecond
	t.Cleanup(func() { createRetryBase = orig })
}

func TestClassifySessions_MergesByIDAndDetectsSplit(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	older := leaseAt("aaaa", 20*time.Minute, now)
	newer := leaseAt("aaaa", 5*time.Minute, now)
	other := leaseAt("bbbb", 2*time.Hour, now)
	c := ClassifySessions([]*transferv1.SessionsResponse{
		sessionFrame("n1", older), sessionFrame("n2", newer), sessionFrame("n3", other),
	})
	if len(c.Occupants) != 2 || c.Occupants[0].Lease.GetSessionId() != "aaaa" || c.Occupants[1].Lease.GetSessionId() != "bbbb" {
		t.Fatalf("occupants must be merged by id in ascending order: %+v", c)
	}
	if c.Occupants[0].Lease.GetLastHeartbeatAt() != newer.GetLastHeartbeatAt() || len(c.Occupants[0].Nodes) != 2 {
		t.Fatalf("the copy with the newest heartbeat must represent the session: %+v", c.Occupants[0])
	}
	if !c.Split {
		t.Fatal("two session ids are a split")
	}
	s := c.String()
	beat := time.Unix(0, newer.GetLastHeartbeatAt()).UTC().Format(time.RFC3339)
	for _, want := range []string{"session aaaa", "n1", "n2", beat, "session bbbb", "split"} {
		if !strings.Contains(s, want) {
			t.Fatalf("String() lacks %q:\n%s", want, s)
		}
	}
	if len(ClassifySessions(nil).Occupants) != 0 {
		t.Fatal("no frames means no sessions")
	}
}

func TestClassifySessions_SplitCountsDistinctNodes(t *testing.T) {
	lease := leaseAt("sess1", time.Minute, time.Now())
	c := ClassifySessions([]*transferv1.SessionsResponse{sessionFrame("data-a", lease), sessionFrame("data-a", lease)})
	if len(c.Occupants) != 1 || len(c.Occupants[0].Nodes) != 1 || c.Split {
		t.Fatalf("the same node answering twice is one holder: %+v", c)
	}
	if whole := ClassifySessions(listedOn("aaaa")); whole.Split || len(whole.Occupants) != 1 {
		t.Fatalf("a session held by every node is not split: %+v", whole)
	}
	partial := ClassifySessions(append(listedOn("aaaa"), noneFrame("data-c:17912")))
	if !partial.Split {
		t.Fatal("a session held by only some nodes is split")
	}
}

func TestCreateSession_RefusesOccupantWithoutPreempt(t *testing.T) {
	f := newFakeLiaison()
	f.listed = listedOn("b0b0")
	conn := dial(t, serveLiaison(t, f))
	var announced string
	_, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{OnCreated: func(id string) { announced = id }})
	var exitErr *exitcode.ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != exitcode.ExitPreflight {
		t.Fatalf("want exit 2, got %v", err)
	}
	beat := time.Unix(0, f.listed[0].GetSession().GetLastHeartbeatAt()).UTC().Format(time.RFC3339)
	for _, want := range []string{"give --preempt to take it over", "session b0b0", "data-a:17912", "data-b:17912", beat, "already exists"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal must render the occupant (%q missing):\n%v", want, err)
		}
	}
	if f.createAttempts != 1 || f.lastPlan().GetCreate().GetPreempt() {
		t.Fatalf("ALREADY_EXISTS is terminal and the client never preempts on its own: attempts=%d", f.createAttempts)
	}
	if announced != "feedfeed" {
		t.Fatalf("the id must be announced as soon as the created frame arrives, got %q", announced)
	}
}

func TestCreateSession_PreemptTakesOverAndReports(t *testing.T) {
	f := newFakeLiaison()
	f.listed = listedOn("b0b0")
	conn := dial(t, serveLiaison(t, f))
	sess, result, err := CreateSession(context.Background(), conn, nil, SessionOptions{Preempt: true})
	if err != nil || !f.lastPlan().GetCreate().GetPreempt() || sess.ID != "feedfeed" {
		t.Fatalf("preempt must take over the occupant: %v %+v", err, sess)
	}
	if len(sess.Preempted) != 2 || sess.Preempted[0] != "b0b0" || len(result.AnsweredNodes) != 2 {
		t.Fatalf("preempted ids must be surfaced for the warning: %+v %+v", sess, result)
	}
}

// An operator who confirms takes the occupants over: the create is made once more with
// preempt. One who declines gets the refusal, and the occupants stay.
func TestCreateSession_ConfirmTakesOverOnYesOnly(t *testing.T) {
	for _, yes := range []bool{true, false} {
		f := newFakeLiaison()
		f.listed = listedOn("b0b0")
		conn := dial(t, serveLiaison(t, f))
		var asked string
		sess, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{Confirm: func(occupants string) bool {
			asked = occupants
			return yes
		}})
		if !strings.Contains(asked, "session b0b0") || !strings.Contains(asked, "data-a:17912") {
			t.Fatalf("the question must name the occupants, got %q", asked)
		}
		if yes {
			if err != nil || f.createAttempts != 2 || !f.lastPlan().GetCreate().GetPreempt() || len(sess.Preempted) == 0 || sess.Preempted[0] != "b0b0" {
				t.Fatalf("a yes takes over with preempt: err=%v attempts=%d sess=%+v", err, f.createAttempts, sess)
			}
			continue
		}
		var exitErr *exitcode.ExitError
		if !errors.As(err, &exitErr) || exitErr.Code != exitcode.ExitPreflight || f.createAttempts != 1 ||
			!strings.Contains(err.Error(), "left as they are") || !strings.Contains(err.Error(), "bydbctl data release-session --id b0b0") {
			t.Fatalf("a no is a refusal naming how to release the occupant, made once: err=%v attempts=%d", err, f.createAttempts)
		}
	}
}

// --preempt needs no question.
func TestCreateSession_PreemptIsNotAsked(t *testing.T) {
	f := newFakeLiaison()
	f.listed = listedOn("b0b0")
	conn := dial(t, serveLiaison(t, f))
	_, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{Preempt: true, Confirm: func(string) bool {
		t.Fatal("asked although --preempt was given")
		return false
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateSession_FailureAfterCreatedExitsRuntime(t *testing.T) {
	shortenRetries(t)
	f := newFakeLiaison()
	f.failAfterFirstFrame = true
	conn := dial(t, serveLiaison(t, f))
	_, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{})
	var exitErr *exitcode.ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != exitcode.ExitRuntime || !strings.Contains(err.Error(), "session feedfeed") ||
		!strings.Contains(err.Error(), "release it with: bydbctl data release-session --id feedfeed") {
		t.Fatalf("a failure after the session id is known exits 3 naming the session and how to release it, got %v", err)
	}
	if f.createAttempts != createRetries {
		t.Fatalf("INTERNAL is retried, attempts=%d", f.createAttempts)
	}
}

// A retry refused with ALREADY_EXISTS blames the session an earlier attempt announced only
// when that session still holds the cluster; another occupant is a preflight refusal.
func TestCreateSession_AlreadyExistsAfterAnAnnouncedAttempt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		occupant string
		message  string
		code     int
	}{
		{"the earlier attempt still holds", "feedfeed", "release it with: bydbctl data release-session --id feedfeed", exitcode.ExitRuntime},
		{"another session holds", "b0b0", "give --preempt to take it over", exitcode.ExitPreflight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortenRetries(t)
			f := newFakeLiaison()
			f.failAfterFirstFrame = true
			f.onCreateAttempt = func(f *fakeLiaison, attempt int) {
				if attempt == 2 {
					f.failAfterFirstFrame = false
					f.listed = listedOn(tc.occupant)
				}
			}
			conn := dial(t, serveLiaison(t, f))
			_, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{})
			requireExit(t, err, tc.code, tc.message)
		})
	}
}

func TestCreateSession_RetriesATransientFailure(t *testing.T) {
	shortenRetries(t)
	f := newFakeLiaison()
	f.failCreateTimes = 1
	conn := dial(t, serveLiaison(t, f))
	sess, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{})
	if err != nil || sess.ID != "feedfeed" || f.createAttempts != 2 {
		t.Fatalf("one UNAVAILABLE must be retried: err=%v attempts=%d", err, f.createAttempts)
	}
}

func TestCreateSession_ExhaustedRetriesReportEveryAttempt(t *testing.T) {
	shortenRetries(t)
	f := newFakeLiaison()
	f.failCreateTimes = createRetries + 10
	conn := dial(t, serveLiaison(t, f))
	_, _, err := CreateSession(context.Background(), conn, nil, SessionOptions{})
	var exitErr *exitcode.ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != exitcode.ExitRuntime {
		t.Fatalf("UNAVAILABLE before any frame maps through exitcode.ExitCodeFor, got %v", err)
	}
	for i := 1; i <= createRetries; i++ {
		if !strings.Contains(err.Error(), fmt.Sprintf("#%d", i)) {
			t.Fatalf("the error must accumulate every attempt, lacks #%d:\n%v", i, err)
		}
	}
}

func TestCreateSession_ContextCanceledDuringBackoff(t *testing.T) {
	orig := createRetryBase
	createRetryBase = 500 * time.Millisecond
	t.Cleanup(func() { createRetryBase = orig })
	f := newFakeLiaison()
	f.failCreateTimes = createRetries + 10
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := CreateSession(ctx, conn, nil, SessionOptions{})
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("error must wrap the context error, got: %v", err)
	}
}

func TestHeartbeat_SendsHeartbeatAction(t *testing.T) {
	f := newFakeLiaison()
	conn := dial(t, serveLiaison(t, f))
	stop, lost := StartHeartbeat(context.Background(), conn, "abcd", 20*time.Millisecond, logger.GetLogger("t"), io.Discard)
	deadline := time.Now().Add(5 * time.Second)
	for f.countSessions(transferv1.SessionsRequest_ACTION_HEARTBEAT) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if n := f.countSessions(transferv1.SessionsRequest_ACTION_HEARTBEAT); n < 3 {
		t.Fatalf("want >=3 heartbeats, got %d", n)
	}
	if r := f.lastSessions(); r.GetSessionId() != "abcd" {
		t.Fatalf("heartbeat must name the session: %+v", r)
	}
	select {
	case err := <-lost:
		t.Fatalf("a healthy heartbeat must not report the session lost: %v", err)
	default:
	}
}

func TestHeartbeat_LostOnFailedPrecondition(t *testing.T) {
	f := newFakeLiaison()
	f.hbCode = codes.FailedPrecondition
	conn := dial(t, serveLiaison(t, f))
	stop, lost := StartHeartbeat(context.Background(), conn, "abcd", 20*time.Millisecond, logger.GetLogger("t"), io.Discard)
	defer stop()
	select {
	case err := <-lost:
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "abcd") {
			t.Fatalf("lost must carry the node's answer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FAILED_PRECONDITION must be reported on lost")
	}
	time.Sleep(100 * time.Millisecond)
	if n := f.countSessions(transferv1.SessionsRequest_ACTION_HEARTBEAT); n != 1 {
		t.Fatalf("the heartbeat must stop after the session is lost, got %d", n)
	}
}

func TestHeartbeat_OtherStatusesAreRetried(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.Unavailable, codes.Internal} {
		if heartbeatLost(status.Error(code, "x")) {
			t.Fatalf("%v must not stop the heartbeat", code)
		}
	}
}

func TestHeartbeatOnce_ErrorFramesAreACoverageGap(t *testing.T) {
	f := newFakeLiaison()
	f.hbFrames = []*transferv1.SessionsResponse{doneFrame("data-a:17912"), errorFrame("data-b:17912", "export session abcd not found on this node")}
	conn := dial(t, serveLiaison(t, f))
	err := HeartbeatOnce(context.Background(), conn, "abcd")
	if err == nil || !strings.Contains(err.Error(), "heartbeat did not reach [data-b:17912]") || heartbeatLost(err) {
		t.Fatalf("an error frame must fail the heartbeat naming the node, got %v", err)
	}
}

func TestCreateExitCode(t *testing.T) {
	for _, c := range []struct {
		err     error
		name    string
		want    int
		started bool
	}{
		{name: "refused before the id", err: status.Error(codes.FailedPrecondition, "x"), want: exitcode.ExitPreflight},
		{name: "failed after the id", err: status.Error(codes.Internal, "x"), started: true, want: exitcode.ExitRuntime},
		{name: "a node without data export", err: status.Error(codes.Unimplemented, "x"), started: true, want: exitcode.ExitPreflight},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := createExitCode(c.started, c.err); got != c.want {
				t.Fatalf("got exit %d, want %d", got, c.want)
			}
		})
	}
}
