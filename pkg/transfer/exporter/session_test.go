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
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

func TestReleaseSession(t *testing.T) {
	f := newFakeLiaison()
	conn := dial(t, serveLiaison(t, f))
	result, err := ReleaseSession(context.Background(), conn, "abcd")
	if err != nil || len(result.Failed) != 0 || len(result.NotHeld) != 0 || len(result.Released) == 0 {
		t.Fatalf("release = %+v %v", result, err)
	}
	if r := f.lastSessions(); r.GetAction() != transferv1.SessionsRequest_ACTION_RELEASE || r.GetSessionId() != "abcd" {
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
	result, err := ReleaseSession(context.Background(), conn, "abcd")
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
	result, err := ReleaseSession(context.Background(), conn, "abcd")
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
		ExitUsage:     status.Error(codes.InvalidArgument, "bad id"),
		ExitPreflight: status.Error(codes.PermissionDenied, "no"),
		ExitRuntime:   status.Error(codes.Unavailable, "down"),
	}
	for want, err := range cases {
		if got := ExitCodeFor(err); got != want {
			t.Fatalf("ExitCodeFor(%v) = %d, want %d", err, got, want)
		}
	}
	for _, code := range []codes.Code{codes.Unauthenticated, codes.FailedPrecondition, codes.NotFound, codes.Unimplemented} {
		if got := ExitCodeFor(status.Error(code, "x")); got != ExitPreflight {
			t.Fatalf("ExitCodeFor(%v) = %d, want %d", code, got, ExitPreflight)
		}
	}
	if ExitCodeFor(errors.New("plain")) != ExitRuntime || ExitCodeFor(Exit(ExitUsage, "u")) != ExitUsage {
		t.Fatal("a plain error is a runtime failure; an ExitError keeps its code")
	}
}
