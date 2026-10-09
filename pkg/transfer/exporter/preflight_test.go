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
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestPreflight_PicksFirstReachableLiaison(t *testing.T) {
	f := newFakeLiaison()
	addr := serveLiaison(t, f)
	conn, info, err := Preflight(context.Background(), []string{"127.0.0.1:1", addr}, ConnectOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if info.Liaison != addr || info.Standalone || len(info.DataNodes) != 2 || info.DataNodes[0].GetMetadata().GetName() != "data-a:17912" {
		t.Fatalf("info = %+v", info)
	}
}

func TestPreflight_RejectsANonLiaison(t *testing.T) {
	f := newFakeLiaison()
	f.self.Roles = []databasev1.Role{databasev1.Role_ROLE_DATA}
	addr := serveLiaison(t, f)
	_, _, err := Preflight(context.Background(), []string{addr}, ConnectOptions{Timeout: 2 * time.Second})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitPreflight || !strings.Contains(err.Error(), "not a liaison") {
		t.Fatalf("want exit 2 'not a liaison', got %v", err)
	}
}

func TestPreflight_StandaloneFallback(t *testing.T) {
	f := newFakeLiaison()
	f.dataNodes = nil
	f.self.Roles = []databasev1.Role{databasev1.Role_ROLE_LIAISON, databasev1.Role_ROLE_DATA}
	addr := serveLiaison(t, f)
	conn, info, err := Preflight(context.Background(), []string{addr}, ConnectOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if !info.Standalone || len(info.DataNodes) != 1 || info.DataNodes[0].GetMetadata().GetName() != "liaison-1:17912" {
		t.Fatalf("standalone fallback must use the process itself: %+v", info)
	}
}

func TestPreflight_NoDataNodesIsAnError(t *testing.T) {
	f := newFakeLiaison()
	f.dataNodes = nil
	addr := serveLiaison(t, f)
	_, _, err := Preflight(context.Background(), []string{addr}, ConnectOptions{Timeout: 2 * time.Second})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitPreflight || !strings.Contains(err.Error(), "has not discovered any data node") {
		t.Fatalf("want exit 2 'has not discovered any data node', got %v", err)
	}
}

func TestPreflight_AllUnreachable(t *testing.T) {
	_, _, err := Preflight(context.Background(), []string{"127.0.0.1:1"}, ConnectOptions{Timeout: time.Second})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitPreflight {
		t.Fatalf("want exit 2, got %v", err)
	}
	if _, _, err := Preflight(context.Background(), nil, ConnectOptions{}); err == nil {
		t.Fatal("no candidates must be a usage error")
	}
}

func TestPreflight_CredentialsReachEveryRPC(t *testing.T) {
	f := newFakeLiaison()
	f.requireUser = "bydb-admin"
	addr := serveLiaison(t, f)
	conn, _, err := Preflight(context.Background(), []string{addr}, ConnectOptions{Timeout: 2 * time.Second, Username: "bydb-admin", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// A later RPC on the same connection must still carry the credentials.
	if _, err := FetchGroups(context.Background(), conn); err != nil {
		t.Fatalf("credentials must be attached to every RPC: %v", err)
	}
	if _, _, err := Preflight(context.Background(), []string{addr}, ConnectOptions{Timeout: 2 * time.Second}); err == nil {
		t.Fatal("without credentials the liaison must reject the preflight")
	}
}

func TestPreflight_GetClusterStateFails(t *testing.T) {
	f := newFakeLiaison()
	f.failGetClusterState = true
	addr := serveLiaison(t, f)
	_, _, err := Preflight(context.Background(), []string{addr}, ConnectOptions{Timeout: 2 * time.Second})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitPreflight {
		t.Fatalf("GetClusterState failure must produce ExitPreflight, got %v", err)
	}
	if !strings.Contains(err.Error(), "GetClusterState") {
		t.Fatalf("error must mention GetClusterState: %v", err)
	}
}

func TestPreflight_SkipsACandidateLostInTransport(t *testing.T) {
	broken := newFakeLiaison()
	broken.failGetClusterState = true // answers UNAVAILABLE: a transport-class failure
	good := newFakeLiaison()
	conn, info, err := Preflight(context.Background(), []string{serveLiaison(t, broken), serveLiaison(t, good)}, ConnectOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("a transport failure on one candidate must fall through to the next: %v", err)
	}
	defer conn.Close()
	if info.Liaison == "" || len(info.DataNodes) != 2 {
		t.Fatalf("info = %+v", info)
	}
	_, _, err = Preflight(context.Background(), []string{"127.0.0.1:1", serveLiaison(t, broken)}, ConnectOptions{Timeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "127.0.0.1:1") || !strings.Contains(err.Error(), "GetClusterState") {
		t.Fatalf("every candidate's failure must be reported, got %v", err)
	}
	refusing := newFakeLiaison()
	refusing.requireUser = "admin" // UNAUTHENTICATED is a refusal, not a transport loss
	_, _, err = Preflight(context.Background(), []string{serveLiaison(t, refusing), serveLiaison(t, good)}, ConnectOptions{Timeout: 2 * time.Second})
	var refusedErr *ExitError
	if !errors.As(err, &refusedErr) || refusedErr.Code != ExitPreflight || !strings.Contains(err.Error(), "refused the credentials") ||
		!strings.Contains(err.Error(), "GetCurrentNode") {
		t.Fatalf("a credential refusal from GetCurrentNode must stop at the refusing candidate as refused credentials, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = Preflight(ctx, []string{serveLiaison(t, good)}, ConnectOptions{Timeout: 2 * time.Second}); !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled ctx must stop before dialing, got %v", err)
	}
}

func TestPreflight_StopsAtACandidateThatRefusesTheCredentials(t *testing.T) {
	refusing := newFakeLiaison()
	refusing.requireUser = "admin"
	refusing.healthAuth = true
	refusingAddr := serveLiaison(t, refusing)
	good := newFakeLiaison()
	_, _, err := Preflight(context.Background(), []string{refusingAddr, serveLiaison(t, good)}, ConnectOptions{Timeout: time.Second})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitPreflight || !strings.Contains(err.Error(), "liaison "+refusingAddr+" refused the credentials") {
		t.Fatalf("a health check refused for the credentials must stop at that candidate with exit 2, got %v", err)
	}
}

func TestPreflight_SkipsALiaisonWithAnEmptyRegistry(t *testing.T) {
	empty := newFakeLiaison()
	empty.dataNodes = nil
	good := newFakeLiaison()
	goodAddr := serveLiaison(t, good)
	conn, info, err := Preflight(context.Background(), []string{serveLiaison(t, empty), goodAddr}, ConnectOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("a liaison that has not discovered data nodes yet must be skipped: %v", err)
	}
	defer conn.Close()
	if info.Liaison != goodAddr || len(info.DataNodes) != 2 {
		t.Fatalf("info = %+v", info)
	}
}

func TestPreflight_SkipsACandidateThatNeverAnswers(t *testing.T) {
	saved := describeTimeout
	describeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { describeTimeout = saved })
	hung := newFakeLiaison()
	hung.hangDescribe = true
	good := newFakeLiaison()
	goodAddr := serveLiaison(t, good)
	start := time.Now()
	conn, info, err := Preflight(context.Background(), []string{serveLiaison(t, hung), goodAddr}, ConnectOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("a candidate that never answers must time out and fall through to the next: %v", err)
	}
	defer conn.Close()
	if info.Liaison != goodAddr {
		t.Fatalf("info = %+v", info)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the hung candidate must be bounded by the per-candidate timeout, took %v", elapsed)
	}
}

// A canceled or expired caller context is a runtime failure carrying the context error, not
// "none of the liaison addresses is usable".
func TestPreflight_CanceledContextIsARuntimeFailure(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := Preflight(canceledCtx, []string{"127.0.0.1:1"}, ConnectOptions{Timeout: time.Second})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitRuntime || !errors.Is(err, context.Canceled) {
		t.Fatalf("want exit 3 wrapping context.Canceled, got %v", err)
	}

	hung := newFakeLiaison()
	hung.hangDescribe = true
	deadlineCtx, cancelDeadline := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelDeadline()
	_, _, err = Preflight(deadlineCtx, []string{serveLiaison(t, hung), serveLiaison(t, newFakeLiaison())}, ConnectOptions{Timeout: 2 * time.Second})
	if !errors.As(err, &exitErr) || exitErr.Code != ExitRuntime || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want exit 3 wrapping context.DeadlineExceeded, got %v", err)
	}
}

func TestFetchGroups_ExitCodeFollowsTheStatus(t *testing.T) {
	f := newFakeLiaison()
	f.failList = true
	conn, _, err := Preflight(context.Background(), []string{serveLiaison(t, f)}, ConnectOptions{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = FetchGroups(context.Background(), conn)
	if code := ExitCodeFor(err); code != ExitRuntime || !strings.Contains(err.Error(), "list groups") {
		t.Fatalf("an UNAVAILABLE registry is a run-time failure (exit 3), got exit %d: %v", code, err)
	}
}
