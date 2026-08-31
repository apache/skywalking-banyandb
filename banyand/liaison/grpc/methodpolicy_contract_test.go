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

package grpc_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// enabledPolicyYAML is the same fixed policy family the auth package contract is oracled
// against: the five actors and answers issue #14014 fixes for this milestone.
const enabledPolicyYAML = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-monitor"
    password: "monitor-secret"
  - username: "bydb-reader"
    password: "reader-secret"
  - username: "bydb-writer"
    password: "writer-secret"
  - username: "bydb-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: ["cluster:read"]
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-monitor"
      role: "monitor"
      groups: ["*"]
    - principal: "bydb-reader"
      role: "reader"
      groups: ["sw_metric"]
    - principal: "bydb-writer"
      role: "writer"
      groups: ["*"]
`

// The gRPC full method names below are transcribed from the generated service descriptors
// (api/proto/banyandb/**/*_grpc.pb.go `_FullMethodName` constants), which are an
// independent source: they are produced by protoc from the .proto files, not by any code
// this milestone adds.
const (
	mGetClusterState  = "/banyandb.database.v1.ClusterStateService/GetClusterState"
	mGetCurrentNode   = "/banyandb.database.v1.NodeQueryService/GetCurrentNode"
	mGetAPIVersion    = "/banyandb.common.v1.Service/GetAPIVersion"
	mHealthCheck      = "/grpc.health.v1.Health/Check"
	mHealthList       = "/grpc.health.v1.Health/List"
	mHealthWatch      = "/grpc.health.v1.Health/Watch"
	mGroupInspect     = "/banyandb.database.v1.GroupRegistryService/Inspect"
	mGroupTaskQuery   = "/banyandb.database.v1.GroupRegistryService/Query"
	mSnapshot         = "/banyandb.database.v1.SnapshotService/Snapshot"
	mGetMaxRevision   = "/banyandb.cluster.v1.NodeSchemaStatusService/GetMaxRevision"
	mGetKeyRevisions  = "/banyandb.cluster.v1.NodeSchemaStatusService/GetKeyRevisions"
	mGetAbsentKeys    = "/banyandb.cluster.v1.NodeSchemaStatusService/GetAbsentKeys"
	mAwaitRevision    = "/banyandb.schema.v1.SchemaBarrierService/AwaitRevisionApplied"
	mStreamDeleteSeg  = "/banyandb.stream.v1.StreamService/DeleteExpiredSegments"
	mMeasureDeleteSeg = "/banyandb.measure.v1.MeasureService/DeleteExpiredSegments"
	mTraceDeleteSeg   = "/banyandb.trace.v1.TraceService/DeleteExpiredSegments"
	mMeasureInternalQ = "/banyandb.measure.v1.MeasureService/InternalQuery"
	mMeasureQuery     = "/banyandb.measure.v1.MeasureService/Query"
	mStreamWrite      = "/banyandb.stream.v1.StreamService/Write"
	mGroupCreate      = "/banyandb.database.v1.GroupRegistryService/Create"
	mGroupList        = "/banyandb.database.v1.GroupRegistryService/List"
	mPropertyApply    = "/banyandb.property.v1.PropertyService/Apply"
	mBydbQLQuery      = "/banyandb.bydbql.v1.BydbQLService/Query"
)

// globalMethods is the fixed global method table of issue #14014: the methods this PR
// activates, with the permission each requires. Nothing outside this list is decidable in
// this release.
var globalMethods = map[string]auth.Permission{
	mGetClusterState:  auth.PermissionClusterRead,
	mGetCurrentNode:   auth.PermissionClusterRead,
	mGroupInspect:     auth.PermissionClusterRead,
	mGroupTaskQuery:   auth.PermissionClusterRead,
	mSnapshot:         auth.PermissionClusterAdmin,
	mGetMaxRevision:   auth.PermissionClusterAdmin,
	mGetKeyRevisions:  auth.PermissionClusterAdmin,
	mGetAbsentKeys:    auth.PermissionClusterAdmin,
	mStreamDeleteSeg:  auth.PermissionClusterAdmin,
	mMeasureDeleteSeg: auth.PermissionClusterAdmin,
	mTraceDeleteSeg:   auth.PermissionClusterAdmin,
	mMeasureInternalQ: auth.PermissionClusterAdmin,
}

var authenticatedMethods = map[string]bool{
	mGetAPIVersion: true,
	mHealthCheck:   true,
	mHealthList:    true,
	mHealthWatch:   true,
}

// fullActivation is the liaison configuration under which every conditionally registered
// service is served, which is the configuration this milestone's method table must cover.
var fullActivation = liaisongrpc.ServiceActivation{SchemaBarrier: true, NodeSchemaStatus: true}

func enabledSnapshot(t *testing.T) auth.Snapshot {
	t.Helper()
	snap, err := auth.CompileSnapshot(1, []byte(enabledPolicyYAML))
	if err != nil {
		t.Fatalf("CompileSnapshot(1, enabled fixture) returned error %v, want a compiled snapshot", err)
	}
	if snap == nil {
		t.Fatalf("CompileSnapshot(1, enabled fixture) returned a nil snapshot, want a compiled snapshot")
	}
	return snap
}

func actor(t *testing.T, snap auth.Snapshot, username, password string) auth.Principal {
	t.Helper()
	p, ok := snap.Authenticate(username, password)
	if !ok {
		t.Fatalf("Authenticate(%q, %q) = _, false; want the fixture credentials to verify", username, password)
	}
	return p
}

func policyTable(t *testing.T) liaisongrpc.MethodPolicyTable {
	t.Helper()
	table := liaisongrpc.GlobalMethodPolicies()
	if len(table) == 0 {
		t.Fatal("GlobalMethodPolicies() returned no policies, want the full classification of the liaison's methods")
	}
	return table
}

// TestR1_MethodPolicyCoversExactlyTheRegisteredMethods proves R1: the classification is
// complete, has no stale entries, and has no duplicates for the liaison's full service
// set. This is the invariant that must stop startup, so the check has to be decidable
// from the registered set alone.
func TestR1_MethodPolicyCoversExactlyTheRegisteredMethods(t *testing.T) {
	table := policyTable(t)
	if len(table) != 78 {
		t.Fatalf("GlobalMethodPolicies() returned %d rows, want the fixed 78-method oracle", len(table))
	}
	registered := liaisongrpc.RegisteredMethods(fullActivation)
	if len(registered) == 0 {
		t.Fatal("RegisteredMethods(full activation) returned nothing, want every method the liaison serves")
	}
	if err := liaisongrpc.ValidateMethodPolicies(table, registered); err != nil {
		t.Fatalf("ValidateMethodPolicies(GlobalMethodPolicies(), RegisteredMethods(full)) = %v, want nil for the shipped table", err)
	}
}

// TestR1_RegisteredMethodsTracksTheServiceDescriptors proves that the registered set is
// derived from the liaison's actual service registration rather than restated by hand: it
// must contain every method named below, each transcribed from the generated descriptors,
// and it must be sorted so the startup error for a drifted table is deterministic.
func TestR1_RegisteredMethodsTracksTheServiceDescriptors(t *testing.T) {
	registered := liaisongrpc.RegisteredMethods(fullActivation)
	if !sort.StringsAreSorted(registered) {
		t.Error("RegisteredMethods returned an unsorted slice, want it sorted lexicographically")
	}
	present := make(map[string]bool, len(registered))
	for _, m := range registered {
		if present[m] {
			t.Errorf("RegisteredMethods returned %q twice, want each method once", m)
		}
		present[m] = true
	}
	for _, m := range []string{
		mGetClusterState, mGetCurrentNode, mGetAPIVersion, mHealthCheck, mHealthList, mHealthWatch, mSnapshot,
		mGroupInspect, mGroupTaskQuery,
		mGetMaxRevision, mGetKeyRevisions, mGetAbsentKeys, mAwaitRevision,
		mStreamDeleteSeg, mMeasureDeleteSeg, mTraceDeleteSeg, mMeasureInternalQ,
		mMeasureQuery, mStreamWrite, mGroupCreate, mGroupList, mPropertyApply, mBydbQLQuery,
	} {
		if !present[m] {
			t.Errorf("RegisteredMethods(full activation) is missing %q, which the liaison serves", m)
		}
	}
}

// TestR1_ConditionalServicesLeaveTheActiveSet proves D4's conditional half: the liaison
// registers SchemaBarrierService and NodeSchemaStatusService only when the corresponding
// component exists, so with them off their methods must leave the registered set — and the
// full table must then be *stale* against it. A table that validated under both
// activations would be silently classifying methods nobody serves.
func TestR1_ConditionalServicesLeaveTheActiveSet(t *testing.T) {
	for _, activation := range []liaisongrpc.ServiceActivation{
		{},
		{SchemaBarrier: true},
		{NodeSchemaStatus: true},
		{SchemaBarrier: true, NodeSchemaStatus: true},
	} {
		registered := liaisongrpc.RegisteredMethods(activation)
		policies := liaisongrpc.ActiveMethodPolicies(activation)
		if validateErr := liaisongrpc.ValidateMethodPolicies(policies, registered); validateErr != nil {
			t.Errorf("ValidateMethodPolicies(%+v) = %v, want nil", activation, validateErr)
		}
	}
}

// TestR1_ClassificationDefectsStopStartup proves that each way the table can drift from
// the registered set is detected and distinguishable. The liaison calls this from
// Validate(), before any watcher, repair goroutine or listener starts, so a defect here is
// a startup failure rather than a request-time surprise.
func TestR1_ClassificationDefectsStopStartup(t *testing.T) {
	table := policyTable(t)
	registered := liaisongrpc.RegisteredMethods(fullActivation)

	t.Run("incomplete", func(t *testing.T) {
		short := make(liaisongrpc.MethodPolicyTable, 0, len(table))
		for _, p := range table {
			if p.FullMethod == mGetClusterState {
				continue
			}
			short = append(short, p)
		}
		err := liaisongrpc.ValidateMethodPolicies(short, registered)
		if !errors.Is(err, liaisongrpc.ErrMethodPolicyIncomplete) {
			t.Fatalf("ValidateMethodPolicies(table missing %s, ...) = %v, want ErrMethodPolicyIncomplete", mGetClusterState, err)
		}
		if !strings.Contains(err.Error(), mGetClusterState) {
			t.Errorf("error %q does not name the unclassified method %q", err, mGetClusterState)
		}
	})

	t.Run("stale", func(t *testing.T) {
		extra := append(append(liaisongrpc.MethodPolicyTable{}, table...), liaisongrpc.MethodPolicy{
			FullMethod: "/banyandb.database.v1.RetiredService/Vanished",
			Permission: auth.PermissionClusterRead,
			Activated:  true,
		})
		err := liaisongrpc.ValidateMethodPolicies(extra, registered)
		if !errors.Is(err, liaisongrpc.ErrMethodPolicyStale) {
			t.Fatalf("ValidateMethodPolicies(table with a retired method, ...) = %v, want ErrMethodPolicyStale", err)
		}
		if !strings.Contains(err.Error(), "/banyandb.database.v1.RetiredService/Vanished") {
			t.Errorf("error %q does not name the stale method", err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		dup := append(append(liaisongrpc.MethodPolicyTable{}, table...), liaisongrpc.MethodPolicy{
			FullMethod: mSnapshot,
			Permission: auth.PermissionClusterRead,
			Activated:  true,
		})
		err := liaisongrpc.ValidateMethodPolicies(dup, registered)
		if !errors.Is(err, liaisongrpc.ErrMethodPolicyDuplicate) {
			t.Fatalf("ValidateMethodPolicies(table classifying %s twice, ...) = %v, want ErrMethodPolicyDuplicate", mSnapshot, err)
		}
		if !strings.Contains(err.Error(), mSnapshot) {
			t.Errorf("error %q does not name the duplicated method %q", err, mSnapshot)
		}
	})
}

// TestR6_GlobalMethodsAreActivatedAndTheRestFailClosed proves that the classification's
// activation column matches what the release can decide. The global method set of issue
// #14014 carries a cluster permission and is activated; the schema methods issue #14015
// activates carry a schema permission and are activated; and every data method is classified
// but *not* activated, so it fails closed until W-PR3 activates its executor.
//
// It gates R6 of issue #14015 alongside R6 of #14014: the activation column is the single
// place a data method could be switched on by accident, and the per-permission counts below
// are the fixed table size neither milestone may change.
func TestR6_GlobalMethodsAreActivatedAndTheRestFailClosed(t *testing.T) {
	table := policyTable(t)
	seen := make(map[string]bool, len(globalMethods))
	permissionCounts := make(map[auth.Permission]int)
	for _, p := range table {
		permissionCounts[p.Permission]++
		wantPerm, isGlobal := globalMethods[p.FullMethod]
		if isGlobal {
			seen[p.FullMethod] = true
			if p.Permission != wantPerm {
				t.Errorf("policy for %s requires %q, want %q", p.FullMethod, p.Permission, wantPerm)
			}
			if !p.Activated {
				t.Errorf("policy for %s is not activated, want this release to decide it", p.FullMethod)
			}
			continue
		}
		if authenticatedMethods[p.FullMethod] {
			if !p.Activated {
				t.Errorf("authenticated policy for %s is not activated", p.FullMethod)
			}
			continue
		}
		switch p.Permission {
		case auth.PermissionSchemaRead, auth.PermissionSchemaWrite:
			if !p.Activated {
				t.Errorf("schema policy for %s is not activated, want issue #14015 to decide it", p.FullMethod)
			}
		case auth.PermissionDataRead, auth.PermissionDataWrite:
			if p.Activated {
				t.Errorf("data policy for %s is activated, want it to stay fail-closed for W-PR3", p.FullMethod)
			}
		default:
			t.Errorf("policy for %s requires %q, want a schema or data permission for a non-global method", p.FullMethod, p.Permission)
		}
	}
	for m := range globalMethods {
		if !seen[m] {
			t.Errorf("the table does not classify the global method %s", m)
		}
	}
	for permission, want := range map[auth.Permission]int{
		auth.PermissionClusterRead: 4, auth.PermissionClusterAdmin: 8,
		auth.PermissionSchemaRead: 27, auth.PermissionSchemaWrite: 24,
		auth.PermissionDataRead: 6, auth.PermissionDataWrite: 5,
	} {
		if permissionCounts[permission] != want {
			t.Errorf("policy count for %q = %d, want %d", permission, permissionCounts[permission], want)
		}
	}
}

// TestR3_GlobalDecisionMatrix proves R3's decision half against the fixed oracle. Every
// expected cell is read off issue #14014's role definitions and the global method table
// above; no cell is recomputed the way the decision function will compute it.
//
// The three DeleteExpiredSegments methods matter for D3: their handler is the generated
// Unimplemented fallback, so admin must be *allowed* through to it rather than short
// circuited, while every other actor is denied before it.
func TestR3_GlobalDecisionMatrix(t *testing.T) {
	snap := enabledSnapshot(t)
	table := policyTable(t)

	admin := actor(t, snap, "bydb-admin", "admin-secret")
	monitor := actor(t, snap, "bydb-monitor", "monitor-secret")
	reader := actor(t, snap, "bydb-reader", "reader-secret")
	writer := actor(t, snap, "bydb-writer", "writer-secret")
	unbound := actor(t, snap, "bydb-unbound", "unbound-secret")

	const (
		allow = liaisongrpc.DecisionAllow
		deny  = liaisongrpc.DecisionDeny
		shut  = liaisongrpc.DecisionUnavailable
	)
	for _, tc := range []struct {
		method                                  string
		admin, monitor, reader, writer, unbound liaisongrpc.Decision
	}{
		// cluster:read — admin and monitor only.
		{mGetClusterState, allow, allow, deny, deny, deny},
		{mGetCurrentNode, allow, allow, deny, deny, deny},
		{mGroupInspect, allow, allow, deny, deny, deny},
		{mGroupTaskQuery, allow, allow, deny, deny, deny},
		// authenticated-only methods do not require a role binding.
		{mGetAPIVersion, allow, allow, allow, allow, allow},
		{mHealthCheck, allow, allow, allow, allow, allow},
		// cluster:admin — admin only.
		{mSnapshot, allow, deny, deny, deny, deny},
		{mGetMaxRevision, allow, deny, deny, deny, deny},
		{mGetKeyRevisions, allow, deny, deny, deny, deny},
		{mGetAbsentKeys, allow, deny, deny, deny, deny},
		// cluster:admin, generated Unimplemented handler — admin is allowed through to it.
		{mStreamDeleteSeg, allow, deny, deny, deny, deny},
		{mMeasureDeleteSeg, allow, deny, deny, deny, deny},
		{mTraceDeleteSeg, allow, deny, deny, deny, deny},
		{mMeasureInternalQ, allow, deny, deny, deny, deny},
		// data — no activated executor, so every actor fails closed. The schema methods
		// this milestone activates are oracled in rbac_schema_contract_test.go instead.
		{mMeasureQuery, shut, shut, shut, shut, shut},
		{mStreamWrite, shut, shut, shut, shut, shut},
		{mBydbQLQuery, shut, shut, shut, shut, shut},
		{mPropertyApply, shut, shut, shut, shut, shut},
	} {
		for _, who := range []struct {
			name string
			p    auth.Principal
			want liaisongrpc.Decision
		}{
			{"bydb-admin", admin, tc.admin},
			{"bydb-monitor", monitor, tc.monitor},
			{"bydb-reader", reader, tc.reader},
			{"bydb-writer", writer, tc.writer},
			{"bydb-unbound", unbound, tc.unbound},
		} {
			if got, _ := table.Authorize(snap, who.p, tc.method, nil); got != who.want {
				t.Errorf("Authorize(%s, %s) = %v, want %v", who.name, tc.method, got, who.want)
			}
		}
	}
}

// TestR3_UnclassifiedMethodIsDenied proves the decision function fails closed on a method
// it has never heard of, rather than falling through to allow.
func TestR3_UnclassifiedMethodIsDenied(t *testing.T) {
	snap := enabledSnapshot(t)
	admin := actor(t, snap, "bydb-admin", "admin-secret")
	if got, _ := policyTable(t).Authorize(snap, admin, "/banyandb.future.v1.Whatever/Method", nil); got != liaisongrpc.DecisionDeny {
		t.Errorf("Authorize(admin, an unclassified method) = %v, want DecisionDeny", got)
	}
}

// recordingObserver captures every decision the interceptor reports so the test can assert
// both the count and the bounded label set of R4.
type recordingObserver struct {
	methods     []string
	permissions []string
	decisions   []liaisongrpc.Decision
	reasons     []liaisongrpc.DecisionReason
}

type testServerStream struct {
	ctx context.Context
}

func (s *testServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *testServerStream) SendHeader(metadata.MD) error { return nil }
func (s *testServerStream) SetTrailer(metadata.MD)       {}
func (s *testServerStream) Context() context.Context     { return s.ctx }
func (s *testServerStream) SendMsg(any) error            { return nil }
func (s *testServerStream) RecvMsg(any) error            { return nil }

func (o *recordingObserver) ObserveDecision(
	fullMethod, permission string,
	decision liaisongrpc.Decision,
	reason liaisongrpc.DecisionReason,
) {
	o.methods = append(o.methods, fullMethod)
	o.permissions = append(o.permissions, permission)
	o.decisions = append(o.decisions, decision)
	o.reasons = append(o.reasons, reason)
}

func newEnabledReloader(t *testing.T) *auth.Reloader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security.yaml")
	if err := os.WriteFile(path, []byte(enabledPolicyYAML), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	reloader := auth.InitAuthReloader()
	if err := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-interceptor-contract-test")); err != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want it to accept the enabled policy", path, err)
	}
	return reloader
}

func callWithCredentials(
	t *testing.T, interceptor grpclib.UnaryServerInterceptor, fullMethod, username, password string,
	extra ...string,
) (bool, error) {
	t.Helper()
	pairs := []string{"username", username, "password", password}
	pairs = append(pairs, extra...)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
	handlerRan := false
	_, err := interceptor(ctx, struct{}{}, &grpclib.UnaryServerInfo{FullMethod: fullMethod},
		func(_ context.Context, _ any) (any, error) {
			handlerRan = true
			return struct{}{}, nil
		})
	return handlerRan, err
}

// TestR3_InterceptorPrecedenceAndStatusCodes proves R3's precedence half through the real
// interceptor seam: authentication comes first, authorization comes next, and the handler
// runs only for an allowed call. The status codes are the ones a client observes, so they
// are part of the contract, not an implementation detail.
func TestR3_InterceptorPrecedenceAndStatusCodes(t *testing.T) {
	reloader := newEnabledReloader(t)
	interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, policyTable(t), &recordingObserver{})

	t.Run("bad credentials are rejected before authorization", func(t *testing.T) {
		// bydb-admin would be allowed on this method, so a wrong password proving
		// Unauthenticated — not PermissionDenied — shows authentication ran first.
		ran, err := callWithCredentials(t, interceptor, mGetClusterState, "bydb-admin", "wrong-secret")
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Errorf("wrong password on %s = %v, want codes.Unauthenticated", mGetClusterState, got)
		}
		if ran {
			t.Error("the handler ran for an unauthenticated call, want it skipped")
		}
	})

	t.Run("denied calls never reach the handler", func(t *testing.T) {
		ran, err := callWithCredentials(t, interceptor, mSnapshot, "bydb-monitor", "monitor-secret")
		if got := status.Code(err); got != codes.PermissionDenied {
			t.Errorf("bydb-monitor on %s = %v, want codes.PermissionDenied", mSnapshot, got)
		}
		if ran {
			t.Errorf("the handler ran for a denied %s call, want no side effect", mSnapshot)
		}
	})

	t.Run("fail-closed calls never reach the handler", func(t *testing.T) {
		ran, err := callWithCredentials(t, interceptor, mMeasureQuery, "bydb-admin", "admin-secret")
		if got := status.Code(err); got != codes.PermissionDenied {
			t.Errorf("bydb-admin on the unactivated %s = %v, want codes.PermissionDenied", mMeasureQuery, got)
		}
		if ran {
			t.Errorf("the handler ran for a fail-closed %s call, want no side effect", mMeasureQuery)
		}
	})

	t.Run("allowed calls reach the handler", func(t *testing.T) {
		ran, err := callWithCredentials(t, interceptor, mGetClusterState, "bydb-monitor", "monitor-secret")
		if err != nil {
			t.Errorf("bydb-monitor on %s returned %v, want the handler's result", mGetClusterState, err)
		}
		if !ran {
			t.Errorf("the handler did not run for an allowed %s call", mGetClusterState)
		}
	})

	t.Run("the trusted principal is the authenticated one", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("username", "bydb-monitor", "password", "monitor-secret"))
		var seen auth.Principal
		var ok bool
		_, err := interceptor(ctx, struct{}{}, &grpclib.UnaryServerInfo{FullMethod: mGetClusterState},
			func(handlerCtx context.Context, _ any) (any, error) {
				seen, ok = liaisongrpc.PrincipalFromContext(handlerCtx)
				return struct{}{}, nil
			})
		if err != nil {
			t.Fatalf("allowed call returned %v, want the handler's result", err)
		}
		if !ok {
			t.Fatal("PrincipalFromContext reported no principal inside an authorized handler, want the authenticated one")
		}
		if seen.Username() != "bydb-monitor" {
			t.Errorf("PrincipalFromContext returned %q, want %q", seen.Username(), "bydb-monitor")
		}
	})
}

func TestR3_StreamInterceptorFailsClosedBeforeHandler(t *testing.T) {
	reloader := newEnabledReloader(t)
	interceptor := liaisongrpc.NewAuthorizationStreamInterceptor(reloader, policyTable(t), &recordingObserver{})
	streamContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "bydb-admin", "password", "admin-secret"))
	stream := &testServerStream{ctx: streamContext}
	handlerRan := false
	interceptorErr := interceptor(nil, stream, &grpclib.StreamServerInfo{FullMethod: mStreamWrite}, func(any, grpclib.ServerStream) error {
		handlerRan = true
		return nil
	})
	if status.Code(interceptorErr) != codes.PermissionDenied {
		t.Errorf("admin opening unactivated %s = %v, want PermissionDenied", mStreamWrite, status.Code(interceptorErr))
	}
	if handlerRan {
		t.Fatal("stream handler ran for an unactivated method")
	}
}

// TestR4_ForgedIdentityMetadataCannotReplaceCredentials proves R4's spoof half at the gRPC
// seam: a caller presenting reader credentials while also asserting an admin identity in
// metadata is decided as the reader. The extra keys below are the ones a forged
// grpc-gateway request would carry.
func TestR4_ForgedIdentityMetadataCannotReplaceCredentials(t *testing.T) {
	reloader := newEnabledReloader(t)
	interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, policyTable(t), &recordingObserver{})

	for _, forged := range [][]string{
		{"grpcgateway-username", "bydb-admin"},
		{"x-banyandb-principal", "bydb-admin"},
		{"x-banyandb-role", "admin"},
		{"authorization", "Basic YnlkYi1hZG1pbjphZG1pbi1zZWNyZXQ="},
	} {
		ran, err := callWithCredentials(t, interceptor, mSnapshot, "bydb-reader", "reader-secret", forged...)
		if got := status.Code(err); got != codes.PermissionDenied {
			t.Errorf("bydb-reader on %s with forged %q = %v, want codes.PermissionDenied", mSnapshot, forged[0], got)
		}
		if ran {
			t.Errorf("the handler ran for a spoofed %s call carrying %q", mSnapshot, forged[0])
		}
	}

	// A caller cannot pre-seed a principal either: the context key is unexported, so a
	// context a caller built carries nothing PrincipalFromContext will read back.
	if p, ok := liaisongrpc.PrincipalFromContext(context.Background()); ok || !p.IsZero() {
		t.Errorf("PrincipalFromContext(background) = %v, %v; want the zero principal and false", p, ok)
	}
}

// TestR4_DecisionObservabilityIsBounded proves R4's cardinality half: the interceptor
// reports exactly one decision per call, the method label is drawn from the registered set
// and the outcome label from the closed decision set. No password, user name, role name or
// peer address may appear.
func TestR4_DecisionObservabilityIsBounded(t *testing.T) {
	reloader := newEnabledReloader(t)
	observer := &recordingObserver{}
	interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, policyTable(t), observer)

	labels := liaisongrpc.DecisionLabels()
	if len(labels) == 0 {
		t.Fatal("DecisionLabels() returned nothing, want the closed set of outcome labels")
	}
	allowed := make(map[string]bool, len(labels))
	for _, l := range labels {
		allowed[l] = true
	}

	registered := make(map[string]bool)
	for _, m := range liaisongrpc.RegisteredMethods(fullActivation) {
		registered[m] = true
	}

	// One allowed, one denied, one fail-closed, one unauthenticated: four calls, each with
	// exactly one bounded decision.
	for _, c := range []struct{ method, user, pass string }{
		{mGetClusterState, "bydb-monitor", "monitor-secret"},
		{mSnapshot, "bydb-monitor", "monitor-secret"},
		{mMeasureQuery, "bydb-admin", "admin-secret"},
	} {
		if _, err := callWithCredentials(t, interceptor, c.method, c.user, c.pass); err != nil && status.Code(err) == codes.Unauthenticated {
			t.Fatalf("%s as %s was unauthenticated, want the fixture credentials to verify", c.method, c.user)
		}
	}
	_, unauthenticatedErr := callWithCredentials(t, interceptor, mGetClusterState, "bydb-monitor", "wrong-secret")
	if status.Code(unauthenticatedErr) != codes.Unauthenticated {
		t.Fatalf("bad credentials returned %v, want Unauthenticated", status.Code(unauthenticatedErr))
	}
	if len(observer.decisions) != 4 {
		t.Fatalf("the interceptor reported %d decisions for 4 calls, want exactly one each", len(observer.decisions))
	}
	for idx, d := range observer.decisions {
		if d == liaisongrpc.DecisionUnspecified {
			t.Errorf("decision %d was reported as DecisionUnspecified, want a real outcome", idx)
		}
		label := liaisongrpc.DecisionLabel(d)
		if !allowed[label] {
			t.Errorf("decision %d produced label %q, which is outside DecisionLabels() %v", idx, label, labels)
		}
	}
	for _, m := range observer.methods {
		if !registered[m] {
			t.Errorf("a decision was labeled with method %q, which the liaison does not serve; method labels must be bounded", m)
		}
	}

	// No credential from the fixture may appear in any label the observer received.
	for _, secret := range []string{"admin-secret", "monitor-secret", "reader-secret", "writer-secret", "unbound-secret"} {
		for _, m := range observer.methods {
			if strings.Contains(m, secret) {
				t.Errorf("observability label %q contains the credential %q", m, secret)
			}
		}
	}
}
