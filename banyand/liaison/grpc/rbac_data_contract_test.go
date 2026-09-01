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
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// dataPolicyYAML is the canonical fixture family of the #13994 design narrowed to the actors
// issue #14016 needs: an operator administrator, an exact-scope writer and reader on alpha, a
// wildcard reader, a custom cluster-only monitor that proves a cluster grant buys no data
// access, and an authenticated principal with no binding at all.
const dataPolicyYAML = `
users:
  - username: "dat-admin"
    password: "admin-secret"
  - username: "dat-writer-alpha"
    password: "writer-alpha-secret"
  - username: "dat-reader-alpha"
    password: "reader-alpha-secret"
  - username: "dat-reader-all"
    password: "reader-all-secret"
  - username: "dat-monitor"
    password: "monitor-secret"
  - username: "dat-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: ["cluster:read"]
  bindings:
    - principal: "dat-admin"
      role: "admin"
      groups: ["*"]
    - principal: "dat-writer-alpha"
      role: "writer"
      groups: ["rbac-alpha"]
    - principal: "dat-reader-alpha"
      role: "reader"
      groups: ["rbac-alpha"]
    - principal: "dat-reader-all"
      role: "reader"
      groups: ["*"]
    - principal: "dat-monitor"
      role: "monitor"
      groups: ["*"]
`

// revokedDataPolicyYAML is the same fixture with the alpha writer's binding removed. Publishing
// it as a second revision is how a test observes a revocation land on a stream that is already
// open, without the caller reconnecting or the process restarting.
const revokedDataPolicyYAML = `
users:
  - username: "dat-admin"
    password: "admin-secret"
  - username: "dat-writer-alpha"
    password: "writer-alpha-secret"
  - username: "dat-reader-alpha"
    password: "reader-alpha-secret"
rbac:
  enabled: true
  bindings:
    - principal: "dat-admin"
      role: "admin"
      groups: ["*"]
    - principal: "dat-reader-alpha"
      role: "reader"
      groups: ["rbac-alpha"]
`

// usersOnlyDataPolicyYAML is a deployment that authenticates and authorizes nothing, which is
// what every BanyanDB running today with an auth file looks like.
const usersOnlyDataPolicyYAML = `
users:
  - username: "dat-legacy"
    password: "legacy-secret"
`

// The gRPC full method names of the eleven data methods issue #14016 owns, transcribed from
// the generated service descriptors' _FullMethodName constants. protoc produces them from the
// .proto files, so they are an independent source from anything this milestone adds.
// mMeasureQuery, mStreamWrite, mPropertyApply and mBydbQLQuery are already declared by the
// #14014 contract and are reused here.
const (
	mStreamQuery    = "/banyandb.stream.v1.StreamService/Query"
	mMeasureTopN    = "/banyandb.measure.v1.MeasureService/TopN"
	mTraceQuery     = "/banyandb.trace.v1.TraceService/Query"
	mPropertyQuery  = "/banyandb.property.v1.PropertyService/Query"
	mPropertyDelete = "/banyandb.property.v1.PropertyService/Delete"
	mMeasureWrite   = "/banyandb.measure.v1.MeasureService/Write"
	mTraceWrite     = "/banyandb.trace.v1.TraceService/Write"
)

// dataMethodOracle is the fixed eleven-method table of issue #14016, read off its ordered
// internal rounds C1-C8 and the design's API policy map. Nothing here is derived from
// GlobalMethodPolicies(); it is the answer that table has to agree with.
var dataMethodOracle = []struct {
	method     string
	permission auth.Permission
	scope      liaisongrpc.ScopeFamily
}{
	{method: mStreamQuery, permission: auth.PermissionDataRead, scope: liaisongrpc.ScopeRepeatedGroups},
	{method: mMeasureQuery, permission: auth.PermissionDataRead, scope: liaisongrpc.ScopeRepeatedGroups},
	{method: mMeasureTopN, permission: auth.PermissionDataRead, scope: liaisongrpc.ScopeRepeatedGroups},
	{method: mTraceQuery, permission: auth.PermissionDataRead, scope: liaisongrpc.ScopeRepeatedGroups},
	{method: mPropertyQuery, permission: auth.PermissionDataRead, scope: liaisongrpc.ScopeRepeatedGroups},
	{method: mPropertyApply, permission: auth.PermissionDataWrite, scope: liaisongrpc.ScopePropertyGroup},
	{method: mPropertyDelete, permission: auth.PermissionDataWrite, scope: liaisongrpc.ScopeDirectGroup},
	{method: mStreamWrite, permission: auth.PermissionDataWrite, scope: liaisongrpc.ScopeFrameGroups},
	{method: mMeasureWrite, permission: auth.PermissionDataWrite, scope: liaisongrpc.ScopeFrameGroups},
	{method: mTraceWrite, permission: auth.PermissionDataWrite, scope: liaisongrpc.ScopeFrameGroups},
	{method: mBydbQLQuery, permission: auth.PermissionDataRead, scope: liaisongrpc.ScopePostTransform},
}

func dataSnapshot(t *testing.T, policy string) auth.Snapshot {
	t.Helper()
	snap, compileErr := auth.CompileSnapshot(1, []byte(policy))
	if compileErr != nil {
		t.Fatalf("CompileSnapshot(1, data fixture) returned error %v, want a compiled snapshot", compileErr)
	}
	return snap
}

// dataActors resolves the fixture's principals by role name so a table can name the role it
// means rather than repeating credentials.
func dataActors(t *testing.T, snap auth.Snapshot) map[string]auth.Principal {
	t.Helper()
	return map[string]auth.Principal{
		"admin":        actor(t, snap, "dat-admin", "admin-secret"),
		"writer-alpha": actor(t, snap, "dat-writer-alpha", "writer-alpha-secret"),
		"reader-alpha": actor(t, snap, "dat-reader-alpha", "reader-alpha-secret"),
		"reader-all":   actor(t, snap, "dat-reader-all", "reader-all-secret"),
		"monitor":      actor(t, snap, "dat-monitor", "monitor-secret"),
		"unbound":      actor(t, snap, "dat-unbound", "unbound-secret"),
	}
}

func newDataReloader(t *testing.T, policy string) *auth.Reloader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security.yaml")
	if writeErr := os.WriteFile(path, []byte(policy), 0o600); writeErr != nil {
		t.Fatalf("writing %s: %v", path, writeErr)
	}
	reloader := auth.InitAuthReloader()
	if configErr := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-data-contract-test")); configErr != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want it to accept the data fixture", path, configErr)
	}
	return reloader
}

// callDataMethod drives one typed request through a unary interceptor as the named fixture
// user and reports whether the handler ran. A denied data mutation that still reaches its
// handler has already had its side effect, so "did the handler run" is the assertion that
// matters, not the status code alone.
func callDataMethod(
	t *testing.T, interceptor grpclib.UnaryServerInterceptor, fullMethod, username, password string, request any,
) (bool, error) {
	t.Helper()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", username, "password", password))
	handlerRan := false
	_, callErr := interceptor(ctx, request, &grpclib.UnaryServerInfo{FullMethod: fullMethod},
		func(_ context.Context, _ any) (any, error) {
			handlerRan = true
			return struct{}{}, nil
		})
	return handlerRan, callErr
}

// scriptedStream is a ServerStream that hands out a fixed sequence of write frames, standing
// in for the client half of a bidirectional write. It copies each frame into the target the
// caller supplied, exactly as the transport does, so the wrapper under test sees the frame
// through the same seam a real write handler would.
type scriptedStream struct {
	ctx    context.Context
	frames []proto.Message
	index  int
}

func (s *scriptedStream) SetHeader(metadata.MD) error  { return nil }
func (s *scriptedStream) SendHeader(metadata.MD) error { return nil }
func (s *scriptedStream) SetTrailer(metadata.MD)       {}
func (s *scriptedStream) Context() context.Context     { return s.ctx }
func (s *scriptedStream) SendMsg(any) error            { return nil }

func (s *scriptedStream) RecvMsg(target any) error {
	if s.index >= len(s.frames) {
		return io.EOF
	}
	message, isProto := target.(proto.Message)
	if !isProto {
		return status.Errorf(codes.Internal, "scripted stream received %T, want a proto message", target)
	}
	proto.Merge(message, s.frames[s.index])
	s.index++
	return nil
}

// scriptedSnapshots publishes a fixed sequence of revisions, advancing one step per call and
// holding at the last. It is how a revocation is made to land between two frames of a stream
// that is already open.
type scriptedSnapshots struct {
	revisions []auth.Snapshot
	index     int
}

func (s *scriptedSnapshots) CurrentSnapshot() auth.Snapshot {
	current := s.revisions[s.index]
	if s.index < len(s.revisions)-1 {
		s.index++
	}
	return current
}

// TestDataR1_RepeatedGroupReadsAreAllOrNothing proves R1: every native read names its groups
// in one repeated field, the resolved scope set is deduplicated, and the permission is
// required for all of them, so one forbidden group denies the request rather than quietly
// running the authorized part of it.
//
// Every expected cell below is read off issue #14016's R1 and the design's API policy map;
// none is recomputed the way the decision function computes it.
func TestDataR1_RepeatedGroupReadsAreAllOrNothing(t *testing.T) {
	snap := dataSnapshot(t, dataPolicyYAML)
	table := policyTable(t)
	actors := dataActors(t, snap)

	t.Run("the five read methods carry the repeated-groups family", func(t *testing.T) {
		for _, method := range []string{mStreamQuery, mMeasureQuery, mMeasureTopN, mTraceQuery, mPropertyQuery} {
			policy, classified := table.Policy(method)
			if !classified {
				t.Fatalf("GlobalMethodPolicies() does not classify %s", method)
			}
			if policy.Scope != liaisongrpc.ScopeRepeatedGroups {
				t.Errorf("policy for %s reads scope family %v, want ScopeRepeatedGroups", method, policy.Scope)
			}
			if policy.Permission != auth.PermissionDataRead {
				t.Errorf("policy for %s requires %q, want %q", method, policy.Permission, auth.PermissionDataRead)
			}
			if !policy.Activated {
				t.Errorf("policy for %s is not activated, want issue #14016 to decide it", method)
			}
		}
	})

	t.Run("the family reads every listed group and deduplicates it", func(t *testing.T) {
		for _, tc := range []struct {
			request any
			name    string
			want    []string
		}{
			{
				name:    "stream query lists one group",
				request: &streamv1.QueryRequest{Groups: []string{groupAlpha}},
				want:    []string{"rbac-alpha"},
			},
			{
				name:    "measure query repeats a group and names another",
				request: &measurev1.QueryRequest{Groups: []string{groupBeta, groupAlpha, groupBeta}},
				want:    []string{"rbac-alpha", "rbac-beta"},
			},
			{
				name:    "measure topn lists both groups",
				request: &measurev1.TopNRequest{Groups: []string{groupAlpha, groupBeta}},
				want:    []string{"rbac-alpha", "rbac-beta"},
			},
			{
				name:    "trace query lists one group",
				request: &tracev1.QueryRequest{Groups: []string{groupBeta}},
				want:    []string{"rbac-beta"},
			},
			{
				name:    "property query lists both groups",
				request: &propertyv1.QueryRequest{Groups: []string{groupBeta, groupAlpha}},
				want:    []string{"rbac-alpha", "rbac-beta"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, scopeErr := liaisongrpc.RequestScopes(liaisongrpc.ScopeRepeatedGroups, tc.request)
				if scopeErr != nil {
					t.Fatalf("RequestScopes(ScopeRepeatedGroups, %T) = %v, want %v", tc.request, scopeErr, tc.want)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("RequestScopes(ScopeRepeatedGroups, %T) = %v, want %v", tc.request, got, tc.want)
				}
			})
		}
	})

	t.Run("a request naming no readable group is malformed, not denied", func(t *testing.T) {
		for _, request := range []any{
			&measurev1.QueryRequest{},
			&measurev1.QueryRequest{Groups: []string{""}},
			&measurev1.QueryRequest{Groups: []string{groupAlpha, "   "}},
			&streamv1.QueryRequest{Groups: []string{" "}},
			&databasev1QueryStandIn{},
		} {
			if _, scopeErr := liaisongrpc.RequestScopes(liaisongrpc.ScopeRepeatedGroups, request); !errors.Is(scopeErr, liaisongrpc.ErrScopeUnresolvable) {
				t.Errorf("RequestScopes(ScopeRepeatedGroups, %#v) = %v, want an ErrScopeUnresolvable", request, scopeErr)
			}
		}
	})

	t.Run("one forbidden group denies the whole read", func(t *testing.T) {
		for _, tc := range []struct {
			request  any
			who      string
			method   string
			want     liaisongrpc.Decision
			wantCode codes.Code
		}{
			{who: "reader-alpha", method: mMeasureQuery, request: &measurev1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow},
			{who: "reader-alpha", method: mMeasureQuery, request: &measurev1.QueryRequest{Groups: []string{groupBeta}}, want: liaisongrpc.DecisionDeny},
			{
				who: "reader-alpha", method: mMeasureQuery,
				request: &measurev1.QueryRequest{Groups: []string{groupAlpha, groupBeta}}, want: liaisongrpc.DecisionDeny,
			},
			{
				who: "reader-all", method: mMeasureQuery,
				request: &measurev1.QueryRequest{Groups: []string{groupAlpha, groupBeta}}, want: liaisongrpc.DecisionAllow,
			},
			{who: "writer-alpha", method: mStreamQuery, request: &streamv1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow},
			{who: "admin", method: mTraceQuery, request: &tracev1.QueryRequest{Groups: []string{groupBeta}}, want: liaisongrpc.DecisionAllow},
			{who: "monitor", method: mPropertyQuery, request: &propertyv1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionDeny},
			{who: "unbound", method: mMeasureTopN, request: &measurev1.TopNRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionDeny},
			{who: "reader-alpha", method: mMeasureQuery, request: &measurev1.QueryRequest{}, want: liaisongrpc.DecisionInvalidRequest},
		} {
			got, reason := table.Authorize(snap, actors[tc.who], tc.method, tc.request)
			if got != tc.want {
				t.Errorf("Authorize(%s, %s, %v) = %v (%s), want %v", tc.who, tc.method, tc.request, got, reason, tc.want)
			}
		}
	})

	t.Run("a denied multi-group read never reaches its handler", func(t *testing.T) {
		interceptor := liaisongrpc.NewAuthorizationInterceptor(newDataReloader(t, dataPolicyYAML), table, &recordingObserver{})
		ran, callErr := callDataMethod(t, interceptor, mMeasureQuery, "dat-reader-alpha", "reader-alpha-secret",
			&measurev1.QueryRequest{Groups: []string{groupAlpha, groupBeta}})
		if got := status.Code(callErr); got != codes.PermissionDenied {
			t.Errorf("the alpha reader querying alpha+beta = %v, want codes.PermissionDenied", got)
		}
		if ran {
			t.Error("the query handler ran for a partly forbidden multi-group read, want no partial result")
		}
	})
}

// databasev1QueryStandIn is a request type no data service sends. The repeated-groups family
// must reject it rather than treat an unknown shape as addressing no group, which a global
// grant would then satisfy.
type databasev1QueryStandIn struct{}

// TestDataR2_PropertyMutationsResolveTheResourceGroup proves R2: Property Apply is scoped by
// the group inside the property body it carries and Property Delete by the group it names
// directly, both are decided before the handler runs, and a denied mutation therefore leaves
// the record absent or unchanged because no handler ever saw it.
func TestDataR2_PropertyMutationsResolveTheResourceGroup(t *testing.T) {
	snap := dataSnapshot(t, dataPolicyYAML)
	table := policyTable(t)
	actors := dataActors(t, snap)

	t.Run("the two mutations carry their agreed families", func(t *testing.T) {
		for method, wantScope := range map[string]liaisongrpc.ScopeFamily{
			mPropertyApply:  liaisongrpc.ScopePropertyGroup,
			mPropertyDelete: liaisongrpc.ScopeDirectGroup,
		} {
			policy, classified := table.Policy(method)
			if !classified {
				t.Fatalf("GlobalMethodPolicies() does not classify %s", method)
			}
			if policy.Scope != wantScope {
				t.Errorf("policy for %s reads scope family %v, want %v", method, policy.Scope, wantScope)
			}
			if policy.Permission != auth.PermissionDataWrite {
				t.Errorf("policy for %s requires %q, want %q", method, policy.Permission, auth.PermissionDataWrite)
			}
			if !policy.Activated {
				t.Errorf("policy for %s is not activated, want issue #14016 to decide it", method)
			}
		}
	})

	t.Run("apply reads the group out of the property body", func(t *testing.T) {
		got, scopeErr := liaisongrpc.RequestScopes(liaisongrpc.ScopePropertyGroup, applyIn(groupAlpha))
		if scopeErr != nil {
			t.Fatalf("RequestScopes(ScopePropertyGroup, an alpha apply) = %v, want [rbac-alpha]", scopeErr)
		}
		if !reflect.DeepEqual(got, []string{"rbac-alpha"}) {
			t.Errorf("RequestScopes(ScopePropertyGroup, an alpha apply) = %v, want [rbac-alpha]", got)
		}
		for _, malformed := range []any{
			&propertyv1.ApplyRequest{},
			&propertyv1.ApplyRequest{Property: &propertyv1.Property{}},
			&propertyv1.ApplyRequest{Property: &propertyv1.Property{Metadata: &commonv1.Metadata{}}},
			&propertyv1.DeleteRequest{Group: groupAlpha},
		} {
			if _, scopeErr := liaisongrpc.RequestScopes(liaisongrpc.ScopePropertyGroup, malformed); !errors.Is(scopeErr, liaisongrpc.ErrScopeUnresolvable) {
				t.Errorf("RequestScopes(ScopePropertyGroup, %T) = %v, want an ErrScopeUnresolvable", malformed, scopeErr)
			}
		}
	})

	t.Run("delete reads the group it names directly", func(t *testing.T) {
		got, scopeErr := liaisongrpc.RequestScopes(liaisongrpc.ScopeDirectGroup,
			&propertyv1.DeleteRequest{Group: groupBeta, Name: "endpoint", Id: "1"})
		if scopeErr != nil {
			t.Fatalf("RequestScopes(ScopeDirectGroup, a beta property delete) = %v, want [rbac-beta]", scopeErr)
		}
		if !reflect.DeepEqual(got, []string{"rbac-beta"}) {
			t.Errorf("RequestScopes(ScopeDirectGroup, a beta property delete) = %v, want [rbac-beta]", got)
		}
		if _, scopeErr := liaisongrpc.RequestScopes(liaisongrpc.ScopeDirectGroup, &propertyv1.DeleteRequest{}); !errors.Is(scopeErr, liaisongrpc.ErrScopeUnresolvable) {
			t.Errorf("RequestScopes(ScopeDirectGroup, a property delete naming no group) = %v, want an ErrScopeUnresolvable", scopeErr)
		}
	})

	t.Run("the decision matrix", func(t *testing.T) {
		for _, tc := range []struct {
			request any
			who     string
			method  string
			want    liaisongrpc.Decision
		}{
			{who: "writer-alpha", method: mPropertyApply, request: applyIn(groupAlpha), want: liaisongrpc.DecisionAllow},
			{who: "writer-alpha", method: mPropertyApply, request: applyIn(groupBeta), want: liaisongrpc.DecisionDeny},
			{who: "reader-alpha", method: mPropertyApply, request: applyIn(groupAlpha), want: liaisongrpc.DecisionDeny},
			{who: "admin", method: mPropertyApply, request: applyIn(groupBeta), want: liaisongrpc.DecisionAllow},
			{who: "monitor", method: mPropertyApply, request: applyIn(groupAlpha), want: liaisongrpc.DecisionDeny},
			{
				who: "writer-alpha", method: mPropertyDelete,
				request: &propertyv1.DeleteRequest{Group: groupAlpha, Name: "endpoint", Id: "1"}, want: liaisongrpc.DecisionAllow,
			},
			{
				who: "reader-alpha", method: mPropertyDelete,
				request: &propertyv1.DeleteRequest{Group: groupAlpha, Name: "endpoint", Id: "1"}, want: liaisongrpc.DecisionDeny,
			},
			{
				who: "writer-alpha", method: mPropertyDelete,
				request: &propertyv1.DeleteRequest{Group: groupBeta, Name: "endpoint", Id: "1"}, want: liaisongrpc.DecisionDeny,
			},
		} {
			if got, reason := table.Authorize(snap, actors[tc.who], tc.method, tc.request); got != tc.want {
				t.Errorf("Authorize(%s, %s) = %v (%s), want %v", tc.who, tc.method, got, reason, tc.want)
			}
		}
	})

	t.Run("a denied mutation never reaches its handler", func(t *testing.T) {
		interceptor := liaisongrpc.NewAuthorizationInterceptor(newDataReloader(t, dataPolicyYAML), table, &recordingObserver{})
		for _, tc := range []struct {
			request  any
			name     string
			method   string
			username string
			password string
		}{
			{
				name: "the alpha reader cannot apply in alpha", method: mPropertyApply,
				username: "dat-reader-alpha", password: "reader-alpha-secret", request: applyIn(groupAlpha),
			},
			{
				name: "the alpha writer cannot apply in beta", method: mPropertyApply,
				username: "dat-writer-alpha", password: "writer-alpha-secret", request: applyIn(groupBeta),
			},
			{
				name: "the alpha writer cannot delete in beta", method: mPropertyDelete,
				username: "dat-writer-alpha", password: "writer-alpha-secret",
				request: &propertyv1.DeleteRequest{Group: groupBeta, Name: "endpoint", Id: "1"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ran, callErr := callDataMethod(t, interceptor, tc.method, tc.username, tc.password, tc.request)
				if got := status.Code(callErr); got != codes.PermissionDenied {
					t.Errorf("%s on %s = %v, want codes.PermissionDenied", tc.username, tc.method, got)
				}
				if ran {
					t.Errorf("the %s handler ran for a denied call, so the mutation had its side effect", tc.method)
				}
			})
		}

		ran, callErr := callDataMethod(t, interceptor, mPropertyApply, "dat-writer-alpha", "writer-alpha-secret", applyIn(groupAlpha))
		if callErr != nil {
			t.Fatalf("the alpha writer applying in alpha = %v, want the handler's result", callErr)
		}
		if !ran {
			t.Error("the alpha writer's own-scope apply did not reach its handler")
		}
	})
}

// applyIn builds the Property Apply request the fixture uses. Both fixture groups produce an
// identical body apart from the group, so a scope leak shows up as the wrong group answering
// rather than as a differently shaped request.
func applyIn(group string) *propertyv1.ApplyRequest {
	return &propertyv1.ApplyRequest{
		Property: &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: group, Name: "endpoint"},
			Id:       "1",
		},
	}
}

// TestDataR3_WriteFramesUseTheSnapshotInForce proves R3: a write stream is admitted only to a
// principal holding data:write somewhere, each resource-bearing frame is then decided against
// the group that frame resolves to, and each decision reads the snapshot in force at that
// moment — so removing a binding denies the next frame of a stream that is already open.
func TestDataR3_WriteFramesUseTheSnapshotInForce(t *testing.T) {
	snap := dataSnapshot(t, dataPolicyYAML)
	table := policyTable(t)
	actors := dataActors(t, snap)

	t.Run("the three write methods carry the frame family", func(t *testing.T) {
		for _, method := range []string{mStreamWrite, mMeasureWrite, mTraceWrite} {
			policy, classified := table.Policy(method)
			if !classified {
				t.Fatalf("GlobalMethodPolicies() does not classify %s", method)
			}
			if policy.Scope != liaisongrpc.ScopeFrameGroups {
				t.Errorf("policy for %s reads scope family %v, want ScopeFrameGroups", method, policy.Scope)
			}
			if policy.Permission != auth.PermissionDataWrite {
				t.Errorf("policy for %s requires %q, want %q", method, policy.Permission, auth.PermissionDataWrite)
			}
			if !policy.Activated {
				t.Errorf("policy for %s is not activated, want issue #14016 to decide it", method)
			}
		}
	})

	// A principal with no data:write grant anywhere is rejected when the stream opens rather
	// than after it has sent a frame, which is what the design means by rejecting immediately.
	t.Run("opening the stream requires a write grant somewhere", func(t *testing.T) {
		for who, want := range map[string]liaisongrpc.Decision{
			"admin":        liaisongrpc.DecisionAllow,
			"writer-alpha": liaisongrpc.DecisionAllow,
			"reader-alpha": liaisongrpc.DecisionDeny,
			"reader-all":   liaisongrpc.DecisionDeny,
			"monitor":      liaisongrpc.DecisionDeny,
			"unbound":      liaisongrpc.DecisionDeny,
		} {
			for _, method := range []string{mStreamWrite, mMeasureWrite, mTraceWrite} {
				if got, reason := table.Authorize(snap, actors[who], method, nil); got != want {
					t.Errorf("Authorize(%s, opening %s) = %v (%s), want %v", who, method, got, reason, want)
				}
			}
		}
	})

	// The three write services share one metadata contract: the first frame carries it and a
	// later frame may omit it to continue in the same group. Each expected value below is read
	// off that contract, which the write.proto comments state, not off the resolver.
	t.Run("a frame resolves to its own group or continues in the last one", func(t *testing.T) {
		for _, tc := range []struct {
			frame     any
			name      string
			lastGroup string
			want      string
		}{
			{
				name:  "a measure frame carrying metadata establishes the group",
				frame: &measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure}, MessageId: 1},
				want:  "rbac-alpha",
			},
			{
				name:      "a measure frame carrying no metadata continues in the last group",
				frame:     &measurev1.WriteRequest{MessageId: 2},
				lastGroup: "rbac-alpha",
				want:      "rbac-alpha",
			},
			{
				name:      "a stream frame carrying metadata moves to the group it names",
				frame:     &streamv1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupBeta, Name: fixtureMeasure}, MessageId: 3},
				lastGroup: "rbac-alpha",
				want:      "rbac-beta",
			},
			{
				name:  "a trace frame carrying metadata establishes the group",
				frame: &tracev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure}, Version: 1},
				want:  "rbac-alpha",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, groupErr := liaisongrpc.FrameGroup(tc.frame, tc.lastGroup)
				if groupErr != nil {
					t.Fatalf("FrameGroup(%T, %q) = %v, want %q", tc.frame, tc.lastGroup, groupErr, tc.want)
				}
				if got != tc.want {
					t.Errorf("FrameGroup(%T, %q) = %q, want %q", tc.frame, tc.lastGroup, got, tc.want)
				}
			})
		}
	})

	t.Run("a frame no group can be read from is unresolvable", func(t *testing.T) {
		for _, tc := range []struct {
			frame     any
			name      string
			lastGroup string
		}{
			{name: "a first frame with no metadata", frame: &measurev1.WriteRequest{MessageId: 1}},
			{
				name:  "a frame naming an empty group",
				frame: &streamv1.WriteRequest{Metadata: &commonv1.Metadata{Group: "  "}, MessageId: 1},
			},
			{name: "a frame no write service sends", frame: &measurev1.QueryRequest{Groups: []string{groupAlpha}}, lastGroup: "rbac-alpha"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, groupErr := liaisongrpc.FrameGroup(tc.frame, tc.lastGroup); !errors.Is(groupErr, liaisongrpc.ErrFrameGroupUnresolvable) {
					t.Errorf("FrameGroup(%T, %q) = %v, want an ErrFrameGroupUnresolvable", tc.frame, tc.lastGroup, groupErr)
				}
			})
		}
	})

	writePolicy, _ := table.Policy(mMeasureWrite)

	t.Run("an allowed frame reaches the handler and a forbidden one does not", func(t *testing.T) {
		observer := &recordingObserver{}
		stream := &scriptedStream{
			ctx: context.Background(),
			frames: []proto.Message{
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure}, MessageId: 1},
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupBeta, Name: fixtureMeasure}, MessageId: 2},
			},
		}
		authorized := liaisongrpc.NewFrameAuthorizer(stream, liaisongrpc.FrameAuthorization{
			Policy:    writePolicy,
			Snapshots: &scriptedSnapshots{revisions: []auth.Snapshot{snap}},
			Observer:  observer,
			Principal: actors["writer-alpha"],
		})

		first := &measurev1.WriteRequest{}
		if recvErr := authorized.RecvMsg(first); recvErr != nil {
			t.Fatalf("the alpha writer's alpha frame = %v, want it delivered to the handler", recvErr)
		}
		if first.GetMetadata().GetGroup() != groupAlpha {
			t.Errorf("the delivered frame names group %q, want %q untouched", first.GetMetadata().GetGroup(), groupAlpha)
		}

		second := &measurev1.WriteRequest{}
		recvErr := authorized.RecvMsg(second)
		if got := status.Code(recvErr); got != codes.PermissionDenied {
			t.Errorf("the alpha writer's beta frame = %v, want codes.PermissionDenied", got)
		}
		if second.GetMessageId() != 0 {
			t.Errorf("a denied frame was handed to the handler as message %d, want it withheld", second.GetMessageId())
		}
		if len(observer.decisions) != 2 {
			t.Fatalf("the wrapper reported %d decisions for 2 frames, want exactly one each", len(observer.decisions))
		}
		if observer.decisions[0] != liaisongrpc.DecisionAllow || observer.decisions[1] != liaisongrpc.DecisionDeny {
			t.Errorf("the wrapper reported %v, want [DecisionAllow DecisionDeny]", observer.decisions)
		}
		for _, permission := range observer.permissions {
			if permission != string(auth.PermissionDataWrite) {
				t.Errorf("a frame decision reported permission %q, want %q", permission, auth.PermissionDataWrite)
			}
		}
	})

	t.Run("a continuation frame is decided in the group it continues", func(t *testing.T) {
		stream := &scriptedStream{
			ctx: context.Background(),
			frames: []proto.Message{
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure}, MessageId: 1},
				&measurev1.WriteRequest{MessageId: 2},
			},
		}
		authorized := liaisongrpc.NewFrameAuthorizer(stream, liaisongrpc.FrameAuthorization{
			Policy:    writePolicy,
			Snapshots: &scriptedSnapshots{revisions: []auth.Snapshot{snap}},
			Principal: actors["writer-alpha"],
		})
		if recvErr := authorized.RecvMsg(&measurev1.WriteRequest{}); recvErr != nil {
			t.Fatalf("the alpha writer's first frame = %v, want it delivered", recvErr)
		}
		if recvErr := authorized.RecvMsg(&measurev1.WriteRequest{}); recvErr != nil {
			t.Errorf("a continuation frame in the same group = %v, want it delivered", recvErr)
		}
	})

	// Removing the writer's binding must land on the stream that is already open: the caller
	// neither reconnects nor restarts, and the very next frame is decided by the new revision.
	t.Run("revoking a binding denies the next frame", func(t *testing.T) {
		revoked := dataSnapshot(t, revokedDataPolicyYAML)
		if revoked.Revision() == 0 {
			t.Fatal("the revoked fixture compiled at revision 0, want a later revision than the one in force")
		}
		stream := &scriptedStream{
			ctx: context.Background(),
			frames: []proto.Message{
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure}, MessageId: 1},
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure}, MessageId: 2},
			},
		}
		authorized := liaisongrpc.NewFrameAuthorizer(stream, liaisongrpc.FrameAuthorization{
			Policy:    writePolicy,
			Snapshots: &scriptedSnapshots{revisions: []auth.Snapshot{snap, revoked}},
			Principal: actors["writer-alpha"],
		})
		if recvErr := authorized.RecvMsg(&measurev1.WriteRequest{}); recvErr != nil {
			t.Fatalf("the frame sent before the revocation = %v, want it delivered", recvErr)
		}
		if got := status.Code(authorized.RecvMsg(&measurev1.WriteRequest{})); got != codes.PermissionDenied {
			t.Errorf("the frame sent after the revocation = %v, want codes.PermissionDenied", got)
		}
	})

	t.Run("a transport error is returned unchanged", func(t *testing.T) {
		authorized := liaisongrpc.NewFrameAuthorizer(&scriptedStream{ctx: context.Background()}, liaisongrpc.FrameAuthorization{
			Policy:    writePolicy,
			Snapshots: &scriptedSnapshots{revisions: []auth.Snapshot{snap}},
			Principal: actors["writer-alpha"],
		})
		if recvErr := authorized.RecvMsg(&measurev1.WriteRequest{}); !errors.Is(recvErr, io.EOF) {
			t.Errorf("a closed send side = %v, want io.EOF passed through so the handler completes its batch", recvErr)
		}
	})
}

// TestDataR4_ByDBQLAuthorizesTheTransformedRequest proves R4: a ByDBQL query is authorized
// over the native request it transformed into, not over its text, so its groups are decided
// exactly as the equivalent native method's are and no parameter, casing or comment can
// address a group the decision does not see. The handler takes that decision against the
// snapshot the request was admitted with, which is why the snapshot has to be readable from
// the request context.
func TestDataR4_ByDBQLAuthorizesTheTransformedRequest(t *testing.T) {
	snap := dataSnapshot(t, dataPolicyYAML)
	table := policyTable(t)
	actors := dataActors(t, snap)

	t.Run("the query method carries the post-transform family", func(t *testing.T) {
		policy, classified := table.Policy(mBydbQLQuery)
		if !classified {
			t.Fatalf("GlobalMethodPolicies() does not classify %s", mBydbQLQuery)
		}
		if policy.Scope != liaisongrpc.ScopePostTransform {
			t.Errorf("policy for %s reads scope family %v, want ScopePostTransform", mBydbQLQuery, policy.Scope)
		}
		if policy.Permission != auth.PermissionDataRead {
			t.Errorf("policy for %s requires %q, want %q", mBydbQLQuery, policy.Permission, auth.PermissionDataRead)
		}
		if !policy.Activated {
			t.Errorf("policy for %s is not activated, want issue #14016 to decide it", mBydbQLQuery)
		}
	})

	// The interceptor cannot see the groups a query addresses, so it admits any principal
	// holding data:read somewhere and denies one holding it nowhere. The exact decision is the
	// handler's.
	t.Run("the interceptor admits a data reader and denies everyone else", func(t *testing.T) {
		request := &bydbqlv1.QueryRequest{Query: "SELECT * FROM MEASURE service_cpm IN rbac-alpha"}
		for who, want := range map[string]liaisongrpc.Decision{
			"admin":        liaisongrpc.DecisionAllow,
			"reader-alpha": liaisongrpc.DecisionAllow,
			"writer-alpha": liaisongrpc.DecisionAllow,
			"monitor":      liaisongrpc.DecisionDeny,
			"unbound":      liaisongrpc.DecisionDeny,
		} {
			if got, reason := table.Authorize(snap, actors[who], mBydbQLQuery, request); got != want {
				t.Errorf("Authorize(%s, %s) = %v (%s), want %v", who, mBydbQLQuery, got, reason, want)
			}
		}
	})

	// Every native result family ByDBQL can produce is decided here, and each is decided the
	// way its own service's Query would be: all groups or none.
	t.Run("the transformed request decides the query", func(t *testing.T) {
		for _, tc := range []struct {
			native any
			name   string
			who    string
			want   liaisongrpc.Decision
		}{
			{name: "stream in alpha", who: "reader-alpha", native: &streamv1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow},
			{name: "measure in alpha", who: "reader-alpha", native: &measurev1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow},
			{name: "topn in alpha", who: "reader-alpha", native: &measurev1.TopNRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow},
			{name: "trace in alpha", who: "reader-alpha", native: &tracev1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow},
			{
				name: "property in alpha", who: "reader-alpha",
				native: &propertyv1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionAllow,
			},
			{name: "measure in beta", who: "reader-alpha", native: &measurev1.QueryRequest{Groups: []string{groupBeta}}, want: liaisongrpc.DecisionDeny},
			{
				name: "measure across alpha and beta", who: "reader-alpha",
				native: &measurev1.QueryRequest{Groups: []string{groupAlpha, groupBeta}}, want: liaisongrpc.DecisionDeny,
			},
			{
				name: "a wildcard reader across alpha and beta", who: "reader-all",
				native: &measurev1.QueryRequest{Groups: []string{groupAlpha, groupBeta}}, want: liaisongrpc.DecisionAllow,
			},
			{name: "a cluster monitor", who: "monitor", native: &measurev1.QueryRequest{Groups: []string{groupAlpha}}, want: liaisongrpc.DecisionDeny},
			{name: "a request naming no group", who: "reader-alpha", native: &measurev1.QueryRequest{}, want: liaisongrpc.DecisionInvalidRequest},
			{name: "a request of no native type", who: "admin", native: &bydbqlv1.QueryRequest{Query: "SELECT 1"}, want: liaisongrpc.DecisionInvalidRequest},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, reason := liaisongrpc.AuthorizeTransformedRequest(snap, actors[tc.who], tc.native)
				if got != tc.want {
					t.Errorf("AuthorizeTransformedRequest(%s, %T) = %v (%s), want %v", tc.who, tc.native, got, reason, tc.want)
				}
			})
		}
	})

	// The handler must decide against the snapshot the request was admitted with. Asking the
	// reloader again would let a reload that lands mid-handler mix one revision's admission
	// with another revision's grants.
	t.Run("the request's snapshot is readable inside the handler", func(t *testing.T) {
		reloader := newDataReloader(t, dataPolicyYAML)
		interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, table, &recordingObserver{})
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("username", "dat-reader-alpha", "password", "reader-alpha-secret"))
		var seen auth.Snapshot
		var established bool
		_, callErr := interceptor(ctx, &bydbqlv1.QueryRequest{Query: "SELECT * FROM MEASURE service_cpm IN rbac-alpha"},
			&grpclib.UnaryServerInfo{FullMethod: mBydbQLQuery},
			func(handlerCtx context.Context, _ any) (any, error) {
				seen, established = liaisongrpc.SnapshotFromContext(handlerCtx)
				return struct{}{}, nil
			})
		if callErr != nil {
			t.Fatalf("the alpha reader's ByDBQL query = %v, want the handler's result", callErr)
		}
		if !established {
			t.Fatal("SnapshotFromContext reported no snapshot inside an authorized handler, want the one the request was decided from")
		}
		if seen.Revision() != reloader.CurrentSnapshot().Revision() {
			t.Errorf("the handler saw revision %d, want the request's revision %d", seen.Revision(), reloader.CurrentSnapshot().Revision())
		}
	})

	t.Run("a caller cannot seed a snapshot of its own", func(t *testing.T) {
		if _, established := liaisongrpc.SnapshotFromContext(context.Background()); established {
			t.Error("SnapshotFromContext read a snapshot out of a context no interceptor touched")
		}
	})
}

// TestDataR5_DataPathsAreUnchangedWithoutRBAC proves R5: a deployment with no auth file, or
// with a users-only one, sees every data path exactly as it did before. This is the
// compatibility half of the milestone — the per-frame wrapper and the ByDBQL gate are new code
// on the hot path of every existing deployment, and neither may change what one observes.
func TestDataR5_DataPathsAreUnchangedWithoutRBAC(t *testing.T) {
	table := policyTable(t)
	reloader := newDataReloader(t, usersOnlyDataPolicyYAML)
	interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, table, nil)

	// Groups this users-only deployment never granted anybody access to. Under RBAC they would
	// be denied; here they must reach their handlers untouched.
	t.Run("every data method reaches its handler", func(t *testing.T) {
		for _, tc := range []struct {
			request any
			name    string
			method  string
		}{
			{name: "measure query", method: mMeasureQuery, request: &measurev1.QueryRequest{Groups: []string{"sw_metric"}}},
			{name: "stream query", method: mStreamQuery, request: &streamv1.QueryRequest{Groups: []string{"sw_record"}}},
			{name: "trace query", method: mTraceQuery, request: &tracev1.QueryRequest{Groups: []string{"sw_trace"}}},
			{name: "measure topn", method: mMeasureTopN, request: &measurev1.TopNRequest{Groups: []string{"sw_metric"}}},
			{name: "property query", method: mPropertyQuery, request: &propertyv1.QueryRequest{Groups: []string{"sw_property"}}},
			{name: "property apply", method: mPropertyApply, request: applyIn("sw_property")},
			{name: "property delete", method: mPropertyDelete, request: &propertyv1.DeleteRequest{Group: "sw_property", Name: "endpoint", Id: "1"}},
			{name: "bydbql query", method: mBydbQLQuery, request: &bydbqlv1.QueryRequest{Query: "SELECT * FROM MEASURE service_cpm IN sw_metric"}},
			// A request the scope family could not be read from must stay the request
			// validator's business, not become an authorization answer.
			{name: "a malformed measure query", method: mMeasureQuery, request: &measurev1.QueryRequest{}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ran, callErr := callDataMethod(t, interceptor, tc.method, "dat-legacy", "legacy-secret", tc.request)
				if callErr != nil {
					t.Fatalf("%s with a users-only policy = %v, want the handler's result", tc.method, callErr)
				}
				if !ran {
					t.Fatalf("%s with a users-only policy did not reach its handler", tc.method)
				}
			})
		}
	})

	t.Run("a write stream delivers every frame", func(t *testing.T) {
		streamInterceptor := liaisongrpc.NewAuthorizationStreamInterceptor(reloader, table, nil)
		streamCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "dat-legacy", "password", "legacy-secret"))
		frames := &scriptedStream{
			ctx: streamCtx,
			frames: []proto.Message{
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: "sw_metric", Name: fixtureMeasure}, MessageId: 1},
				&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: "sw_record", Name: fixtureMeasure}, MessageId: 2},
			},
		}
		handlerRan := false
		handlerErr := streamInterceptor(nil, frames, &grpclib.StreamServerInfo{FullMethod: mMeasureWrite},
			func(_ any, handed grpclib.ServerStream) error {
				handlerRan = true
				for received := 0; received < 2; received++ {
					if recvErr := handed.RecvMsg(&measurev1.WriteRequest{}); recvErr != nil {
						return recvErr
					}
				}
				return nil
			})
		if handlerErr != nil {
			t.Fatalf("a users-only write stream = %v, want every frame delivered", handlerErr)
		}
		if !handlerRan {
			t.Fatal("a users-only write stream did not reach its handler")
		}
	})

	t.Run("a deployment with no auth file authorizes nothing", func(t *testing.T) {
		bare := liaisongrpc.NewAuthorizationInterceptor(nil, table, nil)
		ran, callErr := callDataMethod(t, bare, mMeasureQuery, "", "", &measurev1.QueryRequest{Groups: []string{"sw_metric"}})
		if callErr != nil {
			t.Fatalf("a deployment with no auth file returned %v on a data method, want the handler's result", callErr)
		}
		if !ran {
			t.Fatal("a deployment with no auth file did not reach its data handler")
		}
	})

	t.Run("the transformed-request gate is inert without RBAC", func(t *testing.T) {
		usersOnly := dataSnapshot(t, usersOnlyDataPolicyYAML)
		got, _ := liaisongrpc.AuthorizeTransformedRequest(usersOnly, auth.Principal{}, &measurev1.QueryRequest{Groups: []string{"sw_metric"}})
		if got != liaisongrpc.DecisionAllow {
			t.Errorf("AuthorizeTransformedRequest with RBAC off = %v, want DecisionAllow", got)
		}
	})
}

// TestDataR6_EveryLiaisonMethodIsActivatedAndBounded proves R6, the closing criterion of
// #13994: at this merge every method the liaison serves is either explicitly authorized or
// retains its documented authenticated/health behavior, no method is left fail-closed, every
// permission-bearing method names the scope family its decision reads, decision labels stay
// inside the bounded set, and the operator documentation no longer describes a coverage gap
// that has closed.
//
// It supersedes TestSchemaR6_DataMethodsStayFailClosed, which asserted the opposite for as
// long as W-PR2 was the head of this series.
func TestDataR6_EveryLiaisonMethodIsActivatedAndBounded(t *testing.T) {
	table := policyTable(t)

	t.Run("no method is left fail-closed", func(t *testing.T) {
		for _, policy := range table {
			if !policy.Activated {
				t.Errorf("policy for %s is not activated; #13994 closes only when every method is decidable", policy.FullMethod)
			}
			if policy.Access == liaisongrpc.MethodAccessPermission && policy.Scope == liaisongrpc.ScopeUnspecified {
				t.Errorf("policy for %s names no scope family, so its decision has no request shape to read", policy.FullMethod)
			}
		}
	})

	t.Run("the eleven data methods match the issue's oracle", func(t *testing.T) {
		classified := 0
		for _, policy := range table {
			switch policy.Permission {
			case auth.PermissionDataRead, auth.PermissionDataWrite:
				classified++
			default:
			}
		}
		if classified != len(dataMethodOracle) {
			t.Errorf("the table classifies %d data methods, want the fixed %d of issue #14016", classified, len(dataMethodOracle))
		}
		for _, want := range dataMethodOracle {
			policy, exists := table.Policy(want.method)
			if !exists {
				t.Errorf("the table does not classify the data method %s", want.method)
				continue
			}
			if policy.Permission != want.permission {
				t.Errorf("policy for %s requires %q, want %q", want.method, policy.Permission, want.permission)
			}
			if policy.Scope != want.scope {
				t.Errorf("policy for %s reads scope family %v, want %v", want.method, policy.Scope, want.scope)
			}
		}
	})

	t.Run("the deferred families are the three the design names", func(t *testing.T) {
		want := []liaisongrpc.ScopeFamily{
			liaisongrpc.ScopeVisibleGroups, liaisongrpc.ScopeFrameGroups, liaisongrpc.ScopePostTransform,
		}
		if got := liaisongrpc.DeferredScopeFamilies(); !reflect.DeepEqual(got, want) {
			t.Errorf("DeferredScopeFamilies() = %v, want %v", got, want)
		}
	})

	t.Run("data decisions stay inside the bounded label set", func(t *testing.T) {
		bounded := make(map[liaisongrpc.DecisionReason]bool)
		for _, reason := range liaisongrpc.DecisionReasons() {
			bounded[reason] = true
		}
		snap := dataSnapshot(t, dataPolicyYAML)
		actors := dataActors(t, snap)
		for _, tc := range []struct {
			request any
			method  string
		}{
			{method: mMeasureQuery, request: &measurev1.QueryRequest{Groups: []string{groupAlpha}}},
			{method: mMeasureQuery, request: &measurev1.QueryRequest{Groups: []string{groupBeta}}},
			{method: mMeasureQuery, request: &measurev1.QueryRequest{}},
			{method: mPropertyApply, request: applyIn(groupAlpha)},
			{method: mPropertyDelete, request: &propertyv1.DeleteRequest{Group: groupAlpha, Name: "endpoint", Id: "1"}},
			{method: mMeasureWrite, request: nil},
			{method: mBydbQLQuery, request: &bydbqlv1.QueryRequest{Query: "SELECT 1"}},
		} {
			for _, who := range []string{"admin", "reader-alpha", "monitor", "unbound"} {
				if _, reason := table.Authorize(snap, actors[who], tc.method, tc.request); !bounded[reason] {
					t.Errorf("Authorize(%s, %s) reported %q, which is outside DecisionReasons()", who, tc.method, reason)
				}
			}
		}
	})

	// Acceptance criterion "operator/security docs match the shipped YAML", and gate 8 of the
	// design, "the Canopy service-account boundary remains documented". Comparison is over
	// whitespace-normalized text so a rewrap cannot pass or fail it.
	t.Run("the operator documentation describes the shipped coverage", func(t *testing.T) {
		raw, readErr := os.ReadFile(filepath.Join("..", "..", "..", "docs", "operation", "security.md"))
		if readErr != nil {
			t.Fatalf("reading the security documentation: %v", readErr)
		}
		normalized := strings.Join(strings.Fields(string(raw)), " ")
		if strings.Contains(normalized, "are not active in this release") {
			t.Error("docs/operation/security.md still describes schema and data executors as inactive, which this milestone activates")
		}
		if !strings.Contains(normalized, "Canopy") {
			t.Error("docs/operation/security.md does not document the Canopy service-account trust boundary, which is gate 8 of #13994")
		}
	})
}
