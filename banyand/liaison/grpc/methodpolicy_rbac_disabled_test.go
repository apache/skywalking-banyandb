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
	"os"
	"path/filepath"
	"testing"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const usersOnlyInterceptorPolicyYAML = `
users:
  - username: "alice"
    password: "secret"
`

// TestR2_RBACDisabledUnaryInterceptorAuthenticatesWithoutAuthorizing proves that a
// users-only deployment continues to authenticate at the unary boundary without applying
// the unavailable method policies introduced for RBAC-enabled deployments.
func TestR2_RBACDisabledUnaryInterceptorAuthenticatesWithoutAuthorizing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "security.yaml")
	if writeErr := os.WriteFile(path, []byte(usersOnlyInterceptorPolicyYAML), 0o600); writeErr != nil {
		t.Fatalf("writing %s: %v", path, writeErr)
	}

	reloader := auth.InitAuthReloader()
	if configErr := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-disabled-interceptor-test")); configErr != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want a users-only policy to load", path, configErr)
	}
	if reloader.CurrentSnapshot().RBACEnabled() {
		t.Fatal("CurrentSnapshot().RBACEnabled() = true, want false for a users-only policy")
	}

	interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, liaisongrpc.GlobalMethodPolicies(), nil)
	info := &grpclib.UnaryServerInfo{FullMethod: "/banyandb.measure.v1.MeasureService/Query"}

	t.Run("valid credentials reach an unactivated method", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "alice", "password", "secret"))
		handlerCalled := false
		_, callErr := interceptor(ctx, struct{}{}, info, func(context.Context, any) (any, error) {
			handlerCalled = true
			return nil, nil
		})
		if callErr != nil {
			t.Fatalf("interceptor(users-only credentials, unactivated method) = %v, want nil", callErr)
		}
		if !handlerCalled {
			t.Fatal("interceptor(users-only credentials, unactivated method) did not invoke the handler")
		}
	})

	t.Run("missing credentials are unauthenticated", func(t *testing.T) {
		handlerCalled := false
		_, callErr := interceptor(context.Background(), struct{}{}, info, func(context.Context, any) (any, error) {
			handlerCalled = true
			return nil, nil
		})
		if status.Code(callErr) != codes.Unauthenticated {
			t.Fatalf("interceptor(missing credentials) status = %s, want %s", status.Code(callErr), codes.Unauthenticated)
		}
		if handlerCalled {
			t.Fatal("interceptor(missing credentials) invoked the handler, want authentication to reject first")
		}
	})
}

// TestSchemaR5_SchemaMethodsAreUnchangedWithoutRBAC proves R5, the compatibility requirement this
// milestone carries whether or not the issue states one: a deployment that ships no `rbac`
// block sees the schema API exactly as it did before. Group scope is never consulted, a
// malformed group is left to the request validator rather than answered InvalidArgument by
// authorization, and Group.List returns every group the handler produced — an existing
// bydbctl or OAP client must not silently start receiving a shorter list.
func TestSchemaR5_SchemaMethodsAreUnchangedWithoutRBAC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "security.yaml")
	if writeErr := os.WriteFile(path, []byte(usersOnlyInterceptorPolicyYAML), 0o600); writeErr != nil {
		t.Fatalf("writing %s: %v", path, writeErr)
	}
	reloader := auth.InitAuthReloader()
	if configErr := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-disabled-schema-test")); configErr != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want a users-only policy to load", path, configErr)
	}
	table := liaisongrpc.GlobalMethodPolicies()
	interceptor := liaisongrpc.NewAuthorizationInterceptor(reloader, table, nil)

	unfiltered := &databasev1.GroupRegistryServiceListResponse{
		Group: []*commonv1.Group{
			{Metadata: &commonv1.Metadata{Name: "sw_metric"}},
			{Metadata: &commonv1.Metadata{Name: "sw_record"}},
		},
	}
	for _, tc := range []struct {
		request any
		reply   any
		method  string
		name    string
	}{
		{
			name:   "a scoped read reaches its handler",
			method: "/banyandb.database.v1.GroupRegistryService/Get",
			// A group this users-only deployment never granted anybody access to.
			request: &databasev1.GroupRegistryServiceGetRequest{Group: "sw_metric"},
			reply:   &databasev1.GroupRegistryServiceGetResponse{},
		},
		{
			name:    "a scoped write reaches its handler",
			method:  "/banyandb.database.v1.MeasureRegistryService/Create",
			request: &databasev1.MeasureRegistryServiceCreateRequest{},
			reply:   &databasev1.MeasureRegistryServiceCreateResponse{},
		},
		{
			name:    "a malformed group is not turned into an authorization answer",
			method:  "/banyandb.database.v1.GroupRegistryService/Get",
			request: &databasev1.GroupRegistryServiceGetRequest{},
			reply:   &databasev1.GroupRegistryServiceGetResponse{},
		},
		{
			name:    "a barrier wait reaches its handler",
			method:  "/banyandb.schema.v1.SchemaBarrierService/AwaitRevisionApplied",
			request: struct{}{},
			reply:   struct{}{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "alice", "password", "secret"))
			handlerCalled := false
			got, callErr := interceptor(ctx, tc.request, &grpclib.UnaryServerInfo{FullMethod: tc.method},
				func(context.Context, any) (any, error) {
					handlerCalled = true
					return tc.reply, nil
				})
			if callErr != nil {
				t.Fatalf("%s with a users-only policy = %v, want the handler's result", tc.method, callErr)
			}
			if !handlerCalled {
				t.Fatalf("%s with a users-only policy did not reach its handler", tc.method)
			}
			if got != tc.reply {
				t.Errorf("%s returned a different value than the handler produced, want the reply passed through", tc.method)
			}
		})
	}

	t.Run("Group.List is not filtered", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "alice", "password", "secret"))
		got, callErr := interceptor(ctx, &databasev1.GroupRegistryServiceListRequest{},
			&grpclib.UnaryServerInfo{FullMethod: "/banyandb.database.v1.GroupRegistryService/List"},
			func(context.Context, any) (any, error) { return unfiltered, nil })
		if callErr != nil {
			t.Fatalf("Group.List with a users-only policy = %v, want the handler's result", callErr)
		}
		listed, ok := got.(*databasev1.GroupRegistryServiceListResponse)
		if !ok {
			t.Fatalf("Group.List returned %T, want the handler's Group List response", got)
		}
		if len(listed.GetGroup()) != 2 {
			t.Errorf("Group.List returned %d groups with RBAC off, want the handler's 2 untouched", len(listed.GetGroup()))
		}
	})

	t.Run("no auth file at all authorizes nothing", func(t *testing.T) {
		bare := liaisongrpc.NewAuthorizationInterceptor(nil, table, nil)
		handlerCalled := false
		if _, callErr := bare(context.Background(), &databasev1.GroupRegistryServiceGetRequest{},
			&grpclib.UnaryServerInfo{FullMethod: "/banyandb.database.v1.GroupRegistryService/Get"},
			func(context.Context, any) (any, error) {
				handlerCalled = true
				return nil, nil
			}); callErr != nil {
			t.Fatalf("a deployment with no auth file returned %v on a schema method, want the handler's result", callErr)
		}
		if !handlerCalled {
			t.Fatal("a deployment with no auth file did not reach its schema handler")
		}
	})
}
