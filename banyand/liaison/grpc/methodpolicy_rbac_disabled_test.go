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
