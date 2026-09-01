// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package grpc

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const bydbQLObservabilityPolicy = `
users:
  - username: "reader"
    password: "reader-secret"
rbac:
  enabled: true
  bindings:
    - principal: "reader"
      role: "reader"
      groups: ["*"]
`

func TestBydbQLQuery_ParseErrorRecordsAdmissionDecision(t *testing.T) {
	t.Helper()
	snapshot, snapshotErr := auth.CompileSnapshot(1, []byte(bydbQLObservabilityPolicy))
	require.NoError(t, snapshotErr)

	factory, metricSet := newRecordingMetrics(t)
	service := &bydbQLService{metrics: metricSet, cache: newPreparedCache(16, 1<<20, metricSet)}
	ctx := ContextWithSnapshot(context.Background(), snapshot)
	_, queryErr := service.Query(ctx, &bydbqlv1.QueryRequest{Query: "NOT A QUERY"})
	require.Equal(t, codes.InvalidArgument, status.Code(queryErr))

	decisionCalls := factory.counter("rbac_decisions_total").snapshot()
	require.Len(t, decisionCalls, 1, "an admitted ByDBQL call must record one decision even when parsing fails")
	assert.Equal(t, []string{"allow", "data:read", "banyandb.bydbql.v1.BydbQLService/Query", "granted"}, decisionCalls[0].labels)
}

func TestBydbQLQuery_ValidatorErrorRecordsAdmissionDecision(t *testing.T) {
	reloader := newBydbQLObservabilityReloader(t)
	factory, metricSet := newRecordingMetrics(t)
	interceptor := NewAuthorizationInterceptor(reloader, GlobalMethodPolicies(), metricSet)
	info := &grpclib.UnaryServerInfo{FullMethod: bydbQLQueryFullMethod}
	validator := grpc_validator.UnaryServerInterceptor()
	handlerCalled := false
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "reader", "password", "reader-secret"))

	_, queryErr := interceptor(ctx, &bydbqlv1.QueryRequest{}, info, func(validationCtx context.Context, request any) (any, error) {
		return validator(validationCtx, request, info, func(context.Context, any) (any, error) {
			handlerCalled = true
			return struct{}{}, nil
		})
	})

	require.Equal(t, codes.InvalidArgument, status.Code(queryErr))
	assert.False(t, handlerCalled, "the request validator must reject an empty query before the handler runs")
	decisionCalls := factory.counter("rbac_decisions_total").snapshot()
	require.Len(t, decisionCalls, 1, "a validator-rejected ByDBQL call must record its admission decision exactly once")
	assert.Equal(t, []string{"allow", "data:read", "banyandb.bydbql.v1.BydbQLService/Query", "granted"}, decisionCalls[0].labels)
}

func TestBydbQLQuery_PostTransformDenyRecordsGateDecision(t *testing.T) {
	reloader := newBydbQLObservabilityReloader(t)
	factory, metricSet := newRecordingMetrics(t)
	interceptor := NewAuthorizationInterceptor(reloader, GlobalMethodPolicies(), metricSet)
	info := &grpclib.UnaryServerInfo{FullMethod: bydbQLQueryFullMethod}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "reader", "password", "reader-secret"))

	_, queryErr := interceptor(ctx, &bydbqlv1.QueryRequest{Query: "select * from measure"}, info, func(handlerCtx context.Context, _ any) (any, error) {
		decisionSlot, exists := postTransformDecisionFromContext(handlerCtx)
		require.True(t, exists, "the post-transform decision slot must reach the ByDBQL handler")
		decisionSlot.decision = DecisionDeny
		decisionSlot.reason = DecisionReasonPermissionMissing
		return nil, decisionError(DecisionDeny)
	})

	require.Equal(t, codes.PermissionDenied, status.Code(queryErr))
	decisionCalls := factory.counter("rbac_decisions_total").snapshot()
	require.Len(t, decisionCalls, 1, "a post-transform denied ByDBQL call must record one decision")
	assert.Equal(t, []string{"deny", "data:read", "banyandb.bydbql.v1.BydbQLService/Query", "permission_missing"}, decisionCalls[0].labels)
}

func newBydbQLObservabilityReloader(t *testing.T) *auth.Reloader {
	t.Helper()
	policyPath := filepath.Join(t.TempDir(), "security.yaml")
	require.NoError(t, os.WriteFile(policyPath, []byte(bydbQLObservabilityPolicy), 0o600))
	reloader := auth.InitAuthReloader()
	require.NoError(t, reloader.ConfigAuthReloader(policyPath, false, logger.GetLogger("rbac-bydbql-observability-test")))
	return reloader
}
