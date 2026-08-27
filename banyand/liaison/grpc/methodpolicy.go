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

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// Errors reported by ValidateMethodPolicies. Each wraps the offending gRPC full method
// name, and each stops liaison startup before any watcher, goroutine or listener starts.
var (
	// ErrMethodPolicyIncomplete reports a gRPC method the liaison serves that the table
	// does not classify. An unclassified method has no permission to fail closed on.
	ErrMethodPolicyIncomplete = errors.New("method policy: registered method is unclassified")
	// ErrMethodPolicyStale reports a classified method the liaison does not serve. A stale
	// entry means the table drifted from the service registration it is meant to cover.
	ErrMethodPolicyStale = errors.New("method policy: classified method is not registered")
	// ErrMethodPolicyDuplicate reports a method classified more than once, which would make
	// the permission that applies to it depend on iteration order.
	ErrMethodPolicyDuplicate = errors.New("method policy: method classified more than once")
)

// Decision is the outcome of one authorization decision at the liaison boundary.
type Decision int

// The closed set of authorization outcomes. DecisionUnspecified is the zero value and is
// never a valid answer, so a decision that was never taken cannot be mistaken for one.
const (
	DecisionUnspecified Decision = iota
	// DecisionAllow lets the request reach its handler.
	DecisionAllow
	// DecisionDeny rejects the request because the principal does not hold the method's
	// permission.
	DecisionDeny
	// DecisionUnavailable rejects the request because this release activates no executor
	// for the method's permission. It is the fail-closed outcome and, like DecisionDeny,
	// is reported to the caller as codes.PermissionDenied.
	DecisionUnavailable
)

// DecisionLabel returns the bounded observability label for the decision: "allow", "deny",
// "unavailable", or "unspecified". The set is closed and contains no caller-controlled
// text, so it cannot grow the cardinality of the metrics it labels.
func DecisionLabel(_ Decision) string {
	return ""
}

// DecisionLabels returns every value DecisionLabel produces, in a fixed order.
func DecisionLabels() []string {
	return nil
}

// MethodPolicy classifies one gRPC method the liaison serves.
type MethodPolicy struct {
	// FullMethod is the gRPC full method name, for example
	// "/banyandb.database.v1.ClusterStateService/GetClusterState".
	FullMethod string
	// Permission is the capability a principal must hold to invoke the method.
	Permission auth.Permission
	// Activated reports whether this release has an executor that can decide Permission.
	// A registered method whose permission has no activated executor fails closed for
	// every principal, admin included.
	Activated bool
}

// MethodPolicyTable is the complete classification of the gRPC methods a liaison serves.
type MethodPolicyTable []MethodPolicy

// ServiceActivation reports which conditionally registered gRPC services a liaison serves.
// The liaison registers these two only when the corresponding component is constructed, so
// the set of methods it actually serves is a runtime property, not a compile-time one.
type ServiceActivation struct {
	// SchemaBarrier is true when banyandb.schema.v1.SchemaBarrierService is registered.
	SchemaBarrier bool
	// NodeSchemaStatus is true when banyandb.cluster.v1.NodeSchemaStatusService is
	// registered.
	NodeSchemaStatus bool
}

// RegisteredMethods returns every gRPC full method name the liaison serves under the given
// activation, sorted lexicographically. It is derived from the registered services'
// descriptors, so it tracks a service gaining or losing a method without the method policy
// table being edited.
func RegisteredMethods(_ ServiceActivation) []string {
	return nil
}

// GlobalMethodPolicies returns the method policy table for this release: every method the
// liaison can serve, the permission it requires, and whether this release can decide that
// permission. Cluster permissions are activated; schema and data permissions are not, and
// the methods carrying them fail closed until a later release activates their executor.
func GlobalMethodPolicies() MethodPolicyTable {
	return nil
}

// Policy returns the policy classifying the given gRPC full method name.
func (MethodPolicyTable) Policy(_ string) (MethodPolicy, bool) {
	return MethodPolicy{}, false
}

// Authorize decides the given gRPC method for the given principal against the grants in
// the given snapshot. It returns DecisionUnavailable when the method's permission has no
// activated executor, DecisionDeny when the principal does not hold the permission or when
// the method is unclassified, and DecisionAllow only when an activated permission is held.
func (MethodPolicyTable) Authorize(_ auth.Snapshot, _ auth.Principal, _ string) Decision {
	return DecisionUnspecified
}

// ValidateMethodPolicies reports whether the given table classifies exactly the given
// registered methods: every registered method carries one policy, no policy names a method
// that is not registered, and no method is classified twice. The liaison calls it from
// Validate(), so a defect stops startup before any watcher, goroutine or listener starts.
func ValidateMethodPolicies(_ MethodPolicyTable, _ []string) error {
	return errors.New("method policy validation is not built yet")
}

// DecisionObserver records authorization outcomes for observability. The interceptor
// reports one decision per unary request, identified only by a method name drawn from the
// registered set and a Decision drawn from the closed outcome set, so no caller-controlled
// string can reach a metric label.
type DecisionObserver interface {
	// ObserveDecision records one authorization decision for fullMethod.
	ObserveDecision(fullMethod string, decision Decision)
}

// PrincipalFromContext returns the trusted principal the authorization interceptor
// established for the given context, and reports whether one was established. The principal is stored
// under a key this package does not export and is minted only by
// auth.Snapshot.Authenticate, so an identity a caller supplied in gRPC metadata or an HTTP
// header can never be read back through this function.
func PrincipalFromContext(_ context.Context) (auth.Principal, bool) {
	return auth.Principal{}, false
}

// NewAuthorizationInterceptor returns the unary interceptor that owns the authoritative
// global authorization decision. For every call it authenticates the caller against the
// snapshot in force, attaches the resulting trusted principal to the request context,
// decides the method against the given table, and only then invokes the handler. It
// reports the decision to the given observer exactly once per call.
//
// Precedence is fixed: a caller with no or bad credentials is rejected with
// codes.Unauthenticated before the request is validated; a caller whose decision is
// DecisionDeny or DecisionUnavailable is rejected with codes.PermissionDenied before the
// handler runs, so a denied call has no side effect. A method whose handler is the
// generated Unimplemented fallback is authorized first, so an allowed principal receives
// codes.Unimplemented from the handler and a denied one receives codes.PermissionDenied.
//
// When the snapshot has RBAC disabled the interceptor performs authentication only, which
// leaves no-auth and users-only deployments behaving exactly as they did before.
func NewAuthorizationInterceptor(_ *auth.Reloader, _ MethodPolicyTable, _ DecisionObserver) grpc.UnaryServerInterceptor {
	return func(_ context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		return nil, errors.New("the authorization interceptor is not built yet")
	}
}
