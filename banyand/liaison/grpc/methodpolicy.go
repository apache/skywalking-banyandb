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
	"errors"
	"fmt"
	"sort"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
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
func DecisionLabel(decision Decision) string {
	switch decision {
	case DecisionAllow:
		return "allow"
	case DecisionDeny:
		return "deny"
	case DecisionUnavailable:
		return "unavailable"
	default:
		return "unspecified"
	}
}

// DecisionLabels returns every value DecisionLabel produces, in a fixed order.
func DecisionLabels() []string {
	return []string{"allow", "deny", "unavailable", "unspecified"}
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
func RegisteredMethods(activation ServiceActivation) []string {
	methods := make(map[string]struct{})
	for _, descriptor := range registeredServiceDescriptors(activation) {
		for _, method := range descriptor.Methods {
			methods[fullMethod(descriptor, method.MethodName)] = struct{}{}
		}
		for _, stream := range descriptor.Streams {
			methods[fullMethod(descriptor, stream.StreamName)] = struct{}{}
		}
	}
	registered := make([]string, 0, len(methods))
	for method := range methods {
		registered = append(registered, method)
	}
	sort.Strings(registered)
	return registered
}

func registeredServiceDescriptors(activation ServiceActivation) []grpclib.ServiceDesc {
	descriptors := []grpclib.ServiceDesc{
		commonv1.Service_ServiceDesc,
		streamv1.StreamService_ServiceDesc,
		measurev1.MeasureService_ServiceDesc,
		tracev1.TraceService_ServiceDesc,
		bydbqlv1.BydbQLService_ServiceDesc,
		databasev1.GroupRegistryService_ServiceDesc,
		databasev1.IndexRuleBindingRegistryService_ServiceDesc,
		databasev1.IndexRuleRegistryService_ServiceDesc,
		databasev1.StreamRegistryService_ServiceDesc,
		databasev1.MeasureRegistryService_ServiceDesc,
		propertyv1.PropertyService_ServiceDesc,
		databasev1.TopNAggregationRegistryService_ServiceDesc,
		databasev1.SnapshotService_ServiceDesc,
		databasev1.PropertyRegistryService_ServiceDesc,
		databasev1.TraceRegistryService_ServiceDesc,
		databasev1.ClusterStateService_ServiceDesc,
		databasev1.NodeQueryService_ServiceDesc,
		grpc_health_v1.Health_ServiceDesc,
	}
	if activation.SchemaBarrier {
		descriptors = append(descriptors, schemav1.SchemaBarrierService_ServiceDesc)
	}
	if activation.NodeSchemaStatus {
		descriptors = append(descriptors, clusterv1.NodeSchemaStatusService_ServiceDesc)
	}
	return descriptors
}

func fullMethod(descriptor grpclib.ServiceDesc, method string) string {
	return "/" + descriptor.ServiceName + "/" + method
}

// GlobalMethodPolicies returns the method policy table for this release: every method the
// liaison can serve, the permission it requires, and whether this release can decide that
// permission. Cluster permissions are activated; schema and data permissions are not, and
// the methods carrying them fail closed until a later release activates their executor.
func GlobalMethodPolicies() MethodPolicyTable {
	globalPermissions := map[string]auth.Permission{
		fullMethod(commonv1.Service_ServiceDesc, "GetAPIVersion"):                     auth.PermissionClusterRead,
		fullMethod(databasev1.ClusterStateService_ServiceDesc, "GetClusterState"):     auth.PermissionClusterRead,
		fullMethod(databasev1.NodeQueryService_ServiceDesc, "GetCurrentNode"):         auth.PermissionClusterRead,
		fullMethod(grpc_health_v1.Health_ServiceDesc, "Check"):                        auth.PermissionClusterRead,
		fullMethod(databasev1.SnapshotService_ServiceDesc, "Snapshot"):                auth.PermissionClusterAdmin,
		fullMethod(clusterv1.NodeSchemaStatusService_ServiceDesc, "GetMaxRevision"):   auth.PermissionClusterAdmin,
		fullMethod(clusterv1.NodeSchemaStatusService_ServiceDesc, "GetKeyRevisions"):  auth.PermissionClusterAdmin,
		fullMethod(clusterv1.NodeSchemaStatusService_ServiceDesc, "GetAbsentKeys"):    auth.PermissionClusterAdmin,
		fullMethod(schemav1.SchemaBarrierService_ServiceDesc, "AwaitRevisionApplied"): auth.PermissionClusterAdmin,
		fullMethod(streamv1.StreamService_ServiceDesc, "DeleteExpiredSegments"):       auth.PermissionClusterAdmin,
		fullMethod(measurev1.MeasureService_ServiceDesc, "DeleteExpiredSegments"):     auth.PermissionClusterAdmin,
		fullMethod(tracev1.TraceService_ServiceDesc, "DeleteExpiredSegments"):         auth.PermissionClusterAdmin,
	}
	registered := RegisteredMethods(ServiceActivation{SchemaBarrier: true, NodeSchemaStatus: true})
	policies := make(MethodPolicyTable, 0, len(registered))
	for _, method := range registered {
		permission, activated := globalPermissions[method]
		if !activated {
			permission = auth.PermissionDataRead
		}
		policies = append(policies, MethodPolicy{FullMethod: method, Permission: permission, Activated: activated})
	}
	return policies
}

// Policy returns the policy classifying the given gRPC full method name.
func (table MethodPolicyTable) Policy(fullMethod string) (MethodPolicy, bool) {
	for _, policy := range table {
		if policy.FullMethod == fullMethod {
			return policy, true
		}
	}
	return MethodPolicy{}, false
}

// Authorize decides the given gRPC method for the given principal against the grants in
// the given snapshot. It returns DecisionUnavailable when the method's permission has no
// activated executor, DecisionDeny when the principal does not hold the permission or when
// the method is unclassified, and DecisionAllow only when an activated permission is held.
func (table MethodPolicyTable) Authorize(snapshot auth.Snapshot, principal auth.Principal, fullMethod string) Decision {
	policy, exists := table.Policy(fullMethod)
	if !exists {
		return DecisionDeny
	}
	if !policy.Activated {
		return DecisionUnavailable
	}
	if snapshot == nil || !snapshot.Allows(principal, policy.Permission) {
		return DecisionDeny
	}
	return DecisionAllow
}

// ValidateMethodPolicies reports whether the given table classifies exactly the given
// registered methods: every registered method carries one policy, no policy names a method
// that is not registered, and no method is classified twice. The liaison calls it from
// Validate(), so a defect stops startup before any watcher, goroutine or listener starts.
func ValidateMethodPolicies(table MethodPolicyTable, registered []string) error {
	registeredSet := make(map[string]struct{}, len(registered))
	for _, method := range registered {
		registeredSet[method] = struct{}{}
	}
	classified := make(map[string]struct{}, len(table))
	for _, policy := range table {
		if _, exists := classified[policy.FullMethod]; exists {
			return fmt.Errorf("%w: %s", ErrMethodPolicyDuplicate, policy.FullMethod)
		}
		classified[policy.FullMethod] = struct{}{}
		if _, exists := registeredSet[policy.FullMethod]; !exists {
			return fmt.Errorf("%w: %s", ErrMethodPolicyStale, policy.FullMethod)
		}
	}
	for _, method := range registered {
		if _, exists := classified[method]; !exists {
			return fmt.Errorf("%w: %s", ErrMethodPolicyIncomplete, method)
		}
	}
	return nil
}

// DecisionObserver records authorization outcomes for observability. The interceptor
// reports one decision per unary request, identified only by a method name drawn from the
// registered set and a Decision drawn from the closed outcome set, so no caller-controlled
// string can reach a metric label.
type DecisionObserver interface {
	// ObserveDecision records one authorization decision for fullMethod.
	ObserveDecision(fullMethod string, decision Decision)
}

type principalContextKey struct{}

// PrincipalFromContext returns the trusted principal the authorization interceptor
// established for the given context, and reports whether one was established. The principal is stored
// under a key this package does not export and is minted only by
// auth.Snapshot.Authenticate, so an identity a caller supplied in gRPC metadata or an HTTP
// header can never be read back through this function.
func PrincipalFromContext(ctx context.Context) (auth.Principal, bool) {
	principal, exists := ctx.Value(principalContextKey{}).(auth.Principal)
	if !exists || principal.IsZero() {
		return auth.Principal{}, false
	}
	return principal, true
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
func NewAuthorizationInterceptor(reloader *auth.Reloader, table MethodPolicyTable, observer DecisionObserver) grpclib.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpclib.UnaryServerInfo, handler grpclib.UnaryHandler) (any, error) {
		if reloader == nil {
			return handler(ctx, request)
		}
		configuration := reloader.GetConfig()
		if configuration == nil || !configuration.Enabled {
			return handler(ctx, request)
		}
		if info == nil {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
		if info.FullMethod == "/grpc.health.v1.Health/Check" && !configuration.HealthAuthEnabled {
			return handler(ctx, request)
		}

		snapshot := reloader.CurrentSnapshot()
		principal, authenticated := authenticatePrincipal(ctx, snapshot)
		if !authenticated {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		handlerContext := context.WithValue(ctx, principalContextKey{}, principal)
		if !snapshot.RBACEnabled() {
			return handler(handlerContext, request)
		}

		decision := table.Authorize(snapshot, principal, info.FullMethod)
		if _, classified := table.Policy(info.FullMethod); classified && observer != nil {
			observer.ObserveDecision(info.FullMethod, decision)
		}
		if decision != DecisionAllow {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
		return handler(handlerContext, request)
	}
}

func authenticatePrincipal(ctx context.Context, snapshot auth.Snapshot) (auth.Principal, bool) {
	metadataValues, exists := metadata.FromIncomingContext(ctx)
	if !exists {
		return auth.Principal{}, false
	}
	usernames := metadataValues.Get("username")
	passwords := metadataValues.Get("password")
	if len(usernames) == 0 || len(passwords) == 0 {
		return auth.Principal{}, false
	}
	return snapshot.Authenticate(usernames[0], passwords[0])
}
