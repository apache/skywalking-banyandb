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
	"strings"

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
	// DecisionInvalidRequest rejects the request because the group scope its method's policy
	// needs cannot be read from it. It is reported to the caller as codes.InvalidArgument, so
	// an authenticated caller sending a malformed request learns that it was malformed and
	// learns nothing about what it would have been allowed to do.
	DecisionInvalidRequest
)

// DecisionReason is a bounded explanation for an authorization decision.
type DecisionReason string

// Bounded decision reasons used by authorization observability.
const (
	DecisionReasonGranted             DecisionReason = "granted"
	DecisionReasonUnauthenticated     DecisionReason = "unauthenticated"
	DecisionReasonPermissionMissing   DecisionReason = "permission_missing"
	DecisionReasonExecutorUnavailable DecisionReason = "executor_unavailable"
	DecisionReasonHealthExempt        DecisionReason = "health_exempt"
	DecisionReasonInvalidRequest      DecisionReason = "invalid_request"
)

// DecisionReasons returns every bounded reason an authorization decision can carry, in a
// fixed order. It is the closed label set observability is allowed to report.
func DecisionReasons() []DecisionReason {
	return []DecisionReason{
		DecisionReasonGranted,
		DecisionReasonUnauthenticated,
		DecisionReasonPermissionMissing,
		DecisionReasonExecutorUnavailable,
		DecisionReasonHealthExempt,
		DecisionReasonInvalidRequest,
	}
}

// DecisionLabel returns the bounded observability label for the decision. Internal deny
// variants collapse to "deny" and are distinguished by a bounded reason label.
func DecisionLabel(decision Decision) string {
	if decision == DecisionAllow {
		return "allow"
	}
	return "deny"
}

// DecisionLabels returns every value DecisionLabel produces, in a fixed order.
func DecisionLabels() []string {
	return []string{DecisionLabel(DecisionAllow), DecisionLabel(DecisionDeny)}
}

// MethodPolicy classifies one gRPC method the liaison serves.
type MethodPolicy struct {
	// FullMethod is the gRPC full method name, for example
	// "/banyandb.database.v1.ClusterStateService/GetClusterState".
	FullMethod string
	// Permission is the capability a principal must hold to invoke the method.
	Permission auth.Permission
	// Access identifies authentication-only, health-policy, or RBAC permission checks.
	Access MethodAccess
	// Scope names the typed extractor that resolves the method's group scopes from its
	// request. It pairs the method with exactly one scope family, so a decision for the
	// method reads the groups out of the request shape that method actually has.
	Scope ScopeFamily
	// Activated reports whether this release has an executor that can decide Permission.
	// A registered method whose permission has no activated executor fails closed for
	// every principal, admin included.
	Activated bool
}

// MethodAccess identifies the authorization executor family for a method.
type MethodAccess int

const (
	// MethodAccessPermission requires the policy's RBAC permission.
	MethodAccessPermission MethodAccess = iota
	// MethodAccessAuthenticated requires valid credentials but no role binding.
	MethodAccessAuthenticated
	// MethodAccessHealth preserves the configurable health-check authentication policy.
	MethodAccessHealth
)

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

// RegisteredServiceMethods returns the methods exposed by an already registered gRPC server.
func RegisteredServiceMethods(serviceInfo map[string]grpclib.ServiceInfo) []string {
	registered := make([]string, 0)
	for serviceName, service := range serviceInfo {
		for _, method := range service.Methods {
			registered = append(registered, "/"+serviceName+"/"+method.Name)
		}
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
// liaison can serve, the permission it requires, the scope family a decision for it reads out
// of its request, and whether this release can decide that permission.
func GlobalMethodPolicies() MethodPolicyTable {
	policies := MethodPolicyTable{
		authenticatedPolicy("/banyandb.common.v1.Service/GetAPIVersion"),
		healthPolicy("/grpc.health.v1.Health/Check"),
		authenticatedPolicy("/grpc.health.v1.Health/List"),
		authenticatedPolicy("/grpc.health.v1.Health/Watch"),
		permissionPolicy("/banyandb.database.v1.ClusterStateService/GetClusterState", auth.PermissionClusterRead, ScopeGlobal),
		permissionPolicy("/banyandb.database.v1.NodeQueryService/GetCurrentNode", auth.PermissionClusterRead, ScopeGlobal),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Inspect", auth.PermissionClusterRead, ScopeGlobal),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Query", auth.PermissionClusterRead, ScopeGlobal),
		permissionPolicy("/banyandb.database.v1.SnapshotService/Snapshot", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.stream.v1.StreamService/DeleteExpiredSegments", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.measure.v1.MeasureService/DeleteExpiredSegments", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.trace.v1.TraceService/DeleteExpiredSegments", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.measure.v1.MeasureService/InternalQuery", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.cluster.v1.NodeSchemaStatusService/GetMaxRevision", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.cluster.v1.NodeSchemaStatusService/GetKeyRevisions", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.cluster.v1.NodeSchemaStatusService/GetAbsentKeys", auth.PermissionClusterAdmin, ScopeGlobal),
		permissionPolicy("/banyandb.schema.v1.SchemaBarrierService/AwaitRevisionApplied", auth.PermissionSchemaRead, ScopeGlobal),
		permissionPolicy("/banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied", auth.PermissionSchemaRead, ScopeSchemaKeys),
		permissionPolicy("/banyandb.schema.v1.SchemaBarrierService/AwaitSchemaDeleted", auth.PermissionSchemaRead, ScopeSchemaKeys),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Get", auth.PermissionSchemaRead, ScopeDirectGroup),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/List", auth.PermissionSchemaRead, ScopeVisibleGroups),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Exist", auth.PermissionSchemaRead, ScopeDirectGroup),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Create", auth.PermissionSchemaWrite, ScopeGroupBodyName),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Update", auth.PermissionSchemaWrite, ScopeGroupBodyName),
		permissionPolicy("/banyandb.database.v1.GroupRegistryService/Delete", auth.PermissionSchemaWrite, ScopeDirectGroup),
		permissionPolicy("/banyandb.stream.v1.StreamService/Query", auth.PermissionDataRead, ScopeRepeatedGroups),
		permissionPolicy("/banyandb.stream.v1.StreamService/Write", auth.PermissionDataWrite, ScopeFrameGroups),
		permissionPolicy("/banyandb.measure.v1.MeasureService/Query", auth.PermissionDataRead, ScopeRepeatedGroups),
		permissionPolicy("/banyandb.measure.v1.MeasureService/TopN", auth.PermissionDataRead, ScopeRepeatedGroups),
		permissionPolicy("/banyandb.measure.v1.MeasureService/Write", auth.PermissionDataWrite, ScopeFrameGroups),
		permissionPolicy("/banyandb.trace.v1.TraceService/Query", auth.PermissionDataRead, ScopeRepeatedGroups),
		permissionPolicy("/banyandb.trace.v1.TraceService/Write", auth.PermissionDataWrite, ScopeFrameGroups),
		permissionPolicy("/banyandb.property.v1.PropertyService/Query", auth.PermissionDataRead, ScopeRepeatedGroups),
		permissionPolicy("/banyandb.property.v1.PropertyService/Apply", auth.PermissionDataWrite, ScopePropertyGroup),
		permissionPolicy("/banyandb.property.v1.PropertyService/Delete", auth.PermissionDataWrite, ScopeDirectGroup),
		permissionPolicy("/banyandb.bydbql.v1.BydbQLService/Query", auth.PermissionDataRead, ScopePostTransform),
	}
	for _, service := range []string{
		"StreamRegistryService", "MeasureRegistryService", "TraceRegistryService", "IndexRuleRegistryService",
		"IndexRuleBindingRegistryService", "TopNAggregationRegistryService", "PropertyRegistryService",
	} {
		servicePrefix := "/banyandb.database.v1." + service + "/"
		policies = append(policies,
			permissionPolicy(servicePrefix+"Get", auth.PermissionSchemaRead, ScopeMetadataGroup),
			permissionPolicy(servicePrefix+"List", auth.PermissionSchemaRead, ScopeDirectGroup),
			permissionPolicy(servicePrefix+"Exist", auth.PermissionSchemaRead, ScopeMetadataGroup),
			permissionPolicy(servicePrefix+"Create", auth.PermissionSchemaWrite, ScopeResourceMetadataGroup),
			permissionPolicy(servicePrefix+"Update", auth.PermissionSchemaWrite, ScopeResourceMetadataGroup),
			permissionPolicy(servicePrefix+"Delete", auth.PermissionSchemaWrite, ScopeMetadataGroup),
		)
	}
	return policies
}

func permissionPolicy(method string, permission auth.Permission, scope ScopeFamily) MethodPolicy {
	return MethodPolicy{FullMethod: method, Permission: permission, Access: MethodAccessPermission, Scope: scope, Activated: true}
}

func authenticatedPolicy(method string) MethodPolicy {
	return MethodPolicy{FullMethod: method, Access: MethodAccessAuthenticated, Activated: true}
}

func healthPolicy(method string) MethodPolicy {
	return MethodPolicy{FullMethod: method, Access: MethodAccessHealth, Activated: true}
}

// ActiveMethodPolicies returns the fixed policy subset for the conditionally registered services.
func ActiveMethodPolicies(activation ServiceActivation) MethodPolicyTable {
	allPolicies := GlobalMethodPolicies()
	activePolicies := make(MethodPolicyTable, 0, len(allPolicies))
	for _, policy := range allPolicies {
		if strings.HasPrefix(policy.FullMethod, "/banyandb.schema.v1.SchemaBarrierService/") && !activation.SchemaBarrier {
			continue
		}
		if strings.HasPrefix(policy.FullMethod, "/banyandb.cluster.v1.NodeSchemaStatusService/") && !activation.NodeSchemaStatus {
			continue
		}
		activePolicies = append(activePolicies, policy)
	}
	return activePolicies
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

// Authorize decides whether principal may invoke fullMethod for the scopes in request.
// It returns the bounded outcome and reason reported by authorization observability.
func (table MethodPolicyTable) Authorize(
	snapshot auth.Snapshot, principal auth.Principal, fullMethod string, request any,
) (Decision, DecisionReason) {
	policy, exists := table.Policy(fullMethod)
	if !exists {
		return DecisionDeny, DecisionReasonPermissionMissing
	}
	if policy.Access == MethodAccessAuthenticated || policy.Access == MethodAccessHealth {
		return DecisionAllow, DecisionReasonGranted
	}
	if !policy.Activated {
		return DecisionUnavailable, DecisionReasonExecutorUnavailable
	}
	scopes, scopeErr := RequestScopes(policy.Scope, request)
	if scopeErr != nil {
		return DecisionInvalidRequest, DecisionReasonInvalidRequest
	}
	if snapshot == nil {
		return DecisionDeny, DecisionReasonPermissionMissing
	}
	if isDeferredScope(policy.Scope) {
		if !snapshot.AllowsAny(principal, policy.Permission) {
			return DecisionDeny, DecisionReasonPermissionMissing
		}
		return DecisionAllow, DecisionReasonGranted
	}
	if !snapshot.Allows(principal, policy.Permission, scopes...) {
		return DecisionDeny, DecisionReasonPermissionMissing
	}
	return DecisionAllow, DecisionReasonGranted
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
	ObserveDecision(fullMethod, permission string, decision Decision, reason DecisionReason)
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
		policy, classified := table.Policy(info.FullMethod)
		snapshot := reloader.CurrentSnapshot()
		if policy.Access == MethodAccessHealth && !configuration.HealthAuthEnabled {
			observeDecision(observer, snapshot, policy, DecisionAllow, DecisionReasonHealthExempt)
			return handler(ctx, request)
		}

		principal, authenticated := authenticatePrincipal(ctx, snapshot)
		if !authenticated {
			if classified {
				observeDecision(observer, snapshot, policy, DecisionDeny, DecisionReasonUnauthenticated)
			}
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		handlerContext := ContextWithSnapshot(context.WithValue(ctx, principalContextKey{}, principal), snapshot)
		if !snapshot.RBACEnabled() {
			return handler(handlerContext, request)
		}

		decision, reason := table.Authorize(snapshot, principal, info.FullMethod, request)
		if classified && (policy.Scope != ScopePostTransform || decision != DecisionAllow) {
			observeDecision(observer, snapshot, policy, decision, reason)
		}
		if decision != DecisionAllow {
			return nil, decisionError(decision)
		}
		var postTransformDecision *postTransformDecisionSlot
		if classified && policy.Scope == ScopePostTransform {
			handlerContext, postTransformDecision = contextWithPostTransformDecision(handlerContext)
		}
		reply, handlerErr := handler(handlerContext, request)
		if postTransformDecision != nil {
			observeDecision(observer, snapshot, policy, postTransformDecision.decision, postTransformDecision.reason)
		}
		if handlerErr != nil {
			return reply, handlerErr
		}
		return FilterResponse(snapshot, principal, policy, reply), nil
	}
}

type principalServerStream struct {
	grpclib.ServerStream
	ctx context.Context
}

func (s *principalServerStream) Context() context.Context {
	return s.ctx
}

// NewAuthorizationStreamInterceptor authenticates and authorizes a stream before its handler starts.
func NewAuthorizationStreamInterceptor(
	reloader *auth.Reloader,
	table MethodPolicyTable,
	observer DecisionObserver,
) grpclib.StreamServerInterceptor {
	return func(server any, stream grpclib.ServerStream, info *grpclib.StreamServerInfo, handler grpclib.StreamHandler) error {
		if reloader == nil {
			return handler(server, stream)
		}
		configuration := reloader.GetConfig()
		if configuration == nil || !configuration.Enabled {
			return handler(server, stream)
		}
		if info == nil {
			return status.Error(codes.PermissionDenied, "permission denied")
		}
		policy, classified := table.Policy(info.FullMethod)
		snapshot := reloader.CurrentSnapshot()
		principal, authenticated := authenticatePrincipal(stream.Context(), snapshot)
		if !authenticated {
			if classified {
				observeDecision(observer, snapshot, policy, DecisionDeny, DecisionReasonUnauthenticated)
			}
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
		handlerContext := ContextWithSnapshot(context.WithValue(stream.Context(), principalContextKey{}, principal), snapshot)
		trustedStream := &principalServerStream{ServerStream: stream, ctx: handlerContext}
		if !snapshot.RBACEnabled() {
			return handler(server, trustedStream)
		}

		decision, reason := table.Authorize(snapshot, principal, info.FullMethod, nil)
		if classified && (policy.Scope != ScopeFrameGroups || decision != DecisionAllow) {
			observeDecision(observer, snapshot, policy, decision, reason)
		}
		if decision != DecisionAllow {
			return decisionError(decision)
		}
		if policy.Scope == ScopeFrameGroups {
			return handler(server, NewFrameAuthorizer(trustedStream, FrameAuthorization{
				Snapshots: reloader,
				Observer:  observer,
				Principal: principal,
				Policy:    policy,
			}))
		}
		return handler(server, trustedStream)
	}
}

func observeDecision(observer DecisionObserver, snapshot auth.Snapshot, policy MethodPolicy, decision Decision, reason DecisionReason) {
	if observer == nil || snapshot == nil || !snapshot.RBACEnabled() {
		return
	}
	observer.ObserveDecision(policy.FullMethod, permissionLabel(policy), decision, reason)
}

func permissionLabel(policy MethodPolicy) string {
	switch policy.Access {
	case MethodAccessAuthenticated:
		return "authenticated"
	case MethodAccessHealth:
		return "health"
	default:
		return string(policy.Permission)
	}
}

func decisionError(decision Decision) error {
	if decision == DecisionInvalidRequest {
		return status.Error(codes.InvalidArgument, "invalid request")
	}
	return status.Error(codes.PermissionDenied, "permission denied")
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
