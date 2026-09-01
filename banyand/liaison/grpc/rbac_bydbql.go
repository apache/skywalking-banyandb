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

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

type (
	snapshotContextKey              struct{}
	postTransformDecisionContextKey struct{}
)

type postTransformDecisionSlot struct {
	reason   DecisionReason
	decision Decision
}

// SnapshotFromContext returns the security snapshot the authorization interceptor took the
// request's decision from, and reports whether one was established.
//
// A handler that has to decide something itself must decide it against this snapshot rather
// than by asking the reloader again. The two differ whenever a reload lands while the handler
// runs, and a handler that re-reads would mix one revision's admission with another
// revision's grants. The snapshot is stored under a key this package does not export, so a
// context a caller built carries nothing this function will read back.
func SnapshotFromContext(ctx context.Context) (auth.Snapshot, bool) {
	snapshot, exists := ctx.Value(snapshotContextKey{}).(auth.Snapshot)
	if !exists || snapshot == nil {
		return nil, false
	}
	return snapshot, true
}

// ContextWithSnapshot returns ctx carrying snapshot as the one the request's authorization
// decision was taken from. Only the authorization interceptor calls it, which is what makes
// SnapshotFromContext trustworthy inside a handler.
func ContextWithSnapshot(ctx context.Context, snapshot auth.Snapshot) context.Context {
	return context.WithValue(ctx, snapshotContextKey{}, snapshot)
}

func contextWithPostTransformDecision(ctx context.Context) (context.Context, *postTransformDecisionSlot) {
	slot := &postTransformDecisionSlot{decision: DecisionAllow, reason: DecisionReasonGranted}
	return context.WithValue(ctx, postTransformDecisionContextKey{}, slot), slot
}

func postTransformDecisionFromContext(ctx context.Context) (*postTransformDecisionSlot, bool) {
	slot, exists := ctx.Value(postTransformDecisionContextKey{}).(*postTransformDecisionSlot)
	if !exists || slot == nil {
		return nil, false
	}
	return slot, true
}

// AuthorizeTransformedRequest decides the native request a ByDBQL query was transformed into,
// and returns the outcome together with the bounded reason the liaison reports for it.
//
// It is the whole of ByDBQL's authorization. The raw query text, its parameters, its casing,
// its comments and the transport that carried it are not resources and are never inspected:
// only the typed native request the transformer produced is, and it is decided exactly as the
// equivalent native method would be — every group it lists must be held for
// auth.PermissionDataRead, so one forbidden group denies the whole query. Because the
// decision is taken over the transformed request, no query text can address a group the
// decision does not see.
//
// The ByDBQL handler calls it after transformation and before it dispatches to the native
// handler, so a denied query never reaches one. A snapshot with RBAC off, which is what a
// users-only or no-auth-file deployment produces, allows every query: those deployments see
// ByDBQL exactly as they did before.
func AuthorizeTransformedRequest(snapshot auth.Snapshot, principal auth.Principal, request any) (Decision, DecisionReason) {
	if snapshot == nil || !snapshot.RBACEnabled() {
		return DecisionAllow, DecisionReasonGranted
	}
	scopes, scopeErr := RequestScopes(ScopeRepeatedGroups, request)
	if scopeErr != nil {
		return DecisionInvalidRequest, DecisionReasonInvalidRequest
	}
	if !snapshot.Allows(principal, auth.PermissionDataRead, scopes...) {
		return DecisionDeny, DecisionReasonPermissionMissing
	}
	return DecisionAllow, DecisionReasonGranted
}
