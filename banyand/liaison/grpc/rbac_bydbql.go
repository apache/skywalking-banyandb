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

// SnapshotFromContext returns the request's authorization snapshot, if established.
func SnapshotFromContext(ctx context.Context) (auth.Snapshot, bool) {
	snapshot, exists := ctx.Value(snapshotContextKey{}).(auth.Snapshot)
	if !exists || snapshot == nil {
		return nil, false
	}
	return snapshot, true
}

// ContextWithSnapshot returns a context carrying the request's authorization snapshot.
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

// AuthorizeTransformedRequest decides whether principal may read every group addressed
// by a transformed ByDBQL request.
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
