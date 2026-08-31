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
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// FilterResponse reduces a handler's reply to the part of it the principal may see.
//
// It is selected by the policy's scope family rather than by the reply's type: a policy whose
// Scope is ScopeVisibleGroups has its listed groups reduced to those principal holds the
// policy's permission for, keeping every group when the grant is the wildcard one, and every
// other policy has its reply returned unchanged. The snapshot passed in is the one the
// request's authorization decision was taken from, so a policy reload that lands while the
// handler runs cannot mix one revision's decision with another revision's visibility.
//
// A principal that holds the permission in no scope at all is rejected before its handler
// runs and never reaches this function, so an empty result here means the caller's scopes
// simply cover none of the listed groups.
func FilterResponse(snapshot auth.Snapshot, principal auth.Principal, policy MethodPolicy, reply any) any {
	if policy.Scope != ScopeVisibleGroups || snapshot == nil {
		return reply
	}
	groupList, ok := reply.(*databasev1.GroupRegistryServiceListResponse)
	if !ok || groupList == nil {
		return reply
	}
	filtered := make([]*commonv1.Group, 0, len(groupList.GetGroup()))
	for _, group := range groupList.GetGroup() {
		if group != nil && snapshot.Allows(principal, policy.Permission, group.GetMetadata().GetName()) {
			filtered = append(filtered, group)
		}
	}
	return &databasev1.GroupRegistryServiceListResponse{Group: filtered}
}
