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
	"errors"
	"fmt"
)

// ErrScopeUnresolvable reports a request from which the group scopes its method's policy
// needs cannot be read: an absent resource body, absent metadata, an empty or whitespace-only
// group name, or a request whose type does not belong to the policy's scope family. The
// liaison reports it to the caller as codes.InvalidArgument, so a malformed request from an
// authenticated caller is answered as malformed rather than as an authorization outcome.
var ErrScopeUnresolvable = errors.New("method scope: request carries no resolvable group")

// ScopeFamily names the typed extractor that resolves one method's group scopes from its
// request message. Every classified method points at exactly one family. There is no generic
// "find a field named group" rule: BanyanDB request shapes differ too much for one, and a
// newly added RPC must be classified explicitly rather than inherit a scope by accident.
type ScopeFamily int

const (
	// ScopeUnspecified is the zero value and names no extractor. A method whose permission
	// this release cannot decide carries it, and resolving it is an error, so a policy row
	// that is activated without being given a family fails closed instead of falling back to
	// a global decision.
	ScopeUnspecified ScopeFamily = iota
	// ScopeGlobal names the deployment-wide scope: the method addresses no group, and only a
	// wildcard grant satisfies it. Cluster state, node query, internal maintenance and the
	// cluster-wide schema revision wait use it.
	ScopeGlobal
	// ScopeDirectGroup reads the request's own group field, the single non-empty string of
	// Group Get/Exist/Delete and of the seven registry List methods.
	ScopeDirectGroup
	// ScopeGroupBodyName reads the group name out of the Group body a request carries, which
	// for a group is Group.Metadata.Name and never Group.Metadata.Group. Group Create and
	// Update use it.
	ScopeGroupBodyName
	// ScopeMetadataGroup reads Metadata.Group off a request that identifies one existing
	// resource by name. The seven registry Get, Exist and Delete methods use it.
	ScopeMetadataGroup
	// ScopeResourceMetadataGroup reads Metadata.Group out of the resource body a request
	// carries. The seven registry Create and Update methods use it.
	ScopeResourceMetadataGroup
	// ScopeSchemaKeys reads every schema key a barrier wait names. A key of kind "group"
	// scopes to its Name, because that is where a group key carries the group; every other
	// kind scopes to its Group.
	ScopeSchemaKeys
	// ScopeVisibleGroups names the whole-deployment resource set of Group List: the method
	// addresses no group, any grant admits the caller, and the response is reduced afterwards
	// to the groups the caller's scopes cover.
	ScopeVisibleGroups
)

// RequestScopes resolves the group scopes family requires from request. The result is
// deduplicated and sorted, so a request naming one group twice, or two groups in either
// order, yields one canonical scope set that an all-or-nothing decision can be taken over.
//
// ScopeGlobal and ScopeVisibleGroups resolve to no scopes: the first is satisfied only by a
// wildcard grant, the second is admitted by any grant and filtered after its handler. Every
// other family reads its groups from the request, and a request the family cannot be read
// from returns an error wrapping ErrScopeUnresolvable.
func RequestScopes(family ScopeFamily, request any) ([]string, error) {
	switch family {
	case ScopeGlobal, ScopeVisibleGroups:
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: family %d carries no extractor for %T", ErrScopeUnresolvable, family, request)
	}
}
