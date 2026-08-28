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
	"sort"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
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
	case ScopeDirectGroup:
		return directGroupScopes(family, request)
	case ScopeGroupBodyName:
		return groupBodyNameScopes(family, request)
	case ScopeMetadataGroup:
		return metadataGroupScopes(family, request)
	case ScopeResourceMetadataGroup:
		return resourceMetadataGroupScopes(family, request)
	case ScopeSchemaKeys:
		return schemaKeyScopes(family, request)
	default:
		return nil, unresolvableScope(family, request)
	}
}

func directGroupScopes(family ScopeFamily, request any) ([]string, error) {
	switch typedRequest := request.(type) {
	case *databasev1.GroupRegistryServiceGetRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.GroupRegistryServiceExistRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.GroupRegistryServiceDeleteRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.StreamRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.MeasureRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.TraceRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.IndexRuleRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.IndexRuleBindingRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.TopNAggregationRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.PropertyRegistryServiceListRequest:
		if typedRequest != nil {
			return oneScope(family, request, typedRequest.GetGroup())
		}
	}
	return nil, unresolvableScope(family, request)
}

func groupBodyNameScopes(family ScopeFamily, request any) ([]string, error) {
	switch typedRequest := request.(type) {
	case *databasev1.GroupRegistryServiceCreateRequest:
		if typedRequest != nil {
			return groupNameScope(family, request, typedRequest.GetGroup())
		}
	case *databasev1.GroupRegistryServiceUpdateRequest:
		if typedRequest != nil {
			return groupNameScope(family, request, typedRequest.GetGroup())
		}
	}
	return nil, unresolvableScope(family, request)
}

func groupNameScope(family ScopeFamily, request any, group *commonv1.Group) ([]string, error) {
	if group == nil || group.GetMetadata() == nil {
		return nil, unresolvableScope(family, request)
	}
	return oneScope(family, request, group.GetMetadata().GetName())
}

func metadataGroupScopes(family ScopeFamily, request any) ([]string, error) {
	switch typedRequest := request.(type) {
	case *databasev1.StreamRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.StreamRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.StreamRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.MeasureRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.MeasureRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.MeasureRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.TraceRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.TraceRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.TraceRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.IndexRuleRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.IndexRuleRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.IndexRuleRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.IndexRuleBindingRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.IndexRuleBindingRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.IndexRuleBindingRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.TopNAggregationRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.TopNAggregationRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.TopNAggregationRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.PropertyRegistryServiceGetRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.PropertyRegistryServiceExistRequest:
		return metadataScope(family, request, typedRequest)
	case *databasev1.PropertyRegistryServiceDeleteRequest:
		return metadataScope(family, request, typedRequest)
	default:
		return nil, unresolvableScope(family, request)
	}
}

type metadataRequest interface {
	GetMetadata() *commonv1.Metadata
}

func metadataScope(family ScopeFamily, request any, metadataCarrier metadataRequest) ([]string, error) {
	if metadataCarrier == nil || metadataCarrier.GetMetadata() == nil {
		return nil, unresolvableScope(family, request)
	}
	return oneScope(family, request, metadataCarrier.GetMetadata().GetGroup())
}

func resourceMetadataGroupScopes(family ScopeFamily, request any) ([]string, error) {
	switch typedRequest := request.(type) {
	case *databasev1.StreamRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetStream())
	case *databasev1.StreamRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetStream())
	case *databasev1.MeasureRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetMeasure())
	case *databasev1.MeasureRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetMeasure())
	case *databasev1.TraceRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetTrace())
	case *databasev1.TraceRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetTrace())
	case *databasev1.IndexRuleRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetIndexRule())
	case *databasev1.IndexRuleRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetIndexRule())
	case *databasev1.IndexRuleBindingRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetIndexRuleBinding())
	case *databasev1.IndexRuleBindingRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetIndexRuleBinding())
	case *databasev1.TopNAggregationRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetTopNAggregation())
	case *databasev1.TopNAggregationRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetTopNAggregation())
	case *databasev1.PropertyRegistryServiceCreateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetProperty())
	case *databasev1.PropertyRegistryServiceUpdateRequest:
		return resourceMetadataScope(family, request, typedRequest.GetProperty())
	default:
		return nil, unresolvableScope(family, request)
	}
}

type resourceWithMetadata interface {
	GetMetadata() *commonv1.Metadata
}

func resourceMetadataScope(family ScopeFamily, request any, resource resourceWithMetadata) ([]string, error) {
	if resource == nil || resource.GetMetadata() == nil {
		return nil, unresolvableScope(family, request)
	}
	return oneScope(family, request, resource.GetMetadata().GetGroup())
}

func schemaKeyScopes(family ScopeFamily, request any) ([]string, error) {
	var keys []*schemav1.SchemaKey
	switch typedRequest := request.(type) {
	case *schemav1.AwaitSchemaAppliedRequest:
		if typedRequest == nil {
			return nil, unresolvableScope(family, request)
		}
		keys = typedRequest.GetKeys()
	case *schemav1.AwaitSchemaDeletedRequest:
		if typedRequest == nil {
			return nil, unresolvableScope(family, request)
		}
		keys = typedRequest.GetKeys()
	default:
		return nil, unresolvableScope(family, request)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	groups := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			return nil, unresolvableScope(family, request)
		}
		group := key.GetGroup()
		if key.GetKind() == "group" {
			group = key.GetName()
		}
		groups = append(groups, group)
	}
	return normalizedScopes(family, request, groups)
}

func oneScope(family ScopeFamily, request any, group string) ([]string, error) {
	return normalizedScopes(family, request, []string{group})
}

func normalizedScopes(family ScopeFamily, request any, groups []string) ([]string, error) {
	resolved := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		scope := strings.TrimSpace(group)
		if scope == "" {
			return nil, unresolvableScope(family, request)
		}
		resolved[scope] = struct{}{}
	}
	scopes := make([]string, 0, len(resolved))
	for scope := range resolved {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	return scopes, nil
}

func unresolvableScope(family ScopeFamily, request any) error {
	return fmt.Errorf("%w: family %d carries no resolvable scope for %T", ErrScopeUnresolvable, family, request)
}
