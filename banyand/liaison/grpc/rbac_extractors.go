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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// ErrScopeUnresolvable reports a request with no resolvable group scope.
var ErrScopeUnresolvable = errors.New("method scope: request carries no resolvable group")

// ScopeFamily identifies how a method's group scopes are resolved.
type ScopeFamily int

const (
	// ScopeUnspecified names no scope resolver.
	ScopeUnspecified ScopeFamily = iota
	// ScopeGlobal requires a wildcard grant.
	ScopeGlobal
	// ScopeDirectGroup reads a request's group field.
	ScopeDirectGroup
	// ScopeGroupBodyName reads the name of a group body.
	ScopeGroupBodyName
	// ScopeMetadataGroup reads the group from request metadata.
	ScopeMetadataGroup
	// ScopeResourceMetadataGroup reads the group from resource metadata.
	ScopeResourceMetadataGroup
	// ScopeSchemaKeys reads the groups addressed by schema keys.
	ScopeSchemaKeys
	// ScopeVisibleGroups defers authorization to response filtering.
	ScopeVisibleGroups
	// ScopeRepeatedGroups reads a request's repeated groups field.
	ScopeRepeatedGroups
	// ScopePropertyGroup reads the group from a property body.
	ScopePropertyGroup
	// ScopeFrameGroups defers authorization to individual stream frames.
	ScopeFrameGroups
	// ScopePostTransform defers authorization until a query is transformed.
	ScopePostTransform
)

func isDeferredScope(family ScopeFamily) bool {
	switch family {
	case ScopeVisibleGroups, ScopeFrameGroups, ScopePostTransform:
		return true
	default:
		return false
	}
}

// RequestScopes returns the canonical group scopes addressed by request. Unresolvable
// requests return an error wrapping ErrScopeUnresolvable.
func RequestScopes(family ScopeFamily, request any) ([]string, error) {
	if family == ScopeGlobal || isDeferredScope(family) {
		return nil, nil
	}
	switch family {
	case ScopeRepeatedGroups:
		return repeatedGroupScopes(family, request)
	case ScopePropertyGroup:
		return propertyGroupScopes(family, request)
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

// repeatedGroupScopes resolves the deduplicated, sorted set of groups a native read request
// lists in its repeated groups field: the Stream, Measure, Trace and Property query requests
// and the Measure TopN request. A request of any other type, or one listing an empty or
// whitespace-only group, carries no resolvable scope.
func repeatedGroupScopes(family ScopeFamily, request any) ([]string, error) {
	var groups []string
	switch typedRequest := request.(type) {
	case *streamv1.QueryRequest:
		if typedRequest != nil {
			groups = typedRequest.GetGroups()
		}
	case *measurev1.QueryRequest:
		if typedRequest != nil {
			groups = typedRequest.GetGroups()
		}
	case *measurev1.TopNRequest:
		if typedRequest != nil {
			groups = typedRequest.GetGroups()
		}
	case *tracev1.QueryRequest:
		if typedRequest != nil {
			groups = typedRequest.GetGroups()
		}
	case *propertyv1.QueryRequest:
		if typedRequest != nil {
			groups = typedRequest.GetGroups()
		}
	default:
		return nil, unresolvableScope(family, request)
	}
	if len(groups) == 0 {
		return nil, unresolvableScope(family, request)
	}
	return normalizedScopes(family, request, groups)
}

// propertyGroupScopes resolves the group of the Property body a mutation carries, which is
// Property.Metadata.Group. A request with no property, no metadata, or an empty group carries
// no resolvable scope.
func propertyGroupScopes(family ScopeFamily, request any) ([]string, error) {
	typedRequest, matched := request.(*propertyv1.ApplyRequest)
	if !matched || typedRequest == nil || typedRequest.GetProperty() == nil || typedRequest.GetProperty().GetMetadata() == nil {
		return nil, unresolvableScope(family, request)
	}
	return oneScope(family, request, typedRequest.GetProperty().GetMetadata().GetGroup())
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
	case *propertyv1.DeleteRequest:
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
