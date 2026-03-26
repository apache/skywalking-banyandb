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

package sub

import (
	"context"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// InspectAll lists all groups and returns their full lifecycle info.
func (s *server) InspectAll(ctx context.Context, _ *fodcv1.InspectAllRequest) (*fodcv1.InspectAllResponse, error) {
	if s.metadataRepo == nil {
		return nil, fmt.Errorf("metadata repository not available")
	}
	groups, err := s.metadataRepo.GroupRegistry().ListGroup(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	result := make([]*fodcv1.GroupLifecycleInfo, 0, len(groups))
	for _, group := range groups {
		if group == nil || group.Metadata == nil {
			continue
		}
		info := s.inspectGroup(ctx, group)
		result = append(result, info)
	}
	return &fodcv1.InspectAllResponse{Groups: result}, nil
}

func (s *server) inspectGroup(ctx context.Context, group *commonv1.Group) *fodcv1.GroupLifecycleInfo {
	groupName := group.Metadata.Name
	info := &fodcv1.GroupLifecycleInfo{
		Name:         groupName,
		Catalog:      catalogToString(group.Catalog),
		ResourceOpts: group.ResourceOpts,
	}
	dataInfo, err := s.metadataRepo.CollectDataInfo(ctx, groupName)
	if err != nil {
		s.log.Warn().Err(err).Str("group", groupName).Msg("Failed to collect data info")
	} else {
		info.DataInfo = dataInfo
	}
	return info
}

func catalogToString(catalog commonv1.Catalog) string {
	if name, ok := commonv1.Catalog_name[int32(catalog)]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_%d", catalog)
}
