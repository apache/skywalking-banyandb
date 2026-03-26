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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockGroupRegistry struct {
	schema.Group
	err    error
	groups []*commonv1.Group
}

func (m *mockGroupRegistry) ListGroup(_ context.Context) ([]*commonv1.Group, error) {
	return m.groups, m.err
}

type mockMetadataRepo struct {
	metadata.Repo
	groupRegistry *mockGroupRegistry
	dataInfoMap   map[string][]*databasev1.DataInfo
	dataInfoErr   map[string]error
}

func (m *mockMetadataRepo) GroupRegistry() schema.Group {
	return m.groupRegistry
}

func (m *mockMetadataRepo) CollectDataInfo(_ context.Context, group string) ([]*databasev1.DataInfo, error) {
	if m.dataInfoErr != nil {
		if err, ok := m.dataInfoErr[group]; ok {
			return nil, err
		}
	}
	if m.dataInfoMap != nil {
		return m.dataInfoMap[group], nil
	}
	return nil, nil
}

func TestCatalogToString(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		catalog  commonv1.Catalog
	}{
		{
			name:     "unspecified",
			catalog:  commonv1.Catalog_CATALOG_UNSPECIFIED,
			expected: "CATALOG_UNSPECIFIED",
		},
		{
			name:     "stream",
			catalog:  commonv1.Catalog_CATALOG_STREAM,
			expected: "CATALOG_STREAM",
		},
		{
			name:     "measure",
			catalog:  commonv1.Catalog_CATALOG_MEASURE,
			expected: "CATALOG_MEASURE",
		},
		{
			name:     "unknown value",
			catalog:  commonv1.Catalog(999),
			expected: "UNKNOWN_999",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := catalogToString(tt.catalog)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInspectAll_NilMetadataRepo(t *testing.T) {
	s := &server{
		log: logger.GetLogger("test"),
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "metadata repository not available")
}

func TestInspectAll_EmptyGroups(t *testing.T) {
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{},
		},
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Empty(t, resp.Groups)
}

func TestInspectAll_SkipsNilGroups(t *testing.T) {
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{
				nil,
				{Metadata: nil},
				{
					Metadata: &commonv1.Metadata{Name: "valid_group"},
					Catalog:  commonv1.Catalog_CATALOG_STREAM,
				},
			},
		},
		dataInfoMap: map[string][]*databasev1.DataInfo{
			"valid_group": {},
		},
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 1)
	assert.Equal(t, "valid_group", resp.Groups[0].Name)
	assert.Equal(t, "CATALOG_STREAM", resp.Groups[0].Catalog)
}

func TestInspectAll_MultipleGroups(t *testing.T) {
	resourceOpts := &commonv1.ResourceOpts{
		ShardNum: 2,
		SegmentInterval: &commonv1.IntervalRule{
			Unit: commonv1.IntervalRule_UNIT_DAY,
			Num:  1,
		},
		Ttl: &commonv1.IntervalRule{
			Unit: commonv1.IntervalRule_UNIT_DAY,
			Num:  7,
		},
	}
	dataInfo := []*databasev1.DataInfo{
		{DataSizeBytes: 1024},
	}
	repo := &mockMetadataRepo{
		groupRegistry: &mockGroupRegistry{
			groups: []*commonv1.Group{
				{
					Metadata:     &commonv1.Metadata{Name: "sw_metric"},
					Catalog:      commonv1.Catalog_CATALOG_MEASURE,
					ResourceOpts: resourceOpts,
				},
				{
					Metadata: &commonv1.Metadata{Name: "sw_record"},
					Catalog:  commonv1.Catalog_CATALOG_STREAM,
				},
			},
		},
		dataInfoMap: map[string][]*databasev1.DataInfo{
			"sw_metric": dataInfo,
			"sw_record": {},
		},
		dataInfoErr: map[string]error{
			"sw_record": fmt.Errorf("collect error"),
		},
	}
	s := &server{
		log:          logger.GetLogger("test"),
		metadataRepo: repo,
	}
	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Groups, 2)

	assert.Equal(t, "sw_metric", resp.Groups[0].Name)
	assert.Equal(t, "CATALOG_MEASURE", resp.Groups[0].Catalog)
	assert.Equal(t, resourceOpts, resp.Groups[0].ResourceOpts)
	assert.Equal(t, dataInfo, resp.Groups[0].DataInfo)

	assert.Equal(t, "sw_record", resp.Groups[1].Name)
	assert.Equal(t, "CATALOG_STREAM", resp.Groups[1].Catalog)
	assert.Nil(t, resp.Groups[1].DataInfo)
}
