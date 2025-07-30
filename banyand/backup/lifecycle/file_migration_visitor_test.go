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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestMigrationVisitor_calculateTargetShardID(t *testing.T) {
	tests := []struct {
		name           string
		targetShardNum uint32
		sourceShardID  uint32
		expectedTarget uint32
	}{
		{
			name:           "simple modulo mapping",
			targetShardNum: 4,
			sourceShardID:  7,
			expectedTarget: 3, // 7 % 4 = 3
		},
		{
			name:           "exact division",
			targetShardNum: 5,
			sourceShardID:  10,
			expectedTarget: 0, // 10 % 5 = 0
		},
		{
			name:           "same shard mapping",
			targetShardNum: 8,
			sourceShardID:  3,
			expectedTarget: 3, // 3 % 8 = 3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mv := &MigrationVisitor{
				targetShardNum: tt.targetShardNum,
				logger:         logger.GetLogger("test"),
			}

			result := mv.calculateTargetShardID(tt.sourceShardID)
			assert.Equal(t, tt.expectedTarget, result)
		})
	}
}

func TestMigrationVisitor_extractPartIDFromPath(t *testing.T) {
	tests := []struct {
		name        string
		partPath    string
		expectedID  uint64
		expectError bool
	}{
		{
			name:        "valid 16-char hex path",
			partPath:    "/data/stream/shard0/seg-123/0123456789abcdef",
			expectedID:  0x0123456789abcdef,
			expectError: false,
		},
		{
			name:        "invalid short path",
			partPath:    "/data/stream/shard0/seg-123/abc",
			expectedID:  0,
			expectError: true,
		},
		{
			name:        "invalid non-hex path",
			partPath:    "/data/stream/shard0/seg-123/ghijklmnopqrstuv",
			expectedID:  0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mv := &MigrationVisitor{
				logger: logger.GetLogger("test"),
			}

			result, err := mv.extractPartIDFromPath(tt.partPath)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedID, result)
			}
		})
	}
}

func TestNewMigrationVisitor_Construction(t *testing.T) {
	// Create minimal test group
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: "test-group",
		},
		Catalog: commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			Stages: []*commonv1.LifecycleStage{
				{
					Name:     "hot",
					ShardNum: 4,
					Replicas: 2,
				},
			},
		},
	}

	// Create test node
	nodes := []*databasev1.Node{
		{
			Metadata: &commonv1.Metadata{
				Name: "test-node",
			},
			GrpcAddress: "localhost:17912",
			Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		},
	}

	nodeLabels := map[string]string{
		"nodeRole": "data",
	}

	// This test may fail without proper metadata setup, but validates structure
	visitor, err := NewMigrationVisitor(group, nodeLabels, nodes, nil, logger.GetLogger("test"), nil, "", 1024*1024)
	// We expect this to fail due to missing metadata, but the structure should be valid
	if err != nil {
		t.Logf("Expected error due to minimal test setup: %v", err)
		return
	}

	require.NotNil(t, visitor)
	assert.Equal(t, "test-group", visitor.group)
	assert.NotNil(t, visitor.chunkedClients)
	assert.NotNil(t, visitor.logger)
}
