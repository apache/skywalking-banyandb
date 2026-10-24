// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

func TestNativeLoggerHonorsConfiguredLevel(t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, logger.Init(logger.Logging{Env: "prod", Level: "debug"}))
	})
	require.NoError(t, logger.Init(logger.Logging{Env: "prod", Level: "info"}))
	require.Equal(t, zerolog.InfoLevel, nativeLogger().GetLevel())
}

// TestScopeLabelTagsAlignWithValues pins a tag value to the position of its own tag
// name: buildTags and buildTagValues both range over the same label map.
func TestScopeLabelTagsAlignWithValues(t *testing.T) {
	scope := meter.NewHierarchicalScope(ObservabilityGroupName, ".").
		ConstLabels(meter.LabelPairs{"zone": "eu-west", "team": "core", "region": "west", "env": "prod"})
	nodeInfo := NodeInfo{
		Type:        "streaming",
		NodeID:      "node-1",
		GrpcAddress: "127.0.0.1:17876",
		HTTPAddress: "127.0.0.1:17877",
	}
	// node_type, node_id, grpc_address and http_address precede the scope labels.
	const scopeTagsBegin = 4
	for range 100 {
		tags, _ := buildTags(scope, nil)
		tagValues := buildTagValues(nodeInfo, scope)
		require.Len(t, tagValues, len(tags))
		actual := make(meter.LabelPairs, len(tags)-scopeTagsBegin)
		for i := scopeTagsBegin; i < len(tags); i++ {
			actual[tags[i].Name] = tagValues[i].GetStr().GetValue()
		}
		require.Equal(t, scope.GetLabels(), actual)
	}
}
