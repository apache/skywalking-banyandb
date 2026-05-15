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

package diagnostics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
}

func TestCollectDiagnosticsReturnsCachedSnapshotWhenGRPCServiceUnset(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "diagnostics")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	aggregator := NewAggregator(testRegistry, nil, testLogger)

	expected := &AggregatedCrashRecord{
		AgentID:     "agent-1",
		PodName:     "pod-a",
		Role:        "datanode",
		ArtifactDir: "artifact-1",
	}
	aggregator.cacheMu.Lock()
	aggregator.cache["agent-1::artifact-1"] = expected
	aggregator.cacheMu.Unlock()

	records, collectErr := aggregator.CollectDiagnostics(context.Background(), nil)
	require.NoError(t, collectErr)
	require.Len(t, records, 1)
	assert.Equal(t, expected, records[0])
}
