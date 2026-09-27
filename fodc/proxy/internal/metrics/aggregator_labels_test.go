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

package metrics

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// collectOne runs one collection for agentID in the background, pushes req through the
// real ProcessMetricsFromAgent once the collector has subscribed, and returns the result.
func collectOne(t *testing.T, aggregator *Aggregator, agentID string, req *fodcv1.StreamMetricsRequest) []*AggregatedMetric {
	t.Helper()
	agentInfo, err := aggregator.registry.GetAgentByID(agentID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got []*AggregatedMetric
	var collectErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, collectErr = aggregator.CollectMetricsFromAgents(ctx, &Filter{AgentIDs: []string{agentID}})
	}()
	require.Eventually(t, func() bool { return aggregator.ActiveCollections() == 1 }, time.Second, 5*time.Millisecond)
	require.NoError(t, aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo, req))
	<-done
	require.NoError(t, collectErr)
	require.Len(t, got, len(req.Metrics))
	return got
}

func TestProcessMetricsFromAgent_ReusesProtoLabelMap(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)
	agentID := createTestAgent(t, testRegistry, "hot-0", "data", map[string]string{"type": "hot", "pod_name": "ignored"})

	now := time.Now()
	req := createTestStreamMetricsRequest("banyandb_up", 1, map[string]string{"group": "sw_trace"}, &now)
	protoLabels := req.Metrics[0].Labels

	got := collectOne(t, aggregator, agentID, req)

	require.Equal(t, reflect.ValueOf(protoLabels).Pointer(), reflect.ValueOf(got[0].Labels).Pointer(),
		"the aggregated metric must reference the decoded proto's label map, not a copy")
	require.Equal(t, "hot", got[0].Labels["node_type"], "node labels are overlaid in place")
	require.NotContains(t, got[0].Labels, "node_pod_name", "pod_name is a first-class label and is never re-prefixed")
	require.Equal(t, "sw_trace", got[0].Labels["group"])
}

func TestProcessMetricsFromAgent_NilLabelsGetsNodeLabels(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)
	agentID := createTestAgent(t, testRegistry, "hot-0", "data", map[string]string{"type": "hot"})

	req := &fodcv1.StreamMetricsRequest{Metrics: []*fodcv1.Metric{{Name: "banyandb_up", Value: 1}}} // Labels == nil

	got := collectOne(t, aggregator, agentID, req)

	require.Equal(t, map[string]string{"node_type": "hot"}, got[0].Labels)
}

func TestProcessMetricsFromAgent_NilLabelsWithNothingToOverlayStaysNil(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)
	agentID := createTestAgent(t, testRegistry, "hot-0", "data", nil)

	req := &fodcv1.StreamMetricsRequest{Metrics: []*fodcv1.Metric{{Name: "banyandb_up", Value: 1}}}

	got := collectOne(t, aggregator, agentID, req)

	require.Nil(t, got[0].Labels, "no map is allocated when there is nothing to overlay")
}
