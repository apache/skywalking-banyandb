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
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
)

func metricsChunk(hasMore bool, names ...string) *fodcv1.StreamMetricsRequest {
	req := &fodcv1.StreamMetricsRequest{Timestamp: timestamppb.Now(), HasMore: hasMore}
	for _, name := range names {
		req.Metrics = append(req.Metrics, &fodcv1.Metric{Name: name, Value: 1, Labels: map[string]string{"series": name}})
	}
	return req
}

// collectWhileStreaming runs the agent's metrics stream on service, starts one collection
// for that agent, and feeds chunks once the collection is waiting. It returns the metric
// names the collection delivered.
func collectWhileStreaming(t *testing.T, service *FODCService, testRegistry *registry.AgentRegistry, chunks ...*fodcv1.StreamMetricsRequest) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agentID, registerErr := testRegistry.RegisterAgent(ctx, registry.AgentIdentity{PodName: "liaison-0", Role: "liaison", ContainerNames: []string{"liaison"}})
	require.NoError(t, registerErr)

	stream := newMockStreamMetricsServer(metadata.NewIncomingContext(ctx, metadata.New(map[string]string{"agent_id": agentID})))
	streamDone := make(chan error, 1)
	go func() { streamDone <- service.StreamMetrics(stream) }()

	type result struct {
		err     error
		metrics []*metrics.AggregatedMetric
	}
	collected := make(chan result, 1)
	go func() {
		got, collectErr := service.metricsAggregator.GetLatestMetrics(ctx, &metrics.Filter{AgentIDs: []string{agentID}})
		collected <- result{metrics: got, err: collectErr}
	}()
	require.Eventually(t, func() bool { return service.metricsAggregator.ActiveCollections() == 1 }, 2*time.Second, 5*time.Millisecond)

	for _, chunk := range chunks {
		stream.AddRequest(chunk)
	}

	var got result
	select {
	case got = <-collected:
	case <-time.After(5 * time.Second):
		t.Fatal("collection did not complete")
	}
	require.NoError(t, got.err)
	names := make([]string, 0, len(got.metrics))
	for _, metric := range got.metrics {
		names = append(names, metric.Name)
	}
	sort.Strings(names)

	cancel()
	<-streamDone
	return names
}

func TestStreamMetrics_MergesChunkedReply(t *testing.T) {
	service, testRegistry := newTestService(t)

	names := collectWhileStreaming(t, service, testRegistry,
		metricsChunk(true, "a", "b"),
		metricsChunk(true, "c"),
		metricsChunk(false, "d"),
	)

	assert.Equal(t, []string{"a", "b", "c", "d"}, names, "every chunk of one reply must reach the collection")
}

func TestStreamMetrics_UnchunkedReplyFromOlderAgent(t *testing.T) {
	service, testRegistry := newTestService(t)

	names := collectWhileStreaming(t, service, testRegistry, metricsChunk(false, "a", "b"))

	assert.Equal(t, []string{"a", "b"}, names)
}

func TestStreamMetrics_DropsReplyOverPendingLimit(t *testing.T) {
	original := maxPendingMetricsBytes
	maxPendingMetricsBytes = 64
	t.Cleanup(func() { maxPendingMetricsBytes = original })
	service, testRegistry := newTestService(t)

	names := collectWhileStreaming(t, service, testRegistry,
		metricsChunk(true, "first-chunk-metric-with-a-long-name", "second-metric-with-a-long-name"),
		metricsChunk(true, "dropped"),
		metricsChunk(false, "last"),
	)

	assert.Empty(t, names, "an oversized reply is discarded, and the collection is answered instead of waiting out its timeout")
}
