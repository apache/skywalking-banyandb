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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
)

// replyingSender simulates a live agent: every RequestMetrics is answered
// asynchronously, the way a real agent answers the proxy's collect request over
// its gRPC stream.
type replyingSender struct {
	aggregator *Aggregator
	reg        *registry.AgentRegistry
	delay      time.Duration
	wg         sync.WaitGroup
}

func (s *replyingSender) RequestMetrics(agentID string, _ *time.Time, _ *time.Time) error {
	s.wg.Add(1)
	//panicdiag:allow-rawgo test-only agent stub; a panic here must fail the test loudly rather than be recovered and hidden
	go func() {
		defer s.wg.Done()
		time.Sleep(s.delay)
		agentInfo, err := s.reg.GetAgentByID(agentID)
		if err != nil {
			return
		}
		now := time.Now()
		req := createTestStreamMetricsRequest("banyandb_system_up_time", 1, nil, &now)
		_ = s.aggregator.ProcessMetricsFromAgent(context.Background(), agentID, agentInfo, req)
	}()
	return nil
}

// TestCollectMetricsFromAgents_ConcurrentScrapes reproduces the production failure:
// the OTel collector scrapes /metrics every 10s and Prometheus every 30s, so their
// collections regularly overlap. Every scrape must get every agent's metrics.
func TestCollectMetricsFromAgents_ConcurrentScrapes(t *testing.T) {
	initTestLogger(t)
	aggregator, testRegistry, _ := newTestAggregator(t)

	sender := &replyingSender{aggregator: aggregator, reg: testRegistry, delay: 200 * time.Millisecond}
	aggregator.SetGRPCService(sender)

	agentCount := 3
	for i := 0; i < agentCount; i++ {
		createTestAgent(t, testRegistry, "pod", "datanode-warm", nil)
	}

	const scrapers = 2
	results := make([][]*AggregatedMetric, scrapers)
	errs := make([]error, scrapers)

	var wg sync.WaitGroup
	for i := 0; i < scrapers; i++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test-only scrape driver; a panic here must fail the test loudly rather than be recovered and hidden
		go func(idx int) {
			defer wg.Done()
			// Stagger the second scrape so it lands while the first is still collecting.
			time.Sleep(time.Duration(idx) * 50 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			results[idx], errs[idx] = aggregator.GetLatestMetrics(ctx, nil)
		}(i)
	}
	wg.Wait()
	sender.wg.Wait()

	for i := 0; i < scrapers; i++ {
		require.NoError(t, errs[i])
		require.Len(t, results[i], agentCount,
			"scrape %d must observe all %d agents, got %d", i, agentCount, len(results[i]))
	}
	require.Equal(t, 0, aggregator.ActiveCollections(), "every subscription must be released")
}

// TestCollectMetricsFromAgents_DuplicateAgentID guards the per-collection subscription
// bookkeeping: only one subscription per agent is tracked for cleanup, so an agent named
// twice must be subscribed to only once or the extra subscription is never released.
func TestCollectMetricsFromAgents_DuplicateAgentID(t *testing.T) {
	initTestLogger(t)
	aggregator, testRegistry, _ := newTestAggregator(t)

	sender := &replyingSender{aggregator: aggregator, reg: testRegistry, delay: 20 * time.Millisecond}
	aggregator.SetGRPCService(sender)

	agentID := createTestAgent(t, testRegistry, "pod", "datanode-warm", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	collected, err := aggregator.CollectMetricsFromAgents(ctx, &Filter{AgentIDs: []string{agentID, agentID}})
	require.NoError(t, err)
	require.Len(t, collected, 1, "a repeated agent must be collected from once, not twice")

	sender.wg.Wait()
	require.Equal(t, 0, aggregator.ActiveCollections(), "every subscription must be released")
}

// TestProcessMetricsFromAgent_CancelledContextStillBroadcasts pins the broadcast down: a
// canceled context must not cut the fan-out short. A subscriber that cannot take the value
// (buffer already full) must not cost the other subscribers their data - they would each go
// on to wait out a full collection timeout for metrics that had already arrived.
func TestProcessMetricsFromAgent_CancelledContextStillBroadcasts(t *testing.T) {
	initTestLogger(t)
	aggregator, testRegistry, _ := newTestAggregator(t)

	agentID := createTestAgent(t, testRegistry, "pod", "datanode-warm", nil)
	agentInfo, err := testRegistry.GetAgentByID(agentID)
	require.NoError(t, err)

	// Subscriber "blocked" has a full buffer, so its send can never proceed.
	blockedSubID, blockedCh := aggregator.subscribe(agentID)
	blockedCh <- nil
	healthySubID, healthyCh := aggregator.subscribe(agentID)
	defer aggregator.unsubscribe(agentID, blockedSubID)
	defer aggregator.unsubscribe(agentID, healthySubID)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	now := time.Now()
	processErr := aggregator.ProcessMetricsFromAgent(ctx, agentID, agentInfo,
		createTestStreamMetricsRequest("banyandb_system_up_time", 1, nil, &now))
	require.ErrorIs(t, processErr, context.Canceled, "the canceled context must still be reported")

	select {
	case metrics := <-healthyCh:
		require.Len(t, metrics, 1)
	default:
		t.Fatal("healthy subscriber was starved because the broadcast stopped early")
	}
}
