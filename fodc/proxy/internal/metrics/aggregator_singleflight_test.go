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
)

func newReplyingSetup(t *testing.T, agents int, delay time.Duration) (*Aggregator, *replyingSender) {
	t.Helper()
	aggregator, testRegistry, _ := newTestAggregator(t)
	sender := &replyingSender{aggregator: aggregator, reg: testRegistry, delay: delay}
	aggregator.SetGRPCService(sender)
	for i := 0; i < agents; i++ {
		createTestAgent(t, testRegistry, "pod", "datanode-warm", nil)
	}
	return aggregator, sender
}

// In production two scrapers (the OTel collector every 10s and Prometheus every 30s) hit
// /metrics within tens of milliseconds of each other every 30s. One collection must serve
// both: every agent is asked once and both scrapers read the same result.
func TestCollectMetricsFromAgents_OverlappingScrapesShareOneCollection(t *testing.T) {
	const agents = 3
	aggregator, sender := newReplyingSetup(t, agents, 200*time.Millisecond)

	results := make([][]*AggregatedMetric, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test-only scrape driver; a panic here must fail the test loudly rather than be recovered and hidden
		go func(idx int) {
			defer wg.Done()
			time.Sleep(time.Duration(idx) * 50 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			results[idx], errs[idx] = aggregator.GetLatestMetrics(ctx, nil)
		}(i)
	}
	wg.Wait()
	sender.wg.Wait()

	for i := 0; i < 2; i++ {
		require.NoError(t, errs[i])
		require.Len(t, results[i], agents)
	}
	require.EqualValues(t, agents, sender.requests.Load(), "each agent is asked once, not once per scraper")
	require.Same(t, results[0][0], results[1][0], "both scrapers read the same collection")
	require.Equal(t, len(results[0]), cap(results[0]), "a shared result must not carry spare capacity an append could write into")
	require.Equal(t, 0, aggregator.ActiveCollections())
}

func TestCollectMetricsFromAgents_SequentialScrapesCollectSeparately(t *testing.T) {
	const agents = 2
	aggregator, sender := newReplyingSetup(t, agents, 10*time.Millisecond)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		got, err := aggregator.GetLatestMetrics(ctx, nil)
		cancel()
		require.NoError(t, err)
		require.Len(t, got, agents)
	}
	sender.wg.Wait()
	require.EqualValues(t, 2*agents, sender.requests.Load(), "a scrape that starts after the previous one finished collects afresh")
}

func TestCollectMetricsFromAgents_DifferentFiltersAreNotCoalesced(t *testing.T) {
	aggregator, testRegistry, _ := newTestAggregator(t)
	sender := &replyingSender{aggregator: aggregator, reg: testRegistry, delay: 200 * time.Millisecond}
	aggregator.SetGRPCService(sender)
	createTestAgent(t, testRegistry, "hot-0", "data", nil)
	createTestAgent(t, testRegistry, "liaison-0", "liaison", nil)

	var wg sync.WaitGroup
	got := make([][]*AggregatedMetric, 2)
	for i, role := range []string{"data", "liaison"} {
		wg.Add(1)
		//panicdiag:allow-rawgo test-only scrape driver; a panic here must fail the test loudly rather than be recovered and hidden
		go func(idx int, role string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got[idx], _ = aggregator.GetLatestMetrics(ctx, &Filter{Role: role})
		}(i, role)
	}
	wg.Wait()
	sender.wg.Wait()
	require.Len(t, got[0], 1)
	require.Len(t, got[1], 1)
	require.EqualValues(t, 2, sender.requests.Load(), "one collection per distinct filter")
}

func TestCollectMetricsFromAgents_FollowerCancelDoesNotAffectLeader(t *testing.T) {
	const agents = 2
	aggregator, sender := newReplyingSetup(t, agents, 300*time.Millisecond)

	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer leaderCancel()
	var leaderGot []*AggregatedMetric
	var leaderErr error
	var wg sync.WaitGroup
	wg.Add(1)
	//panicdiag:allow-rawgo test-only scrape driver; a panic here must fail the test loudly rather than be recovered and hidden
	go func() {
		defer wg.Done()
		leaderGot, leaderErr = aggregator.GetLatestMetrics(leaderCtx, nil)
	}()
	require.Eventually(t, func() bool { return aggregator.ActiveCollections() == agents }, time.Second, 5*time.Millisecond)

	followerCtx, followerCancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, followerCancel)
	start := time.Now()
	_, followerErr := aggregator.GetLatestMetrics(followerCtx, nil)
	require.ErrorIs(t, followerErr, context.Canceled)
	require.Less(t, time.Since(start), 250*time.Millisecond, "a canceled follower returns at once instead of waiting for the round")

	wg.Wait()
	sender.wg.Wait()
	require.NoError(t, leaderErr)
	require.Len(t, leaderGot, agents)
}

func TestCollectMetricsFromAgents_LeaderCancelStillServesFollower(t *testing.T) {
	const agents = 2
	aggregator, sender := newReplyingSetup(t, agents, 300*time.Millisecond)

	leaderCtx, leaderCancel := context.WithCancel(context.Background())
	var leaderGot []*AggregatedMetric
	var leaderErr error
	var wg sync.WaitGroup
	wg.Add(1)
	//panicdiag:allow-rawgo test-only scrape driver; a panic here must fail the test loudly rather than be recovered and hidden
	go func() {
		defer wg.Done()
		leaderGot, leaderErr = aggregator.GetLatestMetrics(leaderCtx, nil)
	}()
	require.Eventually(t, func() bool { return aggregator.ActiveCollections() == agents }, time.Second, 5*time.Millisecond)

	followerCtx, followerCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer followerCancel()
	var followerGot []*AggregatedMetric
	var followerErr error
	wg.Add(1)
	//panicdiag:allow-rawgo test-only scrape driver; a panic here must fail the test loudly rather than be recovered and hidden
	go func() {
		defer wg.Done()
		followerGot, followerErr = aggregator.GetLatestMetrics(followerCtx, nil)
	}()
	time.Sleep(50 * time.Millisecond)
	leaderCancel() // the scraper that started the round goes away mid-collection

	wg.Wait()
	sender.wg.Wait()
	require.NoError(t, leaderErr, "the round is not aborted by its starter's cancellation")
	require.Len(t, leaderGot, agents)
	require.NoError(t, followerErr, "the round the leader started must still complete for everyone else")
	require.Len(t, followerGot, agents)
	require.EqualValues(t, agents, sender.requests.Load())
}
