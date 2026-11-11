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

package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

// chunkTestDescription is long so that a modest number of samples exceeds one chunk,
// the same way a real liaison's histogram descriptions inflate a time-window reply.
var chunkTestDescription = strings.Repeat("a verbose metric description ", 8)

// recordSamples fills fr with seriesCount series, each holding samplesPerSeries values
// stamped one second apart ending at end.
func recordSamples(t *testing.T, fr *flightrecorder.FlightRecorder, seriesCount, samplesPerSeries int, end time.Time) {
	t.Helper()
	rawMetrics := make([]metrics.RawMetric, 0, seriesCount)
	for idx := 0; idx < seriesCount; idx++ {
		rawMetrics = append(rawMetrics, metrics.RawMetric{
			Name: "banyandb_queue_pub_total_latency_bucket",
			Desc: chunkTestDescription,
			Type: "histogram",
			Labels: []metrics.Label{
				{Name: "series", Value: fmt.Sprintf("series-%06d", idx)},
				{Name: "topic", Value: "measure-write-with-a-long-topic-name"},
			},
			Value: float64(idx),
		})
	}
	require.NoError(t, fr.Update(rawMetrics))
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	start := end.Add(-time.Duration(samplesPerSeries-1) * time.Second)
	for sample := 1; sample < samplesPerSeries; sample++ {
		require.NoError(t, datasources[0].UpdateBatch(rawMetrics, start.Add(time.Duration(sample)*time.Second).Unix()))
	}
}

// assertChunkedReply checks that sent is one reply split into bounded chunks, and returns
// the number of metrics it carries.
func assertChunkedReply(t *testing.T, sent []*fodcv1.StreamMetricsRequest) int {
	t.Helper()
	require.NotEmpty(t, sent)
	total := 0
	for idx, req := range sent {
		assert.LessOrEqual(t, proto.Size(req), maxMetricsChunkBytes, "chunk %d exceeds the chunk budget", idx)
		assert.Equal(t, idx < len(sent)-1, req.GetHasMore(), "only the last chunk may clear has_more (chunk %d)", idx)
		total += len(req.GetMetrics())
	}
	return total
}

func sentMetricsRequests(stream *mockStreamMetricsClient) []*fodcv1.StreamMetricsRequest {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return append([]*fodcv1.StreamMetricsRequest(nil), stream.sentRequests...)
}

// newChunkTestClient returns a client with a flight recorder that holds every test sample and a
// mock metrics stream that records every reply chunk. The recorder allocates its whole capacity
// up front, so it is kept small.
func newChunkTestClient(t *testing.T) (*Client, *flightrecorder.FlightRecorder, *mockStreamMetricsClient) {
	t.Helper()
	fr := flightrecorder.NewFlightRecorder(16 << 20)
	pc := NewProxyClient("localhost:8080", "liaison", "192.168.1.1", []string{"liaison"}, nil, 5*time.Second, 10*time.Second, fr, nil, nil, nil, nil,
		initTestLogger(t))
	stream := newMockStreamMetricsClient(context.Background())
	pc.streamsMu.Lock()
	pc.metricsStream = stream
	pc.streamsMu.Unlock()
	return pc, fr, stream
}

// windowFilter covers the hour before end. fr.Update stamps the first sample with its own
// clock, which can tick past end, so the window ends a minute later.
func windowFilter(end time.Time) *MetricsRequestFilter {
	start, filterEnd := end.Add(-time.Hour), end.Add(time.Minute)
	return &MetricsRequestFilter{StartTime: &start, EndTime: &filterEnd}
}

func TestProxyClient_RetrieveAndSendMetrics_ChunksLargeTimeWindow(t *testing.T) {
	const seriesCount, samplesPerSeries = 400, 60
	pc, fr, stream := newChunkTestClient(t)
	end := time.Now()
	recordSamples(t, fr, seriesCount, samplesPerSeries, end)

	require.NoError(t, pc.RetrieveAndSendMetrics(context.Background(), windowFilter(end)))

	sent := sentMetricsRequests(stream)
	assert.Greater(t, len(sent), 1, "a reply over the chunk budget must be split")
	assert.Equal(t, seriesCount*samplesPerSeries, assertChunkedReply(t, sent))
}

func TestProxyClient_RetrieveAndSendMetrics_ChunksLargeLatest(t *testing.T) {
	const seriesCount = 20000
	pc, fr, stream := newChunkTestClient(t)
	recordSamples(t, fr, seriesCount, 1, time.Now())

	require.NoError(t, pc.RetrieveAndSendMetrics(context.Background(), nil))

	sent := sentMetricsRequests(stream)
	assert.Greater(t, len(sent), 1, "a reply over the chunk budget must be split")
	assert.Equal(t, seriesCount, assertChunkedReply(t, sent))
}

func TestProxyClient_RetrieveAndSendMetrics_SmallReplyIsOneMessage(t *testing.T) {
	pc, fr, stream := newChunkTestClient(t)
	end := time.Now()
	recordSamples(t, fr, 10, 3, end)

	require.NoError(t, pc.RetrieveAndSendMetrics(context.Background(), windowFilter(end)))

	sent := sentMetricsRequests(stream)
	require.Len(t, sent, 1)
	assert.False(t, sent[0].GetHasMore())
	assert.Len(t, sent[0].GetMetrics(), 30)
}

// A metric that alone exceeds the chunk budget cannot be sent within it, so it is left out
// and the rest of the reply still arrives.
func TestProxyClient_SendMetricsChunks_SkipsOversizedMetric(t *testing.T) {
	pc, _, stream := newChunkTestClient(t)
	protoMetrics := []*fodcv1.Metric{
		{Name: "before", Value: 1},
		{Name: "oversized", Description: strings.Repeat("x", maxMetricsChunkBytes), Value: 2},
		{Name: "after", Value: 3},
	}

	require.NoError(t, pc.sendMetricsChunks(stream, protoMetrics))

	sent := sentMetricsRequests(stream)
	assertChunkedReply(t, sent)
	var names []string
	for _, req := range sent {
		for _, metric := range req.GetMetrics() {
			names = append(names, metric.GetName())
		}
	}
	assert.Equal(t, []string{"before", "after"}, names)
}

// newStreamRestartClient returns a connected client whose registration stream is live and
// whose StreamMetrics calls are counted and answered by openStream.
func newStreamRestartClient(t *testing.T, openStream func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error)) *Client {
	t.Helper()
	pc := NewProxyClient("localhost:8080", "liaison", "192.168.1.1", []string{"liaison"}, nil, 5*time.Second, 10*time.Second,
		flightrecorder.NewFlightRecorder(1<<20), nil, nil, nil, nil, initTestLogger(t))
	pc.streamsMu.Lock()
	pc.client = &mockFODCServiceClient{streamMetricsFunc: openStream}
	pc.agentID = "agent-1"
	pc.registrationStream = newMockRegisterAgentClient(context.Background())
	pc.streamsMu.Unlock()
	return pc
}

func TestProxyClient_MetricsStreamError_RestartsOnlyMetricsStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var opened atomic.Int32
	replacement := newMockStreamMetricsClient(ctx)
	pc := newStreamRestartClient(t, func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		opened.Add(1)
		return replacement, nil
	})
	var reconnects atomic.Int32
	pc.reconnectFn = func(context.Context) { reconnects.Add(1) }

	failed := newMockStreamMetricsClient(ctx)
	failed.SetRecvError(status.Error(codes.ResourceExhausted, "trying to send message larger than max"))
	pc.streamsMu.Lock()
	pc.metricsStream = failed
	registration := pc.registrationStream
	pc.streamsMu.Unlock()

	pc.handleMetricsStream(ctx, failed)

	require.Eventually(t, func() bool {
		pc.streamsMu.RLock()
		defer pc.streamsMu.RUnlock()
		return pc.metricsStream == replacement
	}, 5*time.Second, 10*time.Millisecond, "the failed metrics stream should be replaced in place")
	assert.Equal(t, int32(1), opened.Load())
	assert.Zero(t, reconnects.Load(), "a healthy connection must not be torn down for one stream")
	pc.streamsMu.RLock()
	defer pc.streamsMu.RUnlock()
	assert.Same(t, registration, pc.registrationStream, "other streams must survive")
	assert.Equal(t, "agent-1", pc.agentID, "the agent keeps its identity")
}

func TestProxyClient_MetricsStreamError_FallsBackToReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := newStreamRestartClient(t, func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		return nil, errors.New("connection is closing")
	})
	var reconnects atomic.Int32
	pc.reconnectFn = func(context.Context) { reconnects.Add(1) }

	failed := newMockStreamMetricsClient(ctx)
	failed.SetRecvError(status.Error(codes.ResourceExhausted, "trying to send message larger than max"))
	pc.streamsMu.Lock()
	pc.metricsStream = failed
	pc.streamsMu.Unlock()

	pc.handleMetricsStream(ctx, failed)

	require.Eventually(t, func() bool { return reconnects.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"a metrics stream that cannot be reopened must fall back to a full reconnect")
}

// A full reconnect another stream asks for while the metrics stream is being reopened must
// still run: the restart rebuilds only the metrics stream, so skipping the request would leave
// the failed stream dead.
func TestProxyClient_MetricsStreamRestart_KeepsConcurrentReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opening := make(chan struct{})
	release := make(chan struct{})
	var openOnce sync.Once
	pc := newStreamRestartClient(t, func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		// The replacement stream ends at once, so the client reopens it again: only the first open signals.
		openOnce.Do(func() { close(opening) })
		<-release
		return newMockStreamMetricsClient(ctx), nil
	})
	var reconnects atomic.Int32
	pc.reconnectFn = func(context.Context) { reconnects.Add(1) }

	failed := newMockStreamMetricsClient(ctx)
	pc.streamsMu.Lock()
	pc.metricsStream = failed
	pc.streamsMu.Unlock()

	restarted := make(chan struct{})
	go func() {
		pc.restartMetricsStream(ctx, failed)
		close(restarted)
	}()
	<-opening
	// Another stream's receive loop fails while the metrics stream is reopening.
	pc.reconnect(ctx)
	close(release)
	<-restarted

	assert.Equal(t, int32(1), reconnects.Load(), "the reconnect requested during the restart must not be lost")
}

// A restart started its replacement stream's receive loop while still holding the slot. If that
// replacement fails at once, its own restart finds the slot busy and must wait for it rather
// than give up and leave the stream without a receive loop.
func TestProxyClient_MetricsStreamRestart_WaitsForBusySlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replacement := newMockStreamMetricsClient(ctx)
	pc := newStreamRestartClient(t, func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		return replacement, nil
	})
	pc.reconnectFn = func(context.Context) {}

	failed := newMockStreamMetricsClient(ctx)
	pc.streamsMu.Lock()
	pc.metricsStream = failed
	pc.streamsMu.Unlock()

	pc.reconnectCh <- struct{}{} // the earlier restart still holds the slot
	restarted := make(chan struct{})
	go func() {
		pc.restartMetricsStream(ctx, failed)
		close(restarted)
	}()
	require.Never(t, func() bool {
		select {
		case <-restarted:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 5*time.Millisecond, "the restart must wait for the busy slot instead of giving up")
	<-pc.reconnectCh
	<-restarted

	pc.streamsMu.RLock()
	defer pc.streamsMu.RUnlock()
	assert.Same(t, replacement, pc.metricsStream, "the failed stream must be reopened once the slot is free")
}

func TestProxyClient_MetricsStreamConnectionLost_Reconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var opened atomic.Int32
	pc := newStreamRestartClient(t, func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		opened.Add(1)
		return newMockStreamMetricsClient(ctx), nil
	})
	var reconnects atomic.Int32
	pc.reconnectFn = func(context.Context) { reconnects.Add(1) }

	failed := newMockStreamMetricsClient(ctx)
	failed.SetRecvError(status.Error(codes.Unavailable, "transport is closing"))
	pc.streamsMu.Lock()
	pc.metricsStream = failed
	pc.streamsMu.Unlock()

	pc.handleMetricsStream(ctx, failed)

	require.Eventually(t, func() bool { return reconnects.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"a lost connection must rebuild every stream, including the registration")
	assert.Zero(t, opened.Load(), "the metrics stream alone must not be reopened on a lost connection")
}
