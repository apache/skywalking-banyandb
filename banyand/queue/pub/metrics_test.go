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

package pub

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type fakeSendClient struct {
	clusterv1.Service_SendClient
	ctx context.Context

	sendErrs []error
	sendIdx  int
}

func (f *fakeSendClient) Send(_ *clusterv1.SendRequest) error {
	if f.sendIdx >= len(f.sendErrs) {
		return nil
	}
	err := f.sendErrs[f.sendIdx]
	f.sendIdx++
	return err
}

func (f *fakeSendClient) Context() context.Context {
	return f.ctx
}

type countingCounter struct {
	count float64
}

func (c *countingCounter) Inc(delta float64, _ ...string) {
	c.count += delta
}

func (*countingCounter) Delete(_ ...string) bool {
	return true
}

// errReasonCapturer records send_err_total increments keyed by the `reason` label (third label).
type errReasonCapturer struct {
	byReason map[string]float64
	mu       sync.Mutex
}

func newErrReasonCapturer() *errReasonCapturer {
	return &errReasonCapturer{byReason: make(map[string]float64)}
}

func (c *errReasonCapturer) Inc(delta float64, labels ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(labels) < 3 {
		return
	}
	reason := labels[2]
	c.byReason[reason] += delta
}

func (c *errReasonCapturer) Delete(_ ...string) bool {
	return true
}

func (c *errReasonCapturer) sum(reason string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byReason[reason]
}

type noopGauge struct{}

func (*noopGauge) Set(_ float64, _ ...string) {}
func (*noopGauge) Add(_ float64, _ ...string) {}
func (*noopGauge) Delete(_ ...string) bool    { return true }

type valGauge struct {
	mu  sync.Mutex
	val float64
}

func (g *valGauge) Add(delta float64, _ ...string) {
	g.mu.Lock()
	g.val += delta
	g.mu.Unlock()
}

func (g *valGauge) Set(v float64, _ ...string) {
	g.mu.Lock()
	g.val = v
	g.mu.Unlock()
}

func (*valGauge) Delete(_ ...string) bool { return true }

func (g *valGauge) value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.val
}

// inflightKeyedGauge tracks inflight_requests Add deltas per (topic, node) label pair.
type inflightKeyedGauge struct {
	vals map[string]float64
	mu   sync.Mutex
}

func newInflightKeyedGauge() *inflightKeyedGauge {
	return &inflightKeyedGauge{vals: make(map[string]float64)}
}

func inflightReqKey(topic, node string) string {
	return strings.Join([]string{topic, node}, "|")
}

func (g *inflightKeyedGauge) Add(delta float64, labels ...string) {
	if len(labels) < 2 {
		return
	}
	k := inflightReqKey(labels[0], labels[1])
	g.mu.Lock()
	g.vals[k] += delta
	g.mu.Unlock()
}

func (*inflightKeyedGauge) Set(_ float64, _ ...string) {}

func (*inflightKeyedGauge) Delete(_ ...string) bool {
	return true
}

func (g *inflightKeyedGauge) net(topic, node string) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.vals[inflightReqKey(topic, node)]
}

type noopHistogram struct{}

func (*noopHistogram) Observe(_ float64, _ ...string) {}
func (*noopHistogram) Delete(_ ...string) bool        { return true }

func newPubMetricsWithErrCapture(sendErr *errReasonCapturer) *pubMetrics {
	return &pubMetrics{
		sendSuccessTotal:    &countingCounter{},
		sendErrTotal:        sendErr,
		sendBytesTotal:      &countingCounter{},
		sendDurationSeconds: &noopHistogram{},
		sendRetryAttempts:   &countingCounter{},
		sendRetryExhausted:  &countingCounter{},
		sendBackoffSeconds:  &countingCounter{},
		inflightStreams:     &noopGauge{},
		inflightRequests:    &noopGauge{},
	}
}

func newPubWithConnMgrForMetrics(t *testing.T, pm *pubMetrics) *pub {
	t.Helper()
	p := &pub{
		handlers: make(map[bus.Topic]schema.EventHandler),
		log:      logger.GetLogger("queue-pub-metrics-test"),
		metrics:  pm,
		closer:   run.NewCloser(1),
	}
	p.connMgr = grpchelper.NewConnManager(grpchelper.ConnManagerConfig[*client]{
		Handler:        p,
		Logger:         p.log,
		RetryPolicy:    "",
		MaxRecvMsgSize: 4 << 20,
	})
	return p
}

// TestRetryMetrics ensures that retry-related metrics are updated when retrySend
// observes retryable errors and eventually exhausts retries.
func TestRetryMetrics(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{
		metrics: &pubMetrics{
			sendRetryAttempts:   &countingCounter{},
			sendRetryExhausted:  &countingCounter{},
			sendErrTotal:        sendErrCap,
			sendBackoffSeconds:  &countingCounter{},
			sendSuccessTotal:    &countingCounter{},
			sendBytesTotal:      &countingCounter{},
			sendDurationSeconds: &noopHistogram{},
			inflightStreams:     &noopGauge{},
			inflightRequests:    &noopGauge{},
		},
	}

	bp := &batchPublisher{
		pub: p,
	}

	ctx := context.Background()
	client := &fakeSendClient{
		sendErrs: []error{
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
		},
		ctx: ctx,
	}

	req := &clusterv1.SendRequest{
		Topic: "test-topic",
		Body:  []byte("payload"),
	}

	const nodeName = "test-node"
	const topicStr = "test-topic"

	retryErr := bp.retrySend(ctx, client, req, nodeName, topicStr)
	require.Error(t, retryErr)

	retries := p.metrics.sendRetryAttempts.(*countingCounter).count
	require.Equal(t, float64(4), retries, "one retry counter per transient failure before exhaustion")

	exhausted := p.metrics.sendRetryExhausted.(*countingCounter).count
	require.Equal(t, float64(1), exhausted)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonRetryExhausted))
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonNonTransient))

	backoff := p.metrics.sendBackoffSeconds.(*countingCounter).count
	require.Greater(t, backoff, float64(0), "backoff should be recorded between retry attempts")
}

func TestRetrySendNonTransientRecordsReason(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{metrics: newPubMetricsWithErrCapture(sendErrCap)}
	bp := &batchPublisher{pub: p}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return status.Error(codes.InvalidArgument, "non-transient")
	})

	err := bp.retrySend(ctx, mockStream, &clusterv1.SendRequest{}, "n1", "t1")
	require.Error(t, err)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonNonTransient))
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonRetryExhausted))
	require.Equal(t, float64(0), p.metrics.sendRetryAttempts.(*countingCounter).count)
}

func TestRetrySendCanceledRecordsReason(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{metrics: newPubMetricsWithErrCapture(sendErrCap)}
	bp := &batchPublisher{pub: p}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := NewMockSendClient(context.Background())
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return status.Error(codes.Unavailable, "unavailable")
	})

	err := bp.retrySend(ctx, mockStream, &clusterv1.SendRequest{}, "n1", "t1")
	require.ErrorIs(t, err, context.Canceled)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonCanceled))
}

func TestRetrySendStreamCanceledRecordsReason(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{metrics: newPubMetricsWithErrCapture(sendErrCap)}
	bp := &batchPublisher{pub: p}

	streamCtx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := NewMockSendClient(streamCtx)

	err := bp.retrySend(context.Background(), mockStream, &clusterv1.SendRequest{}, "n1", "t1")
	require.ErrorIs(t, err, context.Canceled)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonStreamCanceled))
}

func TestListenBatchResponseRecordsRecvError(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{pub: p}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return nil, status.Error(codes.Unavailable, "recv failed")
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", "topic-a")

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonRecvError))
}

func TestListenBatchResponseRecvNonFailoverStillRecordsRecvError(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{pub: p}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad")
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", "topic-a")

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonRecvError))
}

func TestListenBatchResponseServerRejectedWithoutFailover(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{pub: p}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return &clusterv1.SendResponse{
			Error:  "rejected",
			Status: modelv1.Status_STATUS_INTERNAL_ERROR,
		}, nil
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", "topic-a")

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonServerRejected))
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonRecvError))

	// listenBatchResponse always closes bc in defer; a closed empty channel yields (zero, false).
	evt, ok := <-bc
	if ok {
		t.Fatalf("unexpected failover event: %+v", evt)
	}
}

func TestListenBatchResponseDiskFullSendsFailoverEvent(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{pub: p}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return &clusterv1.SendResponse{
			Error:  "disk full",
			Status: modelv1.Status_STATUS_DISK_FULL,
		}, nil
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", "topic-a")

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonServerRejected))

	select {
	case evt := <-bc:
		require.Equal(t, "node-a", evt.n)
		require.NotNil(t, evt.e)
		require.Equal(t, modelv1.Status_STATUS_DISK_FULL, evt.e.Status())
	default:
		t.Fatal("expected failover batchEvent on disk full response")
	}
}

func TestCloseDecrementsInflightStreams(t *testing.T) {
	streamGauge := &valGauge{}
	pm := &pubMetrics{
		sendSuccessTotal:    &countingCounter{},
		sendErrTotal:        newErrReasonCapturer(),
		sendBytesTotal:      &countingCounter{},
		sendDurationSeconds: &noopHistogram{},
		sendRetryAttempts:   &countingCounter{},
		sendRetryExhausted:  &countingCounter{},
		sendBackoffSeconds:  &countingCounter{},
		inflightStreams:     streamGauge,
		inflightRequests:    &noopGauge{},
	}
	p := newPubWithConnMgrForMetrics(t, pm)

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	doneCh := make(chan struct{})
	close(doneCh)

	bp := p.NewBatchPublisher(time.Second).(*batchPublisher)
	bp.streams["node-x"] = writeStream{
		client:    mockStream,
		ctxDoneCh: doneCh,
	}

	streamGauge.Add(1, "node-x")

	_, closeErr := bp.Close()
	require.NoError(t, closeErr)
	require.Equal(t, float64(0), streamGauge.value(), "Close should decrement inflight_streams once per open stream")
}

// TestPublishInflightRequestsBalancedWhenStreamPreexists checks that Publish increments then decrements
// inflight_requests (via defer) when reusing an existing stream, without requiring GetClient / new stream setup.
func TestPublishInflightRequestsBalancedWhenStreamPreexists(t *testing.T) {
	inflightReq := newInflightKeyedGauge()
	sendErrCap := newErrReasonCapturer()
	attempts := &countingCounter{}
	pm := &pubMetrics{
		sendSuccessTotal:    attempts,
		sendErrTotal:        sendErrCap,
		sendBytesTotal:      &countingCounter{},
		sendDurationSeconds: &noopHistogram{},
		sendRetryAttempts:   &countingCounter{},
		sendRetryExhausted:  &countingCounter{},
		sendBackoffSeconds:  &countingCounter{},
		inflightStreams:     &noopGauge{},
		inflightRequests:    inflightReq,
	}
	p := newPubWithConnMgrForMetrics(t, pm)

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return nil
	})

	doneCh := make(chan struct{})
	close(doneCh)

	const nodeName = "node-a"
	topic := data.TopicMeasureWrite
	topicStr := topic.String()

	bp := p.NewBatchPublisher(10 * time.Second).(*batchPublisher)
	bp.streams[nodeName] = writeStream{
		client:    mockStream,
		ctxDoneCh: doneCh,
	}

	msg := bus.NewMessageWithNode(1, nodeName, []byte("payload"))

	_, publishErr := bp.Publish(ctx, topic, msg)
	require.NoError(t, publishErr)

	require.Equal(t, float64(1), attempts.count, "successful retrySend should record send_success_total")
	require.Equal(t, float64(0), inflightReq.net(topicStr, nodeName),
		"inflight_requests defer must balance +1/-1 for topic/node")
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonRetryExhausted))
}
