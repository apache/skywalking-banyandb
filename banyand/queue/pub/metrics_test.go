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

// Label order: operation, group, remote_node, remote_role, remote_tier, error_type.
type errReasonCapturerImpl struct {
	byReason map[string]float64
}

func newErrReasonCapturer() *errReasonCapturerImpl {
	return &errReasonCapturerImpl{byReason: make(map[string]float64)}
}

func (c *errReasonCapturerImpl) Inc(delta float64, labels ...string) {
	// error_type is the last label (index 5 for totalErr)
	if len(labels) < 1 {
		return
	}
	errorType := labels[len(labels)-1]
	c.byReason[errorType] += delta
}

func (c *errReasonCapturerImpl) Delete(_ ...string) bool {
	return true
}

func (c *errReasonCapturerImpl) sum(reason string) float64 {
	return c.byReason[reason]
}

type noopHistogram struct{}

func (*noopHistogram) Observe(_ float64, _ ...string) {}
func (*noopHistogram) Delete(_ ...string) bool        { return true }

func newPubMetricsWithErrCapture(totalErr *errReasonCapturerImpl) *pubMetrics { //nolint:exhaustruct
	return &pubMetrics{
		totalStarted:       &countingCounter{},
		totalFinished:      &countingCounter{},
		totalLatency:       &noopHistogram{},
		totalErr:           totalErr,
		sentBytes:          &countingCounter{},
		totalBatchStarted:  &countingCounter{},
		totalBatchFinished: &countingCounter{},
		totalBatchLatency:  &noopHistogram{},
	}
}

func newPubWithConnMgrForMetrics(t *testing.T, pm *pubMetrics) *pub {
	t.Helper()
	p := &pub{ //nolint:exhaustruct
		handlers:  make(map[bus.Topic]schema.EventHandler),
		log:       logger.GetLogger("queue-pub-metrics-test"),
		metrics:   pm,
		closer:    run.NewCloser(1),
		nodeCache: make(map[string]nodeInfo),
	}
	p.connMgr = grpchelper.NewConnManager(grpchelper.ConnManagerConfig[*client]{
		Handler:        p,
		Logger:         p.log,
		RetryPolicy:    "",
		MaxRecvMsgSize: 4 << 20,
	})
	return p
}

// TestRetryMetrics verifies that retry-exhausted and error-type metrics are updated when
// retrySend observes retryable errors and eventually exhausts retries.
func TestRetryMetrics(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{ //nolint:exhaustruct
		metrics: &pubMetrics{
			totalStarted:  &countingCounter{},
			totalFinished: &countingCounter{},
			totalLatency:  &noopHistogram{},
			totalErr:      sendErrCap,
			sentBytes:     &countingCounter{},
		},
		nodeCache: make(map[string]nodeInfo),
	}

	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
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

	retryErr := bp.retrySend(ctx, client, req, nodeName)
	require.Error(t, retryErr)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonRetryExhausted))
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonNonTransient))
}

func topicPtr(t bus.Topic) *bus.Topic {
	return &t
}

func TestRetrySendNonTransientRecordsReason(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{ //nolint:exhaustruct
		metrics:   newPubMetricsWithErrCapture(sendErrCap),
		nodeCache: make(map[string]nodeInfo),
	}
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return status.Error(codes.InvalidArgument, "non-transient")
	})

	err := bp.retrySend(ctx, mockStream, &clusterv1.SendRequest{}, "n1")
	require.Error(t, err)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonNonTransient))
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonRetryExhausted))
}

func TestRetrySendCanceledRecordsReason(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{ //nolint:exhaustruct
		metrics:   newPubMetricsWithErrCapture(sendErrCap),
		nodeCache: make(map[string]nodeInfo),
	}
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := NewMockSendClient(context.Background())
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return status.Error(codes.Unavailable, "unavailable")
	})

	err := bp.retrySend(ctx, mockStream, &clusterv1.SendRequest{}, "n1")
	require.ErrorIs(t, err, context.Canceled)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonCanceled))
}

func TestRetrySendStreamCanceledRecordsReason(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	p := &pub{ //nolint:exhaustruct
		metrics:   newPubMetricsWithErrCapture(sendErrCap),
		nodeCache: make(map[string]nodeInfo),
	}
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := NewMockSendClient(streamCtx)

	err := bp.retrySend(context.Background(), mockStream, &clusterv1.SendRequest{}, "n1")
	require.ErrorIs(t, err, context.Canceled)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonStreamCanceled))
}

func TestListenBatchResponseRecordsRecvError(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return nil, status.Error(codes.Unavailable, "recv failed")
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group", nil)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonRecvError))
}

func TestListenBatchResponseRecvNonFailoverStillRecordsRecvError(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "bad")
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group", nil)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonRecvError))
}

func TestListenBatchResponseServerRejectedWithoutFailover(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return &clusterv1.SendResponse{
			Error:  "rejected",
			Status: modelv1.Status_STATUS_INTERNAL_ERROR,
		}, nil
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group", nil)

	require.Equal(t, float64(1), sendErrCap.sum(sendErrReasonServerRejected))
	require.Equal(t, float64(0), sendErrCap.sum(sendErrReasonRecvError))

	// Non-failover server rejections are now surfaced to the caller via batchEvent.
	select {
	case evt, ok := <-bc:
		require.True(t, ok, "expected a batchEvent for non-failover server rejection")
		require.Equal(t, "node-a", evt.n)
		require.NotNil(t, evt.e)
		require.Equal(t, modelv1.Status_STATUS_INTERNAL_ERROR, evt.e.Status())
	default:
		t.Fatal("expected batchEvent for server_rejected but channel was empty")
	}
}

func TestListenBatchResponseDiskFullSendsFailoverEvent(t *testing.T) {
	sendErrCap := newErrReasonCapturer()
	pm := newPubMetricsWithErrCapture(sendErrCap)
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return &clusterv1.SendResponse{
			Error:  "disk full",
			Status: modelv1.Status_STATUS_DISK_FULL,
		}, nil
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group", nil)

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

// TestPublishRecordsStartedAndFinished verifies that a successful retrySend increments
// totalStarted and totalFinished with matching label counts.
func TestPublishRecordsStartedAndFinished(t *testing.T) {
	started := &countingCounter{}
	finished := &countingCounter{}
	pm := &pubMetrics{ //nolint:exhaustruct
		totalStarted:  started,
		totalFinished: finished,
		totalLatency:  &noopHistogram{},
		totalErr:      newErrReasonCapturer(),
		sentBytes:     &countingCounter{},
	}
	p := newPubWithConnMgrForMetrics(t, pm)

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	var sendSucceededAt time.Time
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		sendSucceededAt = time.Now()
		return nil
	})

	doneCh := make(chan struct{})
	close(doneCh)

	const nodeName = "node-a"
	topic := data.TopicMeasureWrite

	bp := p.NewBatchPublisher(10 * time.Second).(*batchPublisher)
	bp.streams[nodeName] = writeStream{
		client:    mockStream,
		ctxDoneCh: doneCh,
	}

	msg := bus.NewMessageWithNode(1, nodeName, []byte("payload"))

	_, publishErr := bp.Publish(ctx, topic, msg)
	require.NoError(t, publishErr)

	require.Equal(t, float64(1), started.count, "totalStarted must be 1 on success")
	require.Equal(t, float64(1), finished.count, "totalFinished must be 1 on success")
	stream := bp.streams[nodeName]
	require.True(t, stream.firstFrameSent)
	require.False(t, stream.firstFrameAt.Before(sendSucceededAt), "batch open time must start only after the first frame is sent successfully")
}

type countingHistogram struct {
	values []float64
	labels [][]string
	mu     sync.Mutex
	count  int
}

func (h *countingHistogram) Observe(value float64, labels ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.values = append(h.values, value)
	h.labels = append(h.labels, append([]string(nil), labels...))
}

func (*countingHistogram) Delete(_ ...string) bool { return true }

func (h *countingHistogram) snapshot() ([]float64, [][]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	values := append([]float64(nil), h.values...)
	labels := make([][]string, len(h.labels))
	for idx := range h.labels {
		labels[idx] = append([]string(nil), h.labels[idx]...)
	}
	return values, labels
}

// TestListenBatchResponseCtxDoneTicksNoBatchFinished verifies that the ctx.Done() early-return
// path in listenBatchResponse does NOT increment total_batch_finished or total_batch_latency.
func TestListenBatchResponseCtxDoneTicksNoBatchFinished(t *testing.T) {
	batchFinished := &countingCounter{}
	batchLatency := &countingHistogram{}
	pm := &pubMetrics{ //nolint:exhaustruct
		totalStarted:       &countingCounter{},
		totalFinished:      &countingCounter{},
		totalLatency:       &noopHistogram{},
		totalErr:           newErrReasonCapturer(),
		sentBytes:          &countingCounter{},
		totalBatchStarted:  &countingCounter{},
		totalBatchFinished: batchFinished,
		totalBatchLatency:  batchLatency,
	}
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	// Cancel the context before calling listenBatchResponse — the ctx.Done() select fires immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := NewMockSendClient(context.Background())
	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group", nil)

	require.Equal(t, float64(0), batchFinished.count, "total_batch_finished must NOT be ticked on ctx.Done() early-return")
	require.Equal(t, 0, batchLatency.count, "total_batch_latency must NOT be observed on ctx.Done() early-return")
}

// TestListenBatchResponseSuccessTicksBatchFinished verifies that a successful single-response path
// increments total_batch_finished exactly once and observes total_batch_latency exactly once with group="".
func TestListenBatchResponseSuccessTicksBatchFinished(t *testing.T) {
	batchFinished := &countingCounter{}
	batchLatency := &countingHistogram{}
	pm := &pubMetrics{ //nolint:exhaustruct
		totalStarted:       &countingCounter{},
		totalFinished:      &countingCounter{},
		totalLatency:       &noopHistogram{},
		totalErr:           newErrReasonCapturer(),
		sentBytes:          &countingCounter{},
		totalBatchStarted:  &countingCounter{},
		totalBatchFinished: batchFinished,
		totalBatchLatency:  batchLatency,
	}
	p := newPubWithConnMgrForMetrics(t, pm)
	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	// Return a successful response (nil error, empty Error field → success path).
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return &clusterv1.SendResponse{}, nil
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group", nil)

	require.Equal(t, float64(1), batchFinished.count, "total_batch_finished must be 1 on success")
	require.Equal(t, 1, batchLatency.count, "total_batch_latency must be observed once on success")
}

func TestBatchTimingRecordsResponseBeforeSealOnce(t *testing.T) {
	timing := &batchTiming{}
	terminalAt := time.Unix(1_000, 0)

	duration, ready, recorded := timing.recordTerminal(terminalAt)
	require.True(t, recorded)
	require.False(t, ready)
	require.Zero(t, duration)

	duration, ready, sealed := timing.seal(terminalAt.Add(5 * time.Second))
	require.True(t, sealed)
	require.True(t, ready)
	require.Zero(t, duration, "a response received before sealing must have zero post-seal admission duration")

	_, ready, recorded = timing.recordTerminal(terminalAt.Add(10 * time.Second))
	require.False(t, recorded, "the first terminal outcome must win")
	require.False(t, ready, "admission duration must be emitted exactly once")
	_, ready, sealed = timing.seal(terminalAt.Add(20 * time.Second))
	require.False(t, sealed, "a batch target must be sealed exactly once")
	require.False(t, ready, "admission duration must be emitted exactly once")
}

func TestBatchLifecycleMetricsSeparateHeldOpenFromFastAdmission(t *testing.T) {
	openDuration := &countingHistogram{}
	admissionDuration := &countingHistogram{}
	p := &pub{ //nolint:exhaustruct
		metrics: &pubMetrics{ //nolint:exhaustruct
			batchOpenDuration:      openDuration,
			batchAdmissionDuration: admissionDuration,
		},
		nodeCache: map[string]nodeInfo{"node-a": {role: "data", tier: "hot"}},
	}
	bp := &batchPublisher{pub: p, topic: topicPtr(data.TopicMeasureWrite)}
	firstFrameAt := time.Unix(2_000, 0)
	sealedAt := firstFrameAt.Add(6 * time.Hour)
	stream := writeStream{ //nolint:exhaustruct
		firstFrameAt:   firstFrameAt,
		firstFrameSent: true,
		group:          "test-group",
		timing:         &batchTiming{},
	}

	bp.sealBatchStream("node-a", stream, sealedAt)
	bp.sealBatchStream("node-a", stream, sealedAt.Add(time.Second))
	require.True(t, bp.recordBatchTerminal("node-a", stream.group, stream.timing, sealedAt.Add(50*time.Millisecond)))
	require.False(t, bp.recordBatchTerminal("node-a", stream.group, stream.timing, sealedAt.Add(time.Second)))

	openValues, openLabels := openDuration.snapshot()
	require.Equal(t, []float64{21_600}, openValues, "the open phase must retain the long client-held duration")
	require.Len(t, openLabels, 1)
	require.Equal(t, "test-group", openLabels[0][1])
	require.Equal(t, "node-a", openLabels[0][2])
	require.Equal(t, "data", openLabels[0][3])
	require.Equal(t, "hot", openLabels[0][4])
	admissionValues, admissionLabels := admissionDuration.snapshot()
	require.Equal(t, []float64{0.05}, admissionValues, "a fast ACK must only contribute its short post-seal duration")
	require.Equal(t, openLabels, admissionLabels)
}

func TestBatchAdmissionTimeoutRecordsTerminalDuration(t *testing.T) {
	openDuration := &countingHistogram{}
	admissionDuration := &countingHistogram{}
	timeoutTotal := &countingCounter{}
	p := &pub{ //nolint:exhaustruct
		metrics: &pubMetrics{ //nolint:exhaustruct
			batchOpenDuration:          openDuration,
			batchAdmissionDuration:     admissionDuration,
			batchAdmissionTimeoutTotal: timeoutTotal,
		},
		nodeCache: make(map[string]nodeInfo),
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	bp := &batchPublisher{
		pub:     p,
		topic:   topicPtr(data.TopicMeasureWrite),
		timeout: 20 * time.Millisecond,
		streams: map[string]writeStream{
			"node-a": {
				client:         NewMockSendClient(streamCtx),
				ctxDoneCh:      streamCtx.Done(),
				cancel:         streamCancel,
				firstFrameAt:   time.Now().Add(-time.Second),
				firstFrameSent: true,
				group:          "test-group",
				timing:         &batchTiming{},
			},
		},
	}

	cee, closeErr := bp.Close()
	require.NoError(t, closeErr)
	require.Empty(t, cee)
	require.Equal(t, float64(1), timeoutTotal.count)
	openValues, _ := openDuration.snapshot()
	require.Len(t, openValues, 1)
	admissionValues, _ := admissionDuration.snapshot()
	require.Len(t, admissionValues, 1, "the timeout must also be the target's terminal admission outcome")
	require.Positive(t, admissionValues[0])
}

func TestBatchAdmissionTimeoutCountsOnlyPendingTargets(t *testing.T) {
	openDuration := &countingHistogram{}
	admissionDuration := &countingHistogram{}
	timeoutTotal := &countingCounter{}
	migrationOpenDuration := &countingHistogram{}
	migrationAdmissionDuration := &countingHistogram{}
	migrationTimeoutTotal := &countingCounter{}
	p := &pub{ //nolint:exhaustruct
		metrics: &pubMetrics{ //nolint:exhaustruct
			batchOpenDuration:          openDuration,
			batchAdmissionDuration:     admissionDuration,
			batchAdmissionTimeoutTotal: timeoutTotal,
		},
		migrationMetrics: &pubMigrationMetrics{ //nolint:exhaustruct
			batchOpenDuration:          migrationOpenDuration,
			batchAdmissionDuration:     migrationAdmissionDuration,
			batchAdmissionTimeoutTotal: migrationTimeoutTotal,
		},
		nodeCache: make(map[string]nodeInfo),
	}
	completedCtx, completedCancel := context.WithCancel(context.Background())
	completedTiming := &batchTiming{}
	_, _, recorded := completedTiming.recordTerminal(time.Now())
	require.True(t, recorded)
	completedCancel()
	pendingCtx, pendingCancel := context.WithCancel(context.Background())
	defer pendingCancel()
	firstFrameAt := time.Now().Add(-time.Second)
	bp := &batchPublisher{
		pub:     p,
		topic:   topicPtr(data.TopicMeasureWrite),
		timeout: 20 * time.Millisecond,
		streams: map[string]writeStream{
			"completed": {
				client:         NewMockSendClient(completedCtx),
				ctxDoneCh:      completedCtx.Done(),
				cancel:         completedCancel,
				firstFrameAt:   firstFrameAt,
				firstFrameSent: true,
				group:          "test-group",
				timing:         completedTiming,
			},
			"pending": {
				client:         NewMockSendClient(pendingCtx),
				ctxDoneCh:      pendingCtx.Done(),
				cancel:         pendingCancel,
				firstFrameAt:   firstFrameAt,
				firstFrameSent: true,
				group:          "test-group",
				timing:         &batchTiming{},
			},
		},
	}

	cee, closeErr := bp.Close()
	require.NoError(t, closeErr)
	require.Empty(t, cee)
	require.Equal(t, float64(1), timeoutTotal.count, "only the still-pending target should time out")
	require.Equal(t, timeoutTotal.count, migrationTimeoutTotal.count, "migration timeout count must mirror queue_pub")
	openValues, _ := openDuration.snapshot()
	migrationOpenValues, _ := migrationOpenDuration.snapshot()
	require.Len(t, openValues, 2, "open duration must be recorded once for every successfully opened target")
	require.Equal(t, openValues, migrationOpenValues)
	admissionValues, _ := admissionDuration.snapshot()
	migrationAdmissionValues, _ := migrationAdmissionDuration.snapshot()
	require.Len(t, admissionValues, 2, "every terminal outcome, including timeout, must record admission duration")
	require.Equal(t, admissionValues, migrationAdmissionValues)
	select {
	case <-pendingCtx.Done():
	default:
		t.Fatal("timed-out stream context was not canceled")
	}
}

func TestBatchCloseSealsEachTargetBeforeClosingItsStream(t *testing.T) {
	admissionDuration := &countingHistogram{}
	p := &pub{ //nolint:exhaustruct
		metrics: &pubMetrics{ //nolint:exhaustruct
			batchOpenDuration:      &countingHistogram{},
			batchAdmissionDuration: admissionDuration,
		},
		nodeCache: make(map[string]nodeInfo),
	}
	const (
		group      = "test-group"
		closeDelay = 20 * time.Millisecond
	)
	bp := &batchPublisher{ //nolint:exhaustruct
		pub:     p,
		topic:   topicPtr(data.TopicMeasureWrite),
		timeout: time.Second,
		streams: make(map[string]writeStream),
	}
	newStream := func(nodeName string) writeStream {
		streamCtx, streamCancel := context.WithCancel(context.Background())
		timing := &batchTiming{}
		mockStream := NewMockSendClient(streamCtx)
		mockStream.SetCloseSendFunc(func() error {
			time.Sleep(closeDelay)
			bp.recordBatchTerminal(nodeName, group, timing, time.Now())
			streamCancel()
			time.Sleep(closeDelay)
			return nil
		})
		return writeStream{ //nolint:exhaustruct
			client:         mockStream,
			ctxDoneCh:      streamCtx.Done(),
			cancel:         streamCancel,
			firstFrameAt:   time.Now().Add(-time.Second),
			firstFrameSent: true,
			group:          group,
			timing:         timing,
		}
	}
	bp.streams["node-a"] = newStream("node-a")
	bp.streams["node-b"] = newStream("node-b")

	cee, closeErr := bp.Close()
	require.NoError(t, closeErr)
	require.Empty(t, cee)
	admissionValues, _ := admissionDuration.snapshot()
	require.Len(t, admissionValues, 2)
	for _, duration := range admissionValues {
		require.GreaterOrEqual(t, duration, (closeDelay / 2).Seconds(), "each target's admission phase must begin before its own CloseSend call")
	}
}

func newCloseTriggeredResponseStream(ctx context.Context, response *clusterv1.SendResponse) (*MockSendClient, <-chan struct{}) {
	sealed := make(chan struct{})
	listenerStarted := make(chan struct{})
	mockStream := NewMockSendClient(ctx)
	mockStream.SetCloseSendFunc(func() error {
		close(sealed)
		return nil
	})
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		close(listenerStarted)
		<-sealed
		return response, nil
	})
	return mockStream, listenerStarted
}

func waitForBatchListener(t *testing.T, listenerStarted <-chan struct{}) {
	t.Helper()
	select {
	case <-listenerStarted:
	case <-time.After(time.Second):
		t.Fatal("batch response listener did not start")
	}
}

func TestBatchHeldOpenFastAckSeparatesDurations(t *testing.T) {
	legacyBatchLatency := &countingHistogram{}
	openDuration := &countingHistogram{}
	admissionDuration := &countingHistogram{}
	timeoutTotal := &countingCounter{}
	pm := newPubMetricsWithErrCapture(newErrReasonCapturer())
	pm.totalBatchLatency = legacyBatchLatency
	pm.batchOpenDuration = openDuration
	pm.batchAdmissionDuration = admissionDuration
	pm.batchAdmissionTimeoutTotal = timeoutTotal
	p := newPubWithConnMgrForMetrics(t, pm)
	t.Cleanup(p.GracefulStop)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	mockStream, listenerStarted := newCloseTriggeredResponseStream(streamCtx, &clusterv1.SendResponse{})
	const nodeName = "node-a"
	const group = "test-group"
	timing := &batchTiming{}
	eventCh := make(chan batchEvent, 1)
	firstFrameAt := time.Now().Add(-6 * time.Hour)
	bp := &batchPublisher{
		pub:     p,
		topic:   topicPtr(data.TopicMeasureWrite),
		timeout: time.Second,
		streams: map[string]writeStream{
			nodeName: {
				client:         mockStream,
				ctxDoneCh:      streamCtx.Done(),
				cancel:         streamCancel,
				batchStart:     firstFrameAt,
				firstFrameAt:   firstFrameAt,
				firstFrameSent: true,
				group:          group,
				timing:         timing,
			},
		},
		f: batchFuture{
			events:   []chan batchEvent{eventCh},
			errNodes: make(map[string]struct{}),
			errors:   make(map[string]batchEvent),
			l:        p.log,
		},
	}
	go bp.listenBatchResponse(streamCtx, mockStream, streamCancel, eventCh, nodeName, firstFrameAt, group, timing)
	waitForBatchListener(t, listenerStarted)

	cee, closeErr := bp.Close()
	require.NoError(t, closeErr)
	require.Empty(t, cee)
	require.Zero(t, timeoutTotal.count)
	openValues, _ := openDuration.snapshot()
	require.Len(t, openValues, 1)
	require.GreaterOrEqual(t, openValues[0], (6 * time.Hour).Seconds())
	admissionValues, _ := admissionDuration.snapshot()
	require.Len(t, admissionValues, 1)
	require.Less(t, admissionValues[0], time.Second.Seconds())
	legacyLatencyValues, _ := legacyBatchLatency.snapshot()
	require.Len(t, legacyLatencyValues, 1, "the legacy end-to-end batch latency must remain available")
}

func TestBatchPromptRejectionReturnsErrorWithoutAdmissionTimeout(t *testing.T) {
	batchFinished := &countingCounter{}
	legacyBatchLatency := &countingHistogram{}
	openDuration := &countingHistogram{}
	admissionDuration := &countingHistogram{}
	timeoutTotal := &countingCounter{}
	pm := newPubMetricsWithErrCapture(newErrReasonCapturer())
	pm.totalBatchFinished = batchFinished
	pm.totalBatchLatency = legacyBatchLatency
	pm.batchOpenDuration = openDuration
	pm.batchAdmissionDuration = admissionDuration
	pm.batchAdmissionTimeoutTotal = timeoutTotal
	p := newPubWithConnMgrForMetrics(t, pm)
	t.Cleanup(p.GracefulStop)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	mockStream, listenerStarted := newCloseTriggeredResponseStream(streamCtx, &clusterv1.SendResponse{
		Error:  "rejected",
		Status: modelv1.Status_STATUS_INTERNAL_ERROR,
	})

	const nodeName = "node-a"
	const group = "test-group"
	timing := &batchTiming{}
	eventCh := make(chan batchEvent, 1)
	bp := &batchPublisher{
		pub:     p,
		topic:   topicPtr(data.TopicMeasureWrite),
		timeout: 100 * time.Millisecond,
		streams: map[string]writeStream{
			nodeName: {
				client:         mockStream,
				ctxDoneCh:      streamCtx.Done(),
				cancel:         streamCancel,
				batchStart:     time.Now(),
				firstFrameAt:   time.Now().Add(-time.Second),
				firstFrameSent: true,
				group:          group,
				timing:         timing,
			},
		},
		f: batchFuture{
			events:   []chan batchEvent{eventCh},
			errNodes: make(map[string]struct{}),
			errors:   make(map[string]batchEvent),
			l:        p.log,
		},
	}
	go bp.listenBatchResponse(streamCtx, mockStream, streamCancel, eventCh, nodeName, bp.streams[nodeName].batchStart, group, timing)
	waitForBatchListener(t, listenerStarted)

	cee, closeErr := bp.Close()
	require.NoError(t, closeErr)
	require.Contains(t, cee, nodeName)
	require.Equal(t, modelv1.Status_STATUS_INTERNAL_ERROR, cee[nodeName].Status())
	require.Zero(t, timeoutTotal.count, "a prompt rejection must not be misclassified as an admission timeout")
	require.Equal(t, float64(1), batchFinished.count)
	legacyLatencyValues, _ := legacyBatchLatency.snapshot()
	require.Len(t, legacyLatencyValues, 1, "the existing total_batch_latency observation must be preserved")
	openValues, _ := openDuration.snapshot()
	require.Len(t, openValues, 1)
	admissionValues, _ := admissionDuration.snapshot()
	require.Len(t, admissionValues, 1)
}
