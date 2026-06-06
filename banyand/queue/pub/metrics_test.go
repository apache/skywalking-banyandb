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
		totalStarted:  &countingCounter{},
		totalFinished: &countingCounter{},
		totalLatency:  &noopHistogram{},
		totalErr:      totalErr,
		sentBytes:     &countingCounter{},
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
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a")

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
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a")

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
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a")

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
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a")

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
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
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
}
