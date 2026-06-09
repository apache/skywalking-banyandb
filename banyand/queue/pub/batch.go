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
	"fmt"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	apidata "github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	defaultMaxRetries        = 3
	defaultPerRequestTimeout = 2 * time.Second
	defaultBackoffBase       = 500 * time.Millisecond
	defaultBackoffMax        = 30 * time.Second

	// Local send-side failures (before the frame leaves the publisher).
	sendErrReasonNonTransient   = "non_transient"
	sendErrReasonCanceled       = "canceled"
	sendErrReasonStreamCanceled = "stream_canceled"
	sendErrReasonRetryExhausted = "retry_exhausted"
	// Remote-side failures (observed after the frame was written to the stream).
	sendErrReasonRecvError      = "recv_error"      // s.Recv returned an error (connection/protocol layer).
	sendErrReasonServerRejected = "server_rejected" // Server responded with a non-empty Error (includes failover statuses).
	// Failures specific to the non-batched publish path (query/control operations).
	sendErrReasonSendError    = "send_error"    // Opening the stream or writing the first frame failed.
	sendErrReasonDecodeError  = "decode_error"  // The response body could not be decoded.
	sendErrReasonInvalidTopic = "invalid_topic" // No response codec is registered for the topic.
)

type writeStream struct {
	client         clusterv1.Service_SendClient
	ctxDoneCh      <-chan struct{}
	firstFrameSent bool // false until the first frame is sent; the first frame carries the sender_* identity labels
}

type batchPublisher struct {
	pub         *pub
	streams     map[string]writeStream
	topic       *bus.Topic
	failedNodes map[string]*common.Error
	f           batchFuture
	timeout     time.Duration
}

// NewBatchPublisher returns a new batch publisher.
func (p *pub) NewBatchPublisher(timeout time.Duration) queue.BatchPublisher {
	return &batchPublisher{
		pub:     p,
		streams: make(map[string]writeStream),
		timeout: timeout,
		f:       batchFuture{errNodes: make(map[string]struct{}), errors: make(map[string]batchEvent), l: p.log},
	}
}

func (bp *batchPublisher) Publish(ctx context.Context, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	if bp.topic == nil {
		bp.topic = &topic
	}
	var err error
	for _, m := range messages {
		r, errM2R := messageToRequest(topic, m)
		if errM2R != nil {
			err = multierr.Append(err, fmt.Errorf("failed to marshal message %T: %w", m, errM2R))
			continue
		}
		node := m.Node()

		// Check circuit breaker before attempting send
		if !bp.pub.connMgr.IsRequestAllowed(node) {
			err = multierr.Append(err, fmt.Errorf("circuit breaker open for node %s", node))
			continue
		}

		sendData := func() (success bool) {
			if stream, ok := bp.streams[node]; ok {
				defer func() {
					if !success {
						delete(bp.streams, node)
					}
				}()
				select {
				case <-ctx.Done():
					return false
				case <-stream.client.Context().Done():
					return false
				default:
				}
				// Stamp sender identity on the first frame of each stream.
				if !stream.firstFrameSent {
					r.SenderNode = bp.pub.selfNode
					r.SenderRole = bp.pub.selfRole
					r.SenderTier = bp.pub.selfTier
				} else {
					r.SenderNode = ""
					r.SenderRole = ""
					r.SenderTier = ""
				}
				errSend := bp.retrySend(ctx, stream.client, r, node)
				if errSend != nil {
					err = multierr.Append(err, fmt.Errorf("failed to send message to node %s: %w", node, errSend))
					// Record failure for circuit breaker (only for transient/internal errors)
					bp.pub.connMgr.RecordFailure(node, errSend)
					return false
				}
				if !stream.firstFrameSent {
					ws := bp.streams[node]
					ws.firstFrameSent = true
					bp.streams[node] = ws
				}
				// Record success for circuit breaker
				bp.pub.connMgr.RecordSuccess(node)
				return true
			}
			return false
		}
		if sendData() {
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if bp.failedNodes != nil {
			if ce := bp.failedNodes[node]; ce != nil {
				err = multierr.Append(err, ce)
			}
			continue
		}
		var nodeClient *client
		// nolint: contextcheck
		if func() bool {
			var clientOK bool
			nodeClient, clientOK = bp.pub.connMgr.GetClient(node)
			if !clientOK {
				err = multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
				return true
			}
			succeed, ce := bp.pub.checkWritable(ctx, node, topic)
			if succeed {
				return false
			}
			if bp.failedNodes == nil {
				bp.failedNodes = make(map[string]*common.Error)
			}
			bp.failedNodes[node] = ce
			err = multierr.Append(err, ce)
			return true
		}() {
			continue
		}

		streamCtx, cancel := context.WithTimeout(ctx, bp.timeout)
		// this assignment is for getting around the go vet lint
		deferFn := cancel
		stream, errCreateStream := nodeClient.client.Send(streamCtx)
		if errCreateStream != nil {
			// Release the timeout context when Send() fails; otherwise listenBatchResponse,
			// which normally invokes deferFn on exit, is never spawned and streamCtx leaks
			// until bp.timeout expires, accumulating timer goroutines under repeated failures.
			cancel()
			err = multierr.Append(err, fmt.Errorf("failed to get stream for node %s: %w", node, errCreateStream))
			continue
		}
		bp.streams[node] = writeStream{
			client:    stream,
			ctxDoneCh: streamCtx.Done(),
		}
		bp.f.events = append(bp.f.events, make(chan batchEvent))
		_ = sendData()
		nodeName := node
		recvStream := stream
		recvDeferFn := deferFn
		recvBC := bp.f.events[len(bp.f.events)-1]
		run.Go(ctx, "batch-stream-recv", bp.pub.log, func(runCtx context.Context) {
			bp.listenBatchResponse(runCtx, recvStream, recvDeferFn, recvBC, nodeName)
		})
	}
	return nil, err
}

func (bp *batchPublisher) hasMetrics() bool {
	return bp.pub != nil && bp.pub.metrics != nil
}

// listenBatchResponse receives the server response and records failover events and end-to-end failure metrics.
func (bp *batchPublisher) listenBatchResponse(ctx context.Context, s clusterv1.Service_SendClient, deferFn func(), bc chan batchEvent, curNode string) {
	defer func() {
		close(bc)
		deferFn()
	}()
	select {
	case <-ctx.Done():
		return
	default:
	}

	var topic bus.Topic
	if bp.topic != nil {
		topic = *bp.topic
	}
	operation := apidata.OperationOf(topic)
	var info nodeInfo
	if bp.pub != nil {
		info = bp.pub.getNodeInfo(curNode)
	}

	resp, errRecv := s.Recv()
	if errRecv != nil {
		if bp.hasMetrics() {
			bp.pub.metrics.totalErr.Inc(1, operation, "", curNode, info.role, info.tier, sendErrReasonRecvError)
		}
		if grpchelper.IsFailoverError(errRecv) {
			// Record circuit breaker failure before creating failover event
			bp.pub.connMgr.RecordFailure(curNode, errRecv)
			select {
			case bc <- batchEvent{n: curNode, e: common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, errRecv.Error())}:
			case <-ctx.Done():
			}
		}
		return
	}
	if resp == nil || resp.Error == "" || resp.Status == modelv1.Status_STATUS_SUCCEED {
		return
	}
	if bp.hasMetrics() {
		bp.pub.metrics.totalErr.Inc(1, operation, "", curNode, info.role, info.tier, sendErrReasonServerRejected)
	}
	ce := common.NewErrorWithStatus(resp.Status, resp.Error)
	// Only failover statuses trigger circuit-breaker accounting; other server-side
	// rejections (e.g. invalid argument) are surfaced to the caller but do not count
	// toward node health.
	if isFailoverStatus(resp.Status) {
		bp.pub.connMgr.RecordFailure(curNode, ce)
	}
	// Always surface a batchEvent for any non-empty resp.Error so that Close() exposes
	// the rejection to the caller.
	select {
	case bc <- batchEvent{n: curNode, e: ce}:
	case <-ctx.Done():
	}
}

func (bp *batchPublisher) Close() (cee map[string]*common.Error, err error) {
	for nodeName := range bp.streams {
		stream := bp.streams[nodeName]
		err = multierr.Append(err, stream.client.CloseSend())
	}
	for i := range bp.streams {
		<-bp.streams[i].ctxDoneCh
	}
	batchEvents := bp.f.get()
	if len(batchEvents) < 1 {
		return nil, err
	}
	if bp.pub.closer.AddRunning() {
		run.Go(context.Background(), "batch-failover", bp.pub.log, func(ctx context.Context) {
			defer bp.pub.closer.Done()
			for n, e := range batchEvents {
				// Record circuit breaker failure before failover
				bp.pub.connMgr.RecordFailure(n, e.e)
				if bp.topic == nil {
					bp.pub.failover(ctx, n, e.e, apidata.TopicCommon)
					continue
				}
				bp.pub.failover(ctx, n, e.e, *bp.topic)
			}
		})
	}
	cee = make(map[string]*common.Error, len(batchEvents))
	for n, be := range batchEvents {
		cee[n] = be.e
	}
	return cee, err
}

type batchEvent struct {
	e *common.Error
	n string
}

type batchFuture struct {
	errNodes map[string]struct{}
	errors   map[string]batchEvent
	l        *logger.Logger
	events   []chan batchEvent
}

func (b *batchFuture) get() map[string]batchEvent {
	var wg sync.WaitGroup
	var mux sync.Mutex
	wg.Add(len(b.events))
	for _, e := range b.events {
		go func(e chan batchEvent, mux *sync.Mutex) {
			defer wg.Done()
			for evt := range e {
				func() {
					mux.Lock()
					defer mux.Unlock()
					b.l.Error().Str("err_msg", evt.e.Error()).Str("code", modelv1.Status_name[int32(evt.e.Status())]).Msgf("node %s returns error", evt.n)
					b.errors[evt.n] = evt
				}()
			}
		}(e, &mux)
	}
	wg.Wait()
	mux.Lock()
	defer mux.Unlock()
	if len(b.errors) < 1 {
		return nil
	}
	result := make(map[string]batchEvent, len(b.errors))
	for k, v := range b.errors {
		result[k] = v
	}
	return result
}

func isFailoverStatus(s modelv1.Status) bool {
	return s == modelv1.Status_STATUS_DISK_FULL
}

// retrySend implements bounded retries for client streaming sends with exponential backoff and jitter.
func (bp *batchPublisher) retrySend(ctx context.Context, stream clusterv1.Service_SendClient, r *clusterv1.SendRequest, node string) error {
	var lastErr error
	start := time.Now()

	var topic bus.Topic
	if bp.topic != nil {
		topic = *bp.topic
	}
	operation := apidata.OperationOf(topic)
	var info nodeInfo
	if bp.pub != nil {
		info = bp.pub.getNodeInfo(node)
	}
	group := r.GetGroup()

	observeLatency := func() {
		if bp.hasMetrics() {
			bp.pub.metrics.totalLatency.Observe(time.Since(start).Seconds(), operation, group, node, info.role, info.tier)
		}
	}

	for attempt := 0; attempt <= defaultMaxRetries; attempt++ {
		// Create per-attempt timeout context
		attemptCtx, cancel := context.WithTimeout(ctx, defaultPerRequestTimeout)

		// Check if parent context is canceled or stream context is done
		select {
		case <-ctx.Done():
			cancel()
			if bp.hasMetrics() {
				bp.pub.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonCanceled)
			}
			observeLatency()
			return ctx.Err()
		case <-stream.Context().Done():
			cancel()
			if bp.hasMetrics() {
				bp.pub.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonStreamCanceled)
			}
			observeLatency()
			return stream.Context().Err()
		case <-attemptCtx.Done():
			cancel()
			if attempt == 0 {
				// First attempt timed out before we could even try
				lastErr = attemptCtx.Err()
				continue
			}
		default:
		}

		// Attempt to send
		sendErr := stream.Send(r)
		cancel()

		if sendErr == nil {
			if bp.hasMetrics() {
				bp.pub.metrics.totalStarted.Inc(1, operation, group, node, info.role, info.tier)
				bp.pub.metrics.totalFinished.Inc(1, operation, group, node, info.role, info.tier)
			}
			// Success writing to the local stream; end-to-end ack is observed in listenBatchResponse.
			observeLatency()
			return nil
		}

		lastErr = sendErr

		// Check if error is retryable
		if !grpchelper.IsTransientError(sendErr) {
			// Non-transient error, don't retry
			if bp.hasMetrics() {
				bp.pub.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonNonTransient)
			}
			observeLatency()
			return sendErr
		}

		// If this was the last attempt, don't sleep
		if attempt >= defaultMaxRetries {
			break
		}

		// Calculate backoff with jitter
		backoff := grpchelper.JitteredBackoff(defaultBackoffBase, defaultBackoffMax, attempt, grpchelper.DefaultJitterFactor)

		// Sleep with backoff, but respect context cancellation
		select {
		case <-time.After(backoff):
			// Continue to next attempt
		case <-ctx.Done():
			if bp.hasMetrics() {
				bp.pub.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonCanceled)
			}
			observeLatency()
			return ctx.Err()
		case <-stream.Context().Done():
			if bp.hasMetrics() {
				bp.pub.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonStreamCanceled)
			}
			observeLatency()
			return stream.Context().Err()
		}
	}

	// All retries exhausted
	if bp.hasMetrics() {
		bp.pub.metrics.totalErr.Inc(1, operation, group, node, info.role, info.tier, sendErrReasonRetryExhausted)
	}
	observeLatency()
	return fmt.Errorf("retry exhausted for node %s after %d attempts, last error: %w", node, defaultMaxRetries+1, lastErr)
}
