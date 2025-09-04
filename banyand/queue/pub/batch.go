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
	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type writeStream struct {
	client    clusterv1.Service_SendClient
	ctxDoneCh <-chan struct{}
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
		if !bp.pub.isRequestAllowed(node) {
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
				errSend := bp.retrySend(ctx, stream.client, r, node)
				if errSend != nil {
					err = multierr.Append(err, fmt.Errorf("failed to send message to node %s: %w", node, errSend))
					// Record failure for circuit breaker
					bp.pub.recordFailure(node)
					return false
				}
				// Record success for circuit breaker
				bp.pub.recordSuccess(node)
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
		var client *client
		// nolint: contextcheck
		if func() bool {
			bp.pub.mu.RLock()
			defer bp.pub.mu.RUnlock()
			var ok bool
			client, ok = bp.pub.active[node]
			if !ok {
				err = multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
				return true
			}
			succeed, ce := bp.pub.checkWritable(node, topic)
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
		stream, errCreateStream := client.client.Send(streamCtx)
		if errCreateStream != nil {
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
		go func(s clusterv1.Service_SendClient, deferFn func(), bc chan batchEvent, curNode string) {
			defer func() {
				close(bc)
				deferFn()
			}()
			select {
			case <-ctx.Done():
				return
			default:
			}
			resp, errRecv := s.Recv()
			if errRecv != nil {
				if isFailoverError(errRecv) {
					bc <- batchEvent{n: curNode, e: common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, errRecv.Error())}
				}
				return
			}
			if resp == nil {
				return
			}
			if resp.Error == "" {
				return
			}
			if isFailoverStatus(resp.Status) {
				ce := common.NewErrorWithStatus(resp.Status, resp.Error)
				bc <- batchEvent{n: curNode, e: ce}
			}
		}(stream, deferFn, bp.f.events[len(bp.f.events)-1], nodeName)
	}
	return nil, err
}

func (bp *batchPublisher) Close() (cee map[string]*common.Error, err error) {
	for i := range bp.streams {
		err = multierr.Append(err, bp.streams[i].client.CloseSend())
	}
	for i := range bp.streams {
		<-bp.streams[i].ctxDoneCh
	}
	batchEvents := bp.f.get()
	if len(batchEvents) < 1 {
		return nil, err
	}
	if bp.pub.closer.AddRunning() {
		go func() {
			defer bp.pub.closer.Done()
			for n, e := range batchEvents {
				if bp.topic == nil {
					bp.pub.failover(n, e.e, data.TopicCommon)
					continue
				}
				bp.pub.failover(n, e.e, *bp.topic)
			}
		}()
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

	for attempt := 0; attempt <= defaultMaxRetries; attempt++ {
		// Create per-attempt timeout context
		attemptCtx, cancel := context.WithTimeout(ctx, defaultPerRequestTimeout)

		// Check if parent context is canceled or stream context is done
		select {
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		case <-stream.Context().Done():
			cancel()
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
			// Success
			return nil
		}

		lastErr = sendErr

		// Check if error is retryable
		if !isTransientError(sendErr) {
			// Non-transient error, don't retry
			return sendErr
		}

		// If this was the last attempt, don't sleep
		if attempt >= defaultMaxRetries {
			break
		}

		// Calculate backoff with jitter
		backoff := jitteredBackoff(defaultBackoffBase, defaultBackoffMax, attempt, defaultJitterFactor)

		// Sleep with backoff, but respect context cancellation
		select {
		case <-time.After(backoff):
			// Continue to next attempt
		case <-ctx.Done():
			return ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	// All retries exhausted
	return fmt.Errorf("retry exhausted for node %s after %d attempts, last error: %w", node, defaultMaxRetries+1, lastErr)
}
