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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	// errWriteRowSerialize marks a row the session could not serialize.
	errWriteRowSerialize = errors.New("failed to serialize the write request")
	// errWriteRowRejected marks a failure that concerns a single row and leaves the session usable.
	errWriteRowRejected = errors.New("the write request was rejected")
)

const (
	defaultWriteSealRows        = 4096
	defaultWriteSealBytes       = 8 << 20
	defaultWriteSealInterval    = 200 * time.Millisecond
	defaultWriteMaxInflight     = 4
	defaultWritePressureTimeout = 30 * time.Second
	defaultWriteRetryBackoff    = 100 * time.Millisecond
	defaultWriteRetryBackoffMax = 2 * time.Second
	// writeMemoryHighRetryAfter is the pause requested from clients while the
	// liaison's own memory protector reports high pressure.
	writeMemoryHighRetryAfter = 500 * time.Millisecond

	sealReasonRows     = "rows"
	sealReasonBytes    = "bytes"
	sealReasonInterval = "interval"
	sealReasonSwitch   = "switch"
	sealReasonClose    = "close"

	backpressureReasonInflight = "inflight"
	backpressureReasonMemory   = "memory"
)

// writeSessionConfig holds the knobs of a write session. Zero values select the defaults.
type writeSessionConfig struct {
	// sealRows seals a node batch once it holds this many rows.
	sealRows int
	// sealBytes seals a node batch once its marshaled rows reach this many bytes.
	sealBytes run.Bytes
	// sealInterval seals a node batch this long after its first row.
	sealInterval time.Duration
	// maxInflight bounds the sealed, unacknowledged batches per node.
	maxInflight int
	// pressureTimeout bounds how long a batch is retried while its node reports DISK_FULL or STATUS_SERVER_BUSY.
	pressureTimeout time.Duration
	// retryBackoff and retryBackoffMax shape the busy retry backoff.
	retryBackoff    time.Duration
	retryBackoffMax time.Duration
}

func (c writeSessionConfig) withDefaults() writeSessionConfig {
	if c.sealRows <= 0 {
		c.sealRows = defaultWriteSealRows
	}
	if c.sealBytes <= 0 {
		c.sealBytes = defaultWriteSealBytes
	}
	if c.sealInterval <= 0 {
		c.sealInterval = defaultWriteSealInterval
	}
	if c.maxInflight <= 0 {
		c.maxInflight = defaultWriteMaxInflight
	}
	if c.pressureTimeout <= 0 {
		c.pressureTimeout = defaultWritePressureTimeout
	}
	if c.retryBackoff <= 0 {
		c.retryBackoff = defaultWriteRetryBackoff
	}
	if c.retryBackoffMax <= 0 {
		c.retryBackoffMax = defaultWriteRetryBackoffMax
	}
	return c
}

// registerFlags binds the write session flags to c and sets their defaults.
func (c *writeSessionConfig) registerFlags(fs *run.FlagSet) {
	c.sealBytes = defaultWriteSealBytes
	fs.IntVar(&c.sealRows, "write-batch-seal-rows", defaultWriteSealRows,
		"seal a write stream's batch for a data node once it holds this many rows")
	fs.VarP(&c.sealBytes, "write-batch-seal-bytes", "", "seal a write stream's batch for a data node once its rows reach this size")
	fs.DurationVar(&c.sealInterval, "write-batch-seal-interval", defaultWriteSealInterval,
		"seal a write stream's batch for a data node this long after its first row")
	fs.IntVar(&c.maxInflight, "write-max-inflight-batches", defaultWriteMaxInflight,
		"the maximum sealed batches a write stream keeps unacknowledged per data node before it stops reading from the client")
	fs.DurationVar(&c.pressureTimeout, "write-pressure-timeout", defaultWritePressureTimeout,
		"how long a write batch is retried while its data node is full or under memory pressure before the stream fails with RESOURCE_EXHAUSTED")
}

// writeAck identifies one client message awaiting acknowledgement.
type writeAck struct {
	metadata *commonv1.Metadata
	version  uint64
}

// writeReply is the model-independent form of a WriteResponse.
type writeReply struct {
	metadata     *commonv1.Metadata
	version      uint64
	status       modelv1.Status
	retryAfterMs uint32
}

// writeRow is one message of a batch: the serialized internal request the session owns until the batch is acknowledged.
type writeRow struct {
	ack     writeAck
	payload []byte
}

type writeBatch struct {
	openedAt time.Time
	sealedAt time.Time
	timer    *time.Timer
	node     string
	group    string
	rows     []writeRow
	bytes    int64
	epoch    uint64
}

// writeSessionDeps is the per-model adapter of a write session.
type writeSessionDeps struct {
	// newPublisher returns a fresh publisher for one attempt of one sealed batch.
	newPublisher func() queue.BatchPublisher
	// send writes one response to the client. It is only called from the session's sender goroutine.
	send func(reply writeReply) error
	// memoryHigh reports whether the liaison's own memory protector is under high pressure.
	memoryHigh func() bool
	// release is called once for every row when its batch is finished, with the row's group.
	release func(group string)
	log     *logger.Logger
	metrics *metrics
	service string
	topic   bus.Topic
}

// writeSession batches the rows of one client Write stream per data node, acknowledges each sealed batch when its
// node has applied it, and sends the responses through a queue that never blocks the receive loop.
type writeSession struct {
	ctx       context.Context
	open      map[string]*writeBatch
	inflight  map[string]int
	notify    chan struct{}
	fatalCh   chan struct{}
	fatalErr  error
	replies   *replyQueue
	deps      writeSessionDeps
	cfg       writeSessionConfig
	wg        sync.WaitGroup
	mu        sync.Mutex
	busyUntil atomic.Int64
}

func newWriteSession(ctx context.Context, cfg writeSessionConfig, deps writeSessionDeps) *writeSession {
	s := &writeSession{
		ctx:      ctx,
		cfg:      cfg.withDefaults(),
		deps:     deps,
		open:     make(map[string]*writeBatch),
		inflight: make(map[string]int),
		notify:   make(chan struct{}),
		fatalCh:  make(chan struct{}),
	}
	s.replies = newReplyQueue(ctx, deps.send, deps.log)
	return s
}

// Err returns the session's terminal error, or nil while it is healthy.
func (s *writeSession) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fatalErr
}

// Fatal returns a channel closed when the session can no longer accept rows.
func (s *writeSession) Fatal() <-chan struct{} { return s.fatalCh }

// Reply queues an immediate response, such as a per-row rejection. It never blocks.
func (s *writeSession) Reply(reply writeReply) {
	reply.retryAfterMs = s.retryAfterMs()
	s.replies.push(reply)
}

func (s *writeSession) retryAfterMs() uint32 {
	var d time.Duration
	if until := s.busyUntil.Load(); until > 0 {
		d = time.Until(time.Unix(0, until))
	}
	if s.deps.memoryHigh != nil && s.deps.memoryHigh() {
		if d < writeMemoryHighRetryAfter {
			d = writeMemoryHighRetryAfter
		}
	}
	if d <= 0 {
		return 0
	}
	if ms := d.Milliseconds(); ms > 0 {
		return uint32(ms)
	}
	return 1
}

// Append adds one row to the open batch of node. The build callback serializes the row and is told whether the row
// opens a batch, in which case it must carry the metadata and tag spec. Append blocks while the node already has the
// maximum of sealed, unacknowledged batches, and returns the session's terminal error once it has failed.
func (s *writeSession) Append(node, group string, epoch uint64, ack writeAck, build func(first bool) ([]byte, error)) error {
	s.mu.Lock()
	waited := false
	for {
		if s.fatalErr != nil {
			err := s.fatalErr
			s.mu.Unlock()
			return err
		}
		if err := s.ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if b := s.open[node]; b != nil && b.epoch != epoch {
			s.sealLocked(b, sealReasonSwitch)
		}
		if s.inflight[node] < s.cfg.maxInflight {
			break
		}
		if !waited {
			waited = true
			s.observeBackpressure(backpressureReasonInflight)
		}
		wake := s.notify
		s.mu.Unlock()
		select {
		case <-wake:
		case <-s.fatalCh:
		case <-s.ctx.Done():
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	b := s.open[node]
	payload, err := build(b == nil)
	if err != nil {
		return fmt.Errorf("%w: %w", errWriteRowSerialize, err)
	}
	if b == nil {
		b = &writeBatch{node: node, group: group, epoch: epoch, openedAt: time.Now()}
		s.open[node] = b
		b.timer = time.AfterFunc(s.cfg.sealInterval, func() { s.sealIfOpen(b, sealReasonInterval) })
	}
	b.rows = append(b.rows, writeRow{ack: ack, payload: payload})
	b.bytes += int64(len(payload))
	switch {
	case len(b.rows) >= s.cfg.sealRows:
		s.sealLocked(b, sealReasonRows)
	case b.bytes >= int64(s.cfg.sealBytes):
		s.sealLocked(b, sealReasonBytes)
	}
	return nil
}

func (s *writeSession) sealIfOpen(b *writeBatch, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open[b.node] == b {
		s.sealLocked(b, reason)
	}
}

func (s *writeSession) sealLocked(b *writeBatch, reason string) {
	delete(s.open, b.node)
	if b.timer != nil {
		b.timer.Stop()
	}
	b.sealedAt = time.Now()
	s.inflight[b.node]++
	s.wg.Add(1)
	if m := s.deps.metrics; m != nil {
		m.writeBatchSealed.Inc(1, s.deps.service, reason)
	}
	run.Go(s.ctx, "write-batch", s.deps.log, func(context.Context) { s.runBatch(b) })
}

func (s *writeSession) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fatalErr != nil {
		return
	}
	s.fatalErr = err
	close(s.fatalCh)
	s.broadcastLocked()
}

func (s *writeSession) broadcastLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

// Finish seals every open batch, waits for every sealed batch to be acknowledged, flushes the response queue and
// returns the session's terminal error, if any. After a terminal error the rows that were never sent are reported
// as not written.
func (s *writeSession) Finish() error {
	s.mu.Lock()
	var discarded []*writeBatch
	for _, b := range s.open {
		if s.fatalErr != nil {
			delete(s.open, b.node)
			if b.timer != nil {
				b.timer.Stop()
			}
			discarded = append(discarded, b)
			continue
		}
		s.sealLocked(b, sealReasonClose)
	}
	s.mu.Unlock()
	for _, b := range discarded {
		s.finishBatch(b, modelv1.Status_STATUS_SERVER_BUSY)
	}
	s.wg.Wait()
	s.replies.closeAndWait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fatalErr
}

type batchOutcome struct {
	err        error
	status     modelv1.Status
	notApplied bool
}

func (s *writeSession) runBatch(b *writeBatch) {
	defer s.wg.Done()
	var firstBusy time.Time
	for attempt := 0; ; attempt++ {
		out := s.publishOnce(b)
		if !out.notApplied {
			s.finishBatch(b, out.status)
			return
		}
		if m := s.deps.metrics; m != nil {
			m.writeBatchBusyRetry.Inc(1, s.deps.service, out.status.String())
		}
		now := time.Now()
		if firstBusy.IsZero() {
			firstBusy = now
		}
		elapsed := now.Sub(firstBusy)
		if elapsed >= s.cfg.pressureTimeout {
			s.deps.log.Warn().Str("node", b.node).Stringer("status", out.status).Dur("elapsed", elapsed).
				Msg("the data node stayed unable to accept writes; failing the write stream")
			s.fail(status.Errorf(codes.ResourceExhausted, "data node %s is unable to accept writes (%s) for %s", b.node, out.status, elapsed))
			s.finishBatch(b, modelv1.Status_STATUS_SERVER_BUSY)
			return
		}
		wait := grpchelper.JitteredBackoff(s.cfg.retryBackoff, s.cfg.retryBackoffMax, attempt, grpchelper.DefaultJitterFactor)
		if remaining := s.cfg.pressureTimeout - elapsed; wait > remaining {
			wait = remaining
		}
		s.busyUntil.Store(now.Add(wait).UnixNano())
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-s.fatalCh:
			timer.Stop()
			s.finishBatch(b, modelv1.Status_STATUS_SERVER_BUSY)
			return
		case <-s.ctx.Done():
			timer.Stop()
			s.finishBatch(b, modelv1.Status_STATUS_SERVER_BUSY)
			return
		}
	}
}

// publishOnce sends the batch's rows through a fresh publisher and classifies the outcome. A batch is only reported
// as not applied when the node refused it before its write listener ran.
func (s *writeSession) publishOnce(b *writeBatch) batchOutcome {
	publisher := s.deps.newPublisher()
	var publishErr error
	failedAt := -1
	for i := range b.rows {
		msg := bus.NewBatchMessageWithNodeAndGroup(bus.MessageID(time.Now().UnixNano()), b.node, b.group, b.rows[i].payload)
		if _, err := publisher.Publish(s.ctx, s.deps.topic, msg); err != nil {
			publishErr, failedAt = err, i
			break
		}
	}
	cee, closeErr := publisher.Close()
	if publishErr != nil {
		var ce *common.Error
		if failedAt == 0 && errors.As(publishErr, &ce) && isNotAppliedStatus(ce.Status()) {
			return batchOutcome{notApplied: true, status: ce.Status(), err: publishErr}
		}
		s.deps.log.Error().Err(publishErr).Str("node", b.node).Int("rows", len(b.rows)).Msg("publishing a sealed write batch failed")
		return batchOutcome{status: modelv1.Status_STATUS_INTERNAL_ERROR, err: publishErr}
	}
	if ce := cee[b.node]; ce != nil {
		if isNotAppliedStatus(ce.Status()) {
			return batchOutcome{notApplied: true, status: ce.Status(), err: ce}
		}
		return batchOutcome{status: ce.Status(), err: ce}
	}
	if closeErr != nil {
		s.deps.log.Error().Err(closeErr).Str("node", b.node).Msg("failed to close the batch publisher")
		return batchOutcome{status: modelv1.Status_STATUS_INTERNAL_ERROR, err: closeErr}
	}
	return batchOutcome{status: modelv1.Status_STATUS_SUCCEED}
}

func isNotAppliedStatus(st modelv1.Status) bool {
	return st == modelv1.Status_STATUS_DISK_FULL || st == modelv1.Status_STATUS_SERVER_BUSY
}

// finishBatch queues one response per row, drops the batch's rows and, for a sealed batch, frees its in-flight slot.
func (s *writeSession) finishBatch(b *writeBatch, st modelv1.Status) {
	sealed := !b.sealedAt.IsZero()
	retryAfter := s.retryAfterMs()
	rows := b.rows
	b.rows = nil
	for i := range rows {
		s.replies.push(writeReply{metadata: rows[i].ack.metadata, version: rows[i].ack.version, status: st, retryAfterMs: retryAfter})
		if s.deps.release != nil {
			s.deps.release(rows[i].ack.metadata.GetGroup())
		}
	}
	if m := s.deps.metrics; m != nil {
		m.writeBatchFinished.Inc(1, s.deps.service, st.String())
		m.writeBatchRows.Inc(float64(len(rows)), s.deps.service)
		if sealed {
			m.writeBatchAckLatency.Observe(time.Since(b.sealedAt).Seconds(), s.deps.service)
		}
	}
	if !sealed {
		return
	}
	s.mu.Lock()
	s.inflight[b.node]--
	s.broadcastLocked()
	s.mu.Unlock()
}

func (s *writeSession) observeBackpressure(reason string) {
	if m := s.deps.metrics; m != nil {
		m.writeBackpressure.Inc(1, s.deps.service, reason)
	}
}

// replyQueue hands responses to a dedicated sender goroutine. push never blocks, so a client that does not read
// accumulates its responses here instead of stalling the receive loop.
type replyQueue struct {
	send  func(reply writeReply) error
	log   *logger.Logger
	cond  *sync.Cond
	done  chan struct{}
	items []writeReply
	mu    sync.Mutex
	// closed means no more pushes follow; dead means the stream refused a response and the rest are dropped.
	closed bool
	dead   bool
}

func newReplyQueue(ctx context.Context, send func(reply writeReply) error, log *logger.Logger) *replyQueue {
	q := &replyQueue{send: send, log: log, done: make(chan struct{})}
	q.cond = sync.NewCond(&q.mu)
	run.Go(ctx, "write-reply-sender", log, func(context.Context) { q.run() })
	return q
}

func (q *replyQueue) push(reply writeReply) {
	q.mu.Lock()
	if !q.dead {
		q.items = append(q.items, reply)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

func (q *replyQueue) run() {
	defer close(q.done)
	for {
		q.mu.Lock()
		for len(q.items) == 0 && !q.closed {
			q.cond.Wait()
		}
		if len(q.items) == 0 {
			q.mu.Unlock()
			return
		}
		batch := q.items
		q.items = nil
		q.mu.Unlock()
		for i := range batch {
			if err := q.send(batch[i]); err != nil {
				q.log.Debug().Err(err).Msg("failed to send a write response; dropping the rest")
				q.mu.Lock()
				q.dead = true
				q.items = nil
				q.mu.Unlock()
				break
			}
		}
	}
}

// closeAndWait flushes the queued responses and stops the sender goroutine.
func (q *replyQueue) closeAndWait() {
	q.mu.Lock()
	q.closed = true
	q.cond.Signal()
	q.mu.Unlock()
	<-q.done
}
