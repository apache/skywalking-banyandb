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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// rowReplayMaxInflight bounds how many row-replay batches may be sent but not
// yet confirmed at once. A depth of 2 lets one batch's receiver-side write
// overlap with the building and sending of the next batch (double buffering)
// while capping the extra sender/receiver memory and write contention.
const rowReplayMaxInflight = 2

// nodeReplayError reports a failed row-replay batch. It distinguishes a global
// send/confirm failure (err: the whole publish/close failed, or a row could not
// be built or routed) from per-node delivery failures (cee: the receiver
// rejected rows on specific nodes, keyed by node). At least one is set whenever
// this error is returned; newNodeReplayError yields nil when neither is.
type nodeReplayError struct {
	err error
	cee map[string]*common.Error
}

func (e *nodeReplayError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return fmt.Sprintf("%d node error(s)", len(e.cee))
}

func (e *nodeReplayError) Unwrap() error {
	return e.err
}

// newNodeReplayError reduces a confirmed batch's result to an error: nil when
// the batch fully succeeded (no global error and no per-node errors), otherwise
// a *nodeReplayError carrying whichever failures occurred.
func newNodeReplayError(cee map[string]*common.Error, err error) error {
	if err == nil && len(cee) == 0 {
		return nil
	}
	return &nodeReplayError{cee: cee, err: err}
}

// recordReplayNodeErrors records any per-node delivery errors carried by err
// into progress so they surface in the migration report. It is a no-op for a
// global-only error.
func recordReplayNodeErrors(progress *Progress, group string, err error) {
	var nre *nodeReplayError
	if errors.As(err, &nre) && len(nre.cee) > 0 {
		progress.RecordRowReplayNodeErrors(group, nre.cee)
	}
}

// confirmPipeline bounds the number of in-flight (sent but unconfirmed)
// row-replay batches. Each batch is confirmed on its own client-streaming
// publisher in a background goroutine that delivers its error (nil on success)
// on a channel; the pipeline keeps those channels in FIFO order so
// confirmations are observed in send order. It is driven by a single goroutine
// (the part's replay loop) and needs no internal locking.
type confirmPipeline struct {
	inflight []chan error
	depth    int
	// waited accumulates, over the pipeline's lifetime, the wall time the driving
	// goroutine spent blocked on confirmations (the in-flight bound in add plus
	// drain). Since that goroutine is either building or blocked here, it measures
	// how long the sender was starved waiting on the receiver.
	waited time.Duration
}

func newConfirmPipeline() *confirmPipeline {
	return &confirmPipeline{depth: rowReplayMaxInflight}
}

// add registers a batch confirmation that will be delivered on ch. When the
// pipeline is already full it first waits for the oldest in-flight confirmation
// so no more than depth batches are ever outstanding, returning that awaited
// error (a non-nil error means an earlier batch must abort the part).
func (p *confirmPipeline) add(ch chan error) error {
	var awaited error
	if len(p.inflight) >= p.depth {
		start := time.Now()
		awaited = <-p.inflight[0]
		p.waited += time.Since(start)
		p.inflight = p.inflight[1:]
	}
	p.inflight = append(p.inflight, ch)
	return awaited
}

// drain waits for every remaining in-flight confirmation and returns the first
// failing error (or nil when all succeeded). It is safe to call more than once;
// a drained pipeline is empty.
func (p *confirmPipeline) drain() error {
	var first error
	for _, ch := range p.inflight {
		start := time.Now()
		out := <-ch
		p.waited += time.Since(start)
		if first == nil && out != nil {
			first = out
		}
	}
	p.inflight = nil
	return first
}

// batchSender buffers row-replay messages and, once a batch fills, sends it on a
// client-streaming publisher and confirms it asynchronously through a bounded
// confirmPipeline. Each batch is published on its own publisher (rotated in
// immediately) and confirmed in the background, so one batch's receiver-side
// write overlaps with building and sending the next. It owns the whole publisher
// lifecycle (open, rotate, close) so the per-data-type replayers only build
// messages and enqueue them. batchSize and timeout are supplied by each data
// type. A sender is driven by a single goroutine; only the buffer is
// mutex-guarded, while each background confirmation touches just its own
// captured publisher and channel.
type batchSender struct {
	client    queue.Client
	publisher queue.BatchPublisher
	pipeline  *confirmPipeline
	batch     []bus.Message
	topic     bus.Topic
	timeout   time.Duration
	batchSize int
	mu        sync.Mutex
}

// newBatchSender builds a sender for one data type. batchSize and timeout are
// per-type knobs each replayer (measure/stream/trace) supplies from its own
// constants; they coincide at 2000/30s today, which is why unparam is silenced.
//
//nolint:unparam
func newBatchSender(client queue.Client, topic bus.Topic, batchSize int, timeout time.Duration) *batchSender {
	return &batchSender{
		client:    client,
		publisher: client.NewBatchPublisher(timeout),
		pipeline:  newConfirmPipeline(),
		topic:     topic,
		batch:     make([]bus.Message, 0, batchSize),
		timeout:   timeout,
		batchSize: batchSize,
	}
}

// enqueue buffers msg and, when the batch is full, sends and asynchronously
// confirms it. The returned error is non-nil only when the in-flight bound
// forced it to wait on (and surface) an earlier batch's failure.
func (s *batchSender) enqueue(ctx context.Context, msg bus.Message) error {
	s.mu.Lock()
	s.batch = append(s.batch, msg)
	full := len(s.batch) >= s.batchSize
	s.mu.Unlock()
	if full {
		return s.flush(ctx)
	}
	return nil
}

// flush sends the buffered batch on the current publisher, rotates in a fresh
// publisher for the next batch, and confirms the sent batch (closing its stream
// to collect the per-node delivery result) in the background. The returned error
// is non-nil only when the in-flight bound made it wait on (and surface) an
// earlier batch's result. It is a no-op when the buffer is empty.
func (s *batchSender) flush(ctx context.Context) error {
	pending := s.take()
	if len(pending) == 0 {
		return nil
	}
	_, pubErr := s.publisher.Publish(ctx, s.topic, pending...)
	pub := s.publisher
	s.publisher = s.client.NewBatchPublisher(s.timeout)
	ch := make(chan error, 1)
	go func() {
		// The pipeline blocks in add/drain until exactly one value arrives on ch, so
		// the send must happen on every path: a deferred send (with recover)
		// guarantees it even if pub.Close panics, turning a would-be deadlock into a
		// surfaced error.
		var out error
		defer func() {
			if rec := recover(); rec != nil {
				out = fmt.Errorf("row-replay batch confirm panicked: %v", rec)
			}
			ch <- out
		}()
		cee, closeErr := pub.Close()
		err := pubErr
		if err == nil {
			err = closeErr
		}
		out = newNodeReplayError(cee, err)
	}()
	return s.pipeline.add(ch)
}

// drain waits for every in-flight batch confirmation and returns the first
// failure (or nil when all succeeded).
func (s *batchSender) drain() error {
	return s.pipeline.drain()
}

// rowCursor is the subset of a dump iterator the replay driver needs; the emit
// closure reads the current row itself. *dump.Iterator[T] satisfies it for any T.
type rowCursor interface {
	Next() bool
	Err() error
	Position() dump.Position
}

// replay drives one part's row-replay: it pulls rows from cur, sends each through
// emit (which builds the message and enqueues it on this sender), flushes the
// trailing batch, then drains the pipeline so the part is reported durable only
// once every batch is confirmed. counter (when non-nil) counts completed parts,
// and a per-part build_send vs confirm_wait timing line is logged on success. The
// returned error is non-nil when any row build/route, iteration, or batch
// confirmation failed; the caller maps that to its progress bookkeeping.
func (s *batchSender) replay(ctx context.Context, l *logger.Logger, group, part string, counter *uint64,
	cur rowCursor, emit func() error,
) (int, error) {
	start := time.Now()
	waited0 := s.pipeline.waited
	warnAbort := func(rows int, err error) {
		pos := cur.Position()
		l.Warn().Err(err).
			Str("group", group).Str("part", part).
			Int("block_idx", pos.BlockIdx).Int("row_idx", pos.RowIdx).
			Int("rows_published", rows).
			Msg("row-replay aborted mid-part; will retry on resume")
	}
	rowCount := 0
	var failure error
	for cur.Next() {
		if err := emit(); err != nil {
			warnAbort(rowCount, err)
			failure = err
			break
		}
		rowCount++
	}
	if failure == nil {
		if iterErr := cur.Err(); iterErr != nil {
			warnAbort(rowCount, iterErr)
			failure = iterErr
		} else {
			failure = s.flush(ctx)
		}
	}
	// Wait for every in-flight batch confirmation before returning so the part is
	// only reported durable once all of its rows are acknowledged.
	if drained := s.drain(); failure == nil {
		failure = drained
	}
	if failure != nil {
		// Drop the failed part's un-flushed residual rows (an abort skips the
		// trailing flush) so a later close() cannot resurrect rows the caller was
		// told were not delivered; the whole part is re-replayed on resume.
		s.discard()
		return rowCount, failure
	}
	if counter != nil {
		atomic.AddUint64(counter, 1)
	}
	logRowReplayTiming(l, group, part, rowCount, time.Since(start), s.pipeline.waited-waited0)
	return rowCount, nil
}

// logRowReplayTiming emits one info line per replayed part splitting its wall
// time into build_send (sender CPU: decode, route, marshal, publish) and
// confirm_wait (time the sender was blocked waiting on the receiver). A high
// confirm_wait points at the receiver; a high build_send points at the sender.
func logRowReplayTiming(l *logger.Logger, group, part string, rows int, total, wait time.Duration) {
	if rows == 0 {
		return
	}
	buildSend := total - wait
	if buildSend < 0 {
		buildSend = 0
	}
	l.Info().
		Str("group", group).
		Str("part", part).
		Int("rows", rows).
		Dur("total", total).
		Dur("build_send", buildSend).
		Dur("confirm_wait", wait).
		Msg("row-replay part timing")
}

// close waits for any outstanding confirmations and closes the idle publisher.
// By construction the buffer is empty here: every replay flushes its trailing
// batch on success or discards it on failure, so close never needs to send.
func (s *batchSender) close() (map[string]*common.Error, error) {
	drained := s.drain()
	cee, closeErr := s.publisher.Close()
	if drained != nil {
		var nre *nodeReplayError
		if errors.As(drained, &nre) {
			return nre.cee, nre.err
		}
		return nil, drained
	}
	return cee, closeErr
}

// take atomically swaps out the buffered batch, returning nil when empty.
func (s *batchSender) take() []bus.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.batch) == 0 {
		return nil
	}
	pending := s.batch
	s.batch = make([]bus.Message, 0, s.batchSize)
	return pending
}

// discard drops any buffered rows not yet flushed, abandoning the residual of a
// failed part so it is never sent. It is take() with the rows thrown away.
func (s *batchSender) discard() {
	s.take()
}
