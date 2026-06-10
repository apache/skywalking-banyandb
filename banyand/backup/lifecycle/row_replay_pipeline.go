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
	"runtime/debug"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// rowReplayMaxInflight bounds how many row-replay batches may be sent but not
// yet confirmed at once. A depth of 2 lets one batch's receiver-side write
// overlap with the building and sending of the next batch (double buffering)
// while capping the extra sender/receiver memory and write contention.
const rowReplayMaxInflight = 2

// rowReplayMaxBatchRows and rowReplayMaxBatchBytes bound a row-replay batch by
// row count and by total in-flight marshaled bytes. A batch flushes when either
// cap is reached, so the normal stream of small rows is cut by the row count
// (unchanged throughput) while a run of unusually large rows is cut by the byte
// budget — pinning peak marshal memory regardless of body-size distribution and
// preventing a cluster of large bodies from ballooning one batch. Both are
// overridable via lifecycle flags; the defaults need no configuration.
var (
	rowReplayMaxBatchRows  = 2000
	rowReplayMaxBatchBytes = run.Bytes(32 << 20)
)

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

// recordNodeErrors appends one node-scoped MigrationError per failing node so
// per-node delivery/close errors surface in the migration report.
func recordNodeErrors(progress *Progress, group, sourceStage, targetStage, catalog string, cee map[string]*common.Error) {
	for nodeID, ce := range cee {
		if ce == nil {
			continue
		}
		progress.AddMigrationError(MigrationError{
			SourceStage: sourceStage, TargetStage: targetStage, Group: group,
			Catalog: catalog, Scope: scopeNode, Node: nodeID, Error: ce.Error(),
		})
	}
}

// recordReplayNodeErrors records any per-node delivery errors carried by err. It
// is a no-op for a global-only error.
func recordReplayNodeErrors(progress *Progress, group, sourceStage, targetStage, catalog string, err error) {
	var nre *nodeReplayError
	if errors.As(err, &nre) && len(nre.cee) > 0 {
		recordNodeErrors(progress, group, sourceStage, targetStage, catalog, nre.cee)
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
// type. A sender is driven by a single goroutine (the part's replay loop): only
// it touches the buffer, and each background confirmation touches just its own
// captured publisher and channel, so no buffer locking is needed.
type batchSender struct {
	client    queue.Client
	publisher queue.BatchPublisher
	pipeline  *confirmPipeline
	pool      *marshalBufferPool
	batch     []bus.Message
	lent      [][]byte
	// skippedDetail locates a bounded sample of skipped series (part, seriesID,
	// reason) so the migration report can point an operator at the source data,
	// not just a count. Capped at maxSkipDetail to bound memory.
	skippedDetail []skipError
	topic         bus.Topic
	timeout       time.Duration
	maxRows       int
	maxBytes      int
	inflightBytes int
	// Flush-reason tallies: which cap forced each batch out. A flush triggered by
	// both caps at once is attributed to bytes (the memory-protecting cap that
	// fires early in the big-body cluster). tailFlushes counts the trailing
	// end-of-part flush that neither cap triggered.
	rowLimitFlushes  int
	byteLimitFlushes int
	tailFlushes      int
	// skippedRows counts rows dropped because their series could not be resolved
	// (errSkipSeries: a part block referencing a series absent from the segment's
	// sidx that could not be rebuilt from the part's columns). The part still
	// completes; this surfaces how much a source-data gap dropped instead of
	// aborting the whole migration.
	skippedRows int
}

// maxSkipDetail bounds how many distinct skipped-series locations are retained
// for the report; the full count still accrues in skippedRows.
const maxSkipDetail = 200

// recordSkip tallies one skipped series and, up to the cap, retains its location.
func (s *batchSender) recordSkip(err error) {
	s.skippedRows++
	if c := asSkipError(err); c != nil && len(s.skippedDetail) < maxSkipDetail {
		s.skippedDetail = append(s.skippedDetail, *c)
	}
}

// newBatchSender builds a sender for one data type. maxRows and maxBytes bound a
// batch by row count and total in-flight marshaled bytes; timeout is the
// publisher's per-batch deadline. Each replayer (measure/stream/trace) supplies
// these from the shared row-replay knobs.
func newBatchSender(client queue.Client, topic bus.Topic, maxRows, maxBytes int, timeout time.Duration) *batchSender {
	return &batchSender{
		client:    client,
		publisher: client.NewBatchPublisher(timeout),
		pipeline:  newConfirmPipeline(),
		pool:      newMarshalBufferPool(maxBytes),
		topic:     topic,
		batch:     make([]bus.Message, 0, maxRows),
		timeout:   timeout,
		maxRows:   maxRows,
		maxBytes:  maxBytes,
	}
}

// enqueueMarshaled marshals m into a size-class-pooled buffer and enqueues it as
// a []byte message, so the body is serialized once here instead of allocating a
// fresh []byte in the shared publish path. The borrowed buffer is tracked in lent
// and returned to the pool by flush once Publish has put every body on the wire
// (gRPC SendMsg is synchronous), keeping reuse safe. The buffer's capacity feeds
// the byte budget. sizeHint sizes the borrow: the caller passes a per-block size
// (one proto.Size per series instead of per row, since rows of a block share a
// schema), and MarshalAppend grows the buffer on the rare cross-class row, so the
// hint only affects reuse efficiency, never correctness.
func (s *batchSender) enqueueMarshaled(ctx context.Context, nodeID, group string, m proto.Message, sizeHint int) error {
	buf, err := proto.MarshalOptions{}.MarshalAppend(s.pool.borrow(sizeHint), m)
	if err != nil {
		s.pool.put(buf)
		return fmt.Errorf("marshal row-replay message: %w", err)
	}
	s.lent = append(s.lent, buf)
	// Carry the group explicitly: the body is opaque marshaled bytes, so the
	// publisher metrics path cannot recover the group from it (unlike the
	// proto-message path), and the queue metrics would otherwise lose the label.
	msg := bus.NewBatchMessageWithNodeAndGroup(bus.MessageID(time.Now().UnixNano()), nodeID, group, buf)
	return s.enqueue(ctx, cap(buf), msg)
}

// routeAndEnqueue picks the target node for iwr and enqueues it. The
// proto-message replayers (stream/trace) share this Pick+build+enqueue tail; the
// measure columnar path routes inline via routeColumnar + enqueueMarshaled.
func (s *batchSender) routeAndEnqueue(ctx context.Context, selector node.Selector,
	group, name string, shardID uint32, iwr proto.Message,
) error {
	nodeID, err := selector.Pick(group, name, shardID, 0)
	if err != nil {
		return fmt.Errorf("pick target node for %s: %w", name, err)
	}
	msg := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
	return s.enqueue(ctx, proto.Size(iwr), msg)
}

// enqueue buffers msg and, once the batch fills by row count or in-flight byte
// budget, sends and asynchronously confirms it. size is the message's marshaled
// size (cap of the pooled buffer for enqueueMarshaled, proto.Size for the
// proto-message path) and feeds the byte budget. The returned error is non-nil
// only when a flush's in-flight bound surfaced an earlier batch's failure.
func (s *batchSender) enqueue(ctx context.Context, size int, msg bus.Message) error {
	s.batch = append(s.batch, msg)
	s.inflightBytes += size
	byteCap := s.inflightBytes >= s.maxBytes
	rowCap := len(s.batch) >= s.maxRows
	if rowCap || byteCap {
		if byteCap {
			s.byteLimitFlushes++
		} else {
			s.rowLimitFlushes++
		}
		return s.flush(ctx)
	}
	return nil
}

// flush sends the buffered batch on the current publisher, rotates in a fresh
// publisher for the next batch, and confirms the sent batch (closing its stream
// to collect the per-node delivery result) in the background. The returned error
// aborts the part promptly: it is this batch's synchronous send error, or — when
// the in-flight bound made it wait — an earlier batch's surfaced result (which
// takes priority, preserving send order). It is a no-op when the buffer is empty.
func (s *batchSender) flush(ctx context.Context) error {
	pending := s.take()
	if len(pending) == 0 {
		return nil
	}
	_, pubErr := s.publisher.Publish(ctx, s.topic, pending...)
	// Publish is synchronous: every body is serialized onto the wire before it
	// returns, so the marshaled buffers are no longer referenced by the transport
	// and can be returned to the pool for the next batch to reuse.
	s.pool.returnAll(s.lent)
	s.lent = s.lent[:0]
	s.inflightBytes = 0
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
				out = fmt.Errorf("row-replay batch confirm panicked: %v\n%s", rec, debug.Stack())
			}
			ch <- out
		}()
		cee, closeErr := pub.Close()
		err := pubErr
		if err == nil {
			err = closeErr
		} else {
			cee = nil // publish failed — receiver saw nothing, so per-node cee is noise
		}
		out = newNodeReplayError(cee, err)
	}()
	// Abort promptly: an earlier in-flight batch's failure (via the bound) wins,
	// otherwise this batch's own synchronous send error.
	if awaited := s.pipeline.add(ch); awaited != nil {
		return awaited
	}
	if pubErr != nil {
		return newNodeReplayError(nil, pubErr)
	}
	return nil
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
			// A series whose entity cannot be resolved (sidx gap) is skipped, not
			// fatal: drop its rows, tally them, and keep migrating the rest so one
			// localized source-data gap does not block the whole part/migration.
			if errors.Is(err, errSkipSeries) {
				s.recordSkip(err)
				continue
			}
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
			if len(s.batch) > 0 {
				s.tailFlushes++
			}
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

// take swaps out the buffered batch, returning nil when empty.
func (s *batchSender) take() []bus.Message {
	if len(s.batch) == 0 {
		return nil
	}
	pending := s.batch
	s.batch = make([]bus.Message, 0, s.maxRows)
	return pending
}

// discard drops any buffered rows not yet flushed, abandoning the residual of a
// failed part so it is never sent. The abandoned marshal buffers are dropped (not
// returned to the pool) and the byte counter is reset, since no Publish ran.
func (s *batchSender) discard() {
	s.take()
	s.lent = s.lent[:0]
	s.inflightBytes = 0
}
