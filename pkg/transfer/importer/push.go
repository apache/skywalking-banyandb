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

package importer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// PushOptions configure Push.
type PushOptions struct {
	// Log receives warnings and retry notes; Err the work items done and not done of a push that
	// fails, Log when nil.
	Log io.Writer
	Err io.Writer
	// Hook, when set, is called on the worker goroutine after every acknowledged batch with
	// "batch <work item key> seq=<n> rows=<r>".
	Hook func(event string)
	// Progress, when set, shows the progress per (catalog, group, target stage).
	Progress *output.Progress
	// reconciler, when set, decides from the target's counts whether a stream or trace part
	// that failed on a target after its completion was sent landed there; without it such a
	// failure fails the import.
	reconciler *reconciler
	// track, when set, records the progress in the progress file.
	track *tracker
	// resume, when set, says what a resumed push skips: CSV rows up to each item's cursor, native
	// payloads done, and the nodes a payload is pushed again to.
	resume *resumePlan
	// Import holds the batch, rate, retry and parallelism settings; math.MaxInt parallelism
	// means max.
	config.Import
}

// streamKey identifies a (catalog, group, target stage): the ImportRows stream a worker keeps
// for it, and the rows verify reconciles together.
type streamKey struct {
	group   string
	stage   string
	catalog commonv1.Catalog
}

// PushKey groups the acknowledged rows of the work items of one kind sent to one
// (catalog, group, target stage); the stage is "" for property.
type PushKey struct {
	streamKey
	Kind WorkItemKind
}

func entryStreamKey(e *Entry) streamKey {
	return streamKey{catalog: e.Item.Catalog, group: e.Item.Group, stage: e.TargetStage}
}

// PushResult is what Push sent.
type PushResult struct {
	// Rows are the acknowledged rows per key.
	Rows map[PushKey]uint64
	// Done lists the keys of the work items whose every batch was acknowledged.
	Done []string
}

// WorkerCount is the effective number of push workers for entries: max is the number of
// distinct target data nodes of the entries, an explicit count is kept; either is clamped
// to the number of work items. The reason explains the value for the operator.
func WorkerCount(parallelism int, entries []Entry) (int, string) {
	if len(entries) == 0 {
		return 0, "no work item"
	}
	n := parallelism
	reason := "--parallelism"
	if parallelism == math.MaxInt {
		var nodes []string
		for _, e := range entries {
			nodes = append(nodes, e.TargetNodes...)
		}
		nodes = transfer.UniqueSorted(nodes)
		n = max(1, len(nodes))
		reason = fmt.Sprintf("max = %d target data node(s)", len(nodes))
	}
	if n > len(entries) {
		n = len(entries)
		reason += fmt.Sprintf(", clamped to the %d work item(s)", len(entries))
	}
	return n, reason
}

type builderKey struct {
	group   string
	catalog commonv1.Catalog
}

// newBuilders returns the row builder of every (catalog, group) of entries, for the target schema.
// The CSV columns are laid out by the artifact's schema, or by the target's for an artifact
// exported without it, which then must match the schema the rows were exported with.
func newBuilders(art *Artifact, target *Schemas, entries []Entry) (map[builderKey]*rowBuilder, error) {
	fromTarget := !art.HasSchema()
	columns := &art.Schemas
	if fromTarget {
		columns = target
	}
	builders := map[builderKey]*rowBuilder{}
	for i := range entries {
		k := builderKey{catalog: entries[i].Item.Catalog, group: entries[i].Item.Group}
		if _, ok := builders[k]; ok {
			continue
		}
		layout, err := columns.Layout(k.catalog, k.group)
		if err != nil {
			return nil, fmt.Errorf("%w: layout of %s group %s: %w", errCorrupt, entries[i].Item.CatalogName(), k.group, err)
		}
		b := newRowBuilder(layout, target, k.catalog, k.group)
		b.targetColumns = fromTarget
		builders[k] = b
	}
	return builders, nil
}

type pusher struct {
	art      *Artifact
	builders map[builderKey]*rowBuilder
	limiter  *rate.Limiter
	rows     map[PushKey]uint64
	done     map[string]bool
	progress *importProgress
	liaisons []*cluster.Liaison
	opts     PushOptions
	mu       sync.Mutex
}

// Push sends the rows of every entry to the liaisons with a pool of workers.
// Worker i uses liaison i % L and lazily opens one ImportRows stream per (catalog, group,
// target stage), reused across its work items. After the first exchange of a stream, a
// worker keeps up to --write-inflight-batches batches sent and not yet acknowledged on it:
// it builds and sends the next batch while the liaison works on the previous ones, so a
// worker holds at most --write-inflight-batches × --write-batch-bytes of rows. A work item
// is done once every one of its batches is acknowledged. Only the first exchange of a stream is
// retried: UNAVAILABLE and load-protection RESOURCE_EXHAUSTED move to the next liaison,
// SCHEMA_NOT_FOUND (a schema not live yet) waits; each with a back-off within --send-retry-timeout.
// Any later error ends the import: every worker is canceled and the error, an exit-3
// ExitError, lists the work items done and not done. target is the target schema the rows
// are assembled for.
func Push(ctx context.Context, liaisons []*cluster.Liaison, art *Artifact, target *Schemas, entries []Entry, opts PushOptions) (*PushResult, error) {
	if opts.Log == nil {
		opts.Log = io.Discard
	}
	builders, err := newBuilders(art, target, entries)
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	p := &pusher{art: art, opts: opts, liaisons: liaisons, builders: builders, rows: map[PushKey]uint64{}, done: map[string]bool{}}
	if opts.Progress != nil {
		p.progress = newImportProgress(opts.Progress, entries)
	}
	p.limiter = ratelimit.NewClient(opts.WriteRateLimit, opts.WriteBatchSize)
	opts.track.sendWindow(opts.WriteInflightBatches, opts.WriteBatchSize)
	workers, _ := WorkerCount(opts.Parallelism, entries)
	result := &PushResult{Rows: p.rows}
	if workers == 0 {
		return result, nil
	}
	work := make(chan int, len(entries))
	for i := range entries {
		work <- i
	}
	close(work)
	// The first worker failure cancels the others and is the cause Push reports.
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	runWorkers(runCtx, cancel, workers, "import-push-worker", func(ctx context.Context, i int) {
		w := &worker{
			p: p, next: i, streams: map[streamKey]*importStream{}, items: map[string]*itemProgress{}, window: max(1, opts.WriteInflightBatches),
			fail: cancel,
		}
		if workerErr := w.run(ctx, entries, work); workerErr != nil {
			cancel(workerErr)
		}
	})
	return finishPush(runCtx, opts.errLog(), entries, p.done, result, opts.track, "stream and trace rows")
}

// runWorkers runs fn(ctx, i) for i in [0, n) concurrently and waits for all of them; a worker
// that ends without returning, a panic, cancels ctx.
func runWorkers(ctx context.Context, cancel context.CancelCauseFunc, n int, name string, fn func(ctx context.Context, i int)) {
	l := logger.GetLogger("importer")
	tasks := make([]*run.Task, n)
	finished := make([]bool, n)
	for i := range n {
		tasks[i] = run.Go(ctx, name, l, func(ctx context.Context) {
			fn(ctx, i)
			finished[i] = true
		})
	}
	for i, t := range tasks {
		t.Wait()
		if !finished[i] {
			cancel(errors.New("an import worker failed unexpectedly"))
		}
	}
}

// errLog is where a failed push lists its work items: Err, else Log.
func (o *PushOptions) errLog() io.Writer {
	if o.Err != nil {
		return o.Err
	}
	return o.Log
}

// interrupted tells whether ctx was stopped by an interrupt (a plain cancel, as a signal does),
// not by a failure that canceled it with its cause.
func interrupted(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), context.Canceled)
}

// finishPush fills result.Done from done. When runCtx was canceled it lists the work items
// done and not done to log and returns the exit-3 error: with a progress file (track), how to
// resume; without one, what a rerun stores twice (duplicated). An interrupt once every item is
// done is no failure; any other cause is, every item done or not.
func finishPush(runCtx context.Context, log io.Writer, entries []Entry, done map[string]bool, result *PushResult, track *tracker, duplicated string,
) (*PushResult, error) {
	var undone []string
	for i := range entries {
		if k := entries[i].Item.Key(); done[k] {
			result.Done = append(result.Done, k)
		} else {
			undone = append(undone, k)
		}
	}
	if runCtx.Err() == nil || (len(undone) == 0 && interrupted(runCtx)) {
		// An interrupt that came once every item was done stopped nothing: the caller sees it next.
		// A failure, such as a stream that did not close cleanly or a progress write, is reported.
		return result, nil
	}
	fmt.Fprintf(log, "work items done (%d):\n", len(result.Done))
	for _, k := range result.Done {
		fmt.Fprintf(log, "  %s\n", k)
	}
	fmt.Fprintf(log, "work items not done (%d):\n", len(undone))
	for _, k := range undone {
		fmt.Fprintf(log, "  %s\n", k)
	}
	if track != nil {
		return result, namingProgress(exitcode.Exit(exitcode.ExitRuntime, "import failed: %w; %d of this run's %d work item(s) done (listed above); "+
			"the progress file %s is kept: %s (what is done is not pushed again; only the payloads "+
			"a resume lists as AMBIGUOUS may be stored twice, as resumeDuplicationPolicy decides)",
			context.Cause(runCtx), len(result.Done), len(entries), track.path, resumeHint))
	}
	return result, exitcode.Exit(exitcode.ExitRuntime, "import failed: %w; %d of %d work item(s) done (listed above), %s of a rerun are stored twice",
		context.Cause(runCtx), len(result.Done), len(entries), duplicated)
}

// importStream is one ImportRows stream of a worker, the batch being filled for it and the
// batches sent on it and not acknowledged yet, oldest first.
type importStream struct {
	client   transferv1.ImportService_ImportRowsClient
	cancel   context.CancelFunc
	via      *cluster.Liaison
	pending  []row
	inflight []*sentBatch
	key      streamKey
	bytes    int
	seq      uint64
}

// sentBatch is what a worker keeps of a sent batch until it is acknowledged: its work item
// and where its rows come from, to name them if it fails.
type sentBatch struct {
	entry *Entry
	rows  []rowPos
	bytes int
	seq   uint64
}

// itemProgress counts the batches of a work item not acknowledged yet; the item is done when
// the count is 0 once all its rows were sent.
type itemProgress struct {
	// start and rows are when the item started and its acknowledged rows, for the progress.
	start time.Time
	// from is where a CSV item starts; started records that its first batch is about to be sent.
	from *csvCursor
	// lastSent is the Sent mark recorded last, and restore the one before it, which is recorded
	// again when the batch of lastSent was not sent after all.
	lastSent *csvMark
	restore  *csvMark
	rows     uint64
	// sentRows counts the rows of the item this run sent.
	sentRows uint64
	unacked  int
	sent     bool
	started  bool
}

// drainTimeout bounds how long a stopped worker waits for the acknowledgements of the batches it
// has in flight; tests shorten it.
var drainTimeout = 30 * time.Second

type worker struct {
	p       *pusher
	streams map[streamKey]*importStream
	items   map[string]*itemProgress
	// streamCtx is the context of the worker's streams: it outlives the worker's context by up
	// to drainTimeout, so that a stopped worker still receives the acknowledgements in flight.
	streamCtx     context.Context
	cancelStreams context.CancelFunc
	// fail, when set, cancels the other workers with the cause of this one's failure, before it
	// waits for its own acknowledgements.
	fail   context.CancelCauseFunc
	next   int
	window int
}

func (w *worker) liaison() *cluster.Liaison { return w.p.liaisons[w.next%len(w.p.liaisons)] }

func (w *worker) run(ctx context.Context, entries []Entry, work <-chan int) (err error) {
	w.streamCtx, w.cancelStreams = context.WithCancel(context.WithoutCancel(ctx))
	drain := drainTimeout
	stopTimer := context.AfterFunc(ctx, func() { time.AfterFunc(drain, w.cancelStreams) })
	defer func() {
		if err != nil && ctx.Err() == nil && w.fail != nil {
			// The others stop sending before this worker waits for its acknowledgements.
			w.fail(err)
		}
		live := err == nil && ctx.Err() == nil
		if !live {
			// Not stopped yet: a worker that fails on its own bounds its wait for the acknowledgements too.
			if stopTimer() {
				defer time.AfterFunc(drain, w.cancelStreams).Stop()
			}
			// A stopped worker sends nothing more, but first collects the acknowledgements of the
			// batches in flight, so they are recorded instead of left ambiguous; then it only cancels
			// its streams: a clean close would fail with Canceled and hide the cause Push reports.
			if n := w.inflight(); n > 0 {
				waitingForBatches(w.p.opts.Log, drain, n, interrupted(ctx))
			}
			w.settle()
		}
		// The stop watch stays on through a clean close: a stop meanwhile cancels the streams
		// drainTimeout later, so a liaison that never ends a stream cannot hold the import.
		if closeErr := w.closeAll(live); live {
			err = closeErr
		}
		stopTimer()
		w.cancelStreams()
	}()
	for idx := range work {
		if ctx.Err() != nil {
			return nil
		}
		e := &entries[idx]
		if err = w.runEntry(ctx, e); err != nil {
			if ctx.Err() != nil {
				// Another worker failed first, or the import was interrupted: that is the error.
				return nil
			}
			return err
		}
		ip := w.progress(e)
		ip.sent = true
		if ip.unacked == 0 {
			if err = w.markDone(e, ip); err != nil {
				return err
			}
		}
	}
	if err = w.drain(); err != nil && ctx.Err() != nil {
		return nil
	}
	return err
}

func (w *worker) progress(e *Entry) *itemProgress {
	ip, ok := w.items[e.Item.Key()]
	if !ok {
		ip = &itemProgress{}
		w.items[e.Item.Key()] = ip
	}
	return ip
}

func (w *worker) markDone(e *Entry, ip *itemProgress) error {
	if err := w.p.opts.track.itemComplete(e.Item.Key()); err != nil {
		return err
	}
	w.p.mu.Lock()
	w.p.done[e.Item.Key()] = true
	w.p.mu.Unlock()
	w.p.progress.itemDone(e, ip)
	return nil
}

// inflight counts the batches sent on the worker's streams and not acknowledged yet.
func (w *worker) inflight() int {
	n := 0
	for _, st := range w.streams {
		if st.client != nil {
			n += len(st.inflight)
		}
	}
	return n
}

// settle receives the acknowledgements of the batches in flight on every stream of a stopped
// worker, until a stream fails or drainTimeout after the stop cancels the streams.
func (w *worker) settle() {
	for _, k := range w.sortedKeys() {
		st := w.streams[k]
		for st.client != nil && len(st.inflight) > 0 {
			if w.recvAck(st) != nil {
				break
			}
		}
	}
}

// drain waits for the acknowledgement of every batch in flight on the worker's streams but
// the one of except, if any.
func (w *worker) drain(except ...streamKey) error {
	for _, k := range w.sortedKeys() {
		if slices.Contains(except, k) {
			continue
		}
		st := w.streams[k]
		for len(st.inflight) > 0 {
			if err := w.recvAck(st); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *worker) runEntry(ctx context.Context, e *Entry) error {
	k := entryStreamKey(e)
	// Batches stay in flight across the work items of one stream; moving to another stream
	// settles the others first, so their work items are known done.
	if err := w.drain(k); err != nil {
		return err
	}
	st, ok := w.streams[k]
	if !ok {
		st = &importStream{key: k}
		w.streams[k] = st
	}
	ip := w.progress(e)
	w.p.progress.itemStarted(e, ip)
	from := w.p.opts.resume.cursor(e.Item.Key())
	ip.from = from
	b := w.p.builders[builderKey{catalog: e.Item.Catalog, group: e.Item.Group}]
	// A failed batch already names the work item; reading and assembling errors do not.
	var batchErr error
	if err := b.itemRows(ctx, w.p.art.Dir, &e.Item, from, func(r row) error {
		batchErr = w.add(ctx, st, e, r)
		return batchErr
	}); err != nil {
		if batchErr != nil {
			return batchErr
		}
		return fmt.Errorf("%s: %w", e.Item.Key(), err)
	}
	return w.flush(ctx, st, e)
}

// add appends a row to the stream's batch, sending the batch first when the row would push
// it past --write-batch-bytes, and after it once it holds --write-batch-size rows or the
// bytes. A row larger than --write-batch-bytes alone is sent alone with a warning. A row
// costs its length-delimited size plus the one-byte field tag inside RowBatch.
func (w *worker) add(ctx context.Context, st *importStream, e *Entry, r row) error {
	size := 1 + csvfmt.RowSize(r.msg)
	maxBytes := int(w.p.opts.WriteBatchBytes)
	if len(st.pending) > 0 && st.bytes+size > maxBytes {
		if err := w.flush(ctx, st, e); err != nil {
			return err
		}
	}
	st.pending = append(st.pending, r)
	st.bytes += size
	if size > maxBytes {
		warnOversizedRow(w.p.opts.Log, e.Item.Key(), r.file, r.record, size, maxBytes)
		return w.flush(ctx, st, e)
	}
	if len(st.pending) >= w.p.opts.WriteBatchSize || st.bytes >= maxBytes {
		return w.flush(ctx, st, e)
	}
	return nil
}

// flush sends the stream's batch. The first batch of a stream is one exchange with the init,
// retried by open; a later one joins the batches in flight, and flush then waits for the
// oldest acknowledgements until fewer than --write-inflight-batches are in flight, so the
// next batch is built while the liaison works on the ones sent.
func (w *worker) flush(ctx context.Context, st *importStream, e *Entry) error {
	n := len(st.pending)
	if n == 0 {
		return nil
	}
	if ctx.Err() != nil {
		// Stopped: nothing more is sent.
		return context.Cause(ctx)
	}
	if w.p.limiter != nil {
		if err := w.p.limiter.WaitN(ctx, n); err != nil {
			return fmt.Errorf("%s: %w", e.Item.Key(), err)
		}
	}
	b := &sentBatch{entry: e, seq: st.seq, bytes: st.bytes, rows: make([]rowPos, n)}
	for i, r := range st.pending {
		b.rows[i] = rowPos{file: r.file, record: r.record}
	}
	if err := w.recordSending(e, b); err != nil {
		return err
	}
	req := batchRequest(st.seq, st.pending)
	st.seq++
	st.pending = st.pending[:0]
	st.bytes = 0
	w.progress(e).unacked++
	if st.client == nil {
		if sent, err := w.open(ctx, st, req); err != nil {
			if !sent {
				// The batch reached no liaison: its rows were not sent, the window stays as it was.
				return w.failUnsent(st, e, b, err)
			}
			return w.batchError(st, b, err)
		}
		return w.acked(st, b)
	}
	st.inflight = append(st.inflight, b)
	if err := st.client.Send(req); err != nil {
		return w.sendFailed(st, e, b, err)
	}
	for len(st.inflight) >= w.window {
		if err := w.recvAck(st); err != nil {
			return err
		}
	}
	return nil
}

// failUnsent fails b, whose rows were not sent: they leave the window a resume treats as
// ambiguous. An error of leaving it joins the batch error.
func (w *worker) failUnsent(st *importStream, e *Entry, b *sentBatch, err error) error {
	if unsentErr := w.unsent(e, b); unsentErr != nil {
		return errors.Join(w.batchError(st, b, err), unsentErr)
	}
	return w.batchError(st, b, err)
}

// sendFailed handles the failure of st.client.Send for b, the last batch in flight.
func (w *worker) sendFailed(st *importStream, e *Entry, b *sentBatch, err error) error {
	st.inflight = st.inflight[:len(st.inflight)-1]
	if !errors.Is(err, io.EOF) {
		// gRPC returns an error of its own making (a batch over the client's send limit, one it
		// cannot encode) only when none of b reached the wire: the rows were not sent. Whatever
		// else is in flight stays in the window, as the stream ends with it.
		return w.failUnsent(st, e, b, err)
	}
	// The liaison ended the stream before b reached it: b was not sent, so its rows leave the
	// window a resume treats as ambiguous. The status follows the acknowledgements already sent.
	if err = w.unsent(e, b); err != nil {
		return err
	}
	for len(st.inflight) > 0 {
		if err = w.recvAck(st); err != nil {
			return err
		}
	}
	if _, err = st.client.Recv(); err == nil || errors.Is(err, io.EOF) {
		err = errors.New("the liaison ended the import stream")
	}
	return w.batchError(st, b, err)
}

// recvAck receives the acknowledgement of the oldest batch in flight on the stream. A failure
// is reported against the batch the liaison names, else the oldest one.
func (w *worker) recvAck(st *importStream) error {
	if len(st.inflight) == 0 {
		return errors.New("the liaison ended the import stream")
	}
	oldest := st.inflight[0]
	resp, err := st.client.Recv()
	if err = ackErr(resp, err, oldest.seq); err != nil {
		return w.batchError(st, w.failedBatch(st, err), err)
	}
	st.inflight[0] = nil
	st.inflight = st.inflight[1:]
	return w.acked(st, oldest)
}

// failedBatch is the batch in flight a failure names by its batch_seq, else the oldest one.
func (w *worker) failedBatch(st *importStream, err error) *sentBatch {
	_, _, md := failureOf(err)
	if seq, convErr := strconv.ParseUint(md[transfer.ImportMetadataBatchSeq], 10, 64); convErr == nil {
		for _, b := range st.inflight {
			if b.seq == seq {
				return b
			}
		}
	}
	return st.inflight[0]
}

// acked counts the rows of an acknowledged batch, moves its work item's cursor past them, and
// marks the work item done once all its batches are acknowledged.
func (w *worker) acked(st *importStream, b *sentBatch) error {
	k := PushKey{streamKey: st.key, Kind: b.entry.Item.Kind}
	w.p.mu.Lock()
	w.p.rows[k] += uint64(len(b.rows))
	w.p.mu.Unlock()
	if len(b.rows) > 0 {
		if err := w.p.opts.track.csvAcked(b.entry, k, b.rows[len(b.rows)-1], len(b.rows)); err != nil {
			return err
		}
	}
	ip := w.progress(b.entry)
	w.p.progress.acked(b.entry, ip, len(b.rows))
	if w.p.opts.Hook != nil {
		w.p.opts.Hook(fmt.Sprintf("batch %s seq=%d rows=%d", b.entry.Item.Key(), b.seq, len(b.rows)))
	}
	ip.unacked--
	if ip.sent && ip.unacked == 0 {
		return w.markDone(b.entry, ip)
	}
	return nil
}

// recordSending records, before a batch is sent, the last row it sends: from here on its rows may
// land, and a resume treats the rows after the cursor up to it as ambiguous. The first batch of
// an item also records that the item started, synced.
func (w *worker) recordSending(e *Entry, b *sentBatch) error {
	ip := w.progress(e)
	var base uint64
	if ip.from != nil {
		base = ip.from.Rows
	}
	ip.sentRows += uint64(len(b.rows))
	last := b.rows[len(b.rows)-1]
	mark := csvMark{File: last.file, Record: last.record, Rows: base + ip.sentRows}
	if !ip.started {
		if ip.from != nil {
			ip.lastSent = ip.from.Sent
		}
		if err := w.p.opts.track.csvStarted(e.Item.Key(), ip.from, mark); err != nil {
			return err
		}
		ip.started = true
	} else if err := w.p.opts.track.csvSending(e.Item.Key(), mark); err != nil {
		return err
	}
	prev := ip.lastSent
	ip.lastSent = &mark
	ip.restore = prev
	return nil
}

// unsent records that batch b of e, recorded as sending, was not sent.
func (w *worker) unsent(e *Entry, b *sentBatch) error {
	ip := w.progress(e)
	ip.sentRows -= uint64(len(b.rows))
	ip.lastSent = ip.restore
	return w.p.opts.track.csvUnsent(e.Item.Key(), ip.restore)
}

func batchRequest(seq uint64, rows []row) *transferv1.ImportRowsRequest {
	// A stream carries one catalog, so the rows are of the kind of the first.
	b := &transferv1.ImportRowsRequest_RowBatch{BatchSeq: seq}
	switch rows[0].msg.(type) {
	case *transferv1.ImportRowsRequest_StreamRow:
		b.Rows = &transferv1.ImportRowsRequest_RowBatch_Stream{
			Stream: &transferv1.ImportRowsRequest_StreamRows{Rows: msgsOf[*transferv1.ImportRowsRequest_StreamRow](rows)},
		}
	case *transferv1.ImportRowsRequest_MeasureRow:
		b.Rows = &transferv1.ImportRowsRequest_RowBatch_Measure{
			Measure: &transferv1.ImportRowsRequest_MeasureRows{Rows: msgsOf[*transferv1.ImportRowsRequest_MeasureRow](rows)},
		}
	case *transferv1.ImportRowsRequest_TraceRow:
		b.Rows = &transferv1.ImportRowsRequest_RowBatch_Trace{
			Trace: &transferv1.ImportRowsRequest_TraceRows{Rows: msgsOf[*transferv1.ImportRowsRequest_TraceRow](rows)},
		}
	case *transferv1.ImportRowsRequest_PropertyRow:
		b.Rows = &transferv1.ImportRowsRequest_RowBatch_Property{
			Property: &transferv1.ImportRowsRequest_PropertyRows{Rows: msgsOf[*transferv1.ImportRowsRequest_PropertyRow](rows)},
		}
	}
	return &transferv1.ImportRowsRequest{Frame: &transferv1.ImportRowsRequest_Batch{Batch: b}}
}

// msgsOf returns the messages of rows, all of type T.
func msgsOf[T any](rows []row) []T {
	out := make([]T, len(rows))
	for i, r := range rows {
		out[i] = r.msg.(T)
	}
	return out
}

// open opens the stream and performs its first exchange (init plus the first batch). A
// refusal before anything was acknowledged is retried: UNAVAILABLE and a RESOURCE_EXHAUSTED
// load refusal on the next liaison, SCHEMA_NOT_FOUND (the schema is not live on the liaison
// yet) on the same one, with a back-off doubling from transfer.RetryBase to transfer.RetryMax until
// --send-retry-timeout is used up. NODE_FAILED (the batch may be partly written) and
// MESSAGE_TOO_LARGE are never retried, nor is a stream or trace batch's UNAVAILABLE without
// a detail once the batch was sent (see retryableFirst). sent reports whether any attempt may have
// written the batch: one that sent it and was not refused as NOT_WRITTEN or SCHEMA_NOT_FOUND,
// which the liaison answers before it writes anything.
func (w *worker) open(ctx context.Context, st *importStream, first *transferv1.ImportRowsRequest) (sent bool, err error) {
	deadline := time.Now().Add(w.p.opts.SendRetryTimeout)
	backoff := transfer.RetryBase
	init := &transferv1.ImportRowsRequest_Init{
		Catalog: st.key.catalog, Group: st.key.group, Stage: st.key.stage,
		LiaisonBytesPerSecond: ratelimit.PerSecond(w.p.opts.LiaisonRateLimit),
		RateLimit:             ratelimit.Proto(w.p.opts.NodeRateLimit, w.p.opts.NodeRowRateLimit),
	}
	// A resent stream or trace batch writes its rows twice; measure and property overwrite.
	duplicates := storesDuplicates(st.key.catalog)
	for {
		via := w.liaison()
		st.via = via
		streamCtx, cancel := context.WithCancel(w.streamCtx)
		attemptSent := false
		var client transferv1.ImportService_ImportRowsClient
		//nolint:contextcheck // the stream outlives the worker's context by up to drainTimeout to collect the acknowledgements in flight
		client, err = transferv1.NewImportServiceClient(via.Conn).ImportRows(streamCtx)
		if err == nil {
			err = sendErr(client, client.Send(&transferv1.ImportRowsRequest{Frame: &transferv1.ImportRowsRequest_Init_{Init: init}}))
		}
		if err == nil {
			attemptSent, err = exchange(client, first)
		}
		if err == nil {
			st.client, st.cancel = client, cancel
			return true, nil
		}
		cancel()
		if ctx.Err() != nil {
			return sent || attemptSent, context.Cause(ctx)
		}
		code, reason, _ := failureOf(err)
		if attemptSent && reason != transfer.ImportReasonNotWritten && reason != transfer.ImportReasonSchemaNotFound {
			sent = true
		}
		if !retryableFirst(code, reason, attemptSent && duplicates) || time.Now().Add(backoff).After(deadline) {
			return sent, err
		}
		if !schemaNotFound(code, reason) {
			w.next++
		}
		streamRetry(w.p.opts.Log, st.key, via.Addr, code, status.Convert(err).Message(), w.liaison().Addr, backoff)
		if err = transfer.SleepCtx(ctx, backoff); err != nil {
			return sent, err
		}
		backoff = transfer.NextBackoff(backoff)
	}
}

// failureOf returns the status code of an import failure and the reason and metadata of its
// ImportService ErrorInfo detail; the reason is "" when none is attached (a transport error,
// or a refusal the liaison makes before its import handler, such as load shedding), and the
// code is then all there is to go by.
func failureOf(err error) (codes.Code, string, map[string]string) {
	st := status.Convert(err)
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == transfer.ImportErrorDomain {
			return st.Code(), info.GetReason(), info.GetMetadata()
		}
	}
	return st.Code(), "", nil
}

// retryableFirst tells whether a refused first exchange may be retried. NODE_FAILED comes
// after the batch fanned out, so other nodes may already hold it and a resend would store
// stream and trace rows twice; a MESSAGE_TOO_LARGE or ROW_INVALID batch would only fail
// again. NOT_WRITTEN says the liaison refused before writing anything. Without a detail,
// UNAVAILABLE, NOT_FOUND and RESOURCE_EXHAUSTED (load protection) are retried, except an
// UNAVAILABLE when sentDuplicable: a stream or trace batch was sent, and a liaison that broke
// off after fanning it out cannot say so, so a resend could store its rows twice.
func retryableFirst(code codes.Code, reason string, sentDuplicable bool) bool {
	switch reason {
	case transfer.ImportReasonNodeFailed, transfer.ImportReasonMessageTooLarge, transfer.ImportReasonRowInvalid:
		return false
	case transfer.ImportReasonSchemaNotFound, transfer.ImportReasonNotWritten:
		return true
	}
	if code == codes.Unavailable {
		return !sentDuplicable
	}
	return code == codes.NotFound || code == codes.ResourceExhausted
}

// schemaNotFound tells whether a refusal names a schema not live on the liaison yet: waiting
// on the same liaison may help, another one would not know it sooner.
func schemaNotFound(code codes.Code, reason string) bool {
	return reason == transfer.ImportReasonSchemaNotFound || (reason == "" && code == codes.NotFound)
}

// sendErr turns a Send failure into the stream's status: Send reports io.EOF when the server
// ended the stream, and the reason comes from Recv.
func sendErr(client transferv1.ImportService_ImportRowsClient, err error) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	if _, recvErr := client.Recv(); recvErr != nil && !errors.Is(recvErr, io.EOF) {
		return recvErr
	}
	return errors.New("the liaison ended the import stream")
}

// exchange sends one batch and waits for its acknowledgement. It reports whether the batch
// was sent: a failure after that may have come after the liaison wrote it.
func exchange(client transferv1.ImportService_ImportRowsClient, req *transferv1.ImportRowsRequest) (bool, error) {
	if err := client.Send(req); err != nil {
		return false, sendErr(client, err)
	}
	resp, err := client.Recv()
	return true, ackErr(resp, err, req.GetBatch().GetBatchSeq())
}

// ackErr checks the Recv result of the acknowledgement of batch seq.
func ackErr(resp *transferv1.ImportRowsResponse, err error, seq uint64) error {
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("the liaison ended the import stream without acknowledging batch_seq=%d", seq)
	}
	if err != nil {
		return err
	}
	if resp.GetBatchSeq() != seq {
		return fmt.Errorf("the liaison acknowledged batch_seq=%d, expected %d", resp.GetBatchSeq(), seq)
	}
	return nil
}

// batchError names the work item, the liaison, the batch and the artifact rows of a failed
// batch: the row the liaison named, or the batch's range of records.
func (w *worker) batchError(st *importStream, b *sentBatch, err error) error {
	via := "<none>"
	if st.via != nil {
		via = st.via.Addr
	}
	code, reason, md := failureOf(err)
	where := ""
	if len(b.rows) > 0 {
		first, last := b.rows[0], b.rows[len(b.rows)-1]
		where = fmt.Sprintf("file %s record %d to file %s record %d", first.file, first.record, last.file, last.record)
		if reason == transfer.ImportReasonRowInvalid {
			if i, convErr := strconv.Atoi(md[transfer.ImportMetadataRow]); convErr == nil && i >= 0 && i < len(b.rows) {
				where = fmt.Sprintf("file %s record %d", b.rows[i].file, b.rows[i].record)
			}
		}
	}
	prefix := fmt.Sprintf("%s via %s: batch_seq=%d", b.entry.Item.Key(), via, b.seq)
	size := fmt.Sprintf("%d row(s), about %.1f MiB, ending at %s", len(b.rows), float64(b.bytes)/(1<<20), where)
	switch {
	case reason == transfer.ImportReasonMessageTooLarge:
		return fmt.Errorf("%s of %s: %w; the batch exceeds the liaison's --max-recv-msg-size (16 MiB by default): lower --write-batch-bytes",
			prefix, size, err)
	case reason == transfer.ImportReasonNodeFailed && code == codes.ResourceExhausted:
		return fmt.Errorf("%s of %s: %w; data node %s refused the batch for lack of resources (see the cause above): "+
			"free disk space or wait for its load to drop first", prefix, size, err, md[transfer.ImportMetadataNode])
	case reason == "" && code == codes.ResourceExhausted:
		return fmt.Errorf("%s of %s: %w; the liaison refused the import stream under load until --send-retry-timeout (%s) ran out: "+
			"retry later or raise --send-retry-timeout", prefix, size, err, w.p.opts.SendRetryTimeout)
	}
	return fmt.Errorf("%s (%s): %w", prefix, where, err)
}

// closeAll ends every stream of the worker; when clean, it half-closes each and waits for
// the liaison to end it.
func (w *worker) closeAll(clean bool) error {
	var errs []error
	for _, k := range w.sortedKeys() {
		st := w.streams[k]
		if st.client == nil {
			continue
		}
		if clean {
			if err := st.client.CloseSend(); err != nil {
				errs = append(errs, fmt.Errorf("close the import stream of %s/%s: %w", k.group, stageLabel(k.stage), err))
			} else if _, err = st.client.Recv(); !errors.Is(err, io.EOF) {
				errs = append(errs, fmt.Errorf("end of the import stream of %s/%s: %w", k.group, stageLabel(k.stage), err))
			}
		}
		st.cancel()
	}
	return errors.Join(errs...)
}

// sortedKeys lists the worker's streams in a stable order.
func (w *worker) sortedKeys() []streamKey {
	return slices.SortedFunc(maps.Keys(w.streams), func(a, b streamKey) int {
		return cmp.Or(cmp.Compare(a.catalog, b.catalog), strings.Compare(a.group, b.group), strings.Compare(a.stage, b.stage))
	})
}
