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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// lockedBuffer is a bytes.Buffer the workers of a push may write concurrently.
type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// twoGroupArtifact writes a native artifact of one stream shard item in each of the groups,
// each of one part of 4 rows, routed to d1 and d2.
func twoGroupArtifact(t *testing.T, groups ...string) (*Artifact, []Entry) {
	t.Helper()
	dir := t.TempDir()
	art := &Artifact{Dir: dir, Manifest: &manifest.Manifest{Format: manifest.FormatNative}, Schemas: NewSchemas()}
	var entries []Entry
	for _, g := range groups {
		shard := writeContainer(t, dir, fmt.Sprintf("nodes/n1/stream/%s/seg-%s/shard-0", g, nativeSegment),
			member{"0000000000000001/metadata.json", coreMeta(4)},
			member{"0000000000000001/primary.bin", "PRIMARY"},
			member{"0000000000000001/meta.bin", "META"},
			member{"0000000000000001/timestamps.bin", "TS"},
			member{"0000000000000001.snp", `["0000000000000001"]`},
		)
		item := WorkItem{
			Catalog: commonv1.Catalog_CATALOG_STREAM, Group: g, Stage: "hot", Segment: nativeSegment, SegmentVersion: "1.5.0", Kind: ShardItem,
			Sources: []WorkSource{{Node: "n1", Files: []manifest.File{shard}, MinTimestamp: nativeSegTime + 1, MaxTimestamp: nativeSegTime + 2, Rows: 4}},
		}
		entries = append(entries, Entry{Item: item, TargetStage: "hot", TargetNodes: []string{"d1", "d2"}, Copies: 2})
	}
	return art, entries
}

// drainRun pushes a stream part of group "slow" whose answer the liaison holds back, while the
// part of group "bad" fails on every attempt once the slow one's completion arrived: the failing
// worker stops the run with the slow payload's completion in flight. release, when it returns
// true, lets the slow answer go; it is polled with the log of the push.
func drainRun(t *testing.T, release func(log string) bool) (*progressState, *lockedBuffer, error) {
	t.Helper()
	shortenPartRetries(t)
	drain := drainTimeout
	drainTimeout = time.Second
	t.Cleanup(func() { drainTimeout = drain })
	art, entries := twoGroupArtifact(t, "slow", "bad")
	slowCompleted := make(chan struct{})
	var once sync.Once
	log := &lockedBuffer{}
	f := &fakePartsLiaison{}
	f.answer = func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		if p.init.GetGroup() == "bad" {
			resp := okTargets("d1", "d2")
			resp.Targets[0].Success, resp.Targets[0].Error = false, "refused"
			resp.Targets[1].Success, resp.Targets[1].Error = false, "refused"
			return resp, nil
		}
		once.Do(func() { close(slowCompleted) })
		deadline := time.Now().Add(5 * time.Second)
		for !release(log.String()) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		return okTargets("d1", "d2"), nil
	}
	f.early = func(init *transferv1.ImportPartsRequest_Init) *transferv1.ImportPartsResponse {
		if init.GetGroup() == "bad" {
			<-slowCompleted
		}
		return nil
	}
	li := f.serve(t)
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(art, testBindings())
	track, err := startTracker(context.Background(), path, state, 1, nil)
	require.NoError(t, err)
	opts := nativePushOptions()
	opts.Parallelism = 2
	opts.Log, opts.track = log, track
	_, pushErr := PushParts(context.Background(), []*cluster.Liaison{li}, art, entries, opts)
	require.NoError(t, track.close())
	return state, log, pushErr
}

// A worker failing stops the run while another worker waits for the answer of a part whose
// completion it sent: that worker waits for the answer, bounded by drainTimeout, and records where
// the part landed, so a resume finds nothing ambiguous of it.
func TestPushParts_StopWaitsForTheAnswerOfASentCompletion(t *testing.T) {
	state, log, err := drainRun(t, func(log string) bool { return strings.Contains(log, "waiting up to") })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream/bad/hot/20261018/shard-0")
	assert.Contains(t, log.String(), "waiting up to 1s for the answer of the KIND_PART slow/hot shard 0 in flight, so that it is recorded; "+
		"press Ctrl-C twice to stop now", "a failure, not a Ctrl-C, started the drain")
	slow := state.Done["stream/slow/hot/20261018/shard-0"]
	require.NotNil(t, slow, log.String())
	assert.True(t, slow.Complete, "the answer of its only part completes the item")
	assert.Equal(t, map[string]uint64{"d1": 4, "d2": 4}, slow.Landed, "where the part landed is recorded")
	for _, in := range state.Inflight {
		assert.NotEqual(t, "stream/slow/hot/20261018/shard-0", in.Key, "the slow part is no longer in flight")
	}
	_, entries := twoGroupArtifact(t, "slow", "bad")
	for _, a := range classify(state, entries, &config.Import{}, nil, "") {
		assert.NotEqual(t, "slow", a.entry.Item.Group, "the slow part is not ambiguous: %s", a.name())
	}
}

// An answer that does not come within drainTimeout leaves the part ambiguous on every node it was
// sent to, recorded with no_answer: the nodes may still commit it, so a resume does not take
// counts that read it as not landed for its absence.
func TestPushParts_StopDrainIsBounded(t *testing.T) {
	start := time.Now()
	state, log, err := drainRun(t, func(string) bool { return time.Since(start) > 8*time.Second })
	require.Error(t, err)
	assert.Less(t, time.Since(start), 8*time.Second, "the run does not wait for the answer past drainTimeout")
	assert.Contains(t, log.String(), "waiting up to 1s for the answer of the KIND_PART slow/hot shard 0 in flight")
	for _, in := range state.Inflight {
		assert.NotEqual(t, "stream/slow/hot/20261018/shard-0", in.Key, "the part is recorded as answered by no node")
	}
	slow := state.Done["stream/slow/hot/20261018/shard-0"]
	require.NotNil(t, slow, log.String())
	var parts []*payloadRecord
	for _, p := range slow.Parts {
		parts = append(parts, p)
	}
	require.Len(t, parts, 1)
	assert.Equal(t, []string{"d1", "d2"}, parts[0].CompletionSent)
	assert.Equal(t, map[string]string{"d1": causeNoAnswer, "d2": causeNoAnswer}, parts[0].Causes)
	_, entries := twoGroupArtifact(t, "slow", "bad")
	var ambiguous []string
	for _, a := range classify(state, entries, &config.Import{}, nil, "") {
		ambiguous = append(ambiguous, a.entry.Item.Group)
		assert.Equal(t, map[string]string{"d1": causeNoAnswer, "d2": causeNoAnswer}, a.causes)
	}
	assert.Contains(t, ambiguous, "slow")
}

// A controlled stop (Ctrl-C) that comes after a worker sent the completion of a part waits for
// that part's answer, bounded by drainTimeout, and records where it landed: the liaison holds the
// answer back until the push logs that it waits, so the stop always falls in that window.
func TestPushParts_ControlledStopWaitsForTheAnswerOfASentCompletion(t *testing.T) {
	shortenPartRetries(t)
	drain := drainTimeout
	drainTimeout = 2 * time.Second
	t.Cleanup(func() { drainTimeout = drain })
	art, entries := twoGroupArtifact(t, "slow")
	log := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &fakePartsLiaison{}
	f.answer = func(_ int, _ *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		// The completion arrived: stop the run now, and answer only once the push waits for it.
		cancel()
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(log.String(), "waiting up to") && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		return okTargets("d1", "d2"), nil
	}
	li := f.serve(t)
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(art, testBindings())
	track, err := startTracker(context.Background(), path, state, 1, nil)
	require.NoError(t, err)
	opts := nativePushOptions()
	opts.Log, opts.track = log, track
	_, pushErr := PushParts(ctx, []*cluster.Liaison{li}, art, entries, opts)
	require.NoError(t, track.close())
	// The only item completed with the answer: the stop stopped nothing, so the push succeeds and
	// the caller sees the stop next.
	require.NoError(t, pushErr)
	assert.Contains(t, log.String(), "waiting up to 2s for the answer of the KIND_PART slow/hot shard 0 in flight, so that it is recorded; "+
		"press Ctrl-C again to stop now")
	slow := state.Done["stream/slow/hot/20261018/shard-0"]
	require.NotNil(t, slow, log.String())
	assert.True(t, slow.Complete)
	assert.Equal(t, map[string]uint64{"d1": 4, "d2": 4}, slow.Landed)
	assert.Empty(t, state.Inflight, "nothing is left in flight")
	assert.Empty(t, classify(state, entries, &config.Import{}, nil, ""), "a resume finds nothing ambiguous")
}

// A stopped worker collects the acknowledgements of the batches in flight before it closes its
// streams: they are recorded, not left ambiguous.
func TestResumeCSV_StopCollectsTheAcknowledgementsInFlight(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.setAckGate(func(_ context.Context, init *transferv1.ImportRowsRequest_Init, s uint64) {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && s == 1 {
			cancel()
			time.Sleep(50 * time.Millisecond)
		}
	})
	run := runImportEnv(ctx, f, li, opts, nil, nil)
	f.setAckGate(nil)
	require.Equal(t, exitcode.ExitRuntime, run.code(), run.log)
	c := readState(t, ProgressPath(opts)).Done[streamItemKey].Cursor
	require.NotNil(t, c)
	assert.Equal(t, uint64(2), c.Rows, "the batch acknowledged after the stop is recorded")
	assert.Zero(t, c.window(), "nothing was sent after the last acknowledgement")

	opts.DryRun = true
	dry := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, dry.err, dry.log)
	assert.NotContains(t, dry.log, "AMBIGUOUS")
}

// stuckRows refuses the first batch and never acknowledges another until the worker's streams end.
type stuckRows struct {
	transferv1.ImportService_ImportRowsClient
	w     *worker
	recvs int
}

func (s *stuckRows) Recv() (*transferv1.ImportRowsResponse, error) {
	s.recvs++
	if s.recvs == 1 {
		return nil, status.Error(codes.Internal, "batch refused")
	}
	<-s.w.streamCtx.Done()
	return nil, s.w.streamCtx.Err()
}

// A worker failing on its own, its run not canceled, still bounds its wait for the
// acknowledgements in flight.
func TestWorker_OwnFailureBoundsTheDrain(t *testing.T) {
	drain := drainTimeout
	drainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { drainTimeout = drain })
	e := &Entry{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem}, TargetStage: "hot"}
	k := entryStreamKey(e)
	var log bytes.Buffer
	w := &worker{p: &pusher{opts: PushOptions{Log: &log}, rows: map[PushKey]uint64{}, done: map[string]bool{}}, items: map[string]*itemProgress{}, window: 2}
	client := &stuckRows{w: w}
	w.streams = map[streamKey]*importStream{k: {key: k, client: client, cancel: func() {}, inflight: []*sentBatch{
		{entry: e, seq: 0, rows: []rowPos{{file: "f", record: 1}}}, {entry: e, seq: 1, rows: []rowPos{{file: "f", record: 2}}},
	}}}
	work := make(chan int)
	close(work)
	done := make(chan error, 1)
	go func() { done <- w.run(context.Background(), nil, work) }()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "batch refused")
	case <-time.After(5 * time.Second):
		t.Fatal("the worker waits for the acknowledgements in flight without a bound")
	}
	assert.Contains(t, log.String(), "waiting up to 100ms for the acknowledgements of 2 batch(es) in flight, so that they are recorded; press Ctrl-C twice to stop now")
}

// hangingClose acknowledges nothing more: the end of its stream never comes until the worker's
// streams are canceled. entered is closed once the worker waits for that end.
type hangingClose struct {
	transferv1.ImportService_ImportRowsClient
	w       *worker
	entered chan struct{}
}

func (h *hangingClose) CloseSend() error { return nil }

func (h *hangingClose) Recv() (*transferv1.ImportRowsResponse, error) {
	close(h.entered)
	<-h.w.streamCtx.Done()
	return nil, h.w.streamCtx.Err()
}

// A stop that comes while a worker closes its streams cleanly cancels them drainTimeout later: a
// liaison that never ends a stream does not hold the import past the first Ctrl-C.
func TestWorker_StopDuringTheCleanCloseIsBounded(t *testing.T) {
	drain := drainTimeout
	drainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { drainTimeout = drain })
	k := streamKey{catalog: commonv1.Catalog_CATALOG_STREAM, group: "g", stage: "hot"}
	w := &worker{p: &pusher{opts: PushOptions{Log: &bytes.Buffer{}}, rows: map[PushKey]uint64{}, done: map[string]bool{}}, items: map[string]*itemProgress{}, window: 1}
	client := &hangingClose{w: w, entered: make(chan struct{})}
	w.streams = map[streamKey]*importStream{k: {key: k, client: client, cancel: func() {}}}
	work := make(chan int)
	close(work)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.run(ctx, nil, work) }()
	<-client.entered
	cancel()
	select {
	case err := <-done:
		require.Error(t, err, "the clean close was cut short")
	case <-time.After(5 * time.Second):
		t.Fatal("a stop during the clean close does not end the wait for the end of the stream")
	}
}

// failFirstRows refuses the first acknowledgement; the next one waits until the worker's streams
// end, and records whether the worker had canceled the others by then.
type failFirstRows struct {
	transferv1.ImportService_ImportRowsClient
	w              *worker
	failedBySettle *atomic.Bool
	failed         *atomic.Bool
	recvs          int
}

func (s *failFirstRows) Recv() (*transferv1.ImportRowsResponse, error) {
	s.recvs++
	if s.recvs == 1 {
		return nil, status.Error(codes.Internal, "batch refused")
	}
	s.failedBySettle.Store(s.failed.Load())
	<-s.w.streamCtx.Done()
	return nil, s.w.streamCtx.Err()
}

// A worker that fails cancels the other workers first, and only then waits for the
// acknowledgements of its own batches in flight.
func TestWorker_FailureCancelsTheOthersBeforeTheDrain(t *testing.T) {
	drain := drainTimeout
	drainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { drainTimeout = drain })
	e := &Entry{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem}, TargetStage: "hot"}
	k := entryStreamKey(e)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var failed, failedBySettle atomic.Bool
	w := &worker{
		p:     &pusher{opts: PushOptions{Log: &bytes.Buffer{}}, rows: map[PushKey]uint64{}, done: map[string]bool{}},
		items: map[string]*itemProgress{}, window: 2,
		fail: func(err error) {
			failed.Store(true)
			cancel(err)
		},
	}
	client := &failFirstRows{w: w, failed: &failed, failedBySettle: &failedBySettle}
	w.streams = map[streamKey]*importStream{k: {key: k, client: client, cancel: func() {}, inflight: []*sentBatch{
		{entry: e, seq: 0, rows: []rowPos{{file: "f", record: 1}}}, {entry: e, seq: 1, rows: []rowPos{{file: "f", record: 2}}},
	}}}
	work := make(chan int)
	close(work)
	err := w.run(ctx, nil, work)
	require.Error(t, err)
	assert.True(t, failedBySettle.Load(), "the others are canceled before the worker drains")
	assert.ErrorContains(t, context.Cause(ctx), "batch refused")
}

// An interrupt that comes once every work item is done stopped nothing: the push succeeds.
func TestFinishPush_InterruptAfterTheLastItemIsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entries := []Entry{{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem}}}
	var log bytes.Buffer
	res, err := finishPush(ctx, &log, entries, map[string]bool{entries[0].Item.Key(): true}, &PushResult{}, nil, "rows")
	require.NoError(t, err)
	assert.Equal(t, []string{entries[0].Item.Key()}, res.Done)
	_, err = finishPush(ctx, &log, entries, map[string]bool{}, &PushResult{}, nil, "rows")
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
}

// A failure that canceled the run once every work item was done, such as a stream that did not
// close cleanly, is reported, not taken for an interrupt.
func TestFinishPush_FailureAfterTheLastItemIsReported(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("close the import stream: node failed"))
	entries := []Entry{{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem}}}
	var log bytes.Buffer
	_, err := finishPush(ctx, &log, entries, map[string]bool{entries[0].Item.Key(): true}, &PushResult{}, nil, "rows")
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
	assert.Contains(t, err.Error(), "close the import stream: node failed")
}

// endedRows is a stream the liaison ended with failure before the batch sent next reached it:
// Send returns io.EOF and Recv the status, after the acknowledgements of acks.
type endedRows struct {
	transferv1.ImportService_ImportRowsClient
	failure error
	acks    []uint64
}

func (s *endedRows) Send(*transferv1.ImportRowsRequest) error { return io.EOF }

func (s *endedRows) Recv() (*transferv1.ImportRowsResponse, error) {
	if len(s.acks) > 0 {
		seq := s.acks[0]
		s.acks = s.acks[1:]
		return &transferv1.ImportRowsResponse{BatchSeq: seq}, nil
	}
	return nil, s.failure
}

// A batch whose Send finds the stream ended by the liaison was not sent: its rows are not in the
// window a resume treats as ambiguous, which ends at the batch sent before it; the batch in flight
// before it is still acknowledged.
func TestWorker_BatchSentOnAnEndedStreamIsNotSent(t *testing.T) {
	e := &Entry{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem}, TargetStage: "hot"}
	key, k := e.Item.Key(), entryStreamKey(e)
	path := filepath.Join(t.TempDir(), "p.json")
	track, err := startTracker(context.Background(), path, newProgress(csvArtifact(), testBindings()), 1, nil)
	require.NoError(t, err)
	first, second := csvMark{File: "f", Record: 1, Rows: 1}, csvMark{File: "f", Record: 2, Rows: 2}
	require.NoError(t, track.csvStarted(key, nil, first))
	require.NoError(t, track.csvSending(key, second))
	w := &worker{
		p:     &pusher{opts: PushOptions{Log: &bytes.Buffer{}, track: track}, rows: map[PushKey]uint64{}, done: map[string]bool{}},
		items: map[string]*itemProgress{key: {started: true, sentRows: 2, lastSent: &second, restore: &first, unacked: 2}}, window: 4,
	}
	client := &endedRows{failure: status.Error(codes.Internal, "node failed"), acks: []uint64{0}}
	st := &importStream{key: k, client: client, cancel: func() {}, seq: 2, inflight: []*sentBatch{
		{entry: e, seq: 0, rows: []rowPos{{file: "f", record: 1}}}, {entry: e, seq: 1, rows: []rowPos{{file: "f", record: 2}}},
	}}
	st.pending = []row{{msg: &transferv1.ImportRowsRequest_StreamRow{}, file: "f", record: 3}}
	err = w.flush(context.Background(), st, e)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node failed")
	require.NoError(t, track.close())
	state := &progressState{}
	require.NoError(t, resume.Read(path, progressKind, progressVersion, state))
	c := state.Done[key].Cursor
	require.NotNil(t, c)
	assert.Equal(t, &second, c.Sent, "the window ends at the last batch sent")
	assert.Equal(t, uint64(1), c.Record, "the batch in flight before is acknowledged")
}
