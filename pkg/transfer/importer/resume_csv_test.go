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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

const streamItemKey = "stream/sw_stream/hot/20261016/shard-0"

// resumeImport is the import of the data fixture one batch per row, with one worker and one
// batch in flight, so that an interruption is at a known row.
func resumeImport(t *testing.T, dir string) config.Import {
	t.Helper()
	opts := defaultImport(t, dir)
	opts.WriteBatchSize, opts.WriteInflightBatches, opts.Parallelism = 1, 1, 1
	opts.VerifyTimeout = 5 * time.Second
	return opts
}

// runImportEnv runs an import through li with ctx, hook and confirm.
func runImportEnv(ctx context.Context, f *fakeLiaison, li *cluster.Liaison, opts config.Import, hook func(string), confirm func(context.Context, string) bool) importRun {
	var out, log bytes.Buffer
	res, err := RunImport(ctx, Env{
		Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
		Now: func() time.Time { return dataNow }, Hook: hook, Confirm: confirm,
	}, opts)
	return importRun{res: res, err: err, out: out.String(), log: log.String()}
}

// storedCounts copies what the fake stored, per node and shard.
func storedCounts(f *fakeLiaison) (map[CountKey]int64, map[CountKey]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.counts), maps.Clone(f.properties)
}

// uninterrupted is what one uninterrupted import of the fixture stores.
func uninterrupted(t *testing.T) (map[CountKey]int64, map[CountKey]int64) {
	t.Helper()
	f := newFakeLiaison()
	li := f.serve(t)
	run := runImportEnv(context.Background(), f, li, resumeImport(t, writeDataArtifact(t)), nil, nil)
	require.NoError(t, run.err, run.log)
	return storedCounts(f)
}

// cancelAt returns a hook that cancels the run at the n-th acknowledged batch.
func cancelAt(n int32, cancel context.CancelFunc) func(string) {
	var seen atomic.Int32
	return func(event string) {
		if strings.HasPrefix(event, "batch ") && seen.Add(1) == n {
			cancel()
		}
	}
}

// crashAt returns a hook that, at the n-th acknowledged batch, stops every write and removal of
// the progress file and of its cursor side file, and cancels the run: the files are left as a
// kill -9 at that moment leaves them. restore makes the writes work again, for the next run.
func crashAt(t *testing.T, n int32, cancel context.CancelFunc) (hook func(string), restore func()) {
	t.Helper()
	var crashed atomic.Bool
	writeP, writeC, removeC := writeProgress, writeCursors, removeCursors
	writeProgress = func(path string, v any) error {
		if crashed.Load() {
			return nil
		}
		return writeP(path, v)
	}
	writeCursors = func(side string, data []byte) error {
		if crashed.Load() {
			return nil
		}
		return writeC(side, data)
	}
	removeCursors = func(name string) error {
		if crashed.Load() {
			return nil
		}
		return removeC(name)
	}
	t.Cleanup(func() { writeProgress, writeCursors, removeCursors = writeP, writeC, removeC })
	var seen atomic.Int32
	return func(event string) {
		if strings.HasPrefix(event, "batch ") && seen.Add(1) == n {
			crashed.Store(true)
			cancel()
		}
	}, func() { crashed.Store(false) }
}

func readState(t *testing.T, path string) *progressState {
	t.Helper()
	state := &progressState{}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, state))
	require.NoError(t, readPlan(path, state), "the plan side file")
	return state
}

// completeCSV runs the whole import of the data fixture once.
func completeCSV(t *testing.T, f *fakeLiaison, li *cluster.Liaison, opts config.Import) {
	t.Helper()
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
}

// A finished import keeps two files: the progress file and the plan side file, which holds the
// baseline too. The cursor side file and the lock file go with the run.
func TestResumeCSV_AFinishedImportKeepsTwoFiles(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = true
	completeCSV(t, f, li, opts)
	names, err := filepath.Glob(filepath.Join(opts.Input, ".import-progress*"))
	require.NoError(t, err)
	for i := range names {
		names[i] = filepath.Base(names[i])
	}
	assert.ElementsMatch(t, []string{ProgressFileName, ".import-progress-plan.json"}, names)
	state := readState(t, ProgressPath(opts))
	require.NotNil(t, state.BaselineAt)
	require.NotNil(t, state.baseline, "the plan side file holds the baseline")
	assert.NotEmpty(t, state.Plan)
}

// interruptedCSV leaves a progress file of the data fixture interrupted at the n-th batch.
func interruptedCSV(t *testing.T, f *fakeLiaison, li *cluster.Liaison, opts config.Import, n int32) importRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := runImportEnv(ctx, f, li, opts, cancelAt(n, cancel), nil)
	require.Equal(t, exitcode.ExitRuntime, run.code(), run.log)
	return run
}

func editState(t *testing.T, path string, edit func(s *progressState)) {
	t.Helper()
	state := readState(t, path)
	edit(state)
	if state.Plan != nil {
		digest, err := writePlan(path, state.Plan, state.baseline)
		require.NoError(t, err)
		state.PlanDigest = digest
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

// interruptedAfterAck leaves a progress file whose stream item has its batch seq applied by the
// fake but not acknowledged: the fake's ackGate cancels the run before the acknowledgement.
func interruptedAfterAck(t *testing.T, f *fakeLiaison, li *cluster.Liaison, opts config.Import, seq uint64) {
	t.Helper()
	// The acknowledgement comes after the stopped worker gave up waiting for it.
	drain := drainTimeout
	drainTimeout = 100 * time.Millisecond
	defer func() { drainTimeout = drain }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.setAckGate(func(streamCtx context.Context, init *transferv1.ImportRowsRequest_Init, s uint64) {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && s == seq {
			cancel()
			// The worker cancels its stream once it gave up waiting for the acknowledgement.
			<-streamCtx.Done()
		}
	})
	run := runImportEnv(ctx, f, li, opts, nil, nil)
	f.setAckGate(nil)
	require.Equal(t, exitcode.ExitRuntime, run.code(), run.log)
}

func storedStreamRows(f *fakeLiaison) int64 {
	rows, _ := storedCounts(f)
	var n int64
	for k, c := range rows {
		if k.Catalog == commonv1.Catalog_CATALOG_STREAM {
			n += c
		}
	}
	return n
}

func TestResumeCSV_InterruptedPushesStoreWhatOneRunStores(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	noRestore := func() {}
	for _, tc := range []struct {
		interrupt func(t *testing.T, f *fakeLiaison, cancel context.CancelFunc) (func(string), func())
		name      string
		// acked is how many batches the first run gets acknowledged.
		acked int
	}{
		{name: "ctx canceled from the hook", acked: 5, interrupt: func(_ *testing.T, _ *fakeLiaison, cancel context.CancelFunc) (func(string), func()) {
			return cancelAt(5, cancel), noRestore
		}},
		{name: "crash after the 5th batch", acked: 5, interrupt: func(t *testing.T, _ *fakeLiaison, cancel context.CancelFunc) (func(string), func()) {
			return crashAt(t, 5, cancel)
		}},
		{name: "crash after the 2nd batch", acked: 2, interrupt: func(t *testing.T, _ *fakeLiaison, cancel context.CancelFunc) (func(string), func()) {
			return crashAt(t, 2, cancel)
		}},
		{name: "mid-stream error", acked: 5, interrupt: func(_ *testing.T, f *fakeLiaison, _ context.CancelFunc) (func(string), func()) {
			f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error {
				if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM && seq == 2 {
					return status.Error(codes.Internal, "disk full")
				}
				return nil
			}
			return nil, noRestore
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLiaison()
			li := f.serve(t)
			dir := writeDataArtifact(t)
			opts := resumeImport(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hook, restore := tc.interrupt(t, f, cancel)
			first := runImportEnv(ctx, f, li, opts, hook, nil)
			require.Equal(t, exitcode.ExitRuntime, first.code(), first.log)
			restore()
			f.importErr = nil
			batches := len(f.receivedBatches())
			// The batch after the interruption may still reach the fake before the cancel does.
			require.GreaterOrEqual(t, batches, tc.acked, "the first run stops at the interruption")
			require.LessOrEqual(t, batches, tc.acked+1, "the first run stops at the interruption")

			opts.Resume, opts.Verify = true, true
			second := runImportEnv(context.Background(), f, li, opts, nil, nil)
			require.NoError(t, second.err, second.log)
			rows, props := storedCounts(f)
			assert.Equal(t, wantRows, rows, "every row stored once, per node and shard\n%s", second.log)
			assert.Equal(t, wantProps, props)
			assert.Equal(t, 7, len(f.receivedBatches()), "the resume sends only the %d batch(es) the first run did not get acknowledged", 7-batches)
			assert.Contains(t, second.log, "RESUME   "+ProgressPath(opts))
			assert.Regexp(t, `(?m)^VERIFY    ok`, second.log)
			assert.FileExists(t, ProgressPath(opts), "the progress file is kept after the import")
			path := ProgressPath(opts)
			state := readState(t, path)
			require.Len(t, state.Done, 3, "the resume records every item")
			for key, r := range state.Done {
				assert.True(t, r.Complete, key)
			}
			assert.Empty(t, state.Inflight)
			var pushed uint64
			for _, c := range state.Pushed {
				pushed += c.Rows
			}
			assert.Equal(t, uint64(7), pushed, "the pushed rows over both runs are the fixture's")
			assert.NoFileExists(t, resume.SidePath(path, cursorSide), "the cursor side file is removed once the import completes")
		})
	}
}

// A crash between the side file and the progress file write of an item's completion leaves the
// side file with every row of the item pushed: its cursor completes the item on a resume, so
// nothing acknowledged is pushed again.
func TestResumeCSV_CrashAtTheCompletionOfAnItem(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var crashed atomic.Bool
	writeP, writeC, removeC := writeProgress, writeCursors, removeCursors
	t.Cleanup(func() { writeProgress, writeCursors, removeCursors = writeP, writeC, removeC })
	writeProgress = func(path string, v any) error {
		if crashed.Load() {
			return nil
		}
		if s, ok := v.(*progressState); ok && s.Done[streamItemKey] != nil && s.Done[streamItemKey].Complete {
			// The side file of this fold is written; the progress file never is.
			crashed.Store(true)
			cancel()
			return nil
		}
		return writeP(path, v)
	}
	writeCursors = func(side string, data []byte) error {
		if crashed.Load() {
			return nil
		}
		return writeC(side, data)
	}
	removeCursors = func(name string) error {
		if crashed.Load() {
			return nil
		}
		return removeC(name)
	}
	first := runImportEnv(ctx, f, li, opts, nil, nil)
	require.True(t, crashed.Load(), first.log)
	crashed.Store(false)
	path := ProgressPath(opts)
	require.False(t, readState(t, path).Done[streamItemKey].Complete, "the progress file misses the completion")

	opts.Resume, opts.DuplicationPolicy, opts.Verify = true, config.DuplicationBlock, true
	second := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, second.err, second.log)
	rows, props := storedCounts(f)
	assert.Equal(t, wantRows, rows, "every row stored once\n%s", second.log)
	assert.Equal(t, wantProps, props)
	var pushed uint64
	for _, c := range readState(t, path).Pushed {
		pushed += c.Rows
	}
	assert.Equal(t, uint64(7), pushed, "the pushed rows are not counted twice")
}

func TestResumeCSV_SeveralWorkersAndBatchesInFlight(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	for _, n := range []int32{1, 3, 5} {
		f := newFakeLiaison()
		li := f.serve(t)
		opts := resumeImport(t, writeDataArtifact(t))
		opts.Parallelism, opts.WriteInflightBatches = 3, 3
		ctx, cancel := context.WithCancel(context.Background())
		first := runImportEnv(ctx, f, li, opts, cancelAt(n, cancel), nil)
		cancel()
		if first.err == nil {
			// Every item completed before the stop took effect: it stopped nothing.
			require.NotNil(t, readState(t, ProgressPath(opts)).FinishedAt, "batch %d", n)
			continue
		}
		require.Equal(t, exitcode.ExitRuntime, first.code(), first.log)
		opts.Resume, opts.Verify = true, true
		run := runImportEnv(context.Background(), f, li, opts, nil, nil)
		require.NoError(t, run.err, run.log)
		rows, props := storedCounts(f)
		// The fake counts every row it gets: a measure or property row pushed again, which the
		// target overwrites, counts twice there.
		for k, want := range wantProps {
			assert.GreaterOrEqual(t, props[k], want, "batch %d: %v", n, k)
		}
		state := readState(t, ProgressPath(opts))
		var dups int64
		for _, d := range state.Duplicates {
			dups += int64(d.Rows)
		}
		var extra int64
		for k, want := range wantRows {
			assert.GreaterOrEqual(t, rows[k], want, "batch %d: %v", n, k)
			if k.Catalog == commonv1.Catalog_CATALOG_STREAM {
				extra += rows[k] - want
			}
		}
		// Per node: every node holds a copy, so the extra rows per node are at most the duplicates.
		assert.LessOrEqual(t, extra, 2*dups, "batch %d: rows stored twice are only those the resume lists", n)
		for key, r := range state.Done {
			assert.True(t, r.Complete, "batch %d: %s", n, key)
		}
	}
}

func TestResumeCSV_CursorSkipsAcknowledgedRows(t *testing.T) {
	art, err := OpenArtifact(context.Background(), writeDataArtifact(t), true)
	require.NoError(t, err)
	items, err := BuildWorkItems(art.Manifest)
	require.NoError(t, err)
	var item *WorkItem
	for i := range items {
		if items[i].Key() == streamItemKey {
			item = &items[i]
		}
	}
	require.NotNil(t, item)
	b := newRowBuilder(layoutOf(t, &art.Schemas, commonv1.Catalog_CATALOG_STREAM, "sw_stream"), &art.Schemas, commonv1.Catalog_CATALOG_STREAM, "sw_stream")
	read := func(from *csvCursor) ([]uint64, error) {
		var records []uint64
		readErr := b.itemRows(context.Background(), art.Dir, item, from, func(r row) error {
			records = append(records, r.record)
			return nil
		})
		return records, readErr
	}
	file := item.Sources[0].Files[0].Path
	got, err := read(nil)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, got)
	got, err = read(&csvCursor{})
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, got, "a started item without an acknowledged row starts over")
	got, err = read(&csvCursor{File: file, Record: 2})
	require.NoError(t, err)
	assert.Equal(t, []uint64{3}, got)
	_, err = read(&csvCursor{File: "nodes/other.csv", Record: 2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not hold")
}

// With --checkpoint-every-batches above 1 the progress file lags the acknowledged batches: the
// cursors of the batches acknowledged since the last fold are only in the side file. A crash
// there leaves them out of the progress file, and the resume takes them from the side file, so
// it neither sends them again nor loses them.
func TestResumeCSV_AckedButNotFoldedRowsComeFromTheSideFile(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.CheckpointEveryBatches = 4
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hook, restore := crashAt(t, 6, cancel)
	first := runImportEnv(ctx, f, li, opts, hook, nil)
	require.Equal(t, exitcode.ExitRuntime, first.code(), first.log)
	restore()
	batches := len(f.receivedBatches())
	require.GreaterOrEqual(t, batches, 6)

	path := ProgressPath(opts)
	folded := readState(t, path)
	side, _, err := readCursors(path, folded.FoldSeq)
	require.NoError(t, err)
	require.NotNil(t, side, "the crash leaves the side file")
	sideRows, foldedRows := uint64(0), uint64(0)
	for _, c := range side.Pushed {
		sideRows += c.Rows
	}
	for _, c := range folded.Pushed {
		foldedRows += c.Rows
	}
	assert.Equal(t, uint64(6), sideRows, "the side file has every acknowledged batch")
	assert.Less(t, foldedRows, sideRows, "the progress file lacks the batches acknowledged since the last fold")

	opts.Resume, opts.Verify = true, true
	second := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, second.err, second.log)
	assert.Len(t, f.receivedBatches(), 7, "the resume sends only the batches the side file does not record")
	rows, props := storedCounts(f)
	assert.Equal(t, wantRows, rows, "every row stored once, per node and shard\n%s", second.log)
	assert.Equal(t, wantProps, props)
	var pushed uint64
	for _, c := range readState(t, path).Pushed {
		pushed += c.Rows
	}
	assert.Equal(t, uint64(7), pushed)
	assert.NoFileExists(t, resume.SidePath(path, cursorSide))
}

func TestResumeCSV_AppliedButNotAcknowledgedFollowsThePolicy(t *testing.T) {
	fastPolls(t)
	for _, tc := range []struct {
		policy     config.DuplicationPolicy
		log        string
		noLog      string
		seq        uint64
		code       int
		streamRows int64
	}{
		{policy: config.DuplicationBlock, seq: 1, code: exitcode.ExitHalted, streamRows: 2, log: "resumeDuplicationPolicy block: 1 payload(s)"},
		{
			policy: config.DuplicationWarn, seq: 1, streamRows: 4,
			log: "WARN " + streamItemKey + " rows after nodes/data-a/stream/sw_stream/seg-20261016/shard-0-00001.csv record 1 " +
				"up to record 2 (1 row(s) sent) may have landed already",
		},
		{policy: config.DuplicationPush, seq: 1, streamRows: 4, log: "within the 1 row(s) the resumed import pushed again", noLog: "WARN " + streamItemKey},
		// skip leaves out only the row sent; the row after it was never sent and is pushed.
		{policy: config.DuplicationSkip, seq: 1, streamRows: 3, log: "verify: on, since resumeDuplicationPolicy skip left payloads out"},
		{policy: config.DuplicationSkip, seq: 2, streamRows: 3, log: "verify: on, since resumeDuplicationPolicy skip left payloads out"},
	} {
		t.Run(string(tc.policy)+"/seq "+string(rune('0'+tc.seq)), func(t *testing.T) {
			f := newFakeLiaison()
			li := f.serve(t)
			opts := resumeImport(t, writeDataArtifact(t))
			interruptedAfterAck(t, f, li, opts, tc.seq)
			require.Equal(t, int64(tc.seq+1), storedStreamRows(f), "the stream batch is applied, not acknowledged")
			batches := len(f.receivedBatches())

			opts.Resume, opts.DuplicationPolicy = true, tc.policy
			opts.Verify = tc.policy != config.DuplicationSkip
			opts.VerifyTimeout = time.Second
			run := runImportEnv(context.Background(), f, li, opts, nil, nil)
			assert.Equal(t, tc.code, run.code(), "%v\n%s", run.err, run.log)
			if run.err != nil {
				assert.Contains(t, run.err.Error()+run.log, tc.log)
			} else {
				assert.Contains(t, run.log, tc.log)
			}
			if tc.noLog != "" {
				assert.NotContains(t, run.log, tc.noLog)
			}
			assert.Equal(t, tc.streamRows, storedStreamRows(f))
			assert.Contains(t, run.log, "AMBIGUOUS 1 payload(s)")
			if tc.policy == config.DuplicationBlock {
				assert.Len(t, f.receivedBatches(), batches, "block pushes nothing")
			}
		})
	}
}

// A first batch that reached no liaison was not sent: its rows are not ambiguous on a resume, so
// block does not halt it.
func TestResumeCSV_UnsentFirstBatchIsNotAmbiguous(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	retry := transfer.RetryBase
	transfer.RetryBase = time.Millisecond
	t.Cleanup(func() { transfer.RetryBase = retry })
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.SendRetryTimeout = 50 * time.Millisecond
	f.importErr = func(_ context.Context, init *transferv1.ImportRowsRequest_Init, _ uint64) error {
		if init.GetCatalog() == commonv1.Catalog_CATALOG_STREAM {
			return importStatus(codes.Unavailable, transfer.ImportReasonNotWritten, nil, "no node to write to")
		}
		return nil
	}
	first := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.Equal(t, exitcode.ExitRuntime, first.code(), first.log)
	c := readState(t, ProgressPath(opts)).Done[streamItemKey].Cursor
	require.NotNil(t, c)
	assert.Nil(t, c.Sent, "the batch never sent is not recorded as sent")

	f.importErr = nil
	opts.Resume, opts.DuplicationPolicy, opts.Verify = true, config.DuplicationBlock, true
	second := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, second.err, second.log)
	assert.NotContains(t, second.log, "AMBIGUOUS")
	rows, props := storedCounts(f)
	assert.Equal(t, wantRows, rows)
	assert.Equal(t, wantProps, props)
}

func TestResumeCSV_SkipKeepsForcingVerifyUntilItPasses(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedAfterAck(t, f, li, opts, 1)
	path := ProgressPath(opts)
	// The last row counts as sent too, although it never was: skip leaves it out, and verify misses it.
	editState(t, path, func(s *progressState) {
		c := s.Done[streamItemKey].Cursor
		c.Sent = &csvMark{File: c.Sent.File, Record: c.Sent.Record + 1, Rows: c.Sent.Rows + 1}
	})
	opts.Resume, opts.DuplicationPolicy, opts.Verify, opts.VerifyTimeout = true, config.DuplicationSkip, false, time.Second
	first := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.Equal(t, exitcode.ExitVerify, first.code(), first.log)
	skipped := readState(t, path).SkippedPayloads
	require.Len(t, skipped, 1)
	assert.Contains(t, skipped[0], streamItemKey)

	// A later run without --verify, with nothing left to push, still verifies what skip left out.
	opts.DuplicationPolicy = ""
	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitVerify, again.code(), again.log)
	assert.Contains(t, again.log, "verify: on, since resumeDuplicationPolicy skip left payloads out")
	assert.Contains(t, again.err.Error(), "resumeDuplicationPolicy skip left out: "+streamItemKey)
}

func TestResumeCSV_CompleteItemMayChangeItsTarget(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	path := ProgressPath(opts)
	var complete string
	editState(t, path, func(s *progressState) {
		for i := range s.Plan {
			if r := s.Done[s.Plan[i].Key]; r != nil && r.Complete {
				complete = s.Plan[i].Key
				s.Plan[i].TargetNodes = slices.Concat(s.Plan[i].TargetNodes, []string{"data-a"})
				break
			}
		}
	})
	require.NotEmpty(t, complete)
	opts.Resume = true
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	rows, props := storedCounts(f)
	assert.Equal(t, wantRows, rows, "the complete item is not pushed again")
	assert.Equal(t, wantProps, props)
}

func TestResumeCSV_CompleteItemsAreNotCheckedAgain(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	opts.DryRun = true
	dry := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, dry.err, dry.log)
	require.NotNil(t, dry.res.Plan.RowCheck)
	assert.Equal(t, 1, dry.res.Plan.RowCheck.Items, "only the item left is checked")
}

// A data node added to the target between two runs holds none of the import's rows yet: the
// resume counts it before it pushes anything and records it with the baseline, so the verify of
// the run that finishes the import passes, and a later run does not verify again.
func TestResume_DataNodeAddedBetweenRunsIsCountedBeforeThePush(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = true
	ctx, cancel := context.WithCancel(context.Background())
	first := runImportEnv(ctx, f, li, opts, cancelAt(1, cancel), nil)
	cancel()
	require.Equal(t, exitcode.ExitRuntime, first.code(), first.log)

	f.mu.Lock()
	f.nodes = append(f.nodes, "data-c")
	// The stream item, which has no progress, now goes to the node added.
	f.reroute = func(group string, r *transferv1.TargetsResponse) {
		if group == "sw_stream" {
			for _, s := range r.GetShards() {
				s.NodeIds = []string{"data-c"}
			}
		}
	}
	f.mu.Unlock()
	opts.Resume = true
	second := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, second.err, second.log)
	assert.Contains(t, second.log, "baseline: target node(s) data-c, which the baseline did not count, were counted before the push")
	stored, _ := storedCounts(f)
	var onNew int64
	for k, n := range stored {
		if k.Node == "data-c" {
			onNew += n
		}
	}
	require.Positive(t, onNew, "the resume pushes rows to the node added\n%s", second.log)

	third := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, third.err, third.log)
	assert.NotContains(t, third.log, "since no verify of this import has passed yet", third.log)
}

func TestResumeCSV_RefusesAnotherArtifactTargetOrPlan(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	run := runImportEnv(ctx, f, li, opts, cancelAt(6, cancel), nil)
	cancel()
	require.Equal(t, exitcode.ExitRuntime, run.code())
	path := ProgressPath(opts)
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	planFile := resume.SidePath(path, planSide)
	originalPlan, err := os.ReadFile(planFile)
	require.NoError(t, err)
	opts.Resume = true
	for _, tc := range []struct {
		edit func(s *progressState)
		name string
		want string
	}{
		{name: "binding", edit: func(s *progressState) { s.Bindings.Options["allSources"] = "true" }, want: "allSources: recorded true, now false"},
		{name: "target node gone", edit: func(s *progressState) {
			for i := range s.Plan {
				if s.Plan[i].Key == streamItemKey {
					s.Plan[i].TargetNodes = append(s.Plan[i].TargetNodes, "data-gone")
				}
			}
		}, want: "target node data-gone of the recorded import is no longer a data node of the target"},
		{name: "sources", edit: func(s *progressState) { s.Plan[0].Sources = []string{"data-z"} }, want: "sources [data-a], recorded [data-z]"},
		{name: "target nodes of a started item", edit: func(s *progressState) {
			for i := range s.Plan {
				if s.Plan[i].Key == streamItemKey {
					s.Plan[i].TargetNodes = []string{"data-a"}
				}
			}
		}, want: streamItemKey + ": target hot [data-a data-b], recorded hot [data-a]"},
		{name: "unknown kind", edit: func(s *progressState) { s.Kind = "export-progress" }, want: "this command resumes \"import-progress\""},
		{
			name: "older version", edit: func(s *progressState) { s.Version = 1 },
			want: "(progress file version 1, this bydbctl reads version 2), so it cannot be resumed; nothing was written. " +
				"Resume it with the bydbctl version that wrote it; or give --restart",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := readState(t, path)
			tc.edit(state)
			digest, planErr := writePlan(path, state.Plan, state.baseline)
			require.NoError(t, planErr)
			state.PlanDigest = digest
			data, marshalErr := json.Marshal(state)
			require.NoError(t, marshalErr)
			require.NoError(t, os.WriteFile(path, data, 0o600))
			batches := len(f.receivedBatches())
			rerun := runImportEnv(context.Background(), f, li, opts, nil, nil)
			assert.Equal(t, exitcode.ExitResume, rerun.code(), rerun.log)
			assert.Contains(t, rerun.err.Error(), tc.want)
			assert.Len(t, f.receivedBatches(), batches, "nothing is sent")
			after, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			assert.Equal(t, data, after, "the progress file is left as it is")
		})
		require.NoError(t, os.WriteFile(path, original, 0o600))
		require.NoError(t, os.WriteFile(planFile, originalPlan, 0o600))
	}
}

// progressFilesOf reads the progress file of path and its side files, the lock file and the
// temporary files a run creates and removes aside.
func progressFilesOf(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, ".import-progress*"))
	require.NoError(t, err)
	out := map[string][]byte{}
	for _, name := range names {
		if strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".tmp") {
			continue
		}
		data, readErr := os.ReadFile(name)
		require.NoError(t, readErr)
		out[filepath.Base(name)] = data
	}
	return out
}

// A progress file another version of bydbctl wrote is refused (exit 6) before anything is read
// of it or printed, however the command is run, and the refusal names the file, both versions
// and how to go on; the files are left as they are.
func TestResumeCSV_ProgressFileOfAnotherVersionIsRefusedWithTheNextStep(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	path := ProgressPath(opts)
	for _, version := range []int{1, progressVersion + 1} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			editState(t, path, func(s *progressState) { s.Version = version })
			before := progressFilesOf(t, opts.Input)
			batches := len(f.receivedBatches())
			want := []string{
				"an earlier import left the progress file " + path,
				fmt.Sprintf("(progress file version %d, this bydbctl reads version %d)", version, progressVersion),
				"so it cannot be resumed; nothing was written",
				"Resume it with the bydbctl version that wrote it",
				"give --restart to import from scratch",
				"kept as .bak files",
				"stream and trace data already imported into this target is then stored twice",
			}
			for name, tweak := range map[string]func(*config.Import){
				"no terminal, no --resume": func(*config.Import) {},
				"--resume":                 func(o *config.Import) { o.Resume = true },
				"--dry-run":                func(o *config.Import) { o.DryRun = true },
				"-o json":                  func(o *config.Import) { o.Resume, o.OutputFormat = true, "json" },
				"--dry-run -o yaml":        func(o *config.Import) { o.DryRun, o.OutputFormat = true, "yaml" },
			} {
				run := opts
				tweak(&run)
				got := runImportEnv(context.Background(), f, li, run, nil, nil)
				require.Equal(t, exitcode.ExitResume, got.code(), "%s: %v\n%s", name, got.err, got.log)
				for _, w := range want {
					assert.Contains(t, got.err.Error(), w, name)
				}
				for _, text := range []string{got.out, got.log} {
					assert.NotContains(t, text, "RESUME ", "%s: a file that cannot be read has no summary", name)
					assert.NotContains(t, text, "DONE ", name)
				}
				assert.Len(t, f.receivedBatches(), batches, "%s: nothing is sent", name)
				assert.Equal(t, before, progressFilesOf(t, opts.Input), "%s: the files are left as they are", name)
			}
		})
	}
}

// The next step the refusal offers works: --restart sets the progress file of another version
// aside, with its side files under one stamp, and imports everything again.
func TestResumeCSV_RestartImportsOverAProgressFileOfAnotherVersion(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	path := ProgressPath(opts)
	editState(t, path, func(s *progressState) { s.Version = 1 })
	before := progressFilesOf(t, opts.Input)

	opts.Restart = true
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	assert.Equal(t, progressVersion, readState(t, path).Version, "the new progress file is of this version")
	var stamp string
	for name, data := range before {
		sides, err := filepath.Glob(filepath.Join(opts.Input, name+".*.bak"))
		require.NoError(t, err)
		require.Len(t, sides, 1, name)
		moved, err := os.ReadFile(sides[0])
		require.NoError(t, err)
		assert.Equal(t, data, moved, "%s is kept as it was", name)
		this := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(sides[0]), name+"."), ".bak")
		assert.Equal(t, cmp.Or(stamp, this), this, "the files of one restart share a stamp")
		stamp = this
	}
}

func TestResumeCSV_DryRunComparesThePlan(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	path := ProgressPath(opts)
	editState(t, path, func(s *progressState) { s.Plan[0].Sources = []string{"data-z"} })
	opts.DryRun = true
	dry := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitResume, dry.code(), dry.log)
	assert.Contains(t, dry.err.Error(), "recorded [data-z]")
	assert.Contains(t, dry.err.Error(), "stored twice")

	// A real run compares the plans before asking.
	opts.DryRun = false
	asked := false
	run := runImportEnv(context.Background(), f, li, opts, nil, func(context.Context, string) bool {
		asked = true
		return true
	})
	assert.Equal(t, exitcode.ExitResume, run.code(), run.log)
	assert.False(t, asked, "a resume refused by its plan is not offered")
}

func TestResumeCSV_SemanticallyBrokenProgressFileExitsSix(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	path := ProgressPath(opts)
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	opts.Resume = true
	for _, tc := range []struct {
		edit func(s *progressState)
		want string
	}{
		{edit: func(s *progressState) { s.Done["stream/x"] = nil }, want: `done "stream/x" has no record`},
		{edit: func(s *progressState) {
			s.Done["stream/x"] = &itemRecord{Parts: map[string]*payloadRecord{"nosource": {}}}
		}, want: "is not <source>/"},
		{edit: func(s *progressState) { s.Done["stream/x"] = &itemRecord{Parts: map[string]*payloadRecord{"n1/x": {}}} }, want: "part id"},
		{edit: func(s *progressState) {
			s.Done["stream/x"] = &itemRecord{Blobs: map[string]*payloadRecord{"n1/BOGUS": {}}}
		}, want: `unknown payload kind "BOGUS"`},
		{edit: func(s *progressState) {
			s.Inflight["0"] = inflightPayload{Key: streamItemKey, Source: "n1", Kind: "BOGUS"}
		}, want: "unknown payload kind"},
		{edit: func(s *progressState) { s.Duplicates = []rowCount{{Catalog: "nope", Kind: "shard"}} }, want: "unknown catalog"},
	} {
		editState(t, path, tc.edit)
		run := runImportEnv(context.Background(), f, li, opts, nil, nil)
		assert.Equal(t, exitcode.ExitResume, run.code(), "%s: %v", tc.want, run.err)
		if run.err != nil {
			assert.Contains(t, run.err.Error(), tc.want)
		}
		require.NoError(t, os.WriteFile(path, original, 0o600))
	}
}

// A progress file that cannot be read says how to import anyway.
func TestResumeCSV_UnreadableProgressFileNamesAWayOut(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	require.NoError(t, os.WriteFile(ProgressPath(opts), []byte("{not json"), 0o600))
	opts.Resume = true
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitResume, run.code(), run.log)
	assert.Contains(t, run.err.Error(), "give --checkpoint <another path> to run this import separately, or --restart")
}

func TestResumeCSV_ResumeWithoutAProgressFileExitsOne(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Resume = true
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, run.code(), run.log)
	assert.Contains(t, run.err.Error(), "--resume given but no progress file at "+ProgressPath(opts)+"; without --resume a fresh import starts")
	assert.Empty(t, f.receivedBatches())
	assert.NoFileExists(t, ProgressPath(opts))
}

// Not at a terminal, an unfinished import without --resume prints the resume summary, as the
// question at a terminal follows it, then stops with exit 1; it writes nothing and changes no file.
func TestResumeCSV_NoTerminalPrintsSummaryAndChangesNothing(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 3)
	batches := len(f.receivedBatches())
	before := dirSnapshot(t, filepath.Dir(ProgressPath(opts)))
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, run.code(), run.log)
	assert.Contains(t, run.err.Error(), "add --resume to continue it (needed when not at a terminal)")
	assert.Contains(t, run.err.Error(), "give --restart to import from scratch")
	for _, want := range []string{"RESUME   ", "DONE     ", "LEFT     "} {
		assert.Contains(t, run.log, want)
	}
	assert.Len(t, f.receivedBatches(), batches, "nothing is written to the target")
	assert.Equal(t, before, dirSnapshot(t, filepath.Dir(ProgressPath(opts))), "no file is written, changed or removed")
}

// dirSnapshot maps every file under dir to its content.
func dirSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(path)
		out[path] = string(data)
		return readErr
	}))
	return out
}

func TestResumeCSV_Confirmation(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	run := runImportEnv(ctx, f, li, opts, cancelAt(5, cancel), nil)
	cancel()
	require.Equal(t, exitcode.ExitRuntime, run.code())
	batches := len(f.receivedBatches())

	noTerminal := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, noTerminal.code())
	assert.Contains(t, noTerminal.err.Error(), "add --resume to continue")
	assert.Contains(t, noTerminal.err.Error(), "give --restart to import from scratch")
	// Not at a terminal, the run stops after the summary the question would follow.
	assert.Contains(t, noTerminal.log, "RESUME   ")

	var asked []string
	no := runImportEnv(context.Background(), f, li, opts, nil, func(_ context.Context, q string) bool {
		asked = append(asked, q)
		return false
	})
	assert.Equal(t, exitcode.ExitUsage, no.code())
	assert.Contains(t, no.err.Error(), "not resumed")
	assert.Contains(t, no.err.Error(), "stream and trace data already imported is then stored twice")
	assert.Equal(t, []string{"Resume? [y/N] "}, asked)
	assert.Len(t, f.receivedBatches(), batches, "nothing is sent without a yes")

	yes := runImportEnv(context.Background(), f, li, opts, nil, func(context.Context, string) bool { return true })
	require.NoError(t, yes.err, yes.log)
	assert.Len(t, f.receivedBatches(), 7)
}

// A run that is refused for lack of --resume reads no row; one that is asked or has --resume does.
func TestResumeCSV_RefusalReadsNoRows(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	run := runImportEnv(ctx, f, li, opts, cancelAt(5, cancel), nil)
	cancel()
	require.Equal(t, exitcode.ExitRuntime, run.code())

	var calls atomic.Int32
	orig := checkRowsFn
	checkRowsFn = func(ctx context.Context, art *Artifact, target *Schemas, entries []Entry, batchBytes uint64) (*RowCheck, error) {
		calls.Add(1)
		return orig(ctx, art, target, entries, batchBytes)
	}
	t.Cleanup(func() { checkRowsFn = orig })

	noTerminal := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, noTerminal.code(), noTerminal.log)
	assert.Contains(t, noTerminal.log, "RESUME   ", "the summary is printed")
	assert.Zero(t, calls.Load(), "no row is read for a run that is refused")

	asked := runImportEnv(context.Background(), f, li, opts, nil, func(context.Context, string) bool { return false })
	assert.Equal(t, exitcode.ExitUsage, asked.code())
	assert.EqualValues(t, 1, calls.Load(), "a run at a terminal reads the rows")

	opts.Resume = true
	resumed := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, resumed.err, resumed.log)
	assert.EqualValues(t, 2, calls.Load(), "a run with --resume reads the rows")
}

func TestResumeCSV_DryRunWritesNothing(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	run := runImportEnv(ctx, f, li, opts, cancelAt(5, cancel), nil)
	cancel()
	require.Equal(t, exitcode.ExitRuntime, run.code())
	path := ProgressPath(opts)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	batches, writes := len(f.receivedBatches()), len(f.writeLog())

	opts.DryRun = true
	dry := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, dry.err, dry.log)
	assert.Contains(t, dry.log, "DONE     ")
	assert.Contains(t, dry.log, "2 work item(s), ")
	assert.Contains(t, dry.log, "LEFT     ")
	assert.Regexp(t, `1 work item\(s\), \d+ rows, est\. `, dry.log)
	assert.NotContains(t, dry.log, "AMBIGUOUS", "the stopped run collected the acknowledgements of its batches in flight")
	assert.Contains(t, dry.out, "sw_stream", "the plan of what is left")
	assert.NotRegexp(t, `(?m)^measure\s+sw_metric`, dry.out, "a finished item is not planned again")
	assert.NotContains(t, dry.out, "RESULT", "the dry run of a resume lists what is left, not the whole plan")

	opts.Restart = true
	fresh := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, fresh.err, fresh.log)
	assert.Contains(t, fresh.out, "RESULT    import", "the dry run of --restart previews the whole plan of a fresh import")
	opts.Restart = false
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Len(t, f.receivedBatches(), batches)
	assert.Len(t, f.writeLog(), writes)
}

func TestResumeCSV_DryRunStructuredOutputNamesTheProgress(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	opts.DryRun, opts.OutputFormat = true, "json"
	dry := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, dry.err, dry.log)
	var plan struct {
		Resumed *ResumedProgress `json:"resumed"`
	}
	require.NoError(t, json.Unmarshal([]byte(dry.out), &plan), dry.out)
	require.NotNil(t, plan.Resumed)
	assert.Equal(t, ProgressPath(opts), plan.Resumed.Path)
	assert.Equal(t, 3, plan.Resumed.Total)
	assert.Equal(t, 2, plan.Resumed.Done)
	assert.Nil(t, plan.Resumed.FinishedAt)

	fresh := resumeImport(t, writeDataArtifact(t))
	fresh.DryRun, fresh.OutputFormat = true, "json"
	none := runImportEnv(context.Background(), f, li, fresh, nil, nil)
	require.NoError(t, none.err, none.log)
	assert.NotContains(t, none.out, `"resumed"`)
}

// A schema-only import reads no progress file: it writes the schema over an import finished or
// interrupted, and leaves the progress file and its cursor side file as they are.
func TestImport_SchemaOnlyIgnoresTheProgressFile(t *testing.T) {
	fastPolls(t)
	for _, interrupted := range []bool{false, true} {
		f := newFakeLiaison()
		li := f.serve(t)
		opts := resumeImport(t, writeDataArtifact(t))
		path := ProgressPath(opts)
		if interrupted {
			ctx, cancel := context.WithCancel(context.Background())
			hook, restore := crashAt(t, 3, cancel)
			run := runImportEnv(ctx, f, li, opts, hook, nil)
			cancel()
			restore()
			require.Equal(t, exitcode.ExitRuntime, run.code(), run.log)
			require.FileExists(t, resume.SidePath(path, cursorSide))
		} else {
			completeCSV(t, f, li, opts)
		}
		progress, err := os.ReadFile(path)
		require.NoError(t, err)
		cursors, _ := os.ReadFile(resume.SidePath(path, cursorSide))
		batches := len(f.receivedBatches())

		opts.SchemaOnly, opts.Resume = true, true
		run := runImportEnv(context.Background(), f, li, opts, nil, nil)
		require.NoError(t, run.err, run.log)
		assert.Contains(t, run.log, "schema-only import done")
		assert.Contains(t, run.log, "NOTE a schema-only import uses no progress file")
		assert.NotContains(t, run.log, "RESUME ")
		assert.Len(t, f.receivedBatches(), batches)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, progress, after, "interrupted=%v", interrupted)
		afterCursors, _ := os.ReadFile(resume.SidePath(path, cursorSide))
		assert.Equal(t, cursors, afterCursors)
	}
}

// --resume from BYDBCTL_IMPORT_RESUME resumes a progress file without asking when there is one and
// starts a fresh import when there is none; the messages name where it came from.
func TestResume_FromTheEnvironment(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Resume = true
	run := func() importRun {
		var out, log bytes.Buffer
		res, err := RunImport(context.Background(), Env{
			Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
			Now: func() time.Time { return dataNow }, ResumeFromEnv: true,
		}, opts)
		return importRun{res: res, err: err, out: out.String(), log: log.String()}
	}

	// No progress file: the flag exits 1, the variable starts a fresh import (interrupted here).
	fresh := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, fresh.code(), "the flag without a progress file exits 1")
	ctx, cancel := context.WithCancel(context.Background())
	var out, log bytes.Buffer
	_, err := RunImport(ctx, Env{
		Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
		Now: func() time.Time { return dataNow }, ResumeFromEnv: true, Hook: cancelAt(5, cancel),
	}, opts)
	cancel()
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err), log.String())
	assert.Contains(t, log.String(), "NOTE BYDBCTL_IMPORT_RESUME is set, but there is no progress file at "+ProgressPath(opts)+": a fresh import starts")
	require.FileExists(t, ProgressPath(opts))

	// A progress file: resumed without asking, naming the variable.
	resumed := run()
	require.NoError(t, resumed.err, resumed.log)
	assert.Contains(t, resumed.log, "resume: "+ProgressPath(opts)+" is continued without asking (BYDBCTL_IMPORT_RESUME)")
	rows, props := storedCounts(f)
	assert.Equal(t, wantRows, rows)
	assert.Equal(t, wantProps, props)
}
