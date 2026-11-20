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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// lostSideReason is what the summary says when the cursor side file cannot be trusted.
const lostSideReason = "the cursor side file is missing, empty or older than the progress file; the rows up to the recorded send limit are treated as possibly landed"

// sideLosses are the states a power loss leaves the unsynced cursor side file in.
var sideLosses = map[string]func(t *testing.T, side string){
	"missing": func(t *testing.T, side string) { require.NoError(t, os.Remove(side)) },
	"empty":   func(t *testing.T, side string) { require.NoError(t, os.WriteFile(side, nil, 0o600)) },
	"all NUL": func(t *testing.T, side string) {
		data, err := os.ReadFile(side)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(side, make([]byte, len(data)), 0o600))
	},
	"older than the progress file": func(t *testing.T, side string) {
		data, err := os.ReadFile(side)
		require.NoError(t, err)
		var cs cursorState
		require.NoError(t, json.Unmarshal(data, &cs))
		cs.FoldSeq--
		data, err = json.Marshal(cs)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(side, data, 0o600))
	},
}

// crashedAfterFold leaves the progress of the data fixture as a power loss leaves it: the progress
// file was last written when the stream item had two rows acknowledged and none in flight, and the
// third row was sent and acknowledged since, which only the cursor side file records; loss says
// what became of that file. With checkpoint-every-batches 3 the sixth acknowledgement of the run,
// the second row of the stream item after the two of the measure and the two of the property item,
// is the last fold.
func crashedAfterFold(t *testing.T, loss func(t *testing.T, side string)) (*fakeLiaison, *cluster.Liaison, config.Import, int) {
	t.Helper()
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.CheckpointEveryBatches = 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hook, restore := crashAt(t, 7, cancel)
	run := runImportEnv(ctx, f, li, opts, hook, nil)
	restore()
	require.Equal(t, exitcode.ExitRuntime, run.code(), run.log)
	path := ProgressPath(opts)
	folded := readState(t, path).Done[streamItemKey].Cursor
	require.NotNil(t, folded)
	require.Equal(t, uint64(2), folded.Rows, "the progress file has two rows of the stream item acknowledged")
	side, _, err := readCursors(path, readState(t, path).FoldSeq)
	require.NoError(t, err)
	require.NotNil(t, side)
	require.Equal(t, uint64(3), side.Cursors[streamItemKey].Rows, "the side file has the third row acknowledged")
	loss(t, resume.SidePath(path, cursorSide))
	return f, li, opts, len(f.receivedBatches())
}

// The rows sent after the last fold are only in the cursor side file; once it is lost, they are
// ambiguous: the policy decides them, block halting before anything is pushed.
func TestResumeCSV_LostCursorSideFileTreatsTheRowsUpToTheSendLimitAsAmbiguous(t *testing.T) {
	for name, loss := range sideLosses {
		t.Run(name, func(t *testing.T) {
			f, li, opts, batches := crashedAfterFold(t, loss)
			opts.Resume, opts.DuplicationPolicy = true, config.DuplicationBlock
			run := runImportEnv(context.Background(), f, li, opts, nil, nil)
			assert.Equal(t, exitcode.ExitHalted, run.code(), "%v\n%s", run.err, run.log)
			assert.Contains(t, run.log, "AMBIGUOUS")
			assert.Contains(t, run.log, streamItemKey)
			assert.Contains(t, run.log, lostSideReason)
			assert.Len(t, f.receivedBatches(), batches, "block pushes nothing")
		})
	}
}

// The cursor side file a run removes after folding it into the progress file is not a lost one:
// a resume after a block halt reads the window from the progress file and says nothing about a
// lost side file.
func TestResumeCSV_SideFileFoldedByABlockedRunIsNotLost(t *testing.T) {
	f, li, opts, batches := crashedAfterFold(t, func(t *testing.T, side string) {
		// The third row was sent but its acknowledgement is not recorded: it may have landed.
		data, err := os.ReadFile(side)
		require.NoError(t, err)
		var cs cursorState
		require.NoError(t, json.Unmarshal(data, &cs))
		c := cs.Cursors[streamItemKey]
		c.Sent = &csvMark{File: c.File, Record: c.Record, Rows: c.Rows}
		c.Record, c.Rows = c.Record-1, c.Rows-1
		data, err = json.Marshal(cs)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(side, data, 0o600))
	})
	opts.Resume, opts.DuplicationPolicy = true, config.DuplicationBlock
	blocked := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.Equal(t, exitcode.ExitHalted, blocked.code(), "%v\n%s", blocked.err, blocked.log)
	assert.NotContains(t, blocked.log, lostSideReason, "the side file of the crashed run is intact")
	path := ProgressPath(opts)
	_, err := os.Stat(resume.SidePath(path, cursorSide))
	require.ErrorIs(t, err, os.ErrNotExist, "the blocked run folded the side file and removed it")

	opts.DuplicationPolicy = config.DuplicationWarn
	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "AMBIGUOUS")
	assert.Contains(t, again.log, streamItemKey)
	assert.NotContains(t, again.log, lostSideReason)
	assert.Greater(t, len(f.receivedBatches()), batches)
}

// The policy decides the rows up to the send limit as it decides any ambiguous rows.
func TestResumeCSV_LostCursorSideFileFollowsThePolicy(t *testing.T) {
	for _, tc := range []struct {
		policy     string
		log        string
		noLog      string
		code       int
		streamRows int64
	}{
		{policy: config.DuplicationBlock, code: exitcode.ExitHalted, streamRows: 3, log: "resumeDuplicationPolicy block: 1 payload(s)"},
		{
			policy: config.DuplicationWarn, streamRows: 4,
			log: "WARN " + streamItemKey + " rows after nodes/data-a/stream/sw_stream/seg-20261016/shard-0-00001.csv record 2 " +
				"up to its recorded send limit (1 row(s)) may have landed already",
		},
		{policy: config.DuplicationPush, streamRows: 4, log: "within the 1 row(s) the resumed import pushed again", noLog: "WARN " + streamItemKey},
		{policy: config.DuplicationSkip, streamRows: 3, log: "verify: on, since resumeDuplicationPolicy skip left payloads out"},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			f, li, opts, batches := crashedAfterFold(t, sideLosses["empty"])
			require.Equal(t, int64(3), storedStreamRows(f))
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
			assert.Contains(t, run.log, "AMBIGUOUS 1 payload(s)")
			assert.Contains(t, run.log, lostSideReason)
			assert.Equal(t, tc.streamRows, storedStreamRows(f))
			if tc.policy == config.DuplicationBlock {
				assert.Len(t, f.receivedBatches(), batches, "block pushes nothing")
			}
			if tc.policy == config.DuplicationSkip {
				assert.Len(t, f.receivedBatches(), batches, "skip leaves the rows that may have landed out")
				assert.True(t, readState(t, ProgressPath(opts)).Done[streamItemKey].Complete, "the window reaches the last row of the item")
			}
		})
	}
}

// A side file the resume can use keeps its window: the progress file's send limit is not read.
func TestResumeCSV_IntactCursorSideFileIsUsedAsBefore(t *testing.T) {
	f, li, opts, batches := crashedAfterFold(t, func(*testing.T, string) {})
	opts.Resume, opts.DuplicationPolicy, opts.Verify = true, config.DuplicationBlock, true
	opts.VerifyTimeout = time.Second
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	assert.NotContains(t, run.log, "AMBIGUOUS")
	assert.NotContains(t, run.log, lostSideReason)
	assert.Len(t, f.receivedBatches(), batches, "every row was acknowledged: nothing is sent again")
	assert.Equal(t, int64(3), storedStreamRows(f))
}

// A run that ends, interrupted or not, leaves no send limit: its progress file is exact, and a
// resume after the side file was removed finds nothing ambiguous.
func TestResumeCSV_EndedRunLeavesNoSendLimit(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.CheckpointEveryBatches = 3
	interruptedCSV(t, f, li, opts, 6)
	path := ProgressPath(opts)
	cursor := readState(t, path).Done[streamItemKey].Cursor
	require.NotNil(t, cursor)
	assert.Zero(t, cursor.SendLimit)
	assert.Equal(t, uint64(2), cursor.Rows)
	assert.NoFileExists(t, resume.SidePath(path, cursorSide))

	opts.Resume, opts.DuplicationPolicy = true, config.DuplicationBlock
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	assert.NotContains(t, run.log, "AMBIGUOUS")
	assert.Equal(t, int64(3), storedStreamRows(f))
}

// skip leaves out the rows up to the send limit, which has no position of its own: the position
// of its last row is found by reading the rows the way the push reads them.
func TestLocateLimitWindows_FindsTheLastRowOfTheWindow(t *testing.T) {
	art, err := OpenArtifact(context.Background(), writeDataArtifact(t), true)
	require.NoError(t, err)
	items, err := BuildWorkItems(art.Manifest)
	require.NoError(t, err)
	var e *Entry
	for i := range items {
		if items[i].Key() == streamItemKey {
			e = &Entry{Item: items[i]}
		}
	}
	require.NotNil(t, e)
	file := e.Item.Sources[0].Files[0].Path
	layout := layoutOf(t, &art.Schemas, commonv1.Catalog_CATALOG_STREAM, "sw_stream")
	builders := map[builderKey]*rowBuilder{
		{catalog: commonv1.Catalog_CATALOG_STREAM, group: "sw_stream"}: newRowBuilder(layout, &art.Schemas, commonv1.Catalog_CATALOG_STREAM, "sw_stream"),
	}
	for _, tc := range []struct {
		limit uint64
		rows  uint64
		want  uint64
	}{{limit: 2, rows: 0, want: 2}, {limit: 3, rows: 1, want: 3}, {limit: 9, rows: 1, want: 3}} {
		c := &csvCursor{Rows: tc.rows, SendLimit: tc.limit, limitWindow: true}
		if tc.rows > 0 {
			c.File, c.Record = file, tc.rows
		}
		c.Sent = &csvMark{Rows: sendLimitOf(e, c)}
		require.NoError(t, scanLimitWindows(context.Background(), art.Dir, builders, []*ambiguousPayload{{entry: e, cursor: c}}))
		assert.Equal(t, csvMark{File: file, Record: tc.want, Rows: tc.want}, *c.Sent, "%+v", tc)
	}
}

// writesOf records the send limit of every cursor of every write of the progress file.
func writesOf(t *testing.T) func() []map[string]uint64 {
	t.Helper()
	write := writeProgress
	t.Cleanup(func() { writeProgress = write })
	var mu sync.Mutex
	var out []map[string]uint64
	writeProgress = func(path string, v any) error {
		limits := map[string]uint64{}
		for k, r := range v.(*progressState).Done {
			if r.Cursor != nil {
				limits[k] = r.Cursor.SendLimit
			}
		}
		mu.Lock()
		out = append(out, limits)
		mu.Unlock()
		return write(path, v)
	}
	return func() []map[string]uint64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]uint64(nil), out...)
	}
}

// A batch is sent only once the progress file on disk holds a send limit that covers its last row.
func TestTracker_SendWaitsForTheSyncedSendLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	writes := writesOf(t)
	track, err := startTracker(context.Background(), path, newProgress(csvArtifact(), testBindings()), 4, nil)
	require.NoError(t, err)
	track.sendWindow(2, 1)
	key := (&Entry{Item: WorkItem{Group: "g"}}).Item.Key()
	require.NoError(t, track.csvStarted(key, nil, csvMark{File: "f", Record: 1, Rows: 1}))
	for row := uint64(2); row <= 40; row++ {
		before := len(writes())
		require.NoError(t, track.csvSending(key, csvMark{File: "f", Record: row, Rows: row}))
		limits := writes()
		var synced uint64
		for _, l := range limits {
			synced = max(synced, l[key])
		}
		assert.GreaterOrEqual(t, synced, row, "row %d is sent after a synced write that covers it", row)
		if len(limits) > before {
			assert.Less(t, row, synced, "a write the sender waited for raises the limit past the row")
		}
	}
	require.NoError(t, track.close())
}

// With the limit put ahead as far as the checkpoint interval and the batches in flight, the sender
// never writes the progress file itself in a steady run: every state written is a fold, and a fold
// is one per --checkpoint-every-batches acknowledgements.
func TestTracker_SteadyRunAddsNoSyncedWriteForTheSendLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	writes := countingFoldWrites(t)
	const every, window, batches = 4, 2, 80
	track, err := startTracker(context.Background(), path, newProgress(csvArtifact(), testBindings()), every, nil)
	require.NoError(t, err)
	track.sendWindow(window, 1)
	entry := &Entry{Item: WorkItem{Group: "g"}}
	key := entry.Item.Key()
	file := func(i int) csvMark { return csvMark{File: "f", Record: uint64(i), Rows: uint64(i)} }
	require.NoError(t, track.csvStarted(key, nil, file(1)))
	// window batches are in flight: batch i is sent once batch i-window is acknowledged.
	acked := 0
	for i := 2; i <= batches; i++ {
		require.NoError(t, track.csvSending(key, file(i)))
		if i-acked > window {
			acked++
			require.NoError(t, track.csvAcked(entry, PushKey{}, rowPos{file: "f", record: uint64(acked)}, 1))
		}
	}
	// The state of the item start, and one fold per `every` acknowledgements.
	assert.Equal(t, 1+acked/every, writes())
}

// A side file a power loss left missing, empty or all NUL is lost, not unusable: the resume goes
// on from the progress file, which names the rows that may have landed.
func TestReadCursors_PowerLossStatesAreLostNotUnusable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	side := resume.SidePath(path, cursorSide)
	for name, loss := range sideLosses {
		data, err := json.Marshal(cursorState{FoldSeq: 3, Cursors: map[string]*csvCursor{"k": {Rows: 2}}})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(side, data, 0o600))
		loss(t, side)
		cs, lost, readErr := readCursors(path, 3)
		require.NoError(t, readErr, name)
		assert.Nil(t, cs, name)
		assert.True(t, lost, name)
	}
}
