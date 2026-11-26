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
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// countingWrites counts the writes of the progress file.
func countingWrites(t *testing.T) func() int {
	t.Helper()
	write := writeProgress
	t.Cleanup(func() { writeProgress = write })
	var mu sync.Mutex
	n := 0
	writeProgress = func(path string, v any) error {
		mu.Lock()
		n++
		mu.Unlock()
		return write(path, v)
	}
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}

// countingFoldWrites counts the states of the progress file written, by FoldSeq: every fold is
// one state, whether the persister writes it once or twice (it writes a queued fold on its own
// when it picks it up before the Sync that follows reaches the queue), and a fold the sender
// causes is one more.
func countingFoldWrites(t *testing.T) func() int {
	t.Helper()
	write := writeProgress
	t.Cleanup(func() { writeProgress = write })
	var mu sync.Mutex
	seqs := map[uint64]struct{}{}
	writeProgress = func(path string, v any) error {
		mu.Lock()
		seqs[v.(*progressState).FoldSeq] = struct{}{}
		mu.Unlock()
		return write(path, v)
	}
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seqs)
	}
}

// lockGone asserts that the lock file of the progress file of opts is gone.
func lockGone(t *testing.T, opts config.Import, when string) {
	t.Helper()
	_, err := os.Stat(resume.LockPath(ProgressPath(opts)))
	assert.True(t, errors.Is(err, os.ErrNotExist), "the lock file is gone after %s: %v", when, err)
}

func TestImport_FirstRunPrintsItsProgressFileAndNamesItOnFailure(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	run := interruptedCSV(t, f, li, opts, 3)
	path := ProgressPath(opts)
	assert.Contains(t, run.log, "progress: "+path+" (kept after the import")
	assert.Contains(t, run.err.Error(), "the progress file "+path+" is kept: run the same command again to resume (add --resume when not at a terminal)")
	assert.NotContains(t, run.err.Error(), "of a rerun are stored twice")
	assert.Equal(t, clusterOf(f, li.Addr).DataNodeNames(), readState(t, path).Bindings.TargetNodes)
}

func TestImport_UnwritableProgressFileExitsOneBeforeTheSchema(t *testing.T) {
	for _, dry := range []bool{false, true} {
		f := newFakeLiaison()
		li := f.serve(t)
		opts := resumeImport(t, writeDataArtifact(t))
		opts.DryRun = dry
		opts.Checkpoint = filepath.Join(t.TempDir(), "missing", "progress.json")
		run := runImportEnv(context.Background(), f, li, opts, nil, nil)
		assert.Equal(t, exitcode.ExitUsage, run.code(), run.log)
		require.Error(t, run.err)
		assert.Contains(t, run.err.Error(), "Give --checkpoint <a path in a writable directory>")
		assert.Empty(t, f.writeLog(), "the schema is not written")
		assert.Empty(t, f.receivedBatches())
	}
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}
	f := newFakeLiaison()
	li := f.serve(t)
	dir := writeDataArtifact(t)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	run := runImportEnv(context.Background(), f, li, resumeImport(t, dir), nil, nil)
	assert.Equal(t, exitcode.ExitUsage, run.code(), run.log)
	require.Error(t, run.err)
	assert.Contains(t, run.err.Error(), "Give --checkpoint <a path in a writable directory>", "the lock that cannot be taken names the way out")
	assert.Equal(t, 1, strings.Count(run.err.Error(), ProgressPath(resumeImport(t, dir))+" "), "the progress file is named once: %v", run.err)
	assert.Empty(t, f.writeLog(), "the schema is not written")
}

// The temporary files a crash left of the progress file are removed once its lock is held.
func TestImport_RemovesTheTemporaryFilesOfACrash(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	path := ProgressPath(opts)
	tmp, err := os.CreateTemp(filepath.Dir(path), resume.TempPattern(path))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	completeCSV(t, f, li, opts)
	assert.NoFileExists(t, tmp.Name())
}

// A cursor side file an earlier import left without its progress file, such as after a --restart
// cut short or a progress file removed by hand, is removed before a fresh import first writes its
// progress file: the progress file's FoldSeq starts at 0, so a resume after a crash in between
// would take that side file's cursors for the fresh import's and skip rows.
func TestImport_FreshImportRemovesAStaleSideFileFirst(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	path := ProgressPath(opts)
	side := resume.SidePath(path, cursorSide)
	data, err := json.Marshal(cursorState{FoldSeq: 7, Cursors: map[string]*csvCursor{streamItemKey: {File: "f", Record: 2, Rows: 2}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(side, data, 0o600))
	write := writeProgress
	t.Cleanup(func() { writeProgress = write })
	var mu sync.Mutex
	var first, staleAtFirst bool
	writeProgress = func(p string, v any) error {
		mu.Lock()
		if !first {
			first = true
			if read, readErr := os.ReadFile(side); readErr == nil && bytes.Equal(read, data) {
				staleAtFirst = true
			}
		}
		mu.Unlock()
		return write(p, v)
	}
	completeCSV(t, f, li, opts)
	assert.True(t, first)
	assert.False(t, staleAtFirst, "the stale side file is gone before the first write of the progress file")
}

func TestResumeCSV_OneProgressFilePerArtifact(t *testing.T) {
	fastPolls(t)
	dir := writeDataArtifact(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	run := runImportEnv(ctx, f, li, opts, cancelAt(4, cancel), nil)
	cancel()
	require.Equal(t, exitcode.ExitRuntime, run.code(), run.log)
	path := ProgressPath(opts)
	assert.Equal(t, filepath.Join(dir, ProgressFileName), path)
	require.FileExists(t, path)
	original, err := os.ReadFile(path)
	require.NoError(t, err)

	// The same artifact into another cluster that lacks the recorded target nodes is refused with
	// the nodes in clear: another cluster needs another --checkpoint.
	other := newFakeLiaison()
	other.nodes = []string{"data-x", "data-y"}
	otherLi := other.serve(t)
	opts.Resume = true
	refused := runImportEnv(context.Background(), other, otherLi, opts, nil, nil)
	assert.Equal(t, exitcode.ExitResume, refused.code(), refused.log)
	assert.Contains(t, refused.err.Error(), "target node data-a of the recorded import is no longer a data node of the target")
	assert.Contains(t, refused.err.Error(), "to import this artifact into another cluster give --checkpoint <another path>")
	assert.Empty(t, other.receivedBatches())
	opts.Checkpoint = filepath.Join(t.TempDir(), "other.json")
	opts.Resume = false
	fresh := runImportEnv(context.Background(), other, otherLi, opts, nil, nil)
	require.NoError(t, fresh.err, fresh.log)
	assert.NotContains(t, fresh.log, "RESUME ")

	// The same cluster with other options is refused, field by field.
	opts.Checkpoint, opts.Resume = "", true
	opts.AllSources = !opts.AllSources
	refused = runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitResume, refused.code(), refused.log)
	assert.Contains(t, refused.err.Error(), "allSources: recorded false (default), now true")
	assert.Contains(t, refused.err.Error(), "give --checkpoint <another path> to run this import separately")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, original, after)

	// A target that gained a node resumes.
	opts.AllSources = !opts.AllSources
	f.nodes = append(f.nodes, "data-new")
	batches := len(f.receivedBatches())
	resumed := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, resumed.err, resumed.log)
	assert.Contains(t, resumed.log, "RESUME ")
	assert.Len(t, f.receivedBatches(), batches+7-4, "only the batches left are sent")
}

func TestResumeCSV_ProgressWriteFailureIsARuntimeFailure(t *testing.T) {
	write := writeProgress
	t.Cleanup(func() { writeProgress = write })
	var mu sync.Mutex
	writes := 0
	failAt := 0
	writeProgress = func(path string, v any) error {
		mu.Lock()
		defer mu.Unlock()
		writes++
		if failAt > 0 && writes >= failAt {
			return errors.New("disk full")
		}
		return write(path, v)
	}
	// The first write creates the progress file; the 4th records the first finished item.
	for _, at := range []int{1, 4} {
		f := newFakeLiaison()
		li := f.serve(t)
		mu.Lock()
		writes, failAt = 0, at
		mu.Unlock()
		run := runImportEnv(context.Background(), f, li, resumeImport(t, writeDataArtifact(t)), nil, nil)
		assert.Equal(t, exitcode.ExitRuntime, run.code(), "write %d: %v", at, run.err)
		assert.Contains(t, run.err.Error(), "disk full")
		assert.Less(t, len(f.receivedBatches()), 7, "the import stops")
	}
}

// The write failure close reports is reported once: a second close, such as the deferred one of a
// run whose closeChecked failed, returns nil instead of the same failure for a second message.
func TestTracker_SecondCloseReportsNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	write := writeProgress
	t.Cleanup(func() { writeProgress = write })
	writeProgress = func(string, any) error { return errors.New("disk full") }
	track, err := startTracker(context.Background(), path, newProgress(csvArtifact(), testBindings()), 1, nil)
	require.NoError(t, err)
	require.Error(t, track.sync(nil))
	err = track.close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk full")
	assert.NoError(t, track.close())
}

// A failed write of the progress file names its path once: the write's own error names it.
func TestTracker_WriteFailureNamesThePathOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gone", "p.json")
	track, err := startTracker(context.Background(), path, newProgress(csvArtifact(), testBindings()), 1, nil)
	require.NoError(t, err)
	for _, err = range []error{track.sync(nil), track.close()} {
		require.Error(t, err)
		assert.Equal(t, 1, strings.Count(err.Error(), path), err.Error())
	}
}

// The refusal of a progress file that cannot be written keeps the cause it wraps.
func TestUnwritable_WrapsTheCause(t *testing.T) {
	err := unwritable("p.json", &os.PathError{Op: "open", Path: "p.json", Err: os.ErrPermission})
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.Equal(t, exitcode.ExitUsage, exitcode.For(err))
}

func TestResumeCSV_AnotherBydbctlHoldsTheLock(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	unlock, err := resume.Lock(ProgressPath(opts))
	require.NoError(t, err)
	defer func() { _ = unlock() }()
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, run.code(), run.log)
	assert.Contains(t, run.err.Error(), "another bydbctl is using "+ProgressPath(opts))
	assert.Contains(t, run.err.Error(), anotherClusterHint)
	assert.Empty(t, f.receivedBatches())
}

func TestLock_IsReleasedOnEveryWayOut(t *testing.T) {
	t.Run("success and a rerun", func(t *testing.T) {
		f := newFakeLiaison()
		li := f.serve(t)
		opts := defaultImport(t, writeDataArtifact(t))
		require.NoError(t, runImport(t, f, li, opts).err)
		lockGone(t, opts, "a successful import")
		require.NoError(t, runImport(t, f, li, opts).err)
		lockGone(t, opts, "a rerun of a finished import")
	})
	t.Run("failure", func(t *testing.T) {
		fastPolls(t)
		f := newFakeLiaison()
		li := f.serve(t)
		f.dropNode = "data-a"
		opts := defaultImport(t, writeDataArtifact(t))
		opts.Verify, opts.VerifyTimeout = true, 100*time.Millisecond
		run := runImport(t, f, li, opts)
		require.Equal(t, exitcode.ExitVerify, run.code(), run.log)
		lockGone(t, opts, "a failed import")
	})
	t.Run("interrupt", func(t *testing.T) {
		f := newFakeLiaison()
		li := f.serve(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.importErr = func(context.Context, *transferv1.ImportRowsRequest_Init, uint64) error {
			cancel()
			return nil
		}
		opts := defaultImport(t, writeDataArtifact(t))
		var log bytes.Buffer
		env := Env{Log: &log, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}}, Now: func() time.Time { return dataNow }}
		_, err := RunImport(ctx, env, opts)
		require.Error(t, err, log.String())
		assert.FileExists(t, ProgressPath(opts), "the interrupted import keeps its progress file")
		lockGone(t, opts, "an interrupted import")
	})
	t.Run("restart", func(t *testing.T) {
		f := newFakeLiaison()
		li := f.serve(t)
		opts := defaultImport(t, writeDataArtifact(t))
		require.NoError(t, runImport(t, f, li, opts).err)
		opts.Restart = true
		run := runImport(t, f, li, opts)
		require.NoError(t, run.err, run.log)
		assert.Contains(t, run.log, "WARN --restart:")
		lockGone(t, opts, "--restart")
	})
	t.Run("a stale lock is taken over with a warning on the import's log", func(t *testing.T) {
		f := newFakeLiaison()
		li := f.serve(t)
		opts := defaultImport(t, writeDataArtifact(t))
		// A lock file a process that is gone left, without a file lock held on it.
		require.NoError(t, os.WriteFile(resume.LockPath(ProgressPath(opts)), []byte(`{"pid":999999999,"host":"elsewhere","command":"bydbctl"}`), 0o600))
		run := runImport(t, f, li, opts)
		require.NoError(t, run.err, run.log)
		assert.Contains(t, run.log, "WARN taking over a stale lock of ")
		assert.Contains(t, run.log, resume.LockPath(ProgressPath(opts)))
		lockGone(t, opts, "an import that took a stale lock over")
	})
}

// The progress file names the plan by the digest of its side file and does not hold it.
func TestProgress_PlanIsInItsSideFile(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	path := ProgressPath(opts)
	raw := map[string]json.RawMessage{}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.NotContains(t, raw, "plan", "the plan is not in the progress file")
	assert.Contains(t, raw, "planDigest")
	side, err := os.ReadFile(resume.SidePath(path, planSide))
	require.NoError(t, err)
	var digest string
	require.NoError(t, json.Unmarshal(raw["planDigest"], &digest))
	assert.Equal(t, resume.DigestBytes(side), digest)
	assert.Len(t, readState(t, path).Plan, 3)

	// A plan side file that is not the one recorded is refused, naming it.
	require.NoError(t, os.WriteFile(resume.SidePath(path, planSide), []byte(`{"plan":[]}`+"\n"), 0o600))
	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitResume, again.code(), again.log)
	assert.Contains(t, again.err.Error(), resume.SidePath(path, planSide)+" is not the one its progress file was written with")
	require.NoError(t, os.WriteFile(resume.SidePath(path, planSide), side, 0o600))

	// --restart keeps the plan side file with the progress file.
	opts.Restart = true
	restarted := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, restarted.err, restarted.log)
	baks, err := filepath.Glob(resume.SidePath(path, planSide) + ".2*.bak")
	require.NoError(t, err)
	assert.Len(t, baks, 1)
}

// Appending to the plan rewrites its side file under a digest the progress file names first: a
// crash between the two writes leaves a side file a load accepts. A crash after the rewrite of the
// side file leaves the import unfinished, so the items appended are pushed; one before it leaves
// the old plan, and the import finished.
func TestAppendPlan_CrashBetweenTheWritesIsReadable(t *testing.T) {
	path := t.TempDir() + "/p.json"
	state := newProgress(csvArtifact(), testBindings())
	state.Plan = []plannedItem{{Key: "a"}}
	digest, err := writePlan(path, state.Plan, nil)
	require.NoError(t, err)
	state.PlanDigest = digest
	finished := time.Now().UTC()
	state.FinishedAt = &finished
	require.NoError(t, writeProgress(path, state))

	// The side file is rewritten, the progress file names the next digest only.
	next := append(slices.Clone(state.Plan), plannedItem{Key: "b"})
	_, nextDigest, err := resume.EncodeSide(recordedPlan{Plan: next})
	require.NoError(t, err)
	state.PlanNextDigest = nextDigest
	require.NoError(t, writeProgress(path, state))
	written, err := writePlan(path, next, nil)
	require.NoError(t, err)
	assert.Equal(t, nextDigest, written, "the digest announced is the digest written")

	loaded, err := loadProgress(path, state.Bindings)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, recordedKeys(loaded))
	assert.Equal(t, nextDigest, loaded.PlanDigest)
	assert.Nil(t, loaded.FinishedAt, "the items appended are pushed")

	// The crash came before the rewrite: the plan read is the old one, and the import is finished.
	_, err = writePlan(path, state.Plan, nil)
	require.NoError(t, err)
	loaded, err = loadProgress(path, state.Bindings)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, recordedKeys(loaded))
	assert.NotNil(t, loaded.FinishedAt)

	// A side file of neither digest is refused.
	_, err = writePlan(path, []plannedItem{{Key: "z"}}, nil)
	require.NoError(t, err)
	_, err = loadProgress(path, state.Bindings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not the one its progress file was written with")

	// The tracker announces, writes and records.
	state.PlanNextDigest = ""
	state.Plan = []plannedItem{{Key: "a"}}
	state.PlanDigest, err = writePlan(path, state.Plan, nil)
	require.NoError(t, err)
	track, err := startTracker(context.Background(), path, state, 1, nil)
	require.NoError(t, err)
	require.NoError(t, track.appendPlan(state, []plannedItem{{Key: "c"}}))
	require.NoError(t, track.close())
	loaded, err = loadProgress(path, state.Bindings)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "c"}, recordedKeys(loaded))
	assert.Empty(t, loaded.PlanNextDigest)
	assert.Nil(t, loaded.FinishedAt, "an import with items appended is not finished until they are pushed")
}

// The baseline records the documents of every property group, once they are counted.
func TestBaseline_RecordsPropertyDocuments(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	f.properties[CountKey{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: "sw_prop", Node: "data-a"}] = 4
	opts := verifyingImport(t)
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err, run.log)
	baseline := readState(t, ProgressPath(opts)).baseline
	require.NotNil(t, baseline)
	assert.True(t, baseline.PropertiesCounted)
	assert.Equal(t, int64(4), baseline.Properties["sw_prop"])
}

// The baseline is kept in the plan side file, which is rewritten under a digest the progress file
// names first: a crash between the two writes leaves a side file a load accepts. A baseline the
// side file holds while the progress file records none is not used.
func TestExtendBaseline_CrashBetweenTheWritesIsReadable(t *testing.T) {
	path := t.TempDir() + "/p.json"
	state := newProgress(csvArtifact(), testBindings())
	state.Plan = []plannedItem{{Key: "a"}}
	base := &Counts{
		Shards:   map[CountKey]map[string]int64{{Catalog: stream, Group: "g", Stage: "hot", Node: "a"}: {"20261016": 1}},
		Segments: map[CountKey]map[string]int64{},
	}
	digest, err := writePlan(path, state.Plan, base)
	require.NoError(t, err)
	state.PlanDigest = digest
	require.NoError(t, writeProgress(path, state))
	assert.Nil(t, readState(t, path).baseline, "no baseline time recorded: the baseline is counted again")
	at := time.Now()
	state.BaselineAt = &at
	require.NoError(t, writeProgress(path, state))
	require.NotNil(t, readState(t, path).baseline)

	merged := &Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}}
	maps.Copy(merged.Shards, base.Shards)
	merged.Shards[CountKey{Catalog: stream, Group: "new", Stage: "hot", Node: "a"}] = map[string]int64{"20261016": 9}
	_, nextDigest, err := resume.EncodeSide(planRecord(state.Plan, merged))
	require.NoError(t, err)
	// The crash: the progress file names the next digest, the side file was rewritten, and the
	// progress file was not written again.
	state.PlanNextDigest = nextDigest
	require.NoError(t, writeProgress(path, state))
	_, err = writePlan(path, state.Plan, merged)
	require.NoError(t, err)

	loaded := readState(t, path)
	require.NotNil(t, loaded.baseline)
	assert.Equal(t, int64(9), loaded.baseline.Shards[CountKey{Catalog: stream, Group: "new", Stage: "hot", Node: "a"}]["20261016"])
	assert.Equal(t, nextDigest, loaded.PlanDigest)
	assert.Empty(t, loaded.PlanNextDigest)
	assert.Equal(t, []string{"a"}, recordedKeys(loaded))
}

// A plan side file (which holds the baseline) that is missing, changed or another one is found
// before anything is asked or written, by a dry run too.
func TestResumeCSV_BadPlanSideFileExitsSixBeforeAnything(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 3)
	path := ProgressPath(opts)
	require.NotNil(t, readState(t, path).baseline)
	side := resume.SidePath(path, planSide)
	original, err := os.ReadFile(side)
	require.NoError(t, err)
	other, err := json.Marshal(planRecord(readState(t, path).Plan, emptyCounts().set(stream, "g", "hot", "a", 0, "s", 1)))
	require.NoError(t, err)
	flipped := bytes.Clone(original)
	flipped[len(flipped)/2] ^= 1
	for name, edit := range map[string]func(){
		"missing": func() { require.NoError(t, os.Remove(side)) },
		"changed": func() { require.NoError(t, os.WriteFile(side, flipped, 0o600)) },
		"another": func() { require.NoError(t, os.WriteFile(side, other, 0o600)) },
	} {
		edit()
		for _, dry := range []bool{false, true} {
			o := opts
			o.DryRun = dry
			writes, batches := len(f.writeLog()), len(f.receivedBatches())
			asked := false
			run := runImportEnv(context.Background(), f, li, o, nil, func(context.Context, string) bool {
				asked = true
				return true
			})
			assert.Equal(t, exitcode.ExitResume, run.code(), "%s dry=%v: %s", name, dry, run.log)
			assert.False(t, asked, name)
			assert.Len(t, f.writeLog(), writes, name)
			assert.Len(t, f.receivedBatches(), batches, name)
		}
		require.NoError(t, os.WriteFile(side, original, 0o600))
	}
}

// A CSV item records its start in the progress file, synced, and every later batch sent in the
// side file only; each fold writes the side file first, under the FoldSeq it then writes.
func TestTracker_CSVSentMarkAndFoldSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(csvArtifact(), testBindings())
	writes := countingWrites(t)
	track, err := startTracker(context.Background(), path, state, 64, nil)
	require.NoError(t, err)
	require.NoError(t, track.csvStarted("k", nil, csvMark{File: "f", Record: 1, Rows: 1}))
	assert.Equal(t, 1, writes(), "an item start is synced")
	disk := readState(t, path)
	assert.Equal(t, uint64(1), disk.FoldSeq)
	assert.Equal(t, &csvMark{File: "f", Record: 1, Rows: 1}, disk.Done["k"].Cursor.Sent)
	require.NoError(t, track.csvSending("k", csvMark{File: "f", Record: 3, Rows: 3}))
	assert.Equal(t, 1, writes(), "a batch sent goes to the side file only")
	side, _, err := readCursors(path, disk.FoldSeq)
	require.NoError(t, err)
	require.NotNil(t, side)
	assert.Equal(t, uint64(3), side.Cursors["k"].Sent.Rows)
	assert.Equal(t, uint64(3), side.Cursors["k"].window())
	require.NoError(t, track.close())
}

// The cursor side file is used only when it is not older than the progress file.
func TestReadCursors_OlderSideFileIsIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	data, err := json.Marshal(cursorState{FoldSeq: 3, Cursors: map[string]*csvCursor{"k": {File: "f", Record: 2, Rows: 2}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(resume.SidePath(path, cursorSide), data, 0o600))
	for _, tc := range []struct {
		foldSeq uint64
		fresh   bool
	}{{3, true}, {2, true}, {4, false}} {
		cs, lost, readErr := readCursors(path, tc.foldSeq)
		require.NoError(t, readErr)
		assert.Equal(t, tc.fresh, cs != nil, "foldSeq %d: a fold after the side file was written makes it stale", tc.foldSeq)
		assert.Equal(t, !tc.fresh, lost)
	}
}

// The side file is replaced, never rewritten in place: a write killed before the rename leaves the
// old side file whole, and its temporary file is one claimProgress removes; what is written is
// plain JSON a reader accepts. A side file that cannot be parsed refuses the resume instead of
// falling back to the older cursors of the progress file.
// The side file is written through a new temporary file each time: a symbolic link planted where
// a temporary file of it may be is not followed, and a write leaves no temporary file.
func TestTracker_SideFileWriteFollowsNoLink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	side := resume.SidePath(path, cursorSide)
	victim := filepath.Join(t.TempDir(), "victim")
	require.NoError(t, os.WriteFile(victim, []byte("kept"), 0o600))
	require.NoError(t, os.Symlink(victim, filepath.Join(dir, "."+filepath.Base(side)+".0.tmp")))
	require.NoError(t, writeCursors(side, []byte(`{"foldSeq":1}`)))
	got, err := os.ReadFile(victim)
	require.NoError(t, err)
	assert.Equal(t, "kept", string(got))
	data, err := os.ReadFile(side)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foldSeq":1}`, string(data))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.Type()&os.ModeSymlink == 0 {
			assert.False(t, resume.IsTemp(side, e.Name()), "no temporary file is left: %s", e.Name())
		}
	}
}

func TestTracker_SideFileIsReplacedNeverTorn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(csvArtifact(), testBindings())
	track, err := startTracker(context.Background(), path, state, 64, nil)
	require.NoError(t, err)
	long := strings.Repeat("k", 5000)
	require.NoError(t, track.csvStarted(long, nil, csvMark{File: "f", Record: 1, Rows: 1}))
	require.NoError(t, track.csvStarted("k2", nil, csvMark{File: "f", Record: 1, Rows: 1}))
	side := resume.SidePath(path, cursorSide)
	before, err := os.ReadFile(side)
	require.NoError(t, err)

	// Killed in the middle of the write: half the new content is in the temporary file.
	writeC := writeCursors
	t.Cleanup(func() { writeCursors = writeC })
	var tmp string
	writeCursors = func(side string, data []byte) error {
		f, createErr := os.CreateTemp(filepath.Dir(side), resume.TempPattern(side))
		require.NoError(t, createErr)
		tmp = f.Name()
		_, writeErr := f.Write(data[:len(data)/2])
		require.NoError(t, writeErr)
		require.NoError(t, f.Close())
		return errors.New("killed")
	}
	require.Error(t, track.csvSending(long, csvMark{File: "f", Record: 2, Rows: 2}))
	after, err := os.ReadFile(side)
	require.NoError(t, err)
	assert.Equal(t, before, after, "the side file is left whole")
	cs, _, err := readCursors(path, 0)
	require.NoError(t, err)
	require.NotNil(t, cs)
	assert.Equal(t, uint64(1), cs.Cursors[long].Sent.Rows)
	assert.True(t, resume.IsTemp(side, filepath.Base(tmp)), "the temporary file is one of the side file's")
	require.NoError(t, resume.RemoveTemps(side))
	assert.NoFileExists(t, tmp)

	writeCursors = writeC
	require.NoError(t, track.itemComplete(long))
	require.NoError(t, track.csvSending("k2", csvMark{File: "f", Record: 2, Rows: 2}))
	shrunk, err := os.ReadFile(side)
	require.NoError(t, err)
	assert.Less(t, len(shrunk), len(before), "a shorter content leaves no stale tail")
	cs, _, err = readCursors(path, 0)
	require.NoError(t, err)
	require.NotNil(t, cs)
	assert.NotContains(t, cs.Cursors, long)
	assert.Equal(t, uint64(2), cs.Cursors["k2"].Sent.Rows)
	require.NoError(t, track.close())

	require.NoError(t, resume.WriteFile(path, state))
	require.NoError(t, os.WriteFile(side, []byte(`{"cursors":{"k2":{"rows":1}},"pushed":[],"foldSeq":9}xx`), 0o600))
	_, _, err = readCursors(path, 0)
	require.Error(t, err)
	_, err = loadProgress(path, state.Bindings)
	assert.Equal(t, exitcode.ExitResume, exitcode.For(err))
	assert.Contains(t, err.Error(), "cursor side file")
	assert.Contains(t, err.Error(), "--checkpoint <another path>")
}

// A worker that waits for a fold to be on disk holds no lock: the others record their batches
// meanwhile.
func TestTracker_NoLockIsHeldWhileAFoldIsWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(csvArtifact(), testBindings())
	track, err := startTracker(context.Background(), path, state, 64, nil)
	require.NoError(t, err)
	require.NoError(t, track.csvStarted("k1", nil, csvMark{File: "f", Record: 1, Rows: 1}))
	writeP := writeProgress
	t.Cleanup(func() { writeProgress = writeP })
	release, writing := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	// A failed assertion ends the test with the write still blocked: the cleanup lets it return.
	t.Cleanup(func() {
		unblock.Do(func() { close(release) })
		_ = track.close()
	})
	var blocked atomic.Bool
	writeProgress = func(p string, v any) error {
		if blocked.CompareAndSwap(false, true) {
			close(writing)
			<-release
		}
		return writeP(p, v)
	}
	started := make(chan error, 1)
	go func() { started <- track.csvStarted("k2", nil, csvMark{File: "f", Record: 1, Rows: 1}) }()
	<-writing
	sent := make(chan error, 1)
	go func() { sent <- track.csvSending("k1", csvMark{File: "f", Record: 2, Rows: 2}) }()
	select {
	case err = <-sent:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a batch waits for the write of another worker's fold")
	}
	select {
	case <-started:
		t.Fatal("csvStarted returned before its fold was on disk")
	default:
	}
	unblock.Do(func() { close(release) })
	require.NoError(t, <-started)
	assert.Equal(t, uint64(1), readState(t, path).Done["k2"].Cursor.Sent.Rows)
	require.NoError(t, track.close())
}

// A second close folds and writes nothing again: the side file the first one removed stays gone.
func TestTracker_CloseTwiceLeavesNoSideFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(csvArtifact(), testBindings())
	track, err := startTracker(context.Background(), path, state, 64, nil)
	require.NoError(t, err)
	require.NoError(t, track.csvStarted("k1", nil, csvMark{File: "f", Record: 1, Rows: 1}))
	side := resume.SidePath(path, cursorSide)
	require.FileExists(t, side)
	require.NoError(t, track.close())
	assert.NoFileExists(t, side)
	require.NoError(t, track.close())
	assert.NoFileExists(t, side)
	assert.Equal(t, uint64(1), readState(t, path).Done["k1"].Cursor.Sent.Rows)
}

// payloadAnswered writes nothing by itself; a complete item keeps only its landed totals.
func TestTracker_LazyAnswersAndCompactedItems(t *testing.T) {
	art, entries, e := nativeShardEntry(t)
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(art, testBindings())
	var err error
	state.PlanDigest, err = writePlan(path, nil, nil)
	require.NoError(t, err)
	writes := countingWrites(t)
	track, err := startTracker(context.Background(), path, state, 1, nil)
	require.NoError(t, err)
	ref := partRef("n1", 1, 4)
	require.NoError(t, track.payloadSending(0, e, ref, []string{"d1", "d2"}))
	assert.Equal(t, 1, writes())
	track.payloadAnswered(0, e, ref, &payloadRecord{Nodes: []string{"d1", "d2"}, Repushed: []string{"d2"}, Rows: 4}, true)
	require.NoError(t, track.sync(nil))
	assert.Equal(t, 2, writes(), "the answer is written with the next write, not by itself")
	require.NoError(t, track.itemComplete(e.Item.Key()))
	require.NoError(t, track.close())
	r := readState(t, path).Done[e.Item.Key()]
	require.NotNil(t, r)
	assert.True(t, r.Complete)
	assert.Nil(t, r.Parts, "the parts of a complete item are dropped")
	assert.Equal(t, map[string]uint64{"d1": 4, "d2": 4}, r.Landed)
	assert.Equal(t, []string{"d2"}, r.Tainted)

	rec := newReconciler(emptyCounts(), nil)
	seedLanded(rec, readState(t, path), plannedByKey(entries), plannedByKey(entries))
	at := partTargetKey(e.Item.Catalog, e.Item.Group, e.TargetStage, e.Item.Segment, e.Item.Shard, "d1")
	assert.Equal(t, int64(4), rec.landed[at])
}

// Where a payload landed is on disk before the back-off of its retry.
func TestPushParts_AnswerIsWrittenBeforeTheRetry(t *testing.T) {
	shortenPartRetries(t)
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	var onDisk atomic.Value
	s.set(func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() != nativefmt.KindPart {
			return nil, nil, nil
		}
		if n > 0 {
			state := &progressState{}
			if data, err := os.ReadFile(path); err == nil && json.Unmarshal(data, state) == nil {
				if r := state.Done[streamShardKey]; r != nil && r.Parts["n1/1"] != nil {
					onDisk.Store(strings.Join(r.Parts["n1/1"].Nodes, ","))
				}
			}
			return nil, nil, nil
		}
		resp := okTargets("d1", "d2")
		resp.Targets[1].Success, resp.Targets[1].Error = false, "disk full"
		return []string{"d1"}, resp, nil
	}, nil)
	log, err := nativeRun(context.Background(), t, li, path, false, "")
	require.NoError(t, err, log)
	assert.Equal(t, "d1", onDisk.Load(), "the node that stored the part is recorded before the retry")
}

// --restart sets the progress file aside only once the run is about to push: a run refused
// before keeps it in place, and two restarts in the same second keep both earlier files.
func TestRestartProgress_KeepsEveryEarlierFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	now := time.Date(2026, 11, 13, 1, 2, 3, 0, time.UTC)
	plan := &Plan{}
	for i := range 2 {
		require.NoError(t, os.WriteFile(path, []byte{byte('0' + i)}, 0o600))
		require.NoError(t, os.WriteFile(resume.SidePath(path, planSide), []byte{byte('0' + i)}, 0o600))
		require.NoError(t, restartProgress(io.Discard, path, plan, importBindings{}, false, now))
		assert.NoFileExists(t, path)
	}
	baks, err := filepath.Glob(path + ".2*.bak")
	require.NoError(t, err)
	assert.Len(t, baks, 2, "the second restart does not overwrite the first one's file")
	sideBaks, err := filepath.Glob(resume.SidePath(path, planSide) + ".*.bak")
	require.NoError(t, err)
	assert.Len(t, sideBaks, 2)

	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 3)
	progress := ProgressPath(opts)
	opts.Restart, opts.AbortOnPlanChange = true, true
	f.targetsHook = func(_ string, resp *transferv1.TargetsResponse) {
		if resp.GetGroup() == "sw_stream" {
			resp.Copies = 1
			for _, sh := range resp.Shards {
				sh.NodeIds = sh.NodeIds[:1]
			}
		}
	}
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	if run.code() == exitcode.ExitPlanChanged {
		assert.FileExists(t, progress, "a run that stops before the push keeps the progress file in place")
	}
}

// --restart moves the side files, the ones of a version 1 file too, and then the progress file, every
// file under one timestamp, and removes the temporary files of the side files.
func TestRestartProgress_OneStampProgressFileLast(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	require.NoError(t, resume.WriteFile(path, newProgress(csvArtifact(), testBindings())))
	for _, side := range progressSides {
		require.NoError(t, os.WriteFile(resume.SidePath(path, side), []byte("{}"), 0o600))
	}
	now := time.Date(2026, 11, 1, 2, 3, 4, 0, time.UTC)
	stamp := now.Format("20060102T150405Z")
	// A .bak of that stamp exists already: every file moves under the next one.
	require.NoError(t, os.WriteFile(resume.SidePath(path, planSide)+"."+stamp+".bak", nil, 0o600))
	renameFails := filepath.Join(dir, "missing")
	legacy := legacySidePaths(path)
	for _, name := range legacy {
		require.NoError(t, os.WriteFile(name, []byte("v1"), 0o600))
	}
	temp := filepath.Join(dir, "."+filepath.Base(resume.SidePath(path, cursorSide))+".123.tmp")
	require.NoError(t, os.WriteFile(temp, []byte("torn"), 0o600))
	require.NoError(t, restartProgress(&bytes.Buffer{}, path, &Plan{}, testBindings(), false, now))
	assert.FileExists(t, path+"."+stamp+"-1.bak")
	for _, side := range progressSides {
		assert.FileExists(t, resume.SidePath(path, side)+"."+stamp+"-1.bak")
	}
	for _, name := range legacy {
		assert.NoFileExists(t, name, "a file of version 1 is not left behind")
		assert.FileExists(t, name+"."+stamp+"-1.bak", "it is kept with the progress file")
	}
	assert.NoFileExists(t, temp, "the temporary file of a side file is removed")
	assert.NoFileExists(t, path)
	err := restartProgress(&bytes.Buffer{}, renameFails, &Plan{}, testBindings(), false, now)
	require.Error(t, err, "nothing to move")
	assert.True(t, strings.Contains(err.Error(), "no data was sent"))
	assert.False(t, errors.Is(err, os.ErrExist))
}

// A failed write of the progress file names it, and is marked so: RunImport adds no resume hint to
// a failure that already names the progress file, and does to any other.
func TestWriteProgress_FailureIsMarkedAsNamingTheProgressFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "p.json")
	err := writeProgress(path, &progressState{})
	var named *progressNamedError
	require.ErrorAs(t, err, &named)
	assert.Contains(t, err.Error(), path)
	wrapped := exitcode.Exit(exitcode.ExitRuntime, "record the progress: %w", err)
	assert.ErrorAs(t, wrapped, &named, "the mark survives the wrapping")
	assert.NoError(t, namingProgress(nil))
}
