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

package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

const keptText = "are kept\n  to resume: run the same command again"

// unitBudget lets n units finish across every Export stream of the fake; the next unit is
// interrupted: stop == nil fails the stream, otherwise stop is called (an operator's Ctrl-C)
// and the unit waits for the cancellation. With partial, the interrupted unit first writes its
// first file whole, an extra volume no rerun writes, and the start of a third file (a .tmp).
func unitBudget(n int, partial bool, stop func()) func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
	var mu sync.Mutex
	finished := 0
	return scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		mu.Lock()
		if finished < n {
			finished++
			mu.Unlock()
			return sendUnit(stream, f.filesFor(req), nil)
		}
		mu.Unlock()
		if partial {
			if err := sendPartialUnit(stream, req, f.filesFor(req)[0]); err != nil {
				return err
			}
		}
		if stop == nil {
			return status.Error(codes.Internal, "part read failed (injected)")
		}
		stop()
		<-stream.Context().Done()
		return stream.Context().Err()
	})
}

// sendPartialUnit starts a unit and streams file whole, an extra volume of it and half of a
// third file.
func sendPartialUnit(stream transferv1.ExportService_ExportServer, req *transferv1.ExportRequest, file fakeFile) error {
	extra := strings.Replace(file.path, "-00001.csv", "-00099.csv", 1)
	torn := strings.Replace(file.path, "-00001.csv", "-00098.csv", 1)
	frames := []*transferv1.ExportResponse{{Frame: &transferv1.ExportResponse_UnitStarted{UnitStarted: &transferv1.UnitStarted{
		TotalParts: uint32(len(req.GetSegment().GetShardIds())),
	}}}}
	for _, p := range []string{file.path, extra} {
		frames = append(frames,
			&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{RelativePath: p, ShardId: file.shard}}},
			&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: file.content}}},
			&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileFinished{FileFinished: &transferv1.FileFinished{
				Bytes: uint64(len(file.content)), Crc32: crc32.ChecksumIEEE(file.content), RowCounts: file.rows, SchemaModRevisions: file.revs,
			}}})
	}
	frames = append(frames,
		&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{RelativePath: torn, ShardId: file.shard}}},
		&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: file.content[:4]}}})
	for _, r := range frames {
		if err := stream.Send(r); err != nil {
			return err
		}
	}
	return nil
}

// tree reads every file below dir, keyed by its slash path.
func tree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(dir, p)
		out[filepath.ToSlash(rel)] = body
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// requireSameArtifact compares two artifact directories file by file; the manifests are
// compared without exportedAt.
func requireSameArtifact(t *testing.T, want, got string) {
	t.Helper()
	a, b := tree(t, want), tree(t, got)
	if !slices.Equal(slices.Sorted(maps.Keys(a)), slices.Sorted(maps.Keys(b))) {
		t.Fatalf("file sets differ:\nwant %v\ngot  %v", slices.Sorted(maps.Keys(a)), slices.Sorted(maps.Keys(b)))
	}
	for name, body := range a {
		other := b[name]
		if name == manifest.FileName {
			body, other = withoutExportedAt(t, body), withoutExportedAt(t, other)
		}
		if !bytes.Equal(body, other) {
			t.Fatalf("%s differs:\nwant %s\ngot  %s", name, body, other)
		}
	}
}

func withoutExportedAt(t *testing.T, body []byte) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	delete(m, "exportedAt")
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// requireUntouched checks that a refused resume changed nothing in the output directory.
func requireUntouched(t *testing.T, before map[string][]byte, dir string) {
	t.Helper()
	after := tree(t, dir)
	if !maps.EqualFunc(before, after, bytes.Equal) {
		t.Fatalf("a refused resume must leave the directory as it was:\nbefore %v\nafter  %v",
			slices.Sorted(maps.Keys(before)), slices.Sorted(maps.Keys(after)))
	}
}

// baseline runs an uninterrupted export of a fresh fake, set up by setup, into its own directory.
func baseline(t *testing.T, setup func(*fakeLiaison, *testOptions)) string {
	t.Helper()
	f := exportFake()
	opts, log := exportOptions(t, f)
	setup(f, &opts)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("baseline: %v\n%s", err, log)
	}
	return opts.Output
}

func csvSetup(*fakeLiaison, *testOptions) {}

func nativeSetup(_ *fakeLiaison, opts *testOptions) {
	opts.Format = transferv1.Format_FORMAT_NATIVE
	opts.NodeRowRateLimit = 0
}

func serialSetup(_ *fakeLiaison, opts *testOptions) { opts.Parallelism = 1 }

// interrupted runs an export of f that the script interrupts, and checks it failed with the
// session and the progress file kept.
func interrupted(ctx context.Context, t *testing.T, f *fakeLiaison, opts testOptions, log *syncBuffer) {
	t.Helper()
	err := RunExport(ctx, envFor(t, opts, f), opts.Export)
	requireExit(t, err, exitcode.ExitRuntime, "")
	requireKept(t, f, log)
	if !strings.Contains(log.String(), "are kept\n  to resume: run the same command again (add --resume when not at a terminal)\n  to give up: ") {
		t.Fatalf("the resume hint is missing:\n%s", log)
	}
	if _, statErr := os.Stat(ProgressPath(opts.Export)); statErr != nil {
		t.Fatalf("an interrupted export keeps its progress file: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(statErr) {
		t.Fatal("an interrupted export writes no manifest")
	}
}

// An interrupted export explains on Err how to resume or give up, before its error; Log keeps
// the information and nothing of that explanation.
func TestRunExport_FailureExplanationGoesToErr(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	errOut := &syncBuffer{}
	opts.Err = errOut
	f.exportScript = unitBudget(1, true, nil)
	opts.Parallelism = 1
	requireExit(t, RunExport(context.Background(), envFor(t, opts, f), opts.Export), exitcode.ExitRuntime, "")
	if !strings.Contains(errOut.String(), "are kept\n  to resume: run the same command again") {
		t.Fatalf("Err lacks the resume hint:\n%s", errOut)
	}
	if strings.Contains(log.String(), "to resume:") || !strings.Contains(log.String(), "export session feedfeed created") {
		t.Fatalf("Log has the explanation of the failure, or lacks the information:\n%s", log)
	}
}

// resumeRun runs the same export again, with --resume, over a fake whose streams finish.
func resumeRun(t *testing.T, f *fakeLiaison, opts testOptions) (*syncBuffer, error) {
	t.Helper()
	f.mu.Lock()
	f.exportScript = nil
	f.mu.Unlock()
	log := &syncBuffer{}
	opts.Log, opts.Out = log, log
	opts.Resume = true
	return log, runExport(t, f, opts)
}

// interruptAfter interrupts an export, one node at a time in id order, after n finished units:
// the interrupted unit leaves an extra volume and a .tmp, and the progress file is kept.
func interruptAfter(t *testing.T, n int) (*fakeLiaison, testOptions) {
	t.Helper()
	f := exportFake()
	opts, log := exportOptions(t, f)
	f.exportScript = unitBudget(n, true, nil)
	opts.Parallelism = 1
	interrupted(context.Background(), t, f, opts, log)
	return f, opts
}

// interruptAfterOne is interruptAfter one finished unit.
func interruptAfterOne(t *testing.T) (*fakeLiaison, testOptions) {
	t.Helper()
	return interruptAfter(t, 1)
}

// interruptNativeAfterOne is interruptAfterOne for a native export.
func interruptNativeAfterOne(t *testing.T) (*fakeLiaison, testOptions) {
	t.Helper()
	f := exportFake()
	opts, log := exportOptions(t, f)
	nativeSetup(f, &opts)
	f.exportScript = unitBudget(1, true, nil)
	opts.Parallelism = 1
	interrupted(context.Background(), t, f, opts, log)
	return f, opts
}

func readState(t *testing.T, path string) *exportProgress {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	state := &exportProgress{}
	if err = json.Unmarshal(body, state); err != nil {
		t.Fatal(err)
	}
	return state
}

// editState changes the progress file of opts in place.
func editState(t *testing.T, opts *testOptions, change func(*exportProgress)) {
	t.Helper()
	path := ProgressPath(opts.Export)
	state := readState(t, path)
	change(state)
	if err := resume.WriteFile(path, state); err != nil {
		t.Fatal(err)
	}
}

// failNthWrite makes the nth write of a progress file fail, and every later one with later.
func failNthWrite(t *testing.T, n int, later bool) {
	t.Helper()
	saved := writeProgress
	t.Cleanup(func() { writeProgress = saved })
	var mu sync.Mutex
	writes := 0
	writeProgress = func(path string, v any) error {
		mu.Lock()
		writes++
		w := writes
		mu.Unlock()
		if w == n || (later && w > n) {
			return errors.New("disk full (injected)")
		}
		return saved(path, v)
	}
}

// requireNodeDone fails unless every unit of node is done in the progress file and one of
// another node is not.
func requireNodeDone(t *testing.T, opts testOptions, node string) {
	t.Helper()
	state, _, jobs, err := loadProgress(ProgressPath(opts.Export), opts.Export)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Done) == 0 {
		t.Fatal("no unit done")
	}
	if left := nodesLeft(jobs); slices.Contains(left, node) || len(left) == 0 {
		t.Fatalf("want every unit of %s done and others left, nodes with units left: %v", node, left)
	}
}

func manifestTimezone(t *testing.T, opts testOptions, node string) string {
	t.Helper()
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range m.Source.Nodes {
		if n.ID == node {
			return n.Timezone
		}
	}
	t.Fatalf("the manifest lacks node %s: %+v", node, m.Source.Nodes)
	return ""
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Ctrl-C ends the export with "export interrupted" and its cause, not with the error of a node
// stream the interrupt canceled.
func TestRunExport_InterruptSaysInterrupted(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	csvSetup(f, &opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.exportScript = unitBudget(1, true, cancel)
	err := RunExport(ctx, envFor(t, opts, f), opts.Export)
	requireExit(t, err, exitcode.ExitRuntime, "export interrupted: context canceled")
	if strings.Contains(err.Error(), "rpc error") || strings.Contains(err.Error(), "node ") {
		t.Fatalf("an interrupt names no canceled node stream: %v", err)
	}
}

func TestResume_AfterAnInterruptEqualsAnUninterruptedExport(t *testing.T) {
	for _, tc := range []struct {
		setup  func(*fakeLiaison, *testOptions)
		name   string
		cancel bool
	}{
		{name: "csv stream error", setup: csvSetup},
		{name: "csv ctrl-c", setup: csvSetup, cancel: true},
		{name: "native stream error", setup: nativeSetup},
		{name: "native ctrl-c", setup: nativeSetup, cancel: true},
	} {
		for _, after := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("%s after %d", tc.name, after), func(t *testing.T) {
				want := baseline(t, tc.setup)
				f := exportFake()
				opts, log := exportOptions(t, f)
				tc.setup(f, &opts)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var stop func()
				if tc.cancel {
					stop = cancel
				}
				f.exportScript = unitBudget(after, true, stop)
				interrupted(ctx, t, f, opts, log)
				// How many units the interrupted run recorded depends on which unit_finished frames
				// the client read before the streams went down; the resume keeps exactly those.
				kept := len(readState(t, ProgressPath(opts.Export)).Done)
				if kept > after || (!tc.cancel && after > 0 && kept == 0) {
					t.Fatalf("%d unit(s) finished before the interrupt, the progress file records %d", after, kept)
				}

				rlog, err := resumeRun(t, f, opts)
				if err != nil {
					t.Fatalf("resume: %v\n%s", err, rlog)
				}
				requireSameArtifact(t, want, opts.Output)
				if got := f.releasedIDs(); len(got) != 1 || got[0] != sessID {
					t.Fatalf("the resumed export releases the session, released %v", got)
				}
				if plan := f.lastPlan(); plan.GetRead().GetId() != sessID {
					t.Fatalf("a resume reads the session, last plan %v", plan)
				}
				for _, w := range []string{"RESUME   " + ProgressPath(opts.Export), "session feedfeed (export)", "resuming export session feedfeed"} {
					if !strings.Contains(rlog.String(), w) {
						t.Fatalf("log lacks %q:\n%s", w, rlog)
					}
				}
				if kept > 0 && !strings.Contains(rlog.String(), fmt.Sprintf("RESUMED %d unit(s)", kept)) {
					t.Fatalf("a resumed run names the %d unit(s) it kept:\n%s", kept, rlog)
				}
				if kept == 0 && strings.Contains(rlog.String(), "RESUMED ") {
					t.Fatalf("nothing was kept:\n%s", rlog)
				}
			})
		}
	}
}

func TestResume_RemovesWhatNoFinishedUnitLists(t *testing.T) {
	f, opts := interruptAfterOne(t)
	var leftovers []string
	for name := range tree(t, opts.Output) {
		if strings.HasSuffix(name, ".tmp") || strings.Contains(name, "-00099.csv") {
			leftovers = append(leftovers, name)
		}
	}
	if len(leftovers) != 2 {
		t.Fatalf("the interrupted unit leaves an extra volume and a .tmp, got %v", leftovers)
	}
	// Workers find no leftover when they start.
	f.mu.Lock()
	f.exportScript = scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		for _, name := range leftovers {
			if _, err := os.Stat(filepath.Join(opts.Output, filepath.FromSlash(name))); !os.IsNotExist(err) {
				return status.Errorf(codes.Internal, "%s is still there when the units start", name)
			}
		}
		return sendUnit(stream, f.filesFor(req), nil)
	})
	f.mu.Unlock()
	opts.Resume = true
	log := &syncBuffer{}
	opts.Log, opts.Out = log, log
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	requireSameArtifact(t, baseline(t, serialSetup), opts.Output)
}

// A resume exports only the units not done; a finished one is not asked for again.
func TestResume_DoesNotRequestAFinishedUnitAgain(t *testing.T) {
	f, opts := interruptAfterOne(t)
	count := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		n := 0
		for _, reqs := range f.exportUnits {
			n += len(reqs)
		}
		return n
	}
	before := count()
	if log, err := resumeRun(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if got := count() - before; got != 3 {
		t.Fatalf("the resume asks for the 3 units left, asked for %d", got)
	}
}

// A resume interrupted again resumes again.
func TestResume_InterruptedAgainThenResumed(t *testing.T) {
	f, opts := interruptAfterOne(t)
	f.mu.Lock()
	f.exportScript = unitBudget(1, false, nil) // this time without leftovers
	f.mu.Unlock()
	log := &syncBuffer{}
	again := opts
	again.Log, again.Out, again.Resume = log, log, true
	requireExit(t, runExport(t, f, again), exitcode.ExitRuntime, "")
	if done := readState(t, ProgressPath(opts.Export)).Done; len(done) != 2 {
		t.Fatalf("the second run finished one more unit: %v\n%s", done, log)
	}
	rlog, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	if !strings.Contains(rlog.String(), "RESUMED 2 unit(s)") {
		t.Fatalf("log:\n%s", rlog)
	}
	requireSameArtifact(t, baseline(t, serialSetup), opts.Output)
}

// --max-file-size only decides where the files of a unit roll over, and an interrupted unit is
// exported again from its start: a resume may change it with a WARN, the units left get the new
// limit and the progress file records it.
func TestResume_AnotherMaxFileSizeIsKeptForTheUnitsLeft(t *testing.T) {
	f, opts := interruptAfterOne(t)
	f.mu.Lock()
	f.exportInits = nil
	f.mu.Unlock()
	again := opts
	again.MaxFileSize = 64 << 20
	rlog, err := resumeRun(t, f, again)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	want := "WARN --max-file-size differs from the earlier run(s) (maxFileSize: recorded 512.0 MiB (default), now 64.0 MiB): " +
		"the files of the 1 unit(s) already exported keep their sizes; the "
	if !strings.Contains(rlog.String(), want) {
		t.Fatalf("want %q in:\n%s", want, rlog)
	}
	f.mu.Lock()
	inits := f.exportInits
	f.mu.Unlock()
	if len(inits) == 0 {
		t.Fatal("nothing was exported")
	}
	for _, init := range inits {
		if init.GetMaxFileSize() != 64<<20 {
			t.Fatalf("a unit left is exported with max_file_size %d", init.GetMaxFileSize())
		}
	}
}

// A unit whose files are on disk but whose unit_finished never reached the progress file (the
// process died first) is exported again: its files are cleaned up like any leftover.
func TestResume_AFinishedUnitTheProgressFileMissedIsExportedAgain(t *testing.T) {
	f, opts := interruptAfterOne(t)
	editState(t, &opts, func(s *exportProgress) { s.Done = []doneUnit{} })
	rlog, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	if strings.Contains(rlog.String(), "RESUMED ") {
		t.Fatalf("nothing was kept:\n%s", rlog)
	}
	requireSameArtifact(t, baseline(t, serialSetup), opts.Output)
}

// A crash after the manifest was written but before the progress file was deleted leaves an
// export whose units are all done: the resume writes the manifest again without the session,
// which may be gone by then.
func TestResume_AllUnitsDoneNeedsNoSession(t *testing.T) {
	saved := writeProgress
	t.Cleanup(func() { writeProgress = saved })
	var mu sync.Mutex
	var last, plan []byte
	writeProgress = func(path string, v any) error {
		if err := saved(path, v); err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		side, _ := os.ReadFile(resume.SidePath(path, planSide))
		mu.Lock()
		last, plan = body, side
		mu.Unlock()
		return err
	}
	f := exportFake()
	opts, log := exportOptions(t, f)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	writeProgress = saved
	want := filepath.Join(t.TempDir(), "want")
	if err := os.CopyFS(want, os.DirFS(opts.Output)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ProgressPath(opts.Export), last, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resume.SidePath(ProgressPath(opts.Export), planSide), plan, 0o600); err != nil {
		t.Fatal(err)
	}
	// The crash came before the manifest was written; a data node left the cluster since,
	// which does not matter: nothing is read from it.
	if err := os.Remove(filepath.Join(opts.Output, manifest.FileName)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.readCode = codes.NotFound
	f.dataNodes = f.dataNodes[:1]
	plans := len(f.plans)
	f.mu.Unlock()
	dryLog := &syncBuffer{}
	dry := opts
	dry.Log, dry.Out = dryLog, dryLog
	if found, dryErr := DryRunResume(envFor(t, dry, f), dry.Export); !found || dryErr != nil ||
		!strings.Contains(dryLog.String(), "NOTE every unit is done: the export resumes by writing the manifest, without export session feedfeed") {
		t.Fatalf("%v %v\n%s", found, dryErr, dryLog)
	}
	rlog, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	if len(f.plans) != plans {
		t.Fatal("an export whose units are all done reads no session")
	}
	requireSameArtifact(t, want, opts.Output)
	for _, w := range []string{"every unit of export session feedfeed is done (4); writing the manifest", "RESUMED 4 unit(s)"} {
		if !strings.Contains(rlog.String(), w) {
			t.Fatalf("log lacks %q:\n%s", w, rlog)
		}
	}
}

// A session lost when every unit is already on disk does not throw the export away: the
// progress file records every unit done, and the same command writes the manifest without the
// session.
func TestRunExport_ASessionLostAfterEveryUnitIsDoneKeepsTheExport(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	shortHeartbeat(t, 20*time.Millisecond)
	var finished atomic.Int32
	// Every node sends its units and then waits for the client to go away instead of ending its
	// stream; once all have, the heartbeat finds the session gone.
	f.exportScript = func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		if err := sendReady(init, stream); err != nil {
			return err
		}
		for {
			req, err := f.recvUnit(init, stream)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err = sendUnit(stream, f.filesFor(req), nil); err != nil {
				return err
			}
		}
		if finished.Add(1) == 2 {
			f.mu.Lock()
			f.hbCode = codes.FailedPrecondition
			f.mu.Unlock()
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	err := runExport(t, f, opts)
	if err != nil {
		requireExit(t, err, exitcode.ExitRuntime, "")
		if strings.Contains(log.String(), "so this export cannot be resumed") || !strings.Contains(log.String(), "to resume: run the same command again") {
			t.Fatalf("an export whose units are all done is offered a resume:\n%s", log)
		}
		f.mu.Lock()
		f.hbCode = codes.OK
		f.mu.Unlock()
		resumed, resumeErr := resumeRun(t, f, opts)
		if resumeErr != nil {
			t.Fatalf("%v\n%s", resumeErr, resumed)
		}
		if !strings.Contains(resumed.String(), "every unit of export session feedfeed is done (4); writing the manifest") {
			t.Fatalf("the resume writes the manifest only:\n%s", resumed)
		}
	}
	if _, statErr := os.Stat(filepath.Join(opts.Output, manifest.FileName)); statErr != nil {
		t.Fatalf("the export completes: %v\n%s", statErr, log)
	}
}

// A crash between the session's creation and the plan's first write leaves a progress file
// without a plan: the summary does not claim nothing is left, and the resume exports it all.
func TestResume_WithoutARecordedPlan(t *testing.T) {
	failNthWrite(t, 2, false)
	f := exportFake()
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), exitcode.ExitRuntime, "disk full (injected)")
	if state := readState(t, ProgressPath(opts.Export)); state.PlanDigest != "" || state.Session.ID != sessID {
		t.Fatalf("the session and no plan are recorded: %+v\n%s", state, log)
	}

	errLog := &syncBuffer{}
	dry := opts
	dry.Log, dry.Out = errLog, errLog
	found, err := DryRunResume(envFor(t, dry, f), dry.Export)
	if !found || err != nil {
		t.Fatalf("found=%v err=%v", found, err)
	}
	for _, w := range []string{"DONE     0 unit(s)", "LEFT     unknown: no plan is recorded yet", "the plan of what is left is read from the export session"} {
		if !strings.Contains(errLog.String(), w) {
			t.Fatalf("dry run lacks %q:\n%s", w, errLog)
		}
	}

	rlog, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	for _, w := range []string{"LEFT     unknown", "resuming export session feedfeed: 0 unit(s) done, 4 left"} {
		if !strings.Contains(rlog.String(), w) {
			t.Fatalf("log lacks %q:\n%s", w, rlog)
		}
	}
	requireSameArtifact(t, baseline(t, csvSetup), opts.Output)
}

// The groups of a resumed manifest are those of the first run: a lifecycle changed since adds
// no stage warning, and a group created since is not written into the schema.
func TestResume_KeepsTheGroupsOfTheFirstRun(t *testing.T) {
	warm := func(f *fakeLiaison, o *testOptions) {
		f.planFrames[2].GetUnits().Stage = "warm"
		o.Parallelism = 1
	}
	for _, tc := range []struct {
		change func(*fakeLiaison)
		name   string
		after  int
	}{
		{name: "lifecycle changed", after: 1, change: func(f *fakeLiaison) { f.groups[0].ResourceOpts.Stages = nil }},
		{name: "group created", after: 0, change: func(f *fakeLiaison) {
			f.groups = append(f.groups, group("sw_new", commonv1.Catalog_CATALOG_MEASURE))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := baseline(t, warm)
			f := exportFake()
			opts, log := exportOptions(t, f)
			warm(f, &opts)
			f.exportScript = unitBudget(tc.after, true, nil)
			interrupted(context.Background(), t, f, opts, log)
			f.mu.Lock()
			tc.change(f)
			f.mu.Unlock()
			rlog, err := resumeRun(t, f, opts)
			if err != nil {
				t.Fatalf("%v\n%s", err, rlog)
			}
			if strings.Contains(rlog.String(), "reports stage") {
				t.Fatalf("no stage warning:\n%s", rlog)
			}
			requireSameArtifact(t, want, opts.Output)
		})
	}
}

func TestResume_ExportedAtIsTheFirstRunsStart(t *testing.T) {
	f, opts := interruptAfterOne(t)
	// The first run started an hour ago: the manifest says so, not when this run started.
	editState(t, &opts, func(s *exportProgress) { s.StartedAt = s.StartedAt.Add(-time.Hour) })
	state := readState(t, ProgressPath(opts.Export))
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if want := state.StartedAt.UTC().Format(time.RFC3339); m.ExportedAt != want {
		t.Fatalf("exportedAt = %s, want the first run's start %s", m.ExportedAt, want)
	}
	for _, w := range []string{" (this run)\ncumulative time ", " over every run, the first started at " + state.StartedAt.Local().Format(time.RFC3339) + "\n"} {
		if !strings.Contains(log.String(), w) {
			t.Fatalf("summary lacks %q:\n%s", w, log)
		}
	}
}

// A crash after the manifest is written and before the progress file is deleted leaves both:
// the same command then writes the manifest again, and it must be the same bytes, or an import
// begun from the first one refuses to resume. Both runs take the export time from the first run's
// start, however long the schema took before the manifest was made.
func TestResume_ManifestWrittenAgainIsTheSame(t *testing.T) {
	f := exportFake()
	f.schema.firstDelay = 1100 * time.Millisecond
	opts, log := exportOptions(t, f)
	path := ProgressPath(opts.Export)
	saved := t.TempDir()
	keep := []string{path, resume.SidePath(path, planSide)}
	f.onRelease = func() {
		for _, p := range keep {
			body, err := os.ReadFile(p)
			if err != nil {
				t.Error(err)
				return
			}
			if err = os.WriteFile(filepath.Join(saved, filepath.Base(p)), body, 0o600); err != nil {
				t.Error(err)
			}
		}
	}
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	f.onRelease = nil
	first, err := os.ReadFile(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range keep {
		body, readErr := os.ReadFile(filepath.Join(saved, filepath.Base(p)))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if err = os.WriteFile(p, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	resumed, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, resumed)
	}
	again, err := os.ReadFile(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, again) {
		t.Fatalf("the manifest written again differs:\nfirst %s\nagain %s", first, again)
	}
}

// The manifest of a resumed export names the time zone of a source node that has left the
// cluster since the first run, as the first run recorded it.
func TestResume_ManifestKeepsTheTimezoneOfANodeThatLeft(t *testing.T) {
	t.Run("recorded", func(t *testing.T) {
		f, opts := interruptAfter(t, 3)
		requireNodeDone(t, opts, nodeA)
		if got := readState(t, ProgressPath(opts.Export)).SourceTimezones[nodeA]; got != "Asia/Shanghai" {
			t.Fatalf("the first run records the time zone of %s, got %q", nodeA, got)
		}
		f.mu.Lock()
		// data-a, whose units are all done, has left: it is not asked any more.
		f.dataNodes = f.dataNodes[1:]
		f.planFrames = slices.DeleteFunc(f.planFrames, func(p *transferv1.PlanResponse) bool { return p.GetUnits().GetNodeId() == nodeA })
		f.mu.Unlock()
		log, err := resumeRun(t, f, opts)
		if err != nil {
			t.Fatalf("%v\n%s", err, log)
		}
		if got := manifestTimezone(t, opts, nodeA); got != "Asia/Shanghai" {
			t.Fatalf("timezone of %s = %q, want the recorded Asia/Shanghai", nodeA, got)
		}
	})
	t.Run("an older progress file records none: the cluster's", func(t *testing.T) {
		f, opts := interruptAfter(t, 1)
		editState(t, &opts, func(s *exportProgress) { s.SourceTimezones = nil })
		log, err := resumeRun(t, f, opts)
		if err != nil {
			t.Fatalf("%v\n%s", err, log)
		}
		if got := manifestTimezone(t, opts, nodeA); got != "Asia/Shanghai" {
			t.Fatalf("timezone of %s = %q, want the cluster's Asia/Shanghai", nodeA, got)
		}
	})
}

// A node whose units are all done need not hold the session any more: that it cannot be read
// does not refuse the resume, as its leaving the cluster does not.
func TestResume_ADoneNodeThatCannotBeReadDoesNotCount(t *testing.T) {
	f, opts := interruptAfter(t, 3)
	requireNodeDone(t, opts, nodeA)
	f.mu.Lock()
	f.summary = &transferv1.PlanSummary{AnsweredNodes: []string{nodeB}, UnreachableNodes: []string{nodeA}}
	f.mu.Unlock()
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if _, statErr := os.Stat(filepath.Join(opts.Output, manifest.FileName)); statErr != nil {
		t.Fatalf("the export completes: %v", statErr)
	}
}

// slowExport delays every Export stream by d, so the heartbeat ticks while the units are sent.
func slowExport(d time.Duration) func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
	return func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		time.Sleep(d)
		return defaultExport(f, init, stream)
	}
}

// heartbeatRequired lists the required_nodes of every heartbeat f answered.
func heartbeatRequired(f *fakeLiaison) [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]string
	for _, c := range f.sessionCalls {
		if c.GetAction() == transferv1.SessionsRequest_ACTION_HEARTBEAT {
			out = append(out, c.GetRequiredNodes())
		}
	}
	return out
}

// A node whose units are all done need not hold the session any more: its lease having expired,
// not swept yet, refuses neither the Plan{read} nor a heartbeat of the resume, which names only
// the nodes with units left as required, and the artifact is the uninterrupted export's.
func TestResume_ADoneNodeWhoseLeaseExpiredDoesNotCount(t *testing.T) {
	want := baseline(t, csvSetup)
	f, opts := interruptAfter(t, 3)
	requireNodeDone(t, opts, nodeA)
	shortHeartbeat(t, 5*time.Millisecond)
	f.mu.Lock()
	f.expiredNodes = []string{nodeA}
	f.exportScript = slowExport(50 * time.Millisecond)
	f.mu.Unlock()
	log := &syncBuffer{}
	opts.Log, opts.Out, opts.Resume = log, log, true
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("an expired lease on a node with no unit left must not stop the resume: %v\n%s", err, log)
	}
	requireSameArtifact(t, want, opts.Output)
	if got := f.lastPlan().GetRead().GetRequiredNodes(); !slices.Equal(got, []string{nodeB}) {
		t.Fatalf("the read requires %v, want only the node with units left", got)
	}
	hbs := heartbeatRequired(f)
	if len(hbs) == 0 || strings.Contains(log.String(), "heartbeat failed") {
		t.Fatalf("the heartbeat ran and did not warn about the done node:\n%s", log)
	}
	for _, got := range hbs {
		if !slices.Equal(got, []string{nodeB}) {
			t.Fatalf("a heartbeat requires %v, want only the node with units left", got)
		}
	}
}

// A node with units left must still hold the session: its expired lease refuses the resume
// with exit 6 and deletes nothing.
func TestResume_ALeaseExpiredOnANodeWithUnitsLeftRefuses(t *testing.T) {
	f, opts := interruptAfter(t, 3)
	f.mu.Lock()
	f.expiredNodes = []string{nodeB}
	f.mu.Unlock()
	before := tree(t, opts.Output)
	log, err := resumeRun(t, f, opts)
	requireExit(t, err, exitcode.ExitResume, "data node "+nodeB+": export session feedfeed expired")
	requireUntouched(t, before, opts.Output)
	if strings.Contains(log.String(), "exporting ") {
		t.Fatalf("a refused resume exports nothing:\n%s", log)
	}
}

// A progress file another version of bydbctl wrote is refused (exit 6) before anything is
// printed or deleted, however the command is run, and the refusal names the file, both versions
// and the two ways on: the version that wrote it, or giving the export up with its session.
func TestResume_ProgressFileOfAnotherVersionIsRefusedWithTheNextStep(t *testing.T) {
	for _, version := range []int{1, progressVersion + 1} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			f, opts := interruptAfterOne(t)
			path := ProgressPath(opts.Export)
			editState(t, &opts, func(s *exportProgress) { s.Version = version })
			before := tree(t, opts.Output)
			plans := len(f.plans)
			want := []string{
				"an earlier export left the progress file " + path,
				fmt.Sprintf("(progress file version %d, this bydbctl reads version %d)", version, progressVersion),
				"so it cannot be resumed; nothing was deleted",
				"Resume it with the bydbctl version that wrote it",
				"to start over: bydbctl data release-session --id feedfeed, then delete what is in " + opts.Output + " and export again",
			}
			check := func(name string, err error, log *syncBuffer) {
				t.Helper()
				requireExit(t, err, exitcode.ExitResume, "")
				for _, w := range want {
					if !strings.Contains(err.Error(), w) {
						t.Fatalf("%s: the refusal lacks %q:\n%v", name, w, err)
					}
				}
				if strings.Contains(log.String(), "RESUME ") || strings.Contains(log.String(), "DONE ") {
					t.Fatalf("%s: a file that cannot be read has no summary:\n%s", name, log)
				}
				requireUntouched(t, before, opts.Output)
				if len(f.plans) != plans {
					t.Fatalf("%s: the cluster was contacted", name)
				}
			}

			log, err := resumeRun(t, f, opts)
			check("--resume", err, log)

			noResume := opts
			noResume.Resume = false
			noResume.Log, noResume.Out = &syncBuffer{}, &syncBuffer{}
			err = runExport(t, f, noResume)
			check("no terminal, no --resume", err, noResume.Log.(*syncBuffer))

			for _, format := range []string{"table", "json", "yaml"} {
				dry := opts
				dry.DryRun, dry.OutputFormat = true, format
				dry.Log, dry.Out = &syncBuffer{}, &syncBuffer{}
				found, dryErr := DryRunResume(envFor(t, dry, f), dry.Export)
				if !found {
					t.Fatalf("--dry-run -o %s found no progress file", format)
				}
				check("--dry-run -o "+format, dryErr, dry.Log.(*syncBuffer))
				if dry.Out.(*syncBuffer).String() != "" {
					t.Fatalf("--dry-run -o %s printed a plan:\n%s", format, dry.Out)
				}
			}
		})
	}
}

// A progress file that records no session id is refused with the general way to give up.
func TestResume_ProgressFileOfAnotherVersionWithoutASession(t *testing.T) {
	f, opts := interruptAfterOne(t)
	editState(t, &opts, func(s *exportProgress) { s.Version, s.Session.ID = 1, "" })
	before := tree(t, opts.Output)
	_, err := resumeRun(t, f, opts)
	requireExit(t, err, exitcode.ExitResume, "release it with: bydbctl data release-session --id <id>; then delete what is in "+opts.Output)
	if strings.Contains(err.Error(), "release-session --id feedfeed") {
		t.Fatalf("no session id is recorded:\n%v", err)
	}
	requireUntouched(t, before, opts.Output)
}

// waitUntil polls cond until it holds, failing after 10 seconds.
func waitUntil(cond func() bool) error {
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// heartbeatsRequiring counts the heartbeats f got whose required nodes are exactly nodes.
func heartbeatsRequiring(f *fakeLiaison, nodes ...string) int {
	n := 0
	for _, got := range heartbeatRequired(f) {
		if slices.Equal(got, nodes) {
			n++
		}
	}
	return n
}

// On a resume, once a node's worker exported every unit of it, the heartbeat no longer requires
// its lease: that lease expiring while another node is still exporting does not lose the session.
func TestResume_ANodeDoneLeavesTheHeartbeatsRequiredNodes(t *testing.T) {
	want := baseline(t, csvSetup)
	f, opts := interruptAfterOne(t)
	if _, _, jobs, err := loadProgress(ProgressPath(opts.Export), opts.Export); err != nil {
		t.Fatal(err)
	} else if left := nodesLeft(jobs); !slices.Equal(left, []string{nodeA, nodeB}) {
		t.Fatalf("want units left on both nodes, got %v", left)
	}
	shortHeartbeat(t, 5*time.Millisecond)
	hbBefore := len(heartbeatRequired(f))
	log := &syncBuffer{}
	opts.Log, opts.Out, opts.Resume = log, log, true
	f.mu.Lock()
	f.exportScript = func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		if init.GetTargetNode() == nodeA {
			// Node A exports nothing before the first heartbeat of the resume went out, which
			// requires both nodes: a node done earlier would already have left the heartbeat.
			if err := waitUntil(func() bool { return len(heartbeatRequired(f)) > hbBefore }); err != nil {
				return status.Error(codes.Internal, "no heartbeat before node A exported")
			}
		}
		if init.GetTargetNode() == nodeB {
			// Node A finishes first; once the heartbeat dropped it, its lease expires and node B
			// keeps exporting over a few more heartbeats.
			if err := waitUntil(func() bool { return heartbeatsRequiring(f, nodeB) > 0 }); err != nil {
				return status.Error(codes.Internal, "the heartbeat kept requiring node A")
			}
			f.mu.Lock()
			f.expiredNodes = []string{nodeA}
			f.mu.Unlock()
			seen := heartbeatsRequiring(f, nodeB)
			if err := waitUntil(func() bool { return heartbeatsRequiring(f, nodeB) >= seen+3 }); err != nil {
				return status.Error(codes.Internal, "no heartbeat after node A expired")
			}
		}
		return defaultExport(f, init, stream)
	}
	f.mu.Unlock()
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("an expired lease on a node with every unit exported must not lose the resume's session: %v\n%s", err, log)
	}
	requireSameArtifact(t, want, opts.Output)
	if hbs := heartbeatRequired(f)[hbBefore:]; !slices.Equal(hbs[0], []string{nodeA, nodeB}) {
		t.Fatalf("a resume first requires every node with units left, got %v", hbs[0])
	}
}

// A fresh export requires every plan node in every heartbeat, also after a node's worker
// exported every unit of it.
func TestRunExport_AFreshExportRequiresEveryPlanNode(t *testing.T) {
	want := baseline(t, csvSetup)
	f := exportFake()
	opts, log := exportOptions(t, f)
	opts.Parallelism = 1
	shortHeartbeat(t, 5*time.Millisecond)
	f.exportScript = func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		if init.GetTargetNode() == nodeB {
			// With one worker, node A is done before node B starts: wait for a few heartbeats.
			seen := len(heartbeatRequired(f))
			if err := waitUntil(func() bool { return len(heartbeatRequired(f)) >= seen+3 }); err != nil {
				return status.Error(codes.Internal, "no heartbeat after node A was done")
			}
		}
		return defaultExport(f, init, stream)
	}
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	requireSameArtifact(t, want, opts.Output)
	hbs := heartbeatRequired(f)
	if len(hbs) < 3 {
		t.Fatalf("want at least 3 heartbeats, got %d", len(hbs))
	}
	for i, got := range hbs {
		if !slices.Equal(got, []string{nodeA, nodeB}) {
			t.Fatalf("heartbeat %d of a fresh export must require every plan node, got %v", i, got)
		}
	}
}

// The cluster gained a data node: like the liaison, the fake reports it unreachable on the
// Plan{read} and answers its heartbeat with an error frame, since it never held the session.
// Neither stops the resume nor is a warning.
func TestResume_AnAddedDataNodeDoesNotRefuse(t *testing.T) {
	f, opts := interruptAfterOne(t)
	f.mu.Lock()
	f.dataNodes = append(f.dataNodes, &databasev1.Node{Metadata: &commonv1.Metadata{Name: "data-c:17912"}, Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}})
	f.mu.Unlock()
	shortHeartbeat(t, 5*time.Millisecond)
	slow := func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		time.Sleep(50 * time.Millisecond)
		return defaultExport(f, init, stream)
	}
	log := &syncBuffer{}
	opts.Log, opts.Out, opts.Resume = log, log, true
	f.mu.Lock()
	f.exportScript = slow
	f.mu.Unlock()
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("a node the cluster gained is not a node of the export: %v\n%s", err, log)
	}
	if f.countSessions(transferv1.SessionsRequest_ACTION_HEARTBEAT) == 0 || strings.Contains(log.String(), "heartbeat failed") {
		t.Fatalf("the heartbeat ran and did not warn about the added node:\n%s", log)
	}
}

// A resumed export reads its own session again and creates none: --preempt has nothing to
// preempt, and a NOTE says it is ignored.
func TestResume_PreemptIsIgnoredWithANote(t *testing.T) {
	f, opts := interruptAfterOne(t)
	opts.Preempt = true
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if !strings.Contains(log.String(), "NOTE --preempt (BYDBCTL_PREEMPT) is ignored: a resumed export creates no session, it reads its own session ") {
		t.Fatalf("no NOTE on --preempt:\n%s", log)
	}
}

func TestResume_ConfirmationMatrix(t *testing.T) {
	t.Run("no terminal, no --resume", func(t *testing.T) {
		f, opts := interruptAfterOne(t)
		before := tree(t, opts.Output)
		plans := len(f.plans)
		log := &syncBuffer{}
		opts.Log, opts.Out = log, log
		requireExit(t, runExport(t, f, opts), exitcode.ExitUsage,
			"add --resume to continue it (needed when not at a terminal); to start over: bydbctl data release-session --id feedfeed, then delete what is in "+
				opts.Output+" and export again")
		requireUntouched(t, before, opts.Output)
		if len(f.plans) != plans {
			t.Fatal("a refused resume reads no session")
		}
		for _, w := range []string{"RESUME   ", "1 unit(s), 1 file(s)", "3 unit(s), ", "est. ", "CATALOG  GROUP", "NODE "} {
			if !strings.Contains(log.String(), w) {
				t.Fatalf("the summary lacks %q:\n%s", w, log)
			}
		}
	})
	for _, yes := range []bool{true, false} {
		t.Run(fmt.Sprintf("asked, answered yes %v", yes), func(t *testing.T) {
			f, opts := interruptAfterOne(t)
			f.mu.Lock()
			f.exportScript = nil
			f.mu.Unlock()
			before := tree(t, opts.Output)
			var question string
			opts.Confirm = func(_ context.Context, q string) bool {
				question = q
				return yes
			}
			log := &syncBuffer{}
			opts.Log, opts.Out = log, log
			err := runExport(t, f, opts)
			if question != "Resume? [y/N] " {
				t.Fatalf("question = %q", question)
			}
			if yes {
				if err != nil {
					t.Fatalf("%v\n%s", err, log)
				}
				if !strings.Contains(log.String(), "resuming export session feedfeed: 1 unit(s) done, 3 left") {
					t.Fatalf("the export was not resumed:\n%s", log)
				}
				if _, statErr := os.Stat(filepath.Join(opts.Output, manifest.FileName)); statErr != nil {
					t.Fatalf("the resumed export writes the manifest: %v", statErr)
				}
				if got := f.releasedIDs(); !slices.Equal(got, []string{"feedfeed"}) {
					t.Fatalf("the resumed export releases its session, released %v", got)
				}
				return
			}
			requireExit(t, err, exitcode.ExitUsage, "not resumed; ")
			requireExit(t, err, exitcode.ExitUsage, "to start over: bydbctl data release-session --id feedfeed")
			requireUntouched(t, before, opts.Output)
		})
	}
	// The local checks come before the question: a resume that is refused anyway asks nothing.
	t.Run("refused before the question", func(t *testing.T) {
		f, opts := interruptAfterOne(t)
		state := readState(t, ProgressPath(opts.Export))
		if err := os.Remove(filepath.Join(opts.Output, filepath.FromSlash(state.Done[0].Files[0].Path))); err != nil {
			t.Fatal(err)
		}
		asked := 0
		opts.Confirm = func(context.Context, string) bool { asked++; return true }
		log := &syncBuffer{}
		opts.Log, opts.Out = log, log
		requireExit(t, runExport(t, f, opts), exitcode.ExitResume, "finished unit ")
		if asked != 0 {
			t.Fatal("a resume that is refused anyway asks nothing")
		}
	})
}

func TestResume_DryRunPrintsAndChangesNothing(t *testing.T) {
	f, opts := interruptAfterOne(t)
	before := tree(t, opts.Output)
	plans := len(f.plans)
	errLog, out := &syncBuffer{}, &syncBuffer{}
	opts.Log, opts.Out = errLog, out
	env := envFor(t, opts, f)
	opts.DryRun = true
	found, err := DryRunResume(env, opts.Export)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v\n%s", found, err, errLog)
	}
	requireUntouched(t, before, opts.Output)
	if len(f.plans) != plans {
		t.Fatal("a dry run reads no session")
	}
	if !strings.Contains(errLog.String(), "NOTE export session feedfeed was not contacted") {
		t.Fatalf("the dry run says the session was not contacted:\n%s", errLog)
	}
	if !strings.Contains(errLog.String(), "LEFT     ") || !strings.Contains(errLog.String(), "3 unit(s), ") ||
		!strings.Contains(errLog.String(), "shard(s), est. ") || !strings.Contains(out.String(), "NODE ") {
		t.Fatalf("summary:\n%s\nplan:\n%s", errLog, out)
	}
	// The plan of what is left lists only the units not done.
	done := readState(t, ProgressPath(opts.Export)).Done[0]
	doneGroup := strings.Split(done.Unit, "/")[1]
	jsonOut := &syncBuffer{}
	env.Out = jsonOut
	opts.OutputFormat = "json"
	if _, err = DryRunResume(env, opts.Export); err != nil {
		t.Fatal(err)
	}
	var rep Report
	if err = json.Unmarshal([]byte(jsonOut.String()), &rep); err != nil {
		t.Fatalf("%v:\n%s", err, jsonOut)
	}
	if len(rep.Rows) != 3 || slices.ContainsFunc(rep.Rows, func(r Row) bool { return r.Node == done.Node && r.Group == doneGroup }) {
		t.Fatalf("the remaining plan lists the 3 units left, not %s of %s: %+v", done.Unit, done.Node, rep.Rows)
	}
	if want := (ResumedProgress{Path: ProgressPath(opts.Export), Done: 1, Total: 4}); rep.Resumed == nil || *rep.Resumed != want {
		t.Fatalf("resumed = %+v, want %+v", rep.Resumed, want)
	}
	opts.Output = t.TempDir()
	if found, err = DryRunResume(env, opts.Export); found || err != nil {
		t.Fatalf("no progress file: found=%v err=%v", found, err)
	}
}

// `--dry-run --checkpoint <file>` without --output previews the recorded output directory.
func TestResume_DryRunWithOnlyTheCheckpoint(t *testing.T) {
	f, opts := interruptAfterOne(t)
	output := opts.Output
	errLog, out := &syncBuffer{}, &syncBuffer{}
	opts.Log, opts.Out = errLog, out
	opts.Checkpoint, opts.Output = ProgressPath(opts.Export), ""
	found, err := DryRunResume(envFor(t, opts, f), opts.Export)
	if !found || err != nil {
		t.Fatalf("found=%v err=%v\n%s", found, err, errLog)
	}
	if !strings.Contains(errLog.String(), "output directory "+output+" (recorded in the progress file)") {
		t.Fatalf("the recorded output directory is named:\n%s", errLog)
	}
}

func TestResume_ProgressWriteFailureExits3(t *testing.T) {
	failNthWrite(t, 3, true) // the session id and the plan are written, the first finished unit fails
	f := exportFake()
	// The first unit finishes; every other waits for its stream to be canceled: only the failed
	// write of the first can end it in time.
	var waitedOut atomic.Bool
	var units atomic.Int32
	f.exportScript = scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		if units.Add(1) == 1 {
			return sendUnit(stream, f.filesFor(req), nil)
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(10 * time.Second):
			waitedOut.Store(true)
			return sendUnit(stream, f.filesFor(req), nil)
		}
	})
	opts, log := exportOptions(t, f)
	opts.Parallelism = 1
	requireExit(t, runExport(t, f, opts), exitcode.ExitRuntime, "disk full (injected)")
	if waitedOut.Load() {
		t.Fatal("a failed progress write cancels the run at once")
	}
	if _, err := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(err) {
		t.Fatal("a run whose progress file failed writes no manifest")
	}
	requireKept(t, f, log)
}

func TestResume_UnavailableSessionReadExits3AndDeletesNothing(t *testing.T) {
	f, opts := interruptAfterOne(t)
	before := tree(t, opts.Output)
	f.mu.Lock()
	f.readCode = codes.Unavailable
	f.mu.Unlock()
	rlog, err := resumeRun(t, f, opts)
	requireExit(t, err, exitcode.ExitRuntime, "read export session feedfeed")
	requireUntouched(t, before, opts.Output)
	if !strings.Contains(rlog.String(), "to resume: run the same command again") {
		t.Fatalf("a retryable failure offers the resume:\n%s", rlog)
	}
}

// A lost session ends the export with no resume offered, and stays the reason of the failure
// when the last write of the progress file fails too.
func TestRunExport_LostSessionOffersNoResume(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failWrite bool
	}{
		{name: "lost session"},
		{name: "lost session and a failed last write", failWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.failWrite {
				failNthWrite(t, 3, true)
			}
			f := exportFake()
			f.hbCode = codes.FailedPrecondition
			f.exportScript = blockUntilCanceled
			opts, log := exportOptions(t, f)
			shortHeartbeat(t, 20*time.Millisecond)
			err := runExport(t, f, opts)
			requireExit(t, err, exitcode.ExitRuntime, "export session feedfeed lost")
			if tc.failWrite {
				requireExit(t, err, exitcode.ExitRuntime, "disk full (injected)")
			}
			if !strings.Contains(log.String(), "export session feedfeed is lost, so this export cannot be resumed") || strings.Contains(log.String(), "to resume:") {
				t.Fatalf("the hint:\n%s", log)
			}
		})
	}
}

// cancelOnWrite cancels once what was written contains needle.
type cancelOnWrite struct {
	cancel func()
	needle string
	syncBuffer
}

func (c *cancelOnWrite) Write(p []byte) (int, error) {
	n, err := c.syncBuffer.Write(p)
	if strings.Contains(c.String(), c.needle) {
		c.cancel()
	}
	return n, err
}

// Ctrl-C after the create announced the session id, before it completed: the session the
// liaison may keep is released, and the error says what happened.
func TestRunExport_InterruptedWhileCreatingReleasesTheSession(t *testing.T) {
	f := exportFake()
	f.hangAfterCreated = true
	opts, _ := exportOptions(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &cancelOnWrite{cancel: cancel, needle: "export session feedfeed created"}
	opts.Log, opts.Out = out, out
	err := RunExport(ctx, envFor(t, opts, f), opts.Export)
	requireExit(t, err, exitcode.ExitRuntime, "export interrupted while its session feedfeed was being created: context canceled; it was released on")
	if got := f.releasedIDs(); len(got) != 1 || got[0] != sessID {
		t.Fatalf("the announced session is released, released %v", got)
	}
	if checkErr := CheckOutputDir(opts.Output, ""); checkErr != nil {
		t.Fatalf("an interrupted create leaves the output directory empty: %v", checkErr)
	}
}

func requireAbandoned(t *testing.T, f *fakeLiaison, opts testOptions, log *syncBuffer) {
	t.Helper()
	if got := f.releasedIDs(); len(got) != 1 || got[0] != sessID {
		t.Fatalf("the announced session is released, released %v", got)
	}
	if resume.Exists(ProgressPath(opts.Export)) || strings.Contains(log.String(), "to resume:") {
		t.Fatalf("no progress file and no resume is offered:\n%s", log)
	}
}

// A create that announced its session and then failed leaves nothing on disk to name the
// session: it is released, and no resume is offered.
func TestRunExport_AnnouncedCreateThatFailsIsReleased(t *testing.T) {
	// The session is released while the progress file that records its id is still there: a run
	// stopped while it waits for the release leaves the id on disk. Released, the error does not
	// also tell to release it.
	t.Run("plan stream fails", func(t *testing.T) {
		f := exportFake()
		f.failAfterFirstFrame = true
		opts, log := exportOptions(t, f)
		path := ProgressPath(opts.Export)
		var existed bool
		f.onRelease = func() { existed = resume.Exists(path) }
		err := runExport(t, f, opts)
		requireExit(t, err, exitcode.ExitRuntime, "creating export session feedfeed failed after it was announced")
		requireExit(t, err, exitcode.ExitRuntime, "it was released on")
		if strings.Contains(err.Error(), "may still be held") {
			t.Fatalf("a released session is not to be released again: %v", err)
		}
		if !existed {
			t.Fatalf("the progress file is there at the release\n%s", log)
		}
		requireAbandoned(t, f, opts, log)
	})
	t.Run("the id cannot be recorded", func(t *testing.T) {
		failNthWrite(t, 1, false)
		f := exportFake()
		opts, log := exportOptions(t, f)
		requireExit(t, runExport(t, f, opts), exitcode.ExitRuntime, "disk full (injected)")
		requireAbandoned(t, f, opts, log)
	})
}

// The session is released before the progress file is deleted: a crash in between leaves the
// progress file, not a session nothing names.
func TestRunExport_ReleasesBeforeItDeletesTheProgressFile(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	path := ProgressPath(opts.Export)
	var existed bool
	f.onRelease = func() { existed = resume.Exists(path) }
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if !existed || resume.Exists(path) {
		t.Fatalf("the progress file is there at the release (%v) and gone after it (%v)", existed, resume.Exists(path))
	}
}

// Finished units are written at most once per progressWriteInterval; the session id and the
// plan at once, and every unit by the end, before the manifest.
func TestRunExport_FinishedUnitsAreWrittenCoalesced(t *testing.T) {
	savedInterval := progressWriteInterval
	progressWriteInterval = time.Hour
	t.Cleanup(func() { progressWriteInterval = savedInterval })
	saved := writeProgress
	t.Cleanup(func() { writeProgress = saved })
	var mu sync.Mutex
	var written []int
	writeProgress = func(path string, v any) error {
		mu.Lock()
		written = append(written, len(v.(*exportProgress).Done))
		mu.Unlock()
		return saved(path, v)
	}
	f := exportFake()
	opts, log := exportOptions(t, f)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	mu.Lock()
	defer mu.Unlock()
	// The session id, the plan, and the four units at the end: the first unit came within the
	// interval of the plan's write.
	if !slices.Equal(written, []int{0, 0, 4}) {
		t.Fatalf("finished units written %v, want [0 0 4]", written)
	}
}

func TestProgress_PlanIsInASideFileAndBothAreIndented(t *testing.T) {
	_, opts := interruptAfterOne(t)
	path := ProgressPath(opts.Export)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "\n  \"planDigest\": ") || !strings.HasSuffix(string(body), "}\n") || strings.Contains(string(body), `"frames"`) {
		t.Fatalf("the progress file is indented JSON without the plan:\n%s", body)
	}
	state := readState(t, path)
	side, err := os.ReadFile(resume.SidePath(path, planSide))
	if err != nil {
		t.Fatal(err)
	}
	if state.PlanDigest != resume.DigestBytes(side) {
		t.Fatalf("the progress file records the digest of the plan side file: %s", state.PlanDigest)
	}
	var plan struct {
		Frames []map[string]any `json:"frames"`
	}
	if err = json.Unmarshal(side, &plan); err != nil || len(plan.Frames) == 0 || !strings.Contains(string(side), "\n    {\n") {
		t.Fatalf("the plan side file is indented and holds each frame as an object, not a string: %v\n%s", err, side)
	}
	if state.Bindings.Options["format"] != "csv" || len(state.Bindings.Selectors) != 3 || len(state.Bindings.SourceNodes) != 2 {
		t.Fatalf("the plain binding values are recorded: %+v", state.Bindings)
	}
}

// A plan side file of the earlier layout, each frame a string holding its JSON, is refused with
// exit 6, by a resume and by ReadProgress.
func TestResume_PlanFramesAsStringsExit6(t *testing.T) {
	f, opts := interruptAfterOne(t)
	path := ProgressPath(opts.Export)
	var plan struct {
		Frames []json.RawMessage `json:"frames"`
		Nodes  []string          `json:"answeredNodes"`
	}
	side, err := os.ReadFile(resume.SidePath(path, planSide))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(side, &plan); err != nil {
		t.Fatal(err)
	}
	for i, frame := range plan.Frames {
		if plan.Frames[i], err = json.Marshal(string(frame)); err != nil {
			t.Fatal(err)
		}
	}
	digest, err := resume.WriteSide(resume.SidePath(path, planSide), plan)
	if err != nil {
		t.Fatal(err)
	}
	state := readState(t, path)
	state.PlanDigest = digest
	if err = writeProgress(path, state); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadProgress(path); err == nil || !strings.Contains(err.Error(), "plan frame 0") {
		t.Fatalf("ReadProgress: %v", err)
	}
	opts.Resume = true
	requireExit(t, runExport(t, f, opts), exitcode.ExitResume, "plan frame 0")
}

// The output directory and the directory of --checkpoint compare as directories, not as
// spellings: the lock file next to the progress file is not taken for something else.
func TestRunExport_OutputSpelledAnotherWayThanTheCheckpoint(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	opts.Output, opts.Checkpoint = link, filepath.Join(target, "p.json")
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
}

// checkedExport resolves the settings of an export from args and env, then runs CheckResume
// and, for a real export, CheckOutputDir, as data export does before contacting anything.
func checkedExport(t *testing.T, env map[string]string, args ...string) (*config.Export, error) {
	t.Helper()
	opts := &config.Export{}
	fs := pflag.NewFlagSet("export", pflag.ContinueOnError)
	config.Bind(fs, opts)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	sources, err := config.Resolve(fs, func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckResume(sources, opts); err != nil || opts.DryRun {
		return opts, err
	}
	return opts, CheckOutputDir(opts.Output, ProgressPath(*opts))
}

func requireChecked(t *testing.T, env map[string]string, args ...string) *config.Export {
	t.Helper()
	opts, err := checkedExport(t, env, args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return opts
}

// An output directory that holds the progress file of an interrupted export is accepted, as a
// resume, and a dry run may then name it to print what is left; a progress file elsewhere is
// named with --checkpoint.
func TestCheckResume_OutputWithAProgressFileIsResumed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "nodes"), "")
	_, err := checkedExport(t, nil, "--output", dir)
	requireExit(t, err, exitcode.ExitUsage, "is not empty")
	requireExit(t, err, exitcode.ExitUsage, "holds no export to resume")
	writeFile(t, filepath.Join(dir, ProgressFileName), "{}")
	requireChecked(t, nil, "--output", dir)
	requireChecked(t, nil, "--dry-run", "--output", dir)

	other := t.TempDir()
	writeFile(t, filepath.Join(other, "data"), "")
	checkpoint := filepath.Join(t.TempDir(), "p.json")
	_, err = checkedExport(t, nil, "--output", other, "--checkpoint", checkpoint)
	requireExit(t, err, exitcode.ExitUsage, "is not empty")
	writeFile(t, checkpoint, "{}")
	requireChecked(t, nil, "--output", other, "--checkpoint", checkpoint)
}

// A --checkpoint that is a directory, or the output directory itself, names no progress file:
// it is refused (exit 1), also by a dry run.
func TestCheckResume_CheckpointThatIsADirectoryIsRefused(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	_, err := checkedExport(t, nil, "--output", out, "--checkpoint", out)
	requireExit(t, err, exitcode.ExitUsage, "--checkpoint "+out+" names a directory; it names the progress file, such as "+filepath.Join(out, ProgressFileName))
	dir := t.TempDir()
	_, err = checkedExport(t, nil, "--output", out, "--checkpoint", dir)
	requireExit(t, err, exitcode.ExitUsage, "is a directory")
	_, err = checkedExport(t, nil, "--dry-run", "--checkpoint", dir)
	requireExit(t, err, exitcode.ExitUsage, "is a directory")
	requireChecked(t, nil, "--output", out, "--checkpoint", filepath.Join(dir, "p.json"))
}

// BYDBCTL_OUTPUT and BYDBCTL_CHECKPOINT are defaults of a real export: a dry run neither refuses
// them nor previews a resume from them; given as a flag, the same directory is previewed.
func TestCheckResume_DryRunDropsTheOutputOfTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	progress := filepath.Join(dir, ProgressFileName)
	writeFile(t, progress, "{}")
	env := map[string]string{"BYDBCTL_OUTPUT": dir, "BYDBCTL_CHECKPOINT": progress}
	if opts := requireChecked(t, env, "--dry-run"); opts.Output != "" || opts.Checkpoint != "" {
		t.Fatalf("the dry run drops the output and the checkpoint of the environment: %q, %q", opts.Output, opts.Checkpoint)
	}
	if opts := requireChecked(t, env); opts.Output != dir || opts.Checkpoint != progress {
		t.Fatalf("a real export keeps them: %q, %q", opts.Output, opts.Checkpoint)
	}
	requireChecked(t, nil, "--dry-run", "--output", dir)
}

// With --output given, a dry run keeps BYDBCTL_CHECKPOINT: the progress file of that export may
// be kept elsewhere, and is what the dry run previews; without it there, the refusal names it.
func TestCheckResume_DryRunWithAnOutputKeepsTheCheckpointOfTheEnvironment(t *testing.T) {
	out := t.TempDir()
	progress := filepath.Join(t.TempDir(), "p.json")
	env := map[string]string{"BYDBCTL_CHECKPOINT": progress}
	_, err := checkedExport(t, env, "--dry-run", "--output", out)
	requireExit(t, err, exitcode.ExitUsage,
		"--output cannot be combined with --dry-run, which writes nothing (it previews a resume only, and there is no progress file at "+progress+")")
	_, err = checkedExport(t, nil, "--dry-run", "--checkpoint", progress)
	requireExit(t, err, exitcode.ExitUsage, "--checkpoint cannot be combined with --dry-run")
	writeFile(t, progress, "{}")
	if opts := requireChecked(t, env, "--dry-run", "--output", out); opts.Checkpoint != progress {
		t.Fatalf("the progress file of the environment is previewed, got %q", opts.Checkpoint)
	}
}

// schema/ is a leftover only with schema files in it; anything else there is refused, not
// deleted.
func TestCheckOutputDir_SchemaWithOtherContentIsNoLeftover(t *testing.T) {
	for _, extra := range []string{"schema/notes.txt", "schema/sub/measure.jsonl"} {
		dir := t.TempDir()
		path := filepath.Join(dir, ProgressFileName)
		for _, name := range []string{"schema/measure.jsonl", extra} {
			p := filepath.Join(dir, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
				t.Fatal(err)
			}
			writeFile(t, p, "x")
		}
		requireExit(t, CheckOutputDir(dir, path), exitcode.ExitUsage, "(it holds schema)")
	}
	// The plan side file of the progress file and its temporary files are leftovers too.
	dir := t.TempDir()
	path := filepath.Join(dir, ProgressFileName)
	for _, name := range []string{filepath.Base(resume.SidePath(path, planSide)), "." + filepath.Base(resume.SidePath(path, planSide)) + ".42.tmp"} {
		writeFile(t, filepath.Join(dir, name), "x")
	}
	if err := CheckOutputDir(dir, path); err != nil {
		t.Fatal(err)
	}
	if err := cleanLeftover(&syncBuffer{}, dir, path); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("the leftovers are removed: %v", entries)
	}
}

func TestCheckOutputDir_LeftoverOfARunKilledBeforeItsFirstWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	path := filepath.Join(dir, ProgressFileName)
	if err := os.MkdirAll(filepath.Join(dir, "schema"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"schema/group.jsonl", "." + ProgressFileName + ".123.tmp", ProgressFileName + ".lock"} {
		writeFile(t, filepath.Join(dir, name), "x")
	}
	if err := CheckOutputDir(dir, path); err != nil {
		t.Fatalf("schema/ and a progress temp file are accepted: %v", err)
	}
	if err := CheckOutputDir(dir, filepath.Join(t.TempDir(), "p.json")); err == nil {
		t.Fatal("with the progress file elsewhere schema/ is not known to be this export's")
	}

	// The export removes them and runs.
	f := exportFake()
	opts, log := exportOptions(t, f)
	opts.Output = dir
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if !strings.Contains(log.String(), "removing from "+dir+" what an export that stopped before it recorded its session left: ..export-progress.json.123.tmp, schema\n") {
		t.Fatalf("the cleanup is reported:\n%s", log)
	}
	if _, err := os.Stat(filepath.Join(dir, "."+ProgressFileName+".123.tmp")); !os.IsNotExist(err) {
		t.Fatalf("the progress temp file is removed: %v", err)
	}

	// Anything more is refused, naming what is there.
	other := t.TempDir()
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		writeFile(t, filepath.Join(other, name), "")
	}
	requireExit(t, CheckOutputDir(other, filepath.Join(other, ProgressFileName)), exitcode.ExitUsage,
		"(it holds a, b, c, d, e, and 2 more); give a new or empty directory, or delete it first with: rm -rf "+other)
}

func TestCleanNodes_EdgeCases(t *testing.T) {
	dir := t.TempDir()
	if err := cleanNodes(dir, filepath.Join(dir, ProgressFileName), nil, ""); err != nil {
		t.Fatalf("no nodes directory is nothing to clean: %v", err)
	}
	write := func(rel string) string {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		writeFile(t, p, "x")
		return p
	}
	keep := write("nodes/a/x.csv")
	drop := write("nodes/a/y.csv.tmp")
	deep := write("nodes/b/c/z.csv")
	manifestTmp := write(".manifest.json.123.tmp")
	if err := cleanNodes(dir, filepath.Join(dir, ProgressFileName), []doneUnit{{Files: []doneFile{{File: manifest.File{Path: "nodes/a/x.csv"}}}}}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("a finished file is kept: %v", err)
	}
	for _, p := range []string{drop, deep, filepath.Dir(deep), filepath.Join(dir, "nodes", "b"), manifestTmp} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s is removed: %v", p, err)
		}
	}
}

func TestDiffPlans(t *testing.T) {
	f := exportFake()
	plan := func() *cluster.PlanResult {
		res := &cluster.PlanResult{}
		for _, fr := range f.planFrames {
			res.Frames = append(res.Frames, proto.Clone(fr.GetUnits()).(*transferv1.UnitFrame))
		}
		return res
	}
	p1, p2 := &transferv1.PartStat{Id: 1, TotalCount: 2}, &transferv1.PartStat{Id: 2, TotalCount: 2}
	recorded, current := plan(), plan()
	recorded.Frames[0].GetUnits()[0].GetSegment().Shards[0].Parts = []*transferv1.PartStat{p1, p2}
	current.Frames[0].GetUnits()[0].GetSegment().Shards[0].Parts = []*transferv1.PartStat{p2, p1}
	if diff := diffPlans(recorded, current, nil); diff != "" {
		t.Fatalf("the order of the parts does not count:\n%s", diff)
	}
	current.Frames = append(current.Frames, unitFrame(nodeB, "", "sw_new", 1, 2, 3).GetUnits())
	if diff := diffPlans(recorded, current, nil); !strings.Contains(diff, "node "+nodeB+" unit measure/sw_new/") || !strings.Contains(diff, "not in the recorded plan") {
		t.Fatalf("an extra unit is named:\n%s", diff)
	}

	// A unit missing now is a difference unless it is done: a done unit is not read again, so
	// a node holding only done units may be gone.
	recorded, current = plan(), plan()
	current.Frames = current.Frames[:len(current.Frames)-1]
	jobs, err := buildJobs(recorded)
	if err != nil {
		t.Fatal(err)
	}
	var lastKey string
	for _, job := range jobs {
		if job.nodeID == nodeB {
			lastKey = job.units[0].key()
		}
	}
	if diff := diffPlans(recorded, current, nil); !strings.Contains(diff, "node "+nodeB+" unit "+lastKey+": no longer in the session") {
		t.Fatalf("a missing unit is named:\n%s", diff)
	}
	if diff := diffPlans(recorded, current, doneKeys([]doneUnit{{Node: nodeB, Unit: lastKey}})); diff != "" {
		t.Fatalf("a missing done unit does not count:\n%s", diff)
	}
}
