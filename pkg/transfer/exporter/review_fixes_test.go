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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// An interrupt reports the cause of the cancellation for the bare error of a node stream it
// canceled, and keeps every error that says more.
func TestRunErr_KeepsWhatSaysMoreThanACancellation(t *testing.T) {
	cause := errors.New("signal: interrupt")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	for _, tc := range []struct {
		err  error
		name string
		want string
	}{
		{name: "context error", err: context.Canceled, want: "export interrupted: " + cause.Error()},
		{name: "grpc cancellation", err: status.Error(codes.Canceled, "context canceled"), want: "export interrupted: " + cause.Error()},
		{name: "session lost", err: sessionLostError{errors.New("session abc lost")}, want: "export interrupted: session abc lost"},
		{name: "schema changed", err: schemaChangedError{errors.New("schema changed")}, want: "export interrupted: schema changed"},
		{name: "announced", err: &announcedError{err: context.Canceled, hints: "release it"}, want: "a session that was announced may still be held"},
		{name: "ordinary failure", err: errors.New("snapshot unit missing"), want: "export interrupted: snapshot unit missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runErr(ctx, context.Background(), tc.err)
			if got == nil || !strings.Contains(got.Error(), tc.want) {
				t.Fatalf("runErr = %v, want it to contain %q", got, tc.want)
			}
		})
	}
}

// A create given up after several attempts announced a session each releases every one of them,
// not only the last the progress file records.
func TestRunExport_CreateRetriesExhaustedReleaseEveryAnnouncedSession(t *testing.T) {
	orig := createRetryBase
	createRetryBase = time.Millisecond
	t.Cleanup(func() { createRetryBase = orig })
	f := exportFake()
	f.failAfterFirstFrame = true
	f.idFor = func(attempt int) string { return fmt.Sprintf("feed%04d", attempt) }
	opts, _ := exportOptions(t, f)
	err := runExport(t, f, opts)
	requireExit(t, err, exitcode.ExitRuntime, "creating export sessions feed0001, feed0002, feed0003 failed after they were announced")
	got := f.releasedIDs()
	slices.Sort(got)
	if want := []string{"feed0001", "feed0002", "feed0003"}; !slices.Equal(got, want) {
		t.Fatalf("released %v, want every announced session %v", got, want)
	}
	if resume.Exists(ProgressPath(opts.Export)) {
		t.Fatal("no progress file is left")
	}
}

// A fresh export refused for its selectors leaves nothing behind, not even the empty output
// directory the lock would create.
func TestRunExport_RefusedSelectorsLeaveNoOutputDirectory(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	opts.Selectors = []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"no_such_group"}}}
	requireExit(t, runExport(t, f, opts), exitcode.ExitUsage, "no_such_group")
	if _, err := os.Stat(opts.Output); !os.IsNotExist(err) {
		t.Fatalf("the output directory is not created: %v", err)
	}
}

// A progress file named like a side file would be its own, or another progress file's.
func TestProgressPath_RefusesTheNameOfASideFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	for _, name := range []string{"p-plan.json", "p-cursor.json"} {
		checkpoint := filepath.Join(t.TempDir(), name)
		_, err := checkedExport(t, nil, "--output", out, "--checkpoint", checkpoint)
		requireExit(t, err, exitcode.ExitUsage, "side file")
		f := exportFake()
		opts, _ := exportOptions(t, f)
		opts.Checkpoint = checkpoint
		requireExit(t, runExport(t, f, opts), exitcode.ExitUsage, "side file")
		if _, statErr := os.Stat(opts.Output); !os.IsNotExist(statErr) {
			t.Fatalf("%s: the output directory is not created: %v", name, statErr)
		}
	}
}

// allDoneProgress is an export whose every unit is done and whose manifest is not written: the
// progress file and the plan of its last write are put back, the manifest removed.
func allDoneProgress(t *testing.T) (*fakeLiaison, testOptions) {
	t.Helper()
	saved := writeProgress
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
	err := runExport(t, f, opts)
	writeProgress = saved
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	path := ProgressPath(opts.Export)
	for p, body := range map[string][]byte{path: last, resume.SidePath(path, planSide): plan} {
		if err = os.WriteFile(p, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Remove(filepath.Join(opts.Output, manifest.FileName)); err != nil {
		t.Fatal(err)
	}
	return f, opts
}

// noPlanProgress is an export interrupted after its session was created and before its plan was
// recorded.
func noPlanProgress(t *testing.T) (*fakeLiaison, testOptions) {
	t.Helper()
	failNthWrite(t, 2, false)
	f := exportFake()
	opts, _ := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), exitcode.ExitRuntime, "disk full (injected)")
	return f, opts
}

// A dry run of a resume that has no units to list still prints the json or yaml document its
// caller parses: with no rows, and the progress it previews.
func TestResume_DryRunJSONAndYAMLWithNothingToList(t *testing.T) {
	for _, tc := range []struct {
		setup func(*testing.T) (*fakeLiaison, testOptions)
		name  string
		done  int
		total int
	}{
		{name: "every unit done", setup: allDoneProgress, done: 4, total: 4},
		{name: "no plan recorded", setup: noPlanProgress, done: 0, total: 0},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(tc.name+" "+format, func(t *testing.T) {
				f, opts := tc.setup(t)
				errLog, out := &syncBuffer{}, &syncBuffer{}
				opts.Log, opts.Out = errLog, out
				opts.DryRun, opts.OutputFormat = true, format
				if found, err := DryRunResume(envFor(t, opts, f), opts.Export); !found || err != nil {
					t.Fatalf("found=%v err=%v\n%s", found, err, errLog)
				}
				var rep Report
				var err error
				if format == "json" {
					err = json.Unmarshal([]byte(out.String()), &rep)
				} else {
					var doc struct {
						Resumed *ResumedProgress `yaml:"resumed"`
						Rows    []Row            `yaml:"rows"`
					}
					err = yaml.Unmarshal([]byte(out.String()), &doc)
					rep.Resumed, rep.Rows = doc.Resumed, doc.Rows
				}
				if err != nil {
					t.Fatalf("%v:\n%s", err, out)
				}
				if len(rep.Rows) != 0 || rep.Resumed == nil || rep.Resumed.Done != tc.done {
					t.Fatalf("want no rows and resumed done=%d, got %+v:\n%s", tc.done, rep, out)
				}
				if !strings.Contains(out.String(), "rows: []") && !strings.Contains(out.String(), `"rows": []`) {
					t.Fatalf("the rows are an empty list:\n%s", out)
				}
			})
		}
	}
	t.Run("table prints nothing", func(t *testing.T) {
		f, opts := allDoneProgress(t)
		errLog, out := &syncBuffer{}, &syncBuffer{}
		opts.Log, opts.Out = errLog, out
		opts.DryRun = true
		if _, err := DryRunResume(envFor(t, opts, f), opts.Export); err != nil || out.String() != "" {
			t.Fatalf("err=%v out=%q", err, out)
		}
	})
}

// A directory under nodes/ that cannot be read refuses the resume with exit 6, like the
// symlinks it does not follow, instead of failing with a bare error.
func TestResume_UnreadableNodesDirectoryExits6(t *testing.T) {
	f, opts := interruptAfterOne(t)
	dir := filepath.Join(opts.Output, nodesDir, "locked")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := os.ReadDir(dir); err == nil {
		t.Skip("the directory can be read anyway (running as root)")
	}
	log, err := resumeRun(t, f, opts)
	requireExit(t, err, exitcode.ExitResume, "nothing was deleted")
	requireExit(t, err, exitcode.ExitResume, dir)
	_ = log
}

// What the earlier runs had in flight is not carried into this run's progress file.
func TestResume_ClearsTheInflightUnitsOfEarlierRuns(t *testing.T) {
	f, opts := interruptAfterOne(t)
	editState(t, &opts, func(s *exportProgress) {
		s.Inflight = map[string]inflightUnit{"ghost:1": {Unit: "stream/old/hot/20260101", StartedAt: time.Now()}}
	})
	saved := writeProgress
	t.Cleanup(func() { writeProgress = saved })
	var mu sync.Mutex
	var written []*exportProgress
	writeProgress = func(path string, v any) error {
		state := v.(*exportProgress)
		mu.Lock()
		written = append(written, &exportProgress{Inflight: maps2(state.Inflight)})
		mu.Unlock()
		return saved(path, v)
	}
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(written) == 0 {
		t.Fatal("the resume wrote no progress file")
	}
	for _, s := range written {
		if _, ok := s.Inflight["ghost:1"]; ok {
			t.Fatalf("an earlier run's in-flight unit is carried on: %v", s.Inflight)
		}
	}
}

func maps2(in map[string]inflightUnit) map[string]inflightUnit {
	out := make(map[string]inflightUnit, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// A plan with no unit left needs no node to hold the session: the read is not sent with an empty
// required_nodes, which the liaison takes for every node.
func TestResume_PlanWithoutUnitsSendsNoEmptyRequiredNodes(t *testing.T) {
	saved := writeProgress
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
	f.planFrames = nil
	opts, log := exportOptions(t, f)
	err := runExport(t, f, opts)
	writeProgress = saved
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	path := ProgressPath(opts.Export)
	for p, body := range map[string][]byte{path: last, resume.SidePath(path, planSide): plan} {
		if err = os.WriteFile(p, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Remove(filepath.Join(opts.Output, manifest.FileName)); err != nil {
		t.Fatal(err)
	}
	rlog, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.plans {
		if p.GetRead() != nil && len(p.GetRead().GetRequiredNodes()) == 0 {
			t.Fatalf("a read of the session names no required node: %v", p)
		}
	}
}

// The left-shard count of the summary counts a shard of a group once, however many nodes hold a
// replica of it.
func TestLeftShards_CountsEachShardOnce(t *testing.T) {
	unit := func(done bool, shards ...uint32) *plannedUnit {
		inv := &transferv1.SegmentInventory{Unit: &transferv1.SegmentUnit{SegmentSuffix: "20260101", ShardIds: shards}}
		for _, s := range shards {
			inv.Shards = append(inv.Shards, &transferv1.ShardStat{ShardId: s})
		}
		return &plannedUnit{segment: inv, done: done}
	}
	jobs := []*nodeJob{
		{nodeID: "a", units: []*plannedUnit{unit(false, 0, 1), unit(true, 7)}},
		{nodeID: "b", units: []*plannedUnit{unit(false, 1, 2)}},
	}
	if got := leftShards(jobs); got != 3 {
		t.Fatalf("shards 0, 1 and 2 are left, got %d", got)
	}
	if got := leftShards(nil); got != 0 {
		t.Fatalf("no plan, no shards, got %d", got)
	}
}

// A refusal marks a value "(default)" against the defaults of the export's format: a native export
// is not compressed, so none is its default compression.
func TestDefaultOptions_FollowTheFormat(t *testing.T) {
	if got := defaultOptions(transferv1.Format_FORMAT_NATIVE)["compress"]; got != "none" {
		t.Fatalf("native compress default = %q", got)
	}
	if got := defaultOptions(transferv1.Format_FORMAT_CSV)["compress"]; got != "gzip" {
		t.Fatalf("csv compress default = %q", got)
	}
}

// The resume summary is a block, then a blank line, then the table of what is left, which does
// not repeat the SNAPSHOT line of a plan: the first run pinned the snapshot already.
func TestResume_LogSeparatesTheSummaryFromTheTableAndSaysNothingOfTheSnapshot(t *testing.T) {
	f, opts := interruptAfterOne(t)
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	text := log.String()
	i := strings.Index(text, "\nNODE ")
	if i < 0 || !strings.Contains(text[:i+1], "est. ") || !strings.HasSuffix(text[:i+1], "\n\n") {
		t.Fatalf("a blank line sets the table off from the summary:\n%s", text)
	}
	if strings.Contains(text, "SNAPSHOT") {
		t.Fatalf("a resume does not say how much room a snapshot needs:\n%s", text)
	}
}

// The times of a resume are shown in the operator's time zone (the process's, which differs from
// UTC under TZ=Asia/Tokyo); the manifest keeps UTC.
func TestResume_ShowsLocalTimeAndKeepsUTCInTheManifest(t *testing.T) {
	f, opts := interruptAfterOne(t)
	state := readState(t, ProgressPath(opts.Export))
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(m.ExportedAt, "Z") || m.ExportedAt != state.StartedAt.UTC().Format(time.RFC3339) {
		t.Fatalf("exportedAt = %s, want the first run's start in UTC", m.ExportedAt)
	}
	if want := "the first started at " + state.StartedAt.Local().Format(time.RFC3339); !strings.Contains(log.String(), want) {
		t.Fatalf("the summary lacks %q:\n%s", want, log)
	}
	if want := "last updated " + state.UpdatedAt.Local().Format(time.RFC3339); !strings.Contains(log.String(), want) {
		t.Fatalf("the resume summary lacks %q:\n%s", want, log)
	}
}

// The progress file of the layout before the plan went to a side file of its own is refused
// with exit 6 and named by its version.
func TestResume_ProgressFileOfThePreviousVersionExits6(t *testing.T) {
	f, opts := interruptAfterOne(t)
	editState(t, &opts, func(s *exportProgress) { s.Version = progressVersion - 1 })
	opts.Resume = true
	err := runExport(t, f, opts)
	requireExit(t, err, exitcode.ExitResume, fmt.Sprintf("version %d file", progressVersion-1))
	requireExit(t, err, exitcode.ExitResume, "nothing was deleted")
}
