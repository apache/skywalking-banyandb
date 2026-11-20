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
	"os"
	"path/filepath"
	"strings"
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

// identityState is a progress file of a target with liaisons and data nodes; complete marks its
// one planned item, on data node "d1", complete.
func identityState(liaisons, nodes []string, complete bool) *progressState {
	key := "stream/g/hot/20261016/shard-0"
	return &progressState{
		Bindings: importBindings{Liaisons: liaisons, TargetNodes: nodes},
		Plan:     []plannedItem{{Key: key, TargetNodes: []string{"d1"}}},
		Done:     map[string]*itemRecord{key: {Complete: complete}},
	}
}

func TestDiffBindings_TargetIdentity(t *testing.T) {
	for name, tc := range map[string]struct {
		state    *progressState
		current  importBindings
		want     []string
		identity bool
	}{
		"same cluster": {
			state: identityState([]string{"l1", "l2"}, []string{"d1", "d2"}, false), current: importBindings{Liaisons: []string{"l1", "l2"}, TargetNodes: []string{"d1", "d2"}},
		},
		"overlapping liaisons": {
			state: identityState([]string{"l1", "l2"}, []string{"d1", "d2"}, false), current: importBindings{Liaisons: []string{"l2", "l3"}, TargetNodes: []string{"d1", "d2"}},
		},
		"superset of liaisons and data nodes": {
			state:   identityState([]string{"l1"}, []string{"d1"}, false),
			current: importBindings{Liaisons: []string{"l1", "l2", "l3"}, TargetNodes: []string{"d1", "d2", "d3"}},
		},
		"subset of liaisons and data nodes": {
			state: identityState([]string{"l1", "l2"}, []string{"d1", "d2"}, false), current: importBindings{Liaisons: []string{"l2"}, TargetNodes: []string{"d1"}},
		},
		"disjoint liaisons": {
			state: identityState([]string{"l1", "l2"}, []string{"d1", "d2"}, false), current: importBindings{Liaisons: []string{"l3"}, TargetNodes: []string{"d1", "d2"}},
			want: []string{"liaisons differs", "recorded [l1 l2], now [l3]: none of the recorded liaisons is a liaison of the target"}, identity: true,
		},
		"standalone of another name": {
			state: identityState([]string{"a"}, []string{"a"}, true), current: importBindings{Liaisons: []string{"b"}, TargetNodes: []string{"b"}},
			want: []string{"liaisons differs", "targetNodes differs", "recorded [a], now [b]: none of the recorded data nodes is a data node of the target"}, identity: true,
		},
		"every item complete and the data nodes disjoint": {
			state: identityState([]string{"l1"}, []string{"d1", "d2"}, true), current: importBindings{Liaisons: []string{"l1"}, TargetNodes: []string{"d3", "d4"}},
			want: []string{"targetNodes differs", "recorded [d1 d2], now [d3 d4]"}, identity: true,
		},
		"a target node of an unfinished item gone": {
			state: identityState([]string{"l1"}, []string{"d1", "d2"}, false), current: importBindings{Liaisons: []string{"l1"}, TargetNodes: []string{"d2"}},
			want: []string{"target node d1 of the recorded import is no longer a data node of the target"}, identity: true,
		},
		"a target node of a complete item gone": {
			state: identityState([]string{"l1"}, []string{"d1", "d2"}, true), current: importBindings{Liaisons: []string{"l1"}, TargetNodes: []string{"d2"}},
		},
		"no liaisons recorded": {
			state: identityState(nil, []string{"d1"}, true), current: importBindings{Liaisons: []string{"l1"}, TargetNodes: []string{"d1"}},
			want: []string{"the progress file records no liaisons (it was written by an earlier version)"}, identity: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			diff := diffBindings(tc.state, tc.current)
			if len(tc.want) == 0 {
				assert.Empty(t, diff)
				return
			}
			for _, w := range tc.want {
				assert.Contains(t, diff, w)
			}
			hints := bindingHints(tc.state, tc.current)
			assert.Equal(t, tc.identity, bytes.Contains([]byte(hints), []byte(otherTargetHint)), hints)
		})
	}
}

// finishedRun runs the import of a finished fixture again, edit changing its environment and
// options.
type finishedRun func(edit func(*Env, *config.Import)) importRun

// finishedFixture imports the data fixture into a fake and returns the fake, the progress file,
// the number of batches the fake received and how to run the import again.
func finishedFixture(t *testing.T) (*fakeLiaison, string, int, finishedRun) {
	t.Helper()
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	first := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, first.err, first.log)
	require.NotNil(t, readState(t, ProgressPath(opts)).FinishedAt)
	run := func(edit func(*Env, *config.Import)) importRun {
		var out, log bytes.Buffer
		env := Env{
			Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
			Now: func() time.Time { return dataNow },
		}
		o := opts
		if edit != nil {
			edit(&env, &o)
		}
		res, err := RunImport(context.Background(), env, o)
		return importRun{res: res, err: err, out: out.String(), log: log.String()}
	}
	return f, ProgressPath(opts), len(f.receivedBatches()), run
}

// withDryRun runs with --dry-run.
func withDryRun(_ *Env, o *config.Import) { o.DryRun = true }

// dropGroup removes a group and every resource in it from the fake, as a reset target lacks them.
func dropGroup(f *fakeLiaison, group string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.schema.Groups, group)
	for k := range f.schema.Streams {
		if k.Group == group {
			delete(f.schema.Streams, k)
		}
	}
	for k := range f.schema.Measures {
		if k.Group == group {
			delete(f.schema.Measures, k)
		}
	}
	for k := range f.schema.Traces {
		if k.Group == group {
			delete(f.schema.Traces, k)
		}
	}
	for k := range f.schema.Properties {
		if k.Group == group {
			delete(f.schema.Properties, k)
		}
	}
	for k := range f.schema.IndexRules {
		if k.Group == group {
			delete(f.schema.IndexRules, k)
		}
	}
	for k := range f.schema.IndexRuleBindings {
		if k.Group == group {
			delete(f.schema.IndexRuleBindings, k)
		}
	}
	for k := range f.schema.TopNs {
		if k.Group == group {
			delete(f.schema.TopNs, k)
		}
	}
	dropCountsLocked(f, group)
}

// dropCounts removes every row of a group from the fake's counts.
func dropCounts(f *fakeLiaison, group string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dropCountsLocked(f, group)
}

func dropCountsLocked(f *fakeLiaison, group string) {
	for k := range f.counts {
		if k.Group == group {
			delete(f.counts, k)
		}
	}
	for k := range f.properties {
		if k.Group == group {
			delete(f.properties, k)
		}
	}
}

// runAt runs an import through li at the TTL reference time now.
func runAt(f *fakeLiaison, li *cluster.Liaison, opts config.Import, now time.Time) importRun {
	var out, log bytes.Buffer
	res, err := RunImport(context.Background(), Env{
		Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
		Now: func() time.Time { return now },
	}, opts)
	return importRun{res: res, err: err, out: out.String(), log: log.String()}
}

func recordedKeys(state *progressState) []string {
	var out []string
	for _, p := range state.Plan {
		out = append(out, p.Key)
	}
	return out
}

// A finished import run again, with --verify given, pushes nothing, lists the target it was
// imported into and verifies against the baseline the first run recorded.
func TestFinishedImport_RunAgainPushesNothing(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	batches := len(f.receivedBatches())

	opts.Verify = true
	again := runExplicit(t, f, li, opts)
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "verify: against the baseline the first run recorded")
	assert.Contains(t, again.log, "already imported into this cluster at")
	assert.Contains(t, again.log, "(liaisons liaison-0, data nodes data-a data-b); nothing is pushed")
	assert.Contains(t, again.log, "FINISHED "+ProgressPath(opts)+"  (finished at ")
	assert.Contains(t, again.log, "3 work item(s), ")
	assert.NotContains(t, again.log, "RESUME ")
	assert.NotContains(t, again.log, "LEFT ")
	assert.NotContains(t, again.log, "AMBIGUOUS ")
	assert.Empty(t, again.out, "a finished import has nothing left to list")
	assert.Len(t, f.receivedBatches(), batches, "nothing is pushed again")
	require.NotNil(t, again.res.Verify, "verify still runs")
	assert.False(t, again.res.Verify.Failed(), again.log)
}

func TestResumeCSV_FinishedImportExitsZeroAndRestartStartsOver(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = true
	opts.VerifyTimeout = time.Second
	first := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, first.err, first.log)
	path := ProgressPath(opts)
	state := readState(t, path)
	require.NotNil(t, state.FinishedAt)
	require.NotNil(t, state.BaselineAt)
	batches := len(f.receivedBatches())

	// No question, nothing pushed, no verify without an explicit --verify.
	again := runImportEnv(context.Background(), f, li, opts, nil, func(context.Context, string) bool {
		t.Error("a finished import asks nothing")
		return false
	})
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "already imported into this cluster at "+state.FinishedAt.Local().Format(time.RFC3339))
	assert.Contains(t, again.log, "NOTE verify skipped: the baseline was taken at "+state.BaselineAt.Local().Format(time.RFC3339))
	assert.Nil(t, again.res.Verify)
	assert.Len(t, f.receivedBatches(), batches)

	// --restart keeps the file as a .bak and imports again, warning what is stored twice.
	opts.Restart = true
	restarted := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, restarted.err, restarted.log)
	assert.Contains(t, restarted.log, "WARN --restart: "+path+" is kept as "+path+".")
	assert.Contains(t, restarted.log, "the stream and trace rows an earlier run already imported are stored twice, in stream sw_stream")
	baks, err := filepath.Glob(path + ".2*.bak")
	require.NoError(t, err)
	assert.Len(t, baks, 1)
	sideBaks, err := filepath.Glob(resume.SidePath(path, planSide) + ".2*.bak")
	require.NoError(t, err)
	assert.Len(t, sideBaks, 1, "the plan side file, with the baseline, is kept with it")
	assert.Greater(t, len(f.receivedBatches()), batches)
	assert.NotNil(t, readState(t, path).FinishedAt)
}

// A finished import whose verify did not pass verifies again when run again, with verify on by
// default; one whose verify passed only says it is done. Payloads skip left out are verified once.
func TestResumeCSV_PendingVerifyRunsUntilItPasses(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = true
	completeCSV(t, f, li, opts)
	path := ProgressPath(opts)
	assert.False(t, readState(t, path).VerifyPending, "the verify passed")

	for _, edit := range []func(s *progressState){
		func(s *progressState) { s.VerifyPending = true },
		func(s *progressState) { s.SkippedPayloads = []string{"x"} },
	} {
		editState(t, path, edit)
		again := runImportEnv(context.Background(), f, li, opts, nil, nil)
		require.NoError(t, again.err, again.log)
		require.NotNil(t, again.res.Verify, again.log)
		state := readState(t, path)
		assert.False(t, state.VerifyPending)
		assert.Empty(t, state.SkippedPayloads)

		quiet := runImportEnv(context.Background(), f, li, opts, nil, nil)
		require.NoError(t, quiet.err, quiet.log)
		assert.Nil(t, quiet.res.Verify, "once verified, a run again only says it is done")
		assert.Contains(t, quiet.log, "NOTE verify skipped")
	}

	editState(t, path, func(s *progressState) { s.VerifyPending = true })
	off := opts
	off.Verify = false
	skipped := runImportEnv(context.Background(), f, li, off, nil, nil)
	require.NoError(t, skipped.err, skipped.log)
	assert.Nil(t, skipped.res.Verify, "--verify=false verifies nothing")
	assert.True(t, readState(t, path).VerifyPending)
}

// A verify that fails leaves the verify pending.
func TestResumeCSV_FailedVerifyStaysPending(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	// data-b acknowledges the rows of shard 1 without storing them.
	opts := defaultImport(t, writeDataArtifact(t))
	opts.Verify, opts.VerifyTimeout = true, 200*time.Millisecond
	f.dropNode = "data-b"
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.Equal(t, exitcode.ExitVerify, run.code(), run.log)
	path := ProgressPath(opts)
	assert.True(t, readState(t, path).VerifyPending)
	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitVerify, again.code(), "the same command verifies again: %s", again.log)
	assert.Contains(t, again.log, "verify: on, since no verify of this import has passed yet")
}

// An item of the recorded plan not complete keeps an import finished by an earlier run open: a
// run that plans it pushes it.
func TestResumeCSV_FinishedWithAnItemLeftResumes(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	completeCSV(t, f, li, opts)
	batches := len(f.receivedBatches())
	path := ProgressPath(opts)
	editState(t, path, func(s *progressState) { delete(s.Done, streamItemKey) })
	opts.Resume = true
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	assert.NotContains(t, run.log, "already imported")
	assert.Contains(t, run.log, "RESUMED   2 of 3 work item(s) done by the previous run(s); 1 left")
	assert.Regexp(t, `(?m)^stream\s+sw_stream\s+hot\s+1\s`, run.out, "what is left, one work item")
	assert.NotContains(t, run.out, "RESULT", "a resume lists only what is left")
	assert.Greater(t, len(f.receivedBatches()), batches)
	assert.True(t, readState(t, path).Done[streamItemKey].Complete)
}

// A finished import whose recorded plan holds an item not complete, such as one a gate refused on
// the run that finished it and accepts now, is a resume like any other: not at a terminal it needs
// --resume, at one it asks, and it does not say the item is new.
func TestResumeCSV_FinishedWithARecordedItemLeftAsks(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	completeCSV(t, f, li, opts)
	batches := len(f.receivedBatches())
	path := ProgressPath(opts)
	editState(t, path, func(s *progressState) { delete(s.Done, streamItemKey) })
	require.NotNil(t, readState(t, path).FinishedAt)

	noTerminal := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitUsage, noTerminal.code(), noTerminal.log)
	assert.Contains(t, noTerminal.err.Error(), "add --resume to continue it (needed when not at a terminal)")
	assert.Len(t, f.receivedBatches(), batches, "nothing is pushed without --resume")

	var asked bool
	run := runImportEnv(context.Background(), f, li, opts, nil, func(context.Context, string) bool {
		asked = true
		return true
	})
	require.NoError(t, run.err, run.log)
	assert.True(t, asked, "the resume is confirmed")
	assert.NotContains(t, run.log, "accepts now")
	assert.Greater(t, len(f.receivedBatches()), batches)
	assert.True(t, readState(t, path).Done[streamItemKey].Complete)
}

// The target nodes of the items complete do not bind: a finished import whose node is gone is
// still done.
func TestResumeCSV_NodeOfACompleteItemGone(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	completeCSV(t, f, li, opts)
	editState(t, ProgressPath(opts), func(s *progressState) {
		s.Plan[0].TargetNodes = append(s.Plan[0].TargetNodes, "data-gone")
	})
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	assert.Contains(t, run.log, "already imported")
}

// A finished import run against another cluster, or from a progress file that records no
// liaisons, is refused (exit 6) and pushes nothing, as a dry run too. Which liaisons and data
// nodes count as the same cluster is TestDiffBindings_TargetIdentity's.
func TestFinishedImport_AnotherClusterIsRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		edit    func(*Env, *config.Import)
		rewrite func(t *testing.T, progress string)
		want    string
	}{
		"disjoint liaisons": {edit: func(env *Env, _ *config.Import) { env.Cluster.Liaisons = []string{"other-liaison"} }, want: "recorded ["},
		"disjoint liaisons, dry run": {edit: func(env *Env, o *config.Import) {
			env.Cluster.Liaisons = []string{"other-liaison"}
			o.DryRun = true
		}, want: "recorded ["},
		"disjoint data nodes": {edit: func(env *Env, _ *config.Import) {
			for _, n := range env.Cluster.DataNodes {
				n.Metadata.Name = "other-" + n.Metadata.Name
			}
		}, want: "recorded ["},
		// An earlier version of the format wrote the progress file without its liaisons.
		"progress file without liaisons": {rewrite: func(t *testing.T, progress string) {
			data, err := os.ReadFile(progress)
			require.NoError(t, err)
			var raw map[string]any
			require.NoError(t, json.Unmarshal(data, &raw))
			delete(raw["bindings"].(map[string]any), "liaisons")
			data, err = json.Marshal(raw)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(progress, data, 0o600))
		}, want: "records no liaisons"},
	} {
		t.Run(name, func(t *testing.T) {
			f, progress, batches, run := finishedFixture(t)
			if tc.rewrite != nil {
				tc.rewrite(t, progress)
			}
			again := run(tc.edit)
			require.Error(t, again.err, again.log)
			assert.Equal(t, exitcode.ExitResume, again.code(), again.err)
			assert.Contains(t, again.err.Error(), otherTargetHint)
			assert.Contains(t, again.err.Error(), tc.want)
			assert.NotContains(t, again.log, "already imported")
			assert.Len(t, f.receivedBatches(), batches, "nothing is pushed")
		})
	}
}

func TestFinishedImport_ResetTargetIsRefused(t *testing.T) {
	for _, dry := range []bool{false, true} {
		name := "real run"
		if dry {
			name = "dry run"
		}
		t.Run(name, func(t *testing.T) {
			f, path, batches, run := finishedFixture(t)
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			dropGroup(f, "sw_stream")
			again := run(nil)
			if dry {
				again = run(withDryRun)
			}
			assert.Equal(t, exitcode.ExitResume, again.code(), again.log)
			assert.Contains(t, again.err.Error(), "the target lacks group/sw_stream, stream/sw_stream/")
			assert.Contains(t, again.err.Error(), "the target looks reset or is another cluster")
			assert.Contains(t, again.err.Error(), otherTargetHint)
			assert.NotContains(t, again.log, "already imported")
			assert.Len(t, f.receivedBatches(), batches)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, before, after, "the progress file is left as it is")
		})
	}
}

func TestFinishedImport_MissingRowsAreRefused(t *testing.T) {
	for _, dry := range []bool{false, true} {
		name := "real run"
		if dry {
			name = "dry run"
		}
		t.Run(name, func(t *testing.T) {
			f, path, batches, run := finishedFixture(t)
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			dropCounts(f, "sw_stream")
			again := run(nil)
			if dry {
				again = run(withDryRun)
			}
			assert.Equal(t, exitcode.ExitResume, again.code(), again.log)
			assert.Contains(t, again.err.Error(), "records data this import wrote that the target does not hold (1 place(s))")
			assert.Contains(t, again.err.Error(), "stream sw_stream stage hot: 3 row(s) imported (stream/sw_stream/hot/20261016/shard-0)")
			assert.Contains(t, again.err.Error(), otherTargetHint)
			assert.Len(t, f.receivedBatches(), batches)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}

// A finished import whose target reports a segment that is not a time is refused (exit 6), as a
// dry run too, and nothing is written.
func TestFinishedImport_UnparseableTargetSegmentIsRefused(t *testing.T) {
	for _, dry := range []bool{false, true} {
		f, path, batches, run := finishedFixture(t)
		before, err := os.ReadFile(path)
		require.NoError(t, err)
		f.mu.Lock()
		f.segment = "2026101600"
		f.mu.Unlock()
		again := run(nil)
		if dry {
			again = run(withDryRun)
		}
		assert.Equal(t, exitcode.ExitResume, again.code(), again.log)
		assert.Contains(t, again.err.Error(), `the target reports segment "2026101600" of `)
		assert.Regexp(t, `group sw_(stream|metric) stage hot on node data-[ab]`, again.err.Error())
		assert.Contains(t, again.err.Error(), "nothing was written")
		assert.NotContains(t, again.log, "already imported")
		assert.Len(t, f.receivedBatches(), batches)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, before, after)
	}
}

func TestFinishedImport_MeasureNeedsRowsOnly(t *testing.T) {
	f, _, _, run := finishedFixture(t)
	f.mu.Lock()
	for k := range f.counts {
		if k.Group == "sw_metric" {
			f.counts[k] = 1
		}
	}
	f.mu.Unlock()
	again := run(nil)
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "already imported into this cluster at ")

	dropCounts(f, "sw_metric")
	gone := run(nil)
	assert.Equal(t, exitcode.ExitResume, gone.code(), gone.log)
	assert.Contains(t, gone.err.Error(), "measure sw_metric stage hot:")
}

func TestFinishedImport_ExpiredSegmentsAreNotChecked(t *testing.T) {
	f, _, _, run := finishedFixture(t)
	dropCounts(f, "sw_stream")
	dropCounts(f, "sw_metric")
	again := run(func(env *Env, _ *config.Import) { env.Now = func() time.Time { return dataNow.AddDate(0, 1, 0) } })
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "already imported into this cluster at ")
}

func TestResumeCSV_ResetTargetIsRefusedBeforeThePush(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := runImportEnv(ctx, f, li, opts, cancelAt(4, cancel), nil)
	require.Error(t, first.err, first.log)
	state := readState(t, ProgressPath(opts))
	require.Nil(t, state.FinishedAt)
	require.NotEmpty(t, state.Done, "some rows are recorded as landed")
	batches, writes := len(f.receivedBatches()), len(f.writeLog())

	for _, g := range []string{"sw_stream", "sw_metric", "sw_prop"} {
		dropGroup(f, g)
	}
	opts.Resume = true
	again := runImportEnv(context.Background(), f, li, opts, nil, nil)
	assert.Equal(t, exitcode.ExitResume, again.code(), again.log)
	assert.Contains(t, again.err.Error(), "the target looks reset or is another cluster")
	assert.Contains(t, again.err.Error(), otherTargetHint)
	assert.Len(t, f.receivedBatches(), batches, "nothing is pushed")
	assert.Len(t, f.writeLog(), writes, "no schema is written")
}

// An import finished while the TTL gate refused the stream and measure items is not "already
// imported" once --force-ttl accepts them: they are appended to the recorded plan and imported,
// without --resume and without pushing anything of the first run again; a run after that is
// already imported. A dry run previews them.
func TestFinishedImport_ItemsOnceRefusedByAGateAreImported(t *testing.T) {
	fastPolls(t)
	wantRows, wantProps := uninterrupted(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	later := dataNow.AddDate(0, 1, 0)

	first := runAt(f, li, opts, later)
	require.NoError(t, first.err, first.log)
	path := ProgressPath(opts)
	state := readState(t, path)
	require.NotNil(t, state.FinishedAt)
	assert.Equal(t, []string{"property/sw_prop/shard-0"}, recordedKeys(state), "the TTL gate refused the stream and measure items")
	rows, props := storedCounts(f)
	assert.Empty(t, rows)
	assert.Equal(t, wantProps, props)
	batches := len(f.receivedBatches())

	opts.ForceTTL = true
	dry := opts
	dry.DryRun = true
	preview := runAt(f, li, dry, later)
	require.NoError(t, preview.err, preview.log)
	assert.Contains(t, preview.log, "NEW 2 work item(s) a gate refused when the plan was recorded are accepted now")
	assert.Contains(t, preview.log, "  measure/sw_metric/hot/20261016/shard-0")
	assert.Contains(t, preview.log, "  stream/sw_stream/hot/20261016/shard-0")
	assert.NotContains(t, preview.log, "already imported")
	assert.Len(t, preview.res.Plan.Entries, 3)
	assert.Len(t, f.receivedBatches(), batches, "a dry run pushes nothing")
	assert.Equal(t, []string{"property/sw_prop/shard-0"}, recordedKeys(readState(t, path)), "a dry run records nothing")

	again := runAt(f, li, opts, later)
	require.NoError(t, again.err, again.log)
	assert.NotContains(t, again.log, "already imported")
	assert.Contains(t, again.log, "the 2 work item(s) a gate refused then and accepts now are imported")
	assert.Contains(t, again.log, "plan: 2 work item(s) a gate refused when the plan was recorded are added to it")
	assert.Contains(t, again.log, "RESUMED   1 of 3 work item(s) done by the previous run(s); 2 left")
	rows, props = storedCounts(f)
	assert.Equal(t, wantRows, rows, "the items once refused are imported")
	assert.Equal(t, wantProps, props, "the property imported by the first run is not pushed again")
	state = readState(t, path)
	require.NotNil(t, state.FinishedAt)
	assert.ElementsMatch(t, []string{"property/sw_prop/shard-0", "measure/sw_metric/hot/20261016/shard-0", "stream/sw_stream/hot/20261016/shard-0"},
		recordedKeys(state))
	assert.Len(t, state.Done, 3)

	batches = len(f.receivedBatches())
	third := runAt(f, li, opts, later)
	require.NoError(t, third.err, third.log)
	assert.Contains(t, third.log, "already imported into this cluster at ")
	assert.Len(t, f.receivedBatches(), batches)
}

// An item not in the recorded plan with progress recorded is not one a gate refused: the plans
// differ. One without progress is new and appended.
func TestComparePlans_NewItems(t *testing.T) {
	plan := &Plan{Entries: []Entry{
		{Item: WorkItem{Catalog: stream, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem}, TargetStage: "hot"},
		{Item: WorkItem{Catalog: stream, Group: "g", Stage: "hot", Segment: "s", Shard: 1, Kind: ShardItem}, TargetStage: "hot"},
	}}
	state := newProgress(csvArtifact(), testBindings())
	state.Plan = planOf(plan.Entries[:1])
	assert.Empty(t, comparePlans(state, plan, true))
	added := newItems(state, plan.Entries)
	require.Len(t, added, 1)
	assert.Equal(t, "stream/g/hot/s/shard-1", added[0].Item.Key())

	state.Done["stream/g/hot/s/shard-1"] = &itemRecord{Cursor: &csvCursor{}}
	assert.Equal(t, "  stream/g/hot/s/shard-1: not in the recorded plan, but the progress file records progress of it", comparePlans(state, plan, true))

	assert.Empty(t, newItems(newProgress(csvArtifact(), testBindings()), plan.Entries), "no recorded plan, nothing to append to")
}

// A finished import verified again counts the groups of the items a gate refuses now too.
func TestRecordedDecision_PlansTheRefusedItemsAgain(t *testing.T) {
	_, entries, e := nativeShardEntry(t)
	gone := e.Item
	gone.Group = "old"
	first := &decision{plan: &Plan{Entries: entries, Rejected: []Rejection{{Item: gone, TargetStage: "hot"}}}}
	state := newProgress(csvArtifact(), testBindings())
	state.Plan = append(planOf(entries), plannedItem{Key: gone.Key(), TargetStage: "hot"})
	d := recordedDecision(first, state)
	var groups []string
	for _, s := range VerifySelectors(d.plan.Entries) {
		groups = append(groups, s.String())
	}
	assert.Contains(t, strings.Join(groups, " "), "old")
	assert.Len(t, first.plan.Entries, len(entries), "the plan is left as it is")
}

// Items a gate refused in the first run, appended by a later one, may be of groups the first run
// imported nothing into, so its baseline lacks them: they are counted before the push and the
// counts are recorded with the baseline under a new digest, so that what the target held there
// before is not taken for rows of the import. A rerun verifies against the extended baseline.
func TestFinishedImport_AppendedItemsOfNewGroupsGetABaseline(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	opts.Verify = true
	later := dataNow.AddDate(0, 1, 0)

	first := runAt(f, li, opts, later)
	require.NoError(t, first.err, first.log)
	path := ProgressPath(opts)
	digest := readState(t, path).PlanDigest
	require.NotEmpty(t, digest)

	// Something else writes 4 rows into sw_stream meanwhile.
	f.mu.Lock()
	f.counts[CountKey{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "sw_stream", Stage: "hot", Node: "data-a", Shard: 0}] = 4
	f.mu.Unlock()

	opts.ForceTTL = true
	again := runAt(f, li, opts, later)
	require.NoError(t, again.err, again.log)
	assert.Contains(t, again.log, "baseline: 2 group(s) the appended items add were counted before the push and recorded with the baseline: sw_metric, sw_stream")

	state := readState(t, path)
	assert.NotEqual(t, digest, state.PlanDigest, "the plan side file was rewritten with the baseline")
	assert.Empty(t, state.PlanNextDigest)
	baseline := state.baseline
	require.NotNil(t, baseline)
	var held int64
	for k, segs := range baseline.Shards {
		if k.Group == "sw_stream" {
			for _, n := range segs {
				held += n
			}
		}
	}
	assert.Equal(t, int64(4), held, "the baseline holds what sw_stream held before the push")

	explicit := runExplicit(t, f, li, opts)
	require.NoError(t, explicit.err, explicit.log)
	assert.Contains(t, explicit.log, "VERIFY    ok")
}
