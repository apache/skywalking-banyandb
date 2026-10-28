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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

type importRun struct {
	res *Result
	err error
	out string
	log string
}

func (r importRun) code() int {
	if r.err == nil {
		return 0
	}
	return exitcode.For(r.err)
}

func runImport(t *testing.T, f *fakeLiaison, li *cluster.Liaison, opts config.Import) importRun {
	t.Helper()
	return runImportVia(t, f, []*cluster.Liaison{li}, opts)
}

// runImportVia runs an import through liaisons, the first being the primary.
func runImportVia(t *testing.T, f *fakeLiaison, liaisons []*cluster.Liaison, opts config.Import) importRun {
	t.Helper()
	var out, log bytes.Buffer
	res, err := RunImport(context.Background(), Env{
		Log: &log, Out: &out, Cluster: clusterOf(f, liaisons[0].Addr), Liaisons: &cluster.Liaisons{All: liaisons},
		Now: func() time.Time { return dataNow },
	}, opts)
	return importRun{res: res, err: err, out: out.String(), log: log.String()}
}

func fastPolls(t *testing.T) {
	t.Helper()
	base, retry := verifyPollInterval, importRetryBase
	verifyPollInterval, importRetryBase = 10*time.Millisecond, time.Millisecond
	t.Cleanup(func() { verifyPollInterval, importRetryBase = base, retry })
}

func TestRunImportWritesSchemaPushesRowsAndVerifies(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := defaultImport(t, writeDataArtifact(t))
	opts.Verify = true
	opts.VerifyTimeout = 5 * time.Second
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err, run.log)
	t.Log(run.out + run.log)

	assert.Equal(t, []string{
		"create group/sw_metric", "create group/sw_prop", "create group/sw_stream",
		"create stream/sw_stream/log", "create measure/sw_metric/cpm", "create property/sw_prop/ui",
	}, f.writeLog(), "dependency order, _top_n_result never written")
	require.Len(t, f.awaits, 1)
	var keys []string
	for _, k := range f.awaits[0].GetKeys() {
		keys = append(keys, k.GetKind()+"/"+k.GetGroup()+"/"+k.GetName())
	}
	assert.Contains(t, keys, "group//sw_metric")
	assert.Contains(t, keys, "measure/sw_metric/"+csvfmt.TopNResultMeasure, "the auto-created _top_n_result is awaited")
	assert.Contains(t, keys, "stream/sw_stream/log")

	var stream, measure, prop int
	for _, b := range f.receivedBatches() {
		stream += len(b.GetStream().GetRows())
		measure += len(b.GetMeasure().GetRows())
		prop += len(b.GetProperty().GetRows())
	}
	assert.Equal(t, 3, stream)
	assert.Equal(t, 2, measure)
	assert.Equal(t, 2, prop)
	require.NotNil(t, run.res.Verify)
	assert.False(t, run.res.Verify.Failed(), run.log)
	assert.Regexp(t, `(?m)^VERIFY    ok in \S+$`, run.log)
	assert.Empty(t, run.res.PlanChanges, "a created group's nodes appearing is not a plan change")
	assert.True(t, strings.HasPrefix(run.out, "TARGET"))
}

func TestRunImportWithoutDataSkipsVerify(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := defaultImport(t, writeArtifact(t, func(m *manifest.Manifest) { m.Units = nil }))
	opts.Verify = true
	opts.VerifyTimeout = 5 * time.Second
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err, run.log)
	assert.Nil(t, run.res.Verify, "nothing was pushed, so there is nothing to verify")
	assert.Zero(t, f.planCalls, "no Plan of the whole target")
}

func TestRunImportDryRunWritesNothingAndPrintsTheSameTable(t *testing.T) {
	dir := writeDataArtifact(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := defaultImport(t, dir)
	opts.DryRun = true
	dry := runImport(t, f, li, opts)
	require.NoError(t, dry.err)
	assert.Empty(t, f.writeLog())
	assert.Empty(t, f.receivedBatches())

	realRun := runImport(t, f, li, defaultImport(t, dir))
	require.NoError(t, realRun.err, realRun.log)
	assert.Equal(t, dry.out, realRun.out, "a dry run and the real run print the same plan")
}

func TestRunImportRefusesAConflictBeforeWriting(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorWithConflict(f)
	run := runImport(t, f, li, defaultImport(t, writeDataArtifact(t)))
	assert.Equal(t, exitcode.ExitPreflight, run.code(), run.err)
	assert.Empty(t, f.writeLog())
	assert.Empty(t, f.receivedBatches())
	assert.Contains(t, run.out, "CONFLICT")
}

func TestRunImportLogsTheRateLimits(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := defaultImport(t, writeDataArtifact(t))
	opts.NodeRowRateLimit = 100
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err)
	assert.Contains(t, run.log, "rate limits: node 0 bytes/s, 100 rows/s per target node;")
}

func TestRunImportCreateRaceIsARuntimeFailure(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	f.createErr = status.Error(codes.AlreadyExists, "group sw_metric already exists")
	run := runImport(t, f, li, defaultImport(t, writeDataArtifact(t)))
	assert.Equal(t, exitcode.ExitRuntime, run.code())
	assert.ErrorContains(t, run.err, "created by someone else")
	assert.Empty(t, f.receivedBatches())
}

func TestRunImportSchemaWaitTimeoutIsARuntimeFailure(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	f.awaitAnswer = &schemav1.AwaitSchemaAppliedResponse{Laggards: []*schemav1.NodeLaggard{{
		Node: "data-b", MissingKeys: []*schemav1.SchemaKey{{Kind: "stream", Group: "sw_stream", Name: "log"}},
	}}}
	run := runImport(t, f, li, defaultImport(t, writeDataArtifact(t)))
	assert.Equal(t, exitcode.ExitRuntime, run.code())
	assert.ErrorContains(t, run.err, "data-b missing [stream/sw_stream/log]")
	assert.Empty(t, f.receivedBatches())
}

func TestRunImportSchemaOnlySendsNoData(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	opts := defaultImport(t, writeDataArtifact(t))
	opts.SchemaOnly = true
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err)
	assert.Len(t, f.writeLog(), 6)
	assert.Empty(t, f.receivedBatches())
	assert.Nil(t, run.res.Recheck)
}

func TestRunImportAbortsOnPlanChange(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	f.targetsHook = func(_ string, resp *transferv1.TargetsResponse) {
		if f.targetCalls > 1 {
			resp.Copies++
		}
	}
	opts := defaultImport(t, writeDataArtifact(t))
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err, "without --abort-on-plan-change a change is only printed")
	assert.NotEmpty(t, run.res.PlanChanges)
	assert.Contains(t, run.log, "differs")

	f.targetCalls = 0
	opts.AbortOnPlanChange = true
	batches := len(f.receivedBatches())
	run = runImport(t, f, li, opts)
	assert.Equal(t, exitcode.ExitPlanChanged, run.code(), run.err)
	assert.Len(t, f.receivedBatches(), batches, "no data is sent")
}

func TestRunImportVerifyMismatchExitsFour(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	// data-b acknowledges the rows of shard 1 without storing them.
	f.dropNode = "data-b"
	opts := defaultImport(t, writeDataArtifact(t))
	opts.Verify = true
	opts.VerifyTimeout = 200 * time.Millisecond
	run := runImport(t, f, li, opts)
	assert.Equal(t, exitcode.ExitVerify, run.code(), run.log)
	assert.Regexp(t, `VERIFY    FAILED in \S+ \(exit 4\)\n  - stream sw_stream/hot: rows grew by 2 per copy, 3 were sent`, run.log)
	assert.Regexp(t, `(?m)^IMPORTED  7 rows of 3 work item\(s\) in \S+$`, run.log)
}

func TestRunImportUnreachableNodeLeavesVerifyIncomplete(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	f.unreachable = []string{"data-a"}
	opts := defaultImport(t, writeDataArtifact(t))
	opts.Verify = true
	opts.VerifyTimeout = 100 * time.Millisecond
	run := runImport(t, f, li, opts)
	assert.Equal(t, exitcode.ExitVerify, run.code(), run.log)
	assert.NotEmpty(t, run.res.Verify.Incomplete)
}

func TestRunImportRejectsAMissingInput(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	run := runImport(t, f, li, defaultImport(t, t.TempDir()))
	assert.Equal(t, exitcode.ExitPreflight, run.code())
}

func TestRunImportOverwriteOfANonEmptyGroupNeedsForce(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorWithConflict(f)
	f.counts[CountKey{Catalog: stream, Group: "sw_stream", Stage: "hot", Node: "data-a"}] = 9
	opts := defaultImport(t, writeDataArtifact(t))
	opts.OnSchemaConflict = "overwrite"
	run := runImport(t, f, li, opts)
	assert.Equal(t, exitcode.ExitPreflight, run.code(), run.err)
	assert.Contains(t, run.out, "group sw_stream holds data; overwrite needs --force")
	assert.Equal(t, 1, f.planCalls, "only the group to overwrite is planned")
	assert.Empty(t, f.writeLog())

	opts.Force = true
	run = runImport(t, f, li, opts)
	require.NoError(t, run.err, run.log)
	assert.Equal(t, []string{"update stream/sw_stream/log"}, f.writeLog())
}

func TestRunImportWarnsWhenForceWasNotNeeded(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorWithConflict(f)
	opts := defaultImport(t, writeDataArtifact(t))
	opts.OnSchemaConflict = "overwrite"
	opts.Force = true
	opts.DryRun = true
	run := runImport(t, f, li, opts)
	require.NoError(t, run.err)
	assert.Contains(t, run.log, "--force was not needed")
}

func TestComparePlansMatchesAnUnknownRoute(t *testing.T) {
	first := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g"}, TargetStage: "hot", Copies: 2, TargetNodes: []string{}}}}
	second := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g"}, TargetStage: "hot", Copies: 1, TargetNodes: []string{"local"}}}}
	assert.Empty(t, ComparePlans(first, second))
	assert.Empty(t, ComparePlans(second, first))

	known := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g"}, TargetStage: "hot", Copies: 2, TargetNodes: []string{"local"}}}}
	assert.NotEmpty(t, ComparePlans(known, second), "a route known before is compared as is")

	moved := &Plan{Entries: []Entry{{Item: WorkItem{Group: "g"}, TargetStage: "warm", Copies: 1, TargetNodes: []string{"local"}}}}
	assert.NotEmpty(t, ComparePlans(first, moved), "an unknown route still compares the stage")
}

func TestRunImportInterruptedVerifyIsARuntimeFailure(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	// data-b drops its rows, so verify keeps polling until it is interrupted.
	f.dropNode = "data-b"
	opts := defaultImport(t, writeDataArtifact(t))
	opts.Verify = true
	opts.VerifyTimeout = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, log bytes.Buffer
	// No Plan is asked during the push, so the first Plan after an acknowledged batch is a
	// verify poll: cancel then, so the interrupt lands in verify whatever the machine speed.
	var pushed atomic.Bool
	f.onPlan = func() {
		if pushed.Load() {
			cancel()
		}
	}
	env := Env{
		Log: &log, Out: &out, Cluster: clusterOf(f, li.Addr), Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}},
		Now:  func() time.Time { return dataNow },
		Hook: func(string) { pushed.Store(true) },
	}
	res, err := RunImport(ctx, env, opts)
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err), log.String())
	require.NotNil(t, res.Verify, "the push finished before the interrupt")
	assert.Contains(t, strings.Join(res.Verify.Incomplete, "\n"), "interrupted")
}

// mirrorWithConflict mirrors the data fixture's schema on f, with stream sw_stream/log carrying an extra tag.
func mirrorWithConflict(f *fakeLiaison) {
	mirrorFake(f, dataSchemas())
	other := testStream("sw_stream", "log")
	other.TagFamilies[0].Tags = append(other.TagFamilies[0].Tags, &databasev1.TagSpec{Name: "extra", Type: databasev1.TagType_TAG_TYPE_STRING})
	f.schema.Streams[ResourceKey{"sw_stream", "log"}] = other
}
