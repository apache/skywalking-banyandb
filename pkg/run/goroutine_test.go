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

package run

import (
	"context"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

// TestGo_DefaultRecoversWithoutTerminatingProcess pins the new default Go
// behavior: a panic inside the launched goroutine is recovered, diagnostics
// are captured, and the process continues. Callers that need fatal semantics
// must opt in via WithRepanic(true).
func TestGo_DefaultRecoversWithoutTerminatingProcess(t *testing.T) {
	t.Helper()

	task := Go(context.Background(), "default-recovery", logger.GetLogger("test"), func(_ context.Context) {
		panic("non-fatal boom")
	})

	select {
	case <-task.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not finish after panic; Done was expected to close")
	}

	outcome := task.Outcome()
	require.NotNil(t, outcome, "Outcome must be populated for the default (non-repanic) path")
	require.True(t, outcome.Panicked, "expected outcome.Panicked = true after recovered panic")
	require.NotNil(t, outcome.Result.Record)
	require.Equal(t, "non-fatal boom", outcome.Result.Record.PanicValue)
}

// TestGo_OutcomeAfterCleanRun pins that a goroutine that returns normally
// produces a non-nil outcome with Panicked=false. Callers that watch
// Done()+Outcome() must be able to distinguish "ran clean" from "panicked".
func TestGo_OutcomeAfterCleanRun(t *testing.T) {
	t.Helper()

	executed := atomic.Bool{}
	task := Go(context.Background(), "clean-run", logger.GetLogger("test"), func(_ context.Context) {
		executed.Store(true)
	})

	outcome := task.Wait()
	require.True(t, executed.Load())
	require.NotNil(t, outcome)
	require.False(t, outcome.Panicked)
	require.Nil(t, outcome.Result.Record)
}

// TestGo_PerCallReporterFires pins that WithReporter delivers the recovery
// result without resorting to the variadic API the previous Go signature
// exposed.
func TestGo_PerCallReporterFires(t *testing.T) {
	t.Helper()

	reported := make(chan panicdiag.RecoveryResult, 1)
	task := Go(context.Background(), "reporter-hook", logger.GetLogger("test"),
		func(_ context.Context) { panic("reporter-boom") },
		WithReporter(func(_ context.Context, result panicdiag.RecoveryResult) {
			reported <- result
		}),
	)
	<-task.Done()

	select {
	case got := <-reported:
		require.Equal(t, "reporter-boom", got.Record.PanicValue)
	case <-time.After(time.Second):
		t.Fatal("expected per-call reporter to fire")
	}
}

// TestGo_RecoveryCapturesBreadcrumbsAddedInFn keeps the assertion that
// breadcrumbs added inside fn show up in the captured artifact, but adapts to
// the new default: no process termination is required, so we read the
// outcome directly via Wait() instead of running a subprocess.
func TestGo_RecoveryCapturesBreadcrumbsAddedInFn(t *testing.T) {
	t.Helper()

	artifactRoot := t.TempDir()
	previousRoot := panicdiag.DefaultArtifactRoot()
	panicdiag.SetDefaultArtifactRoot(artifactRoot)
	t.Cleanup(func() { panicdiag.SetDefaultArtifactRoot(previousRoot) })

	task := Go(context.Background(), "test", logger.GetLogger("test"), func(ctx context.Context) {
		panicdiag.WithBreadcrumb(ctx, "stage-A", "component", nil)
		panicdiag.WithBreadcrumb(ctx, "stage-B", "component", nil)
		panic("boom")
	})

	outcome := task.Wait()
	require.NotNil(t, outcome)
	require.True(t, outcome.Panicked)
	require.NotNil(t, outcome.Result.Record)
	require.Equal(t, "boom", outcome.Result.Record.PanicValue)
	require.Len(t, outcome.Result.Record.Breadcrumbs, 2)
	require.Equal(t, "stage-A", outcome.Result.Record.Breadcrumbs[0].Stage)
	require.Equal(t, "stage-B", outcome.Result.Record.Breadcrumbs[1].Stage)

	collections, listErr := panicdiag.ListCollections(artifactRoot)
	require.NoError(t, listErr)
	require.Len(t, collections, 1)
	require.NotNil(t, collections[0].Record)
	require.Equal(t, "boom", collections[0].Record.PanicValue)
	require.Len(t, collections[0].Record.Breadcrumbs, 2)
}

// TestGo_SiblingsHaveIsolatedBreadcrumbs pins the per-goroutine isolation
// contract that callers rely on: when two goroutines are launched via Go
// from the same parent ctx, each goroutine's panic record contains the
// inherited parent breadcrumbs plus only its own additions, never the
// sibling's. This is what allows panicdiag artifacts to be useful in a
// fan-out workload: without isolation, racy interleaving would smear
// every sibling's stages into every record.
func TestGo_SiblingsHaveIsolatedBreadcrumbs(t *testing.T) {
	t.Helper()

	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	log := logger.GetLogger("test")

	// Parent context carries one breadcrumb both children should inherit.
	parent := panicdiag.WithMutableBreadcrumbs(context.Background())
	parent = panicdiag.WithBreadcrumb(parent, "shared", "parent", nil)

	// Use a barrier so both goroutines have entered fn before either
	// adds its own breadcrumb. This makes the race the test is guarding
	// against (cross-sibling visibility) actually exercisable.
	start := make(chan struct{})

	taskA := Go(parent, "child-A", log, func(ctx context.Context) {
		<-start
		panicdiag.WithBreadcrumb(ctx, "stage-A", "child-A", nil)
		panic("a-boom")
	})
	taskB := Go(parent, "child-B", log, func(ctx context.Context) {
		<-start
		panicdiag.WithBreadcrumb(ctx, "stage-B", "child-B", nil)
		panic("b-boom")
	})

	close(start)

	outcomeA := taskA.Wait()
	outcomeB := taskB.Wait()

	require.True(t, outcomeA.Panicked, "taskA must have panicked")
	require.True(t, outcomeB.Panicked, "taskB must have panicked")
	require.NotNil(t, outcomeA.Result.Record)
	require.NotNil(t, outcomeB.Result.Record)

	stagesOf := func(bcs []panicdiag.Breadcrumb) []string {
		out := make([]string, len(bcs))
		for idx, bc := range bcs {
			out[idx] = bc.Stage
		}
		return out
	}
	stagesA := stagesOf(outcomeA.Result.Record.Breadcrumbs)
	stagesB := stagesOf(outcomeB.Result.Record.Breadcrumbs)

	// Both children must inherit the parent's breadcrumb...
	require.Contains(t, stagesA, "shared", "child-A must inherit parent stages")
	require.Contains(t, stagesB, "shared", "child-B must inherit parent stages")

	// ...each child must see its own addition...
	require.Contains(t, stagesA, "stage-A", "child-A's own breadcrumb must be in its record")
	require.Contains(t, stagesB, "stage-B", "child-B's own breadcrumb must be in its record")

	// ...and crucially, neither child must see the sibling's addition.
	require.NotContains(t, stagesA, "stage-B",
		"child-A's record contains child-B's breadcrumb: sibling stores are not isolated")
	require.NotContains(t, stagesB, "stage-A",
		"child-B's record contains child-A's breadcrumb: sibling stores are not isolated")

	// And the parent's view must remain pristine: child additions must
	// not write back through the shared parent ctx.
	parentStages := stagesOf(panicdiag.BreadcrumbsFromContext(parent))
	require.NotContains(t, parentStages, "stage-A", "child-A wrote back into parent's store")
	require.NotContains(t, parentStages, "stage-B", "child-B wrote back into parent's store")
}

// TestGoOrDie_TerminatesProcessAndWritesArtifact pins both halves of GoOrDie's
// contract: the process exits non-zero on a recovered panic AND the diagnostic
// artifact is fully written before the re-raise kills the process. We exec a
// helper subprocess so we can observe the exit code, then read the artifact
// directory the helper was told to use.
func TestGoOrDie_TerminatesProcessAndWritesArtifact(t *testing.T) {
	t.Helper()

	if os.Getenv("BANYANDB_RUN_GOORDIE_HELPER") == "1" {
		artifactRoot := os.Getenv("BANYANDB_RUN_GOORDIE_ARTIFACT_ROOT")
		require.NotEmpty(t, artifactRoot)
		panicdiag.SetDefaultArtifactRoot(artifactRoot)

		GoOrDie(context.Background(), "test", logger.GetLogger("test"), func(_ context.Context) {
			panic("ordie-boom")
		})

		// GoOrDie kills the process via re-panic; this sleep guards against
		// a regression where the goroutine returns normally instead.
		time.Sleep(5 * time.Second)
		t.Fatal("expected GoOrDie to terminate the helper process")
	}

	artifactRoot := t.TempDir()
	// #nosec G204 -- self-exec of the running test binary; arguments are constant.
	testCmd := exec.Command(os.Args[0], "-test.run=TestGoOrDie_TerminatesProcessAndWritesArtifact")
	testCmd.Env = append(os.Environ(),
		"BANYANDB_RUN_GOORDIE_HELPER=1",
		"BANYANDB_RUN_GOORDIE_ARTIFACT_ROOT="+artifactRoot,
	)
	runErr := testCmd.Run()
	require.Error(t, runErr)
	var exitErr *exec.ExitError
	require.ErrorAs(t, runErr, &exitErr)

	collections, listErr := panicdiag.ListCollections(artifactRoot)
	require.NoError(t, listErr)
	require.Len(t, collections, 1, "GoOrDie must persist the artifact before re-raising")
	require.NotNil(t, collections[0].Record)
	require.Equal(t, "ordie-boom", collections[0].Record.PanicValue)
	require.Equal(t, "test", collections[0].Record.Component)
}

// TestGoOrDie_RunsCleanFnToCompletion pins that GoOrDie does not introduce any
// fatal behavior for non-panicking fns: a clean run must yield Panicked=false
// just like Go.
func TestGoOrDie_RunsCleanFnToCompletion(t *testing.T) {
	t.Helper()

	executed := atomic.Bool{}
	task := GoOrDie(context.Background(), "ordie-clean", logger.GetLogger("test"), func(_ context.Context) {
		executed.Store(true)
	})

	outcome := task.Wait()
	require.True(t, executed.Load())
	require.NotNil(t, outcome)
	require.False(t, outcome.Panicked)
}

// TestGoWithSignal_DeliversPanicOutcome pins the primary use case: the channel
// carries a SignalResult whose Outcome reports the recovered panic when fn
// panics. The caller can pick it up inside a select. This is what makes the
// scheduler-style "panic vs result vs timeout" pattern correct.
func TestGoWithSignal_DeliversPanicOutcome(t *testing.T) {
	t.Helper()

	ch := GoWithSignal(context.Background(), "signal-panic", logger.GetLogger("test"),
		func(_ context.Context) string {
			panic("signal-boom")
		})

	select {
	case r := <-ch:
		require.NotNil(t, r.Outcome)
		require.True(t, r.Outcome.Panicked)
		require.NotNil(t, r.Outcome.Result.Record)
		require.Equal(t, "signal-boom", r.Outcome.Result.Record.PanicValue)
		got, ok := r.Outcome.PanicValue.(string)
		require.True(t, ok)
		require.Equal(t, "signal-boom", got)
		// Value is the zero value of T because fn never returned.
		require.Equal(t, "", r.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("expected outcome to be delivered on the signal channel")
	}
}

// TestGoWithSignal_DeliversTypedValue pins that fn's typed return value flows
// through the same channel as the outcome, so callers no longer need to plumb
// a separate result channel.
func TestGoWithSignal_DeliversTypedValue(t *testing.T) {
	t.Helper()

	ch := GoWithSignal(context.Background(), "signal-typed", logger.GetLogger("test"),
		func(_ context.Context) bool {
			return true
		})

	select {
	case r := <-ch:
		require.NotNil(t, r.Outcome)
		require.False(t, r.Outcome.Panicked)
		require.True(t, r.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("expected SignalResult to be delivered on the channel")
	}
}

// TestGoWithSignal_DeliversCleanOutcome pins that a clean fn still yields one
// SignalResult on the channel, with Panicked=false. Callers that always read
// the channel can branch on outcome.Panicked.
func TestGoWithSignal_DeliversCleanOutcome(t *testing.T) {
	t.Helper()

	executed := atomic.Bool{}
	ch := GoWithSignal(context.Background(), "signal-clean", logger.GetLogger("test"),
		func(_ context.Context) struct{} {
			executed.Store(true)
			return struct{}{}
		})

	select {
	case r := <-ch:
		require.True(t, executed.Load())
		require.NotNil(t, r.Outcome)
		require.False(t, r.Outcome.Panicked)
	case <-time.After(2 * time.Second):
		t.Fatal("expected outcome to be delivered on the signal channel")
	}
}

// TestGoWithSignal_ChannelIsBufferedAndClosed pins two contracts callers rely
// on inside selects: the channel is buffered so the goroutine never blocks on
// send (so a slow caller cannot deadlock the goroutine), and the channel is
// closed after delivery so a "drain after timeout" pattern with a second read
// does not block forever.
func TestGoWithSignal_ChannelIsBufferedAndClosed(t *testing.T) {
	t.Helper()

	ch := GoWithSignal(context.Background(), "signal-closed", logger.GetLogger("test"),
		func(_ context.Context) struct{} { return struct{}{} })

	// Pause briefly so the goroutine has time to send and close. We are not
	// trying to race anything; we only need to guarantee the goroutine is
	// done before the next reads.
	time.Sleep(50 * time.Millisecond)

	first, ok := <-ch
	require.True(t, ok, "expected at least one value on the channel")
	require.NotNil(t, first.Outcome)
	require.False(t, first.Outcome.Panicked)

	// Second read should observe the channel as closed (zero value, ok=false),
	// not block.
	_, ok = <-ch
	require.False(t, ok, "expected channel to be closed after one delivery")
}

// TestGoWithSignal_DoesNotTerminateProcessOnPanic pins that a recovered panic
// stays recovered: process continues, fn's panic does not propagate. This is
// the critical distinction from GoOrDie.
func TestGoWithSignal_DoesNotTerminateProcessOnPanic(t *testing.T) {
	t.Helper()

	ch := GoWithSignal(context.Background(), "signal-non-fatal", logger.GetLogger("test"),
		func(_ context.Context) int {
			panic("non-fatal-boom")
		})

	r := <-ch
	require.NotNil(t, r.Outcome)
	require.True(t, r.Outcome.Panicked)
	// If we reach here the process did not die: that is the contract we are
	// pinning. A regression that turned GoWithSignal into a fatal primitive
	// would never reach this assertion (the test process would have died).
}

// TestGoOrDie_WaitsForArtifactBeforeRepanic pins artifact-before-repanic.
func TestGoOrDie_WaitsForArtifactBeforeRepanic(t *testing.T) {
	t.Helper()

	if os.Getenv("BANYANDB_RUN_GOORDIE_SINK_HELPER") == "1" {
		artifactRoot := os.Getenv("BANYANDB_RUN_GOORDIE_ARTIFACT_ROOT")
		require.NotEmpty(t, artifactRoot)

		sink := panicdiag.NewArtifactSink(0)
		sink.Start()
		panicdiag.SetDefaultArtifactSink(sink)
		panicdiag.SetDefaultArtifactRoot(artifactRoot)

		GoOrDie(context.Background(), "test", logger.GetLogger("test"), func(_ context.Context) {
			panic("ordie-sink-boom")
		})

		time.Sleep(5 * time.Second)
		t.Fatal("expected GoOrDie to terminate the helper process")
	}

	artifactRoot := t.TempDir()
	// #nosec G204 -- self-exec of the running test binary; arguments are constant.
	testCmd := exec.Command(os.Args[0], "-test.run=TestGoOrDie_WaitsForArtifactBeforeRepanic")
	testCmd.Env = append(os.Environ(),
		"BANYANDB_RUN_GOORDIE_SINK_HELPER=1",
		"BANYANDB_RUN_GOORDIE_ARTIFACT_ROOT="+artifactRoot,
	)
	runErr := testCmd.Run()
	require.Error(t, runErr)
	var exitErr *exec.ExitError
	require.ErrorAs(t, runErr, &exitErr)

	// GoOrDie waited on ArtifactDone before re-raising.
	collections, listErr := panicdiag.ListCollections(artifactRoot)
	require.NoError(t, listErr)
	require.Len(t, collections, 1, "GoOrDie must persist the artifact even when the write is async via the sink")
	require.NotNil(t, collections[0].Record)
	require.Equal(t, "ordie-sink-boom", collections[0].Record.PanicValue)
}
