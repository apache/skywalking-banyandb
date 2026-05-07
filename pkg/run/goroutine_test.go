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

// TestGo_WithRepanicTerminatesProcess pins that WithRepanic(true) restores
// the previous fatal-on-panic behavior for callers that need it. We exec a
// subprocess so we can assert the process actually dies.
func TestGo_WithRepanicTerminatesProcess(t *testing.T) {
	t.Helper()

	if os.Getenv("BANYANDB_RUN_GO_REPANIC_HELPER") == "1" {
		Go(context.Background(), "test", logger.GetLogger("test"), func(_ context.Context) {
			panic("repanic-boom")
		}, WithRepanic(true))

		// Repanic kills the process; this sleep guards against a regression
		// where Repanic silently becomes a no-op.
		time.Sleep(5 * time.Second)
		t.Fatal("expected WithRepanic(true) to terminate the helper process")
	}

	// #nosec G204 -- self-exec of the running test binary; arguments are constant.
	testCmd := exec.Command(os.Args[0], "-test.run=TestGo_WithRepanicTerminatesProcess")
	testCmd.Env = append(os.Environ(), "BANYANDB_RUN_GO_REPANIC_HELPER=1")
	runErr := testCmd.Run()
	require.Error(t, runErr)
	var exitErr *exec.ExitError
	require.ErrorAs(t, runErr, &exitErr)
}
