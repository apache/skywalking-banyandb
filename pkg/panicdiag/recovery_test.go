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

package panicdiag

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeCounter struct {
	lastLabels  []string
	lastDelta   float64
	calls       int
	deleteCalls int
	mu          sync.Mutex
}

func (f *fakeCounter) Inc(delta float64, labelValues ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastDelta = delta
	f.lastLabels = append([]string(nil), labelValues...)
}

func (f *fakeCounter) Delete(labelValues ...string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	f.lastLabels = append([]string(nil), labelValues...)
	return true
}

type fakeStateDumper struct {
	state any
	err   error
}

func (f fakeStateDumper) DumpState(_ context.Context) (any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.state, nil
}

func TestWithRecoveryRecoversAndWritesArtifacts(t *testing.T) {
	t.Helper()

	artifactRoot := t.TempDir()
	counter := &fakeCounter{}
	reported := make(chan RecoveryResult, 1)
	ctx := WithBreadcrumb(context.Background(), "poll metrics", "watchdog", map[string]string{
		"endpoint_count": "1",
	})

	WithRecovery(ctx, RecoveryOptions{
		Component:    "watchdog",
		ArtifactRoot: artifactRoot,
		Counter:      counter,
		ProcessMetadata: map[string]string{
			"node": "banyand-0",
		},
	}, func(_ context.Context, result RecoveryResult) {
		reported <- result
	}, func(_ *context.Context) {
		panic("boom")
	})

	select {
	case result := <-reported:
		if result.Record == nil {
			t.Fatal("expected panic record")
		}
		if result.Record.Component != "watchdog" {
			t.Fatalf("component mismatch: got %s", result.Record.Component)
		}
		if result.Record.PanicValue != "boom" {
			t.Fatalf("panic value mismatch: got %s", result.Record.PanicValue)
		}
		if len(result.Record.Breadcrumbs) != 1 {
			t.Fatalf("breadcrumb count mismatch: got %d want 1", len(result.Record.Breadcrumbs))
		}
		if result.Record.Breadcrumbs[0].Stage != "poll metrics" {
			t.Fatalf("breadcrumb stage mismatch: got %s", result.Record.Breadcrumbs[0].Stage)
		}
		if !strings.Contains(result.Record.GoroutineStack, "TestWithRecoveryRecoversAndWritesArtifacts") {
			t.Fatalf("stack missing test frame: %s", result.Record.GoroutineStack)
		}
		if result.ArtifactDir == "" {
			t.Fatal("expected artifact dir")
		}
		if _, err := os.Stat(filepath.Join(result.ArtifactDir, panicJSONFileName)); err != nil {
			t.Fatalf("missing crash summary file: %v", err)
		}
		if result.Record.StateDump != nil {
			t.Fatalf("unexpected state dump status: %#v", result.Record.StateDump)
		}
	default:
		t.Fatal("expected recovery reporter to be called")
	}

	if counter.calls != 1 {
		t.Fatalf("counter calls mismatch: got %d want 1", counter.calls)
	}
	if counter.lastDelta != 1 {
		t.Fatalf("counter delta mismatch: got %v want 1", counter.lastDelta)
	}
	if len(counter.lastLabels) != 1 || counter.lastLabels[0] != "watchdog" {
		t.Fatalf("counter labels mismatch: got %v", counter.lastLabels)
	}
}

func TestWithRecoveryWritesStateDump(t *testing.T) {
	t.Helper()

	artifactRoot := t.TempDir()
	reported := make(chan RecoveryResult, 1)

	WithRecovery(context.Background(), RecoveryOptions{
		Component:       "watchdog",
		ArtifactRoot:    artifactRoot,
		StateDumper:     fakeStateDumper{state: map[string]string{"pod": "banyand-0"}},
		StateLimitBytes: 1024,
	}, func(_ context.Context, result RecoveryResult) {
		reported <- result
	}, func(_ *context.Context) {
		panic("boom")
	})

	result := <-reported
	if result.Record == nil || result.Record.StateDump == nil {
		t.Fatal("expected state dump status")
	}
	if result.Record.StateDump.Error != "" {
		t.Fatalf("unexpected state dump error: %s", result.Record.StateDump.Error)
	}
	if result.Record.StateDump.Truncated {
		t.Fatal("state dump should not be truncated")
	}
	data, err := os.ReadFile(filepath.Join(result.ArtifactDir, deepDumpFileName))
	if err != nil {
		t.Fatalf("read deep dump: %v", err)
	}
	if !strings.Contains(string(data), `"pod": "banyand-0"`) {
		t.Fatalf("unexpected deep dump content: %s", string(data))
	}
}

func TestStateDumperFunc(t *testing.T) {
	t.Helper()

	capturedState := "initial"
	dumper := StateDumperFunc(func(_ context.Context) (any, error) {
		return map[string]string{"state": capturedState}, nil
	})

	capturedState = "updated"
	result, dumpErr := dumper.DumpState(context.Background())
	if dumpErr != nil {
		t.Fatalf("unexpected error: %v", dumpErr)
	}
	stateMap, ok := result.(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", result)
	}
	if stateMap["state"] != "updated" {
		t.Fatalf("expected 'updated' (current named-return value), got %s", stateMap["state"])
	}
}

func TestWithRecoveryStateDumpFailureRecorded(t *testing.T) {
	t.Helper()

	reported := make(chan RecoveryResult, 1)
	WithRecovery(context.Background(), RecoveryOptions{
		Component:       "watchdog",
		ArtifactRoot:    t.TempDir(),
		StateDumper:     fakeStateDumper{err: errors.New("snapshot failed")},
		StateLimitBytes: 1024,
	}, func(_ context.Context, result RecoveryResult) {
		reported <- result
	}, func(_ *context.Context) {
		panic("boom")
	})

	result := <-reported
	if result.Record == nil || result.Record.StateDump == nil {
		t.Fatal("expected state dump status")
	}
	if result.Record.StateDump.Error != "snapshot failed" {
		t.Fatalf("unexpected state dump error: %s", result.Record.StateDump.Error)
	}
}

func TestWithRecoveryNoPanic(t *testing.T) {
	t.Helper()

	artifactRoot := t.TempDir()
	counter := &fakeCounter{}
	reporterCalled := false

	WithRecovery(context.Background(), RecoveryOptions{
		Component:    "watchdog",
		ArtifactRoot: artifactRoot,
		Counter:      counter,
	}, func(_ context.Context, _ RecoveryResult) {
		reporterCalled = true
	}, func(_ *context.Context) {})

	if reporterCalled {
		t.Fatal("reporter should not be called")
	}
	if counter.calls != 0 {
		t.Fatalf("counter should not be incremented, got %d", counter.calls)
	}

	entries, err := os.ReadDir(artifactRoot)
	if err != nil {
		t.Fatalf("read artifact root: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no artifacts, got %d", len(entries))
	}
}

func TestGoWithRecovery(t *testing.T) {
	t.Helper()

	counter := &fakeCounter{}
	reported := make(chan RecoveryResult, 1)

	GoWithRecovery(context.Background(), RecoveryOptions{
		Component: "proxy-lifecycle",
		Counter:   counter,
	}, func(_ context.Context, result RecoveryResult) {
		reported <- result
	}, func(_ *context.Context) {
		panic("async failure")
	})

	result := <-reported
	if result.Record == nil {
		t.Fatal("expected recovery record")
	}
	if result.Record.PanicValue != "async failure" {
		t.Fatalf("panic value mismatch: got %s", result.Record.PanicValue)
	}
	if counter.calls != 1 {
		t.Fatalf("counter calls mismatch: got %d want 1", counter.calls)
	}
}

// TestReporterCompositionWithDefault pins the contract that a per-call reporter
// composes with, rather than shadows, the process-wide default reporter.
// This protects the FODC agent path where SetDefaultReporter installs an
// in-process panic store that must observe every recovered panic.
func TestReporterCompositionWithDefault(t *testing.T) {
	t.Helper()

	previousDefault := defaultReporterPtr.Load()
	t.Cleanup(func() {
		if previousDefault == nil {
			defaultReporterPtr.Store(nil)
			return
		}
		defaultReporterPtr.Store(previousDefault)
	})

	defaultCalls := make(chan RecoveryResult, 1)
	perCallCalls := make(chan RecoveryResult, 1)
	SetDefaultReporter(func(_ context.Context, result RecoveryResult) {
		defaultCalls <- result
	})

	WithRecovery(context.Background(), RecoveryOptions{
		Component: "compose",
	}, func(_ context.Context, result RecoveryResult) {
		perCallCalls <- result
	}, func(_ *context.Context) {
		panic("compose-boom")
	})

	select {
	case got := <-defaultCalls:
		if got.Record == nil || got.Record.PanicValue != "compose-boom" {
			t.Fatalf("default reporter received unexpected record: %#v", got.Record)
		}
	default:
		t.Fatal("expected default reporter to be invoked")
	}

	select {
	case got := <-perCallCalls:
		if got.Record == nil || got.Record.PanicValue != "compose-boom" {
			t.Fatalf("per-call reporter received unexpected record: %#v", got.Record)
		}
	default:
		t.Fatal("expected per-call reporter to be invoked")
	}
}

// TestDefaultAbortRunsAfterReporters pins two contracts: the process-wide
// default abort fires on a recovered panic, and abort runs after every
// reporter so observability is complete before any control-flow side
// effect happens.
func TestDefaultAbortRunsAfterReporters(t *testing.T) {
	t.Helper()

	previousReporter := defaultReporterPtr.Load()
	previousAbort := defaultAbortFuncPtr.Load()
	t.Cleanup(func() {
		if previousReporter == nil {
			defaultReporterPtr.Store(nil)
		} else {
			defaultReporterPtr.Store(previousReporter)
		}
		if previousAbort == nil {
			defaultAbortFuncPtr.Store(nil)
		} else {
			defaultAbortFuncPtr.Store(previousAbort)
		}
	})

	var (
		mu    sync.Mutex
		order []string
	)
	record := func(label string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, label)
	}

	SetDefaultReporter(func(_ context.Context, _ RecoveryResult) {
		record("default-reporter")
	})
	SetDefaultAbortFunc(func(_ context.Context, result RecoveryResult) {
		if result.Record == nil || result.Record.PanicValue != "abort-boom" {
			t.Fatalf("default abort got unexpected record: %#v", result.Record)
		}
		record("default-abort")
	})

	WithRecovery(context.Background(), RecoveryOptions{
		Component: "abort",
	}, func(_ context.Context, _ RecoveryResult) {
		record("per-call-reporter")
	}, func(_ *context.Context) {
		panic("abort-boom")
	})

	mu.Lock()
	defer mu.Unlock()
	want := []string{"default-reporter", "per-call-reporter", "default-abort"}
	if len(order) != len(want) {
		t.Fatalf("hook count mismatch: got %v want %v", order, want)
	}
	for idx, label := range want {
		if order[idx] != label {
			t.Fatalf("hook order at %d: got %s want %s (full %v)", idx, order[idx], label, order)
		}
	}
}

// TestDefaultAbortFiresBeforeReturn pins that the default abort runs inside
// the recovery defer, so it has executed before WithRecovery returns to the
// caller. Callers that re-raise from the outcome (or that the supervisor
// cancels) can rely on the abort having been observed by the time control
// returns.
func TestDefaultAbortFiresBeforeReturn(t *testing.T) {
	t.Helper()

	previousAbort := defaultAbortFuncPtr.Load()
	t.Cleanup(func() {
		if previousAbort == nil {
			defaultAbortFuncPtr.Store(nil)
			return
		}
		defaultAbortFuncPtr.Store(previousAbort)
	})

	aborted := make(chan RecoveryResult, 1)
	SetDefaultAbortFunc(func(_ context.Context, result RecoveryResult) {
		aborted <- result
	})

	outcome := WithRecovery(context.Background(), RecoveryOptions{
		Component: "abort-before-return",
	}, nil, func(_ *context.Context) {
		panic("abort-before-return-boom")
	})

	if outcome == nil || !outcome.Panicked {
		t.Fatal("expected recovered outcome")
	}
	select {
	case got := <-aborted:
		if got.Record == nil || got.Record.PanicValue != "abort-before-return-boom" {
			t.Fatalf("default abort got unexpected record: %#v", got.Record)
		}
	default:
		t.Fatal("expected default abort to have fired before WithRecovery returned")
	}
}

// TestWithRecoveryOutcomeOnPanic pins that the synchronous return value carries
// the recovered panic record so callers can react locally without having to
// smuggle state through an OnAbort closure.
func TestWithRecoveryOutcomeOnPanic(t *testing.T) {
	t.Helper()

	outcome := WithRecovery(context.Background(), RecoveryOptions{
		Component: "outcome-panic",
	}, nil, func(_ *context.Context) {
		panic("outcome-boom")
	})

	if outcome == nil {
		t.Fatal("expected non-nil outcome")
	}
	if !outcome.Panicked {
		t.Fatal("expected outcome.Panicked = true after recovered panic")
	}
	if outcome.Result.Record == nil {
		t.Fatal("expected outcome.Result.Record to be populated")
	}
	if outcome.Result.Record.PanicValue != "outcome-boom" {
		t.Fatalf("PanicValue = %q, want outcome-boom", outcome.Result.Record.PanicValue)
	}
	if got, ok := outcome.PanicValue.(string); !ok || got != "outcome-boom" {
		t.Fatalf("typed PanicValue = %#v, want %q (string)", outcome.PanicValue, "outcome-boom")
	}
}

// TestWithRecoveryOutcomePreservesTypedPanicValue pins that the typed
// PanicValue carries the original (non-stringified) panic argument, which is
// what callers need to re-raise with full fidelity.
func TestWithRecoveryOutcomePreservesTypedPanicValue(t *testing.T) {
	t.Helper()

	type customPanic struct{ msg string }

	outcome := WithRecovery(context.Background(), RecoveryOptions{
		Component: "outcome-typed",
	}, nil, func(_ *context.Context) {
		panic(customPanic{msg: "typed"})
	})

	if outcome == nil || !outcome.Panicked {
		t.Fatal("expected recovered outcome")
	}
	got, ok := outcome.PanicValue.(customPanic)
	if !ok {
		t.Fatalf("PanicValue should be customPanic, got %T", outcome.PanicValue)
	}
	if got.msg != "typed" {
		t.Fatalf("customPanic.msg = %q, want %q", got.msg, "typed")
	}
}

// TestWithRecoveryOutcomeOnSuccess pins that a clean run yields a non-nil
// outcome whose Panicked flag is false. This lets callers branch on a single
// field rather than checking nilness or string-matching log lines.
func TestWithRecoveryOutcomeOnSuccess(t *testing.T) {
	t.Helper()

	outcome := WithRecovery(context.Background(), RecoveryOptions{
		Component: "outcome-clean",
	}, nil, func(_ *context.Context) {})

	if outcome == nil {
		t.Fatal("expected non-nil outcome even on clean run")
	}
	if outcome.Panicked {
		t.Fatal("expected outcome.Panicked = false on clean run")
	}
	if outcome.Result.Record != nil {
		t.Fatalf("expected nil Record on clean run, got %#v", outcome.Result.Record)
	}
}

// TestReporterDefaultFiresWithoutPerCall confirms the default reporter still
// runs when callers do not supply their own reporter (the prior behavior).
func TestReporterDefaultFiresWithoutPerCall(t *testing.T) {
	t.Helper()

	previousDefault := defaultReporterPtr.Load()
	t.Cleanup(func() {
		if previousDefault == nil {
			defaultReporterPtr.Store(nil)
			return
		}
		defaultReporterPtr.Store(previousDefault)
	})

	defaultCalls := make(chan RecoveryResult, 1)
	SetDefaultReporter(func(_ context.Context, result RecoveryResult) {
		defaultCalls <- result
	})

	WithRecovery(context.Background(), RecoveryOptions{
		Component: "default-only",
	}, nil, func(_ *context.Context) {
		panic("default-only-boom")
	})

	select {
	case got := <-defaultCalls:
		if got.Record == nil || got.Record.PanicValue != "default-only-boom" {
			t.Fatalf("default reporter received unexpected record: %#v", got.Record)
		}
	default:
		t.Fatal("expected default reporter to be invoked")
	}
}

// originalPanicValue names the original panic used across the isolation tests
// below; goconst flags repeated literals, and a name also makes assertions
// clearer (the contract is: WithRecovery records the *original* panic, not
// whatever a buggy hook later panicked with).
const originalPanicValue = "original-boom"

// TestPanickingPerCallReporterDoesNotEscape pins that a buggy per-call reporter
// is isolated: it does not propagate as a fresh panic, and the default abort
// still fires afterward. This is the primary defense-in-depth contract: one
// bad hook must not break the rest of the recovery defer.
func TestPanickingPerCallReporterDoesNotEscape(t *testing.T) {
	t.Helper()

	previousAbort := defaultAbortFuncPtr.Load()
	t.Cleanup(func() {
		if previousAbort == nil {
			defaultAbortFuncPtr.Store(nil)
			return
		}
		defaultAbortFuncPtr.Store(previousAbort)
	})

	abortFired := make(chan struct{}, 1)
	SetDefaultAbortFunc(func(_ context.Context, _ RecoveryResult) {
		abortFired <- struct{}{}
	})

	var outcome *RecoveryOutcome
	require := func(cond bool, msg string) {
		if !cond {
			t.Fatal(msg)
		}
	}

	// The whole call must not panic; the per-call reporter's panic must be
	// absorbed by safeCall.
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("WithRecovery propagated a fresh panic from a buggy reporter: %v", rec)
			}
		}()
		outcome = WithRecovery(context.Background(), RecoveryOptions{
			Component: "isolation-reporter",
		}, func(_ context.Context, _ RecoveryResult) {
			panic("buggy-reporter-boom")
		}, func(_ *context.Context) {
			panic(originalPanicValue)
		})
	}()

	require(outcome != nil, "outcome must be populated even when reporter panics")
	require(outcome.Panicked, "Panicked must be true for the original panic")
	require(outcome.Result.Record != nil, "Record must be populated")
	require(outcome.Result.Record.PanicValue == originalPanicValue,
		"recorded panic must be the original, not the reporter's panic")

	// Default abort must still have fired even though the reporter panicked.
	select {
	case <-abortFired:
	default:
		t.Fatal("default abort did not fire: reporter panic broke isolation")
	}
}

// TestPanickingDefaultAbortDoesNotEscape pins that a buggy default abort hook
// does not propagate as a fresh panic.
func TestPanickingDefaultAbortDoesNotEscape(t *testing.T) {
	t.Helper()

	previousAbort := defaultAbortFuncPtr.Load()
	t.Cleanup(func() {
		if previousAbort == nil {
			defaultAbortFuncPtr.Store(nil)
			return
		}
		defaultAbortFuncPtr.Store(previousAbort)
	})

	SetDefaultAbortFunc(func(_ context.Context, _ RecoveryResult) {
		panic("buggy-abort-boom")
	})

	var outcome *RecoveryOutcome
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("WithRecovery propagated a fresh panic from a buggy abort: %v", rec)
			}
		}()
		outcome = WithRecovery(context.Background(), RecoveryOptions{
			Component: "isolation-abort",
		}, nil, func(_ *context.Context) {
			panic(originalPanicValue)
		})
	}()

	if outcome == nil || !outcome.Panicked {
		t.Fatal("outcome must report the original panic even when abort panics")
	}
	if outcome.Result.Record.PanicValue != originalPanicValue {
		t.Fatalf("recorded panic must be the original, got %s", outcome.Result.Record.PanicValue)
	}
}

// TestPanickingStateDumperDoesNotEscape pins that a buggy StateDumper does not
// propagate, and the recovery defer still produces a complete outcome and
// writes the artifact record (sans the failed state dump).
func TestPanickingStateDumperDoesNotEscape(t *testing.T) {
	t.Helper()

	type panickingDumper struct{}
	dumper := stateDumperFunc(func(_ context.Context) (any, error) {
		panic("buggy-dumper-boom")
	})
	_ = panickingDumper{}

	var outcome *RecoveryOutcome
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("WithRecovery propagated a fresh panic from a buggy state dumper: %v", rec)
			}
		}()
		outcome = WithRecovery(context.Background(), RecoveryOptions{
			Component:    "isolation-dumper",
			ArtifactRoot: t.TempDir(),
			StateDumper:  dumper,
		}, nil, func(_ *context.Context) {
			panic(originalPanicValue)
		})
	}()

	if outcome == nil || !outcome.Panicked {
		t.Fatal("outcome must be populated when StateDumper panics")
	}
	if outcome.Result.Record.PanicValue != originalPanicValue {
		t.Fatalf("recorded panic must be the original, got %s", outcome.Result.Record.PanicValue)
	}
}

// stateDumperFunc adapts a func to the StateDumper interface for tests.
type stateDumperFunc func(context.Context) (any, error)

func (f stateDumperFunc) DumpState(ctx context.Context) (any, error) { return f(ctx) }

// TestPanickingCounterDoesNotEscape pins that a buggy panic-counter does not
// propagate. The Counter.Inc call lives early in the recovery defer; a
// pre-isolation panic here would skip everything that follows (artifact
// write, log line, reporters, abort).
func TestPanickingCounterDoesNotEscape(t *testing.T) {
	t.Helper()

	counter := panickingCounter{}

	var outcome *RecoveryOutcome
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("WithRecovery propagated a fresh panic from a buggy counter: %v", rec)
			}
		}()
		outcome = WithRecovery(context.Background(), RecoveryOptions{
			Component: "isolation-counter",
			Counter:   counter,
		}, nil, func(_ *context.Context) {
			panic(originalPanicValue)
		})
	}()

	if outcome == nil || !outcome.Panicked {
		t.Fatal("outcome must be populated when Counter.Inc panics")
	}
	if outcome.Result.Record == nil || outcome.Result.Record.PanicValue != originalPanicValue {
		t.Fatalf("recorded panic must be the original, got %#v", outcome.Result.Record)
	}
}

type panickingCounter struct{}

func (panickingCounter) Inc(_ float64, _ ...string) { panic("buggy-counter-boom") }
func (panickingCounter) Delete(_ ...string) bool    { return false }
