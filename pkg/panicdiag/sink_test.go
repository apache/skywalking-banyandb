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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const sinkTestComponent = "sink-test"

// newTestSink starts a fresh sink and registers a t.Cleanup to drain it.
func newTestSink(t *testing.T, depth int) *ArtifactSink {
	t.Helper()
	s := NewArtifactSink(depth)
	s.Start()
	t.Cleanup(func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Close(drainCtx)
	})
	return s
}

// TestArtifactSink_WritesArtifact pins the happy path: a job submitted to a
// running sink lands on disk after Close drains the queue.
func TestArtifactSink_WritesArtifact(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	s := newTestSink(t, 4)

	record := &PanicRecord{
		OccurredAt: time.Now().UTC(),
		Component:  sinkTestComponent,
		PanicValue: "submitted",
		Recovered:  true,
	}
	if !s.Submit(ArtifactJob{Record: record, RootDir: root}) {
		t.Fatal("Submit returned false on a fresh sink")
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Close(drainCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	collections, err := ListCollections(root)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(collections) != 1 {
		t.Fatalf("collections = %d, want 1", len(collections))
	}
	if collections[0].Record == nil || collections[0].Record.PanicValue != "submitted" {
		t.Fatalf("recorded panic value = %#v", collections[0].Record)
	}
}

// TestArtifactSink_OverflowDropsWithoutBlocking pins bounded Submit behavior.
func TestArtifactSink_OverflowDropsWithoutBlocking(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	s := NewArtifactSink(1) // depth-1 buffer, no worker
	job := ArtifactJob{
		Record:  &PanicRecord{Component: sinkTestComponent, OccurredAt: time.Now().UTC()},
		RootDir: root,
	}

	if !s.Submit(job) {
		t.Fatal("first Submit failed on an empty buffer")
	}

	// Buffer is now full (depth 1, no worker draining).
	done := make(chan bool, 1)
	go func() { done <- s.Submit(job) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Submit returned true on a full queue; bounded contract violated")
		}
	case <-time.After(time.Second):
		t.Fatal("Submit blocked when queue was full; bounded contract violated")
	}
}

// TestArtifactSink_CloseIsIdempotent pins that double-Close does not panic
// and the second Close still waits for the drain.
func TestArtifactSink_CloseIsIdempotent(t *testing.T) {
	t.Helper()
	s := NewArtifactSink(4)
	s.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestArtifactSink_CloseRefusesNewSubmissions pins that Submit after Close
// returns false rather than panicking on a closed channel.
func TestArtifactSink_CloseRefusesNewSubmissions(t *testing.T) {
	t.Helper()
	s := NewArtifactSink(4)
	s.Start()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if s.Submit(ArtifactJob{}) {
		t.Fatal("Submit on a closed sink returned true; should be false")
	}
}

// TestRecoveryUsesSinkWhenRegistered pins async artifact routing.
func TestRecoveryUsesSinkWhenRegistered(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	s := newTestSink(t, 4)

	prev := defaultArtifactSinkPtr.Load()
	SetDefaultArtifactSink(s)
	t.Cleanup(func() {
		if prev == nil {
			SetDefaultArtifactSink(nil)
		} else {
			defaultArtifactSinkPtr.Store(prev)
		}
	})

	WithRecovery(context.Background(), RecoveryOptions{
		Component:    sinkTestComponent,
		ArtifactRoot: root,
	}, nil, func(_ *context.Context) {
		panic("sink-route")
	})

	// Drain the sink so we can read the file.
	drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Cleanup closes again; Close is idempotent.
	_ = s.Close(drainCtx)

	collections, err := ListCollections(root)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(collections) != 1 || collections[0].Record == nil ||
		collections[0].Record.PanicValue != "sink-route" {
		t.Fatalf("expected one record with PanicValue=sink-route, got %#v", collections)
	}
}

// TestRecoverySignalsFireBeforeSinkDrains pins signal-before-artifact ordering.
func TestRecoverySignalsFireBeforeSinkDrains(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	gate := make(chan struct{})
	s := NewArtifactSink(4)
	// Block the worker so the artifact remains queued.
	s.started.Store(true)
	go func() {
		defer close(s.drained)
		<-gate
		for range s.queue {
			// drain remaining jobs after the gate releases
		}
	}()
	t.Cleanup(func() {
		// Release the worker so it can exit, then drain via the public API.
		select {
		case <-gate:
		default:
			close(gate)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})

	prevSink := defaultArtifactSinkPtr.Load()
	SetDefaultArtifactSink(s)
	t.Cleanup(func() {
		if prevSink == nil {
			SetDefaultArtifactSink(nil)
		} else {
			defaultArtifactSinkPtr.Store(prevSink)
		}
	})

	prevAbort := defaultAbortFuncPtr.Load()
	t.Cleanup(func() {
		if prevAbort == nil {
			defaultAbortFuncPtr.Store(nil)
			return
		}
		defaultAbortFuncPtr.Store(prevAbort)
	})

	abortFired := make(chan time.Time, 1)
	SetDefaultAbortFunc(func(_ context.Context, _ RecoveryResult) {
		abortFired <- time.Now()
	})

	WithRecovery(context.Background(), RecoveryOptions{
		Component:    sinkTestComponent,
		ArtifactRoot: root,
	}, nil, func(_ *context.Context) {
		panic("signal-first")
	})

	// Abort must already have fired even though the sink worker is blocked.
	select {
	case <-abortFired:
	case <-time.After(2 * time.Second):
		t.Fatal("default abort did not fire while sink was wedged; signal-first contract broken")
	}

	// Artifact must NOT yet be on disk (sink worker is still blocked).
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				if _, statErr := os.Stat(filepath.Join(root, e.Name(), panicJSONFileName)); statErr == nil {
					t.Fatalf("artifact written before sink drained: sink not actually being used")
				}
			}
		}
	}
}

// TestSinkUsesPrivateRecordClone pins that the worker mutates only its clone.
func TestSinkUsesPrivateRecordClone(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	gate := make(chan struct{})
	s := NewArtifactSink(4)
	s.started.Store(true)
	go func() {
		defer close(s.drained)
		<-gate
		for job := range s.queue {
			runArtifactJob(job)
		}
	}()
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})

	prev := defaultArtifactSinkPtr.Load()
	SetDefaultArtifactSink(s)
	t.Cleanup(func() {
		if prev == nil {
			SetDefaultArtifactSink(nil)
		} else {
			defaultArtifactSinkPtr.Store(prev)
		}
	})

	reported := make(chan *PanicRecord, 1)
	WithRecovery(context.Background(), RecoveryOptions{
		Component:       sinkTestComponent,
		ArtifactRoot:    root,
		StateDumper:     fakeStateDumper{state: map[string]string{"k": "v"}},
		StateLimitBytes: 1024,
	}, func(_ context.Context, result RecoveryResult) {
		reported <- result.Record
	}, func(_ *context.Context) {
		panic("clone-isolation")
	})

	callerRecord := <-reported

	// Worker is still gated, so caller-visible StateDump must be nil.
	if callerRecord == nil {
		t.Fatal("reporter did not receive a record")
	}
	if callerRecord.PanicValue != "clone-isolation" {
		t.Fatalf("PanicValue = %q, want clone-isolation", callerRecord.PanicValue)
	}
	if callerRecord.StateDump != nil {
		t.Fatalf("caller's record was mutated before the worker ran: %#v", callerRecord.StateDump)
	}

	// Release the worker; it writes its clone to disk.
	close(gate)

	// Drain via Close so the worker has finished by the time we read.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("sink Close: %v", err)
	}

	// Caller's record was never the one the worker wrote: still nil.
	if callerRecord.StateDump != nil {
		t.Fatalf("caller's record was mutated by the worker after drain: %#v",
			callerRecord.StateDump)
	}

	// The on-disk artifact has StateDump from the worker's clone.
	collections, err := ListCollections(root)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(collections) != 1 {
		t.Fatalf("collections = %d, want 1", len(collections))
	}
	if collections[0].Record == nil || collections[0].Record.StateDump == nil {
		t.Fatalf("on-disk record missing StateDump: %#v", collections[0].Record)
	}
}

// TestArtifactSink_SubmitDoesNotPanicOnConcurrentClose pins Submit/Close safety.
func TestArtifactSink_SubmitDoesNotPanicOnConcurrentClose(t *testing.T) {
	t.Helper()

	const trials = 32
	const submitsPerTrial = 256

	for trial := 0; trial < trials; trial++ {
		s := NewArtifactSink(2)
		s.Start()

		var wg sync.WaitGroup
		wg.Add(2)

		// Producer: hammer Submit while Close fires concurrently.
		go func() {
			defer wg.Done()
			for i := 0; i < submitsPerTrial; i++ {
				// Submit may return true or false, but must not panic.
				defer func() {
					if rec := recover(); rec != nil {
						t.Errorf("Submit panicked under concurrent Close: %v", rec)
					}
				}()
				s.Submit(ArtifactJob{
					Record: &PanicRecord{
						Component:  "race",
						OccurredAt: time.Now().UTC(),
					},
				})
			}
		}()

		// Closer: race the producer.
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = s.Close(ctx)
		}()

		wg.Wait()
	}
}

// TestRecoveryFallsBackToSyncWithoutSink pins the sync fallback.
func TestRecoveryFallsBackToSyncWithoutSink(t *testing.T) {
	t.Helper()

	prev := defaultArtifactSinkPtr.Load()
	SetDefaultArtifactSink(nil)
	t.Cleanup(func() {
		if prev != nil {
			defaultArtifactSinkPtr.Store(prev)
		}
	})

	root := t.TempDir()
	WithRecovery(context.Background(), RecoveryOptions{
		Component:    sinkTestComponent,
		ArtifactRoot: root,
	}, nil, func(_ *context.Context) {
		panic("sync-fallback")
	})

	// No sink: write completed synchronously.
	collections, err := ListCollections(root)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(collections) != 1 ||
		collections[0].Record.PanicValue != "sync-fallback" {
		t.Fatalf("expected one synchronous record, got %#v", collections)
	}
}

// TestRecoveryReporterReceivesPredictedArtifactDir pins that the per-call
// reporter receives a non-empty ArtifactDir even when the sink has not yet
// run. The path is the deterministic location the worker will create.
func TestRecoveryReporterReceivesPredictedArtifactDir(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	s := newTestSink(t, 4)

	prev := defaultArtifactSinkPtr.Load()
	SetDefaultArtifactSink(s)
	t.Cleanup(func() {
		if prev == nil {
			SetDefaultArtifactSink(nil)
		} else {
			defaultArtifactSinkPtr.Store(prev)
		}
	})

	type observed struct {
		dir string
		mu  sync.Mutex
	}
	got := &observed{}

	WithRecovery(context.Background(), RecoveryOptions{
		Component:    sinkTestComponent,
		ArtifactRoot: root,
	}, func(_ context.Context, result RecoveryResult) {
		got.mu.Lock()
		defer got.mu.Unlock()
		got.dir = result.ArtifactDir
	}, func(_ *context.Context) {
		panic("predicted-dir")
	})

	got.mu.Lock()
	dir := got.dir
	got.mu.Unlock()

	if dir == "" {
		t.Fatal("reporter received empty ArtifactDir; pre-computation failed")
	}
	// The predicted path must be a child of the configured root.
	if rel, err := filepath.Rel(root, dir); err != nil || rel == ".." || rel[:2] == ".." {
		t.Fatalf("predicted dir %q not under root %q (rel=%q err=%v)", dir, root, rel, err)
	}
}
