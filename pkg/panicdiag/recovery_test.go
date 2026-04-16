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
	mu          sync.Mutex
	calls       int
	lastDelta   float64
	lastLabels  []string
	deleteCalls int
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
		if _, err := os.Stat(filepath.Join(result.ArtifactDir, panicRecordFileName)); err != nil {
			t.Fatalf("missing panic record file: %v", err)
		}
		if _, err := os.Stat(filepath.Join(result.ArtifactDir, crashTextFileName)); err != nil {
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
