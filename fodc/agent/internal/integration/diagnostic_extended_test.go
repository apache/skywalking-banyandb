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

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/crashcollector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

func diagnosticLogger(t *testing.T) *logger.Logger {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "debug"}))
	return logger.GetLogger("test", "diagnostic-extended")
}

// TestStateDumpFilesDetectedByWatcher verifies that when a panic is recovered with a
// StateDumperFunc, the resulting artifact directory contains deep-dump.json and
// that DirectoryWatcher surfaces it in Collection.Files alongside the mandatory panic.json.
func TestStateDumpFilesDetectedByWatcher(t *testing.T) {
	t.Helper()

	artifactRoot := t.TempDir()
	log := diagnosticLogger(t)

	panicdiag.WithRecovery(context.Background(), panicdiag.RecoveryOptions{
		Component:    "storage-engine",
		ArtifactRoot: artifactRoot,
		StateDumper: panicdiag.StateDumperFunc(func(_ context.Context) (any, error) {
			return map[string]any{
				"pendingWrites": 42,
				"activeShards":  []string{"shard-0", "shard-1"},
				"lastFlushAt":   "2026-04-20T08:00:00Z",
			}, nil
		}),
	}, nil, func(_ *context.Context) {
		panic("simulated storage fault")
	})

	watcher := crashcollector.NewDirectoryWatcher(log, artifactRoot, crashcollector.Config{})
	watcher.Scan()

	records := watcher.ListCollections()
	require.Len(t, records, 1, "expected exactly one crash collection")

	files := records[0].Collection.Files
	assert.Contains(t, files, "panic.json")
	assert.Contains(t, files, "deep-dump.json", "state dump JSON should appear in Collection.Files")

	rec := records[0].Collection.Record
	require.NotNil(t, rec)
	assert.Equal(t, "storage-engine", rec.Component)
	assert.Equal(t, "simulated storage fault", rec.PanicValue)
}

// TestIncompleteArtifactIgnoredUntilComplete verifies that an artifact directory
// that contains auxiliary files but not panic.json is not stored in the ring
// buffer until the artifact becomes complete. A directory with no panic.json at
// all must be silently ignored by ListCollections.
func TestIncompleteArtifactIgnoredUntilComplete(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	log := diagnosticLogger(t)

	// Create a subdirectory that has auxiliary data but no panic.json.
	artifactSubdir := filepath.Join(dir, "20260420T080000.000000000Z-incomplete-99999")
	require.NoError(t, os.MkdirAll(artifactSubdir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(artifactSubdir, "deep-dump.json"), []byte(`{"state":"partial"}`), 0o600))

	watcher := crashcollector.NewDirectoryWatcher(log, dir, crashcollector.Config{})
	watcher.Scan()

	records := watcher.ListCollections()
	require.Empty(t, records, "incomplete artifact should not be stored in the ring buffer")

	// A directory with no panic.json must be silently ignored.
	emptySubdir := filepath.Join(dir, "20260420T090000.000000000Z-no-record-88888")
	require.NoError(t, os.MkdirAll(emptySubdir, 0o755))
	watcher.Scan()

	records = watcher.ListCollections()
	require.Empty(t, records, "directory without panic.json should be silently skipped")

	panicJSON := []byte(`{"component":"incomplete-component","panicValue":"partial write",` +
		`"goroutineStack":"goroutine 1 [running]:\n","occurredAt":"2026-04-20T08:00:00Z","recovered":false}`)
	require.NoError(t, os.WriteFile(filepath.Join(artifactSubdir, "panic.json"), panicJSON, 0o600))
	watcher.Scan()

	records = watcher.ListCollections()
	require.Len(t, records, 1, "artifact should be stored once all required files exist")
	assert.Equal(t, "incomplete-component", records[0].Collection.Record.Component)
}

// TestDirectoryWatcherEvictsOldestOnRingBufferOverflow writes six crash artifacts
// one at a time, triggering a Scan after each write so the ring buffer receives
// them in chronological (oldest-to-newest) order. With a buffer capacity of three,
// the three oldest must be evicted, leaving the three newest.
func TestDirectoryWatcherEvictsOldestOnRingBufferOverflow(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	log := diagnosticLogger(t)

	type componentCrash struct {
		occurredAt time.Time
		name       string
	}
	crashes := []componentCrash{
		{name: "alpha", occurredAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC)},
		{name: "beta", occurredAt: time.Date(2026, time.April, 20, 10, 0, 1, 0, time.UTC)},
		{name: "gamma", occurredAt: time.Date(2026, time.April, 20, 10, 0, 2, 0, time.UTC)},
		{name: "delta", occurredAt: time.Date(2026, time.April, 20, 10, 0, 3, 0, time.UTC)},
		{name: "epsilon", occurredAt: time.Date(2026, time.April, 20, 10, 0, 4, 0, time.UTC)},
		{name: "zeta", occurredAt: time.Date(2026, time.April, 20, 10, 0, 5, 0, time.UTC)},
	}

	const (
		bufSize              = 3
		estimatedRecordBytes = 16 * 1024
	)
	writer := panicdiag.NewArtifactWriter(dir)
	watcher := crashcollector.NewDirectoryWatcher(log, dir, crashcollector.Config{
		CapacitySizeBytes: int64(bufSize * estimatedRecordBytes),
	})

	// Write and scan incrementally so the ring buffer receives arrivals in
	// chronological order and applies LRU eviction correctly.
	for _, c := range crashes {
		_, writeErr := writer.Write(&panicdiag.PanicRecord{
			OccurredAt: c.occurredAt,
			Component:  c.name,
			PanicValue: fmt.Sprintf("panic from %s", c.name),
			Recovered:  true,
		})
		require.NoError(t, writeErr)
		watcher.Scan()
	}

	records := watcher.ListCollections()
	require.Len(t, records, bufSize, "ring buffer should hold exactly bufSize records")

	retained := make(map[string]bool, bufSize)
	for _, r := range records {
		retained[r.Collection.Record.Component] = true
	}

	assert.True(t, retained["delta"], "delta should be retained")
	assert.True(t, retained["epsilon"], "epsilon should be retained")
	assert.True(t, retained["zeta"], "zeta should be retained")
	assert.False(t, retained["alpha"], "alpha should have been evicted")
	assert.False(t, retained["beta"], "beta should have been evicted")
	assert.False(t, retained["gamma"], "gamma should have been evicted")
}

// TestMultipleComponentCrashesPreserveMetadata writes crash artifacts for four
// distinct BanyanDB components and verifies that DirectoryWatcher stores all of
// them with their correct Component, PanicValue, and Recovered fields intact.
func TestMultipleComponentCrashesPreserveMetadata(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	log := diagnosticLogger(t)

	type componentCrash struct {
		component  string
		panicValue string
		recovered  bool
	}
	crashes := []componentCrash{
		{"data-node", "nil pointer dereference in flush", true},
		{"liaison-node", "concurrent map write", true},
		{"query-engine", "index out of range [5] with length 5", false},
		{"storage-compactor", "runtime: out of memory", false},
	}

	writer := panicdiag.NewArtifactWriter(dir)
	for idx, c := range crashes {
		_, writeErr := writer.Write(&panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 12, 0, idx, 0, time.UTC),
			Component:  c.component,
			PanicValue: c.panicValue,
			Recovered:  c.recovered,
		})
		require.NoError(t, writeErr)
	}

	watcher := crashcollector.NewDirectoryWatcher(log, dir, crashcollector.Config{})
	watcher.Scan()

	records := watcher.ListCollections()
	require.Len(t, records, len(crashes), "all crash artifacts should be detected")

	byComponent := make(map[string]panicdiag.PanicRecord, len(records))
	for _, r := range records {
		require.NotNil(t, r.Collection.Record)
		byComponent[r.Collection.Record.Component] = *r.Collection.Record
	}

	for _, want := range crashes {
		got, exists := byComponent[want.component]
		require.True(t, exists, "component %q should be in collected records", want.component)
		assert.Equal(t, want.panicValue, got.PanicValue, "PanicValue mismatch for %q", want.component)
		assert.Equal(t, want.recovered, got.Recovered, "Recovered flag mismatch for %q", want.component)
	}
}

// TestBreadcrumbsStayInReporter verifies that breadcrumbs appended inside a
// WithRecovery-protected function are reported in process.
func TestBreadcrumbsStayInReporter(t *testing.T) {
	t.Helper()

	artifactRoot := t.TempDir()
	reported := make(chan panicdiag.RecoveryResult, 1)

	// Install a mutable store so breadcrumbs appended inside fn are visible to
	// the recovery defer, which captures ctx before fn executes.
	ctx := panicdiag.WithMutableBreadcrumbs(context.Background())

	panicdiag.WithRecovery(ctx, panicdiag.RecoveryOptions{
		Component:    "query-executor",
		ArtifactRoot: artifactRoot,
	}, func(_ context.Context, result panicdiag.RecoveryResult) {
		reported <- result
	}, func(ctxp *context.Context) {
		*ctxp = panicdiag.WithBreadcrumb(*ctxp, "parse-query", "query-parser",
			map[string]string{"query_id": "q-42", "table": "metrics"})
		*ctxp = panicdiag.WithBreadcrumb(*ctxp, "build-plan", "planner", nil)
		*ctxp = panicdiag.WithBreadcrumb(*ctxp, "execute-scan", "executor",
			map[string]string{"shard": "shard-3"})
		panic("breadcrumb integration panic")
	})

	result := <-reported
	rec := result.Record
	require.NotNil(t, rec)
	assert.Equal(t, "query-executor", rec.Component)

	require.Len(t, rec.Breadcrumbs, 3, "all three breadcrumbs should stay in the reported panic record")
	assert.Equal(t, "parse-query", rec.Breadcrumbs[0].Stage)
	assert.Equal(t, "query-parser", rec.Breadcrumbs[0].Component)
	assert.Equal(t, "q-42", rec.Breadcrumbs[0].Fields["query_id"])
	assert.Equal(t, "metrics", rec.Breadcrumbs[0].Fields["table"])
	assert.Equal(t, "build-plan", rec.Breadcrumbs[1].Stage)
	assert.Empty(t, rec.Breadcrumbs[1].Fields, "planner breadcrumb has no fields")
	assert.Equal(t, "execute-scan", rec.Breadcrumbs[2].Stage)
	assert.Equal(t, "shard-3", rec.Breadcrumbs[2].Fields["shard"])
}
