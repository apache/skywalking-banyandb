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

package crashcollector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

func testLogger(t *testing.T) *logger.Logger {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	return logger.GetLogger("test", t.Name())
}

func TestDirectoryWatcherScanDetectsArtifacts(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	writer := panicdiag.NewArtifactWriter(dir)
	_, writeErr := writer.Write(&panicdiag.PanicRecord{
		OccurredAt: time.Date(2026, time.April, 15, 8, 0, 0, 0, time.UTC),
		Component:  "test-component",
		PanicValue: "boom",
		Recovered:  true,
	})
	require.NoError(t, writeErr)

	watcher := NewDirectoryWatcher(testLogger(t), dir, Config{})
	watcher.Scan()

	records := watcher.ListCollections()
	require.Len(t, records, 1)
	assert.Equal(t, fsSourcePrefix+dir, records[0].SourceEndpoint)
	assert.Equal(t, "test-component", records[0].Collection.Record.Component)
}

func TestDirectoryWatcherDeduplicates(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	writer := panicdiag.NewArtifactWriter(dir)
	_, writeErr := writer.Write(&panicdiag.PanicRecord{
		Component:  "test-component",
		PanicValue: "boom",
	})
	require.NoError(t, writeErr)

	watcher := NewDirectoryWatcher(testLogger(t), dir, Config{})
	watcher.Scan()
	watcher.Scan()

	records := watcher.ListCollections()
	require.Len(t, records, 1, "duplicate scan should not add duplicate records")
}

func TestDirectoryWatcherDetectsMultipleArtifacts(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	writer := panicdiag.NewArtifactWriter(dir)
	_, firstErr := writer.Write(&panicdiag.PanicRecord{
		OccurredAt: time.Date(2026, time.April, 15, 8, 0, 0, 0, time.UTC),
		Component:  "component-a",
		PanicValue: "first panic",
	})
	require.NoError(t, firstErr)

	_, secondErr := writer.Write(&panicdiag.PanicRecord{
		OccurredAt: time.Date(2026, time.April, 15, 8, 0, 1, 0, time.UTC),
		Component:  "component-b",
		PanicValue: "second panic",
	})
	require.NoError(t, secondErr)

	watcher := NewDirectoryWatcher(testLogger(t), dir, Config{})
	watcher.Scan()

	records := watcher.ListCollections()
	require.Len(t, records, 2)
}

func TestDirectoryWatcherStartDetectsArtifactsViaFSNotify(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	watcher := NewDirectoryWatcher(testLogger(t), dir, Config{})

	ctx, cancel := context.WithCancel(context.Background())
	stop := watcher.Start(ctx)
	require.NotNil(t, stop)
	t.Cleanup(func() {
		cancel()
		stop()
	})

	writer := panicdiag.NewArtifactWriter(dir)
	_, writeErr := writer.Write(&panicdiag.PanicRecord{
		Component:  "fs-notify-test",
		PanicValue: "detected via fsnotify",
		Recovered:  true,
	})
	require.NoError(t, writeErr)

	require.Eventually(t, func() bool {
		records := watcher.ListCollections()
		for _, record := range records {
			if record.Collection.Record != nil && record.Collection.Record.Component == "fs-notify-test" {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)
}

func TestDirectoryWatcherScanWaitsForCompleteArtifacts(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	artifactDir := filepath.Join(dir, "20260415T080000.000000000Z-fs-notify-test-123")
	require.NoError(t, os.MkdirAll(artifactDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(artifactDir, "deep-dump.json"), []byte(`{"state":"partial"}`), 0o600))

	watcher := NewDirectoryWatcher(testLogger(t), dir, Config{})
	watcher.Scan()
	assert.Empty(t, watcher.ListCollections(), "incomplete artifacts should not be stored")

	require.NoError(t, os.WriteFile(filepath.Join(artifactDir, "crash.txt"), []byte(`BanyanDB panic recovered
OccurredAt: 2026-04-15T08:00:00Z
Component: fs-notify-test
Recovered: true
Panic: detected via fsnotify

Stack:
goroutine 1 [running]:
`), 0o600))

	watcher.Scan()
	records := watcher.ListCollections()
	require.Len(t, records, 1)
	require.NotNil(t, records[0].Collection.Record)
	assert.Equal(t, "fs-notify-test", records[0].Collection.Record.Component)
}

func TestAnalyzeCrashArtifactComplete(t *testing.T) {
	t.Helper()

	collection := &panicdiag.Collection{
		ArtifactDir: "20260415T080000.000000000Z-test-123",
		Files:       []string{"crash.txt"},
	}
	analysis := analyzeCrashArtifact(collection)
	assert.True(t, analysis.Complete)
	assert.Empty(t, analysis.MissingFiles)
}

func TestAnalyzeCrashArtifactIncomplete(t *testing.T) {
	t.Helper()

	collection := &panicdiag.Collection{
		ArtifactDir: "20260415T080000.000000000Z-test-123",
		Files:       []string{"deep-dump.json"},
	}
	analysis := analyzeCrashArtifact(collection)
	assert.False(t, analysis.Complete)
	assert.Equal(t, []string{"crash.txt"}, analysis.MissingFiles)
}
