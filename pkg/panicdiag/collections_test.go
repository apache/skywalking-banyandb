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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeMinimalArtifact(t *testing.T, rootDir, name string) {
	t.Helper()
	artifactDir := filepath.Join(rootDir, name)
	if mkdirErr := os.MkdirAll(artifactDir, 0o755); mkdirErr != nil {
		t.Fatalf("create artifact dir: %v", mkdirErr)
	}
	record := PanicRecord{
		OccurredAt: time.Now().UTC(),
		Component:  "test",
		PanicValue: "boom",
	}
	recordData, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		t.Fatalf("marshal panic record: %v", marshalErr)
	}
	if writeErr := os.WriteFile(filepath.Join(artifactDir, panicRecordFileName), append(recordData, '\n'), 0o600); writeErr != nil {
		t.Fatalf("write panic record: %v", writeErr)
	}
}

func TestPruneArtifactsRemovesOldest(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	// Create 5 artifacts with names that sort oldest-first lexicographically.
	names := []string{
		"20260101T000000.000000000Z-comp-1",
		"20260102T000000.000000000Z-comp-1",
		"20260103T000000.000000000Z-comp-1",
		"20260104T000000.000000000Z-comp-1",
		"20260105T000000.000000000Z-comp-1",
	}
	for _, name := range names {
		writeMinimalArtifact(t, rootDir, name)
	}

	// Keep only the 3 newest; 2 oldest should be removed.
	if pruneErr := PruneArtifacts(rootDir, 3); pruneErr != nil {
		t.Fatalf("PruneArtifacts: %v", pruneErr)
	}

	entries, readErr := os.ReadDir(rootDir)
	if readErr != nil {
		t.Fatalf("read rootDir: %v", readErr)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 dirs after pruning, got %d", len(entries))
	}
	remaining := make([]string, 0, len(entries))
	for _, e := range entries {
		remaining = append(remaining, e.Name())
	}
	for _, name := range names[:2] {
		for _, rem := range remaining {
			if rem == name {
				t.Fatalf("expected %s to be pruned, but it still exists", name)
			}
		}
	}
}

func TestPruneArtifactsNopWhenUnderLimit(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	writeMinimalArtifact(t, rootDir, "20260101T000000.000000000Z-comp-1")
	writeMinimalArtifact(t, rootDir, "20260102T000000.000000000Z-comp-1")

	if pruneErr := PruneArtifacts(rootDir, 5); pruneErr != nil {
		t.Fatalf("PruneArtifacts: %v", pruneErr)
	}

	entries, readErr := os.ReadDir(rootDir)
	if readErr != nil {
		t.Fatalf("read rootDir: %v", readErr)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 dirs, got %d", len(entries))
	}
}

func TestPruneArtifactsIgnoresNonArtifactDirs(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	// Non-artifact dir (no panic.json).
	if mkdirErr := os.MkdirAll(filepath.Join(rootDir, "not-an-artifact"), 0o755); mkdirErr != nil {
		t.Fatalf("create non-artifact dir: %v", mkdirErr)
	}
	writeMinimalArtifact(t, rootDir, "20260101T000000.000000000Z-comp-1")
	writeMinimalArtifact(t, rootDir, "20260102T000000.000000000Z-comp-1")
	writeMinimalArtifact(t, rootDir, "20260103T000000.000000000Z-comp-1")

	// maxArtifacts=2 should remove only the oldest real artifact, not the non-artifact dir.
	if pruneErr := PruneArtifacts(rootDir, 2); pruneErr != nil {
		t.Fatalf("PruneArtifacts: %v", pruneErr)
	}

	if _, statErr := os.Stat(filepath.Join(rootDir, "not-an-artifact")); statErr != nil {
		t.Fatalf("non-artifact dir should not be removed: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(rootDir, "20260101T000000.000000000Z-comp-1")); !os.IsNotExist(statErr) {
		t.Fatal("expected oldest artifact to be pruned")
	}
}

func TestListCollections(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	artifactDir := filepath.Join(rootDir, "20260413T120000.000000000Z-measure-1234")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Fatalf("create artifact dir: %v", err)
	}
	record := PanicRecord{
		OccurredAt:     time.Date(2026, time.April, 13, 12, 0, 0, 0, time.UTC),
		Component:      "measure",
		PanicValue:     "boom",
		Recovered:      true,
		GoroutineStack: "stack",
	}
	recordData, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal panic record: %v", err)
	}
	if writeErr := os.WriteFile(filepath.Join(artifactDir, panicRecordFileName), append(recordData, '\n'), 0o600); writeErr != nil {
		t.Fatalf("write panic record: %v", writeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(artifactDir, crashTextFileName), []byte("panic\n"), 0o600); writeErr != nil {
		t.Fatalf("write crash text: %v", writeErr)
	}

	collections, err := ListCollections(rootDir)
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(collections) != 1 {
		t.Fatalf("collection count mismatch: got %d want 1", len(collections))
	}
	if collections[0].ArtifactDir != filepath.Base(artifactDir) {
		t.Fatalf("artifact dir mismatch: got %s want %s", collections[0].ArtifactDir, filepath.Base(artifactDir))
	}
	if collections[0].Record == nil || collections[0].Record.Component != "measure" {
		t.Fatalf("unexpected record: %#v", collections[0].Record)
	}
}
