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
	if err := os.WriteFile(filepath.Join(artifactDir, panicRecordFileName), append(recordData, '\n'), 0o644); err != nil {
		t.Fatalf("write panic record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(artifactDir, crashTextFileName), []byte("panic\n"), 0o644); err != nil {
		t.Fatalf("write crash text: %v", err)
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
