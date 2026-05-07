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

	"github.com/stretchr/testify/require"
)

const testPanicValue = "boom"

func TestArtifactWriterWrite(t *testing.T) {
	t.Helper()

	now := time.Date(2026, time.April, 13, 10, 11, 12, 123456789, time.UTC)
	writer := NewArtifactWriter(t.TempDir())
	writer.nowFn = func() time.Time {
		return now
	}
	writer.pidFn = func() int {
		return 4321
	}

	record := &PanicRecord{
		OccurredAt:     now,
		Component:      "query/worker",
		PanicValue:     testPanicValue,
		Recovered:      true,
		GoroutineStack: "goroutine 1 [running]:\nstack",
		ProcessMetadata: map[string]string{
			"node": "banyand-0",
		},
	}

	artifactDir, err := writer.Write(record)
	if err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	expectedDir := filepath.Join(writer.rootDir, "20260413T101112.123456789Z-query-worker-4321")
	if artifactDir != expectedDir {
		t.Fatalf("artifact dir mismatch: got %s want %s", artifactDir, expectedDir)
	}

	summaryPath := filepath.Join(artifactDir, panicJSONFileName)
	summaryData, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read crash summary: %v", err)
	}
	var decoded PanicRecord
	if decodeErr := json.Unmarshal(summaryData, &decoded); decodeErr != nil {
		t.Fatalf("decode crash json: %v", decodeErr)
	}
	if decoded.Component != "query/worker" {
		t.Fatalf("summary missing component: %s", decoded.Component)
	}
	if decoded.PanicValue != testPanicValue {
		t.Fatalf("summary missing panic value: %s", decoded.PanicValue)
	}
}

func TestArtifactWriter_PersistsBreadcrumbs(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	artifactWriter := NewArtifactWriter(dir)
	record := &PanicRecord{
		Component:  "test",
		PanicValue: testPanicValue,
		Breadcrumbs: []Breadcrumb{
			{Stage: "handle", Component: "grpc"},
		},
		ProcessMetadata: map[string]string{
			"pod": "p1",
		},
	}

	out, err := artifactWriter.Write(record)
	require.NoError(t, err)

	collections, err := ListCollections(dir)
	require.NoError(t, err)
	require.Len(t, collections, 1)
	require.Equal(t, filepath.Base(out), collections[0].ArtifactDir)
	require.NotNil(t, collections[0].Record)
	require.Len(t, collections[0].Record.Breadcrumbs, 1)
	require.Equal(t, "handle", collections[0].Record.Breadcrumbs[0].Stage)
	require.Equal(t, "grpc", collections[0].Record.Breadcrumbs[0].Component)
	require.Equal(t, "p1", collections[0].Record.ProcessMetadata["pod"])
}
