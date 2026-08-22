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

package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestExecutionDetailLinesOmitsRawJSONWhenPreviewExists(t *testing.T) {
	executionResult := session.ExecutionResult{
		Summary:  "executed stream BYDBQL query",
		Response: `{"streamResult":{"elements":[{"elementId":"abc"}]}}`,
		Columns:  []string{"timestamp", "trace_id", "content"},
		Preview: [][]string{
			{"2026-07-12T14:12:39.279Z", "trace-1", "Listing top songs"},
		},
	}
	lines := executionDetailLines(executionResult, executionDisplayOptions{width: 100})
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "Table preview") {
		t.Fatalf("expected table preview section, got:\n%s", joined)
	}
	if strings.Contains(joined, `"streamResult"`) {
		t.Fatalf("expected raw JSON to be omitted when preview exists, got:\n%s", joined)
	}
	if !strings.Contains(joined, "Full JSON hidden") {
		t.Fatalf("expected hidden-json note, got:\n%s", joined)
	}
}

func TestExecutionDetailLinesShowsRawJSONWhenRequested(t *testing.T) {
	executionResult := session.ExecutionResult{
		Summary:  "executed stream BYDBQL query",
		Response: `{"streamResult":{"elements":[{"elementId":"abc"}]}}`,
		Columns:  []string{"timestamp"},
		Preview:  [][]string{{"2026-07-12T14:12:39.279Z"}},
	}
	lines := executionDetailLines(executionResult, executionDisplayOptions{width: 100, showRaw: true})
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, `"streamResult"`) {
		t.Fatalf("expected raw JSON when showRaw=true, got:\n%s", joined)
	}
}

func TestFormatPreviewTableAlignsColumns(t *testing.T) {
	table := formatPreviewTable(
		[]string{"trace_id", "content"},
		[][]string{
			{"short", "alpha"},
			{"much-longer-trace", "beta"},
		},
		120,
		1,
	)
	if len(table) < 3 {
		t.Fatalf("expected header, separator, and rows, got %v", table)
	}
	if !strings.Contains(table[0], "trace_id") || !strings.Contains(table[0], "content") {
		t.Fatalf("unexpected header: %q", table[0])
	}
	if !strings.Contains(table[3], "much-longer-trace") {
		t.Fatalf("unexpected row: %q", table[3])
	}
	if !strings.HasPrefix(table[3], ">") {
		t.Fatalf("expected selected row marker, got %q", table[3])
	}
}

func TestFormatJSONResponsePreviewSanitizesBinary(t *testing.T) {
	lines := formatJSONResponsePreview(`{"tags_raw_data":"AAAA","items":[{"binaryData":"BBBB"}]}`, 80, 20)
	joined := strings.Join(lines, "\n")
	if strings.Contains(joined, "AAAA") || strings.Contains(joined, "BBBB") {
		t.Fatalf("expected binary fields to be sanitized, got:\n%s", joined)
	}
	if !strings.Contains(joined, "omitted") {
		t.Fatalf("expected omitted marker, got:\n%s", joined)
	}
}

func TestSelectDisplayColumnsLimitsWidth(t *testing.T) {
	columns := []string{
		"timestamp", "trace_id", "endpoint_id", "content", "service_id",
		"service_instance_id", "span_id", "tags", "elementId",
	}
	selected := selectDisplayColumns(columns)
	if len(selected) != maxExecutionTableColumns {
		t.Fatalf("expected %d columns, got %v", maxExecutionTableColumns, selected)
	}
	if selected[0] != "timestamp" || selected[1] != "trace_id" {
		t.Fatalf("unexpected column order: %v", selected)
	}
}

func TestExportExecutionResultWritesFile(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	executionResult := session.ExecutionResult{
		Query:    "SELECT * FROM STREAM logs LIMIT 1",
		Summary:  "executed stream BYDBQL query",
		Response: `{"streamResult":{"elements":[]}}`,
		Columns:  []string{"trace_id"},
		Preview:  [][]string{{"trace-1"}},
		Rows:     1,
	}
	exportPath, exportErr := exportExecutionResult(executionResult)
	if exportErr != nil {
		t.Fatalf("exportExecutionResult returned error: %v", exportErr)
	}
	expectedPrefix := filepath.Join(homeDir, ".bydbctl", "exports")
	if !strings.HasPrefix(exportPath, expectedPrefix) {
		t.Fatalf("unexpected export path: %s", exportPath)
	}
	payload, readErr := os.ReadFile(exportPath)
	if readErr != nil {
		t.Fatalf("failed to read export file: %v", readErr)
	}
	joined := string(payload)
	if !strings.Contains(joined, `"query"`) || !strings.Contains(joined, `"response"`) {
		t.Fatalf("unexpected export payload: %s", joined)
	}
}
