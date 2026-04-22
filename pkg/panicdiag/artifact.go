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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	panicRecordFileName = "panic.json"
	crashTextFileName   = "crash.txt"
)

// ArtifactWriter writes panic artifacts to disk.
type ArtifactWriter struct {
	nowFn   func() time.Time
	pidFn   func() int
	rootDir string
}

// NewArtifactWriter returns a new ArtifactWriter.
func NewArtifactWriter(rootDir string) *ArtifactWriter {
	return &ArtifactWriter{
		rootDir: rootDir,
		nowFn:   time.Now,
		pidFn:   os.Getpid,
	}
}

// Write persists the given panic record and returns the artifact directory.
func (aw *ArtifactWriter) Write(record *PanicRecord) (string, error) {
	if aw == nil {
		return "", fmt.Errorf("artifact writer is nil")
	}
	if record == nil {
		return "", fmt.Errorf("panic record is nil")
	}
	if aw.rootDir == "" {
		return "", fmt.Errorf("artifact root is empty")
	}

	if err := os.MkdirAll(aw.rootDir, 0o755); err != nil {
		return "", fmt.Errorf("create artifact root: %w", err)
	}

	recordedAt := record.OccurredAt
	if recordedAt.IsZero() {
		recordedAt = aw.nowFn().UTC()
		record.OccurredAt = recordedAt
	}

	artifactDir := filepath.Join(aw.rootDir, aw.dirName(record.Component, recordedAt))
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return "", fmt.Errorf("create artifact dir: %w", err)
	}

	if writeErr := aw.rewritePanicRecord(artifactDir, record); writeErr != nil {
		return "", fmt.Errorf("write panic record: %w", writeErr)
	}

	summaryPath := filepath.Join(artifactDir, crashTextFileName)
	summary := buildCrashSummary(record)
	if writeErr := os.WriteFile(summaryPath, []byte(summary), 0o600); writeErr != nil {
		return "", fmt.Errorf("write crash summary: %w", writeErr)
	}

	// Best-effort: prune old artifact directories to honor the disk quota.
	// Errors here are non-fatal; the caller already has the new artifact path.
	if maxArtifacts := DefaultMaxArtifacts(); maxArtifacts > 0 {
		_ = PruneArtifacts(aw.rootDir, maxArtifacts)
	}

	return artifactDir, nil
}

func (aw *ArtifactWriter) rewritePanicRecord(artifactDir string, record *PanicRecord) error {
	recordPath := filepath.Join(artifactDir, panicRecordFileName)
	recordData, marshalErr := json.MarshalIndent(record, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("marshal panic record: %w", marshalErr)
	}
	recordData = append(recordData, '\n')
	if writeErr := os.WriteFile(recordPath, recordData, 0o600); writeErr != nil {
		return fmt.Errorf("write panic record: %w", writeErr)
	}
	return nil
}

// WriteSpewDump persists a go-spew reflection dump into an existing artifact directory.
func (aw *ArtifactWriter) WriteSpewDump(artifactDir string, value any, limitBytes int64) (bool, string, error) {
	if aw == nil {
		return false, "", fmt.Errorf("artifact writer is nil")
	}
	if artifactDir == "" {
		return false, "", fmt.Errorf("artifact dir is empty")
	}
	dumpPath := filepath.Join(artifactDir, deepDumpSpewFileName)
	truncated, dumpErr := NewBoundedSpewWriter().WriteSpew(dumpPath, value, limitBytes)
	if dumpErr != nil {
		return truncated, "", dumpErr
	}
	return truncated, dumpPath, nil
}

// WriteStateDump persists a deep state dump into an existing artifact directory.
func (aw *ArtifactWriter) WriteStateDump(artifactDir string, value any, limitBytes int64) (bool, string, error) {
	if aw == nil {
		return false, "", fmt.Errorf("artifact writer is nil")
	}
	if artifactDir == "" {
		return false, "", fmt.Errorf("artifact dir is empty")
	}

	dumpPath := filepath.Join(artifactDir, deepDumpFileName)
	truncated, err := NewBoundedStateWriter().WriteJSON(dumpPath, value, limitBytes)
	if err != nil {
		return truncated, "", err
	}
	return truncated, dumpPath, nil
}

func (aw *ArtifactWriter) dirName(component string, occurredAt time.Time) string {
	sanitizedComponent := sanitizeComponent(component)
	return fmt.Sprintf("%s-%s-%d", occurredAt.UTC().Format("20060102T150405.000000000Z"), sanitizedComponent, aw.pidFn())
}

func sanitizeComponent(component string) string {
	if component == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		" ", "-",
		":", "-",
	)
	component = replacer.Replace(component)
	component = strings.Trim(component, "-")
	if component == "" {
		return "unknown"
	}
	return component
}

func buildCrashSummary(record *PanicRecord) string {
	var builder strings.Builder
	builder.WriteString("BanyanDB panic recovered\n")
	builder.WriteString(fmt.Sprintf("OccurredAt: %s\n", record.OccurredAt.UTC().Format(time.RFC3339Nano)))
	builder.WriteString(fmt.Sprintf("Component: %s\n", record.Component))
	builder.WriteString(fmt.Sprintf("Recovered: %t\n", record.Recovered))
	builder.WriteString(fmt.Sprintf("Panic: %s\n\n", record.PanicValue))
	builder.WriteString("Stack:\n")
	builder.WriteString(record.GoroutineStack)
	if !strings.HasSuffix(record.GoroutineStack, "\n") {
		builder.WriteByte('\n')
	}
	return builder.String()
}
