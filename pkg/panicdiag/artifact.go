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

const panicJSONFileName = "panic.json"

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

// ArtifactDirPath returns the deterministic artifact directory path for the
// given record without performing any I/O. The recovery defer uses this to
// populate RecoveryResult.ArtifactDir before the synchronous mkdir runs (or
// before the sink worker creates the directory asynchronously), so observers
// see a path even when the actual files have not been written yet.
func (aw *ArtifactWriter) ArtifactDirPath(record *PanicRecord) string {
	if aw == nil || record == nil || aw.rootDir == "" {
		return ""
	}
	occurredAt := record.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = aw.nowFn().UTC()
	}
	return filepath.Join(aw.rootDir, aw.dirName(record.Component, occurredAt))
}

// MkdirArtifact creates the artifact directory for the given record and returns
// its path. OccurredAt is back-filled when zero. Call WriteRecord once all
// fields (including StateDump) have been populated.
func (aw *ArtifactWriter) MkdirArtifact(record *PanicRecord) (string, error) {
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
	if record.OccurredAt.IsZero() {
		record.OccurredAt = aw.nowFn().UTC()
	}
	artifactDir := filepath.Join(aw.rootDir, aw.dirName(record.Component, record.OccurredAt))
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return "", fmt.Errorf("create artifact dir: %w", err)
	}
	return artifactDir, nil
}

// WriteRecord serializes record as panic.json inside artifactDir.
func (aw *ArtifactWriter) WriteRecord(artifactDir string, record *PanicRecord) error {
	if aw == nil {
		return fmt.Errorf("artifact writer is nil")
	}
	summaryData, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		return fmt.Errorf("marshal crash record: %w", marshalErr)
	}
	if writeErr := os.WriteFile(filepath.Join(artifactDir, panicJSONFileName), summaryData, 0o600); writeErr != nil {
		return fmt.Errorf("write panic json: %w", writeErr)
	}
	return nil
}

// Write persists the given panic record and returns the artifact directory.
// Use MkdirArtifact + WriteRecord directly when additional fields (e.g.
// StateDump) must be populated before the file is written.
func (aw *ArtifactWriter) Write(record *PanicRecord) (string, error) {
	artifactDir, err := aw.MkdirArtifact(record)
	if err != nil {
		return "", err
	}
	if writeErr := aw.WriteRecord(artifactDir, record); writeErr != nil {
		return "", writeErr
	}
	aw.pruneArtifacts()
	return artifactDir, nil
}

func (aw *ArtifactWriter) pruneArtifacts() {
	if maxArtifacts := DefaultMaxArtifacts(); maxArtifacts > 0 {
		_ = PruneArtifacts(aw.rootDir, maxArtifacts)
	}
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
