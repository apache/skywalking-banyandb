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

package backup

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

func TestNewCreateCmd(t *testing.T) {
	baseDir := t.TempDir()
	streamDir := filepath.Join(baseDir, "stream")
	measureDir := filepath.Join(baseDir, "measure")
	propertyDir := filepath.Join(baseDir, "property")

	content := "2023-10-12"
	cmd := newCreateCmd()
	cmd.SetArgs([]string{
		content,
		"--stream-root", baseDir,
		"--measure-root", baseDir,
		"--property-root", baseDir,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("newCreateCmd.Execute() error: %v", err)
	}

	for _, dir := range []struct {
		name string
		root string
	}{
		{"stream", streamDir},
		{"measure", measureDir},
		{"property", propertyDir},
	} {
		fp := filepath.Join(dir.root, "time-dir")
		data, err := os.ReadFile(fp)
		if err != nil {
			t.Errorf("Catalog '%s': expected file at %s, error: %v", dir.name, fp, err)
			continue
		}
		if strings.TrimSpace(string(data)) != content {
			t.Errorf("Catalog '%s': expected content '%s', got '%s'", dir.name, content, string(data))
		}
	}
}

func TestNewReadCmd(t *testing.T) {
	baseDir := t.TempDir()
	streamDir := filepath.Join(baseDir, "stream")
	measureDir := filepath.Join(baseDir, "measure")
	propertyDir := filepath.Join(baseDir, "property")
	for _, dir := range []string{streamDir, measureDir, propertyDir} {
		if err := os.MkdirAll(dir, storage.DirPerm); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
		fp := filepath.Join(dir, "time-dir")
		if err := os.WriteFile(fp, []byte("dummy-time"), storage.FilePerm); err != nil {
			t.Fatalf("Failed to write file %s: %v", fp, err)
		}
	}

	cmd := newReadCmd()
	var outBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetArgs([]string{
		"--stream-root", baseDir,
		"--measure-root", baseDir,
		"--property-root", baseDir,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("newReadCmd.Execute() error: %v", err)
	}

	output := outBuf.String()
	for _, catalog := range []string{"stream", "measure", "property"} {
		if !strings.Contains(output, "Catalog '"+catalog+"'") {
			t.Errorf("Output missing expected catalog '%s'", catalog)
		}
	}
}

func TestNewDeleteCmd(t *testing.T) {
	baseDir := t.TempDir()
	streamDir := filepath.Join(baseDir, "stream")
	measureDir := filepath.Join(baseDir, "measure")
	propertyDir := filepath.Join(baseDir, "property")
	for _, dir := range []string{streamDir, measureDir, propertyDir} {
		if err := os.MkdirAll(dir, storage.DirPerm); err != nil {
			t.Fatalf("Failed to create dir %s: %v", dir, err)
		}
		fp := filepath.Join(dir, "time-dir")
		if err := os.WriteFile(fp, []byte("to-be-deleted"), 0o600); err != nil {
			t.Fatalf("Failed to write file %s: %v", fp, err)
		}
	}

	cmd := newDeleteCmd()
	cmd.SetArgs([]string{
		"--stream-root", baseDir,
		"--measure-root", baseDir,
		"--property-root", baseDir,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("newDeleteCmd.Execute() error: %v", err)
	}

	for _, dir := range []string{streamDir, measureDir, propertyDir} {
		catalog := filepath.Base(dir)
		fp := filepath.Join(dir, fmt.Sprintf("%s-time-dir", catalog))
		if _, err := os.Stat(fp); !os.IsNotExist(err) {
			t.Errorf("Expected file at %s to be deleted, but it exists", fp)
		}
	}
}
