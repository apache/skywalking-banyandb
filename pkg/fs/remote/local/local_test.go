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

package local

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFSOperationsStayWithinBase(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "remote")
	fs, err := NewFS(baseDir)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	const content = "hello"
	filePath := filepath.Join("snapshot", "data", "test.txt")
	if err = fs.Upload(context.Background(), filePath, strings.NewReader(content)); err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	files, err := fs.List(context.Background(), "snapshot")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(files) != 1 || files[0] != filepath.ToSlash(filePath) {
		t.Fatalf("files = %v, want [%s]", files, filepath.ToSlash(filePath))
	}

	reader, err := fs.Download(context.Background(), filePath)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	got, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		t.Fatalf("failed to read downloaded content: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("failed to close downloaded content: %v", closeErr)
	}
	if string(got) != content {
		t.Fatalf("content = %q, want %q", string(got), content)
	}

	if err = fs.Delete(context.Background(), filePath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(baseDir, filePath)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("deleted file exists or stat failed with unexpected error: %v", statErr)
	}
}

func TestFSListMissingPrefixReturnsEmpty(t *testing.T) {
	fs, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	files, err := fs.List(context.Background(), "missing")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("files = %v, want empty", files)
	}
}

func TestFSRejectsPathTraversal(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "remote")
	fs, err := NewFS(baseDir)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	escapedPath := filepath.Join("..", "escaped.txt")
	tests := []struct {
		run  func() error
		name string
	}{
		{
			name: "upload",
			run: func() error {
				return fs.Upload(context.Background(), escapedPath, strings.NewReader("escape"))
			},
		},
		{
			name: "download",
			run: func() error {
				reader, downloadErr := fs.Download(context.Background(), escapedPath)
				if reader != nil {
					reader.Close()
				}
				return downloadErr
			},
		},
		{
			name: "list",
			run: func() error {
				_, listErr := fs.List(context.Background(), escapedPath)
				return listErr
			},
		},
		{
			name: "delete",
			run: func() error {
				return fs.Delete(context.Background(), escapedPath)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.run(); err == nil {
				t.Fatal("expected path traversal to be rejected")
			}
			if _, statErr := os.Stat(filepath.Join(filepath.Dir(baseDir), "escaped.txt")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("escaped file exists or stat failed with unexpected error: %v", statErr)
			}
		})
	}
}

func TestFSRejectsAbsolutePath(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "remote")
	fs, err := NewFS(baseDir)
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	absolutePath := filepath.Join(baseDir, "escaped.txt")
	if err = fs.Upload(context.Background(), absolutePath, strings.NewReader("escape")); err == nil {
		t.Fatal("expected absolute path to be rejected")
	}
	if _, statErr := os.Stat(absolutePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("absolute path was written or stat failed with unexpected error: %v", statErr)
	}
}
