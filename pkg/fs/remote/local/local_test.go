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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
