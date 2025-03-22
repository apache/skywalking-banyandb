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
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

func TestNewFS(t *testing.T) {
	tests := []struct {
		name    string
		dest    string
		wantErr bool
	}{
		{"valid file scheme", "file:///tmp", false},
		{"s3 scheme", "s3:///bucket", false},
		{"malformed URL", ":invalid", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newFS(tt.dest)
			if (err != nil) != tt.wantErr {
				t.Errorf("newFS() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetSnapshotDir(t *testing.T) {
	tests := []struct {
		name        string
		snapshot    *databasev1.Snapshot
		streamRoot  string
		measureRoot string
		propRoot    string
		want        string
		wantErr     bool
	}{
		{
			"stream catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: "test"},
			"/tmp", "/tmp", "/tmp",
			filepath.Join("/tmp/stream", storage.SnapshotsDir, "test"),
			false,
		},
		{
			"unknown catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_UNSPECIFIED, Name: "test"},
			"", "", "",
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := snapshot.Dir(tt.snapshot, tt.streamRoot, tt.measureRoot, tt.propRoot)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSnapshotDir() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getSnapshotDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetTimeDir(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name  string
		style string
		want  string
	}{
		{"hourly", "hourly", now.Format("2006-01-02-15")},
		{"daily (default)", "invalid", now.Format("2006-01-02")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getTimeDir(tt.style)
			if got != tt.want {
				t.Errorf("getTimeDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetAllFiles(t *testing.T) {
	tmpDir := t.TempDir()
	// Create test files and subdirectory.
	os.WriteFile(filepath.Join(tmpDir, "file1"), nil, 0o600)
	os.Mkdir(filepath.Join(tmpDir, "sub"), 0o755)
	os.WriteFile(filepath.Join(tmpDir, "sub/file2"), nil, 0o600)

	want := []string{"file1", "sub/file2"}
	files, err := getAllFiles(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != len(want) {
		t.Fatalf("got %d files, want %d", len(files), len(want))
	}
	for i, f := range want {
		if files[i] != f {
			t.Errorf("file[%d] = %v, want %v", i, files[i], f)
		}
	}
}

type mockFS struct {
	uploaded []string
	deleted  []string
}

func (m *mockFS) List(_ context.Context, prefix string) ([]string, error) {
	return []string{path.Join(prefix, "existing.txt")}, nil // Simulate existing remote file.
}

func (m *mockFS) Upload(_ context.Context, p string, _ io.Reader) error {
	m.uploaded = append(m.uploaded, p)
	return nil
}

func (m *mockFS) Delete(_ context.Context, p string) error {
	m.deleted = append(m.deleted, p)
	return nil
}

func (m *mockFS) Download(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }

func (m *mockFS) Close() error { return nil }

func TestBackupSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "newfile.txt"), nil, 0o600)

	m := &mockFS{}
	err := backupSnapshot(m, tmpDir, "test-snapshot", "daily")
	if err != nil {
		t.Fatal(err)
	}

	wantUpload := "daily/test-snapshot/newfile.txt"
	if len(m.uploaded) != 1 || m.uploaded[0] != wantUpload {
		t.Errorf("uploaded = %v, want %v", m.uploaded, wantUpload)
	}

	wantDelete := "daily/test-snapshot/existing.txt"
	if len(m.deleted) != 1 || m.deleted[0] != wantDelete {
		t.Errorf("deleted = %v, want %v", m.deleted, wantDelete)
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		s     string
		slice []string
		want  bool
	}{
		{slice: []string{"a", "b"}, s: "a", want: true},
		{slice: []string{"a", "b"}, s: "c", want: false},
	}
	for _, tt := range tests {
		got := contains(tt.slice, tt.s)
		if got != tt.want {
			t.Errorf("contains(%v, %s) = %v, want %v", tt.slice, tt.s, got, tt.want)
		}
	}
}
