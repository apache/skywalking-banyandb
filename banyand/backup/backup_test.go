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
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
)

func TestNewFS(t *testing.T) {
	tests := []struct {
		setup   func(cfg *config.FsConfig)
		name    string
		dest    string
		wantErr bool
	}{
		{
			name:    "valid file scheme",
			dest:    "file:///tmp",
			setup:   nil,
			wantErr: false,
		},
		{
			name:    "malformed URL",
			dest:    ":invalid",
			setup:   nil,
			wantErr: true,
		},
		{
			name: "valid s3 scheme",
			dest: "s3://my-bucket/backup-prefix",
			setup: func(cfg *config.FsConfig) {
				cfg.S3 = &config.S3Config{}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := new(config.FsConfig)
			if tt.setup != nil {
				tt.setup(cfg)
			}
			_, err := newFS(tt.dest, cfg)
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
		traceRoot   string
		schemaRoot  string
		want        string
		wantErr     bool
	}{
		{
			"stream catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: "test"},
			"/tmp", "/tmp", "/tmp", "/tmp", "/tmp",
			filepath.Join("/tmp/stream", storage.SnapshotsDir, "test"),
			false,
		},
		{
			"trace catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_TRACE, Name: "test"},
			"/tmp", "/tmp", "/tmp", "/tmp", "/tmp",
			filepath.Join("/tmp/trace", storage.SnapshotsDir, "test"),
			false,
		},
		{
			"property catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Name: "test"},
			"/tmp", "/tmp", "/tmp", "/tmp", "/tmp",
			filepath.Join("/tmp/property", storage.SnapshotsDir, "test", storage.DataDir),
			false,
		},
		{
			"schema-property catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Name: "schema-property/test"},
			"/tmp", "/tmp", "/tmp", "/tmp", "/tmp",
			filepath.Join("/tmp/schema-property", storage.SnapshotsDir, "test", storage.DataDir),
			false,
		},
		{
			"unknown catalog",
			&databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_UNSPECIFIED, Name: "test"},
			"", "", "", "", "",
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := snapshot.Dir(tt.snapshot, tt.streamRoot, tt.measureRoot, tt.propRoot, tt.traceRoot, tt.schemaRoot)
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
	uploadErrOn string
	uploaded    []string
	deleted     []string
	mu          sync.Mutex
}

func (m *mockFS) List(_ context.Context, prefix string) ([]string, error) {
	return []string{path.Join(prefix, "existing.txt")}, nil // Simulate existing remote file.
}

func (m *mockFS) Upload(_ context.Context, p string, _ io.Reader) error {
	if m.uploadErrOn != "" && strings.Contains(p, m.uploadErrOn) {
		return fmt.Errorf("mock upload failure for %s", p)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploaded = append(m.uploaded, p)
	return nil
}

func (m *mockFS) Delete(_ context.Context, p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, p)
	return nil
}

func (m *mockFS) Download(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }

func (m *mockFS) Close() error { return nil }

func TestBackupSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "newfile.txt"), nil, 0o600)

	m := &mockFS{}
	err := backupSnapshot(context.Background(), m, tmpDir, "test-snapshot", "daily", 4)
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

// TestBackupSnapshotConcurrent exercises the concurrent small-file path, the
// sequential large-file path (>= smallFileThreshold), and orphan deletion all
// at once. Run with -race to catch data races in the upload fan-out.
func TestBackupSnapshotConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	const numSmall = 50
	for i := 0; i < numSmall; i++ {
		sub := filepath.Join(tmpDir, fmt.Sprintf("seg-%d", i%5))
		if err := os.MkdirAll(sub, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, fmt.Sprintf("f-%d.tm", i)), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A file at exactly smallFileThreshold takes the sequential branch (size is
	// not strictly less than the threshold).
	if err := os.WriteFile(filepath.Join(tmpDir, "big.bin"), make([]byte, smallFileThreshold), 0o600); err != nil {
		t.Fatal(err)
	}

	m := &mockFS{}
	if err := backupSnapshot(context.Background(), m, tmpDir, "test-snapshot", "daily", 8); err != nil {
		t.Fatal(err)
	}

	if len(m.uploaded) != numSmall+1 {
		t.Fatalf("uploaded %d files, want %d", len(m.uploaded), numSmall+1)
	}
	uploaded := make(map[string]struct{}, len(m.uploaded))
	for _, p := range m.uploaded {
		uploaded[p] = struct{}{}
	}
	if _, ok := uploaded["daily/test-snapshot/big.bin"]; !ok {
		t.Errorf("large file not uploaded; uploaded=%v", m.uploaded)
	}
	wantDelete := "daily/test-snapshot/existing.txt"
	if len(m.deleted) != 1 || m.deleted[0] != wantDelete {
		t.Errorf("deleted = %v, want [%s]", m.deleted, wantDelete)
	}
}

// TestBackupSnapshotUploadError verifies that a failed upload surfaces an error
// and that orphaned remote files are NOT deleted when the backup did not fully
// succeed.
func TestBackupSnapshotUploadError(t *testing.T) {
	tmpDir := t.TempDir()
	for _, name := range []string{"a.tm", "b.tm", "boom.tm"} {
		if err := os.WriteFile(filepath.Join(tmpDir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	m := &mockFS{uploadErrOn: "boom.tm"}
	err := backupSnapshot(context.Background(), m, tmpDir, "test-snapshot", "daily", 4)
	if err == nil {
		t.Fatal("expected an error when an upload fails, got nil")
	}
	if len(m.deleted) != 0 {
		t.Errorf("orphans must not be deleted on a failed backup, deleted = %v", m.deleted)
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

type fakeSnapshotServer struct {
	databasev1.UnimplementedSnapshotServiceServer
	snapshots []*databasev1.Snapshot
}

func (f *fakeSnapshotServer) Snapshot(context.Context, *databasev1.SnapshotRequest) (*databasev1.SnapshotResponse, error) {
	return &databasev1.SnapshotResponse{Snapshots: f.snapshots}, nil
}

// TestBackupActionCatalogResults runs the full backup flow (snapshot RPC, local
// snapshot directories, file:// destination) and verifies that a catalog upload
// failure is reported regardless of the catalogs backed up after it, while a
// snapshot whose directory cannot be resolved is skipped without failing the run.
func TestBackupActionCatalogResults(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("unreadable files cannot be simulated when running as root")
	}
	const snapshotName = "snp"
	type catalogData struct {
		catalog    commonv1.Catalog
		unreadable bool
	}
	tests := []struct {
		name         string
		catalogs     []catalogData
		wantErrs     []string
		wantBacked   []string
		extraUnknown bool
	}{
		{
			name: "earlier catalog failure is not reset by a later success",
			catalogs: []catalogData{
				{catalog: commonv1.Catalog_CATALOG_STREAM, unreadable: true},
				{catalog: commonv1.Catalog_CATALOG_MEASURE},
			},
			wantErrs:   []string{"stream"},
			wantBacked: []string{"measure"},
		},
		{
			name: "failures of several catalogs are all reported",
			catalogs: []catalogData{
				{catalog: commonv1.Catalog_CATALOG_STREAM, unreadable: true},
				{catalog: commonv1.Catalog_CATALOG_MEASURE, unreadable: true},
				{catalog: commonv1.Catalog_CATALOG_TRACE},
			},
			wantErrs:   []string{"stream", "measure"},
			wantBacked: []string{"trace"},
		},
		{
			name: "failure is reported when an unresolvable snapshot follows it",
			catalogs: []catalogData{
				{catalog: commonv1.Catalog_CATALOG_STREAM, unreadable: true},
				{catalog: commonv1.Catalog_CATALOG_MEASURE},
			},
			extraUnknown: true,
			wantErrs:     []string{"stream"},
			wantBacked:   []string{"measure"},
		},
		{
			name: "unresolvable last snapshot does not fail a successful run",
			catalogs: []catalogData{
				{catalog: commonv1.Catalog_CATALOG_STREAM},
				{catalog: commonv1.Catalog_CATALOG_MEASURE},
			},
			extraUnknown: true,
			wantBacked:   []string{"stream", "measure"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataRoot := t.TempDir()
			destRoot := t.TempDir()
			server := &fakeSnapshotServer{}
			for _, c := range tt.catalogs {
				catalogName := snapshot.CatalogName(c.catalog)
				dir := filepath.Join(snapshot.LocalDir(dataRoot, c.catalog), storage.SnapshotsDir, snapshotName)
				if err := os.MkdirAll(dir, 0o750); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(dir, catalogName+".tm")
				if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
				if c.unreadable {
					if err := os.Chmod(file, 0o000); err != nil {
						t.Fatal(err)
					}
				}
				server.snapshots = append(server.snapshots, &databasev1.Snapshot{Catalog: c.catalog, Name: snapshotName})
			}
			if tt.extraUnknown {
				server.snapshots = append(server.snapshots, &databasev1.Snapshot{Catalog: commonv1.Catalog_CATALOG_UNSPECIFIED, Name: snapshotName})
			}
			addr := startSnapshotServer(t, server)

			err := backupAction(context.Background(), backupOptions{
				gRPCAddr:          addr,
				dest:              "file://" + destRoot,
				timeStyle:         "daily",
				streamRoot:        dataRoot,
				measureRoot:       dataRoot,
				propertyRoot:      dataRoot,
				traceRoot:         dataRoot,
				schemaRoot:        dataRoot,
				uploadConcurrency: 2,
			})

			if len(tt.wantErrs) == 0 {
				if err != nil {
					t.Fatalf("backupAction() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("backupAction() error = nil, want failures of %v", tt.wantErrs)
				}
				for _, catalogName := range tt.wantErrs {
					if !strings.Contains(err.Error(), catalogName+".tm") {
						t.Errorf("backupAction() error = %v, want it to report the %s catalog", err, catalogName)
					}
				}
			}
			backed := backedUpCatalogs(t, destRoot)
			for _, catalogName := range tt.wantBacked {
				if _, ok := backed[catalogName]; !ok {
					t.Errorf("catalog %s not backed up; backed up files = %v", catalogName, backed)
				}
			}
			if len(backed) != len(tt.wantBacked) {
				t.Errorf("backed up catalogs = %v, want %v", backed, tt.wantBacked)
			}
		})
	}
}

func startSnapshotServer(t *testing.T, server databasev1.SnapshotServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	databasev1.RegisterSnapshotServiceServer(s, server)
	go func() {
		_ = s.Serve(lis)
	}()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// backedUpCatalogs maps each catalog found under the destination's <timeDir>/<catalog>/ to its uploaded files.
func backedUpCatalogs(t *testing.T, destRoot string) map[string][]string {
	t.Helper()
	files, err := getAllFiles(destRoot)
	if err != nil {
		t.Fatal(err)
	}
	backed := make(map[string][]string)
	for _, f := range files {
		parts := strings.SplitN(f, "/", 3)
		if len(parts) != 3 {
			t.Fatalf("unexpected remote file layout: %s", f)
		}
		backed[parts[1]] = append(backed[parts[1]], parts[2])
	}
	return backed
}
