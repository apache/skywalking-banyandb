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

package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// TestCopyDir_SkipsLockFile asserts the byte-copy never carries the bluge
// exclusive-lock file into the target: a stale lock would block the target
// server from opening the copied index.
func TestCopyDir_SkipsLockFile(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "copy")
	require.NoError(t, os.WriteFile(filepath.Join(src, "seg.dat"), []byte("data"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(src, inverted.LockFilename), []byte("12345"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(src, "nested"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(src, "nested", inverted.LockFilename), []byte("12345"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(src, "nested", "other.dat"), []byte("more"), 0o600))

	n, err := CopyDir(src, dst)
	require.NoError(t, err)
	require.EqualValues(t, 8, n, "total bytes must cover only the two data files")

	gotSeg, err := os.ReadFile(filepath.Join(dst, "seg.dat"))
	require.NoError(t, err)
	require.Equal(t, []byte("data"), gotSeg)
	gotOther, err := os.ReadFile(filepath.Join(dst, "nested", "other.dat"))
	require.NoError(t, err)
	require.Equal(t, []byte("more"), gotOther)
	require.NoFileExists(t, filepath.Join(dst, inverted.LockFilename))
	require.NoFileExists(t, filepath.Join(dst, "nested", inverted.LockFilename))
}

// TestCopyDir_EmptySource asserts copying an empty directory creates the
// destination and reports zero bytes.
func TestCopyDir_EmptySource(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "copy")

	n, err := CopyDir(src, dst)
	require.NoError(t, err)
	require.Zero(t, n)
	require.DirExists(t, dst)
}

// TestCopyDir_MissingSource asserts a non-existent source surfaces an error.
func TestCopyDir_MissingSource(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "copy")

	_, err := CopyDir(filepath.Join(t.TempDir(), "does-not-exist"), dst)
	require.Error(t, err)
}

// TestNoFsyncFS_WriteAtomic_NoTmpLeftover asserts WriteAtomic produces the exact
// content and leaves no ".tmp" sibling behind on success.
func TestNoFsyncFS_WriteAtomic_NoTmpLeftover(t *testing.T) {
	dir := t.TempDir()
	nofs := NoFsyncFS{FileSystem: fs.NewLocalFileSystem()}
	name := filepath.Join(dir, "manifest.json")

	payload := []byte(`["0000000000000001","0000000000000002"]`)
	n, err := nofs.WriteAtomic(payload, name, storage.FilePerm)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	got, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoFileExists(t, name+".tmp", "WriteAtomic must not leave a .tmp sibling on success")
}

// TestNoFsyncFS_WriteAtomic_ConcurrentReadsNeverSeePartial locks the atomicity
// of NoFsyncFS.WriteAtomic: a reader parsing the manifest concurrently with
// repeated rewrites must never observe a truncated file. Before the fix,
// WriteAtomic did a plain O_TRUNC write to the final path, so a concurrent
// ReadSnapshotPartNames caught the truncation window and failed with
// "unexpected end of JSON" — the root cause of the flaky migration snapshot
// tests. Run with -race for maximum sensitivity.
func TestNoFsyncFS_WriteAtomic_ConcurrentReadsNeverSeePartial(t *testing.T) {
	dir := t.TempDir()
	lfs := fs.NewLocalFileSystem()
	nofs := NoFsyncFS{FileSystem: lfs}
	snpPath := filepath.Join(dir, "0000000000000001.snp")

	// Seed so the manifest always exists before readers start; rename then keeps
	// it continuously present, so a reader never sees ENOENT, only (with the bug)
	// a partial file.
	seed, err := json.Marshal([]string{"0000000000000001"})
	require.NoError(t, err)
	_, err = nofs.WriteAtomic(seed, snpPath, storage.FilePerm)
	require.NoError(t, err)

	const writes = 3000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 8)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < writes; i++ {
			// Vary the payload length so the truncation window a non-atomic write
			// would expose differs each iteration.
			names := make([]string, 1+i%17)
			for j := range names {
				names[j] = fmt.Sprintf("%016x", i*100+j)
			}
			data, mErr := json.Marshal(names)
			if mErr != nil {
				errCh <- mErr
				return
			}
			if _, wErr := nofs.WriteAtomic(data, snpPath, storage.FilePerm); wErr != nil {
				errCh <- wErr
				return
			}
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, rErr := storage.ReadSnapshotPartNames(lfs, snpPath); rErr != nil {
					select {
					case errCh <- rErr:
					default:
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("WriteAtomic exposed a partial snapshot to a concurrent reader: %v", e)
	}
}
