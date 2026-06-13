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
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// NoFsyncFS wraps a local FileSystem and skips per-file fsync /
// directory sync calls. Migration is a one-shot bulk import: a crash
// mid-run leaves the target root in an unusable state anyway and the
// operator re-runs after cleaning the target. Per-part fsyncs were the
// dominant wall-time cost (60%+ of CPU samples in syscall.syscall on
// open/write/fsync) precisely because each chunked target part fans
// out 7-10 small files. Skipping fsync trades zero-crash-safety for
// throughput, which is the right trade-off here.
type NoFsyncFS struct {
	fs.FileSystem
}

// Write writes buffer to name without fsyncing the file.
func (f NoFsyncFS) Write(buffer []byte, name string, permission fs.Mode) (int, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", name, err)
	}
	n, err := file.Write(buffer)
	if cerr := file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	// Return n even on a close error: the bytes reached the kernel buffer and
	// callers use n for byte accounting.
	return n, err
}

// WriteAtomic degrades to a plain (non-atomic, non-synced) write.
func (f NoFsyncFS) WriteAtomic(buffer []byte, name string, permission fs.Mode) (int, error) {
	return f.Write(buffer, name, permission)
}

// SyncPath is a no-op.
func (NoFsyncFS) SyncPath(string) {}

// CopyDir recursively byte-copies the src directory into dst and returns the
// bytes copied. The bluge exclusive-lock file is never copied: a stale lock
// traveling with an index dir would block the target from opening it.
// Byte copy is deliberate — a hard-link fast path always tripped cross-FS
// (staging on emptyDir → target on PVC) and was pure overhead. Symlinks are
// not handled: banyandb's segment/shard trees never contain them.
func CopyDir(src, dst string) (int64, error) {
	var total int64
	if err := os.MkdirAll(dst, storage.DirPerm); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if !e.IsDir() && e.Name() == inverted.LockFilename {
			continue
		}
		srcPath := filepath.Join(src, e.Name())
		dstPath := filepath.Join(dst, e.Name())
		if e.IsDir() {
			sub, recErr := CopyDir(srcPath, dstPath)
			if recErr != nil {
				return total, recErr
			}
			total += sub
			continue
		}
		n, copyErr := copyFile(srcPath, dstPath)
		if copyErr != nil {
			return total, copyErr
		}
		total += n
	}
	return total, nil
}

func copyFile(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, storage.FilePerm)
	if err != nil {
		return 0, err
	}
	// No out.Sync(): consistent with NoFsyncFS — migration is a one-shot bulk
	// import and the operator re-runs with --clean on crash.
	n, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return n, copyErr
	}
	return n, closeErr
}
