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

//go:build !windows

package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDirSyncUnsupported(t *testing.T) {
	for _, errno := range []error{syscall.EINVAL, syscall.ENOTSUP, syscall.EOPNOTSUPP} {
		if !dirSyncUnsupported(fmt.Errorf("sync: %w", errno)) {
			t.Fatalf("%v must count as an unsupported directory fsync", errno)
		}
	}
	for _, err := range []error{nil, syscall.EIO, errors.New("other")} {
		if dirSyncUnsupported(err) {
			t.Fatalf("%v must not count as an unsupported directory fsync", err)
		}
	}
}

func TestSyncDirIfSupportedKeepsNotExist(t *testing.T) {
	if err := SyncDirIfSupported(t.TempDir()); err != nil {
		t.Fatalf("sync of an existing directory: %v", err)
	}
	err := SyncDirIfSupported(filepath.Join(t.TempDir(), "missing"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a missing directory must match os.ErrNotExist, got %v", err)
	}
}

func TestCreateLockFilePreserveUntilLocked(t *testing.T) {
	lfs := NewLocalFileSystem()
	path := filepath.Join(t.TempDir(), "lock")
	if err := os.WriteFile(path, []byte("holder"), 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := lfs.CreateLockFile(path, 0o600, PreserveUntilLocked())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	if data, readErr := os.ReadFile(path); readErr != nil || len(data) != 0 {
		t.Fatalf("a lock taken must empty the file, got %q, %v", data, readErr)
	}
	if _, err = held.Write([]byte("mine")); err != nil {
		t.Fatal(err)
	}
	if _, err = lfs.CreateLockFile(path, 0o600, PreserveUntilLocked()); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("a lock another open file holds must match ErrLockHeld, got %v", err)
	}
	if data, readErr := os.ReadFile(path); readErr != nil || string(data) != "mine" {
		t.Fatalf("a refused lock must keep the holder's content, got %q, %v", data, readErr)
	}
}

func TestCreateLockFilePreserveUntilLockedRefusesNonRegular(t *testing.T) {
	lfs := NewLocalFileSystem()
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	if _, err := lfs.CreateLockFile(link, 0o600, PreserveUntilLocked()); err == nil || errors.Is(err, ErrLockHeld) {
		t.Fatalf("a symlink must be refused, got %v", err)
	}
	if data, _ := os.ReadFile(target); string(data) != "keep" {
		t.Fatalf("the symlink's target must be untouched, got %q", data)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("fifos are not available: %v", err)
	}
	if _, err := lfs.CreateLockFile(fifo, 0o600, PreserveUntilLocked()); err == nil || errors.Is(err, ErrLockHeld) {
		t.Fatalf("a FIFO must be refused, got %v", err)
	}
}
