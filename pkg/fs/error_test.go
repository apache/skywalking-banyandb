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

package fs

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"path/filepath"
	"testing"
)

func TestFileSystemErrorIsNotExist(t *testing.T) {
	lfs := NewLocalFileSystem()
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	_, readErr := lfs.Read(missing)
	if readErr == nil {
		t.Fatalf("expected error reading missing file, got nil")
	}
	if !errors.Is(readErr, iofs.ErrNotExist) {
		t.Fatalf("expected errors.Is(err, iofs.ErrNotExist) to be true for %T: %v", readErr, readErr)
	}
}

func TestFileSystemErrorIsMatrix(t *testing.T) {
	tests := []struct {
		target error
		name   string
		code   int
		want   bool
	}{
		{name: "not-exist-matches", code: IsNotExistError, target: iofs.ErrNotExist, want: true},
		{name: "exist-matches", code: isExistError, target: iofs.ErrExist, want: true},
		{name: "permission-matches", code: permissionError, target: iofs.ErrPermission, want: true},
		{name: "not-exist-vs-exist", code: IsNotExistError, target: iofs.ErrExist, want: false},
		{name: "read-vs-not-exist", code: readError, target: iofs.ErrNotExist, want: false},
		{name: "other-vs-permission", code: otherError, target: iofs.ErrPermission, want: false},
		{name: "wrapped-not-exist-does-not-match", code: IsNotExistError, target: fmt.Errorf("wrapped: %w", iofs.ErrNotExist), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := &FileSystemError{Code: tc.code, Message: "test"}
			if got := errors.Is(err, tc.target); got != tc.want {
				t.Fatalf("errors.Is(code=%d, %v) = %v, want %v", tc.code, tc.target, got, tc.want)
			}
		})
	}
}

func TestCreateLockFileHeldIsErrLockHeld(t *testing.T) {
	lfs := NewLocalFileSystem()
	path := filepath.Join(t.TempDir(), "lock")
	held, err := lfs.CreateLockFile(path, 0o600)
	if err != nil {
		t.Fatalf("lock %s: %v", path, err)
	}
	defer func() { _ = held.Close() }()
	_, err = lfs.CreateLockFile(path, 0o600)
	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("a lock another open file holds must match ErrLockHeld, got %v", err)
	}
}

func TestCreateLockFileOtherFailureIsNotErrLockHeld(t *testing.T) {
	lfs := NewLocalFileSystem()
	_, err := lfs.CreateLockFile(filepath.Join(t.TempDir(), "missing", "lock"), 0o600)
	if err == nil || errors.Is(err, ErrLockHeld) {
		t.Fatalf("a lock file that cannot be opened must not match ErrLockHeld, got %v", err)
	}
	for _, code := range []int{lockError, otherError} {
		if errors.Is(&FileSystemError{Code: code, Message: "test"}, ErrLockHeld) {
			t.Fatalf("code %d without a held lock must not match ErrLockHeld", code)
		}
	}
}
