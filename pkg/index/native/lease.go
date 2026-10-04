// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// FileRootLease adapts the database-owned root lock to the neutral native
// owner capability. The caller retains ownership and closes the lock only
// after the native owner has closed.
type FileRootLease struct {
	lock   fs.File
	root   string
	closed atomic.Bool
}

// NewFileRootLease validates that lock is the root/lock capability protecting
// root and returns a lease suitable for OwnerOptions. No directory is created
// and the lock is never acquired by this constructor.
func NewFileRootLease(lock fs.File, root string) (*FileRootLease, error) {
	if lock == nil {
		return nil, ErrLeaseUnavailable
	}
	root = filepath.Clean(root)
	if root == "." || root == string(filepath.Separator) {
		return nil, fmt.Errorf("invalid native root lease path: %w", ErrLeaseUnavailable)
	}
	if filepath.Clean(lock.Path()) != filepath.Join(root, "lock") {
		return nil, fmt.Errorf("native root lock %q is outside root %q: %w", lock.Path(), root, ErrLeaseUnavailable)
	}
	if _, err := lock.Size(); err != nil {
		return nil, fmt.Errorf("validate native root lock: %w", err)
	}
	return &FileRootLease{lock: lock, root: root}, nil
}

// Validate verifies that the capability has not been revoked and that its
// lock pathname still exists. The constructor is intentionally restricted to
// storage-owned callers holding the lock; native code does not acquire or
// infer lock ownership from a pathname.
func (l *FileRootLease) Validate() error {
	if l == nil || l.closed.Load() || l.lock == nil {
		return ErrLeaseUnavailable
	}
	if _, err := l.lock.Size(); err != nil {
		return fmt.Errorf("validate native root lock: %w", err)
	}
	return nil
}

// Revoke invalidates this capability before its database-owned lock is
// released. The lock file may remain on disk, so checking its path alone is
// not sufficient to prove that this lease is still live.
func (l *FileRootLease) Revoke() error {
	if l == nil {
		return ErrLeaseUnavailable
	}
	l.closed.Store(true)
	return nil
}

// ValidatePath verifies that a native index directory remains inside the
// leased root. It does not create or scan the directory.
func (l *FileRootLease) ValidatePath(path string) error {
	if err := l.Validate(); err != nil {
		return err
	}
	relative, err := filepath.Rel(l.root, filepath.Clean(path))
	if err != nil || relative == "." || relative == ".." || len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator) {
		return fmt.Errorf("native index path %q is outside root %q: %w", path, l.root, ErrLeaseUnavailable)
	}
	return nil
}
