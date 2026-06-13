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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

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
