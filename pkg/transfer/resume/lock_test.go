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

package resume

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readLockFile decodes the holder a lock file names.
func readLockFile(t *testing.T, lockPath string) Holder {
	t.Helper()
	data, err := os.ReadFile(lockPath)
	require.NoError(t, err)
	var h Holder
	require.NoError(t, json.Unmarshal(data, &h), "%s", data)
	return h
}

func thisHost(t *testing.T) string {
	t.Helper()
	host, err := os.Hostname()
	require.NoError(t, err)
	return host
}

func TestLock_SecondHolderIsRefusedAndTheFirstKeepsItsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "p.json")
	unlock, err := Lock(path)
	require.NoError(t, err, "the directory is created")
	h := readLockFile(t, LockPath(path))
	assert.Equal(t, os.Getpid(), h.PID)
	assert.Equal(t, filepath.Base(os.Args[0]), h.Command, "the command names the program, no flag")

	_, err = Lock(path)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLocked), "%v", err)
	for _, w := range []string{
		"another bydbctl is using " + path, "lock file " + LockPath(path),
		fmt.Sprintf(", held by pid %d on %s since ", os.Getpid(), thisHost(t)),
	} {
		assert.Contains(t, err.Error(), w)
	}
	assert.NotContains(t, err.Error(), "Cannot lock file", "the file system's own error adds nothing to the holder")
	assert.Equal(t, os.Getpid(), readLockFile(t, LockPath(path)).PID, "a refused Lock leaves the holder's file and its content")

	require.NoError(t, unlock())
	require.NoError(t, unlock(), "unlock is safe to call again")
	_, statErr := os.Stat(LockPath(path))
	assert.True(t, os.IsNotExist(statErr), "unlock removes the lock file: %v", statErr)

	again, err := Lock(path)
	require.NoError(t, err, "a released lock can be taken again")
	require.NoError(t, again())
}

// A lock file that is a symlink is never followed: the file it points to is left as it is.
func TestLock_RefusesASymlinkedLockFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "important")
	require.NoError(t, os.WriteFile(target, []byte("keep me"), 0o600))
	path := filepath.Join(dir, "p.json")
	if err := os.Symlink(target, LockPath(path)); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	_, err := Lock(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), LockPath(path)+" is not a regular file")
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "keep me", string(data))
}

// TestLockHelperProcess is the other process of the tests below, not a test of its own: with
// RESUME_LOCK_HELPER=lock it takes the lock of RESUME_LOCK_PATH, says "locked" and holds it until
// its stdin closes.
func TestLockHelperProcess(_ *testing.T) {
	if os.Getenv("RESUME_LOCK_HELPER") != "lock" {
		return
	}
	unlock, err := Lock(os.Getenv("RESUME_LOCK_PATH"))
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	fmt.Println("locked")
	_, _ = io.Copy(io.Discard, os.Stdin)
	_ = unlock()
	os.Exit(0)
}

// startHolder runs a process that takes the lock of path and holds it.
func startHolder(t *testing.T, path string) (*exec.Cmd, io.WriteCloser) {
	t.Helper()
	child := exec.Command(os.Args[0], "-test.run=^TestLockHelperProcess$") //nolint:gosec // the test binary runs itself as the other process
	child.Env = append(os.Environ(), "RESUME_LOCK_HELPER=lock", "RESUME_LOCK_PATH="+path)
	stdin, err := child.StdinPipe()
	require.NoError(t, err)
	stdout, err := child.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, child.Start())
	// Ends the child on a failed assertion too; Kill and Wait are no-ops on one that ended.
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "locked\n", line)
	return child, stdin
}

func TestLock_AnotherProcess(t *testing.T) {
	t.Run("is refused while the holder runs and its file goes when it ends", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "p.json")
		child, stdin := startHolder(t, path)
		for range 2 {
			_, err := Lock(path)
			require.ErrorIs(t, err, ErrLocked)
			assert.Contains(t, err.Error(), "another bydbctl is using "+path)
			assert.Contains(t, err.Error(), "held by pid "+strconv.Itoa(child.Process.Pid)+" on ", "every refused run names the holder")
		}
		require.NoError(t, stdin.Close())
		require.NoError(t, child.Wait())
		_, err := os.Stat(LockPath(path))
		assert.True(t, os.IsNotExist(err), "the holder removed its lock file: %v", err)
		unlock, err := Lock(path)
		require.NoError(t, err)
		require.NoError(t, unlock())
	})
	t.Run("a killed holder's lock is free at once", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "p.json")
		child, stdin := startHolder(t, path)
		defer func() { _ = stdin.Close() }()
		require.NoError(t, child.Process.Kill())
		_ = child.Wait()
		_, err := os.Stat(LockPath(path))
		require.NoError(t, err, "a killed holder leaves its lock file")
		var warn bytes.Buffer
		unlock, err := LockWith(path, &warn)
		require.NoError(t, err)
		assert.Contains(t, warn.String(), "WARN taking over a stale lock of pid "+strconv.Itoa(child.Process.Pid)+" on ")
		assert.Equal(t, os.Getpid(), readLockFile(t, LockPath(path)).PID)
		require.NoError(t, unlock())
	})
}

// A lock file that cannot be locked for another reason than a holder (a read-only file system,
// an I/O error, no locks available) is no other bydbctl using the progress file.
func TestLock_AFailureOtherThanAHolderIsNotErrLocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	// A stale holder in the lock file must not be named as one using the file.
	host := thisHost(t)
	body, err := json.Marshal(Holder{PID: 999999999, Host: host, StartedAt: time.Now()})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(LockPath(path), body, 0o600))
	prev := openLock
	defer func() { openLock = prev }()
	openLock = func(string) (lockFile, error) { return nil, errors.New("read-only file system") }
	_, err = Lock(path)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrLocked)
	assert.Contains(t, err.Error(), "cannot be locked")
	assert.Contains(t, err.Error(), "read-only file system")
	assert.NotContains(t, err.Error(), "another bydbctl")
	assert.NotContains(t, err.Error(), "999999999")
}

// A lock file is emptied only once it is locked: a refused attempt leaves the holder's entry.
func TestLock_RefusedAttemptKeepsTheHolderEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	unlock, err := Lock(path)
	require.NoError(t, err)
	defer func() { _ = unlock() }()
	before, err := os.ReadFile(LockPath(path))
	require.NoError(t, err)
	for range 3 {
		_, err = Lock(path)
		require.ErrorIs(t, err, ErrLocked)
	}
	after, err := os.ReadFile(LockPath(path))
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after))
}

// A holder shown to the operator is in the time zone of the operator.
func TestHolder_StringIsInLocalTime(t *testing.T) {
	localZone(t, time.FixedZone("T", 9*3600))
	h := Holder{PID: 1, Host: "h", StartedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	assert.Equal(t, "pid 1 on h since 2026-01-02T12:04:05+09:00", h.String())
}
