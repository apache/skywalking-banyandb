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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// ErrLocked is what LockWith returns, wrapped, when another process holds the lock.
var ErrLocked = errors.New("locked by another process")

// lockFile is a locked lock file: closing it releases the lock.
type lockFile interface {
	io.Writer
	io.Closer
}

// openLock locks the lock file at a path; tests replace it.
var openLock = openLockFile

// lockRetryWait is the pause before Lock looks at a lock file that changed while it was locked.
var lockRetryWait = 10 * time.Millisecond

const (
	// lockAttempts bounds how often Lock looks again at a lock file that keeps being replaced.
	lockAttempts = 20
	// maxHolderBytes bounds what is read of a lock file.
	maxHolderBytes = 4096
)

// LockPath is the lock file of the progress file at path.
func LockPath(path string) string { return path + ".lock" }

// Holder is who holds a lock: what the holder writes into the lock file.
type Holder struct {
	StartedAt time.Time `json:"startedAt"`
	Host      string    `json:"host"`
	Command   string    `json:"command"`
	PID       int       `json:"pid"`
}

// String is "pid <n> on <host> since <time> (<command>)".
func (h Holder) String() string {
	s := fmt.Sprintf("pid %d on %s since %s", h.PID, h.Host, h.StartedAt.Local().Format(time.RFC3339))
	if h.Command != "" {
		s += " (" + h.Command + ")"
	}
	return s
}

// Lock is LockWith(path, os.Stderr).
func Lock(path string) (unlock func() error, err error) {
	return LockWith(path, os.Stderr)
}

// LockWith makes this process the only one working on the progress file at path, creating the
// directory of LockPath(path) as needed. It takes the file lock on the lock
// file (flock, LockFileEx on Windows) and writes who holds it (Holder) into the file. The kernel
// releases the lock when its holder ends, however it ends; a lock file a killed run left behind
// is locked again at once, with a WARN on warn naming the run that left it.
//
// A lock another process holds is refused: the error wraps ErrLocked, says "another bydbctl is
// using <path>" and names the lock file and its holder; the caller decides the exit code.
//
// unlock removes the lock file, if it is still this process's, and releases it; it is safe to
// call more than once.
func LockWith(path string, warn io.Writer) (unlock func() error, err error) {
	if warn == nil {
		warn = io.Discard
	}
	lockPath := LockPath(path)
	if err = MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return nil, fmt.Errorf("progress file %s cannot be locked: %w", path, err)
	}
	for attempt := 0; attempt < lockAttempts; attempt++ {
		f, held, prev, lockErr := acquire(path, lockPath)
		if lockErr != nil {
			return nil, lockErr
		}
		if f == nil {
			time.Sleep(lockRetryWait)
			continue
		}
		if prev != nil {
			warnStaleLock(warn, prev, lockPath)
		}
		unlock = releaser(f, lockPath, held)
		if err = writeHolder(f, self()); err != nil {
			_ = unlock()
			return nil, fmt.Errorf("progress file %s cannot be locked: write %s: %w", path, lockPath, err)
		}
		return unlock, nil
	}
	return nil, fmt.Errorf("another bydbctl is using %s (%w; lock file %s keeps changing): wait until it ends, or stop it", path, ErrLocked, lockPath)
}

// acquire makes one attempt at the lock. It returns the locked file, what it is, and the holder
// a run that is gone left in it, or no file and no error when the lock file was replaced while
// it was being locked and is to be looked at again.
func acquire(path, lockPath string) (lockFile, os.FileInfo, *Holder, error) {
	before, err := lockFileInfo(path, lockPath)
	if before == nil {
		return nil, nil, nil, err
	}
	// Read who holds the file first: a lock taken empties it, and on Windows the file system
	// empties it before it tries the lock.
	data, prev := readHolder(lockPath)
	f, err := openLock(lockPath)
	if err != nil {
		// A refused lock may have emptied the file (Windows).
		restoreHolder(lockPath, before, data)
		if errors.Is(err, fs.ErrLockHeld) {
			return nil, nil, nil, refused(path, lockPath, prev)
		}
		// Not a lock another process holds (a read-only file system, an I/O error, no locks
		// available): saying another bydbctl uses the file would send the operator after none.
		return nil, nil, nil, fmt.Errorf("progress file %s cannot be locked: %w", path, err)
	}
	// The holder removes the lock file while it holds the lock: a file opened before that is no
	// longer at the path, and its lock excludes nobody.
	after, err := os.Lstat(lockPath)
	if err != nil || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, nil, nil, nil
	}
	return f, after, prev, nil
}

// lockFileInfo creates the lock file when there is none and describes it, or returns nothing
// and no error when it went away again. It refuses a lock file that is not a regular file:
// opening it must not follow a symlink and empty its target.
func lockFileInfo(path, lockPath string) (os.FileInfo, error) {
	info, err := os.Lstat(lockPath)
	if errors.Is(err, os.ErrNotExist) {
		f, createErr := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil && !errors.Is(createErr, os.ErrExist) {
			return nil, fmt.Errorf("progress file %s cannot be locked: %w", path, createErr)
		}
		if createErr == nil {
			_ = f.Close()
		}
		if info, err = os.Lstat(lockPath); errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("progress file %s cannot be locked: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("progress file %s cannot be locked: %s is not a regular file (%s); remove it if no bydbctl uses %s",
			path, lockPath, info.Mode().Type(), path)
	}
	return info, nil
}

// refused is the error of a lock another process holds.
func refused(path, lockPath string, holder *Holder) error {
	held := ""
	if holder != nil {
		held = ", held by " + holder.String()
	}
	return fmt.Errorf("another bydbctl is using %s (%w; lock file %s%s): wait until it ends, or stop it", path, ErrLocked, lockPath, held)
}

// releaser is the unlock of the lock file f, which is held at lockPath.
func releaser(f lockFile, lockPath string, held os.FileInfo) func() error {
	var once sync.Once
	var err error
	return func() error {
		once.Do(func() {
			if fs.OpenFileNamesMutable {
				// Removed while still locked, so no process locks it in between.
				err = removeIfSame(lockPath, held)
				if closeErr := f.Close(); err == nil {
					err = closeErr
				}
				return
			}
			// An open file cannot be removed on Windows; a process that opened it meanwhile keeps it.
			err = f.Close()
			_ = removeIfSame(lockPath, held)
		})
		return err
	}
}

// removeIfSame removes lockPath when it is still the file held: a lock file deleted meanwhile
// and created anew is another process's and stays.
func removeIfSame(lockPath string, held os.FileInfo) error {
	cur, err := os.Lstat(lockPath)
	if err != nil || !os.SameFile(held, cur) {
		return nil
	}
	if err = os.Remove(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// self is the Holder this process writes.
func self() Holder {
	host, _ := os.Hostname()
	return Holder{PID: os.Getpid(), Host: host, StartedAt: time.Now().Truncate(time.Second), Command: commandLine()}
}

// commandLine is the program name and the subcommand words of this process, up to its first
// flag: flags may carry credentials, which a lock file other users can read the name of must not.
func commandLine() string {
	if len(os.Args) == 0 {
		return ""
	}
	words := []string{filepath.Base(os.Args[0])}
	for _, a := range os.Args[1:] {
		if strings.HasPrefix(a, "-") || len(words) >= 4 {
			break
		}
		words = append(words, a)
	}
	return strings.Join(words, " ")
}

// writeHolder writes h into the lock file f, just truncated, as one line of JSON after a newline:
// LockFileEx locks the first byte, and Windows locks keep other processes from reading it.
func writeHolder(f io.Writer, h Holder) error {
	body, err := json.Marshal(h)
	if err != nil {
		return err
	}
	_, err = f.Write(append(append([]byte{'\n'}, body...), '\n'))
	return err
}

// readHolder reads the lock file at lockPath: its bytes and the holder they name, none when they
// name none (empty, partly written or not a lock file of this bydbctl).
func readHolder(lockPath string) ([]byte, *Holder) {
	f, err := os.Open(lockPath)
	if err != nil {
		return nil, nil
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, maxHolderBytes)
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		// The first byte is locked: read past it.
		n, err = f.ReadAt(buf[1:], 1)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil
		}
		buf[0] = '\n'
		n++
	}
	data := buf[:n]
	var h Holder
	if json.Unmarshal(bytes.TrimSpace(data), &h) != nil || h.PID <= 0 || h.Host == "" {
		return data, nil
	}
	return data, &h
}

// restoreHolder writes data back into the lock file a refused CreateLockFile truncated, when it
// is still the file read and still empty, so that the next process to try it learns its holder.
func restoreHolder(lockPath string, before os.FileInfo, data []byte) {
	if len(data) < 2 {
		return
	}
	f, err := os.OpenFile(lockPath, os.O_WRONLY, 0)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !os.SameFile(before, info) || info.Size() != 0 {
		return
	}
	if _, err = f.WriteAt(data, 0); err != nil {
		_, _ = f.WriteAt(data[1:], 1)
	}
}
