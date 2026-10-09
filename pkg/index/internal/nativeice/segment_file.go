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

package nativeice

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// fsSegmentFile serves a persisted segment from its file through pkg/fs,
// holding one descriptor for the reader's lifetime, like a measure part.
//
// Reads share mu; closing, and renaming where open files cannot be renamed,
// take it exclusively. So a read never runs on a closed descriptor: a read
// after Close reports ErrReaderClosed, and Close waits only for the reads
// already in flight. identity is the opened file's own description (fstat),
// against which garbage collection and a rename's reopen check that a path
// still names this very file.
type fsSegmentFile struct {
	file     fs.File
	identity os.FileInfo
	path     string
	size     int64
	mu       sync.RWMutex
	closed   bool
}

// openSegmentFile opens an immutable segment and returns its size. A segment
// a reader serves is read at scattered offsets for the reader's life, and a
// freshly written one is read back by the next query or merge, so serving
// asks the OS to keep the whole file in its page cache; a file opened only to
// be validated passes false. The advice is only a hint; a rejected one
// leaves the file readable. An open whose file cannot be described fails.
func openSegmentFile(path string, serving bool) (*fsSegmentFile, uint64, error) {
	file, openErr := segmentFileSystem.OpenFile(path)
	if openErr != nil {
		return nil, 0, openErr
	}
	identity, statErr := statSegmentFile(file)
	if statErr != nil {
		return nil, 0, errors.Join(fmt.Errorf("describe segment %q: %w", path, statErr), file.Close())
	}
	if serving {
		_ = fs.AdvisePageCache(file, fs.PageCacheWillNeed)
	}
	return &fsSegmentFile{file: file, identity: identity, path: path, size: identity.Size()}, uint64(identity.Size()), nil
}

// statSegmentFile describes an open segment file; a variable so tests can
// make it fail.
var statSegmentFile = fs.StatFile

func (f *fsSegmentFile) ReadAt(destination []byte, offset int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return 0, fmt.Errorf("segment %q: %w", f.path, ErrReaderClosed)
	}
	return f.file.Read(offset, destination)
}

// Close closes the descriptor once the reads in flight end; later reads
// report ErrReaderClosed.
func (f *fsSegmentFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	return f.file.Close()
}

// sameAs reports, as corruption, unless entry -- an Lstat of a path --
// describes the file the descriptor was opened on, with the same size.
func (f *fsSegmentFile) sameAs(entry os.FileInfo) error {
	if !os.SameFile(f.identity, entry) || entry.Size() != f.size {
		return corruptError("segment %q no longer names the file the reader opened", f.path)
	}
	return nil
}

// renameNeedsClose reports whether a file must be closed before it can be
// renamed; see fs.OpenFileNamesMutable. A variable so tests can exercise the
// close path on any platform.
//
// On Windows, os.OpenFile (syscall.Open) opens files with FILE_SHARE_READ |
// FILE_SHARE_WRITE but without FILE_SHARE_DELETE (Go 1.25), and a file opened
// without it cannot be renamed while open. So there, and only there, a
// rename closes the reader's one descriptor, renames the file, and reopens
// it by its new name, with reads excluded meanwhile.
var renameNeedsClose = !fs.OpenFileNamesMutable

// renameTo renames the segment file to newPath and records the new path.
// Where open files can be renamed the descriptor keeps serving across the
// rename. Elsewhere reads are excluded while the descriptor is closed, the
// file renamed and reopened, and the reopened file is checked to be the same
// file; should the reopen fail, the segment reports the error on every later
// read, which persistence surfaces when it next reads the segment.
func (f *fsSegmentFile) renameTo(newPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fmt.Errorf("segment %q: %w", f.path, ErrReaderClosed)
	}
	if !renameNeedsClose {
		if renameErr := segmentFileSystem.Rename(f.path, newPath); renameErr != nil {
			return renameErr
		}
		f.path = newPath
		return nil
	}
	if closeErr := f.file.Close(); closeErr != nil {
		return closeErr
	}
	renameErr := segmentFileSystem.Rename(f.path, newPath)
	if renameErr == nil {
		f.path = newPath
	}
	file, openErr := segmentFileSystem.OpenFile(f.path)
	if openErr == nil {
		var identity os.FileInfo
		if identity, openErr = statSegmentFile(file); openErr == nil && (!os.SameFile(f.identity, identity) || identity.Size() != f.size) {
			openErr = corruptError("segment %q reopened as a different file", f.path)
		}
		if openErr != nil {
			openErr = errors.Join(openErr, file.Close())
		}
	}
	if openErr != nil {
		f.file = closedSegmentFile{err: fmt.Errorf("reopen segment %q: %w", f.path, openErr)}
		return errors.Join(renameErr, f.file.(closedSegmentFile).err)
	}
	f.file = file
	return renameErr
}

// closedSegmentFile stands in for a descriptor a rename could not reopen;
// every read reports why.
type closedSegmentFile struct {
	fs.File
	err error
}

func (c closedSegmentFile) Read(int64, []byte) (int, error) { return 0, c.err }
func (c closedSegmentFile) Close() error                    { return nil }
