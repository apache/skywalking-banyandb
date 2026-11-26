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

package csv

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/apache/skywalking-banyandb/banyand/transfer/export/snapshot"
	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
)

// localFS is the file system the part readers open snapshot files through.
var localFS = pkgfs.NewLocalFileSystem()

// readOnlyFS is the file system the part readers open snapshot files through. It never
// writes; ReadDir reports a failure instead of panicking (a preempted session can remove
// the directory mid-read). The part readers skip some files they cannot open, so the first
// failure to list, open or read is kept for the caller to check with Err; only a missing file
// may be opened or read without failing, as the readers probe optional files that way.
type readOnlyFS struct {
	pkgfs.FileSystem
	err error
}

// newReadOnlyFS returns a readOnlyFS over the local file system.
func newReadOnlyFS() *readOnlyFS {
	return &readOnlyFS{FileSystem: localFS}
}

// Err returns the first failure to list, open or read a file, nil without one.
func (f *readOnlyFS) Err() error { return f.err }

func (f *readOnlyFS) keep(err error) {
	if err != nil && f.err == nil {
		f.err = err
	}
}

// keepPresent keeps err unless it reports a missing file.
func (f *readOnlyFS) keepPresent(err error) {
	if !errors.Is(err, fs.ErrNotExist) {
		f.keep(err)
	}
}

// ReadDir implements pkgfs.FileSystem.
func (f *readOnlyFS) ReadDir(dirname string) []pkgfs.DirEntry {
	entries, err := f.ReadDirLimit(dirname, 0)
	f.keep(err)
	return entries
}

// OpenFile implements pkgfs.FileSystem.
func (f *readOnlyFS) OpenFile(name string) (pkgfs.File, error) {
	file, err := f.FileSystem.OpenFile(name)
	f.keepPresent(err)
	return file, err
}

// Read implements pkgfs.FileSystem.
func (f *readOnlyFS) Read(name string) ([]byte, error) {
	data, err := f.FileSystem.Read(name)
	f.keepPresent(err)
	return data, err
}

// openSnapshotPart opens part id of shardDir read-only through open (a dump package's OpenPart),
// failing when the part or any file it lists cannot be read.
func openSnapshotPart[P interface{ Close() error }](open func(id uint64, root string, fileSystem pkgfs.FileSystem) (P, error),
	id uint64, shardDir string,
) (P, error) {
	rofs := newReadOnlyFS()
	part, err := open(id, shardDir, rofs)
	if err == nil && rofs.Err() != nil {
		_ = part.Close()
		err = rofs.Err()
	}
	if err != nil {
		var zero P
		return zero, snapshot.ReadError(fmt.Sprintf("open part %016x of %s", id, shardDir), err)
	}
	return part, nil
}
