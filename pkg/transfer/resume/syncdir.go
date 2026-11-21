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

package resume

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// SyncDir makes the new entries of a directory durable: it opens the directory and fsyncs it.
// A file system that cannot fsync a directory (EINVAL or ENOTSUP, as some FUSE and 9p mounts
// answer) is not an error: there is nothing more to do there.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err = d.Sync(); dirSyncUnsupported(err) {
		err = nil
	}
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}
	return err
}

// dirSyncUnsupported reports whether a directory fsync failed only because the file system
// cannot fsync a directory.
func dirSyncUnsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP)
}
