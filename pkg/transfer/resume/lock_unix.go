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
	"fmt"
	"os"
	"syscall"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// openLockFile opens the lock file at lockPath, takes its lock without waiting (flock) and
// empties it. The file is emptied only once it is locked, so a refused attempt leaves the holder's
// entry. It never follows a symlink and refuses anything but a regular file, such as a FIFO.
func openLockFile(lockPath string) (lockFile, error) {
	f, err := os.OpenFile(lockPath, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	switch {
	case err != nil:
	case !info.Mode().IsRegular():
		err = fmt.Errorf("%s is not a regular file (%s)", lockPath, info.Mode().Type())
	default:
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); errors.Is(err, syscall.EWOULDBLOCK) {
			err = fs.ErrLockHeld
		} else if err == nil {
			err = f.Truncate(0)
		}
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
