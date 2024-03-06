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

//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd
// +build darwin dragonfly freebsd linux netbsd openbsd

package fs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// localFileSystem is the implementation of FileSystem interface.
func (*localFileSystem) CreateLockFile(name string, permission Mode) (File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	switch {
	case err == nil:
		if err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return nil, &FileSystemError{
				Code:    lockError,
				Message: fmt.Sprintf("Cannot lock file, file name: %s, error message: %s", name, err),
			}
		}
		return &LocalFile{
			file: file,
		}, nil
	case os.IsExist(err):
		return nil, &FileSystemError{
			Code:    isExistError,
			Message: fmt.Sprintf("File is exist, file name: %s,error message: %s", name, err),
		}
	case os.IsPermission(err):
		return nil, &FileSystemError{
			Code:    permissionError,
			Message: fmt.Sprintf("There is not enough permission, file name: %s, permission: %d,error message: %s", name, permission, err),
		}
	default:
		return nil, &FileSystemError{
			Code:    otherError,
			Message: fmt.Sprintf("Create file return error, file name: %s,error message: %s", name, err),
		}
	}
}

func (fs *localFileSystem) SyncPath(name string) {
	file, err := os.Open(name)
	if err != nil {
		fs.logger.Panic().Str("name", name).Err(err).Msg("failed to open file")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		fs.logger.Panic().Str("name", name).Err(err).Msg("failed to sync file")
	}
	if err := file.Close(); err != nil {
		fs.logger.Panic().Str("name", name).Err(err).Msg("failed to close file")
	}
}

func syncFile(file *os.File) error {
	if err := file.Sync(); err != nil {
		return &FileSystemError{
			Code:    flushError,
			Message: fmt.Sprintf("Flush File error, directory name: %s, error message: %s", file.Name(), err),
		}
	}
	return nil
}
