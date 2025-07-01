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
	"fmt"
	"os"

	"golang.org/x/sys/windows"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// localFileSystem is the implementation of FileSystem interface.
func (*localFileSystem) CreateLockFile(name string, permission Mode) (File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	switch {
	case err == nil:
		lockFlags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
		lockFlags |= uint32(windows.LOCKFILE_EXCLUSIVE_LOCK)
		if err = windows.LockFileEx(windows.Handle(file.Fd()), lockFlags, 0, 1, 0, &windows.Overlapped{}); err != nil {
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

func (fs *localFileSystem) SyncPath(_ string) {}

func syncFile(_ *os.File) error {
	return nil
}

func CompareINode(srcPath, destPath string) error {
	return nil
}

// applyFadviseToFD is a no-op on non-Linux systems.
func applyFadviseToFD(fd uintptr, offset int64, length int64) error {
	return nil
}

// SyncAndDropCache syncs the file data to disk using FlushFileBuffers on Windows.
func SyncAndDropCache(fd uintptr, offset int64, length int64) error {
	// On Windows, we can flush file buffers but don't have a direct equivalent to FADV_DONTNEED
	// Convert the file descriptor to a Windows handle
	handle := windows.Handle(fd)
	if err := windows.FlushFileBuffers(handle); err != nil {
		return err
	}
	logger.GetLogger(moduleName).
		Debug().
		Msg("SyncAndDropCache: flush succeeded, page-cache drop unsupported on windows")
	return nil
}
