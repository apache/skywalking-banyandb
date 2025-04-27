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

//go:build linux
// +build linux

package fs

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/apache/skywalking-banyandb/pkg/logger"
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

func mustGetFileStat(path string) (*syscall.Stat_t, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("failed to convert fileinfo.Sys() to *syscall.Stat_t for %s", path)
	}
	return stat, nil
}

// CompareINode compares the inode of two files.
func CompareINode(srcPath, destPath string) error {
	srcStat, err := mustGetFileStat(srcPath)
	if err != nil {
		return err
	}
	destStat, err := mustGetFileStat(destPath)
	if err != nil {
		return err
	}
	if srcStat.Ino != destStat.Ino {
		return fmt.Errorf("src file inode: %d, dest file inode: %d", srcStat.Ino, destStat.Ino)
	}
	return nil
}

// ApplyFadviseToFD applies FADV_DONTNEED to the given file descriptor.
func ApplyFadviseToFD(fd uintptr, offset int64, length int64) error {
	return unix.Fadvise(int(fd), offset, length, unix.FADV_DONTNEED)
}

// IsFadvisSupported returns true if fadvis is supported on the current platform.
func IsFadvisSupported() bool {
	return true
}

// SyncAndDropCache syncs the file data to disk and then drops it from the page cache.
func SyncAndDropCache(fd uintptr, offset int64, length int64) error {
	if err := unix.Fdatasync(int(fd)); err != nil {
		return err
	}
	if err := unix.Fadvise(int(fd), offset, length, unix.FADV_DONTNEED); err != nil {
		return err
	}

	logger.GetLogger(moduleName).
		Debug().
		Msg("SyncAndDropCache: fdatasync and FADV_DONTNEED succeeded on linux")
	return nil
}
