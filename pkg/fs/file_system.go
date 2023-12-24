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

// Package fs (file system) is an independent component to operate file and directory.
package fs

import (
	"bufio"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const moduleName string = "filesystem"

// Mode contains permission of file and directory.
type Mode uint64

// Iter is used for streaming read.
type Iter struct {
	fileName string
	reader   *bufio.Reader
	buffer   []byte
}

// Writer allows writing data to a file.
type Writer interface {
	// Append mode, which adds new data to the end of a file.
	Write(buffer []byte) (int, error)
	// Returns the absolute path of the file.
	Path() string
	// Close File.
	Close() error
}

// Reader allows reading data from a file.
type Reader interface {
	// Read the entire file using streaming read.
	Read(offset int64, buffer []byte) (int, error)
	// Returns the absolute path of the file.
	Path() string
	// Close File.
	Close() error
}

// Closer allows closing a file.
type Closer interface {
	// Returns the absolute path of the file.
	Path() string
	// Close File.
	Close() error
}

// File operation interface.
type File interface {
	Writer
	Reader
	// Vector Append mode, which supports appending consecutive buffers to the end of the file.
	Writev(iov *[][]byte) (int, error)
	// Reading contiguous regions of a file and dispersing them into discontinuous buffers.
	Readv(offset int64, iov *[][]byte) (int, error)
	// Read the entire file using streaming read.
	StreamRead(buffer []byte) (*Iter, error)
	// Get the file written data's size and return an error if the file does not exist. The unit of file size is Byte.
	Size() (int64, error)
	// Returns the absolute path of the file.
	Path() string
	// Close File.
	Close() error
}

// FileSystem operation interface.
type FileSystem interface {
	// MkdirIfNotExist creates a new directory with the specified name and permission if it does not exist.
	// If the directory exists, it will do nothing.
	MkdirIfNotExist(path string, permission Mode)
	// MkdirPanicIfExist creates a new directory with the specified name and permission if it does not exist.
	// If the directory exists, it will panic.
	MkdirPanicIfExist(path string, permission Mode)
	// ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	ReadDir(dirname string) []DirEntry
	// Create and open the file by specified name and mode.
	CreateFile(name string, permission Mode) (File, error)
	// Create and open lock file by specified name and mode.
	CreateLockFile(name string, permission Mode) (File, error)
	// Open the file by specified name and mode.
	OpenFile(name string) (File, error)
	// Flush mode, which flushes all data to one file.
	Write(buffer []byte, name string, permission Mode) (int, error)
	// Read the entire file using streaming read.
	Read(name string) ([]byte, error)
	// Delete the file.
	DeleteFile(name string) error
	// Delete the directory.
	MustRMAll(path string)
	// SyncPath the directory of file.
	SyncPath(path string)
}

// DirEntry is the interface that wraps the basic information about a file or directory.
type DirEntry interface {
	// Name returns the name of the file or directory.
	Name() string

	// IsDir reports whether the entry describes a directory.
	IsDir() bool
}

func MustFlush(fs FileSystem, buffer []byte, name string, permission Mode) {
	n, err := fs.Write(buffer, name, permission)
	if err != nil {
		logger.GetLogger().Panic().Err(err).Str("path", name).Msg("cannot write data")
	}
	if n != len(buffer) {
		logger.GetLogger().Panic().Int("written", n).Int("expected", len(buffer)).Str("path", name).Msg("BUG: writer wrote wrong number of bytes")
	}
}

// MustWriteData writes data to w and panics if it cannot write all data.
func MustWriteData(w Writer, data []byte) {
	if len(data) == 0 {
		return
	}
	n, err := w.Write(data)
	if err != nil {
		logger.GetLogger().Panic().Err(err).Int("expected", len(data)).Str("path", w.Path()).Msg("cannot write data")
	}
	if n != len(data) {
		logger.GetLogger().Panic().Int("written", n).Int("expected", len(data)).Str("path", w.Path()).Msg("BUG: writer wrote wrong number of bytes")
	}
}

// MustReadData reads data from r and panics if it cannot read all data.
func MustReadData(r Reader, offset int64, buff []byte) {
	n, err := r.Read(offset, buff)
	if err != nil {
		logger.GetLogger().Panic().Err(err).Str("path", r.Path()).Msg("cannot read data")
	}
	if n != len(buff) {
		logger.GetLogger().Panic().Int("read", n).Int("expected", len(buff)).Str("path", r.Path()).Msg("BUG: reader read wrong number of bytes")
	}
}

// MustClose closes c and panics if it cannot close.
func MustClose(c Closer) {
	err := c.Close()
	if err != nil {
		logger.GetLogger().Panic().Err(err).Str("path", c.Path()).Msg("cannot close file")
	}
}
