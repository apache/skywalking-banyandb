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

// File operation interface.
type File interface {
	// Append mode, which adds new data to the end of a file.
	Write(buffer []byte) (int, error)
	// Vector Append mode, which supports appending consecutive buffers to the end of the file.
	Writev(iov *[][]byte) (int, error)
	// Reading a specified location of file.
	Read(offset int64, buffer []byte) (int, error)
	// Reading contiguous regions of a file and dispersing them into discontinuous buffers.
	Readv(offset int64, iov *[][]byte) (int, error)
	// Read the entire file using streaming read.
	StreamRead(buffer []byte) (*Iter, error)
	// Get the file written data's size and return an error if the file does not exist. The unit of file size is Byte.
	Size() (int64, error)
	// Close File.
	Close() error
}

// FileSystem operation interface.
type FileSystem interface {
	// Create and open the file by specified name and mode.
	CreateFile(name string, permission Mode) (File, error)
	// Flush mode, which flushes all data to one file.
	Write(buffer []byte, name string, permission Mode) (int, error)
	// Delete the file.
	DeleteFile(name string) error
}
