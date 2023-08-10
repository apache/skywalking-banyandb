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
	"container/list"
)

// Mode contains permission of file and directory.
type Mode struct{}

// Iter is used for streaming read.
type Iter struct{}

// Dir operation interface.
type Dir interface {
	// Delete the directory.
	DeleteDirectory() error
	// Rename the directory.
	RenameDirectory(newName string) error
	// Get all lists of files or children's directories in the directory.
	ReadDirectory() (list.List, error)
	// Set directory permission.
	SetDirectoryPermission(permission Mode) error
}

// File operation interface.
type File interface {
	// Append mode, which adds new data to the end of a file.
	AppendWriteFile(buffer []byte) error
	// Vector Append mode, which supports appending consecutive buffers to the end of the file.
	AppendWritevFile(iov *[][]byte) error
	// Delete the file.
	DeleteFile() error
	// Reading a specified location of file.
	ReadFile(offset int, buffer []byte) error
	// Reading contiguous regions of a file and dispersing them into discontinuous buffers.
	ReadvFile(iov *[][]byte) error
	// Read the entire file using streaming read
	StreamReadFile(offset int, buffer []byte) (*Iter, error)
	// Rename the file.
	RenameFile(newName string) error
	// Get the file written data's size and return an error if the file does not exist. The unit of file size is Byte.
	GetFileSize() (int, error)
	// Set directory permission.
	SetFilePermission(permission Mode) error
}

// FileSystem operation interface.
type FileSystem interface {
	// Create the directory by specified name and mode.
	CreateDirectory(name string, permission Mode) error
	// Open the directory by specified name.
	OpenDirectory(name string) (*Dir, error)
	// Create the file by specified name and mode.
	CreateFile(name string, permission Mode) error
	// Open the file by specified name.
	OpenFile(name string) (*File, error)
	// Flush mode, which flushes all data to one file.
	FlushWriteFile(buffer []byte, permission Mode) (*File, error)
}
