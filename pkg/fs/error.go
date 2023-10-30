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

import "fmt"

const (
	isExistError    = 0
	isNotExistError = 1
	permissionError = 2
	openError       = 3
	deleteError     = 4
	writeError      = 5
	readError       = 6
	flushError      = 7
	closeError      = 8
	otherError      = 9
)

// FileSystemError implements the Error interface.
type FileSystemError struct {
	Message string
	Code    int
}

func (err *FileSystemError) Error() string {
	return fmt.Sprintf("File system return error: %s, error code: %d", err.Message, err.Code)
}
