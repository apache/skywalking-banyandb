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

// Package file provides utils to handle files.
package file

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

// Read bytes from given file or stdin (in case that path is `-`).
func Read(path string, reader io.Reader) (contents [][]byte, err error) {
	var b []byte
	if path == "-" {
		b, err = io.ReadAll(bufio.NewReader(reader))
		return append(contents, b), err
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		b, err = os.ReadFile(path)
		return append(contents, b), err
	}
	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // prevent panic from failed accessing path
		}
		if filepath.Ext(path) == ".yml" || filepath.Ext(path) == ".yaml" {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			contents = append(contents, content)
		}
		return nil
	})
	return contents, err
}
