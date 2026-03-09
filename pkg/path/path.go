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

// Package path provides utilities to resolve relative paths to absolute paths.
package path

import (
	"path/filepath"
)

// Get resolves the given path to an absolute path.
// If the path is empty, it returns an empty string.
// If the path is already absolute, it returns the path as is.
// Otherwise, it returns the absolute path relative to the current working directory.
func Get(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	return filepath.Abs(p)
}
