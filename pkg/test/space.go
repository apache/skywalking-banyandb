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

package test

import (
	"fmt"
	"os"

	"github.com/stretchr/testify/require"
)

// Space create a tmp dir and returns the path and its clear function.
// It will be panic when encountering some errors.
func Space(t *require.Assertions) (tempDir string, deferFunc func()) {
	var tempDirErr error
	tempDir, tempDirErr = os.MkdirTemp("", "banyandb-test-*")
	t.Nil(tempDirErr)
	return tempDir, func() {
		if err := os.RemoveAll(tempDir); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error while removing dir: %v\n", err)
		}
	}
}

// NewSpace create a tmp dir and returns the path and its clear function.
// It will return any error when failed to create this dir.
func NewSpace() (tempDir string, deferFunc func(), err error) {
	tempDir, err = os.MkdirTemp("", "banyandb-test-*")
	return tempDir, func() {
		if err = os.RemoveAll(tempDir); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error while removing dir: %v\n", err)
		}
	}, err
}
