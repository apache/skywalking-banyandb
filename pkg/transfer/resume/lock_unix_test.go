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
//go:build !windows

package resume

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A lock file that is a FIFO is refused without opening it for a read or write that would wait.
func TestLock_RefusesAFIFOLockFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	if err := syscall.Mkfifo(LockPath(path), 0o600); err != nil {
		t.Skipf("fifos are not available: %v", err)
	}
	_, err := Lock(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a regular file")
}
