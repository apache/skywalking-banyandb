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

package storage

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestDeleteStaleSnapshots(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	snapshotsRoot := filepath.Join(tmpPath, "snapshots")
	fileSystem.MkdirIfNotExist(snapshotsRoot, 0o755)

	snapshotNames := []string{
		"20201010101010-00000001",
		"20201010101010-00000002",
		"20201010101010-00000003",
		"20201010101010-00000004",
	}
	for _, name := range snapshotNames {
		dirPath := filepath.Join(snapshotsRoot, name)
		fileSystem.MkdirIfNotExist(dirPath, 0o755)
	}

	DeleteStaleSnapshots(snapshotsRoot, 2, fileSystem)

	remaining := fileSystem.ReadDir(snapshotsRoot)
	require.Equal(t, 2, len(remaining))

	var names []string
	for _, info := range remaining {
		names = append(names, info.Name())
	}
	sort.Strings(names)
	assert.Equal(t, []string{"20201010101010-00000003", "20201010101010-00000004"}, names)
}
