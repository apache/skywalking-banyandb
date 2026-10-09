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

package trace

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func mkdirs(t *testing.T, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
}

func TestVerifyExportSnapshot(t *testing.T) {
	root := t.TempDir()
	shard := filepath.Join(root, "g", "seg-20260928", "shard-0")
	mkdirs(t, filepath.Join(shard, "0000000000000001"), filepath.Join(shard, sidxDirName, "rule_a", "0000000000000001"))
	require.NoError(t, verifyExportSnapshot(root))
	require.NoError(t, verifyExportSnapshot(filepath.Join(root, "missing")), "an empty snapshot is consistent")

	mkdirs(t, filepath.Join(shard, sidxDirName, "rule_a", "0000000000000002"))
	require.ErrorContains(t, verifyExportSnapshot(root), "has no core part")
}

func TestTakeConsistentSnapshot_RetakesWhenSidxIsAhead(t *testing.T) {
	dst := t.TempDir()
	shard := filepath.Join(dst, "g", "seg-20260928", "shard-0")
	attempts := 0
	err := takeConsistentSnapshot(dst, logger.GetLogger("test"), func() error {
		attempts++
		mkdirs(t, filepath.Join(shard, "0000000000000001"))
		if attempts == 1 {
			// The first snapshot catches a sidx part whose core part is not published yet.
			mkdirs(t, filepath.Join(shard, sidxDirName, "rule", "0000000000000002"))
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "an inconsistent snapshot must be retaken once")
	_, err = os.Stat(filepath.Join(shard, sidxDirName))
	require.True(t, os.IsNotExist(err), "the retaken snapshot must not carry the stale sidx part")
}

func TestTakeConsistentSnapshot_GivesUpAfterAttempts(t *testing.T) {
	dst := t.TempDir()
	attempts := 0
	err := takeConsistentSnapshot(dst, logger.GetLogger("test"), func() error {
		attempts++
		mkdirs(t, filepath.Join(dst, "g", "seg-20260928", "shard-0", sidxDirName, "rule", "0000000000000002"))
		return nil
	})
	require.ErrorContains(t, err, "has no core part")
	require.Equal(t, exportSnapshotAttempts, attempts)
}

func TestTakeConsistentSnapshot_ReturnsTakeError(t *testing.T) {
	injected := errors.New("injected")
	attempts := 0
	err := takeConsistentSnapshot(t.TempDir(), logger.GetLogger("test"), func() error {
		attempts++
		return injected
	})
	require.ErrorIs(t, err, injected)
	require.Equal(t, 1, attempts)
}
