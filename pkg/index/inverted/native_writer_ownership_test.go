// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package inverted

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

func TestNewNativeStoreRequiresLiveRootOwner(t *testing.T) {
	root := t.TempDir()
	shardPath := filepath.Join(root, "group", "shard-0")

	_, err := NewNativeStore(StoreOpts{Path: shardPath}, nil)
	require.Error(t, err)

	localFS := fs.NewLocalFileSystem()
	lock, lockErr := localFS.CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, lockErr)
	owner, ownerErr := NewNativeWriterOwner(lock, root)
	require.NoError(t, ownerErr)

	store, storeErr := NewNativeStore(StoreOpts{Path: shardPath}, owner)
	require.NoError(t, storeErr)
	require.NoError(t, store.Close())
	require.NoError(t, owner.Close())

	_, err = NewNativeStore(StoreOpts{Path: filepath.Join(root, "other", "shard-0")}, owner)
	require.Error(t, err)
}

func TestNewNativeWriterOwnerRejectsUnownedOrMismatchedLocks(t *testing.T) {
	root := t.TempDir()
	localFS := fs.NewLocalFileSystem()

	regularFile, regularErr := localFS.CreateFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, regularErr)
	_, err := NewNativeWriterOwner(regularFile, root)
	require.Error(t, err)
	require.NoError(t, regularFile.Close())

	lock, lockErr := localFS.CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, lockErr)
	_, err = NewNativeWriterOwner(lock, filepath.Join(root, "wrong-root"))
	require.Error(t, err)
	require.NoError(t, lock.Close())
}
