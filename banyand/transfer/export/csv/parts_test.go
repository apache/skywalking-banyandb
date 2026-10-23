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

package csv

import (
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only the parts the newest readable snapshot names are live: a part directory left
// over from a merge is skipped, as is a part a snapshot names but the disk lacks.
func TestLivePartIDs(t *testing.T) {
	dir := t.TempDir()
	for _, part := range []string{"0000000000000001", "0000000000000002", "0000000000000003"} {
		require.NoError(t, os.Mkdir(filepath.Join(dir, part), 0o755))
	}
	write := func(name, content string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}

	ids, err := LivePartIDs(dir)
	require.NoError(t, err)
	require.Empty(t, ids, "a shard without a snapshot holds no live part")

	write("0000000000000004.snp", `["0000000000000003"]`)
	write("0000000000000005.snp", `["0000000000000001","0000000000000002","0000000000000009"]`)
	write("0000000000000006.snp", `not json`)
	ids, err = LivePartIDs(dir)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, ids, "the newest readable snapshot wins")

	// Snapshot files none of which can be read are an error, not an empty shard.
	require.NoError(t, os.Remove(filepath.Join(dir, "0000000000000004.snp")))
	require.NoError(t, os.Remove(filepath.Join(dir, "0000000000000005.snp")))
	_, err = LivePartIDs(dir)
	require.ErrorContains(t, err, "no readable snapshot")
}

// Only the parts the newest snapshot of each requested shard names and holds are counted; a
// requested shard whose snapshot is missing or holds no part lost data.
func TestCountLiveParts(t *testing.T) {
	segDir := t.TempDir()
	shard := func(id int, parts []string, snapshot string) {
		dir := filepath.Join(segDir, fmt.Sprintf("shard-%d", id))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		for _, p := range parts {
			require.NoError(t, os.Mkdir(filepath.Join(dir, p), 0o755))
		}
		if snapshot != "" {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "0000000000000001.snp"), []byte(snapshot), 0o600))
		}
	}
	shard(0, []string{"0000000000000001", "0000000000000002"}, `["0000000000000001","0000000000000002"]`)
	// Part 6 is an in-memory part the snapshot file names but does not hold.
	shard(1, []string{"0000000000000003", "0000000000000004"}, `["0000000000000004","0000000000000006"]`)
	shard(2, []string{"0000000000000005"}, "")
	shard(3, nil, `["0000000000000007"]`)

	parts, err := CountLiveParts(segDir, []uint32{0, 1})
	require.NoError(t, err)
	require.Equal(t, uint32(3), parts)
	parts, err = CountLiveParts(segDir, []uint32{1})
	require.NoError(t, err)
	require.Equal(t, uint32(1), parts)
	parts, err = CountLiveParts(segDir, nil)
	require.NoError(t, err)
	require.Zero(t, parts, "a unit without shards has no parts")
	for _, lost := range []uint32{2, 3, 7} {
		_, err = CountLiveParts(segDir, []uint32{0, lost})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "shard %d lost its snapshot or parts: %v", lost, err)
	}
}

// A session read fails on a shard whose snapshot file is gone or unreadable, or whose snapshot
// holds none of the parts it names, instead of exporting it as empty.
func TestSessionPartIDs(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	for _, p := range []string{"0000000000000001", "0000000000000002"} {
		require.NoError(t, os.Mkdir(filepath.Join(dir, p), 0o755))
	}
	_, err := SessionPartIDs(dir)
	require.ErrorIs(t, err, iofs.ErrNotExist, "no snapshot file")

	write("0000000000000004.snp", `["0000000000000001"]`)
	write("0000000000000005.snp", `["0000000000000002","0000000000000001","0000000000000009"]`)
	ids, err := SessionPartIDs(dir)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, ids, "the newest snapshot's parts on disk")

	write("0000000000000006.snp", `not json`)
	_, err = SessionPartIDs(dir)
	require.Error(t, err, "an unreadable newest snapshot is not skipped")

	write("0000000000000007.snp", `["0000000000000009"]`)
	_, err = SessionPartIDs(dir)
	require.ErrorIs(t, err, iofs.ErrNotExist, "a snapshot holding none of its parts")
}
