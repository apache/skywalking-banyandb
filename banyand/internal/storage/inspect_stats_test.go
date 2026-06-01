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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TestCollectClosedShardInfo verifies that per-shard part statistics are read
// from the parts referenced by the latest snapshot manifest, that orphan part
// directories not in the manifest are excluded, that the highest-epoch manifest
// wins, that a shard without a manifest reports zero, and that non-shard
// directories are ignored. This is the read-only path used to inspect a closed
// segment without reopening it.
func TestCollectClosedShardInfo(t *testing.T) {
	dir := t.TempDir()

	writePart := func(shard, partID string, totalCount, compressed uint64) {
		t.Helper()
		partDir := filepath.Join(dir, shard, partID)
		require.NoError(t, os.MkdirAll(partDir, 0o755))
		meta := fmt.Sprintf(`{"compressedSizeBytes":%d,"totalCount":%d}`, compressed, totalCount)
		require.NoError(t, os.WriteFile(filepath.Join(partDir, partDiskMetadataFilename), []byte(meta), 0o600))
	}
	writeManifest := func(shard string, epoch uint64, partIDs ...string) {
		t.Helper()
		data, err := json.Marshal(partIDs)
		require.NoError(t, err)
		name := fmt.Sprintf("%016x%s", epoch, snapshotFileSuffix)
		require.NoError(t, os.WriteFile(filepath.Join(dir, shard, name), data, 0o600))
	}

	// shard-0: two current parts + one orphan part (on disk but not in the
	// manifest, must be excluded) + a series index dir (no metadata.json).
	writePart("shard-0", "0000000000000001", 10, 100)
	writePart("shard-0", "0000000000000002", 5, 50)
	writePart("shard-0", "0000000000000099", 999, 9999) // orphan
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard-0", seriesIndexDirName), 0o755))
	writeManifest("shard-0", 0x10, "0000000000000001", "0000000000000002")

	// shard-1: a newer manifest supersedes an older one referencing a stale part.
	writePart("shard-1", "0000000000000003", 7, 70)
	writePart("shard-1", "0000000000000004", 4, 40)
	writeManifest("shard-1", 0x05, "0000000000000004") // older epoch
	writeManifest("shard-1", 0x10, "0000000000000003") // newer epoch wins

	// shard-2: a part on disk but no manifest -> reports zero current data.
	writePart("shard-2", "0000000000000005", 3, 30)

	// a non-shard directory must be ignored.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "not-a-shard"), 0o755))

	infos, totalSize := CollectClosedShardInfo(dir)
	byShard := map[uint32]*databasev1.ShardInfo{}
	for _, s := range infos {
		byShard[s.ShardId] = s
	}
	require.Len(t, byShard, 3)

	require.Equal(t, int64(15), byShard[0].DataCount, "orphan part must be excluded")
	require.Equal(t, int64(150), byShard[0].DataSizeBytes)
	require.Equal(t, int64(2), byShard[0].PartCount)
	require.Equal(t, int64(2), byShard[0].FilePartCount)

	require.Equal(t, int64(7), byShard[1].DataCount, "latest manifest must win")
	require.Equal(t, int64(70), byShard[1].DataSizeBytes)
	require.Equal(t, int64(1), byShard[1].PartCount)

	require.Equal(t, int64(0), byShard[2].DataCount, "shard without manifest reports zero")
	require.Equal(t, int64(0), byShard[2].PartCount)

	require.Equal(t, int64(220), totalSize, "total size sums shard data sizes")
}
