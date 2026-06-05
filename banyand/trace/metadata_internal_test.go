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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// seriesIndexStatsSegment is a stub Segment that returns fixed SeriesIndexStats.
// The embedded interface satisfies the Segment[*tsTable, option] signature
// without implementing every method (only SeriesIndexStats is exercised by
// collectSeriesIndexInfo).
type seriesIndexStatsSegment struct {
	storage.Segment[*tsTable, option]
}

func (seriesIndexStatsSegment) SeriesIndexStats() (int64, int64) { return 12, 345 }

// TestSchemaRepo_CollectSeriesIndexInfo_ForwardsStats verifies that
// collectSeriesIndexInfo forwards the segment's SeriesIndexStats (which works
// for both open and closed segments) into the SeriesIndexInfo fields.
func TestSchemaRepo_CollectSeriesIndexInfo_ForwardsStats(t *testing.T) {
	sr := &schemaRepo{}
	info := sr.collectSeriesIndexInfo(seriesIndexStatsSegment{})
	require.NotNil(t, info)
	require.Equal(t, int64(12), info.DataCount)
	require.Equal(t, int64(345), info.DataSizeBytes)
}

// TestCollectClosedShardInfo_RealTracePart simulates a closed segment whose
// shard parts are quiescent on disk: it writes a real trace part exactly as a
// flush does (mustFlush + mustWriteSnapshot), then verifies the closed-read path
// CollectDataInfo relies on reads the per-shard stats straight from disk
// (without opening any table), and that the trace part metadata.json keys the
// storage layer reads match the totals the part actually wrote.
func TestCollectClosedShardInfo_RealTracePart(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	// Lay out a closed segment's on-disk shard: <seg>/shard-0/<part>.
	segDir := filepath.Join(tmpPath, "seg-20240501")
	shardDir := filepath.Join(segDir, "shard-0")

	mp := generateMemPart()
	mp.mustInitFromTraces(ts)
	wantCount := int64(mp.partMetadata.TotalCount)
	require.Positive(t, wantCount)
	const epoch = uint64(1)
	mp.mustFlush(fileSystem, partPath(shardDir, epoch))

	// Write the shard's snapshot manifest referencing the flushed part, as the
	// tsTable does after a flush, so the part is "current" (not an orphan).
	tst := &tsTable{root: shardDir, fileSystem: fileSystem}
	tst.mustWriteSnapshot(epoch, []string{partName(epoch)})

	// The segment is closed (no live tables): stats are read straight from disk.
	infos, totalSize := storage.CollectClosedShardInfo(segDir)
	require.Len(t, infos, 1)
	require.Equal(t, uint32(0), infos[0].ShardId)
	require.Equal(t, wantCount, infos[0].DataCount, "data count must match the flushed part's totalCount")
	require.Positive(t, infos[0].DataSizeBytes)
	require.Equal(t, int64(1), infos[0].PartCount)
	require.Equal(t, int64(1), infos[0].FilePartCount)
	require.Equal(t, infos[0].DataSizeBytes, totalSize)
}

// TestCollectClosedShardInfo_ExcludesOrphanPart verifies the closed-read path
// counts only parts referenced by the latest snapshot manifest: an orphan part
// on disk (left by a crashed merge/flush, not in the manifest) is ignored, just
// as the open path's in-memory snapshot would ignore it.
func TestCollectClosedShardInfo_ExcludesOrphanPart(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	segDir := filepath.Join(tmpPath, "seg-20240501")
	shardDir := filepath.Join(segDir, "shard-0")

	// Current part (in the manifest).
	current := generateMemPart()
	current.mustInitFromTraces(ts)
	wantCount := int64(current.partMetadata.TotalCount)
	current.mustFlush(fileSystem, partPath(shardDir, 1))

	// Orphan part on disk but NOT referenced by the manifest.
	orphan := generateMemPart()
	orphan.mustInitFromTraces(ts)
	orphan.mustFlush(fileSystem, partPath(shardDir, 2))

	tst := &tsTable{root: shardDir, fileSystem: fileSystem}
	tst.mustWriteSnapshot(1, []string{partName(1)})

	infos, _ := storage.CollectClosedShardInfo(segDir)
	require.Len(t, infos, 1)
	require.Equal(t, wantCount, infos[0].DataCount, "orphan part must not be counted")
	require.Equal(t, int64(1), infos[0].PartCount)
}
