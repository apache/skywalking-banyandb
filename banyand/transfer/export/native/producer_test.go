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

package native

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/crc32"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
)

// staleSegment is a segment file no generation references, as a merge leaves behind until the
// owner collects it.
const staleSegment = "0000000000ff.seg"

// writeIndex builds a native index of two documents in dir, adds a stale segment next to it and
// returns the files its generation references, sorted.
func writeIndex(t *testing.T, dir string) []string {
	t.Helper()
	scratch := t.TempDir()
	lockFile, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(scratch, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lockFile, scratch)
	require.NoError(t, err)
	idx := filepath.Join(scratch, "idx")
	owner, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: idx, IdentifierDocValues: true, CompactionThreshold: -1})
	require.NoError(t, err)
	for i := range 2 {
		done := make(chan error, 1)
		require.NoError(t, owner.Batch(context.Background(), native.Batch{
			Documents:          []native.Document{{Identifier: []byte(fmt.Sprintf("doc-%d", i)), Timestamp: int64(i + 1)}},
			PersistentCallback: func(err error) { done <- err },
		}))
		require.NoError(t, <-done)
	}
	require.NoError(t, owner.Close())
	g, err := native.OpenReadOnlyGenerationStrict(idx)
	require.NoError(t, err)
	live := g.ReferencedFiles()
	require.NoError(t, g.Close())
	for _, name := range live {
		data, readErr := os.ReadFile(filepath.Join(idx, name))
		require.NoError(t, readErr)
		writeFile(t, filepath.Join(dir, name), data)
	}
	writeFile(t, filepath.Join(dir, staleSegment), randomBytes(t, 64))
	sort.Strings(live)
	return live
}

// prefixed returns names with prefix prepended.
func prefixed(prefix string, names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = prefix + n
	}
	return out
}

// sentFile is one file rebuilt from the frames a producer sent.
type sentFile struct {
	started  *transferv1.FileStarted
	finished *transferv1.FileFinished
	content  []byte
}

// collect records the frames of a producer and rebuilds its files.
type collect struct {
	cur   *sentFile
	files []*sentFile
}

func (c *collect) send(r *transferv1.ExportResponse) error {
	switch f := r.GetFrame().(type) {
	case *transferv1.ExportResponse_FileStarted:
		if c.cur != nil {
			return fmt.Errorf("nested file %s", f.FileStarted.GetRelativePath())
		}
		c.cur = &sentFile{started: f.FileStarted}
	case *transferv1.ExportResponse_Chunk:
		if len(f.Chunk.GetContent()) > snapshot.ChunkLimit(nil) {
			return fmt.Errorf("chunk of %d bytes", len(f.Chunk.GetContent()))
		}
		if f.Chunk.GetRows() != 0 {
			return fmt.Errorf("a native chunk carries no rows")
		}
		c.cur.content = append(c.cur.content, f.Chunk.GetContent()...)
	case *transferv1.ExportResponse_FileFinished:
		c.cur.finished = f.FileFinished
		c.files = append(c.files, c.cur)
		c.cur = nil
	}
	return nil
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// fakeSegment lays out a trace segment snapshot: a series index with a stale segment, and
// shard 0 holding live parts 1 and 3, a stale part 2, an older snapshot file, two secondary
// index parts (one stale) and a stream-like idx/ with a stale segment. It returns the segment
// directory and the live files of the series index and of idx/.
func fakeSegment(t *testing.T) (segDir string, sidxFiles, idxFiles []string) {
	t.Helper()
	segDir = filepath.Join(t.TempDir(), "sw_trace", "seg-20260907")
	writeFile(t, filepath.Join(segDir, "metadata"), []byte(`{"version":"1.5.0"}`))
	sidxFiles = writeIndex(t, filepath.Join(segDir, "sidx"))
	shard := filepath.Join(segDir, "shard-0")
	for _, id := range []uint64{1, 2, 3} {
		part := filepath.Join(shard, fmt.Sprintf("%016x", id))
		writeFile(t, filepath.Join(part, "metadata.json"), []byte(`{"minTimestamp":1,"maxTimestamp":2,"totalCount":5}`))
		writeFile(t, filepath.Join(part, "spans.bin"), randomBytes(t, 100<<10))
		writeFile(t, filepath.Join(shard, "sidx", "duration", fmt.Sprintf("%016x", id), "manifest.json"), []byte(`{"minKey":1}`))
		writeFile(t, filepath.Join(shard, "sidx", "duration", fmt.Sprintf("%016x", id), "primary.bin"), randomBytes(t, 512))
	}
	writeFile(t, filepath.Join(shard, "0000000000000005.snp"), []byte(`["0000000000000001","0000000000000002"]`))
	writeFile(t, filepath.Join(shard, "0000000000000007.snp"), []byte(`["0000000000000001","0000000000000003","0000000000000009"]`))
	idxFiles = writeIndex(t, filepath.Join(shard, "idx"))
	return segDir, sidxFiles, idxFiles
}

// readContainer opens the volumes of one container from the sent files.
func readContainer(t *testing.T, files []*sentFile, prefix string) *bnc.VolumeSet {
	t.Helper()
	var containers []*bnc.Container
	for i := 1; ; i++ {
		var found *sentFile
		for _, f := range files {
			if f.started.GetRelativePath() == fmt.Sprintf("%s-%05d.bnc", prefix, i) {
				found = f
			}
		}
		if found == nil {
			break
		}
		c, err := bnc.Open(bytes.NewReader(found.content), int64(len(found.content)))
		require.NoError(t, err)
		containers = append(containers, c)
	}
	require.NotEmpty(t, containers, "no volume of %s", prefix)
	set, err := bnc.NewVolumeSet(containers)
	require.NoError(t, err)
	_, err = set.Verify(context.Background())
	require.NoError(t, err)
	return set
}

func paths(set *bnc.VolumeSet) []string {
	var out []string
	for _, f := range set.Files() {
		out = append(out, f.Path)
	}
	sort.Strings(out)
	return out
}

func TestSegmentPacksTheLiveSnapshot(t *testing.T) {
	segDir, sidxFiles, idxFiles := fakeSegment(t)
	for _, maxFileSize := range []uint64{0, bnc.MinFileSize} {
		t.Run(fmt.Sprintf("max-%d", maxFileSize), func(t *testing.T) {
			c := &collect{}
			init := &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE, MaxFileSize: maxFileSize}
			p, err := NewProducer(context.Background(), init, c.send)
			require.NoError(t, err)
			unit := &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_TRACE, Group: "sw_trace", SegmentSuffix: "20260907", ShardIds: []uint32{0}}
			_, _, err = p.Segment(unit, segDir)
			require.NoError(t, err)
			for _, f := range c.files {
				require.Equal(t, uint64(len(f.content)), f.finished.GetBytes())
				require.Equal(t, crc32.ChecksumIEEE(f.content), f.finished.GetCrc32())
				require.Empty(t, f.finished.GetRowCounts())
				if maxFileSize > 0 {
					require.LessOrEqual(t, uint64(len(f.content)), maxFileSize)
				}
			}

			meta := c.files[0]
			require.Equal(t, "trace/sw_trace/seg-20260907/metadata", meta.started.GetRelativePath())
			require.True(t, meta.started.GetSegmentLevel())
			require.Zero(t, meta.finished.GetEntryCount())
			require.Equal(t, `{"version":"1.5.0"}`, string(meta.content))

			sidx := readContainer(t, c.files, "trace/sw_trace/seg-20260907/sidx")
			require.Equal(t, sidxFiles, paths(sidx), "only the live generation of the series index")

			shard := readContainer(t, c.files, "trace/sw_trace/seg-20260907/shard-0")
			want := []string{
				"0000000000000001/metadata.json", "0000000000000001/spans.bin",
				"0000000000000003/metadata.json", "0000000000000003/spans.bin",
				"0000000000000007.snp",
			}
			want = append(want, prefixed("idx/", idxFiles)...)
			want = append(want,
				"sidx/duration/0000000000000001/manifest.json", "sidx/duration/0000000000000001/primary.bin",
				"sidx/duration/0000000000000003/manifest.json", "sidx/duration/0000000000000003/primary.bin",
			)
			sort.Strings(want)
			require.Equal(t, want, paths(shard), "the live parts, the newest snapshot file and the live element index")
			layout := shard.Shard()
			require.Empty(t, layout.Orphans)
			require.Len(t, layout.Parts, 2)
			for _, f := range c.files {
				if f.started.GetRelativePath() != meta.started.GetRelativePath() {
					require.Positive(t, f.finished.GetEntryCount())
				}
				if f.started.GetSegmentLevel() {
					require.Equal(t, uint32(1), f.started.GetShardIndex(), "segment-level files use total_shards")
				} else {
					require.Equal(t, uint32(0), f.started.GetShardId())
				}
			}
			if maxFileSize > 0 {
				require.Greater(t, len(c.files), 3, "a small volume limit splits the shard container")
			}
		})
	}
}

func TestSegmentWithoutSeriesIndexOrShards(t *testing.T) {
	segDir := filepath.Join(t.TempDir(), "seg-20260907")
	writeFile(t, filepath.Join(segDir, "metadata"), []byte("m"))
	c := &collect{}
	p, err := NewProducer(context.Background(), &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE}, c.send)
	require.NoError(t, err)
	unit := &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "g", SegmentSuffix: "20260907"}
	_, _, err = p.Segment(unit, segDir)
	require.NoError(t, err)
	require.Len(t, c.files, 1)
	require.Equal(t, "measure/g/seg-20260907/metadata", c.files[0].started.GetRelativePath())
}

func TestSegmentSnapshotGone(t *testing.T) {
	segDir, _, _ := fakeSegment(t)
	require.NoError(t, os.RemoveAll(filepath.Join(segDir, "shard-0")))
	p, err := NewProducer(context.Background(), &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE}, (&collect{}).send)
	require.NoError(t, err)
	unit := &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_TRACE, Group: "sw_trace", SegmentSuffix: "20260907", ShardIds: []uint32{0}}
	_, _, err = p.Segment(unit, segDir)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
}

func TestIndexOpenErrorOfAGoneSession(t *testing.T) {
	corrupt := errors.New("open missing segment 1: corrupt")
	gone := indexOpenError(filepath.Join(t.TempDir(), "gone", "idx"), corrupt)
	require.ErrorIs(t, gone, iofs.ErrNotExist)
	require.ErrorIs(t, gone, corrupt)
	present := indexOpenError(t.TempDir(), corrupt)
	require.NotErrorIs(t, present, iofs.ErrNotExist)
	require.ErrorIs(t, present, corrupt)
}

func TestPropertyPacksEveryShard(t *testing.T) {
	groupDir := filepath.Join(t.TempDir(), "sw_prop")
	shard0 := writeIndex(t, filepath.Join(groupDir, "shard-0"))
	writeIndex(t, filepath.Join(groupDir, "shard-2"))
	c := &collect{}
	p, err := NewProducer(context.Background(), &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE}, c.send)
	require.NoError(t, err)
	_, _, err = p.Property(&transferv1.PropertyUnit{Group: "sw_prop", ShardIds: []uint32{0, 2}}, groupDir)
	require.NoError(t, err)
	require.Len(t, c.files, 2)
	require.Equal(t, "property/sw_prop/shard-0-00001.bnc", c.files[0].started.GetRelativePath())
	require.Equal(t, uint32(2), c.files[1].started.GetShardId())
	require.Equal(t, uint32(1), c.files[1].started.GetShardIndex())
	require.Equal(t, shard0, paths(readContainer(t, c.files, "property/sw_prop/shard-0")))
}

func TestNewProducerRefusesSettings(t *testing.T) {
	_, err := NewProducer(context.Background(), &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE, MaxFileSize: 1}, nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = NewProducer(context.Background(), &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_GZIP}, nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// A release removes a shard's files before its directory, so a property shard left with its
// segments but no manifest is a lost snapshot, reported like a gone one.
func TestPropertyShardWithoutManifestIsGone(t *testing.T) {
	groupDir := filepath.Join(t.TempDir(), "sw_prop")
	shardDir := filepath.Join(groupDir, "shard-0")
	writeIndex(t, shardDir)
	entries, err := os.ReadDir(shardDir)
	require.NoError(t, err)
	kept := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			kept++
			continue
		}
		require.NoError(t, os.Remove(filepath.Join(shardDir, e.Name())))
	}
	require.NotZero(t, kept, "the shard keeps its segments")
	p, err := NewProducer(context.Background(), &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE}, (&collect{}).send)
	require.NoError(t, err)
	unit := &transferv1.PropertyUnit{Group: "sw_prop", ShardIds: []uint32{0}}
	_, _, err = p.Property(unit, groupDir)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
}
