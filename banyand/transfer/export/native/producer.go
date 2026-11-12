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

// Package native produces the native artifact of an export unit: the engine files of the
// session snapshot packed byte for byte into .bnc containers.
package native

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// Producer writes every unit of one Export stream in the native format. A segment unit
// becomes the segment's metadata file as it is, its series index packed into
// sidx-NNNNN.bnc and every requested shard packed into shard-<N>-NNNNN.bnc; a property unit
// becomes one shard-<N>-NNNNN.bnc per shard. Volumes are at most max_file_size bytes.
type Producer struct {
	ctx         context.Context
	bytes       *rate.Limiter
	send        func(*transferv1.ExportResponse) error
	maxFileSize uint64
}

// NewProducer creates the native producer of one Export stream. A max_file_size below the
// smallest volume a container can have is refused.
func NewProducer(ctx context.Context, init *transferv1.ExportRequest_Init, send func(*transferv1.ExportResponse) error) (*Producer, error) {
	if size := init.GetMaxFileSize(); size != 0 && size < bnc.MinFileSize {
		return nil, status.Errorf(codes.InvalidArgument, "native max_file_size %d is below the minimum of %d bytes", size, bnc.MinFileSize)
	}
	if init.GetCompress() != transferv1.Compress_COMPRESS_NONE {
		return nil, status.Errorf(codes.InvalidArgument, "a native export is not compressed, got %s", init.GetCompress())
	}
	return &Producer{
		ctx:         ctx,
		bytes:       snapshot.NewBucket(init.GetRateLimit().GetBytesPerSecond()),
		send:        send,
		maxFileSize: init.GetMaxFileSize(),
	}, nil
}

// container is one directory of the snapshot packed into the volumes <prefix>-NNNNN.bnc.
type container struct {
	dir          string
	prefix       string
	members      []bnc.Member
	shardID      uint32
	shardIndex   uint32
	segmentLevel bool
}

// unitPlan is what a unit produces: the plain files and the containers. totalShards is the
// shard_index of its segment-level files.
type unitPlan struct {
	plain       []plainFile
	containers  []container
	totalShards uint32
}

// plainFile is a small snapshot file sent as it is, read whole at plan time.
type plainFile struct {
	path string
	data []byte
}

// Segment produces a stream, measure or trace unit; segDir is <root>/<group>/seg-<suffix>
// and every requested shard directory under it exists. Native files carry no row counts.
func (p *Producer) Segment(unit *transferv1.SegmentUnit, segDir string) ([]string, uint64, error) {
	plan, err := planSegment(unit, segDir)
	if err != nil {
		return nil, 0, err
	}
	return nil, 0, p.produce(plan)
}

// Property produces a property unit; groupDir is <root>/<group> and every requested shard
// directory under it exists.
func (p *Producer) Property(unit *transferv1.PropertyUnit, groupDir string) ([]string, uint64, error) {
	plan, err := planProperty(unit, groupDir)
	if err != nil {
		return nil, 0, err
	}
	return nil, 0, p.produce(plan)
}

// planSegment lists what a segment unit produces.
func planSegment(unit *transferv1.SegmentUnit, segDir string) (*unitPlan, error) {
	prefix := fmt.Sprintf("%s/%s/%s%s", transfer.CatalogName(unit.GetCatalog()), csvfmt.EscapePathComponent(unit.GetGroup()),
		nativefmt.SegDirPrefix, unit.GetSegmentSuffix())
	plan := &unitPlan{totalShards: uint32(len(unit.GetShardIds()))}
	metaPath := filepath.Join(segDir, nativefmt.SegmentMetaFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, snapshot.ReadError("read "+metaPath, err)
	}
	plan.plain = append(plan.plain, plainFile{path: prefix + "/" + nativefmt.SegmentMetaFile, data: data})
	sidxDir := filepath.Join(segDir, nativefmt.SeriesIndexDir)
	members, err := listIndexMembers(sidxDir)
	switch {
	case errors.Is(err, iofs.ErrNotExist):
	case err != nil:
		return nil, snapshot.ReadError("list "+sidxDir, err)
	case len(members) > 0:
		plan.containers = append(plan.containers, container{
			dir: sidxDir, prefix: prefix + "/" + nativefmt.SeriesIndexDir, members: bnc.OrderIndexDir(members),
			shardIndex: plan.totalShards, segmentLevel: true,
		})
	}
	for i, shard := range unit.GetShardIds() {
		shardDir := filepath.Join(segDir, nativefmt.ShardDir(shard))
		shardMembers, shardErr := shardMembers(shardDir)
		if shardErr != nil {
			return nil, snapshot.ReadError("list "+shardDir, shardErr)
		}
		plan.containers = append(plan.containers, container{
			dir: shardDir, prefix: prefix + "/" + nativefmt.ShardDir(shard), members: shardMembers,
			shardID: shard, shardIndex: uint32(i),
		})
	}
	return plan, nil
}

// planProperty lists what a property unit produces.
func planProperty(unit *transferv1.PropertyUnit, groupDir string) (*unitPlan, error) {
	prefix := fmt.Sprintf("%s/%s", transfer.CatalogName(commonv1.Catalog_CATALOG_PROPERTY), csvfmt.EscapePathComponent(unit.GetGroup()))
	plan := &unitPlan{totalShards: uint32(len(unit.GetShardIds()))}
	for i, shard := range unit.GetShardIds() {
		shardDir := filepath.Join(groupDir, nativefmt.ShardDir(shard))
		members, err := listIndexMembers(shardDir)
		if err != nil {
			return nil, snapshot.ReadError("list "+shardDir, err)
		}
		if len(members) == 0 {
			// The session's snapshot publishes a manifest for every shard, an empty one
			// included, so a shard with no live file is a lost snapshot, not an empty shard.
			return nil, snapshot.ReadError("list "+shardDir, fmt.Errorf("%w: property shard holds no committed generation", iofs.ErrNotExist))
		}
		plan.containers = append(plan.containers, container{
			dir: shardDir, prefix: prefix + "/" + nativefmt.ShardDir(shard), members: bnc.OrderIndexDir(members),
			shardID: shard, shardIndex: uint32(i),
		})
	}
	return plan, nil
}

// shardMembers lists the files of a shard a table opened on it loads, in container order: the
// parts the newest snapshot file names that are on disk, with their trace secondary index
// parts, that snapshot file, the element index and any other file. Parts no snapshot names
// and older snapshot files are leftovers of a closed segment's merges and flushes, and are
// left out.
func shardMembers(shardDir string) ([]bnc.Member, error) {
	live, newest, err := snapshot.SessionParts(shardDir)
	if err != nil {
		return nil, err
	}
	// A part directory classifies as the files under it; one no live part owns is not walked.
	members, err := listMembers(shardDir, func(rel string) bool {
		c := bnc.ClassifyShardPath(rel + "/")
		if c.Kind != bnc.KindPart {
			return true
		}
		_, found := slices.BinarySearch(live, c.PartID)
		return found
	})
	if err != nil {
		return nil, err
	}
	idx, err := liveIndexFiles(filepath.Join(shardDir, nativefmt.ElementIndexDir))
	if err != nil {
		return nil, err
	}
	kept := members[:0]
	for _, m := range members {
		switch c := bnc.ClassifyShardPath(m.Path); c.Kind {
		case bnc.KindPart:
			if _, found := slices.BinarySearch(live, c.PartID); !found {
				continue
			}
		case bnc.KindSnapshot:
			if m.Path != newest {
				continue
			}
		case bnc.KindElementIndex:
			if !idx[strings.TrimPrefix(m.Path, nativefmt.ElementIndexDir+"/")] {
				continue
			}
		case bnc.KindOther:
		}
		kept = append(kept, m)
	}
	return bnc.OrderShard(kept), nil
}

// listIndexMembers lists the files of the newest committed generation of the native index in
// dir: its manifest and the segments it references. A segment an older generation left behind
// (a merge's input the owner has not collected yet) would be introduced next to the merged
// one on the receiving side, and a receiver that keeps the first copy of a document would then
// keep a stale one. A missing directory is fs.ErrNotExist; one without a committed generation
// holds nothing.
func listIndexMembers(dir string) ([]bnc.Member, error) {
	live, err := liveIndexFiles(dir)
	if err != nil {
		return nil, err
	}
	members, err := listMembers(dir, nil)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(members, func(m bnc.Member) bool { return !live[m.Path] }), nil
}

// liveIndexFiles returns the names of the files the newest committed generation of the native
// index in dir references; none when dir has no committed generation or does not exist.
func liveIndexFiles(dir string) (map[string]bool, error) {
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return map[string]bool{}, nil
		}
		return nil, err
	}
	files, err := native.NewestGenerationFiles(dir)
	if errors.Is(err, native.ErrNoSnapshot) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, indexOpenError(dir, err)
	}
	live := map[string]bool{}
	for _, f := range files {
		live[f] = true
	}
	return live, nil
}

// indexOpenError wraps a failure to open the native index in dir. A session released while the
// index was being opened removes dir, and the open then reports a missing segment as corrupt;
// the error then carries fs.ErrNotExist so that it maps to a gone snapshot, not to Internal.
func indexOpenError(dir string, err error) error {
	if _, statErr := os.Stat(dir); errors.Is(statErr, iofs.ErrNotExist) {
		return fmt.Errorf("open the native index %s: %w", dir, errors.Join(statErr, err))
	}
	return fmt.Errorf("open the native index %s: %w", dir, err)
}

// listMembers lists every regular file under dir with its slash path relative to dir. A
// subdirectory keepDir refuses, given its slash path relative to dir, is not walked; nil
// walks every one.
func listMembers(dir string, keepDir func(rel string) bool) ([]bnc.Member, error) {
	var out []bnc.Member
	err := filepath.WalkDir(dir, func(p string, d iofs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(dir, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if keepDir != nil && rel != "." && !keepDir(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", p)
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		out = append(out, bnc.Member{Path: rel, Size: info.Size()})
		return nil
	})
	return out, err
}

// produce sends the plain files, then every container.
func (p *Producer) produce(plan *unitPlan) error {
	for _, f := range plan.plain {
		if err := p.sendPlain(f, plan.totalShards); err != nil {
			return err
		}
	}
	for _, c := range plan.containers {
		if err := p.pack(c); err != nil {
			return err
		}
	}
	return nil
}

// sendPlain sends one snapshot file as it is, as a segment-level file whose shard_index is
// the unit's shard count. The plan owns f.data, so its chunks are sent without a copy.
func (p *Producer) sendPlain(f plainFile, shardIndex uint32) error {
	framer := p.framer(0, shardIndex, true)
	if err := framer.StartFile(f.path, 1); err != nil {
		return err
	}
	if err := framer.WriteOwned(f.data); err != nil {
		return err
	}
	return framer.Finish(&transferv1.FileFinished{Bytes: uint64(len(f.data)), Crc32: crc32.ChecksumIEEE(f.data)})
}

// pack writes the volumes of one container.
func (p *Producer) pack(c container) error {
	sink := frameSink{Framer: p.framer(c.shardID, c.shardIndex, c.segmentLevel)}
	v, err := bnc.NewVolumeWriter(sink, c.prefix, p.maxFileSize, func(rel string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(c.dir, filepath.FromSlash(rel)))
	})
	if err != nil {
		return packError(c.prefix, err)
	}
	for _, m := range c.members {
		if err = v.Add(p.ctx, m); err != nil {
			return packError(c.prefix, err)
		}
	}
	if err = v.Close(); err != nil {
		return packError(c.prefix, err)
	}
	return nil
}

// packError keeps a stream error as it is and maps any other failure as a snapshot read.
func packError(prefix string, err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return snapshot.ReadError("pack "+prefix, err)
}

// framer returns the framer of one container's volumes or of a plain file.
func (p *Producer) framer(shardID, shardIndex uint32, segmentLevel bool) *snapshot.Framer {
	return &snapshot.Framer{Ctx: p.ctx, Bytes: p.bytes, Send: p.send, ShardID: shardID, ShardIndex: shardIndex, SegmentLevel: segmentLevel}
}

// frameSink is the bnc.Sink of one container: its volumes framed by the Framer.
type frameSink struct {
	*snapshot.Framer
}

// FinishFile implements bnc.Sink.
func (s frameSink) FinishFile(sum bnc.Summary) error {
	return s.Finish(&transferv1.FileFinished{Bytes: sum.Bytes, Crc32: sum.CRC32, EntryCount: sum.Entries})
}
