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

package sidx

import (
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
)

// NewPartsToSync creates a new map of parts to sync.
func NewPartsToSync() map[string][]*Part {
	partsToSync := make(map[string][]*Part)
	return partsToSync
}

// PartsToSync returns the parts to sync.
func (s *sidx) PartsToSync() []*part {
	snapshot := s.currentSnapshot()
	if snapshot == nil {
		return nil
	}
	defer snapshot.decRef()
	partsToSync := make([]*part, 0, len(snapshot.parts))
	for _, pw := range snapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}

	if len(partsToSync) > 1 {
		sort.Slice(partsToSync, func(i, j int) bool {
			return partsToSync[i].partMetadata.ID < partsToSync[j].partMetadata.ID
		})
	}
	return partsToSync
}

// StreamingParts returns the streaming parts.
func (s *sidx) StreamingParts(partIDsToSync map[uint64]struct{}, group string, shardID uint32, name string) ([]queue.StreamingPartData, []func()) {
	snapshot := s.currentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	defer snapshot.decRef()
	var streamingParts []queue.StreamingPartData
	var releaseFuncs []func()
	for _, pw := range snapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			if _, ok := partIDsToSync[pw.p.partMetadata.ID]; ok {
				part := pw.p
				files, release := createPartFileReaders(part)
				releaseFuncs = append(releaseFuncs, release)
				streamingParts = append(streamingParts, queue.StreamingPartData{
					ID:                    part.partMetadata.ID,
					Group:                 group,
					ShardID:               shardID,
					Topic:                 data.TopicTracePartSync.String(),
					Files:                 files,
					CompressedSizeBytes:   part.partMetadata.CompressedSizeBytes,
					UncompressedSizeBytes: part.partMetadata.UncompressedSizeBytes,
					TotalCount:            part.partMetadata.TotalCount,
					BlocksCount:           part.partMetadata.BlocksCount,
					MinTimestamp:          part.partMetadata.SegmentID,
					MinKey:                part.partMetadata.MinKey,
					MaxKey:                part.partMetadata.MaxKey,
					PartType:              name,
				})
			}
		}
	}

	if len(streamingParts) > 1 {
		sort.Slice(streamingParts, func(i, j int) bool {
			return streamingParts[i].ID < streamingParts[j].ID
		})
	}
	return streamingParts, releaseFuncs
}

func createPartFileReaders(part *part) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo
	var buffersToRelease []*bytes.Buffer

	buf := bigValuePool.Get()
	if buf == nil {
		buf = &bytes.Buffer{}
	}
	// Trace metadata
	for i := range part.primaryBlockMetadata {
		buf.Buf = part.primaryBlockMetadata[i].marshal(buf.Buf)
	}
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	bb.Buf = zstd.Compress(bb.Buf[:0], buf.Buf, 1)
	buf.Buf = buf.Buf[:0]
	bigValuePool.Put(buf)
	buffersToRelease = append(buffersToRelease, bb)
	files = append(files,
		queue.FileInfo{
			Name:   SidxMetaName,
			Reader: bb.SequentialRead(),
		},
		queue.FileInfo{
			Name:   SidxPrimaryName,
			Reader: part.primary.SequentialRead(),
		},
		queue.FileInfo{
			Name:   SidxDataName,
			Reader: part.data.SequentialRead(),
		},
		queue.FileInfo{
			Name:   SidxKeysName,
			Reader: part.keys.SequentialRead(),
		},
	)

	// Trace tags data
	if part.tagData != nil {
		for name, reader := range part.tagData {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", TagDataPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
		for name, reader := range part.tagMetadata {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", TagMetadataPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
		for name, reader := range part.tagFilters {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", TagFilterPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
	}

	return files, func() {
		for _, buffer := range buffersToRelease {
			buffer.Buf = buffer.Buf[:0]
			bigValuePool.Put(buffer)
		}
	}
}
