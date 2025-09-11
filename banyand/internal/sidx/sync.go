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

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
)

// NewPartsToSync creates a new map of parts to sync.
func NewPartsToSync() map[string][]*Part {
	partsToSync := make(map[string][]*Part)
	return partsToSync
}

// PartsToSync returns the parts to sync.
func (s *sidx) PartsToSync() []*part {
	var partsToSync []*part
	for _, pw := range s.snapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}
	for i := 0; i < len(partsToSync); i++ {
		for j := i + 1; j < len(partsToSync); j++ {
			if partsToSync[i].partMetadata.ID > partsToSync[j].partMetadata.ID {
				partsToSync[i], partsToSync[j] = partsToSync[j], partsToSync[i]
			}
		}
	}
	return partsToSync
}

// StreamingParts returns the streaming parts.
func (s *sidx) StreamingParts(partsToSync []*part, group string, shardID uint32, name string) ([]queue.StreamingPartData, []func()) {
	var streamingParts []queue.StreamingPartData
	var releaseFuncs []func()
	for _, part := range partsToSync {
		// Create streaming reader for the part
		files, release := createPartFileReaders(part)
		releaseFuncs = append(releaseFuncs, release)

		// Create streaming part sync data
		streamingParts = append(streamingParts, queue.StreamingPartData{
			ID:                    part.partMetadata.ID,
			Group:                 group,
			ShardID:               shardID,
			Topic:                 data.TopicStreamPartSync.String(),
			Files:                 files,
			CompressedSizeBytes:   part.partMetadata.CompressedSizeBytes,
			UncompressedSizeBytes: part.partMetadata.UncompressedSizeBytes,
			TotalCount:            part.partMetadata.TotalCount,
			BlocksCount:           part.partMetadata.BlocksCount,
			MinTimestamp:          part.partMetadata.MinKey,
			MaxTimestamp:          part.partMetadata.MaxKey,
			PartType:              name,
		})
	}
	return streamingParts, releaseFuncs
}

func createPartFileReaders(part *part) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo

	buf := bigValuePool.Get()
	// Trace metadata
	for i := range part.primaryBlockMetadata {
		buf.Buf = part.primaryBlockMetadata[i].marshal(buf.Buf)
	}
	bb := bigValuePool.Get()
	bb.Buf = zstd.Compress(bb.Buf[:0], buf.Buf, 1)
	bigValuePool.Put(buf)
	files = append(files,
		queue.FileInfo{
			Name:   MetaFilename,
			Reader: bb.SequentialRead(),
		},
		queue.FileInfo{
			Name:   PrimaryFilename,
			Reader: part.primary.SequentialRead(),
		},
		queue.FileInfo{
			Name:   DataFilename,
			Reader: part.data.SequentialRead(),
		},
		queue.FileInfo{
			Name:   KeysFilename,
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
	}

	return files, func() {
		bigValuePool.Put(bb)
	}
}
