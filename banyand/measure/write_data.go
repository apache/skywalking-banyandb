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

package measure

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type syncPartContext struct {
	tsTable *tsTable
	writers *writers
	memPart *memPart
}

func (s *syncPartContext) FinishSync() error {
	s.tsTable.mustAddMemPart(s.memPart)
	return s.Close()
}

func (s *syncPartContext) Close() error {
	s.writers.MustClose()
	releaseWriters(s.writers)
	s.writers = nil
	s.memPart = nil
	s.tsTable = nil
	return nil
}

type syncCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpChunkedSyncCallback(l *logger.Logger, schemaRepo *schemaRepo) queue.ChunkedSyncHandler {
	return &syncCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (s *syncCallback) CheckHealth() *common.Error {
	return nil
}

// CreatePartHandler implements queue.ChunkedSyncHandler.
func (s *syncCallback) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	tsdb, err := s.schemaRepo.loadTSDB(ctx.Group)
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to load TSDB for group")
		return nil, err
	}
	segmentTime := time.Unix(0, ctx.MinTimestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(segmentTime)
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Time("segmentTime", segmentTime).Msg("failed to create segment")
		return nil, err
	}
	defer segment.DecRef()
	tsTable, err := segment.CreateTSTableIfNotExist(common.ShardID(ctx.ShardID))
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Uint32("shardID", ctx.ShardID).Msg("failed to create ts table")
		return nil, err
	}

	tsdb.Tick(ctx.MaxTimestamp)
	memPart := generateMemPart()
	memPart.partMetadata.CompressedSizeBytes = ctx.CompressedSizeBytes
	memPart.partMetadata.UncompressedSizeBytes = ctx.UncompressedSizeBytes
	memPart.partMetadata.TotalCount = ctx.TotalCount
	memPart.partMetadata.BlocksCount = ctx.BlocksCount
	memPart.partMetadata.MinTimestamp = ctx.MinTimestamp
	memPart.partMetadata.MaxTimestamp = ctx.MaxTimestamp
	writers := generateWriters()
	writers.mustInitForMemPart(memPart)
	return &syncPartContext{
		tsTable: tsTable,
		writers: writers,
		memPart: memPart,
	}, nil
}

// HandleFileChunk implements queue.ChunkedSyncHandler for streaming file chunks.
func (s *syncCallback) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	partCtx := ctx.Handler.(*syncPartContext)

	// Select the appropriate writer based on the filename and write the chunk.
	fileName := ctx.FileName
	switch {
	case fileName == measureMetaName:
		partCtx.writers.metaWriter.MustWrite(chunk)
	case fileName == measurePrimaryName:
		partCtx.writers.primaryWriter.MustWrite(chunk)
	case fileName == measureTimestampsName:
		partCtx.writers.timestampsWriter.MustWrite(chunk)
	case fileName == measureFieldValuesName:
		partCtx.writers.fieldValuesWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, measureTagFamiliesPrefix):
		tagName := fileName[len(measureTagFamiliesPrefix):]
		_, tagWriter := partCtx.writers.getColumnMetadataWriterAndColumnWriter(tagName)
		tagWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, measureTagMetadataPrefix):
		tagName := fileName[len(measureTagMetadataPrefix):]
		tagMetadataWriter, _ := partCtx.writers.getColumnMetadataWriterAndColumnWriter(tagName)
		tagMetadataWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Msg("unknown file type in chunked sync")
		return fmt.Errorf("unknown file type: %s", fileName)
	}

	return nil
}

// HandlePartComplete implements queue.ChunkedSyncHandler for part completion.
func (s *syncCallback) HandlePartComplete(ctx *queue.ChunkedSyncPartContext) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	partCtx := ctx.Handler.(*syncPartContext)
	return partCtx.FinishSync()
}

func generateWriters() *writers {
	v := writersPool.Get()
	if v == nil {
		return &writers{
			tagFamilyMetadataWriters: make(map[string]*writer),
			tagFamilyWriters:         make(map[string]*writer),
		}
	}
	return v
}

func releaseWriters(sw *writers) {
	sw.reset()
	writersPool.Put(sw)
}

var writersPool = pool.Register[*writers]("measure-writers")

func (sw *writers) mustInitForMemPart(mp *memPart) {
	sw.reset()
	sw.mustCreateTagFamilyWriters = mp.mustCreateMemTagFamilyWriters
	sw.metaWriter.init(&mp.meta)
	sw.primaryWriter.init(&mp.primary)
	sw.timestampsWriter.init(&mp.timestamps)
	sw.fieldValuesWriter.init(&mp.fieldValues)
}
