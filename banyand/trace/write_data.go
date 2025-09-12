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
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type syncPartContext struct {
	tsTable         *tsTable
	l               *logger.Logger
	writers         *writers
	memPart         *memPart
	sidxPartContext *sidx.PartContext
}

func (s *syncPartContext) FinishSync() error {
	if s.memPart != nil {
		s.tsTable.mustAddMemPart(s.memPart)
	}
	if s.sidxPartContext != nil {
		for sidxName, memPart := range s.sidxPartContext.GetMemParts() {
			partPath := fmt.Sprintf("part-%d", memPart.ID())
			fullPartPath := filepath.Join(s.tsTable.root, "sidx", sidxName, partPath)
			memPart.MustFlush(s.tsTable.fileSystem, fullPartPath)

			if s.l != nil {
				s.l.Debug().
					Str("sidxName", sidxName).
					Str("partPath", fullPartPath).
					Msg("flushed sidx memPart to disk")
			}
		}
	}
	return s.Close()
}

func (s *syncPartContext) Close() error {
	if s.writers != nil {
		s.writers.MustClose()
		releaseWriters(s.writers)
		s.writers = nil
	}
	if s.memPart != nil {
		s.memPart = nil
	}
	if s.sidxPartContext != nil {
		s.sidxPartContext.Close()
		s.sidxPartContext = nil
	}
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

	if ctx.PartType != PartTypeCore {
		s.l.Debug().
			Str("group", ctx.Group).
			Uint32("shardID", ctx.ShardID).
			Uint64("partID", ctx.ID).
			Str("partType", ctx.PartType).
			Msg("creating unified part handler for sidx data")
		sidxNames := tsTable.getAllSidxNames()
		sidxMemParts := sidx.NewSidxMemParts()
		for _, sidxName := range sidxNames {
			memPart := sidx.GenerateMemPart()
			memPart.SetPartMetadata(ctx.CompressedSizeBytes, ctx.UncompressedSizeBytes, ctx.TotalCount, ctx.BlocksCount, ctx.MinKey, ctx.MaxKey, ctx.ID)
			writers := sidx.GenerateWriters()
			writers.MustInitForMemPart(memPart)
			sidxMemParts.Set(sidxName, memPart, writers)
			s.l.Debug().
				Str("sidxName", sidxName).
				Uint64("partID", ctx.ID).
				Msg("pre-created sidx memPart and writers")
		}
		return &syncPartContext{
			tsTable:         tsTable,
			l:               s.l,
			sidxPartContext: sidxMemParts,
		}, nil
	}

	memPart := generateMemPart()
	memPart.partMetadata.CompressedSizeBytes = ctx.CompressedSizeBytes
	memPart.partMetadata.UncompressedSpanSizeBytes = ctx.UncompressedSizeBytes
	memPart.partMetadata.TotalCount = ctx.TotalCount
	memPart.partMetadata.BlocksCount = ctx.BlocksCount
	memPart.partMetadata.MinTimestamp = ctx.MinTimestamp
	memPart.partMetadata.MaxTimestamp = ctx.MaxTimestamp
	memPart.partMetadata.ID = ctx.ID
	writers := generateWriters()
	writers.mustInitForMemPart(memPart)
	return &syncPartContext{
		tsTable: tsTable,
		l:       s.l,
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
	fileName := ctx.FileName
	if ctx.PartType != PartTypeCore {
		return s.handleSidxFileChunk(partCtx, ctx, chunk)
	}
	return s.handleTraceFileChunk(partCtx, fileName, chunk)
}

func (s *syncCallback) handleSidxFileChunk(partCtx *syncPartContext, ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	sidxName := ctx.PartType
	fileName := ctx.FileName
	writers, exists := partCtx.sidxPartContext.GetWritersByName(sidxName)
	if !exists {
		return fmt.Errorf("sidx memPart not found for sidx name: %s", sidxName)
	}
	switch {
	case fileName == sidx.PrimaryFilename:
		writers.SidxPrimaryWriter().MustWrite(chunk)
	case fileName == sidx.DataFilename:
		writers.SidxDataWriter().MustWrite(chunk)
	case fileName == sidx.KeysFilename:
		writers.SidxKeysWriter().MustWrite(chunk)
	case fileName == sidx.MetaFilename:
		writers.SidxMetaWriter().MustWrite(chunk)
	case strings.HasPrefix(fileName, sidx.TagDataExtension):
		tagName := fileName[len(sidx.TagDataPrefix):]
		_, tagDataWriter, _ := writers.GetTagWriters(tagName)
		tagDataWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, sidx.TagMetadataExtension):
		tagName := fileName[len(sidx.TagMetadataPrefix):]
		tagMetadataWriter, _, _ := writers.GetTagWriters(tagName)
		tagMetadataWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Str("sidxName", sidxName).Msg("unknown sidx file type")
		return fmt.Errorf("unknown sidx file type: %s for sidx: %s", fileName, sidxName)
	}
	return nil
}

func (s *syncCallback) handleTraceFileChunk(partCtx *syncPartContext, fileName string, chunk []byte) error {
	switch {
	case fileName == metaFilename:
		partCtx.writers.metaWriter.MustWrite(chunk)
	case fileName == primaryFilename:
		partCtx.writers.primaryWriter.MustWrite(chunk)
	case fileName == spansFilename:
		partCtx.writers.spanWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, traceTagsPrefix):
		tagName := fileName[len(traceTagsPrefix):]
		_, tagWriter := partCtx.writers.getWriters(tagName)
		tagWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, traceTagMetadataPrefix):
		tagName := fileName[len(traceTagMetadataPrefix):]
		tagMetadataWriter, _ := partCtx.writers.getWriters(tagName)
		tagMetadataWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Msg("unknown file type in chunked sync")
		return fmt.Errorf("unknown file type: %s", fileName)
	}
	return nil
}
