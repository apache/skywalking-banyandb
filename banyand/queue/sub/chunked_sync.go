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

package sub

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

func checkSyncVersionCompatibility(versionInfo *clusterv1.VersionInfo) (*clusterv1.VersionCompatibility, clusterv1.SyncStatus) {
	if versionInfo == nil {
		return &clusterv1.VersionCompatibility{
			Supported:                   true,
			ServerApiVersion:            apiversion.Version,
			SupportedApiVersions:        []string{apiversion.Version},
			ServerFileFormatVersion:     storage.GetCurrentVersion(),
			SupportedFileFormatVersions: storage.GetCompatibleVersions(),
			Reason:                      "No version info provided, assuming compatible",
		}, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED
	}

	serverAPIVersion := apiversion.Version
	serverFileFormatVersion := storage.GetCurrentVersion()
	compatibleFileFormatVersions := storage.GetCompatibleVersions()

	// Check API version compatibility
	apiCompatible := versionInfo.ApiVersion == serverAPIVersion

	// Check file format version compatibility
	fileFormatCompatible := false
	if versionInfo.FileFormatVersion == serverFileFormatVersion {
		fileFormatCompatible = true
	} else {
		// Check if client's file format version is in our compatible list
		for _, compatVer := range compatibleFileFormatVersions {
			if compatVer == versionInfo.FileFormatVersion {
				fileFormatCompatible = true
				break
			}
		}
	}

	versionCompatibility := &clusterv1.VersionCompatibility{
		ServerApiVersion:            serverAPIVersion,
		SupportedApiVersions:        []string{serverAPIVersion},
		ServerFileFormatVersion:     serverFileFormatVersion,
		SupportedFileFormatVersions: compatibleFileFormatVersions,
	}

	switch {
	case !apiCompatible && !fileFormatCompatible:
		versionCompatibility.Supported = false
		versionCompatibility.Reason = fmt.Sprintf("API version %s not supported (server: %s) and file format version %s not compatible (server: %s, supported: %v)",
			versionInfo.ApiVersion, serverAPIVersion, versionInfo.FileFormatVersion, serverFileFormatVersion, compatibleFileFormatVersions)
		return versionCompatibility, clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED
	case !apiCompatible:
		versionCompatibility.Supported = false
		versionCompatibility.Reason = fmt.Sprintf("API version %s not supported (server: %s)", versionInfo.ApiVersion, serverAPIVersion)
		return versionCompatibility, clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED
	case !fileFormatCompatible:
		versionCompatibility.Supported = false
		versionCompatibility.Reason = fmt.Sprintf("File format version %s not compatible (server: %s, supported: %v)",
			versionInfo.FileFormatVersion, serverFileFormatVersion, compatibleFileFormatVersions)
		return versionCompatibility, clusterv1.SyncStatus_SYNC_STATUS_FORMAT_VERSION_MISMATCH
	}

	versionCompatibility.Supported = true
	versionCompatibility.Reason = "Client version compatible with server"
	return versionCompatibility, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED
}

type chunkBuffer struct {
	chunks        map[uint32]*clusterv1.SyncPartRequest
	lastActivity  time.Time
	bufferTimeout time.Duration
	expectedIndex uint32
	maxBufferSize uint32
}

type syncSession struct {
	startTime      time.Time
	metadata       *clusterv1.SyncMetadata
	partsProgress  map[int]*partProgress
	partCtx        *queue.ChunkedSyncPartContext
	chunkBuffer    *chunkBuffer
	sessionID      string
	errorMsg       string
	totalReceived  uint64
	chunksReceived uint32
	completed      bool
}

type partProgress struct {
	totalBytes    uint32
	receivedBytes uint32
	completed     bool
}

// SyncPart implements clusterv1.ChunkedSyncServiceServer.
func (s *server) SyncPart(stream clusterv1.ChunkedSyncService_SyncPartServer) error {
	ctx := stream.Context()
	var currentSession *syncSession
	var sessionID string
	defer func() {
		if currentSession != nil {
			if currentSession.partCtx != nil {
				currentSession.partCtx.Close()
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.log.Error().Err(err).Msg("failed to receive chunk")
			return err
		}

		sessionID = req.SessionId

		if req.GetMetadata() != nil {
			currentSession = &syncSession{
				sessionID:      sessionID,
				metadata:       req.GetMetadata(),
				startTime:      time.Now(),
				chunksReceived: 0,
				partsProgress:  make(map[int]*partProgress),
			}
			s.log.Info().Str("session_id", sessionID).
				Str("topic", req.GetMetadata().Topic).
				Uint32("total_parts", req.GetMetadata().TotalParts).
				Msg("started chunked sync session")
		}

		if currentSession == nil {
			errMsg := fmt.Sprintf("session %s not found", sessionID)
			s.log.Error().Str("session_id", sessionID).Msg(errMsg)
			return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_SESSION_NOT_FOUND, errMsg, nil)
		}

		if req.GetCompletion() != nil {
			return s.handleCompletion(stream, currentSession, req)
		}

		if err := s.processChunk(stream, currentSession, req); err != nil {
			s.log.Error().Err(err).Str("session_id", sessionID).Msg("failed to process chunk")
			return err
		}
	}

	return nil
}

func (s *server) processChunk(stream clusterv1.ChunkedSyncService_SyncPartServer, session *syncSession, req *clusterv1.SyncPartRequest) error {
	// Check version compatibility on every chunk
	if req.VersionInfo != nil {
		versionCompatibility, status := checkSyncVersionCompatibility(req.VersionInfo)
		if status != clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED {
			s.log.Warn().
				Str("session_id", req.SessionId).
				Str("client_api_version", req.VersionInfo.ApiVersion).
				Str("client_file_format_version", req.VersionInfo.FileFormatVersion).
				Str("reason", versionCompatibility.Reason).
				Msg("sync version compatibility check failed")

			return s.sendResponse(stream, req, status, versionCompatibility.Reason, versionCompatibility)
		}
	}

	if !s.enableChunkReordering {
		return s.processChunkSequential(stream, session, req)
	}

	if session.chunkBuffer == nil {
		session.chunkBuffer = &chunkBuffer{
			chunks:        make(map[uint32]*clusterv1.SyncPartRequest),
			expectedIndex: 0,
			maxBufferSize: s.maxChunkBufferSize,
			bufferTimeout: s.chunkBufferTimeout,
			lastActivity:  time.Now(),
		}
	}

	return s.processChunkWithReordering(stream, session, req)
}

func (s *server) processChunkSequential(stream clusterv1.ChunkedSyncService_SyncPartServer, session *syncSession, req *clusterv1.SyncPartRequest) error {
	if req.ChunkIndex != session.chunksReceived {
		errMsg := fmt.Sprintf("out of order chunk received: expected %d, got %d",
			session.chunksReceived, req.ChunkIndex)
		s.log.Warn().Str("session_id", req.SessionId).
			Uint32("expected_chunk", session.chunksReceived).
			Uint32("received_chunk", req.ChunkIndex).
			Msg("out of order chunk received")
		return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER, errMsg, nil)
	}

	return s.processExpectedChunk(stream, session, req)
}

func (s *server) processChunkWithReordering(stream clusterv1.ChunkedSyncService_SyncPartServer, session *syncSession, req *clusterv1.SyncPartRequest) error {
	buffer := session.chunkBuffer
	buffer.lastActivity = time.Now()

	if req.ChunkIndex == buffer.expectedIndex {
		if err := s.processExpectedChunk(stream, session, req); err != nil {
			return err
		}
		buffer.expectedIndex++

		return s.processBufferedChunks(stream, session)
	}

	if req.ChunkIndex > buffer.expectedIndex {
		gap := req.ChunkIndex - buffer.expectedIndex
		s.updateChunkOrderMetrics("out_of_order_received", req.SessionId)

		if gap > s.maxChunkGapSize {
			errMsg := fmt.Sprintf("chunk gap too large: expected %d, got %d (gap: %d > max: %d)",
				buffer.expectedIndex, req.ChunkIndex, gap, s.maxChunkGapSize)
			s.log.Warn().Str("session_id", req.SessionId).
				Uint32("expected_chunk", buffer.expectedIndex).
				Uint32("received_chunk", req.ChunkIndex).
				Uint32("gap", gap).
				Uint32("max_gap", s.maxChunkGapSize).
				Msg("chunk gap too large, rejecting")
			s.updateChunkOrderMetrics("gap_too_large", req.SessionId)
			return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER, errMsg, nil)
		}

		if uint32(len(buffer.chunks)) >= buffer.maxBufferSize {
			errMsg := fmt.Sprintf("chunk buffer full, cannot store out-of-order chunk %d", req.ChunkIndex)
			s.log.Warn().Str("session_id", req.SessionId).
				Uint32("chunk_index", req.ChunkIndex).
				Uint32("buffer_size", uint32(len(buffer.chunks))).
				Uint32("max_buffer_size", buffer.maxBufferSize).
				Msg("chunk buffer full, rejecting chunk")
			s.updateChunkOrderMetrics("buffer_full", req.SessionId)
			return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER, errMsg, nil)
		}

		buffer.chunks[req.ChunkIndex] = req
		s.log.Info().Str("session_id", req.SessionId).
			Uint32("chunk_index", req.ChunkIndex).
			Uint32("expected_index", buffer.expectedIndex).
			Uint32("buffered_chunks", uint32(len(buffer.chunks))).
			Msg("buffered out-of-order chunk")
		s.updateChunkOrderMetrics("chunk_buffered", req.SessionId)

		return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
			fmt.Sprintf("chunk %d buffered (waiting for %d)", req.ChunkIndex, buffer.expectedIndex), nil)
	}

	if req.ChunkIndex < buffer.expectedIndex {
		s.log.Warn().
			Str("session_id", req.SessionId).
			Uint32("chunk_index", req.ChunkIndex).
			Uint32("expected_index", buffer.expectedIndex).
			Msg("received duplicate or old chunk, ignoring")

		return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
			fmt.Sprintf("duplicate chunk %d ignored", req.ChunkIndex), nil)
	}

	return nil
}

func (s *server) processExpectedChunk(stream clusterv1.ChunkedSyncService_SyncPartServer, session *syncSession, req *clusterv1.SyncPartRequest) error {
	calculatedChecksum := fmt.Sprintf("%x", crc32.ChecksumIEEE(req.ChunkData))
	if calculatedChecksum != req.ChunkChecksum {
		errMsg := fmt.Sprintf("chunk %d checksum mismatch: expected %s, got %s",
			req.ChunkIndex, req.ChunkChecksum, calculatedChecksum)
		s.log.Warn().Str("session_id", req.SessionId).Msg(errMsg)
		return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH, errMsg, nil)
	}

	session.totalReceived += uint64(len(req.ChunkData))
	session.chunksReceived++

	var topic bus.Topic
	t, ok := data.TopicMap[session.metadata.Topic]
	if !ok {
		return fmt.Errorf("unknown sync topic: %s", session.metadata.Topic)
	}
	topic = t

	handler, exists := s.chunkedSyncHandlers[topic]
	if !exists {
		return fmt.Errorf("no handler registered for topic %s", topic)
	}

	for partIndex, partInfo := range req.PartsInfo {
		if session.partCtx != nil && (session.partCtx.ID != partInfo.Id || session.partCtx.PartType != partInfo.PartType) {
			if session.partCtx.Handler != nil {
				if err := session.partCtx.Handler.FinishSync(); err != nil {
					return fmt.Errorf("failed to complete part %d: %w", session.partCtx.ID, err)
				}
			}
		}

		session.partCtx = &queue.ChunkedSyncPartContext{
			ID:                    partInfo.Id,
			Group:                 session.metadata.Group,
			ShardID:               session.metadata.ShardId,
			CompressedSizeBytes:   partInfo.CompressedSizeBytes,
			UncompressedSizeBytes: partInfo.UncompressedSizeBytes,
			TotalCount:            partInfo.TotalCount,
			BlocksCount:           partInfo.BlocksCount,
			MinTimestamp:          partInfo.MinTimestamp,
			MaxTimestamp:          partInfo.MaxTimestamp,
			MinKey:                partInfo.MinKey,
			MaxKey:                partInfo.MaxKey,
			PartType:              partInfo.PartType,
		}
		partHandler, err := handler.CreatePartHandler(session.partCtx)
		if err != nil {
			return fmt.Errorf("failed to create part handler: %w", err)
		}
		session.partCtx.Handler = partHandler

		if err := s.processPart(session, req, partInfo, partIndex, handler); err != nil {
			s.log.Error().Err(err).
				Str("session_id", req.SessionId).
				Int("part_index", partIndex).
				Msg("failed to process part")
			return err
		}
	}

	return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, "", nil)
}

func (s *server) processBufferedChunks(stream clusterv1.ChunkedSyncService_SyncPartServer, session *syncSession) error {
	buffer := session.chunkBuffer

	for {
		if chunk, exists := buffer.chunks[buffer.expectedIndex]; exists {
			delete(buffer.chunks, buffer.expectedIndex)

			s.log.Debug().Str("session_id", session.sessionID).
				Uint32("chunk_index", buffer.expectedIndex).
				Uint32("remaining_buffered", uint32(len(buffer.chunks))).
				Msg("processing buffered chunk")

			if err := s.processExpectedChunk(stream, session, chunk); err != nil {
				return err
			}
			buffer.expectedIndex++
		} else {
			break
		}
	}

	return nil
}

func (s *server) checkBufferTimeout(session *syncSession) error {
	if session.chunkBuffer == nil {
		return nil
	}

	if time.Since(session.chunkBuffer.lastActivity) > session.chunkBuffer.bufferTimeout {
		if len(session.chunkBuffer.chunks) > 0 {
			missing := make([]uint32, 0)
			for i := session.chunkBuffer.expectedIndex; i < session.chunkBuffer.expectedIndex+10; i++ {
				if _, exists := session.chunkBuffer.chunks[i]; !exists {
					missing = append(missing, i)
				}
			}

			return fmt.Errorf("buffer timeout: missing chunks %v after %v",
				missing, session.chunkBuffer.bufferTimeout)
		}
	}

	return nil
}

func (s *server) processPart(session *syncSession, req *clusterv1.SyncPartRequest, partInfo *clusterv1.PartInfo, partIndex int, handler queue.ChunkedSyncHandler) error {
	var totalPartSizeInChunk uint32
	for _, fileInfo := range partInfo.Files {
		totalPartSizeInChunk += fileInfo.Size
	}
	progress := session.partsProgress[partIndex]
	if progress == nil {
		session.partsProgress[partIndex] = &partProgress{
			totalBytes:    totalPartSizeInChunk,
			receivedBytes: 0,
			completed:     false,
		}
		progress = session.partsProgress[partIndex]
	} else {
		progress.completed = false
		progress.totalBytes += totalPartSizeInChunk
	}

	var partDataSize uint32
	for _, fileInfo := range partInfo.Files {
		if fileInfo.Offset >= uint32(len(req.ChunkData)) {
			s.log.Warn().Str("session_id", session.sessionID).
				Str("file_name", fileInfo.Name).
				Uint32("offset", fileInfo.Offset).
				Uint32("chunk_size", uint32(len(req.ChunkData))).
				Msg("file data not in this chunk")
			continue
		}

		fileEndOffset := fileInfo.Offset + fileInfo.Size
		actualFileSize := fileInfo.Size
		if fileEndOffset > uint32(len(req.ChunkData)) {
			actualFileSize = uint32(len(req.ChunkData)) - fileInfo.Offset
		}

		fileChunk := req.ChunkData[fileInfo.Offset : fileInfo.Offset+actualFileSize]
		partDataSize += actualFileSize

		session.partCtx.FileName = fileInfo.Name
		session.partCtx.PartType = partInfo.PartType

		if err := handler.HandleFileChunk(session.partCtx, fileChunk); err != nil {
			return fmt.Errorf("failed to stream file chunk for %s: %w", fileInfo.Name, err)
		}
	}

	progress.receivedBytes += partDataSize
	if progress.receivedBytes > progress.totalBytes {
		return fmt.Errorf("received more bytes (%d) than expected (%d) for part %d",
			progress.receivedBytes, progress.totalBytes, partIndex)
	}

	progress.completed = progress.receivedBytes == progress.totalBytes
	return nil
}

func (s *server) handleCompletion(stream clusterv1.ChunkedSyncService_SyncPartServer, session *syncSession, req *clusterv1.SyncPartRequest) error {
	if session.partCtx.Handler != nil {
		if err := session.partCtx.Handler.FinishSync(); err != nil {
			return fmt.Errorf("failed to complete part %d: %w", session.partCtx.ID, err)
		}
		session.partCtx.Handler = nil
	}

	session.completed = true

	partsResults := make([]*clusterv1.PartResult, 0, len(session.partsProgress))
	allPartsSuccessful := true

	for _, progress := range session.partsProgress {
		success := progress.completed
		if !success {
			allPartsSuccessful = false
		}

		partsResults = append(partsResults, &clusterv1.PartResult{
			Success:        success,
			Error:          session.errorMsg,
			BytesProcessed: progress.receivedBytes,
		})
	}

	syncResult := &clusterv1.SyncResult{
		Success:            allPartsSuccessful,
		TotalBytesReceived: session.totalReceived,
		DurationMs:         time.Since(session.startTime).Milliseconds(),
		ChunksReceived:     session.chunksReceived,
		PartsReceived:      uint32(len(session.partsProgress)),
		PartsResults:       partsResults,
	}

	s.log.Info().
		Str("session_id", session.sessionID).
		Bool("success", syncResult.Success).
		Uint64("bytes_received", syncResult.TotalBytesReceived).
		Int64("duration_ms", syncResult.DurationMs).
		Msg("completed chunked sync session")

	return s.sendResponse(stream, req, clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE, "", syncResult)
}

func (s *server) sendResponse(
	stream clusterv1.ChunkedSyncService_SyncPartServer,
	req *clusterv1.SyncPartRequest,
	status clusterv1.SyncStatus,
	errorMsg string,
	result interface{},
) error {
	resp := &clusterv1.SyncPartResponse{
		SessionId:  req.SessionId,
		ChunkIndex: req.ChunkIndex,
		Status:     status,
		Error:      errorMsg,
	}

	switch r := result.(type) {
	case *clusterv1.SyncResult:
		resp.SyncResult = r
	case *clusterv1.VersionCompatibility:
		resp.VersionCompatibility = r
	}

	return stream.Send(resp)
}
