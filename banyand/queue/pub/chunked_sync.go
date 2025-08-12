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

package pub

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	defaultChunkSize = 1024 * 1024
	maxRetries       = 3
	retryInterval    = 100 * time.Millisecond
)

type chunkedSyncClient struct {
	client    clusterv1.ServiceClient
	conn      *grpc.ClientConn
	log       *logger.Logger
	config    *ChunkedSyncClientConfig
	node      string
	chunkSize uint32
}

// SyncStreamingParts implements queue.ChunkedSyncClient with streaming support.
func (c *chunkedSyncClient) SyncStreamingParts(ctx context.Context, parts []queue.StreamingPartData) (*queue.SyncResult, error) {
	if len(parts) == 0 {
		return &queue.SyncResult{
			Success:     true,
			SessionID:   "",
			PartsCount:  0,
			ChunksCount: 0,
		}, nil
	}
	defer func() {
		for _, part := range parts {
			for _, file := range part.Files {
				fs.MustClose(file.Reader)
			}
		}
	}()

	sessionID := generateSessionID()

	chunkedClient := clusterv1.NewChunkedSyncServiceClient(c.conn)

	stream, err := chunkedClient.SyncPart(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync stream: %w", err)
	}
	defer func() {
		if closeErr := stream.CloseSend(); closeErr != nil {
			c.log.Error().Err(closeErr).Msg("failed to close send stream")
		}
	}()

	startTime := time.Now()

	metadata := &clusterv1.SyncMetadata{
		Group:      parts[0].Group,
		ShardId:    parts[0].ShardID,
		Topic:      parts[0].Topic,
		Timestamp:  startTime.UnixMilli(),
		TotalParts: uint32(len(parts)),
	}

	var totalBytesSent uint64

	totalChunks, err := c.streamPartsAsChunks(stream, sessionID, metadata, parts, &totalBytesSent)
	if err != nil {
		return nil, fmt.Errorf("failed to stream parts: %w", err)
	}
	if totalChunks == 0 {
		return &queue.SyncResult{
			Success:    true,
			SessionID:  sessionID,
			PartsCount: uint32(len(parts)),
		}, nil
	}

	var finalResp *clusterv1.SyncPartResponse
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive final response: %w", err)
		}
		finalResp = resp
		if resp.GetSyncResult() != nil {
			break
		}
	}

	duration := time.Since(startTime)
	success := false

	if finalResp != nil && finalResp.GetSyncResult() != nil {
		result := finalResp.GetSyncResult()
		success = result.Success
	}

	return &queue.SyncResult{
		Success:     success,
		SessionID:   sessionID,
		TotalBytes:  totalBytesSent,
		DurationMs:  duration.Milliseconds(),
		ChunksCount: totalChunks,
		PartsCount:  uint32(len(parts)),
	}, nil
}

// Close implements queue.ChunkedSyncClient.
func (c *chunkedSyncClient) Close() error {
	return nil
}

func (c *chunkedSyncClient) streamPartsAsChunks(
	stream clusterv1.ChunkedSyncService_SyncPartClient,
	sessionID string,
	metadata *clusterv1.SyncMetadata,
	parts []queue.StreamingPartData,
	totalBytesSent *uint64,
) (uint32, error) {
	var totalChunks uint32
	var chunkIndex uint32
	isFirstChunk := true

	buffer := make([]byte, 0, c.chunkSize)

	type fileState struct {
		info       queue.FileInfo
		partIndex  int
		fileIndex  int
		filesCount int
		bytesRead  uint64
		finished   bool
	}

	type chunkFileInfo struct {
		fileInfo    *clusterv1.FileInfo
		filePartIdx int
	}

	var fileStates []*fileState

	for partIdx, part := range parts {
		for fileIdx, file := range part.Files {
			fileStates = append(fileStates, &fileState{
				partIndex:  partIdx,
				fileIndex:  fileIdx,
				filesCount: len(part.Files),
				info:       file,
				bytesRead:  0,
				finished:   false,
			})
		}
	}

	currentFileIdx := 0

	for currentFileIdx < len(fileStates) {
		var chunkFileInfos []*chunkFileInfo

		for len(buffer) < cap(buffer) && currentFileIdx < len(fileStates) {
			fileState := fileStates[currentFileIdx]
			if fileState.finished {
				currentFileIdx++
				continue
			}

			availableSpace := cap(buffer) - len(buffer)
			if availableSpace == 0 {
				break
			}

			fileStartInChunk := len(buffer)
			originalLen := len(buffer)
			buffer = buffer[:originalLen+availableSpace]

			n, err := fileState.info.Reader.Read(buffer[originalLen:])
			buffer = buffer[:originalLen+n]

			if errors.Is(err, io.EOF) {
				fileState.finished = true
				currentFileIdx++
			} else if err != nil {
				return totalChunks, fmt.Errorf("failed to read from file %s: %w", fileState.info.Name, err)
			}

			if n > 0 {
				fileState.bytesRead += uint64(n)

				chunkFileInfos = append(chunkFileInfos, &chunkFileInfo{
					fileInfo: &clusterv1.FileInfo{
						Name:   fileState.info.Name,
						Offset: uint32(fileStartInChunk),
						Size:   uint32(n),
					},
					filePartIdx: fileState.partIndex,
				})
			}
		}

		if len(buffer) > 0 {
			var chunkPartsInfo []*clusterv1.PartInfo
			var currentPartInfo *clusterv1.PartInfo
			currentPartIdx := -1

			for _, chunkFile := range chunkFileInfos {
				filePartIdx := chunkFile.filePartIdx

				if filePartIdx != currentPartIdx {
					if currentPartInfo != nil {
						chunkPartsInfo = append(chunkPartsInfo, currentPartInfo)
					}

					originalPart := parts[filePartIdx]
					currentPartInfo = &clusterv1.PartInfo{
						Id:                    originalPart.ID,
						Files:                 []*clusterv1.FileInfo{},
						CompressedSizeBytes:   originalPart.CompressedSizeBytes,
						UncompressedSizeBytes: originalPart.UncompressedSizeBytes,
						TotalCount:            originalPart.TotalCount,
						BlocksCount:           originalPart.BlocksCount,
						MinTimestamp:          originalPart.MinTimestamp,
						MaxTimestamp:          originalPart.MaxTimestamp,
					}
					currentPartIdx = filePartIdx
				}

				currentPartInfo.Files = append(currentPartInfo.Files, chunkFile.fileInfo)
			}

			if currentPartInfo != nil {
				chunkPartsInfo = append(chunkPartsInfo, currentPartInfo)
			}

			if err := c.sendChunk(stream, sessionID, buffer, chunkPartsInfo, &chunkIndex, &totalChunks, totalBytesSent, isFirstChunk, metadata); err != nil {
				return totalChunks, err
			}
			isFirstChunk = false
			buffer = buffer[:0]
		}

		if len(buffer) == 0 && currentFileIdx >= len(fileStates) {
			break
		}
	}

	if totalChunks > 0 {
		completionReq := &clusterv1.SyncPartRequest{
			SessionId:  sessionID,
			ChunkIndex: chunkIndex + 1,
			Content: &clusterv1.SyncPartRequest_Completion{
				Completion: &clusterv1.SyncCompletion{
					TotalBytesSent: *totalBytesSent,
					TotalPartsSent: uint32(len(parts)),
					TotalChunks:    totalChunks,
				},
			},
		}

		if err := stream.Send(completionReq); err != nil {
			return totalChunks, fmt.Errorf("failed to send completion: %w", err)
		}
		totalChunks++
	}

	return totalChunks, nil
}

func (c *chunkedSyncClient) sendChunk(
	stream clusterv1.ChunkedSyncService_SyncPartClient,
	sessionID string,
	chunkData []byte,
	partsInfo []*clusterv1.PartInfo,
	chunkIndex *uint32,
	totalChunks *uint32,
	totalBytesSent *uint64,
	isFirstChunk bool,
	metadata *clusterv1.SyncMetadata,
) error {
	chunkChecksum := fmt.Sprintf("%x", crc32.ChecksumIEEE(chunkData))

	retryCount := 0
	for {
		req := &clusterv1.SyncPartRequest{
			SessionId:     sessionID,
			ChunkIndex:    *chunkIndex,
			ChunkData:     chunkData,
			ChunkChecksum: chunkChecksum,
			PartsInfo:     partsInfo,
		}

		if isFirstChunk {
			req.Content = &clusterv1.SyncPartRequest_Metadata{
				Metadata: metadata,
			}
		}

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("failed to send chunk %d: %w", *chunkIndex, err)
		}

		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive response for chunk %d: %w", *chunkIndex, err)
		}

		switch resp.Status {
		case clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED:
			*totalBytesSent += uint64(len(chunkData))
			*chunkIndex++
			*totalChunks++
			return nil

		case clusterv1.SyncStatus_SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH:
			retryCount++
			if retryCount > maxRetries {
				return fmt.Errorf("chunk %d checksum mismatch after %d retries", *chunkIndex, maxRetries)
			}
			c.log.Warn().Int("retry_count", retryCount).Uint32("chunk_index", *chunkIndex).Msg("chunk checksum mismatch, retrying")
			time.Sleep(retryInterval * time.Duration(retryCount))
			continue

		case clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER:
			if err := c.handleOutOfOrderResponse(resp, *chunkIndex, &retryCount); err != nil {
				return err
			}
			if strings.Contains(resp.Error, "buffered") {
				*totalBytesSent += uint64(len(chunkData))
				*chunkIndex++
				*totalChunks++
				return nil
			}
			continue

		case clusterv1.SyncStatus_SYNC_STATUS_SESSION_NOT_FOUND:
			return fmt.Errorf("session %s not found on server for chunk %d: %s", sessionID, *chunkIndex, resp.Error)

		default:
			if resp.Error != "" {
				return fmt.Errorf("chunk %d sync failed: %s", *chunkIndex, resp.Error)
			}
			return fmt.Errorf("chunk %d sync failed with status: %v", *chunkIndex, resp.Status)
		}
	}
}

func (c *chunkedSyncClient) handleOutOfOrderResponse(resp *clusterv1.SyncPartResponse, chunkIndex uint32, retryCount *int) error {
	config := c.config
	if config == nil {
		config = &ChunkedSyncClientConfig{
			EnableRetryOnOOO: true,
			MaxOOORetries:    3,
			OOORetryDelay:    100 * time.Millisecond,
		}
	}

	if !config.EnableRetryOnOOO {
		return fmt.Errorf("chunk %d out of order: %s", chunkIndex, resp.Error)
	}

	if strings.Contains(resp.Error, "buffered") {
		c.log.Info().
			Uint32("chunk_index", chunkIndex).
			Str("server_message", resp.Error).
			Msg("chunk was buffered by server due to reordering")
		return nil
	}

	if strings.Contains(resp.Error, "gap too large") || strings.Contains(resp.Error, "buffer full") {
		return fmt.Errorf("unrecoverable out-of-order error for chunk %d: %s", chunkIndex, resp.Error)
	}

	*retryCount++
	if *retryCount > config.MaxOOORetries {
		return fmt.Errorf("chunk %d out of order after %d retries: %s",
			chunkIndex, config.MaxOOORetries, resp.Error)
	}

	c.log.Warn().
		Int("retry_count", *retryCount).
		Uint32("chunk_index", chunkIndex).
		Str("error", resp.Error).
		Msg("retrying out-of-order chunk")

	time.Sleep(config.OOORetryDelay * time.Duration(*retryCount))
	return nil
}

func generateSessionID() string {
	return fmt.Sprintf("sync-%d", time.Now().UnixNano())
}
