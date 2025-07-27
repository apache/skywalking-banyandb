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
	"context"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestChunkedSyncOutOfOrderHandling(t *testing.T) {
	// Initialize logger for tests
	err := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	})
	require.NoError(t, err)

	tests := []struct {
		name                  string
		enableChunkReordering bool
		maxChunkBufferSize    uint32
		maxChunkGapSize       uint32
		chunkSequence         []uint32
		expectedStatus        []clusterv1.SyncStatus
		expectedBuffered      int
	}{
		{
			name:                  "strict_sequential_mode_rejects_out_of_order",
			enableChunkReordering: false,
			chunkSequence:         []uint32{0, 2}, // Missing chunk 1
			expectedStatus:        []clusterv1.SyncStatus{clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER},
			expectedBuffered:      0,
		},
		{
			name:                  "reordering_mode_buffers_out_of_order_chunks",
			enableChunkReordering: true,
			maxChunkBufferSize:    10,
			maxChunkGapSize:       5,
			chunkSequence:         []uint32{0, 2, 1}, // Chunk 2 arrives before 1
			expectedStatus:        []clusterv1.SyncStatus{clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED},
			expectedBuffered:      1, // Chunk 2 gets buffered initially
		},
		{
			name:                  "large_gap_rejected",
			enableChunkReordering: true,
			maxChunkBufferSize:    10,
			maxChunkGapSize:       3,
			chunkSequence:         []uint32{0, 5}, // Gap of 5 > maxGapSize of 3
			expectedStatus:        []clusterv1.SyncStatus{clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER},
			expectedBuffered:      0,
		},
		{
			name:                  "buffer_full_rejection",
			enableChunkReordering: true,
			maxChunkBufferSize:    2,
			maxChunkGapSize:       10,
			chunkSequence:         []uint32{0, 3, 4, 5}, // Fill buffer with chunks 3,4 then try to add 5
			expectedStatus:        []clusterv1.SyncStatus{clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER},
			expectedBuffered:      2, // Chunks 3,4 get buffered, 5 rejected
		},
		{
			name:                  "duplicate_chunk_ignored",
			enableChunkReordering: true,
			maxChunkBufferSize:    10,
			maxChunkGapSize:       5,
			chunkSequence:         []uint32{0, 1, 0}, // Duplicate chunk 0
			expectedStatus:        []clusterv1.SyncStatus{clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED},
			expectedBuffered:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock server instance with specified configuration
			s := &server{
				log:                   logger.GetLogger("test-server-" + tt.name),
				chunkedSyncHandlers:   make(map[bus.Topic]queue.ChunkedSyncHandler),
				enableChunkReordering: tt.enableChunkReordering,
				maxChunkBufferSize:    tt.maxChunkBufferSize,
				maxChunkGapSize:       tt.maxChunkGapSize,
			}

			// Register a mock handler
			mockHandler := &MockChunkedSyncHandler{}
			s.chunkedSyncHandlers[data.TopicStreamPartSync] = mockHandler

			// Create mock stream
			mockStream := &MockSyncPartStream{}

			// Create a session
			session := &syncSession{
				sessionID:      "test-session",
				startTime:      time.Now(),
				chunksReceived: 0,
				metadata: &clusterv1.SyncMetadata{
					Topic: data.TopicStreamPartSync.String(),
					Group: "test-group",
				},
				partsProgress: make(map[int]*partProgress),
			}

			// Process chunks in the specified sequence
			bufferedCount := 0
			for i, chunkIndex := range tt.chunkSequence {
				req := &clusterv1.SyncPartRequest{
					SessionId:     session.sessionID,
					ChunkIndex:    chunkIndex,
					ChunkData:     []byte(fmt.Sprintf("test-data-%d", chunkIndex)),
					ChunkChecksum: fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(fmt.Sprintf("test-data-%d", chunkIndex)))),
					PartsInfo: []*clusterv1.PartInfo{
						{
							Id: uint64(chunkIndex),
							Files: []*clusterv1.FileInfo{
								{
									Name:   "test-file",
									Offset: 0,
									Size:   uint32(len(fmt.Sprintf("test-data-%d", chunkIndex))),
								},
							},
						},
					},
				}

				responsesBefore := len(mockStream.sentResponses)
				err := s.processChunk(mockStream, session, req)
				require.NoError(t, err, "processChunk should not return error for chunk %d", chunkIndex)

				// Verify at least one response was sent
				responsesAfter := len(mockStream.sentResponses)
				assert.True(t, responsesAfter > responsesBefore, "Should have sent at least one response for chunk %d", chunkIndex)

				// For the last response sent, check if it matches expected status (if provided)
				if i < len(tt.expectedStatus) {
					lastResp := mockStream.sentResponses[responsesAfter-1]
					assert.Equal(t, tt.expectedStatus[i], lastResp.Status, "Unexpected status for chunk %d", chunkIndex)
				}

				// Count buffered chunks
				if session.chunkBuffer != nil && len(session.chunkBuffer.chunks) > bufferedCount {
					bufferedCount = len(session.chunkBuffer.chunks)
				}
			}

			// Verify expected number of chunks were buffered
			assert.Equal(t, tt.expectedBuffered, bufferedCount, "Unexpected number of buffered chunks")
		})
	}
}

// MockChunkedSyncHandler implements queue.ChunkedSyncHandler for testing
type MockChunkedSyncHandler struct{}

func (m *MockChunkedSyncHandler) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	return &MockChunkedSyncPartHandler{}, nil
}

func (m *MockChunkedSyncHandler) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	return nil
}

// MockChunkedSyncPartHandler implements queue.PartHandler for testing
type MockChunkedSyncPartHandler struct{}

func (m *MockChunkedSyncPartHandler) FinishSync() error {
	return nil
}

func (m *MockChunkedSyncPartHandler) Close() error {
	return nil
}

// MockSyncPartStream implements clusterv1.ChunkedSyncService_SyncPartServer for testing
type MockSyncPartStream struct {
	sentResponses []*clusterv1.SyncPartResponse
	ctx           context.Context
}

func (m *MockSyncPartStream) Send(resp *clusterv1.SyncPartResponse) error {
	m.sentResponses = append(m.sentResponses, resp)
	return nil
}

func (m *MockSyncPartStream) Recv() (*clusterv1.SyncPartRequest, error) {
	// Not needed for these tests, just return nil
	return nil, nil
}

func (m *MockSyncPartStream) Context() context.Context {
	if m.ctx == nil {
		m.ctx = metadata.NewIncomingContext(context.Background(), metadata.MD{})
	}
	return m.ctx
}

func (m *MockSyncPartStream) SendMsg(msg interface{}) error { return nil }
func (m *MockSyncPartStream) RecvMsg(msg interface{}) error { return nil }
func (m *MockSyncPartStream) SetHeader(metadata.MD) error   { return nil }
func (m *MockSyncPartStream) SendHeader(metadata.MD) error  { return nil }
func (m *MockSyncPartStream) SetTrailer(metadata.MD)        {}

func TestChunkedSyncBufferTimeout(t *testing.T) {
	// Initialize logger for tests
	err := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	})
	require.NoError(t, err)

	s := &server{
		log:                   logger.GetLogger("test-server-timeout"),
		chunkedSyncHandlers:   make(map[bus.Topic]queue.ChunkedSyncHandler),
		enableChunkReordering: true,
		maxChunkBufferSize:    10,
		maxChunkGapSize:       5,
		chunkBufferTimeout:    100 * time.Millisecond, // Short timeout for testing
	}

	session := &syncSession{
		sessionID: "test-session-timeout",
		startTime: time.Now(),
		chunkBuffer: &chunkBuffer{
			chunks:        make(map[uint32]*clusterv1.SyncPartRequest),
			expectedIndex: 1, // Waiting for chunk 1
			maxBufferSize: 10,
			bufferTimeout: 100 * time.Millisecond,
			lastActivity:  time.Now().Add(-200 * time.Millisecond), // Already timed out
		},
	}

	// Add a chunk to the buffer to simulate missing chunks
	session.chunkBuffer.chunks[2] = &clusterv1.SyncPartRequest{ChunkIndex: 2}

	// Check buffer timeout
	err = s.checkBufferTimeout(session)
	assert.Error(t, err, "Should return timeout error")
	assert.Contains(t, err.Error(), "buffer timeout", "Error should mention buffer timeout")
}

func TestChunkedSyncChecksumValidation(t *testing.T) {
	// Initialize logger for tests
	err := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})
	require.NoError(t, err)

	// Create a mock server instance to test the processChunk method directly
	s := &server{
		log:                 logger.GetLogger("test-server"),
		chunkedSyncHandlers: make(map[bus.Topic]queue.ChunkedSyncHandler),
	}

	// Create a mock stream
	mockStream := &MockSyncPartStream{}

	t.Run("valid checksum should be accepted", func(t *testing.T) {
		chunkData := []byte("test chunk data with valid checksum")
		validChecksum := fmt.Sprintf("%x", crc32.ChecksumIEEE(chunkData))

		session := &syncSession{
			sessionID:      "test-session-1",
			chunksReceived: 0,
			partsProgress:  make(map[int]*partProgress),
			metadata: &clusterv1.SyncMetadata{
				Group:   "test-group",
				ShardId: 1,
				Topic:   data.TopicStreamPartSync.String(),
			},
		}

		req := &clusterv1.SyncPartRequest{
			SessionId:     "test-session-1",
			ChunkIndex:    0,
			ChunkData:     chunkData,
			ChunkChecksum: validChecksum,
			PartsInfo: []*clusterv1.PartInfo{
				{
					Id: 1,
					Files: []*clusterv1.FileInfo{
						{
							Name:   "test-file.dat",
							Offset: 0,
							Size:   uint32(len(chunkData)),
						},
					},
					CompressedSizeBytes:   uint64(len(chunkData)),
					UncompressedSizeBytes: uint64(len(chunkData)),
					TotalCount:            1,
					BlocksCount:           1,
					MinTimestamp:          time.Now().UnixMilli(),
					MaxTimestamp:          time.Now().UnixMilli(),
				},
			},
		}

		// Register a mock handler
		mockHandler := &MockChunkedSyncHandler{}
		s.chunkedSyncHandlers[data.TopicStreamPartSync] = mockHandler

		// Test checksum validation - this should succeed
		err := s.processChunk(mockStream, session, req)
		require.NoError(t, err)

		// Verify that the correct response was sent
		assert.Len(t, mockStream.sentResponses, 1)
		resp := mockStream.sentResponses[0]
		assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.Status)
		assert.Empty(t, resp.Error)
		assert.Equal(t, "test-session-1", resp.SessionId)
		assert.Equal(t, uint32(0), resp.ChunkIndex)
	})

	t.Run("invalid checksum should be rejected", func(t *testing.T) {
		chunkData := []byte("test chunk data with invalid checksum")
		invalidChecksum := "deadbeef" // Intentionally wrong checksum

		session := &syncSession{
			sessionID:      "test-session-2",
			chunksReceived: 0,
			partsProgress:  make(map[int]*partProgress),
			metadata: &clusterv1.SyncMetadata{
				Group:   "test-group",
				ShardId: 1,
				Topic:   data.TopicStreamPartSync.String(),
			},
		}

		req := &clusterv1.SyncPartRequest{
			SessionId:     "test-session-2",
			ChunkIndex:    0,
			ChunkData:     chunkData,
			ChunkChecksum: invalidChecksum,
			PartsInfo: []*clusterv1.PartInfo{
				{
					Id: 1,
					Files: []*clusterv1.FileInfo{
						{
							Name:   "test-file.dat",
							Offset: 0,
							Size:   uint32(len(chunkData)),
						},
					},
					CompressedSizeBytes:   uint64(len(chunkData)),
					UncompressedSizeBytes: uint64(len(chunkData)),
					TotalCount:            1,
					BlocksCount:           1,
					MinTimestamp:          time.Now().UnixMilli(),
					MaxTimestamp:          time.Now().UnixMilli(),
				},
			},
		}

		mockStream := &MockSyncPartStream{}

		// Test checksum validation - this should fail
		err := s.processChunk(mockStream, session, req)
		require.NoError(t, err) // processChunk doesn't return error, it sends response

		// Verify that the correct error response was sent
		assert.Len(t, mockStream.sentResponses, 1)
		resp := mockStream.sentResponses[0]
		assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH, resp.Status)
		assert.NotEmpty(t, resp.Error)
		assert.Contains(t, resp.Error, "checksum mismatch")
		assert.Contains(t, resp.Error, "chunk 0")
		assert.Equal(t, "test-session-2", resp.SessionId)
		assert.Equal(t, uint32(0), resp.ChunkIndex)

		// Verify the error message contains both expected and actual checksums
		expectedChecksum := fmt.Sprintf("%x", crc32.ChecksumIEEE(chunkData))
		assert.Contains(t, resp.Error, invalidChecksum)  // sent checksum
		assert.Contains(t, resp.Error, expectedChecksum) // calculated checksum
	})

	t.Run("out of order chunks should be rejected", func(t *testing.T) {
		chunkData := []byte("out of order chunk")
		validChecksum := fmt.Sprintf("%x", crc32.ChecksumIEEE(chunkData))

		session := &syncSession{
			sessionID:      "test-session-3",
			chunksReceived: 0, // Expecting chunk 0, but we'll send chunk 1
			partsProgress:  make(map[int]*partProgress),
			metadata: &clusterv1.SyncMetadata{
				Group:   "test-group",
				ShardId: 1,
				Topic:   data.TopicStreamPartSync.String(),
			},
		}

		req := &clusterv1.SyncPartRequest{
			SessionId:     "test-session-3",
			ChunkIndex:    1, // Wrong index - should be 0
			ChunkData:     chunkData,
			ChunkChecksum: validChecksum,
			PartsInfo: []*clusterv1.PartInfo{
				{
					Id: 1,
					Files: []*clusterv1.FileInfo{
						{
							Name:   "test-file.dat",
							Offset: 0,
							Size:   uint32(len(chunkData)),
						},
					},
					CompressedSizeBytes:   uint64(len(chunkData)),
					UncompressedSizeBytes: uint64(len(chunkData)),
					TotalCount:            1,
					BlocksCount:           1,
					MinTimestamp:          time.Now().UnixMilli(),
					MaxTimestamp:          time.Now().UnixMilli(),
				},
			},
		}

		mockStream := &MockSyncPartStream{}

		// Test out of order validation - this should fail
		err := s.processChunk(mockStream, session, req)
		require.NoError(t, err) // processChunk doesn't return error, it sends response

		// Verify that the correct error response was sent
		assert.Len(t, mockStream.sentResponses, 1)
		resp := mockStream.sentResponses[0]
		assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER, resp.Status)
		assert.NotEmpty(t, resp.Error)
		assert.Contains(t, resp.Error, "out of order chunk")
		assert.Contains(t, resp.Error, "expected 0")
		assert.Contains(t, resp.Error, "got 1")
	})
}

func TestChunkedSyncOutOfOrderBasic(t *testing.T) {
	// Initialize logger for tests
	err := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})
	require.NoError(t, err)

	// Create server with strict sequential mode (reordering disabled)
	s := &server{
		log:                   logger.GetLogger("test-server"),
		chunkedSyncHandlers:   make(map[bus.Topic]queue.ChunkedSyncHandler),
		enableChunkReordering: false, // Strict sequential mode
	}

	// Create mock stream
	mockStream := &MockSyncPartStream{}

	// Create a session
	session := &syncSession{
		sessionID:      "test-session",
		startTime:      time.Now(),
		chunksReceived: 0,
		partsProgress:  make(map[int]*partProgress),
		metadata: &clusterv1.SyncMetadata{
			Group:   "test-group",
			ShardId: 1,
			Topic:   data.TopicStreamPartSync.String(),
		},
	}

	// Register a mock handler
	mockHandler := &MockChunkedSyncHandler{}
	s.chunkedSyncHandlers[data.TopicStreamPartSync] = mockHandler

	t.Run("out of order chunk should be rejected in strict mode", func(t *testing.T) {
		// Send chunk 0 first (should succeed)
		req := &clusterv1.SyncPartRequest{
			SessionId:     "test-session",
			ChunkIndex:    0,
			ChunkData:     []byte("chunk-0-data"),
			ChunkChecksum: fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte("chunk-0-data"))),
			PartsInfo: []*clusterv1.PartInfo{
				{
					Id: 1,
					Files: []*clusterv1.FileInfo{
						{
							Name:   "test-file.dat",
							Offset: 0,
							Size:   13,
						},
					},
				},
			},
		}

		err := s.processChunk(mockStream, session, req)
		require.NoError(t, err) // processChunk doesn't return error, it sends response

		// Verify first chunk was accepted
		assert.Len(t, mockStream.sentResponses, 1)
		resp := mockStream.sentResponses[0]
		assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.Status)

		// Now send chunk 2 (out of order - missing chunk 1)
		req2 := &clusterv1.SyncPartRequest{
			SessionId:     "test-session",
			ChunkIndex:    2,
			ChunkData:     []byte("chunk-2-data"),
			ChunkChecksum: fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte("chunk-2-data"))),
			PartsInfo: []*clusterv1.PartInfo{
				{
					Id: 1,
					Files: []*clusterv1.FileInfo{
						{
							Name:   "test-file.dat",
							Offset: 0,
							Size:   13,
						},
					},
				},
			},
		}

		err = s.processChunk(mockStream, session, req2)
		require.NoError(t, err) // processChunk doesn't return error, it sends response

		// Verify that the correct error response was sent
		assert.Len(t, mockStream.sentResponses, 2)
		resp = mockStream.sentResponses[1]
		assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER, resp.Status)
		assert.NotEmpty(t, resp.Error)
		assert.Contains(t, resp.Error, "out of order chunk")
		assert.Contains(t, resp.Error, "expected 1")
		assert.Contains(t, resp.Error, "got 2")
	})
}
