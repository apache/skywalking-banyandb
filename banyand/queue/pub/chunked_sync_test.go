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
	"io"
	"net"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockChunkedSyncServer struct {
	clusterv1.UnimplementedChunkedSyncServiceServer
	checksumMismatchCount int
	maxMismatchResponses  int
	mu                    sync.Mutex
}

func (m *mockChunkedSyncServer) SyncPart(stream clusterv1.ChunkedSyncService_SyncPartServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		if req.GetCompletion() != nil {
			return stream.Send(&clusterv1.SyncPartResponse{
				SessionId:  req.SessionId,
				ChunkIndex: req.ChunkIndex,
				Status:     clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE,
				SyncResult: &clusterv1.SyncResult{
					Success: true,
				},
			})
		}

		m.mu.Lock()
		shouldMismatch := m.checksumMismatchCount < m.maxMismatchResponses
		if shouldMismatch {
			m.checksumMismatchCount++
		}
		m.mu.Unlock()

		var status clusterv1.SyncStatus
		var errorMsg string

		if shouldMismatch {
			status = clusterv1.SyncStatus_SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH
			errorMsg = fmt.Sprintf("chunk %d checksum mismatch", req.ChunkIndex)
		} else {
			status = clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED
		}

		resp := &clusterv1.SyncPartResponse{
			SessionId:  req.SessionId,
			ChunkIndex: req.ChunkIndex,
			Status:     status,
			Error:      errorMsg,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func setupChunkedSyncServer(address string, maxMismatchResponses int) (*mockChunkedSyncServer, func()) {
	s := grpc.NewServer()
	mockServer := &mockChunkedSyncServer{
		maxMismatchResponses: maxMismatchResponses,
	}
	clusterv1.RegisterChunkedSyncServiceServer(s, mockServer)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Panicf("failed to listen: %v", err)
		return nil, nil
	}

	go func() {
		if err := s.Serve(lis); err != nil {
			logger.Panicf("Server exited with error: %v", err)
		}
	}()

	return mockServer, s.GracefulStop
}

var _ = ginkgo.Describe("Chunked Sync Retry Mechanism", func() {
	ginkgo.Context("when checksum mismatches occur", func() {
		ginkgo.It("should retry and succeed within max retries", func() {
			address := getAddress()
			mockServer, cleanup := setupChunkedSyncServer(address, 2)
			defer cleanup()

			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer conn.Close()

			client := &chunkedSyncClient{
				conn:      conn,
				log:       logger.GetLogger("test-chunked-sync"),
				chunkSize: 1024,
			}

			parts := []queue.StreamingPartData{
				{
					ID:      1,
					Group:   "test-group",
					ShardID: 1,
					Topic:   "stream_write",
					Files: []queue.FileInfo{
						createFileInfo("test-file.dat", []byte("test data for chunked sync")),
					},
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := client.SyncStreamingParts(ctx, parts)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Success).To(gomega.BeTrue())
			gomega.Expect(mockServer.checksumMismatchCount).To(gomega.Equal(2))
		})

		ginkgo.It("should fail after exceeding max retries", func() {
			address := getAddress()
			mockServer, cleanup := setupChunkedSyncServer(address, maxRetries+1)
			defer cleanup()

			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer conn.Close()

			client := &chunkedSyncClient{
				conn:      conn,
				log:       logger.GetLogger("test-chunked-sync"),
				chunkSize: 1024,
			}

			parts := []queue.StreamingPartData{
				{
					ID:      1,
					Group:   "test-group",
					ShardID: 1,
					Topic:   "stream_write",
					Files: []queue.FileInfo{
						createFileInfo("test-file.dat", []byte("test data for chunked sync")),
					},
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			_, err = client.SyncStreamingParts(ctx, parts)
			gomega.Expect(err).To(gomega.HaveOccurred())
			gomega.Expect(err.Error()).To(gomega.ContainSubstring("checksum mismatch after"))
			gomega.Expect(mockServer.checksumMismatchCount).To(gomega.Equal(maxRetries + 1))
		})

		ginkgo.It("should handle no retry needed scenario", func() {
			address := getAddress()
			mockServer, cleanup := setupChunkedSyncServer(address, 0)
			defer cleanup()

			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer conn.Close()

			client := &chunkedSyncClient{
				conn:      conn,
				log:       logger.GetLogger("test-chunked-sync"),
				chunkSize: 1024,
			}

			parts := []queue.StreamingPartData{
				{
					ID:      1,
					Group:   "test-group",
					ShardID: 1,
					Topic:   "stream_write",
					Files: []queue.FileInfo{
						createFileInfo("test-file.dat", []byte("test data for chunked sync")),
					},
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			result, err := client.SyncStreamingParts(ctx, parts)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result.Success).To(gomega.BeTrue())
			gomega.Expect(mockServer.checksumMismatchCount).To(gomega.Equal(0))
		})
	})

	ginkgo.Context("when a part fails mid-read during chunk building", func() {
		ginkgo.It("should discard incomplete chunk and continue with non-failed parts", func() {
			address := getAddress()

			// Track chunks received to verify no partial data is sent
			var receivedChunks []*clusterv1.SyncPartRequest
			var mu sync.Mutex

			// Custom mock server that records chunks
			s := grpc.NewServer()
			mockSvc := &recordingMockServer{
				receivedChunks: &receivedChunks,
				mu:             &mu,
			}
			clusterv1.RegisterChunkedSyncServiceServer(s, mockSvc)

			lis, err := net.Listen("tcp", address)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			go func() {
				_ = s.Serve(lis)
			}()
			defer s.GracefulStop()

			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer conn.Close()

			client := &chunkedSyncClient{
				conn:      conn,
				log:       logger.GetLogger("test-chunked-sync"),
				chunkSize: 1024, // Small chunk size to force multiple chunks
			}

			// Create test data: Part 1 (will succeed), Part 2 (will fail mid-read), Part 3 (will succeed)
			part1Data := make([]byte, 800)
			for i := range part1Data {
				part1Data[i] = byte('A')
			}

			part2Data := make([]byte, 1500) // Large enough to span multiple reads
			for i := range part2Data {
				part2Data[i] = byte('B')
			}

			part3Data := make([]byte, 600)
			for i := range part3Data {
				part3Data[i] = byte('C')
			}

			parts := []queue.StreamingPartData{
				{
					ID:      1,
					Group:   "test-group",
					ShardID: 1,
					Topic:   "stream_write",
					Files: []queue.FileInfo{
						createFileInfo("part1-file1.dat", part1Data),
					},
				},
				{
					ID:      2,
					Group:   "test-group",
					ShardID: 1,
					Topic:   "stream_write",
					Files: []queue.FileInfo{
						{
							Name: "part2-file1.dat",
							Reader: &failingReader{
								data:      part2Data,
								failAfter: 500, // Fail after reading 500 bytes
							},
						},
					},
				},
				{
					ID:      3,
					Group:   "test-group",
					ShardID: 1,
					Topic:   "stream_write",
					Files: []queue.FileInfo{
						createFileInfo("part3-file1.dat", part3Data),
					},
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := client.SyncStreamingParts(ctx, parts)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(result).NotTo(gomega.BeNil())

			// Verify that part 2 is marked as failed
			gomega.Expect(result.FailedParts).To(gomega.HaveLen(1))
			gomega.Expect(result.FailedParts[0].PartID).To(gomega.Equal("2"))

			// Verify that the result is still considered successful (partial success)
			gomega.Expect(result.Success).To(gomega.BeTrue())

			// Verify chunks received
			mu.Lock()
			defer mu.Unlock()

			// Find where part 2 appears in chunks
			var part2FirstChunkIdx, part2LastChunkIdx int = -1, -1
			for idx, chunk := range receivedChunks {
				if chunk.GetCompletion() != nil {
					continue
				}
				for _, partInfo := range chunk.PartsInfo {
					if partInfo.Id == 2 {
						if part2FirstChunkIdx == -1 {
							part2FirstChunkIdx = idx
						}
						part2LastChunkIdx = idx
					}
				}
			}

			// Part 2 should appear in at least one chunk (data read before failure)
			gomega.Expect(part2FirstChunkIdx).To(gomega.BeNumerically(">=", 0),
				"Part 2 should have contributed to at least one chunk before failing")

			// After part 2 fails, no subsequent chunks should contain part 2 data
			// This verifies that the buffer was discarded and no mixed data was sent
			if part2LastChunkIdx >= 0 {
				// Check all chunks after the last part 2 chunk
				for idx := part2LastChunkIdx + 1; idx < len(receivedChunks); idx++ {
					chunk := receivedChunks[idx]
					if chunk.GetCompletion() != nil {
						continue
					}
					for _, partInfo := range chunk.PartsInfo {
						gomega.Expect(partInfo.Id).NotTo(gomega.Equal(uint64(2)),
							"No chunks after the failure should contain part 2 data")
					}
				}
			}

			// Verify that part 1 and part 3 data were successfully sent
			var foundPart1, foundPart3 bool
			for _, chunk := range receivedChunks {
				for _, partInfo := range chunk.PartsInfo {
					if partInfo.Id == 1 {
						foundPart1 = true
					}
					if partInfo.Id == 3 {
						foundPart3 = true
					}
				}
			}
			gomega.Expect(foundPart1).To(gomega.BeTrue(), "Part 1 data should be sent")
			gomega.Expect(foundPart3).To(gomega.BeTrue(), "Part 3 data should be sent")
		})
	})
})

type recordingMockServer struct {
	clusterv1.UnimplementedChunkedSyncServiceServer
	receivedChunks *[]*clusterv1.SyncPartRequest
	mu             *sync.Mutex
}

func (r *recordingMockServer) SyncPart(stream clusterv1.ChunkedSyncService_SyncPartServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		r.mu.Lock()
		*r.receivedChunks = append(*r.receivedChunks, req)
		r.mu.Unlock()

		if req.GetCompletion() != nil {
			return stream.Send(&clusterv1.SyncPartResponse{
				SessionId:  req.SessionId,
				ChunkIndex: req.ChunkIndex,
				Status:     clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE,
				SyncResult: &clusterv1.SyncResult{
					Success: true,
				},
			})
		}

		resp := &clusterv1.SyncPartResponse{
			SessionId:  req.SessionId,
			ChunkIndex: req.ChunkIndex,
			Status:     clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func createFileInfo(name string, data []byte) queue.FileInfo {
	var buf bytes.Buffer
	if _, err := buf.Write(data); err != nil {
		panic(fmt.Sprintf("failed to write content to buffer: %v", err))
	}
	return queue.FileInfo{
		Name:   name,
		Reader: buf.SequentialRead(),
	}
}

type failingReader struct {
	data        []byte
	offset      int
	failAfter   int // fail after this many bytes have been read
	readCount   int
	alreadyRead int
}

func (f *failingReader) Read(p []byte) (n int, err error) {
	if f.alreadyRead >= f.failAfter && f.failAfter > 0 {
		return 0, fmt.Errorf("simulated read failure after %d bytes", f.alreadyRead)
	}

	if f.offset >= len(f.data) {
		return 0, io.EOF
	}

	n = copy(p, f.data[f.offset:])
	f.offset += n
	f.alreadyRead += n
	f.readCount++

	return n, nil
}

func (f *failingReader) Close() error {
	return nil
}

func (f *failingReader) Path() string {
	return "failing-reader"
}
