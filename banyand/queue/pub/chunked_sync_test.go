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
})

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
