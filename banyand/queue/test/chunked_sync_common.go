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

package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/banyand/queue/sub"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// Setup holds the test environment configuration and components.
type Setup struct {
	Server        queue.Server
	Client        queue.Client
	Closer        run.Unit
	ChunkedClient queue.ChunkedSyncClient
	MockHandler   *MockChunkedSyncHandler
	DeferFn       func()
	NodeName      string
	NodeAddr      string
	TestGroup     run.Group
	GRPCPort      uint32
	HTTPPort      uint32
}

// setupChunkedSyncTest creates a complete test environment for chunked sync testing.
func setupChunkedSyncTest(t *testing.T, testName string) *Setup {
	return setupChunkedSyncTestWithChunkSize(t, testName, 1024)
}

// setupChunkedSyncTestWithChunkSize creates a complete test environment for chunked sync testing with custom chunk size.
func setupChunkedSyncTestWithChunkSize(t *testing.T, testName string, chunkSize uint32) *Setup {
	ports, err := test.AllocateFreePorts(2)
	require.NoError(t, err, "Failed to allocate free ports")

	grpcPort := uint32(ports[0])
	httpPort := uint32(ports[1])

	omr := observability.BypassRegistry

	testGroup := run.NewGroup(testName)
	closer, deferFn := run.NewTester(testName + "-closer")

	server := sub.NewServerWithPorts(omr, testName, grpcPort, httpPort)

	mockHandler := NewMockChunkedSyncHandler()

	server.RegisterChunkedSyncHandler(data.TopicStreamPartSync, mockHandler)

	testGroup.Register(closer, server)

	cmd := &cobra.Command{
		Use: testName,
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			UnknownFlags: true, // Ignore unknown flags
		},
		Run: func(_ *cobra.Command, _ []string) {
			err = testGroup.Run(context.Background())
			if err != nil {
				t.Logf("Server group failed: %v", err)
			}
		},
	}
	cmd.Flags().AddFlagSet(testGroup.RegisterFlags().FlagSet)
	go func() {
		require.NoError(t, cmd.Execute())
	}()

	assert.Eventually(t, func() bool {
		errInternal := helpers.HealthCheck(fmt.Sprintf("localhost:%d", grpcPort), 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))()
		return errInternal == nil
	}, flags.EventuallyTimeout, 100*time.Millisecond)

	client := pub.NewWithoutMetadata()

	nodeAddr := fmt.Sprintf("localhost:%d", grpcPort)
	nodeName := testName + "-node"
	node := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Name: nodeName,
			Kind: schema.KindNode,
		},
		Spec: &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: nodeName,
			},
			Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
			GrpcAddress: nodeAddr,
		},
	}

	client.OnAddOrUpdate(node)

	var chunkedClient queue.ChunkedSyncClient
	assert.Eventually(t, func() bool {
		var errInternal error
		chunkedClient, errInternal = client.NewChunkedSyncClient(nodeName, chunkSize)
		if err != nil {
			t.Logf("NewChunkedSyncClient error: %v", err)
		}
		return errInternal == nil
	}, flags.EventuallyTimeout, 100*time.Millisecond)

	return &Setup{
		Server:        server,
		Client:        client,
		MockHandler:   mockHandler,
		TestGroup:     testGroup,
		Closer:        closer,
		DeferFn:       deferFn,
		GRPCPort:      grpcPort,
		HTTPPort:      httpPort,
		NodeName:      nodeName,
		NodeAddr:      nodeAddr,
		ChunkedClient: chunkedClient,
	}
}

// cleanupTestSetup cleans up the test environment.
func cleanupTestSetup(setup *Setup) {
	if setup.ChunkedClient != nil {
		setup.ChunkedClient.Close()
	}
	if setup.DeferFn != nil {
		setup.DeferFn()
	}
}

// createTestDataPart creates a StreamingPartData for testing.
func createTestDataPart(files map[string][]byte, group string, shardID uint32) queue.StreamingPartData {
	var fileInfos []queue.FileInfo
	var totalSize uint64

	for fileName, content := range files {
		var buf bytes.Buffer
		if _, err := buf.Write(content); err != nil {
			panic(fmt.Sprintf("failed to write content to buffer: %v", err))
		}
		fileInfos = append(fileInfos, queue.FileInfo{
			Name:   fileName,
			Reader: buf.SequentialRead(),
		})
		totalSize += uint64(len(content))
	}

	return queue.StreamingPartData{
		ID:                    1,
		Files:                 fileInfos,
		Group:                 group,
		ShardID:               shardID,
		Topic:                 data.TopicStreamPartSync.String(),
		CompressedSizeBytes:   totalSize,
		UncompressedSizeBytes: totalSize,
		TotalCount:            uint64(len(files)),
		BlocksCount:           1,
		MinTimestamp:          time.Now().UnixMilli(),
		MaxTimestamp:          time.Now().UnixMilli(),
	}
}

// createTestDataPartWithMetadata creates a StreamingPartData for testing with specific metadata values.
func createTestDataPartWithMetadata(files map[string][]byte, group string, shardID uint32,
	compressedSize, uncompressedSize, totalCount, blocksCount uint64,
	minTimestamp, maxTimestamp int64, partID uint64,
) queue.StreamingPartData {
	var fileInfos []queue.FileInfo

	for fileName, content := range files {
		var buf bytes.Buffer
		if _, err := buf.Write(content); err != nil {
			panic(fmt.Sprintf("failed to write content to buffer: %v", err))
		}
		fileInfos = append(fileInfos, queue.FileInfo{
			Name:   fileName,
			Reader: buf.SequentialRead(),
		})
	}

	return queue.StreamingPartData{
		ID:                    partID,
		Files:                 fileInfos,
		Group:                 group,
		ShardID:               shardID,
		Topic:                 data.TopicStreamPartSync.String(),
		CompressedSizeBytes:   compressedSize,
		UncompressedSizeBytes: uncompressedSize,
		TotalCount:            totalCount,
		BlocksCount:           blocksCount,
		MinTimestamp:          minTimestamp,
		MaxTimestamp:          maxTimestamp,
	}
}

// MockPartHandler implements queue.PartHandler for testing.
type MockPartHandler struct {
	receivedFiles    map[string][]byte
	syncHandler      *MockChunkedSyncHandler
	receivedChunks   [][]byte
	finishSyncCalled bool
	closeCalled      bool
}

// FinishSync marks the sync as finished and closes the handler.
func (m *MockPartHandler) FinishSync() error {
	m.finishSyncCalled = true

	if m.syncHandler != nil {
		m.syncHandler.mu.Lock()
		m.syncHandler.completedParts = append(m.syncHandler.completedParts, m)
		m.syncHandler.mu.Unlock()
	}
	return m.Close()
}

// Close marks the handler as closed.
func (m *MockPartHandler) Close() error {
	m.closeCalled = true
	return nil
}

// GetReceivedFiles returns a copy of the received files.
func (m *MockPartHandler) GetReceivedFiles() map[string][]byte {
	result := make(map[string][]byte)
	for k, v := range m.receivedFiles {
		result[k] = make([]byte, len(v))
		copy(result[k], v)
	}
	return result
}

// GetReceivedChunksCount returns the number of received chunks.
func (m *MockPartHandler) GetReceivedChunksCount() int {
	return len(m.receivedChunks)
}

// MockChunkedSyncHandler implements queue.ChunkedSyncHandler for testing.
type MockChunkedSyncHandler struct {
	completedParts   []*MockPartHandler
	receivedContexts []*queue.ChunkedSyncPartContext
	mu               sync.Mutex
}

// NewMockChunkedSyncHandler creates a new mock chunked sync handler.
func NewMockChunkedSyncHandler() *MockChunkedSyncHandler {
	return &MockChunkedSyncHandler{
		completedParts:   make([]*MockPartHandler, 0),
		receivedContexts: make([]*queue.ChunkedSyncPartContext, 0),
	}
}

// HandleFileChunk processes incoming file chunks for testing.
func (m *MockChunkedSyncHandler) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	currentPart := ctx.Handler.(*MockPartHandler)

	if currentPart != nil {
		chunkCopy := make([]byte, len(chunk))
		copy(chunkCopy, chunk)
		currentPart.receivedChunks = append(currentPart.receivedChunks, chunkCopy)

		if currentPart.receivedFiles[ctx.FileName] == nil {
			currentPart.receivedFiles[ctx.FileName] = make([]byte, 0)
		}
		currentPart.receivedFiles[ctx.FileName] = append(currentPart.receivedFiles[ctx.FileName], chunkCopy...)
	}

	return nil
}

// CreatePartHandler creates a new part handler for testing.
func (m *MockChunkedSyncHandler) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	m.mu.Lock()
	contextCopy := &queue.ChunkedSyncPartContext{
		ID:                    ctx.ID,
		Group:                 ctx.Group,
		ShardID:               ctx.ShardID,
		CompressedSizeBytes:   ctx.CompressedSizeBytes,
		UncompressedSizeBytes: ctx.UncompressedSizeBytes,
		TotalCount:            ctx.TotalCount,
		BlocksCount:           ctx.BlocksCount,
		MinTimestamp:          ctx.MinTimestamp,
		MaxTimestamp:          ctx.MaxTimestamp,
		MinKey:                ctx.MinKey,
		MaxKey:                ctx.MaxKey,
		TraceIDFilter:         ctx.TraceIDFilter,
		TagType:               ctx.TagType,
	}
	m.receivedContexts = append(m.receivedContexts, contextCopy)
	m.mu.Unlock()

	partHandler := &MockPartHandler{
		receivedFiles:  make(map[string][]byte),
		syncHandler:    m,
		receivedChunks: make([][]byte, 0),
	}
	contextCopy.Handler = partHandler
	return partHandler, nil
}

// GetCompletedParts returns all completed parts.
func (m *MockChunkedSyncHandler) GetCompletedParts() []*MockPartHandler {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*MockPartHandler, len(m.completedParts))
	copy(result, m.completedParts)
	return result
}

// GetReceivedFiles returns all received files across all parts.
func (m *MockChunkedSyncHandler) GetReceivedFiles() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string][]byte)
	for _, part := range m.completedParts {
		for fileName, content := range part.GetReceivedFiles() {
			result[fileName] = content
		}
	}
	return result
}

// GetReceivedChunksCount returns the total number of received chunks.
func (m *MockChunkedSyncHandler) GetReceivedChunksCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, part := range m.completedParts {
		total += part.GetReceivedChunksCount()
	}
	return total
}

// GetReceivedContexts returns all received contexts.
func (m *MockChunkedSyncHandler) GetReceivedContexts() []*queue.ChunkedSyncPartContext {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*queue.ChunkedSyncPartContext, len(m.receivedContexts))
	for i, ctx := range m.receivedContexts {
		contextCopy := &queue.ChunkedSyncPartContext{
			ID:                    ctx.ID,
			Group:                 ctx.Group,
			ShardID:               ctx.ShardID,
			FileName:              ctx.FileName,
			CompressedSizeBytes:   ctx.CompressedSizeBytes,
			UncompressedSizeBytes: ctx.UncompressedSizeBytes,
			TotalCount:            ctx.TotalCount,
			BlocksCount:           ctx.BlocksCount,
			MinTimestamp:          ctx.MinTimestamp,
			MaxTimestamp:          ctx.MaxTimestamp,
			MinKey:                ctx.MinKey,
			MaxKey:                ctx.MaxKey,
			TraceIDFilter:         ctx.TraceIDFilter,
			TagType:               ctx.TagType,
		}
		result[i] = contextCopy
	}
	return result
}
