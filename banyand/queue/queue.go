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

package queue

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Queue builds a data transmission tunnel between subscribers and publishers.
//
//go:generate mockgen -destination=./queue_mock.go -package=queue github.com/apache/skywalking-banyandb/pkg/bus MessageListener
type Queue interface {
	Client
	Server
	run.Service
}

// Client is the interface for publishing data to the queue.
//
//go:generate mockgen -destination=./pipeline_mock.go -package=queue . Client
type Client interface {
	run.Unit
	bus.Publisher
	bus.Broadcaster
	NewBatchPublisher(timeout time.Duration) BatchPublisher
	NewChunkedSyncClient(node string, chunkSize uint32) (ChunkedSyncClient, error)
	Register(bus.Topic, schema.EventHandler)
	OnAddOrUpdate(md schema.Metadata)
	GracefulStop()
}

// Server is the interface for receiving data from the queue.
type Server interface {
	run.Unit
	bus.Subscriber
	RegisterChunkedSyncHandler(topic bus.Topic, handler ChunkedSyncHandler)
	GetPort() *uint32
}

// BatchPublisher is the interface for publishing data in batch.
//
//go:generate mockgen -destination=./batch_publisher_mock.go -package=queue . BatchPublisher
type BatchPublisher interface {
	bus.Publisher
	Close() (map[string]*common.Error, error)
}

// FileInfo represents information about an individual file within a part.
type FileInfo struct {
	Reader fs.SeqReader
	Name   string
}

// StreamingPartData represents streaming part data for syncing.
type StreamingPartData struct {
	Group                 string
	Topic                 string
	PartType              string
	Files                 []FileInfo
	ID                    uint64
	CompressedSizeBytes   uint64
	UncompressedSizeBytes uint64
	TotalCount            uint64
	BlocksCount           uint64
	MinTimestamp          int64
	MaxTimestamp          int64
	MinKey                int64
	MaxKey                int64
	ShardID               uint32
}

// ChunkedSyncClient provides methods for chunked data synchronization with streaming support.
type ChunkedSyncClient interface {
	// SyncStreamingParts sends streaming parts using chunked transfer.
	SyncStreamingParts(ctx context.Context, parts []StreamingPartData) (*SyncResult, error)
	// Close releases resources associated with the client.
	Close() error
}

// ChunkedSyncPartContext represents the context for a chunked sync operation.
type ChunkedSyncPartContext struct {
	Handler               PartHandler
	Group                 string
	FileName              string
	PartType              string
	ID                    uint64
	CompressedSizeBytes   uint64
	UncompressedSizeBytes uint64
	TotalCount            uint64
	BlocksCount           uint64
	MinTimestamp          int64
	MaxTimestamp          int64
	MinKey                int64
	MaxKey                int64
	ShardID               uint32
	TraceIDFilter         any
	TagType               any
}

// Close releases resources associated with the context.
func (c *ChunkedSyncPartContext) Close() error {
	defer func() {
		c.Handler = nil
		c.FileName = ""
	}()
	if c.Handler != nil {
		return c.Handler.Close()
	}
	return nil
}

// PartHandler handles individual parts during sync operations.
type PartHandler interface {
	FinishSync() error
	Close() error
}

// ChunkedSyncHandler handles incoming chunked sync requests on the server side.
//
//go:generate mockgen -destination=./chunked_sync_handler_mock.go -package=queue . ChunkedSyncHandler
type ChunkedSyncHandler interface {
	// HandleFileChunk processes incoming file chunks as they arrive.
	// This method is called for each chunk of file data to enable streaming processing.
	HandleFileChunk(ctx *ChunkedSyncPartContext, chunk []byte) error

	// CreatePartHandler creates a new part handler for the given context.
	CreatePartHandler(ctx *ChunkedSyncPartContext) (PartHandler, error)
}

// FileData represents file data with name and content.
type FileData struct {
	FileName string
	Data     []byte
}

// SyncResult represents the result of a sync operation.
type SyncResult struct {
	SessionID    string
	ErrorMessage string
	TotalBytes   uint64
	DurationMs   int64
	ChunksCount  uint32
	PartsCount   uint32
	Success      bool
}

// SyncMetadata is an alias for clusterv1.SyncMetadata.
type SyncMetadata = clusterv1.SyncMetadata
