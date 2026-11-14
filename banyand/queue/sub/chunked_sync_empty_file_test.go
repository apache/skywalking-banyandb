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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type recordingSyncHandler struct {
	files map[string]int
}

func (r *recordingSyncHandler) CreatePartHandler(_ *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	return &MockChunkedSyncPartHandler{}, nil
}

func (r *recordingSyncHandler) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	r.files[ctx.FileName] += len(chunk)
	return nil
}

func TestChunkedSyncDeliversZeroLengthFiles(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "info"}))
	handler := &recordingSyncHandler{files: map[string]int{}}
	s := &server{
		log:                 logger.GetLogger("test-zero-length-files"),
		chunkedSyncHandlers: map[bus.Topic]queue.ChunkedSyncHandler{data.TopicStreamPartSync: handler},
	}
	session := &syncSession{
		sessionID:     "zero-length",
		startTime:     time.Now(),
		partsProgress: make(map[int]*partProgress),
		metadata:      &clusterv1.SyncMetadata{Group: "g", ShardId: 1, Topic: data.TopicStreamPartSync.String()},
	}
	stream := &MockSyncPartStream{}

	// A chunk whose last file is empty, then a chunk that holds only an empty file.
	chunks := []struct {
		data  []byte
		files []*clusterv1.FileInfo
	}{
		{data: []byte("abc"), files: []*clusterv1.FileInfo{{Name: "full", Offset: 0, Size: 3}, {Name: "empty-tail", Offset: 3, Size: 0}}},
		{data: nil, files: []*clusterv1.FileInfo{{Name: "empty-only", Offset: 0, Size: 0}}},
	}
	for i, c := range chunks {
		req := &clusterv1.SyncPartRequest{
			SessionId:     session.sessionID,
			ChunkIndex:    uint32(i),
			ChunkData:     c.data,
			ChunkChecksum: queue.ChunkChecksum(c.data),
			PartsInfo:     []*clusterv1.PartInfo{{Id: 1, Files: c.files}},
		}
		require.NoError(t, s.processChunk(stream, session, req))
	}

	assert.Equal(t, map[string]int{"full": 3, "empty-tail": 0, "empty-only": 0}, handler.files)
	for i, resp := range stream.sentResponses {
		assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.Status, "chunk %d", i)
	}
}
