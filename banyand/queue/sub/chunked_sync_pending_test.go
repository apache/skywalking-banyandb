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
	"fmt"
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// sealingChunkedSyncHandler records the lifecycle of parts whose handlers are queue.PartSealers.
// The chunk of part busyOn is refused with queue.ErrServerBusy.
type sealingChunkedSyncHandler struct {
	events []string
	busyOn uint64
}

func (h *sealingChunkedSyncHandler) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	h.events = append(h.events, fmt.Sprintf("create id=%d", ctx.ID))
	return &sealingChunkedSyncPart{h: h, id: ctx.ID}, nil
}

func (h *sealingChunkedSyncHandler) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, _ []byte) error {
	if ctx.ID == h.busyOn {
		return queue.ErrServerBusy
	}
	return nil
}

type sealingChunkedSyncPart struct {
	h  *sealingChunkedSyncHandler
	id uint64
}

func (p *sealingChunkedSyncPart) NewPartType(_ *queue.ChunkedSyncPartContext) error { return nil }

func (p *sealingChunkedSyncPart) SealSync() error {
	p.h.events = append(p.h.events, fmt.Sprintf("seal id=%d", p.id))
	return nil
}

func (p *sealingChunkedSyncPart) FinishSync() error {
	p.h.events = append(p.h.events, fmt.Sprintf("finish id=%d", p.id))
	return nil
}

func (p *sealingChunkedSyncPart) Close() error {
	p.h.events = append(p.h.events, fmt.Sprintf("close id=%d", p.id))
	return nil
}

// scriptedSyncPartStream feeds SyncPart a fixed list of requests, then io.EOF.
type scriptedSyncPartStream struct {
	MockSyncPartStream
	requests []*clusterv1.SyncPartRequest
}

func (s *scriptedSyncPartStream) Recv() (*clusterv1.SyncPartRequest, error) {
	if len(s.requests) == 0 {
		return nil, io.EOF
	}
	req := s.requests[0]
	s.requests = s.requests[1:]
	return req, nil
}

func pendingTestChunk(index uint32, partID uint64) *clusterv1.SyncPartRequest {
	chunkData := []byte(fmt.Sprintf("part-%d", partID))
	req := &clusterv1.SyncPartRequest{
		SessionId: "s", ChunkIndex: index, ChunkData: chunkData,
		ChunkChecksum: fmt.Sprintf("%x", crc32.ChecksumIEEE(chunkData)),
		PartsInfo: []*clusterv1.PartInfo{{
			Id: partID, MinTimestamp: 1,
			Files: []*clusterv1.FileInfo{{Name: "f", Size: uint32(len(chunkData))}},
		}},
	}
	if index == 0 {
		req.Content = &clusterv1.SyncPartRequest_Metadata{Metadata: &clusterv1.SyncMetadata{
			Group: "g", Topic: data.TopicTracePartSync.String(), TotalParts: 3,
		}}
	}
	return req
}

func newPendingTestServer(h queue.ChunkedSyncHandler) *server {
	return &server{ //nolint:exhaustruct
		log:                 logger.GetLogger("test-chunked-sync-pending"),
		chunkedSyncHandlers: map[bus.Topic]queue.ChunkedSyncHandler{data.TopicTracePartSync: h},
	}
}

func TestSyncPart_FailedSessionIntroducesNoPart(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	// The sender resends every part of a failed session to the node, so a part introduced
	// before the failure would be stored twice.
	h := &sealingChunkedSyncHandler{busyOn: 3}
	stream := &scriptedSyncPartStream{requests: []*clusterv1.SyncPartRequest{
		pendingTestChunk(0, 1), pendingTestChunk(1, 2), pendingTestChunk(2, 3),
	}}
	require.NoError(t, newPendingTestServer(h).SyncPart(stream))

	require.NotEmpty(t, stream.sentResponses)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SERVER_BUSY, stream.sentResponses[len(stream.sentResponses)-1].Status)
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"create id=3",
		"close id=3", "close id=1", "close id=2",
	}, h.events)
}

func TestSyncPart_CompletionIntroducesEveryPart(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	h := &sealingChunkedSyncHandler{}
	stream := &scriptedSyncPartStream{requests: []*clusterv1.SyncPartRequest{
		pendingTestChunk(0, 1), pendingTestChunk(1, 2), pendingTestChunk(2, 3),
		{SessionId: "s", ChunkIndex: 3, Content: &clusterv1.SyncPartRequest_Completion{Completion: &clusterv1.SyncCompletion{}}},
	}}
	require.NoError(t, newPendingTestServer(h).SyncPart(stream))

	last := stream.sentResponses[len(stream.sentResponses)-1]
	require.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE, last.Status)
	assert.True(t, last.GetSyncResult().GetSuccess())
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"create id=3", "seal id=3",
		"finish id=1", "finish id=2", "finish id=3",
	}, h.events)
}

func TestSyncPart_StreamEndWithoutCompletionIntroducesNoPart(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	// The sender dies or the connection drops after two parts: io.EOF before the completion.
	h := &sealingChunkedSyncHandler{}
	stream := &scriptedSyncPartStream{requests: []*clusterv1.SyncPartRequest{
		pendingTestChunk(0, 1), pendingTestChunk(1, 2),
	}}
	require.NoError(t, newPendingTestServer(h).SyncPart(stream))
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2",
		"close id=2", "close id=1",
	}, h.events)
}
