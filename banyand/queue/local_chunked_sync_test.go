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
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/fileformat"
)

// recSyncHandler records what the chunked sync receiver hands a handler.
type recSyncHandler struct {
	failChunk error
	events    []string
	mu        sync.Mutex
}

func (h *recSyncHandler) recordf(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, fmt.Sprintf(format, args...))
}

func (h *recSyncHandler) recorded() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.events...)
}

func (h *recSyncHandler) CreatePartHandler(ctx *ChunkedSyncPartContext) (PartHandler, error) {
	h.recordf("create id=%d type=%q group=%s shard=%d min=%d", ctx.ID, ctx.PartType, ctx.Group, ctx.ShardID, ctx.MinTimestamp)
	return &recPartHandler{h: h, id: ctx.ID}, nil
}

func (h *recSyncHandler) HandleFileChunk(ctx *ChunkedSyncPartContext, chunk []byte) error {
	if h.failChunk != nil {
		return h.failChunk
	}
	h.recordf("chunk id=%d type=%q file=%s data=%s", ctx.ID, ctx.PartType, ctx.FileName, chunk)
	return nil
}

type recPartHandler struct {
	h  *recSyncHandler
	id uint64
}

func (p *recPartHandler) NewPartType(ctx *ChunkedSyncPartContext) error {
	p.h.recordf("newtype id=%d type=%q", p.id, ctx.PartType)
	return nil
}

func (p *recPartHandler) FinishSync() error {
	p.h.recordf("finish id=%d", p.id)
	return nil
}

func (p *recPartHandler) Close() error {
	p.h.recordf("close id=%d", p.id)
	return nil
}

func relayStream(t *testing.T, h ChunkedSyncHandler) grpc.BidiStreamingClient[clusterv1.SyncPartRequest, clusterv1.SyncPartResponse] {
	t.Helper()
	q := Local()
	q.RegisterChunkedSyncHandler(data.TopicStreamPartSync, h)
	client, err := q.NewChunkedSyncRelayClient("any")
	require.NoError(t, err)
	stream, err := client.SyncPart(context.Background())
	require.NoError(t, err)
	return stream
}

func syncChunk(index uint32, payload string, parts ...*clusterv1.PartInfo) *clusterv1.SyncPartRequest {
	data := []byte(payload)
	return &clusterv1.SyncPartRequest{
		SessionId: "s", ChunkIndex: index, ChunkData: data,
		ChunkChecksum: ChunkChecksum(data), PartsInfo: parts,
	}
}

func withMetadata(req *clusterv1.SyncPartRequest) *clusterv1.SyncPartRequest {
	req.Content = &clusterv1.SyncPartRequest_Metadata{Metadata: &clusterv1.SyncMetadata{
		Group: "g", ShardId: 3, Topic: data.TopicStreamPartSync.String(),
	}}
	return req
}

func exchange(t *testing.T, stream grpc.BidiStreamingClient[clusterv1.SyncPartRequest, clusterv1.SyncPartResponse],
	req *clusterv1.SyncPartRequest,
) (*clusterv1.SyncPartResponse, error) {
	t.Helper()
	require.NoError(t, stream.Send(req))
	return stream.Recv()
}

func TestLocalChunkedSync_PartsAndCompletion(t *testing.T) {
	h := &recSyncHandler{}
	stream := relayStream(t, h)
	// Chunk 0: core part 1 file "a" and the start of its file "b".
	resp, err := exchange(t, stream, withMetadata(syncChunk(0, "aaBB", &clusterv1.PartInfo{
		Id: 1, MinTimestamp: 10,
		Files: []*clusterv1.FileInfo{{Name: "a", Offset: 0, Size: 2}, {Name: "b", Offset: 2, Size: 2}},
	})))
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.GetStatus())
	// Chunk 1: secondary part of the same id, then part 2.
	resp, err = exchange(t, stream, syncChunk(1, "cdE",
		&clusterv1.PartInfo{Id: 1, PartType: "idx", MinTimestamp: 10, Files: []*clusterv1.FileInfo{{Name: "c", Offset: 0, Size: 2}}},
		&clusterv1.PartInfo{Id: 2, MinTimestamp: 20, Files: []*clusterv1.FileInfo{{Name: "e", Offset: 2, Size: 1}}},
	))
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.GetStatus())
	resp, err = exchange(t, stream, &clusterv1.SyncPartRequest{
		SessionId: "s", ChunkIndex: 2, Content: &clusterv1.SyncPartRequest_Completion{Completion: &clusterv1.SyncCompletion{}},
	})
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE, resp.GetStatus())
	assert.Equal(t, []string{
		`create id=1 type="" group=g shard=3 min=10`,
		`chunk id=1 type="" file=a data=aa`,
		`chunk id=1 type="" file=b data=BB`,
		`newtype id=1 type="idx"`,
		`chunk id=1 type="idx" file=c data=cd`,
		`finish id=1`,
		`create id=2 type="" group=g shard=3 min=20`,
		`chunk id=2 type="" file=e data=E`,
		`finish id=2`,
	}, h.recorded())
}

func TestLocalChunkedSync_FirstFrameNeedsMetadata(t *testing.T) {
	stream := relayStream(t, &recSyncHandler{})
	resp, err := exchange(t, stream, syncChunk(0, "x", &clusterv1.PartInfo{Id: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}}))
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SESSION_NOT_FOUND, resp.GetStatus())
	assert.NotEmpty(t, resp.GetError())
}

func TestLocalChunkedSync_UnknownTopic(t *testing.T) {
	stream := relayStream(t, &recSyncHandler{})
	req := syncChunk(0, "x")
	req.Content = &clusterv1.SyncPartRequest_Metadata{Metadata: &clusterv1.SyncMetadata{Group: "g", Topic: "nope"}}
	_, err := exchange(t, stream, req)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestLocalChunkedSync_ChecksumMismatch(t *testing.T) {
	h := &recSyncHandler{}
	stream := relayStream(t, h)
	req := withMetadata(syncChunk(0, "x", &clusterv1.PartInfo{Id: 1, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}}))
	req.ChunkChecksum = "0"
	resp, err := exchange(t, stream, req)
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH, resp.GetStatus())
	assert.NotEmpty(t, resp.GetError())
	assert.Empty(t, h.recorded())
}

func TestLocalChunkedSync_OutOfOrder(t *testing.T) {
	stream := relayStream(t, &recSyncHandler{})
	resp, err := exchange(t, stream, withMetadata(syncChunk(1, "x", &clusterv1.PartInfo{Id: 1, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}})))
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER, resp.GetStatus())
	assert.NotEmpty(t, resp.GetError())
}

func TestLocalChunkedSync_FileOutsideChunk(t *testing.T) {
	stream := relayStream(t, &recSyncHandler{})
	_, err := exchange(t, stream, withMetadata(syncChunk(0, "x", &clusterv1.PartInfo{Id: 1, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 2}}})))
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestLocalChunkedSync_VersionCheck(t *testing.T) {
	tests := []struct {
		info *clusterv1.VersionInfo
		name string
		want clusterv1.SyncStatus
	}{
		{
			name: "current", info: &clusterv1.VersionInfo{ApiVersion: apiversion.Version, FileFormatVersion: fileformat.CurrentVersion},
			want: clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
		},
		{
			name: "unknown format", info: &clusterv1.VersionInfo{ApiVersion: apiversion.Version, FileFormatVersion: "0.0.1"},
			want: clusterv1.SyncStatus_SYNC_STATUS_FORMAT_VERSION_MISMATCH,
		},
		{
			name: "unknown api", info: &clusterv1.VersionInfo{ApiVersion: "0.0.1", FileFormatVersion: fileformat.CurrentVersion},
			want: clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &recSyncHandler{}
			stream := relayStream(t, h)
			req := withMetadata(syncChunk(0, "x", &clusterv1.PartInfo{Id: 1, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}}))
			req.VersionInfo = tt.info
			resp, err := exchange(t, stream, req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, resp.GetStatus())
			if tt.want != clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED {
				assert.NotEmpty(t, resp.GetError())
				// The frame is refused before any handler sees it.
				assert.Empty(t, h.recorded())
			}
		})
	}
}

func TestLocalChunkedSync_ServerBusy(t *testing.T) {
	h := &recSyncHandler{failChunk: ErrServerBusy}
	stream := relayStream(t, h)
	resp, err := exchange(t, stream, withMetadata(syncChunk(0, "x", &clusterv1.PartInfo{Id: 1, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}})))
	require.NoError(t, err)
	assert.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SERVER_BUSY, resp.GetStatus())
	assert.NotEmpty(t, resp.GetError())
	// The refusal ends the session; the deferred cleanup closes the unfinished part, never finishing it.
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{`create id=1 type="" group=g shard=3 min=1`, `close id=1`}, h.recorded())
}

func TestLocalChunkedSync_ClientCloseWithoutCompletionClosesPart(t *testing.T) {
	h := &recSyncHandler{}
	stream := relayStream(t, h)
	_, err := exchange(t, stream, withMetadata(syncChunk(0, "x", &clusterv1.PartInfo{Id: 1, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}})))
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.Error(t, err) // io.EOF
	assert.Equal(t, `close id=1`, h.recorded()[len(h.recorded())-1])
	assert.NotContains(t, h.recorded(), `finish id=1`)
}
