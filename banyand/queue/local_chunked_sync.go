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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fileformat"
)

// localChunkedSync keeps the chunked sync handlers a standalone process registers, so that an
// in-process relay can feed them the frames a data node would receive over gRPC.
type localChunkedSync struct {
	handlers map[bus.Topic]ChunkedSyncHandler
	mu       sync.RWMutex
}

func (l *localChunkedSync) register(topic bus.Topic, handler ChunkedSyncHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.handlers == nil {
		l.handlers = make(map[bus.Topic]ChunkedSyncHandler)
	}
	l.handlers[topic] = handler
}

func (l *localChunkedSync) handler(topic string) (ChunkedSyncHandler, error) {
	t, ok := data.TopicMap[topic]
	if !ok {
		return nil, fmt.Errorf("unknown sync topic: %s", topic)
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	h, ok := l.handlers[t]
	if !ok {
		return nil, fmt.Errorf("no handler registered for topic %s", topic)
	}
	return h, nil
}

// localSyncRelayClient is the in-process cluster.v1 ChunkedSyncService of a standalone
// process.
type localSyncRelayClient struct {
	sync *localChunkedSync
}

// SyncPart opens an in-process SyncPart stream served by localChunkedSync.serve.
func (c localSyncRelayClient) SyncPart(ctx context.Context, _ ...grpc.CallOption,
) (grpc.BidiStreamingClient[clusterv1.SyncPartRequest, clusterv1.SyncPartResponse], error) {
	return startLocalBidiStream(ctx, c.sync.serve), nil
}

// localSyncSession is the state of one in-process SyncPart stream.
type localSyncSession struct {
	handler  ChunkedSyncHandler
	metadata *clusterv1.SyncMetadata
	partCtx  *ChunkedSyncPartContext
	chunks   uint32
}

// serve is the receive side of one in-process SyncPart stream. It follows the
// data node receiver: the first frame carries the metadata, every chunk is acknowledged once
// its files reached the handlers, a part id change finishes the previous part, a part type
// change on the same id opens a secondary part, and the completion finishes the last part
// before SYNC_STATUS_SYNC_COMPLETE is sent. Chunks must arrive in order. A refused frame (an
// unreadable version, no metadata yet, an out of order chunk, a checksum mismatch or a busy
// receiver) is answered with the SyncStatus the data node sends and then ends the session.
func (l *localChunkedSync) serve(stream grpc.BidiStreamingServer[clusterv1.SyncPartRequest, clusterv1.SyncPartResponse]) (err error) {
	session := &localSyncSession{}
	refuse := func(req *clusterv1.SyncPartRequest, st clusterv1.SyncStatus, msg string) error {
		return stream.Send(&clusterv1.SyncPartResponse{SessionId: req.GetSessionId(), ChunkIndex: req.GetChunkIndex(), Status: st, Error: msg})
	}
	defer func() {
		if session.partCtx != nil {
			if closeErr := session.partCtx.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
	}()
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		if syncStatus, reason := CheckSyncVersion(req.GetVersionInfo()); syncStatus != clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED {
			return refuse(req, syncStatus, reason)
		}
		if md := req.GetMetadata(); md != nil {
			if session.metadata != nil {
				return status.Error(codes.InvalidArgument, "an in-process sync stream carries one session")
			}
			h, handlerErr := l.handler(md.GetTopic())
			if handlerErr != nil {
				return status.Error(codes.InvalidArgument, handlerErr.Error())
			}
			session.metadata, session.handler = md, h
		}
		if session.metadata == nil {
			return refuse(req, clusterv1.SyncStatus_SYNC_STATUS_SESSION_NOT_FOUND,
				fmt.Sprintf("session %s not found: the first sync frame must carry the metadata", req.GetSessionId()))
		}
		if req.GetCompletion() != nil {
			if finishErr := session.finishPart(); finishErr != nil {
				return finishErr
			}
			return stream.Send(&clusterv1.SyncPartResponse{
				SessionId: req.GetSessionId(), ChunkIndex: req.GetChunkIndex(), Status: clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE,
			})
		}
		if chunkErr := session.chunk(stream.Context(), req); chunkErr != nil {
			var refusal *syncRefusalError
			switch {
			case errors.Is(chunkErr, ErrServerBusy):
				return refuse(req, clusterv1.SyncStatus_SYNC_STATUS_SERVER_BUSY, "receiver under memory pressure, retry later")
			case errors.As(chunkErr, &refusal):
				return refuse(req, refusal.status, refusal.msg)
			default:
				return chunkErr
			}
		}
		if sendErr := stream.Send(&clusterv1.SyncPartResponse{
			SessionId: req.GetSessionId(), ChunkIndex: req.GetChunkIndex(), Status: clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED,
		}); sendErr != nil {
			return sendErr
		}
	}
}

// ChunkChecksum returns the checksum a chunked sync frame carries for its chunk data: the
// lowercase hexadecimal CRC-32 (IEEE) of data.
func ChunkChecksum(data []byte) string {
	return strconv.FormatUint(uint64(crc32.ChecksumIEEE(data)), 16)
}

// CheckSyncVersion is the version check of a chunked sync receiver: a frame without version info
// is accepted, otherwise the API version must match and the file format version must be the
// current or a compatible one. It returns SYNC_STATUS_CHUNK_RECEIVED when the frame may be read,
// and the refusing status with its reason otherwise. The data node receiver and the in-process
// one share it.
func CheckSyncVersion(info *clusterv1.VersionInfo) (clusterv1.SyncStatus, string) {
	if info == nil {
		return clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, ""
	}
	compatible := fileformat.CompatibleVersions()
	formatOK := info.GetFileFormatVersion() == fileformat.CurrentVersion || slices.Contains(compatible, info.GetFileFormatVersion())
	apiOK := info.GetApiVersion() == apiversion.Version
	switch {
	case !apiOK && !formatOK:
		return clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED, fmt.Sprintf(
			"API version %s not supported (server: %s) and file format version %s not compatible (server: %s, supported: %v)",
			info.GetApiVersion(), apiversion.Version, info.GetFileFormatVersion(), fileformat.CurrentVersion, compatible)
	case !apiOK:
		return clusterv1.SyncStatus_SYNC_STATUS_VERSION_UNSUPPORTED,
			fmt.Sprintf("API version %s not supported (server: %s)", info.GetApiVersion(), apiversion.Version)
	case !formatOK:
		return clusterv1.SyncStatus_SYNC_STATUS_FORMAT_VERSION_MISMATCH, fmt.Sprintf("File format version %s not compatible (server: %s, supported: %v)",
			info.GetFileFormatVersion(), fileformat.CurrentVersion, compatible)
	}
	return clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, ""
}

// syncRefusalError is a chunk the in-process receiver refuses with a SyncStatus, as the data node does.
type syncRefusalError struct {
	msg    string
	status clusterv1.SyncStatus
}

func (r *syncRefusalError) Error() string { return r.msg }

// chunk hands the files of one chunk to the handler.
func (s *localSyncSession) chunk(ctx context.Context, req *clusterv1.SyncPartRequest) error {
	if req.GetChunkIndex() != s.chunks {
		return &syncRefusalError{
			status: clusterv1.SyncStatus_SYNC_STATUS_CHUNK_OUT_OF_ORDER,
			msg:    fmt.Sprintf("out of order chunk received: expected %d, got %d", s.chunks, req.GetChunkIndex()),
		}
	}
	if sum := ChunkChecksum(req.GetChunkData()); sum != req.GetChunkChecksum() {
		return &syncRefusalError{
			status: clusterv1.SyncStatus_SYNC_STATUS_CHUNK_CHECKSUM_MISMATCH,
			msg:    fmt.Sprintf("chunk %d checksum mismatch: expected %s, got %s", req.GetChunkIndex(), req.GetChunkChecksum(), sum),
		}
	}
	s.chunks++
	chunkData := req.GetChunkData()
	for _, info := range req.GetPartsInfo() {
		if err := s.switchPart(ctx, info); err != nil {
			return err
		}
		for _, f := range info.GetFiles() {
			end := uint64(f.GetOffset()) + uint64(f.GetSize())
			if end > uint64(len(chunkData)) {
				return status.Errorf(codes.InvalidArgument, "file %s [%d, %d) is outside chunk %d of %d bytes",
					f.GetName(), f.GetOffset(), end, req.GetChunkIndex(), len(chunkData))
			}
			s.partCtx.FileName = f.GetName()
			if err := s.handler.HandleFileChunk(s.partCtx, chunkData[f.GetOffset():end]); err != nil {
				return fmt.Errorf("failed to stream file chunk for %s: %w", f.GetName(), err)
			}
		}
	}
	return nil
}

// switchPart moves the session to the part info describes.
func (s *localSyncSession) switchPart(ctx context.Context, info *clusterv1.PartInfo) (err error) {
	s.partCtx, err = SwitchPart(ctx, s.partCtx, s.metadata, info, s.handler)
	return err
}

// finishPart finishes the open part, if any.
func (s *localSyncSession) finishPart() error {
	return FinishPart(s.partCtx)
}

// SwitchPart returns the context of the part info describes, the one chunked sync receiver step
// shared by the cluster receiver and the in-process relay. A frame continuing the open part cur
// returns cur, telling its handler about a new part type when the type changes; a frame starting
// another part finishes cur and creates the new part's handler. On an error the returned context
// is the one to close: cur when it could not be finished, the new one when its handler could not
// be created.
func SwitchPart(ctx context.Context, cur *ChunkedSyncPartContext, md *clusterv1.SyncMetadata,
	info *clusterv1.PartInfo, handler ChunkedSyncHandler,
) (*ChunkedSyncPartContext, error) {
	if cur != nil && cur.ID == info.GetId() {
		if cur.PartType == info.GetPartType() {
			return cur, nil
		}
		FillPartContext(cur, info)
		if err := cur.Handler.NewPartType(cur); err != nil {
			return cur, fmt.Errorf("failed to new part type: %w", err)
		}
		return cur, nil
	}
	if err := FinishPart(cur); err != nil {
		return cur, err
	}
	next := &ChunkedSyncPartContext{
		Context: ctx,
		ID:      info.GetId(),
		Group:   md.GetGroup(),
		ShardID: md.GetShardId(),
	}
	FillPartContext(next, info)
	h, err := handler.CreatePartHandler(next)
	if err != nil {
		return next, fmt.Errorf("failed to create part handler: %w", err)
	}
	next.Handler = h
	return next, nil
}

// FinishPart finishes the part c holds, if any, and drops its handler.
func FinishPart(c *ChunkedSyncPartContext) error {
	if c == nil || c.Handler == nil {
		return nil
	}
	if err := c.Handler.FinishSync(); err != nil {
		return fmt.Errorf("failed to complete part %d: %w", c.ID, err)
	}
	c.Handler = nil
	return nil
}

// FillPartContext copies the description of a part from the sync protocol into c.
func FillPartContext(c *ChunkedSyncPartContext, info *clusterv1.PartInfo) {
	c.CompressedSizeBytes = info.GetCompressedSizeBytes()
	c.UncompressedSizeBytes = info.GetUncompressedSizeBytes()
	c.TotalCount = info.GetTotalCount()
	c.BlocksCount = info.GetBlocksCount()
	c.MinTimestamp = info.GetMinTimestamp()
	c.MaxTimestamp = info.GetMaxTimestamp()
	c.MinKey = info.GetMinKey()
	c.MaxKey = info.GetMaxKey()
	c.PartType = info.GetPartType()
}
