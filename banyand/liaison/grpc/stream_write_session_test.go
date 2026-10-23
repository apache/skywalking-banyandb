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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// streamWriteHarness is an in-process gRPC server running the real stream Write handler against fake data nodes.
type streamWriteHarness struct {
	client  streamv1.StreamServiceClient
	fake    *fakePublishers
	service *streamService
	conn    *grpclib.ClientConn
}

func newStreamWriteHarness(t *testing.T, cfg writeSessionConfig) *streamWriteHarness {
	t.Helper()
	id := identity{group: "g", name: "s"}
	er := newEmptyEntityRepo()
	// The entity is the first tag of the first family.
	er.entitiesMap[id] = partition.Locator{TagLocators: []partition.TagLocator{{FamilyOffset: 0, TagOffset: 0}}}
	gr := &groupRepo{
		resourceOpts: map[string]*commonv1.ResourceOpts{"g": {ShardNum: 4}},
		inflight:     make(map[string]*groupInflight),
	}
	fake := &fakePublishers{}
	svc := &streamService{
		discoveryService: &discoveryService{
			entityRepo:      er,
			groupRepo:       gr,
			nodeRegistry:    fakeNodeRegistry{},
			shardingKeyRepo: &shardingKeyRepo{shardingKeysMap: make(map[identity]partition.Locator)},
		},
		pipeline:    fakePipeline{fake: fake},
		l:           logger.GetLogger("test"),
		metrics:     newBypassMetrics(),
		writeConfig: &cfg,
	}
	const window = 65535
	srv := grpclib.NewServer(grpclib.InitialWindowSize(window), grpclib.InitialConnWindowSize(window))
	streamv1.RegisterStreamServiceServer(srv, svc)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpclib.NewClient(lis.Addr().String(), grpclib.WithTransportCredentials(insecure.NewCredentials()),
		grpclib.WithInitialWindowSize(window), grpclib.WithInitialConnWindowSize(window))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return &streamWriteHarness{client: streamv1.NewStreamServiceClient(conn), fake: fake, service: svc, conn: conn}
}

// streamRow builds a stream element whose single tag is the entity, so rows with the same entity share a node.
func streamRow(messageID uint64, withMetadata bool, spec []*streamv1.TagFamilySpec) *streamv1.WriteRequest {
	req := &streamv1.WriteRequest{
		MessageId: messageID,
		Element: &streamv1.ElementValue{
			ElementId: fmt.Sprintf("e-%d", messageID),
			Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "same-entity"}}}},
			}},
		},
		TagFamilySpec: spec,
	}
	if withMetadata {
		req.Metadata = &commonv1.Metadata{Group: "g", Name: "s"}
	}
	return req
}

func streamSpec(tagNames ...string) []*streamv1.TagFamilySpec {
	return []*streamv1.TagFamilySpec{{Name: "searchable", TagNames: tagNames}}
}

func TestStreamWriteSendThenReadClientDoesNotDeadlock(t *testing.T) {
	const total = 200000
	h := newStreamWriteHarness(t, writeSessionConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	spec := streamSpec("svc")
	start := time.Now()
	for id := uint64(1); id <= total; id++ {
		var rowSpec []*streamv1.TagFamilySpec
		if id == 1 {
			rowSpec = spec
		}
		require.NoError(t, stream.Send(streamRow(id, id == 1, rowSpec)))
	}
	require.NoError(t, stream.CloseSend())
	t.Logf("sent %d rows before reading in %s", total, time.Since(start))
	seen := make(map[uint64]struct{}, total)
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED.String(), resp.GetStatus())
		seen[resp.GetMessageId()] = struct{}{}
	}
	assert.Len(t, seen, total)
	rows := 0
	for _, b := range h.fake.attempts() {
		rows += len(b.payloads)
	}
	assert.Equal(t, total, rows)
}

func TestStreamWriteRepeatedMetadataAndSpecShareOneBatch(t *testing.T) {
	h := newStreamWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	// Like many clients, send the same metadata and tag family spec on every message;
	// all rows go to one node, so they must stay in one batch.
	const rows = 10
	for id := uint64(1); id <= rows; id++ {
		require.NoError(t, stream.Send(streamRow(id, true, streamSpec("svc", "k"))))
	}
	require.NoError(t, stream.CloseSend())
	for i := 0; i < rows; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
	}
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
	batches := h.fake.attempts()
	require.Len(t, batches, 1, "repeating identical metadata and spec must not seal the batch")
	require.Len(t, batches[0].payloads, rows)
}

func TestStreamWriteSpecSwitchLandsOnNextBatch(t *testing.T) {
	h := newStreamWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	specA := streamSpec("svc", "k")
	specB := streamSpec("k", "svc")
	// Rows 1-2 use specA; row 3 switches to specB, so it cannot share a batch with them.
	require.NoError(t, stream.Send(streamRow(1, true, specA)))
	require.NoError(t, stream.Send(streamRow(2, false, nil)))
	require.NoError(t, stream.Send(streamRow(3, false, specB)))
	require.NoError(t, stream.Send(streamRow(4, false, nil)))
	require.NoError(t, stream.CloseSend())
	for i := 0; i < 4; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
	}
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)

	batches := h.fake.attempts()
	require.Len(t, batches, 2)
	decode := func(payload []byte) *streamv1.InternalWriteRequest {
		iwr := &streamv1.InternalWriteRequest{}
		require.NoError(t, proto.Unmarshal(payload, iwr))
		return iwr
	}
	specs := map[string]uint64{}
	for _, b := range batches {
		require.Len(t, b.payloads, 2)
		first := decode(b.payloads[0])
		assert.Equal(t, "g", first.GetRequest().GetMetadata().GetGroup(), "every batch carries the metadata on its first message")
		assert.NotEmpty(t, first.GetEntityValues(), "the entity values travel with every message")
		later := decode(b.payloads[1])
		assert.Nil(t, later.GetRequest().GetMetadata())
		assert.Nil(t, later.GetRequest().GetTagFamilySpec())
		assert.Equal(t, fmt.Sprintf("e-%d", later.GetRequest().GetMessageId()), later.GetRequest().GetElement().GetElementId())
		specs[fmt.Sprint(first.GetRequest().GetTagFamilySpec()[0].GetTagNames())] = first.GetRequest().GetMessageId()
	}
	assert.Equal(t, uint64(1), specs["[svc k]"])
	assert.Equal(t, uint64(3), specs["[k svc]"])
}

func TestStreamWritePressureTimeoutEndsStreamWithResourceExhausted(t *testing.T) {
	h := newStreamWriteHarness(t, writeSessionConfig{
		sealRows: 2, pressureTimeout: 200 * time.Millisecond, retryBackoff: 20 * time.Millisecond, retryBackoffMax: 20 * time.Millisecond,
	})
	h.fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
		return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	for id := uint64(1); id <= 2; id++ {
		var spec []*streamv1.TagFamilySpec
		if id == 1 {
			spec = streamSpec("svc")
		}
		require.NoError(t, stream.Send(streamRow(id, id == 1, spec)))
	}
	// The client keeps the stream open and only reads: the server must end it by itself.
	var got []*streamv1.WriteResponse
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			assert.Equal(t, codes.ResourceExhausted, status.Code(recvErr))
			break
		}
		got = append(got, resp)
	}
	require.Len(t, got, 2)
	for _, resp := range got {
		assert.Equal(t, modelv1.Status_STATUS_SERVER_BUSY, resp.GetStatusCode())
	}
}

func TestStreamWriteLiaisonMemoryPressureSetsRetryAfterWhileStreamStaysOpen(t *testing.T) {
	h := newStreamWriteHarness(t, writeSessionConfig{sealRows: 2, sealInterval: time.Hour})
	pm := &togglePressureProtector{}
	h.service.protector = pm
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	// The first row of a pair carries the metadata and spec; metadata is sticky, so the second continues the batch.
	sendRow := func(id uint64, first bool) {
		var spec []*streamv1.TagFamilySpec
		if first {
			spec = streamSpec("svc")
		}
		require.NoError(t, stream.Send(streamRow(id, first, spec)))
	}
	// A row for an unknown stream with a schema revision is rejected on its own, so it exercises the per-row reply path.
	sendMissing := func(id uint64) {
		row := streamRow(id, true, nil)
		row.Metadata = &commonv1.Metadata{Group: "g", Name: "missing", ModRevision: 5}
		require.NoError(t, stream.Send(row))
	}
	recv := func() *streamv1.WriteResponse {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		return resp
	}
	recvByID := func(ids ...uint64) map[uint64]*streamv1.WriteResponse {
		got := make(map[uint64]*streamv1.WriteResponse, len(ids))
		for range ids {
			resp := recv()
			got[resp.GetMessageId()] = resp
		}
		for _, id := range ids {
			require.Contains(t, got, id)
		}
		return got
	}
	wantRetryAfter := uint32(writeMemoryHighRetryAfter.Milliseconds())

	pm.high.Store(true)
	sendMissing(1)
	row := recv()
	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND, row.GetStatusCode())
	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND.String(), row.GetStatus())
	assert.Equal(t, uint64(1), row.GetMessageId())
	assert.Equal(t, wantRetryAfter, row.GetRetryAfterMs(), "a per-row reply asks the client to slow down under pressure")
	sendRow(2, true)
	sendRow(3, false)
	for _, resp := range recvByID(2, 3) {
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode(), "pressure does not change the status of an acknowledgement")
		assert.Equal(t, wantRetryAfter, resp.GetRetryAfterMs(), "a batch acknowledgement asks the client to slow down under pressure")
	}

	pm.high.Store(false)
	sendMissing(4)
	row = recv()
	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND, row.GetStatusCode())
	assert.Zero(t, row.GetRetryAfterMs(), "a per-row reply stops asking once pressure clears")
	sendRow(5, true)
	sendRow(6, false)
	for _, resp := range recvByID(5, 6) {
		assert.Equal(t, modelv1.Status_STATUS_SUCCEED, resp.GetStatusCode())
		assert.Zero(t, resp.GetRetryAfterMs(), "a batch acknowledgement stops asking once pressure clears")
	}

	// The stream was never ended by the pressure: it still ends cleanly on half-close.
	require.NoError(t, stream.CloseSend())
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
}

func TestStreamWriteMetadataRequiredEndsStream(t *testing.T) {
	h := newStreamWriteHarness(t, writeSessionConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(streamRow(7, false, nil)))
	// The reply has no metadata to carry, so the handler ends the stream with an error instead.
	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.NotEqual(t, io.EOF, recvErr)
}
