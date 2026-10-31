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
	"sync/atomic"
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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// measureWriteHarness is an in-process gRPC server running the real measure Write handler against fake data nodes.
type measureWriteHarness struct {
	client  measurev1.MeasureServiceClient
	fake    *fakePublishers
	service *measureService
	conn    *grpclib.ClientConn
}

func newMeasureWriteHarness(t *testing.T, cfg writeSessionConfig) *measureWriteHarness {
	t.Helper()
	id := identity{group: "g", name: "m"}
	er := newEmptyEntityRepo()
	// The entity is the first tag of the first family.
	er.entitiesMap[id] = partition.Locator{TagLocators: []partition.TagLocator{{FamilyOffset: 0, TagOffset: 0}}}
	gr := &groupRepo{
		resourceOpts: map[string]*commonv1.ResourceOpts{"g": {ShardNum: 4}},
		inflight:     make(map[string]*groupInflight),
	}
	fake := &fakePublishers{}
	svc := &measureService{
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
	measurev1.RegisterMeasureServiceServer(srv, svc)
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
	return &measureWriteHarness{client: measurev1.NewMeasureServiceClient(conn), fake: fake, service: svc, conn: conn}
}

// measureRow builds a data point whose single tag is the entity, so rows with the same entity share a node.
func measureRow(messageID uint64, withMetadata bool, spec *measurev1.DataPointSpec) *measurev1.WriteRequest {
	req := &measurev1.WriteRequest{
		MessageId: messageID,
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "same-entity"}}}},
			}},
			Fields: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(messageID)}}}},
		},
		DataPointSpec: spec,
	}
	if withMetadata {
		req.Metadata = &commonv1.Metadata{Group: "g", Name: "m"}
	}
	return req
}

func measureSpec(tagNames ...string) *measurev1.DataPointSpec {
	return &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "searchable", TagNames: tagNames}},
		FieldNames:    []string{"value"},
	}
}

func decodeMeasureRow(t *testing.T, payload []byte) *measurev1.InternalWriteRequest {
	t.Helper()
	iwr := &measurev1.InternalWriteRequest{}
	require.NoError(t, proto.Unmarshal(payload, iwr))
	return iwr
}

func TestMeasureWriteSendThenReadClientDoesNotDeadlock(t *testing.T) {
	const total = 200000
	h := newMeasureWriteHarness(t, writeSessionConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	spec := measureSpec("svc")
	start := time.Now()
	for id := uint64(1); id <= total; id++ {
		var rowSpec *measurev1.DataPointSpec
		if id == 1 {
			rowSpec = spec
		}
		require.NoError(t, stream.Send(measureRow(id, id == 1, rowSpec)))
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

func TestMeasureWriteAcknowledgesEachSealedBatchWhileStreamStaysOpen(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{sealInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	for round := uint64(0); round < 3; round++ {
		first, second := round*2+1, round*2+2
		require.NoError(t, stream.Send(measureRow(first, round == 0, measureSpecFor(round == 0))))
		require.NoError(t, stream.Send(measureRow(second, false, nil)))
		// The client has not half-closed: both acknowledgements must arrive on their own.
		got := map[uint64]modelv1.Status{}
		for i := 0; i < 2; i++ {
			resp, recvErr := stream.Recv()
			require.NoError(t, recvErr)
			got[resp.GetMessageId()] = resp.GetStatusCode()
		}
		assert.Equal(t, map[uint64]modelv1.Status{first: modelv1.Status_STATUS_SUCCEED, second: modelv1.Status_STATUS_SUCCEED}, got)
	}
	require.NoError(t, stream.CloseSend())
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
}

func measureSpecFor(withSpec bool) *measurev1.DataPointSpec {
	if !withSpec {
		return nil
	}
	return measureSpec("svc")
}

func TestMeasureWriteHalfCloseAcknowledgesRowsHeldInTheOpenBatch(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(measureRow(1, true, measureSpec("svc"))))
	require.NoError(t, stream.Send(measureRow(2, false, nil)))
	require.NoError(t, stream.CloseSend())
	got := map[uint64]modelv1.Status{}
	for i := 0; i < 2; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		got[resp.GetMessageId()] = resp.GetStatusCode()
	}
	assert.Len(t, got, 2)
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
	require.Len(t, h.fake.attempts(), 1)
}

func TestMeasureWriteRepeatedMetadataAndSpecShareOneBatch(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	// Like many clients, send the same metadata and data point spec on every message;
	// all rows go to one node, so they must stay in one batch.
	const rows = 10
	for id := uint64(1); id <= rows; id++ {
		require.NoError(t, stream.Send(measureRow(id, true, measureSpec("svc", "k"))))
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

func TestMeasureWriteSpecSwitchLandsOnNextBatch(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{sealInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	specA := measureSpec("svc", "k")
	specB := measureSpec("k", "svc")
	// Rows 1-2 use specA; row 3 switches to specB, so it cannot share a batch with them.
	require.NoError(t, stream.Send(measureRow(1, true, specA)))
	require.NoError(t, stream.Send(measureRow(2, false, nil)))
	require.NoError(t, stream.Send(measureRow(3, false, specB)))
	require.NoError(t, stream.Send(measureRow(4, false, nil)))
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
	specs := map[string]uint64{}
	for _, b := range batches {
		require.Len(t, b.payloads, 2)
		first := decodeMeasureRow(t, b.payloads[0])
		assert.Equal(t, "g", first.GetRequest().GetMetadata().GetGroup(), "every batch carries the metadata on its first message")
		assert.NotEmpty(t, first.GetEntityValues(), "the entity values travel with every message")
		later := decodeMeasureRow(t, b.payloads[1])
		assert.Nil(t, later.GetRequest().GetMetadata())
		assert.Nil(t, later.GetRequest().GetDataPointSpec())
		assert.Equal(t, int64(later.GetRequest().GetMessageId()), later.GetRequest().GetDataPoint().GetFields()[0].GetInt().GetValue())
		specs[fmt.Sprint(first.GetRequest().GetDataPointSpec().GetTagFamilySpec()[0].GetTagNames())] = first.GetRequest().GetMessageId()
	}
	assert.Equal(t, uint64(1), specs["[svc k]"])
	assert.Equal(t, uint64(3), specs["[k svc]"])
}

func TestMeasureWriteKeysAcknowledgementByMessageIDAndDerivesVersion(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{sealInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	explicit := measureRow(42, true, measureSpec("svc"))
	explicit.DataPoint.Version = 7
	require.NoError(t, stream.Send(explicit))
	derived := measureRow(43, false, nil)
	require.NoError(t, stream.Send(derived))
	require.NoError(t, stream.CloseSend())
	got := map[uint64]*measurev1.WriteResponse{}
	for i := 0; i < 2; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		got[resp.GetMessageId()] = resp
	}
	require.Contains(t, got, uint64(42))
	require.Contains(t, got, uint64(43))
	assert.Equal(t, "g", got[42].GetMetadata().GetGroup())
	versions := map[uint64]int64{}
	for _, b := range h.fake.attempts() {
		for _, p := range b.payloads {
			req := decodeMeasureRow(t, p).GetRequest()
			versions[req.GetMessageId()] = req.GetDataPoint().GetVersion()
		}
	}
	assert.Equal(t, int64(7), versions[42], "an explicit data point version is kept")
	assert.Equal(t, int64(43), versions[43], "a missing version derives from the message id")
}

func TestMeasureWriteBusyBatchIsRetriedAndAcknowledgedOnce(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{
		sealInterval: 10 * time.Millisecond, retryBackoff: 10 * time.Millisecond, retryBackoffMax: 10 * time.Millisecond,
	})
	var closes atomic.Int32
	h.fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
		if closes.Add(1) <= 2 {
			return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
		}
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(measureRow(1, true, measureSpec("svc"))))
	require.NoError(t, stream.Send(measureRow(2, false, nil)))
	got := map[uint64]modelv1.Status{}
	for i := 0; i < 2; i++ {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		got[resp.GetMessageId()] = resp.GetStatusCode()
	}
	assert.Equal(t, map[uint64]modelv1.Status{1: modelv1.Status_STATUS_SUCCEED, 2: modelv1.Status_STATUS_SUCCEED}, got)
	require.NoError(t, stream.CloseSend())
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)
	assert.GreaterOrEqual(t, len(h.fake.attempts()), 3, "the busy batch is retried until the node accepts it")
}

func TestMeasureWritePressureTimeoutEndsStreamWithResourceExhausted(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{
		pressureTimeout: 200 * time.Millisecond, retryBackoff: 20 * time.Millisecond, retryBackoffMax: 20 * time.Millisecond,
	})
	h.fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
		return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	for id := uint64(1); id <= 2; id++ {
		var spec *measurev1.DataPointSpec
		if id == 1 {
			spec = measureSpec("svc")
		}
		require.NoError(t, stream.Send(measureRow(id, id == 1, spec)))
	}
	// The client keeps the stream open and only reads: the server must end it by itself.
	var got []*measurev1.WriteResponse
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

func TestMeasureWriteLiaisonMemoryPressureSetsRetryAfterWhileStreamStaysOpen(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{sealInterval: 20 * time.Millisecond})
	pm := &togglePressureProtector{}
	h.service.protector = pm
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	sendRow := func(id uint64, first bool) {
		var spec *measurev1.DataPointSpec
		if first {
			spec = measureSpec("svc")
		}
		require.NoError(t, stream.Send(measureRow(id, first, spec)))
	}
	// A row for an unknown measure with a schema revision is rejected on its own, so it exercises the per-row reply path.
	sendMissing := func(id uint64) {
		row := measureRow(id, true, nil)
		row.Metadata = &commonv1.Metadata{Group: "g", Name: "missing", ModRevision: 5}
		require.NoError(t, stream.Send(row))
	}
	recv := func() *measurev1.WriteResponse {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		return resp
	}
	recvByID := func(ids ...uint64) map[uint64]*measurev1.WriteResponse {
		got := make(map[uint64]*measurev1.WriteResponse, len(ids))
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

func TestMeasureWriteMetadataRequiredEndsStream(t *testing.T) {
	h := newMeasureWriteHarness(t, writeSessionConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := h.client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(measureRow(7, false, nil)))
	// The reply has no metadata to carry, so the handler ends the stream with an error instead.
	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.NotEqual(t, io.EOF, recvErr)
}
