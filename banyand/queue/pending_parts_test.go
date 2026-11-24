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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
)

// sealingSyncHandler records what the receiver hands parts whose handlers are PartSealers.
// The chunk of part busyOn is refused with ErrServerBusy, SealSync of part failSealOn fails, and
// FinishSync of part failFinishOn fails.
type sealingSyncHandler struct {
	recSyncHandler
	busyOn       uint64
	failSealOn   uint64
	failFinishOn uint64
}

func (h *sealingSyncHandler) CreatePartHandler(ctx *ChunkedSyncPartContext) (PartHandler, error) {
	h.recordf("create id=%d", ctx.ID)
	return &sealingPartHandler{h: h, id: ctx.ID}, nil
}

func (h *sealingSyncHandler) HandleFileChunk(ctx *ChunkedSyncPartContext, _ []byte) error {
	if ctx.ID == h.busyOn {
		return ErrServerBusy
	}
	return nil
}

type sealingPartHandler struct {
	h  *sealingSyncHandler
	id uint64
}

func (p *sealingPartHandler) NewPartType(_ *ChunkedSyncPartContext) error { return nil }

func (p *sealingPartHandler) SealSync() error {
	if p.id == p.h.failSealOn {
		return errors.New("disk full")
	}
	p.h.recordf("seal id=%d", p.id)
	return nil
}

func (p *sealingPartHandler) FinishSync() error {
	if p.id == p.h.failFinishOn {
		return errors.New("disk full")
	}
	p.h.recordf("finish id=%d", p.id)
	return nil
}

func (p *sealingPartHandler) Close() error {
	p.h.recordf("close id=%d", p.id)
	return nil
}

func partChunk(index uint32, partID uint64) *clusterv1.SyncPartRequest {
	return syncChunk(index, "x", &clusterv1.PartInfo{Id: partID, MinTimestamp: 1, Files: []*clusterv1.FileInfo{{Name: "a", Size: 1}}})
}

func TestLocalChunkedSync_SealedPartsWaitForCompletion(t *testing.T) {
	h := &sealingSyncHandler{}
	stream := relayStream(t, h)
	for i, req := range []*clusterv1.SyncPartRequest{withMetadata(partChunk(0, 1)), partChunk(1, 2)} {
		resp, err := exchange(t, stream, req)
		require.NoError(t, err)
		require.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.GetStatus(), "chunk %d", i)
	}
	assert.NotContains(t, h.recorded(), "finish id=1", "a part switch only seals the previous part")

	resp, err := exchange(t, stream, &clusterv1.SyncPartRequest{
		SessionId: "s", ChunkIndex: 2, Content: &clusterv1.SyncPartRequest_Completion{Completion: &clusterv1.SyncCompletion{}},
	})
	require.NoError(t, err)
	require.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SYNC_COMPLETE, resp.GetStatus())
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"finish id=1", "finish id=2",
	}, h.recorded())
}

func TestLocalChunkedSync_FailedSessionIntroducesNoPart(t *testing.T) {
	// The sender resends every part of a failed session, so a part introduced before the
	// failure would be stored twice.
	h := &sealingSyncHandler{busyOn: 3}
	stream := relayStream(t, h)
	for _, req := range []*clusterv1.SyncPartRequest{withMetadata(partChunk(0, 1)), partChunk(1, 2)} {
		resp, err := exchange(t, stream, req)
		require.NoError(t, err)
		require.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_CHUNK_RECEIVED, resp.GetStatus())
	}
	resp, err := exchange(t, stream, partChunk(2, 3))
	require.NoError(t, err)
	require.Equal(t, clusterv1.SyncStatus_SYNC_STATUS_SERVER_BUSY, resp.GetStatus())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"create id=3",
		"close id=3", "close id=1", "close id=2",
	}, h.recorded())
}

func TestPendingParts_CommitDiscardsThePartsAfterAFailure(t *testing.T) {
	h := &sealingSyncHandler{failFinishOn: 2}
	md := &clusterv1.SyncMetadata{Group: "g"}
	pending := &PendingParts{}
	var cur *ChunkedSyncPartContext
	var err error
	for _, id := range []uint64{1, 2, 3} {
		cur, err = SwitchPart(context.Background(), cur, pending, md, &clusterv1.PartInfo{Id: id}, h)
		require.NoError(t, err)
	}
	err = pending.Commit(cur)
	require.Error(t, err)
	assert.Equal(t, "failed to complete part 2: disk full", err.Error())
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"create id=3", "seal id=3",
		"finish id=1", "close id=2", "close id=3",
	}, h.recorded())
	require.NoError(t, pending.Discard(nil), "a committed session holds no part")
	assert.Len(t, h.recorded(), 9)
}

func TestPendingParts_NilFinishesAtOnce(t *testing.T) {
	h := &sealingSyncHandler{}
	var pending *PendingParts
	cur, err := SwitchPart(context.Background(), nil, pending, &clusterv1.SyncMetadata{}, &clusterv1.PartInfo{Id: 1}, h)
	require.NoError(t, err)
	require.NoError(t, pending.Commit(cur))
	require.NoError(t, pending.Discard(nil))
	assert.Equal(t, []string{"create id=1", "finish id=1"}, h.recorded(), fmt.Sprint(h.recorded()))
}

// openParts opens the parts ids in a session, sealing every part but the last, which it returns.
func openParts(t *testing.T, h *sealingSyncHandler, pending *PendingParts, ids ...uint64) *ChunkedSyncPartContext {
	var cur *ChunkedSyncPartContext
	var err error
	for _, id := range ids {
		cur, err = SwitchPart(context.Background(), cur, pending, &clusterv1.SyncMetadata{Group: "g"}, &clusterv1.PartInfo{Id: id}, h)
		require.NoError(t, err)
	}
	return cur
}

func TestPendingParts_DiscardClosesTheOpenPartAndEveryHeldPart(t *testing.T) {
	h := &sealingSyncHandler{}
	pending := &PendingParts{}
	cur := openParts(t, h, pending, 1, 2, 3)

	require.NoError(t, pending.Discard(cur))
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"create id=3",
		"close id=3", "close id=1", "close id=2",
	}, h.recorded(), "one call abandons the whole session, the open part included")
	require.Nil(t, cur.Handler)

	require.NoError(t, pending.Discard(cur), "discarding a discarded session is a no-op")
	assert.Len(t, h.recorded(), 8)
}

func TestPendingParts_NilDiscardStillClosesTheOpenPart(t *testing.T) {
	h := &sealingSyncHandler{}
	var pending *PendingParts
	cur := openParts(t, h, pending, 1)
	require.NoError(t, pending.Discard(cur))
	assert.Equal(t, []string{"create id=1", "close id=1"}, h.recorded())
}

func TestPendingParts_CommitLeavesEveryPartToDiscardWhenTheOpenPartCannotBeSealed(t *testing.T) {
	h := &sealingSyncHandler{failSealOn: 3}
	pending := &PendingParts{}
	cur := openParts(t, h, pending, 1, 2, 3)

	err := pending.Commit(cur)
	require.ErrorContains(t, err, "failed to seal part 3: disk full")
	assert.NotContains(t, h.recorded(), "finish id=1", "a failed seal introduces nothing")
	require.NotNil(t, cur.Handler, "the open part is left to the caller")

	require.NoError(t, pending.Discard(cur))
	assert.Equal(t, []string{
		"create id=1", "seal id=1",
		"create id=2", "seal id=2",
		"create id=3",
		"close id=3", "close id=1", "close id=2",
	}, h.recorded())
}
