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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// mockSchemaRegistryServer captures the request_registry command the proxy sends.
type mockSchemaRegistryServer struct {
	ctx  context.Context
	sent chan *fodcv1.StreamSchemaRegistryResponse
}

func (m *mockSchemaRegistryServer) Send(resp *fodcv1.StreamSchemaRegistryResponse) error {
	m.sent <- resp
	return nil
}
func (m *mockSchemaRegistryServer) Recv() (*fodcv1.StreamSchemaRegistryRequest, error) { select {} }
func (m *mockSchemaRegistryServer) Context() context.Context                           { return m.ctx }
func (m *mockSchemaRegistryServer) SendMsg(_ interface{}) error                        { return nil }
func (m *mockSchemaRegistryServer) RecvMsg(_ interface{}) error                        { return nil }
func (m *mockSchemaRegistryServer) SetHeader(_ metadata.MD) error                      { return nil }
func (m *mockSchemaRegistryServer) SendHeader(_ metadata.MD) error                     { return nil }
func (m *mockSchemaRegistryServer) SetTrailer(_ metadata.MD)                           {}

func objFP(kind, name string, fp uint64) *fodcv1.SchemaObjectFingerprint {
	return &fodcv1.SchemaObjectFingerprint{Kind: kind, Name: name, Fingerprint: fp}
}

func TestFetchSchemaRegistry_MergesChunksAndEndsOnDone(t *testing.T) {
	srv := &mockSchemaRegistryServer{ctx: context.Background(), sent: make(chan *fodcv1.StreamSchemaRegistryResponse, 1)}
	ac := &agentConnection{agentID: "liaison-0", schemaRegistryStream: srv}

	go func() {
		// Wait until the request_registry command was sent (channel is set up).
		<-srv.sent
		// Group "g" streams its objects across two chunks that must be merged.
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{
			Group: "g", Objects: []*fodcv1.SchemaObjectFingerprint{objFP("group", "g", 1)},
		}})
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{
			Group: "g", Objects: []*fodcv1.SchemaObjectFingerprint{objFP("stream", "foo", 100)},
		}})
		// A failed group carries an error and no objects.
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{
			Group: "broken", Error: "list streams of broken: boom",
		}})
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Done: true})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	groups, err := ac.fetchSchemaRegistry(ctx)

	require.NoError(t, err)
	require.Len(t, groups, 2)
	byName := map[string]*fodcv1.SchemaRegistryGroup{}
	for _, g := range groups {
		byName[g.GetGroup()] = g
	}
	require.Contains(t, byName, "g")
	// Assert the merged objects' identity (kind/name/fingerprint), not just the
	// count -- a lose-then-pad or reorder merge bug would pass a length-only check.
	gObjs := map[string]uint64{}
	for _, o := range byName["g"].GetObjects() {
		gObjs[o.GetKind()+"/"+o.GetName()] = o.GetFingerprint()
	}
	assert.Equal(t, map[string]uint64{"group/g": 1, "stream/foo": 100}, gObjs, "both chunks' exact fingerprints are merged")
	assert.Equal(t, "list streams of broken: boom", byName["broken"].GetError())
	assert.Empty(t, byName["broken"].GetObjects())
}

func TestFetchSchemaRegistry_ObjectChunkThenErrorChunkSetsError(t *testing.T) {
	srv := &mockSchemaRegistryServer{ctx: context.Background(), sent: make(chan *fodcv1.StreamSchemaRegistryResponse, 1)}
	ac := &agentConnection{agentID: "liaison-0", schemaRegistryStream: srv}

	go func() {
		<-srv.sent
		// A group that streamed some objects and then reports an error on a later chunk.
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{
			Group: "g", Objects: []*fodcv1.SchemaObjectFingerprint{objFP("group", "g", 1)},
		}})
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{Group: "g", Error: "late boom"}})
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Done: true})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	groups, err := ac.fetchSchemaRegistry(ctx)

	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "late boom", groups[0].GetError(), "a later error chunk is recorded on the merged group")
}

func TestDeliverSchemaRegistry_AfterFetchGaveUpDoesNotBlock(t *testing.T) {
	srv := &mockSchemaRegistryServer{ctx: context.Background(), sent: make(chan *fodcv1.StreamSchemaRegistryResponse, 1)}
	ac := &agentConnection{agentID: "liaison-0", schemaRegistryStream: srv}

	// Run a fetch that never gets a done marker; it returns on its ctx timeout and
	// clears its channel in the deferred cleanup.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := ac.fetchSchemaRegistry(ctx)
	require.Error(t, err, "the round times out with no done marker")

	// A chunk arriving after the round is over must return promptly, never block.
	done := make(chan struct{})
	go func() {
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{Group: "late"}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deliverSchemaRegistry blocked after the fetch gave up")
	}
}

func TestFetchSchemaRegistry_FatalErrorOnDone(t *testing.T) {
	srv := &mockSchemaRegistryServer{ctx: context.Background(), sent: make(chan *fodcv1.StreamSchemaRegistryResponse, 1)}
	ac := &agentConnection{agentID: "liaison-0", schemaRegistryStream: srv}

	go func() {
		<-srv.sent
		// The fatal whole-read failure is an empty-name group chunk carrying the reason.
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{Error: "dial 127.0.0.1:17912: connection refused"}})
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Done: true})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	groups, err := ac.fetchSchemaRegistry(ctx)

	require.Error(t, err, "the empty-name error sentinel becomes a Go error")
	assert.Contains(t, err.Error(), "connection refused")
	assert.Nil(t, groups)
}

func TestFetchSchemaRegistry_TimesOutWithoutDone(t *testing.T) {
	srv := &mockSchemaRegistryServer{ctx: context.Background(), sent: make(chan *fodcv1.StreamSchemaRegistryResponse, 1)}
	ac := &agentConnection{agentID: "liaison-0", schemaRegistryStream: srv}

	go func() {
		<-srv.sent
		// One chunk, then silence: the round never gets a done marker.
		ac.deliverSchemaRegistry(&fodcv1.StreamSchemaRegistryRequest{Group: &fodcv1.SchemaRegistryGroup{Group: "g"}})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := ac.fetchSchemaRegistry(ctx)

	require.Error(t, err, "a round with no done marker ends on the collection deadline, never hangs")
}

func TestFetchSchemaRegistry_NoStreamIsError(t *testing.T) {
	ac := &agentConnection{agentID: "liaison-0"}
	_, err := ac.fetchSchemaRegistry(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not established")
}

func TestCancelSchemaRegistryFetch_WakesInflightFetchImmediately(t *testing.T) {
	srv := &mockSchemaRegistryServer{ctx: context.Background(), sent: make(chan *fodcv1.StreamSchemaRegistryResponse, 1)}
	ac := &agentConnection{agentID: "liaison-0", schemaRegistryStream: srv}

	go func() {
		<-srv.sent // the fetch has sent its request and is now waiting
		ac.cancelSchemaRegistryFetch()
	}()

	// ctx far longer than the assertion window, so completing proves the cancel
	// woke the fetch rather than the deadline firing. Reads happen-after fetchDone.
	var gotGroups []*fodcv1.SchemaRegistryGroup
	var gotErr error
	fetchDone := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		gotGroups, gotErr = ac.fetchSchemaRegistry(ctx)
		close(fetchDone)
	}()

	select {
	case <-fetchDone:
		require.Error(t, gotErr, "a mid-fetch disconnect wakes the fetch with a fatal error")
		assert.Contains(t, gotErr.Error(), "disconnected")
		assert.Nil(t, gotGroups)
	case <-time.After(3 * time.Second):
		t.Fatal("fetch was not woken by cancelSchemaRegistryFetch; it waited on ctx instead")
	}
}
