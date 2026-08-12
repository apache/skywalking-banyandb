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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

func makeObjects(n int) []*fodcv1.SchemaObjectFingerprint {
	objs := make([]*fodcv1.SchemaObjectFingerprint, n)
	for i := range objs {
		objs[i] = &fodcv1.SchemaObjectFingerprint{Kind: "indexRule", Name: "r", Fingerprint: uint64(i)}
	}
	return objs
}

func collectChunks(t *testing.T, group *fodcv1.SchemaRegistryGroup) []*fodcv1.StreamSchemaRegistryRequest {
	t.Helper()
	var sent []*fodcv1.StreamSchemaRegistryRequest
	require.NoError(t, sendSchemaRegistryGroup(group, func(req *fodcv1.StreamSchemaRegistryRequest) error {
		sent = append(sent, req)
		return nil
	}))
	return sent
}

func TestSendSchemaRegistryGroup_ChunksObjectsAndMergesBack(t *testing.T) {
	// 120 objects with a chunk size of 50 -> 50 + 50 + 20 across three messages,
	// each carrying the same group name so the proxy can merge them.
	sent := collectChunks(t, &fodcv1.SchemaRegistryGroup{Group: "g", Objects: makeObjects(120)})

	require.Len(t, sent, 3)
	// Reassemble in arrival order and assert the exact fingerprints survive, in
	// order, across the chunk boundaries (makeObjects sets Fingerprint = index).
	var reassembled []*fodcv1.SchemaObjectFingerprint
	for _, req := range sent {
		assert.Equal(t, "g", req.GetGroup().GetGroup())
		reassembled = append(reassembled, req.GetGroup().GetObjects()...)
	}
	require.Len(t, reassembled, 120, "no object is lost across chunks")
	for i, o := range reassembled {
		require.Equal(t, uint64(i), o.GetFingerprint(), "fingerprints preserved in order across boundaries")
	}
	assert.Len(t, sent[0].GetGroup().GetObjects(), schemaObjectChunkSize)
	assert.Len(t, sent[2].GetGroup().GetObjects(), 20)
}

func TestSendSchemaRegistryGroup_ExactMultipleHasNoTrailingEmptyChunk(t *testing.T) {
	// Exactly schemaObjectChunkSize objects must be ONE chunk, with no extra empty
	// trailing message -- the classic off-by-one an exact multiple would expose.
	sent := collectChunks(t, &fodcv1.SchemaRegistryGroup{Group: "g", Objects: makeObjects(schemaObjectChunkSize)})
	require.Len(t, sent, 1)
	assert.Len(t, sent[0].GetGroup().GetObjects(), schemaObjectChunkSize)
}

func TestSendSchemaRegistryGroup_OneOverMultipleSplitsInTwo(t *testing.T) {
	sent := collectChunks(t, &fodcv1.SchemaRegistryGroup{Group: "g", Objects: makeObjects(schemaObjectChunkSize + 1)})
	require.Len(t, sent, 2)
	assert.Len(t, sent[0].GetGroup().GetObjects(), schemaObjectChunkSize)
	assert.Len(t, sent[1].GetGroup().GetObjects(), 1)
}

func TestSendSchemaRegistryGroup_EmptyGroupStillSendsOneMessage(t *testing.T) {
	sent := collectChunks(t, &fodcv1.SchemaRegistryGroup{Group: "empty"})
	require.Len(t, sent, 1, "an empty successful group is still represented so the proxy sees it")
	assert.Empty(t, sent[0].GetGroup().GetObjects())
	assert.Empty(t, sent[0].GetGroup().GetError())
}

func TestSendSchemaRegistryGroup_FailedGroupSendsErrorOnly(t *testing.T) {
	sent := collectChunks(t, &fodcv1.SchemaRegistryGroup{Group: "broken", Error: "boom"})
	require.Len(t, sent, 1)
	assert.Equal(t, "boom", sent[0].GetGroup().GetError())
	assert.Empty(t, sent[0].GetGroup().GetObjects())
}
