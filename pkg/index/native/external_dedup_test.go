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

package native

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// receiveExternalSegment admits one external segment payload into owner and
// requires the receive to succeed.
func receiveExternalSegment(t *testing.T, owner *Owner, payload []byte) {
	t.Helper()
	streamer, err := owner.EnableExternalSegments()
	require.NoError(t, err)
	require.NoError(t, streamer.StartSegment())
	require.NoError(t, streamer.WriteChunk(payload))
	require.NoError(t, streamer.CompleteSegment())
	require.Equal(t, "complete", streamer.Status())
}

func externalDuplicatePayload(t *testing.T) []byte {
	t.Helper()
	payload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("doc-1"), Fields: []nativeice.EncodeField{{Name: "status", Value: []byte("incoming"), Store: true, Index: true}}},
		{Identifier: []byte("doc-2"), Fields: []nativeice.EncodeField{{Name: "status", Value: []byte("external"), Store: true, Index: true}}},
	}})
	require.NoError(t, err)
	return payload
}

func TestOwnerExternalDedupKeepExistingMasksIncomingDuplicate(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, ExternalDedup: ExternalDedupKeepExisting})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("doc-1"), Fields: []Field{{Name: "status", Value: []byte("existing"), Store: true, Index: true}},
	}}}))
	receiveExternalSegment(t, owner, externalDuplicatePayload(t))

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	// The existing document must win: the incoming duplicate for doc-1 is
	// masked in the incoming segment's own deletion bitmap.
	document, found, err := view.Lookup(context.Background(), []byte("doc-1"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("existing"), document.Fields[0].Value)
	matches, err := view.MatchTerms(context.Background(), MatchRequest{Field: "status", Term: []byte("incoming")})
	require.NoError(t, err)
	require.Empty(t, matches.Identifiers, "the masked incoming copy of doc-1 must never become live")
	// An identifier the incoming segment alone carries is unaffected.
	other, found, err := view.Lookup(context.Background(), []byte("doc-2"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("external"), other.Fields[0].Value)
}

func TestOwnerExternalDedupNoneKeepsBothCopiesLive(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("doc-1"), Fields: []Field{{Name: "status", Value: []byte("existing"), Store: true, Index: true}},
	}}}))
	receiveExternalSegment(t, owner, externalDuplicatePayload(t))

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	existingMatch, err := view.MatchTerms(context.Background(), MatchRequest{Field: "status", Term: []byte("existing")})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("doc-1")}, existingMatch.Identifiers)
	incomingMatch, err := view.MatchTerms(context.Background(), MatchRequest{Field: "status", Term: []byte("incoming")})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("doc-1")}, incomingMatch.Identifiers, "ExternalDedupNone leaves both copies live and neither masked")
}

func TestOwnerExternalDedupKeepExistingWithNoExistingDuplicateAdmitsEverything(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, ExternalDedup: ExternalDedupKeepExisting})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	receiveExternalSegment(t, owner, externalDuplicatePayload(t))

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for _, id := range []string{"doc-1", "doc-2"} {
		_, found, err := view.Lookup(context.Background(), []byte(id))
		require.NoError(t, err)
		require.True(t, found, "%s must be admitted when no existing copy is live", id)
	}
}

// TestOwnerExternalDedupPreferIncomingInvalidatesPresenceCache proves that
// masking an existing document's identifier (because an incoming external
// segment carries the same identifier with a different field set) drops any
// presence-cache entry for it, so a later InsertIfAbsent admission is not
// hidden behind a stale "present with the old fields" answer.
func TestOwnerExternalDedupPreferIncomingInvalidatesPresenceCache(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{}, Path: path, ExternalDedup: ExternalDedupPreferIncoming, PresenceCacheBytes: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("doc-1"), Fields: []Field{statusField("local")}}},
	}))
	payload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("doc-1"), Fields: []nativeice.EncodeField{{Name: "other", Value: []byte("incoming"), Store: true, Index: true}}},
	}})
	require.NoError(t, err)
	receiveExternalSegment(t, owner, payload)

	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("doc-1"), Fields: []Field{statusField("resurrected")}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("doc-1"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "status", document.Fields[0].Name)
	require.Equal(t, []byte("resurrected"), document.Fields[0].Value, "a stale cache entry must not hide that the incoming copy lacked \"status\"")
}
