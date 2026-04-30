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

package property

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// nodeStatusFixture builds a NodeSchemaStatusServer backed by a fresh schemaCache
// pre-loaded with the supplied entries. The notifiedModRevision watermark is
// advanced to the highest entry revision so every entry is downstream-coherent
// (the production path advances the watermark only after handlers process the
// event; tests bypass the handler chain by setting it directly).
func nodeStatusFixture(t *testing.T, entries map[string]*cacheEntry) *NodeSchemaStatusServer {
	t.Helper()
	c := newSchemaCache()
	var maxRev int64
	for propID, entry := range entries {
		c.entries[propID] = entry
		if entry.modRevision > maxRev {
			maxRev = entry.modRevision
		}
		if entry.latestUpdateAt > c.latestUpdateAt {
			c.latestUpdateAt = entry.latestUpdateAt
		}
	}
	c.notifiedModRevision = maxRev
	return NewNodeSchemaStatusServer(func() *schemaCache { return c })
}

const testGroup = "g1"

func makeEntry(kind schema.Kind, name string, rev int64) (string, *cacheEntry) {
	propID := kind.String() + "_"
	if kind == schema.KindGroup {
		propID += name
	} else {
		propID += testGroup + "/" + name
	}
	return propID, &cacheEntry{
		group:          testGroup,
		name:           name,
		kind:           kind,
		modRevision:    rev,
		latestUpdateAt: rev,
	}
}

func TestGetMaxRevision_ReflectsLatestApply(t *testing.T) {
	id, entry := makeEntry(schema.KindMeasure, "m1", 42)
	srv := nodeStatusFixture(t, map[string]*cacheEntry{id: entry})

	resp, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(42), resp.GetMaxModRevision())
}

func TestGetMaxRevision_UnchangedByQuery(t *testing.T) {
	id, entry := makeEntry(schema.KindStream, "s1", 100)
	srv := nodeStatusFixture(t, map[string]*cacheEntry{id: entry})

	first, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)
	second, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)
	third, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)

	assert.Equal(t, first.GetMaxModRevision(), second.GetMaxModRevision())
	assert.Equal(t, second.GetMaxModRevision(), third.GetMaxModRevision())
}

func TestGetMaxRevision_NilCache_ReturnsZero(t *testing.T) {
	srv := NewNodeSchemaStatusServer(func() *schemaCache { return nil })

	resp, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.GetMaxModRevision())
}

func TestGetKeyRevisions_PresentAndAbsent(t *testing.T) {
	idMeasure, entryMeasure := makeEntry(schema.KindMeasure, "m1", 50)
	idStream, entryStream := makeEntry(schema.KindStream, "s1", 70)
	srv := nodeStatusFixture(t, map[string]*cacheEntry{
		idMeasure: entryMeasure,
		idStream:  entryStream,
	})

	req := &clusterv1.GetKeyRevisionsRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "measure", Group: "g1", Name: "m1"},     // present, rev 50
			{Kind: "measure", Group: "g1", Name: "absent"}, // absent
			{Kind: "stream", Group: "g1", Name: "s1"},      // present, rev 70
			{Kind: "trace", Group: "g1", Name: "absent"},   // absent (group with no entries)
		},
	}

	resp, err := srv.GetKeyRevisions(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, resp.GetRevisions(), 4, "response preserves input order and length")

	assert.True(t, resp.Revisions[0].GetPresent())
	assert.Equal(t, int64(50), resp.Revisions[0].GetModRevision())
	assert.Equal(t, "m1", resp.Revisions[0].GetKey().GetName())

	assert.False(t, resp.Revisions[1].GetPresent())
	assert.Equal(t, int64(0), resp.Revisions[1].GetModRevision())

	assert.True(t, resp.Revisions[2].GetPresent())
	assert.Equal(t, int64(70), resp.Revisions[2].GetModRevision())

	assert.False(t, resp.Revisions[3].GetPresent())
	assert.Equal(t, int64(0), resp.Revisions[3].GetModRevision())
}

func TestGetKeyRevisions_UnknownKind_ReportedAbsent(t *testing.T) {
	srv := nodeStatusFixture(t, map[string]*cacheEntry{})

	req := &clusterv1.GetKeyRevisionsRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "not_a_real_kind", Group: "g1", Name: "x"},
		},
	}

	resp, err := srv.GetKeyRevisions(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, resp.GetRevisions(), 1)
	assert.False(t, resp.Revisions[0].GetPresent())
}

func TestGetKeyRevisions_NotYetNotified_ReportedAbsent(t *testing.T) {
	// Entry exists in cache map but its mod_revision exceeds notifiedModRevision —
	// downstream handlers have not yet processed it. Per the gate semantics in
	// GetKeyModRevision, the server must report it as not present.
	c := newSchemaCache()
	id, entry := makeEntry(schema.KindMeasure, "m1", 100)
	c.entries[id] = entry
	c.notifiedModRevision = 50 // below entry's rev
	srv := NewNodeSchemaStatusServer(func() *schemaCache { return c })

	req := &clusterv1.GetKeyRevisionsRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "measure", Group: "g1", Name: "m1"},
		},
	}

	resp, err := srv.GetKeyRevisions(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, resp.GetRevisions(), 1)
	assert.False(t, resp.Revisions[0].GetPresent(),
		"entry not yet notified to downstream handlers must report Present=false")
	assert.Equal(t, int64(0), resp.Revisions[0].GetModRevision())
}

func TestGetAbsentKeys_PostDelete(t *testing.T) {
	// Two entries exist; one will be removed; the third reference is to an
	// entry that never existed. Expectation: the deleted key and the
	// never-existed key end up in absent_keys; the surviving key in
	// still_present_keys.
	idAlive, entryAlive := makeEntry(schema.KindMeasure, "alive", 30)
	idDead, entryDead := makeEntry(schema.KindMeasure, "dead", 40)
	c := newSchemaCache()
	c.entries[idAlive] = entryAlive
	c.entries[idDead] = entryDead
	c.notifiedModRevision = 40
	c.latestUpdateAt = 40
	// Simulate a delete event for the "dead" key at a higher revision.
	require.True(t, c.Delete(idDead, 50))
	c.AdvanceNotified(50)
	srv := NewNodeSchemaStatusServer(func() *schemaCache { return c })

	req := &clusterv1.GetAbsentKeysRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "measure", Group: "g1", Name: "alive"},
			{Kind: "measure", Group: "g1", Name: "dead"},
			{Kind: "measure", Group: "g1", Name: "never"},
		},
	}

	resp, err := srv.GetAbsentKeys(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, resp.GetStillPresentKeys(), 1)
	assert.Equal(t, "alive", resp.GetStillPresentKeys()[0].GetName())

	require.Len(t, resp.GetAbsentKeys(), 2)
	absentNames := []string{
		resp.GetAbsentKeys()[0].GetName(),
		resp.GetAbsentKeys()[1].GetName(),
	}
	assert.Contains(t, absentNames, "dead")
	assert.Contains(t, absentNames, "never")
}

func TestGetAbsentKeys_NilCache_AllStillPresent(t *testing.T) {
	// A nil cache means the node has not observed any schema state. The node
	// must therefore report every requested key as still-present so the
	// liaison's AwaitSchemaDeleted barrier keeps polling rather than falsely
	// concluding deletion has been applied. Mirrors the Phase 1 collectPresentKeys
	// nil-cache contract in banyand/liaison/grpc/barrier.go.
	srv := NewNodeSchemaStatusServer(func() *schemaCache { return nil })

	req := &clusterv1.GetAbsentKeysRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "measure", Group: "g1", Name: "x"},
			{Kind: "stream", Group: "g1", Name: "y"},
		},
	}

	resp, err := srv.GetAbsentKeys(context.Background(), req)
	require.NoError(t, err)
	assert.Len(t, resp.GetStillPresentKeys(), 2,
		"nil cache reports every requested key as still-present so the liaison barrier keeps polling")
	assert.Empty(t, resp.GetAbsentKeys())
}

func TestGetKeyRevisions_OverLimit_RejectsWithInvalidArgument(t *testing.T) {
	srv := nodeStatusFixture(t, map[string]*cacheEntry{})
	keys := make([]*schemav1.SchemaKey, nodeStatusMaxKeys+1)
	for i := range keys {
		keys[i] = &schemav1.SchemaKey{Kind: "measure", Group: "g1", Name: strconv.Itoa(i)}
	}

	resp, err := srv.GetKeyRevisions(context.Background(), &clusterv1.GetKeyRevisionsRequest{Keys: keys})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetAbsentKeys_OverLimit_RejectsWithInvalidArgument(t *testing.T) {
	srv := nodeStatusFixture(t, map[string]*cacheEntry{})
	keys := make([]*schemav1.SchemaKey, nodeStatusMaxKeys+1)
	for i := range keys {
		keys[i] = &schemav1.SchemaKey{Kind: "measure", Group: "g1", Name: strconv.Itoa(i)}
	}

	resp, err := srv.GetAbsentKeys(context.Background(), &clusterv1.GetAbsentKeysRequest{Keys: keys})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetKeyRevisions_ChunkedAtLimit(t *testing.T) {
	// Server accepts a request well above the typical 1000-key chunk size used
	// by the liaison fan-out. The proto does not enforce a hard cap on
	// GetKeyRevisions — chunking is the caller's responsibility — so the server
	// must process whatever it is handed without truncation or error.
	const total = 1500
	entries := make(map[string]*cacheEntry, total)
	keys := make([]*schemav1.SchemaKey, total)
	for i := range total {
		name := "m" + strconv.Itoa(i)
		id, entry := makeEntry(schema.KindMeasure, name, int64(i+1))
		entries[id] = entry
		keys[i] = &schemav1.SchemaKey{Kind: "measure", Group: "g1", Name: name}
	}
	srv := nodeStatusFixture(t, entries)

	resp, err := srv.GetKeyRevisions(context.Background(), &clusterv1.GetKeyRevisionsRequest{Keys: keys})
	require.NoError(t, err)
	require.Len(t, resp.GetRevisions(), total)
	for i := range total {
		assert.True(t, resp.Revisions[i].GetPresent(), "key %d must be present", i)
		assert.Equal(t, int64(i+1), resp.Revisions[i].GetModRevision(),
			"key %d carries its assigned revision", i)
	}
}

func TestGetKeyRevisions_EmptyRequest_ReturnsEmpty(t *testing.T) {
	srv := nodeStatusFixture(t, map[string]*cacheEntry{})

	resp, err := srv.GetKeyRevisions(context.Background(), &clusterv1.GetKeyRevisionsRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.GetRevisions(),
		"empty key list yields empty response without panic")
}
