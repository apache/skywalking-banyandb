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

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// externalSegIdentity marshals one series identity (a fixed subject
// testSubjectSvc plus a single string entity value), the same shape
// buildTestSeriesDocs uses.
func externalSegIdentity(t *testing.T, entityValue string) []byte {
	t.Helper()
	var series pbv1.Series
	series.Subject = testSubjectSvc
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityValue}}}}
	require.NoError(t, series.Marshal())
	return append([]byte(nil), series.Buffer...)
}

// singleSegFile returns the bytes of the one *.seg file under dir.
func singleSegFile(t *testing.T, dir string) []byte {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one segment file in %s", dir)
	data, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	return data
}

// buildNativeExternalSegment creates a fresh native owner at a temp dir,
// admits one document carrying tag "marker"=markerValue under identity, and
// returns the resulting committed segment's raw bytes -- a stand-in for a
// segment the new binary's writer produced.
func buildNativeExternalSegment(t *testing.T, identity []byte, markerValue string) []byte {
	t.Helper()
	dir := t.TempDir()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: &testRootLease{}, Path: dir, IdentifierDocValues: true})
	require.NoError(t, err)
	done := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), native.Batch{
		Documents: []native.Document{{
			Identifier: identity,
			Fields:     []native.Field{{Name: "marker", Value: []byte(markerValue), Store: true}},
		}},
		PersistentCallback: func(err error) { done <- err },
	}))
	require.NoError(t, <-done)
	require.NoError(t, owner.Close())
	return singleSegFile(t, dir)
}

// buildLegacyExternalSegment creates a fresh *previous-release* series store
// (pkg/index/inverted) at a temp dir, inserts one document carrying tag
// "marker"=markerValue under identity, and returns the resulting committed
// segment's raw bytes -- a stand-in for a segment the previous release's
// writer produced. Using the legacy writer here is the same sanctioned
// exception the NIDX-03 fixture generator uses (design §12 item 1): this
// test's whole point is proving the native external receiver still accepts
// what the previous release wrote.
func buildLegacyExternalSegment(t *testing.T, identity []byte, markerValue string) []byte {
	t.Helper()
	dir := t.TempDir()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: dir, BatchWaitSec: 0})
	require.NoError(t, err)
	tag := index.NewBytesField(index.FieldKey{TagName: "marker"}, []byte(markerValue))
	tag.Store = true
	require.NoError(t, store.UpdateSeriesBatch(index.Batch{Documents: index.Documents{{
		Fields: []index.Field{tag}, EntityValues: identity,
	}}}))
	require.NoError(t, store.Close())
	return singleSegFile(t, dir)
}

// receiveExternalSegment drives one StartSegment/WriteChunk/CompleteSegment
// cycle against idx's external-segment receiver with payload as the whole
// (single-chunk) segment body.
func receiveExternalSegment(t *testing.T, idx IndexDB, payload []byte) error {
	t.Helper()
	streamer, err := idx.EnableExternalSegments()
	require.NoError(t, err)
	require.NoError(t, streamer.StartSegment())
	if len(payload) > 0 {
		require.NoError(t, streamer.WriteChunk(payload))
	}
	return streamer.CompleteSegment()
}

// TestSeriesIndex_ExternalReceive_AcceptsLegacyAndNativeSegments pins NIDX-03
// §7/§12 item 7: raw segments written by the previous release and by native
// are both accepted by the same external-segment receiver, and both become
// searchable.
func TestSeriesIndex_ExternalReceive_AcceptsLegacyAndNativeSegments(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	identityNative := externalSegIdentity(t, "native-written")
	identityLegacy := externalSegIdentity(t, "legacy-written")

	nativeSeg := buildNativeExternalSegment(t, identityNative, "from-native")
	legacySeg := buildLegacyExternalSegment(t, identityLegacy, "from-legacy")

	require.NoError(t, receiveExternalSegment(t, si, nativeSeg))
	require.NoError(t, receiveExternalSegment(t, si, legacySeg))

	var seriesNative, seriesLegacy pbv1.Series
	require.NoError(t, seriesNative.Unmarshal(identityNative))
	require.NoError(t, seriesLegacy.Unmarshal(identityLegacy))
	sd, _, err := si.Search(ctx, []*pbv1.Series{&seriesNative, &seriesLegacy}, IndexSearchOpts{
		Projection: []index.FieldKey{{TagName: "marker"}},
	})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 2)
	got := map[string]string{}
	for i, s := range sd.SeriesList {
		got[s.EntityValues[0].GetStr().GetValue()] = string(sd.Fields[i]["marker"])
	}
	require.Equal(t, "from-native", got["native-written"])
	require.Equal(t, "from-legacy", got["legacy-written"])
}

// TestSeriesIndex_ExternalReceive_DuplicateIdentifierKeepsExisting pins
// NIDX-03 §4.3/§7/§12 item 7: a duplicate _id arriving via external receive
// keeps the already-live document (ExternalDedupKeepExisting), matching the
// previous release's receiver behavior.
func TestSeriesIndex_ExternalReceive_DuplicateIdentifierKeepsExisting(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	identity := externalSegIdentity(t, "dup")
	first := buildNativeExternalSegment(t, identity, "first")
	second := buildNativeExternalSegment(t, identity, "second")

	require.NoError(t, receiveExternalSegment(t, si, first))
	require.NoError(t, receiveExternalSegment(t, si, second))

	var series pbv1.Series
	require.NoError(t, series.Unmarshal(identity))
	sd, _, err := si.Search(ctx, []*pbv1.Series{&series}, IndexSearchOpts{Projection: []index.FieldKey{{TagName: "marker"}}})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 1, "a duplicate identifier must not create a second live document")
	require.Equal(t, "first", string(sd.Fields[0]["marker"]), "the existing document must win over the incoming duplicate")
}

// TestSeriesIndex_ExternalReceive_TruncatedSegmentRejectedNothingVisible pins
// NIDX-03 §7/§12 item 7: a truncated (corrupt) segment fails validation
// before introduction and leaves nothing visible.
func TestSeriesIndex_ExternalReceive_TruncatedSegmentRejectedNothingVisible(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	identity := externalSegIdentity(t, "truncated")
	seg := buildNativeExternalSegment(t, identity, "value")
	truncated := seg[:len(seg)/2]

	receiveErr := receiveExternalSegment(t, si, truncated)
	require.Error(t, receiveErr, "a truncated segment must be rejected")

	var series pbv1.Series
	require.NoError(t, series.Unmarshal(identity))
	sd, _, err := si.Search(ctx, []*pbv1.Series{&series}, IndexSearchOpts{})
	require.NoError(t, err)
	require.Empty(t, sd.SeriesList, "a rejected segment must leave nothing visible")
}
