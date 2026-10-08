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

// externalSegmentFixtureDir holds the checked-in raw *.seg bytes a previous
// release's index writer produced for the two (identity, marker) pairs the
// tests below need. There is no generator test: regenerating these bytes
// would require the retired third-party index library this repository no
// longer depends on, so the checked-in bytes themselves are the provenance.
// They were produced by commit 8c172364 (NIDX-03, #1397); sha256 of
// legacy_written.seg is
// cd97237e5c7867a74b20e37b0632c07dcd4566efa559c5328a08aac346d1ecd5, and of
// legacy_dup.seg is
// 051a0d74bec04e1cce1da0b2459492e34631e78d502ca7f92895db0fa3af7bed.
const externalSegmentFixtureDir = "testdata/nidx03_fixture/external_segments"

// previousReleaseExternalSegment reads the checked-in raw segment fixture
// name (relative to externalSegmentFixtureDir) -- a stand-in for a segment
// the previous release's writer produced, used to prove the native external
// receiver still accepts what the previous release wrote.
func previousReleaseExternalSegment(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(externalSegmentFixtureDir, name))
	require.NoError(t, err)
	return data
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
	legacySeg := previousReleaseExternalSegment(t, "legacy_written.seg")

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

// TestSeriesIndex_ExternalReceive_LegacyDuplicateKeepsExisting pins the F4
// combination NIDX-03 §4.3/§7/§12 item 7 implies but the two tests above
// only establish separately: AcceptsLegacyAndNativeSegments proves a
// previous-release segment is accepted and searchable, and
// DuplicateIdentifierKeepsExisting proves the dedup rule using two native
// segments. Neither, on its own, proves the dedup rule also holds when the
// already-live document came from the legacy writer. This test receives a
// legacy-written segment first and a native-written duplicate of the same
// identifier second, through the same StartSegment/WriteChunk/
// CompleteSegment receiver both other tests use: the receiver's dedup logic
// (ExternalDedupKeepExisting) operates on parsed documents and never
// branches on which writer produced the bytes, so the legacy-authored
// document must still win.
func TestSeriesIndex_ExternalReceive_LegacyDuplicateKeepsExisting(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	identity := externalSegIdentity(t, "legacy-dup")
	first := previousReleaseExternalSegment(t, "legacy_dup.seg")
	second := buildNativeExternalSegment(t, identity, "native-second")

	require.NoError(t, receiveExternalSegment(t, si, first))
	require.NoError(t, receiveExternalSegment(t, si, second))

	var series pbv1.Series
	require.NoError(t, series.Unmarshal(identity))
	sd, _, err := si.Search(ctx, []*pbv1.Series{&series}, IndexSearchOpts{Projection: []index.FieldKey{{TagName: "marker"}}})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 1, "a duplicate identifier must not create a second live document")
	require.Equal(t, "legacy-first", string(sd.Fields[0]["marker"]), "a legacy-written existing document must also win over an incoming native duplicate")
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
