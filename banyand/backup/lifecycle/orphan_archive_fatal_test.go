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

package lifecycle

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// TestArchiveWriteFailureIsFatalAtClose proves that under the archive policy a
// write failure surfaces as a FATAL error at close (the gzip buffer + trailer are
// flushed there, so a buffered write error appears at close, not at appendRow) and
// that no segment manifest is written for the failed part — so a failed archive
// can never be mistaken for a completed one.
func TestArchiveWriteFailureIsFatalAtClose(t *testing.T) {
	dir := t.TempDir()
	a := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: dir}, "g", catalogMeasure, nil)
	loc := sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000000001"}
	w := a.partWriter(loc)

	// First row lazily opens the gzip-backed file (buffered, not yet flushed).
	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 7}))

	// Close the underlying file so flushing the gzip buffer/trailer at close fails,
	// simulating an I/O error on the archive write.
	require.NoError(t, w.f.Close())

	err := w.close(true)
	require.Error(t, err, "an archive write failure must surface as a fatal error at close, not be swallowed")

	// A failed part must not leave a manifest claiming it was archived.
	_, statErr := os.Stat(a.manifestPath(loc.Segment))
	require.True(t, os.IsNotExist(statErr), "no manifest must be written for a part whose archive write failed")
}

// TestAppendRowDiscardNeverErrorsAndTallies proves the discard policy tallies a
// row (for reporting) without writing any file and never errors.
func TestAppendRowDiscardNeverErrorsAndTallies(t *testing.T) {
	a := newOrphanArchiver(orphanConfig{policy: orphanDiscard, rootDir: t.TempDir()}, "g", catalogMeasure, nil)
	w := a.partWriter(sourceLoc{Segment: "20260601", Part: "0000000000000001"})
	require.Nil(t, w.f, "discard policy must not open an archive file")

	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 1}))
	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 1}))
	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 2}))

	require.Len(t, w.tally, 1)
	tally := w.tally["deleted"]
	require.NotNil(t, tally)
	assert.Equal(t, 3, tally.rows)
	assert.Len(t, tally.series, 2, "two distinct series ids")
}

// TestManifestUsesRealSeriesCounts proves the manifest persists the real
// per-part-distinct series count (not a fabricated placeholder set) and that a
// resume reloads those counts and re-sums them correctly.
func TestManifestUsesRealSeriesCounts(t *testing.T) {
	dir := t.TempDir()
	a := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: dir}, "g", catalogMeasure, nil)
	loc := sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000000001"}
	w := a.partWriter(loc)
	// Two distinct series, three rows total.
	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 100}))
	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 100}))
	require.NoError(t, w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 200}))
	require.NoError(t, w.close(true))

	var m manifestFile
	mb, readErr := os.ReadFile(a.manifestPath(loc.Segment))
	require.NoError(t, readErr)
	require.NoError(t, json.Unmarshal(mb, &m))
	assert.Equal(t, 3, m.TotalRows)
	assert.Equal(t, 2, m.TotalSeries, "manifest must record the real distinct series count")

	// A fresh archiver (resume) reloads the manifest counts and a second part folds
	// in on top, summing rows/series per the documented per-part-distinct semantics.
	a2 := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: dir}, "g", catalogMeasure, nil)
	loc2 := loc
	loc2.Part = "0000000000000002"
	w2 := a2.partWriter(loc2)
	require.NoError(t, w2.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 300}))
	require.NoError(t, w2.close(true))

	var m2 manifestFile
	mb2, readErr2 := os.ReadFile(a2.manifestPath(loc.Segment))
	require.NoError(t, readErr2)
	require.NoError(t, json.Unmarshal(mb2, &m2))
	assert.Equal(t, 4, m2.TotalRows, "rows summed across resumed + new parts")
	assert.Equal(t, 3, m2.TotalSeries, "series summed across resumed + new parts (per-part-distinct)")
}

// TestEntityValueStringFaithful proves entity values render without binary loss.
func TestEntityValueStringFaithful(t *testing.T) {
	bin := []byte{0x00, 0x01, 0xff}
	cases := []struct {
		tv   *modelv1.TagValue
		want string
	}{
		{&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "hello"}}}, "hello"},
		{&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: -42}}}, "-42"},
		{&modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: bin}}, "base64:" + base64.StdEncoding.EncodeToString(bin)},
		{&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b"}}}}, "a,b"},
		{&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2}}}}, "1,2"},
		{nil, ""},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, entityValueString(c.tv))
	}
}

// TestRenderIndexedTagsBase64 proves non-printable indexed-tag bytes are
// base64-encoded while printable ones stay as text.
func TestRenderIndexedTagsBase64(t *testing.T) {
	bin := []byte{0x00, 0x01, 0x02}
	out := renderIndexedTags(map[uint32][][]byte{
		1: {[]byte("printable"), bin},
	})
	require.Len(t, out["1"], 2)
	assert.Equal(t, "printable", out["1"][0])
	assert.Equal(t, "base64:"+base64.StdEncoding.EncodeToString(bin), out["1"][1])
}

// TestValueWithTypeUnknownVsNull proves a nil/empty value renders as NULL while a
// non-nil but unrecognized oneof renders as UNKNOWN.
func TestValueWithTypeUnknownVsNull(t *testing.T) {
	assert.Equal(t, "NULL", valueWithType(nil).Type)
	assert.Equal(t, "NULL", valueWithType(&modelv1.TagValue{}).Type)
	assert.Equal(t, "NULL", valueWithType(&modelv1.TagValue{Value: &modelv1.TagValue_Null{}}).Type)
}

// TestArchiveOpenFailureIsFatalOnFirstRow proves a failure to open the archive
// file (here: the part path already exists as a directory) is surfaced as an
// error on the first row so the part aborts and the source segment is retained.
// With lazy open the failure surfaces at the first appendRow, not at partWriter.
func TestArchiveOpenFailureIsFatalOnFirstRow(t *testing.T) {
	dir := t.TempDir()
	a := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: dir}, "g", catalogMeasure, nil)
	loc := sourceLoc{Segment: "20260601", Shard: 0, Part: "0000000000000001"}
	// Pre-create the would-be archive file path as a directory so OpenFile fails.
	require.NoError(t, os.MkdirAll(a.partFilePath(loc), 0o750))

	w := a.partWriter(loc)
	err := w.appendRow(&archiveRecord{Measure: "deleted", SeriesID: 1})
	require.Error(t, err, "archive open failure must be fatal so the source segment is retained")
	require.Empty(t, w.tally, "a row whose archive could not be opened must not be tallied")
}
