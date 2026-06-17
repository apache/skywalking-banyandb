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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOrphanPolicy(t *testing.T) {
	p, err := parseOrphanPolicy("archive")
	require.NoError(t, err)
	assert.Equal(t, orphanArchive, p)
	p, err = parseOrphanPolicy("discard")
	require.NoError(t, err)
	assert.Equal(t, orphanDiscard, p)
	_, err = parseOrphanPolicy("nope")
	require.Error(t, err)
}

func TestOrphanPaths(t *testing.T) {
	loc := sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000001bca"}
	a := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: "/root"}, "g", "measure", nil)
	// rootDir already points at the catalog's archive subdir, so the path carries
	// only the group (no catalog level); catalog stays inside each record/manifest.
	assert.Equal(t, filepath.FromSlash("/root/g/seg-20260601/shard-0/part-0000000000001bca.jsonl.gz"), a.partFilePath(loc))
	assert.Equal(t, filepath.FromSlash("/root/g/seg-20260601/manifest.json"), a.manifestPath("20260601"))
}

func sampleRec(measure string, sid uint64, ts int64) *archiveRecord {
	return &archiveRecord{
		Group: "g", Catalog: "measure", Measure: measure,
		Source:   sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000001bca"},
		SeriesID: sid, Entity: []string{"e1"}, Timestamp: "2026-06-01T00:00:00Z",
		TimeNanos: ts, Version: 1,
		Fields: map[string]typedValue{"value": {Type: "INT64", Value: int64(42)}},
	}
}

func TestArchiveWriteAndManifest(t *testing.T) {
	root := t.TempDir()
	a := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: root}, "g", "measure", nil)
	loc := sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000001bca"}

	w := a.partWriter(loc)
	require.NoError(t, w.appendRow(sampleRec("m1", 1, 100)))
	require.NoError(t, w.appendRow(sampleRec("m1", 1, 200)))
	require.NoError(t, w.appendRow(sampleRec("m2", 9, 300)))
	require.NoError(t, w.close(true))

	// JSONL (gzip): 3 records.
	assert.Equal(t, 3, len(readArchiveRecords(t, a.partFilePath(loc))))

	// Manifest: m1=2 rows/1 series, m2=1 row/1 series, totals 3/2.
	var m manifestFile
	mb, err := os.ReadFile(a.manifestPath("20260601"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(mb, &m))
	assert.Equal(t, 3, m.TotalRows)
	assert.Equal(t, 2, m.TotalSeries)
	assert.Len(t, m.Measures, 2)
}

func TestArchiveIdempotentOnRewrite(t *testing.T) {
	root := t.TempDir()
	a := newOrphanArchiver(orphanConfig{policy: orphanArchive, rootDir: root}, "g", "measure", nil)
	loc := sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000001bca"}
	for i := 0; i < 2; i++ { // replay same part twice (resume)
		w := a.partWriter(loc)
		require.NoError(t, w.appendRow(sampleRec("m1", 1, 100)))
		require.NoError(t, w.close(true))
	}
	assert.Equal(t, 1, len(readArchiveRecords(t, a.partFilePath(loc))), "truncate-rewrite, no dup lines")
	var m manifestFile
	mb, _ := os.ReadFile(a.manifestPath("20260601"))
	require.NoError(t, json.Unmarshal(mb, &m))
	assert.Equal(t, 1, m.TotalRows, "manifest not double-counted")
	assert.Equal(t, 1, m.TotalSeries, "manifest series not double-counted on resume")
}

func TestDiscardWritesNothing(t *testing.T) {
	root := t.TempDir()
	a := newOrphanArchiver(orphanConfig{policy: orphanDiscard, rootDir: root}, "g", "measure", nil)
	loc := sourceLoc{Stage: "hot", Segment: "20260601", Shard: 0, Part: "0000000000001bca"}
	w := a.partWriter(loc)
	require.NoError(t, w.appendRow(sampleRec("m1", 1, 100)))
	require.NoError(t, w.close(true))
	_, err := os.Stat(filepath.Join(root, "g"))
	assert.True(t, os.IsNotExist(err), "discard creates no files")
}

func TestSkipKindUnwrap(t *testing.T) {
	sidx := &skipError{seriesID: 1, reason: skipReasonIncompletePart}
	orphan := newOrphanSkip("part", 2, "m")
	assert.True(t, errors.Is(sidx, errSkipSeries))
	assert.False(t, errors.Is(sidx, errOrphanSchema))
	assert.True(t, errors.Is(orphan, errOrphanSchema))
	assert.False(t, errors.Is(orphan, errSkipSeries), "orphan must not match sidx-gap")
}
