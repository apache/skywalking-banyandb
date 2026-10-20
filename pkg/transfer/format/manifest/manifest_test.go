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

package manifest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeDir(t *testing.T) {
	cases := []struct {
		id  string
		dir string
	}{
		{id: "10.0.0.5:17912", dir: "10-0-0-5-17912"},
		{id: "data-warm1:17912", dir: "data-warm1-17912"},
		{id: "[::1]:17912", dir: "1-17912"},
		{id: "Data-A:17912", dir: "data-a-17912"},
		{id: "", dir: "node"},
		{id: "::", dir: "node"},
		{id: "--Host__Name--", dir: "host-name"},
		{id: "héllo", dir: "h-llo"},
	}
	valid := regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	for _, c := range cases {
		got := NodeDir(c.id)
		assert.Equal(t, c.dir, got, "id %q", c.id)
		assert.Regexp(t, valid, got)
	}
}

// A long id keeps every character, so ids differing only at the end stay apart.
func TestNodeDirLongIDs(t *testing.T) {
	base := strings.Repeat("very-long-hostname.", 4) + "example.com"
	a := NodeDir(base + ":17912")
	assert.Equal(t, strings.ReplaceAll(base, ".", "-")+"-17912", a)
	assert.NotEqual(t, a, NodeDir(base+":17913"))
}

func TestFormatCRC32(t *testing.T) {
	assert.Equal(t, "0x1f4a03bb", FormatCRC32(0x1f4a03bb))
	assert.Equal(t, "0x00000000", FormatCRC32(0))
	assert.Equal(t, "0xffffffff", FormatCRC32(0xffffffff))
}

func sampleManifest() *Manifest {
	return &Manifest{
		ArtifactVersion: ArtifactVersion,
		Format:          "csv",
		Compress:        "gzip",
		ExportedAt:      "2026-10-10T12:00:00Z",
		Source: Source{Nodes: []Node{
			{ID: "10.0.0.5:17912", Dir: NodeDir("10.0.0.5:17912"), Timezone: "Asia/Shanghai"},
		}},
		Groups: []Group{{Name: "sw_metric", Catalog: "MEASURE", Stages: []Stage{
			{Name: "hot", ShardNum: 2, SegmentInterval: "1d", TTL: "7d", Replicas: 1},
		}}},
		Units: []Unit{{
			Catalog: "MEASURE", Group: "sw_metric", Stage: "hot", Segment: "20260907",
			MinTimestamp: 1788739200000000000, MaxTimestamp: 1788825599999999999,
			Sources: []UnitSource{{
				Node: "10.0.0.5:17912",
				Shards: []Shard{{
					Shard: 0, MinTimestamp: 1, MaxTimestamp: 2, TotalCount: 3,
					Parts:     []Part{{ID: 1, MinTimestamp: 1, MaxTimestamp: 2, TotalCount: 3}},
					RowCounts: map[string]uint64{"service_cpm_minute": 3},
					Files: []File{{
						Path: "nodes/x/measure/sw_metric/seg-20260907/shard-0-00001.csv.gz", Bytes: 52, CRC32: FormatCRC32(0x1f4a03bb),
					}},
				}},
			}},
		}},
		SchemaFiles: []File{{Path: "schema/group.jsonl", Bytes: 812, CRC32: "0x05f5e0fe"}},
		Warnings:    []string{"unit has no rows"},
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	m := sampleManifest()
	require.NoError(t, Write(path, m))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temp file is left behind")

	got, err := Read(path)
	require.NoError(t, err)
	assert.Equal(t, m, got)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	s := string(raw)
	for _, key := range []string{
		`"artifactVersion": 1`, `"format": "csv"`, `"exportedAt"`, `"timezone"`, `"indexMode": false`,
		`"shardNum": 2`, `"segmentInterval"`, `"ttl"`, `"minTimestamp"`, `"rowCounts"`, `"estimatedCompressedBytes"`,
		`"estimatedUncompressedBytes"`, `"schemaFiles"`, `"crc32": "0x1f4a03bb"`, `"totalCount": 3`,
	} {
		assert.Contains(t, s, key)
	}
	for _, absent := range []string{`"schemaOnly"`, `"segmentVersion"`, `"segmentFiles"`, `"entryCount"`, `"docCount"`} {
		assert.NotContains(t, s, absent)
	}
	assert.Contains(t, s, "\n  \"", "indented JSON")

	// Overwriting replaces the content atomically.
	m.Warnings = nil
	m.SchemaOnly = true
	require.NoError(t, Write(path, m))
	got, err = Read(path)
	require.NoError(t, err)
	assert.True(t, got.SchemaOnly)
	assert.Equal(t, []string{}, got.Warnings)
}

func TestWriteNilSlicesAsEmptyArrays(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	m := &Manifest{ArtifactVersion: ArtifactVersion, Format: "csv"}
	require.NoError(t, Write(path, m))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	s := string(raw)
	for _, key := range []string{`"groups": []`, `"units": []`, `"schemaFiles": []`, `"warnings": []`, `"nodes": []`} {
		assert.Contains(t, s, key)
	}
	assert.Nil(t, m.Warnings, "the caller's manifest is not modified")
}

func TestWriteMissingDirFails(t *testing.T) {
	require.Error(t, Write(filepath.Join(t.TempDir(), "missing", FileName), sampleManifest()))
}

func TestReadErrors(t *testing.T) {
	dir := t.TempDir()
	_, err := Read(filepath.Join(dir, FileName))
	require.Error(t, err)
	bad := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))
	_, err = Read(bad)
	require.Error(t, err)
}
