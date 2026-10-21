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
	"crypto/sha256"
	"encoding/hex"
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
		id     string
		prefix string
	}{
		{id: "10.0.0.5:17912", prefix: "10-0-0-5-17912"},
		{id: "data-warm1:17912", prefix: "data-warm1-17912"},
		{id: "[::1]:17912", prefix: "1-17912"},
		{id: "Data-A:17912", prefix: "data-a-17912"},
		{id: "", prefix: "node"},
		{id: "::", prefix: "node"},
		{id: "--Host__Name--", prefix: "host-name"},
		{id: "héllo", prefix: "h-llo"},
	}
	valid := regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*-[0-9a-f]{8}$`)
	for _, c := range cases {
		got := NodeDir(c.id)
		sum := sha256.Sum256([]byte(c.id))
		assert.Equal(t, c.prefix+"-"+hex.EncodeToString(sum[:4]), got, "id %q", c.id)
		assert.Regexp(t, valid, got)
	}
	assert.Equal(t, "10-0-0-5-17912-8a05db98", NodeDir("10.0.0.5:17912"))
}

// Ids that differ only in case or punctuation share the readable prefix, not the directory.
func TestNodeDirKeepsSimilarIDsApart(t *testing.T) {
	assert.Equal(t, "a-b-17912-613e0e89", NodeDir("a.b:17912"))
	assert.Equal(t, "a-b-17912-b5233c34", NodeDir("a-b:17912"))
	assert.Equal(t, "data-1-17912-03d2b6d6", NodeDir("Data-1:17912"))
	assert.Equal(t, "data-1-17912-98a53f33", NodeDir("data-1:17912"))
}

// A long id keeps a bounded readable prefix, and the hash keeps ids differing only at the end apart.
func TestNodeDirLongIDs(t *testing.T) {
	base := strings.Repeat("very-long-hostname.", 4) + "example.com"
	a := NodeDir(base + ":17912")
	assert.Len(t, a, 32+1+8)
	assert.True(t, strings.HasPrefix(a, "very-long-hostname-very-long-hos-"), a)
	assert.NotEqual(t, a, NodeDir(base+":17913"))
	// A cut that ends on a separator does not leave a doubled '-'.
	assert.NotContains(t, NodeDir(strings.Repeat("a", 31)+".b"), "--")
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
