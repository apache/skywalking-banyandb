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
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countsManifest holds every count the exporter records: rolled shard files with rowCounts and
// totalCount, parts, an index-mode source with segment-level files, a property shard with its
// docCount, a replicated shard and a shard that counted no row.
func countsManifest() *Manifest {
	file := func(path string, size uint64, entries uint32) File {
		return File{Path: path, Bytes: size, CRC32: FormatCRC32(uint32(size)), EntryCount: entries}
	}
	replica := func(node string, size uint64) UnitSource {
		return UnitSource{Node: node, Shards: []Shard{{
			Shard: 0, MinTimestamp: 2000, MaxTimestamp: 2500, TotalCount: 25,
			RowCounts: map[string]uint64{"service_log": 25},
			Parts:     []Part{{ID: 61, MinTimestamp: 2000, MaxTimestamp: 2500, TotalCount: 25}},
			Files:     []File{file("nodes/"+NodeDir(node)+"/stream/sw_rep/seg-20261006/shard-0-00001.csv.gz", size, 1)},
		}}}
	}
	return &Manifest{
		ArtifactVersion: ArtifactVersion, Format: "csv", Compress: "gzip", ExportedAt: "2026-10-19T08:00:00Z",
		Source: Source{Nodes: []Node{{ID: "data-a:17912", Dir: "data-a-17912", Timezone: "Asia/Shanghai"}, {ID: "data-b:17912", Dir: "data-b-17912", Timezone: "UTC"}}},
		Groups: []Group{
			{Name: "sw_stream", Catalog: "STREAM", Stages: []Stage{{Name: "hot", ShardNum: 3, SegmentInterval: "1d", TTL: "7d", Replicas: 1}}},
			{Name: "sw_idx", Catalog: "MEASURE", IndexMode: true, Stages: []Stage{{Name: "hot", ShardNum: 2, SegmentInterval: "1d", TTL: "7d"}}},
		},
		Units: []Unit{
			{
				Catalog: "MEASURE", Group: "sw_idx", Stage: "hot", Segment: "20261005", SegmentVersion: "1.3.0", MinTimestamp: 3000, MaxTimestamp: 3900,
				Sources: []UnitSource{{
					Node:       "data-a:17912",
					RowCounts:  map[string]uint64{"endpoint_traffic": 80, "service_traffic": 10},
					TotalCount: 90,
					SegmentFiles: []File{
						file("nodes/data-a-17912/measure/sw_idx/seg-20261005/segment-00001.csv.gz", 900, 2),
						file("nodes/data-a-17912/measure/sw_idx/seg-20261005/segment-00002.csv.gz", 400, 1),
					},
					Shards: []Shard{
						{Shard: 0, MinTimestamp: 3000, MaxTimestamp: 3500, RowCounts: map[string]uint64{}, Parts: []Part{}, Files: []File{}},
						{Shard: 1, MinTimestamp: 3100, MaxTimestamp: 3900, RowCounts: map[string]uint64{}, Parts: []Part{}, Files: []File{}},
					},
				}},
			},
			{
				Catalog: "PROPERTY", Group: "ui_template", Stage: "warm",
				Sources: []UnitSource{{Node: "data-b:17912", Shards: []Shard{{
					Shard: 2, DocCount: 7, TotalCount: 7, EstimatedCompressedBytes: 300, EstimatedUncompressedBytes: 300,
					RowCounts: map[string]uint64{"ui_template": 7}, Parts: []Part{},
					Files: []File{file("nodes/data-b-17912/property/ui_template/shard-2-00001.csv.gz", 120, 1)},
				}}}},
			},
			{
				Catalog: "STREAM", Group: "sw_rep", Stage: "hot", Segment: "20261006", SegmentVersion: "1.3.0", MinTimestamp: 2000, MaxTimestamp: 2500,
				Sources: []UnitSource{replica("data-a:17912", 111), replica("data-b:17912", 112)},
			},
			{
				Catalog: "STREAM", Group: "sw_stream", Stage: "hot", Segment: "20261004", SegmentVersion: "1.3.0", MinTimestamp: 900, MaxTimestamp: 2000,
				Sources: []UnitSource{{Node: "data-a:17912", Shards: []Shard{{
					Shard: 0, MinTimestamp: 1000, MaxTimestamp: 1900, TotalCount: math.MaxUint64 - 1,
					EstimatedCompressedBytes: 4096, EstimatedUncompressedBytes: 16384,
					RowCounts: map[string]uint64{"service_log": math.MaxUint64 - 31, "segment": 30},
					Parts: []Part{
						{ID: math.MaxUint64, MinTimestamp: math.MinInt64, MaxTimestamp: 1500, TotalCount: math.MaxUint64 - 51},
						{ID: 12, MinTimestamp: 1400, MaxTimestamp: math.MaxInt64, TotalCount: 50},
					},
					Files: []File{
						file("nodes/data-a-17912/stream/sw_stream/seg-20261004/shard-0-00001.csv.gz", 700, 1),
						file("nodes/data-a-17912/stream/sw_stream/seg-20261004/shard-0-00002.csv.gz", 300, math.MaxUint32),
					},
				}}}},
			},
		},
		SchemaFiles: []File{},
		Warnings:    []string{"node data-b:17912 unit stream/sw_stream/seg-20261004: shard 2 changed while it was read"},
	}
}

// Every count reads back exactly, the extreme 64-bit values included: a JSON float would
// lose them.
func TestWriteReadKeepsEveryCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	m := countsManifest()
	require.NoError(t, Write(path, m))
	got, err := Read(path)
	require.NoError(t, err)
	assert.Equal(t, m, got)

	stream := got.Units[3].Sources[0].Shards[0]
	assert.Equal(t, uint64(math.MaxUint64-1), stream.TotalCount)
	assert.Equal(t, uint64(math.MaxUint64), stream.Parts[0].ID)
	assert.Equal(t, int64(math.MinInt64), stream.Parts[0].MinTimestamp)
	assert.Equal(t, uint32(math.MaxUint32), stream.Files[1].EntryCount)
	idx := got.Units[0].Sources[0]
	assert.Equal(t, uint64(90), idx.TotalCount)
	assert.Equal(t, map[string]uint64{"endpoint_traffic": 80, "service_traffic": 10}, idx.RowCounts)
	assert.Equal(t, uint64(0), idx.Shards[0].TotalCount)
	assert.NotNil(t, idx.Shards[0].RowCounts, "an empty shard keeps its empty rowCounts")
	assert.Equal(t, uint64(7), got.Units[1].Sources[0].Shards[0].DocCount)
	require.Len(t, got.Units[2].Sources, 2, "both replicas of a shard read back")
}

// The written JSON spells the counts as the importer reads them: a source without
// segment-level files has no rowCounts, totalCount or segmentFiles, a shard always has its
// rowCounts, parts, files and totalCount, and docCount appears only when set.
func TestWriteSpellsTheCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	require.NoError(t, Write(path, countsManifest()))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var doc struct {
		Units []struct {
			Sources []map[string]json.RawMessage `json:"sources"`
		} `json:"units"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.Len(t, doc.Units, 4)

	idx := doc.Units[0].Sources[0]
	assert.JSONEq(t, `{"endpoint_traffic": 80, "service_traffic": 10}`, string(idx["rowCounts"]))
	assert.Equal(t, "90", string(idx["totalCount"]))
	assert.Contains(t, idx, "segmentFiles")
	var idxShards []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(idx["shards"], &idxShards))
	assert.JSONEq(t, `{}`, string(idxShards[0]["rowCounts"]))
	assert.JSONEq(t, `[]`, string(idxShards[0]["parts"]))
	assert.JSONEq(t, `[]`, string(idxShards[0]["files"]))
	assert.Equal(t, "0", string(idxShards[0]["totalCount"]))
	assert.NotContains(t, idxShards[0], "docCount")

	property := doc.Units[1].Sources[0]
	var propertyShards []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(property["shards"], &propertyShards))
	assert.Equal(t, "7", string(propertyShards[0]["docCount"]))
	assert.Equal(t, "7", string(propertyShards[0]["totalCount"]))

	stream := doc.Units[3].Sources[0]
	for _, absent := range []string{"rowCounts", "totalCount", "segmentFiles"} {
		assert.NotContains(t, stream, absent)
	}
	var streamShards []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(stream["shards"], &streamShards))
	assert.Equal(t, "18446744073709551614", string(streamShards[0]["totalCount"]))
}
