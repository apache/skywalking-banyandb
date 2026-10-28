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

package importer

import (
	"context"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// artifactDataFile is the one data file writeArtifact registers.
const artifactDataFile = "nodes/data-a/stream/sw_stream/seg-20261016/shard-0-00001.csv.gz"

// writeFile writes an artifact file and returns its manifest registration.
func writeFile(t *testing.T, dir, rel string, data []byte) manifest.File {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
	require.NoError(t, os.WriteFile(full, data, 0o600))
	return manifest.File{Path: rel, Bytes: uint64(len(data)), CRC32: manifest.FormatCRC32(crc32.ChecksumIEEE(data))}
}

func schemaLines(t *testing.T, msgs ...proto.Message) []byte {
	t.Helper()
	var out []byte
	for _, m := range msgs {
		line, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(m)
		require.NoError(t, err)
		out = append(append(out, line...), '\n')
	}
	return out
}

// writeArtifact writes a small CSV artifact: a stream group and a measure group with its
// _top_n_result, one data file each.
func writeArtifact(t *testing.T, mutate func(m *manifest.Manifest)) string {
	t.Helper()
	dir := t.TempDir()
	byKind := map[string][]byte{
		schemasync.KindGroup:   schemaLines(t, testGroup("sw_stream", commonv1.Catalog_CATALOG_STREAM), testGroup("sw_metric", commonv1.Catalog_CATALOG_MEASURE)),
		schemasync.KindStream:  schemaLines(t, testStream("sw_stream", "log")),
		schemasync.KindMeasure: schemaLines(t, testMeasure("sw_metric", "cpm"), testTopNResult("sw_metric")),
	}
	m := &manifest.Manifest{Format: "csv", ArtifactVersion: 1, ExportedAt: "2026-10-19T08:00:00Z"}
	for _, kind := range schemasync.Kinds {
		m.SchemaFiles = append(m.SchemaFiles, writeFile(t, dir, schemasync.Dir+"/"+kind+".jsonl", byKind[kind]))
	}
	data := writeFile(t, dir, artifactDataFile, []byte("rows"))
	m.Units = []manifest.Unit{{
		Catalog: "STREAM", Group: "sw_stream", Stage: "hot", Segment: "20261016",
		Sources: []manifest.UnitSource{{Node: "data-a", Shards: []manifest.Shard{{Shard: 0, TotalCount: 1, Files: []manifest.File{data}}}}},
	}}
	if mutate != nil {
		mutate(m)
	}
	require.NoError(t, manifest.Write(filepath.Join(dir, manifest.FileName), m))
	return dir
}

func TestOpenArtifact(t *testing.T) {
	dir := writeArtifact(t, nil)
	a, err := OpenArtifact(context.Background(), dir, true)
	require.NoError(t, err)
	assert.Empty(t, a.Warnings)
	assert.Len(t, a.Groups, 2)
	assert.Contains(t, a.Streams, ResourceKey{"sw_stream", "log"})
	assert.Contains(t, a.Measures, ResourceKey{"sw_metric", csvfmt.TopNResultMeasure})

	l, err := a.Layout(commonv1.Catalog_CATALOG_MEASURE, "sw_metric")
	require.NoError(t, err)
	want, err := csvfmt.MeasureLayout([]*databasev1.Measure{testTopNResult("sw_metric"), testMeasure("sw_metric", "cpm")})
	require.NoError(t, err)
	assert.Equal(t, want.Header(), l.Header())
	_, ok := l.FieldIndex(csvfmt.TopNResultMeasure, "value")
	assert.True(t, ok, "the layout holds every artifact measure of the group")
	_, err = a.Layout(commonv1.Catalog_CATALOG_UNSPECIFIED, "sw_metric")
	assert.Error(t, err)
}

func TestOpenArtifactRejects(t *testing.T) {
	setPath := func(p string) func(*manifest.Manifest) {
		return func(m *manifest.Manifest) { m.Units[0].Sources[0].Shards[0].Files[0].Path = p }
	}
	for _, tc := range []struct {
		mutate   func(m *manifest.Manifest)
		damage   func(dir string)
		name     string
		contains string
		checksum bool
	}{
		{name: "not csv", mutate: func(m *manifest.Manifest) { m.Format = "native" }, contains: "only csv"},
		{name: "newer artifact version", mutate: func(m *manifest.Manifest) { m.ArtifactVersion = 2 }, contains: "artifactVersion 2"},
		{
			name: "group of an unknown catalog without schema", contains: `unknown catalog "QUEUE"`,
			mutate: func(m *manifest.Manifest) {
				m.SchemaFiles, m.Groups = nil, []manifest.Group{{Name: "q", Catalog: "QUEUE"}}
			},
		},
		{
			name: "missing data file", contains: "no such file",
			damage: func(dir string) {
				require.NoError(t, os.Remove(filepath.Join(dir, artifactDataFile)))
			},
		},
		{
			name: "size mismatch", contains: "has 5 bytes",
			damage: func(dir string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, artifactDataFile), []byte("rowsx"), 0o600))
			},
		},
		{
			name: "checksum mismatch", contains: "crc32", checksum: true,
			damage: func(dir string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, artifactDataFile), []byte("rowz"), 0o600))
			},
		},
		{name: "path escaping the artifact", contains: "escapes", mutate: setPath("../outside.csv")},
		{name: "backslash parent path", contains: "escapes", mutate: setPath(`..\x`)},
		{name: "windows volume path", contains: "escapes", mutate: setPath(`C:\x`)},
		{name: "backslash separator", contains: "escapes", mutate: setPath(`a\b`)},
		{name: "absolute path", contains: "escapes", mutate: setPath("/etc/passwd")},
		{name: "missing manifest", contains: "manifest", damage: func(dir string) { require.NoError(t, os.Remove(filepath.Join(dir, manifest.FileName))) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeArtifact(t, tc.mutate)
			if tc.damage != nil {
				tc.damage(dir)
			}
			_, err := OpenArtifact(context.Background(), dir, tc.checksum)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.contains)
			assert.Equal(t, exitcode.ExitPreflight, exitcode.For(err))
		})
	}
}

func TestOpenArtifactStopsOnCancel(t *testing.T) {
	dir := writeArtifact(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := OpenArtifact(ctx, dir, true)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
}

func TestCtxReaderStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := ctxReader{ctx: ctx, r: strings.NewReader("data")}
	buf := make([]byte, 2)
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	cancel()
	_, err = r.Read(buf)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestOpenArtifactSkipsChecksumWhenNotAsked(t *testing.T) {
	dir := writeArtifact(t, nil)
	require.NoError(t, os.WriteFile(filepath.Join(dir, artifactDataFile), []byte("rowz"), 0o600))
	_, err := OpenArtifact(context.Background(), dir, false)
	assert.NoError(t, err)
}

func TestOpenArtifactWarnsOnUnknownSchemaFields(t *testing.T) {
	dir := writeArtifact(t, nil)
	rel := schemasync.Dir + "/" + schemasync.KindStream + ".jsonl"
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	require.NoError(t, err)
	line := []byte(`{"metadata":{"group":"sw_stream","name":"log"},"futureField":true}` + "\n")
	for i, f := range m.SchemaFiles {
		if f.Path == rel {
			m.SchemaFiles[i] = writeFile(t, dir, rel, line)
		}
	}
	require.NoError(t, manifest.Write(filepath.Join(dir, manifest.FileName), m))
	a, err := OpenArtifact(context.Background(), dir, true)
	require.NoError(t, err)
	require.Len(t, a.Warnings, 1)
	assert.Contains(t, a.Warnings[0], "schema/stream.jsonl line 1")
	assert.Contains(t, a.Streams, ResourceKey{"sw_stream", "log"})
}

func TestOpenArtifactLeavesOutInternalGroups(t *testing.T) {
	dir := writeArtifact(t, func(m *manifest.Manifest) {
		m.Groups = append(m.Groups,
			manifest.Group{Name: "sw_stream", Catalog: "STREAM"},
			manifest.Group{Name: common.DeletionTaskGroup, Catalog: "PROPERTY"})
		m.Units = append(m.Units, manifest.Unit{Catalog: "PROPERTY", Group: common.DeletionTaskGroup, Stage: "hot"})
	})
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	require.NoError(t, err)
	for i, f := range m.SchemaFiles {
		switch f.Path {
		case schemasync.Dir + "/" + schemasync.KindGroup + ".jsonl":
			m.SchemaFiles[i] = writeFile(t, dir, f.Path, schemaLines(t,
				testGroup("sw_stream", commonv1.Catalog_CATALOG_STREAM), testGroup(common.DeletionTaskGroup, commonv1.Catalog_CATALOG_PROPERTY)))
		case schemasync.Dir + "/" + schemasync.KindProperty + ".jsonl":
			m.SchemaFiles[i] = writeFile(t, dir, f.Path, schemaLines(t, testProperty(common.DeletionTaskGroup, "deletion_task")))
		}
	}
	require.NoError(t, manifest.Write(filepath.Join(dir, manifest.FileName), m))

	a, err := OpenArtifact(context.Background(), dir, true)
	require.NoError(t, err)
	assert.NotContains(t, a.Groups, common.DeletionTaskGroup)
	assert.Empty(t, a.Properties)
	assert.Equal(t, []manifest.Group{{Name: "sw_stream", Catalog: "STREAM"}}, a.Manifest.Groups)
	require.Len(t, a.Manifest.Units, 1)
	assert.Equal(t, "sw_stream", a.Manifest.Units[0].Group)
	assert.Equal(t, []string{"the artifact holds the server's internal group(s) _deletion_task; they are not imported"}, a.Warnings)
}
