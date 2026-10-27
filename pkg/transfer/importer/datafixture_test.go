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
	"bytes"
	"context"
	"encoding/csv"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// dataTime is the time of every fixture row; dataNow keeps it inside the TTL.
var (
	dataTime = time.Date(2026, 10, 16, 5, 0, 0, 0, time.UTC)
	dataNow  = time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
)

// cells is one CSV row by header; a missing header is \N.
type cells map[string]string

// csvFile renders rows for layout as a plain CSV file with its header.
func csvFile(t *testing.T, layout *csvfmt.Layout, rows ...cells) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	require.NoError(t, w.Write(layout.Header()))
	for _, r := range rows {
		rec := layout.NewRow()
		for h, v := range r {
			idx := slices.IndexFunc(layout.Columns, func(c csvfmt.Column) bool { return c.Header == h })
			require.GreaterOrEqual(t, idx, 0, "no column %q", h)
			rec[idx] = v
		}
		require.NoError(t, w.Write(rec))
	}
	w.Flush()
	require.NoError(t, w.Error())
	return buf.Bytes()
}

func cellTime(d time.Duration) string { return csvfmt.EncodeTimestamp(dataTime.Add(d).UnixNano()) }

// dataSchemas is the schema of the data fixture: a stream, a measure group with its
// _top_n_result and a property group.
func dataSchemas() *Schemas {
	s := NewSchemas()
	for _, g := range []*commonv1.Group{
		testGroup("sw_stream", commonv1.Catalog_CATALOG_STREAM), testGroup("sw_metric", commonv1.Catalog_CATALOG_MEASURE),
		testGroup("sw_prop", commonv1.Catalog_CATALOG_PROPERTY),
	} {
		s.Groups[g.GetMetadata().GetName()] = g
	}
	s.Streams[ResourceKey{"sw_stream", "log"}] = testStream("sw_stream", "log")
	s.Measures[ResourceKey{"sw_metric", "cpm"}] = testMeasure("sw_metric", "cpm")
	s.Measures[ResourceKey{"sw_metric", csvfmt.TopNResultMeasure}] = testTopNResult("sw_metric")
	s.Properties[ResourceKey{"sw_prop", "ui"}] = testProperty("sw_prop", "ui")
	return &s
}

func layoutOf(t *testing.T, s *Schemas, catalog commonv1.Catalog, group string) *csvfmt.Layout {
	t.Helper()
	a := &Artifact{Schemas: *s}
	l, err := a.Layout(catalog, group)
	require.NoError(t, err)
	return l
}

// writeDataArtifact writes a CSV artifact with real rows: 3 stream rows, 2 measure rows and
// two revisions of one property, all from node data-a.
func writeDataArtifact(t *testing.T) string {
	t.Helper()
	return writeDataArtifactSchemas(t, dataSchemas())
}

// writeDataArtifactSchemas writes the data fixture with the schema s.
func writeDataArtifactSchemas(t *testing.T, s *Schemas) string {
	t.Helper()
	return writeDataArtifactFrom(t, s,
		cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "1", "default.entity_id": "e1", "default.tier": "gold", "cpm::_field.value": "5"},
		cells{"_name": "cpm", "_timestamp": cellTime(time.Minute), "_version": "2", "default.entity_id": "e2", "cpm::_field.value": "7"},
	)
}

// writeDataArtifactWith writes the data fixture with the given measure rows.
func writeDataArtifactWith(t *testing.T, measureRows ...cells) string {
	t.Helper()
	return writeDataArtifactFrom(t, dataSchemas(), measureRows...)
}

// writeDataArtifactFrom writes the data fixture with the schema s and the given measure rows.
func writeDataArtifactFrom(t *testing.T, s *Schemas, measureRows ...cells) string {
	t.Helper()
	dir := t.TempDir()
	byKind := map[string][]proto.Message{
		schemasync.KindGroup:    {s.Groups["sw_metric"], s.Groups["sw_prop"], s.Groups["sw_stream"]},
		schemasync.KindStream:   {s.Streams[ResourceKey{"sw_stream", "log"}]},
		schemasync.KindMeasure:  {s.Measures[ResourceKey{"sw_metric", "cpm"}], s.Measures[ResourceKey{"sw_metric", csvfmt.TopNResultMeasure}]},
		schemasync.KindProperty: {s.Properties[ResourceKey{"sw_prop", "ui"}]},
	}
	m := &manifest.Manifest{
		Format: "csv", ArtifactVersion: 1, ExportedAt: "2026-10-17T00:00:00Z",
		Source: manifest.Source{Nodes: []manifest.Node{{ID: "data-a", Dir: "data-a", Timezone: "UTC"}}},
	}
	for _, kind := range schemasync.Kinds {
		m.SchemaFiles = append(m.SchemaFiles, writeFile(t, dir, schemasync.Dir+"/"+kind+".jsonl", schemaLines(t, byKind[kind]...)))
	}
	streamFile := writeFile(t, dir, "nodes/data-a/stream/sw_stream/seg-20261016/shard-0-00001.csv", csvFile(t,
		layoutOf(t, s, commonv1.Catalog_CATALOG_STREAM, "sw_stream"),
		cells{"_name": "log", "_element_id": csvfmt.EncodeElementID(1), "_timestamp": cellTime(0), "default.svc": "a", "default.latency": "10"},
		cells{"_name": "log", "_element_id": csvfmt.EncodeElementID(2), "_timestamp": cellTime(time.Minute), "default.svc": "b", "default.latency": "20"},
		cells{"_name": "log", "_element_id": csvfmt.EncodeElementID(3), "_timestamp": cellTime(2 * time.Minute), "default.svc": "c"},
	))
	measureFile := writeFile(t, dir, "nodes/data-a/measure/sw_metric/seg-20261016/shard-0-00001.csv", csvFile(t,
		layoutOf(t, s, commonv1.Catalog_CATALOG_MEASURE, "sw_metric"), measureRows...))
	propFile := writeFile(t, dir, "nodes/data-a/property/sw_prop/shard-0-00001.csv", csvFile(t,
		layoutOf(t, s, commonv1.Catalog_CATALOG_PROPERTY, "sw_prop"),
		cells{"_name": "ui", "_id": "p1", "_mod_revision": "5", "_create_revision": "1", "_delete_time": "0", "content": "new"},
		cells{"_name": "ui", "_id": "p1", "_mod_revision": "3", "_create_revision": "1", "_delete_time": cellTime(0), "content": "old"},
	))
	seg := func(f manifest.File, rows uint64, maxTS int64) []manifest.Shard {
		return []manifest.Shard{{
			Shard: 0, TotalCount: rows, MinTimestamp: dataTime.UnixNano(), MaxTimestamp: maxTS, Files: []manifest.File{f},
			Parts: []manifest.Part{{ID: 1, MinTimestamp: dataTime.UnixNano(), MaxTimestamp: maxTS, TotalCount: rows}},
		}}
	}
	m.Units = []manifest.Unit{
		{
			Catalog: "measure", Group: "sw_metric", Stage: "hot", Segment: "20261016",
			Sources: []manifest.UnitSource{{Node: "data-a", Shards: seg(measureFile, uint64(len(measureRows)), dataTime.Add(time.Minute).UnixNano())}},
		},
		{
			Catalog: "property", Group: "sw_prop",
			Sources: []manifest.UnitSource{{Node: "data-a", Shards: []manifest.Shard{{Shard: 0, TotalCount: 2, Files: []manifest.File{propFile}}}}},
		},
		{
			Catalog: "stream", Group: "sw_stream", Stage: "hot", Segment: "20261016",
			Sources: []manifest.UnitSource{{Node: "data-a", Shards: seg(streamFile, 3, dataTime.Add(2*time.Minute).UnixNano())}},
		},
	}
	require.NoError(t, manifest.Write(filepath.Join(dir, manifest.FileName), m))
	return dir
}

// defaultImport returns the bydbctl defaults of an import of dir, without verification.
func defaultImport(t *testing.T, dir string) config.Import {
	t.Helper()
	var opts config.Import
	fs := pflag.NewFlagSet("import", pflag.ContinueOnError)
	config.Bind(fs, &opts)
	_, err := config.Resolve(fs, nil, &opts)
	require.NoError(t, err)
	opts.Input = dir
	opts.Verify = false
	opts.SendRetryTimeout = time.Second
	wait := schemaWaitTimeout
	schemaWaitTimeout = time.Second
	t.Cleanup(func() { schemaWaitTimeout = wait })
	return opts
}

// mirrorFake stores every resource of s on the fake, as an already imported target.
func mirrorFake(f *fakeLiaison, s *Schemas) {
	for _, g := range s.Groups {
		f.schema.Groups[g.GetMetadata().GetName()] = restamp(g).(*commonv1.Group)
	}
	mirror(f.schema.Streams, s.Streams)
	mirror(f.schema.Measures, s.Measures)
	mirror(f.schema.Properties, s.Properties)
}

// entriesFor decides the artifact against the fake and returns the plan and target state.
func entriesFor(t *testing.T, li *cluster.Liaison, f *fakeLiaison, dir string) (*Artifact, *decision) {
	t.Helper()
	art, err := OpenArtifact(context.Background(), dir, true)
	require.NoError(t, err)
	opts := defaultImport(t, dir)
	d := decider{liaisons: []*cluster.Liaison{li}, cluster: clusterOf(f, li.Addr), art: art, opts: &opts, endpoint: li.Addr}
	dec, err := d.decide(context.Background(), dataNow, false)
	require.NoError(t, err)
	return art, dec
}
