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

package rowdiff

import (
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

func meta(group, name string) *commonv1.Metadata {
	return &commonv1.Metadata{Group: group, Name: name}
}

func testStream(group string, extraTag bool) *databasev1.Stream {
	tags := []*databasev1.TagSpec{
		{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "latency", Type: databasev1.TagType_TAG_TYPE_INT},
	}
	if extraTag {
		tags = append(tags, &databasev1.TagSpec{Name: "extra", Type: databasev1.TagType_TAG_TYPE_STRING})
	}
	return &databasev1.Stream{
		Metadata:    meta(group, "sw"),
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "searchable", Tags: tags}},
		Entity:      &databasev1.Entity{TagNames: []string{"svc"}},
	}
}

func testMeasure(group string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: meta(group, "cpm"),
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "layer", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	}
}

func testTrace(group string) *databasev1.Trace {
	return &databasev1.Trace{
		Metadata: meta(group, "segment"),
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "ts", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "ts",
	}
}

func testProperty(group string) *databasev1.Property {
	return &databasev1.Property{
		Metadata: meta(group, "ui"),
		Tags:     []*databasev1.TagSpec{{Name: "title", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}
}

// unitSpec is one unit of a test artifact: rows are header->cell maps, "_name" included.
type unitSpec struct {
	sources map[string][]map[string]string // node -> rows
	catalog string
	group   string
	stage   string
}

type artifactSpec struct {
	schemas []proto.Message
	units   []unitSpec
	gzip    bool
}

type diskSink struct {
	f     *os.File
	root  string
	paths []manifest.File
}

func (d *diskSink) StartFile(rel string, _ uint32) error {
	full := filepath.Join(d.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return err
	}
	f, err := os.Create(full)
	if err != nil {
		return err
	}
	d.f = f
	d.paths = append(d.paths, manifest.File{Path: rel})
	return nil
}

func (d *diskSink) Write(p []byte) error {
	_, err := d.f.Write(p)
	return err
}

func (d *diskSink) FinishFile(size uint64, crc uint32, _ map[string]uint64) error {
	last := &d.paths[len(d.paths)-1]
	last.Bytes = size
	last.CRC32 = manifest.FormatCRC32(crc)
	return d.f.Close()
}

func layoutOf(t *testing.T, catalog, group string, schemas []proto.Message) *csvfmt.Layout {
	t.Helper()
	var l *csvfmt.Layout
	var err error
	switch catalog {
	case catalogStream:
		var list []*databasev1.Stream
		for _, m := range schemas {
			if s, ok := m.(*databasev1.Stream); ok && s.GetMetadata().GetGroup() == group {
				list = append(list, s)
			}
		}
		l, err = csvfmt.StreamLayout(list)
	case catalogMeasure:
		var list []*databasev1.Measure
		for _, m := range schemas {
			if s, ok := m.(*databasev1.Measure); ok && s.GetMetadata().GetGroup() == group {
				list = append(list, s)
			}
		}
		l, err = csvfmt.MeasureLayout(list)
	case catalogTrace:
		var list []*databasev1.Trace
		for _, m := range schemas {
			if s, ok := m.(*databasev1.Trace); ok && s.GetMetadata().GetGroup() == group {
				list = append(list, s)
			}
		}
		l, err = csvfmt.TraceLayout(list)
	case catalogProperty:
		var list []*databasev1.Property
		for _, m := range schemas {
			if s, ok := m.(*databasev1.Property); ok && s.GetMetadata().GetGroup() == group {
				list = append(list, s)
			}
		}
		l, err = csvfmt.PropertyLayout(list)
	}
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func writeArtifact(t *testing.T, spec artifactSpec) string {
	t.Helper()
	dir := t.TempDir()
	m := &manifest.Manifest{Format: "csv", Compress: "none", ArtifactVersion: manifest.ArtifactVersion}
	if spec.gzip {
		m.Compress = "gzip"
	}
	lines := map[string][]string{}
	for _, s := range spec.schemas {
		var kind string
		switch s.(type) {
		case *databasev1.Stream:
			kind = schemasync.KindStream
		case *databasev1.Measure:
			kind = schemasync.KindMeasure
		case *databasev1.Trace:
			kind = schemasync.KindTrace
		case *databasev1.Property:
			kind = schemasync.KindProperty
		}
		b, err := protojson.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		lines[kind] = append(lines[kind], string(b))
	}
	if err := os.MkdirAll(filepath.Join(dir, schemasync.Dir), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, kind := range schemasync.Kinds {
		rel := path.Join(schemasync.Dir, kind+".jsonl")
		content := strings.Join(lines[kind], "\n")
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(rel)), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		m.SchemaFiles = append(m.SchemaFiles, manifest.File{Path: rel, Bytes: uint64(len(content))})
	}
	groups := map[string]bool{}
	for ui, u := range spec.units {
		if !groups[u.group] {
			groups[u.group] = true
			m.Groups = append(m.Groups, manifest.Group{Name: u.group, Catalog: strings.ToUpper(u.catalog)})
		}
		l := layoutOf(t, u.catalog, u.group, spec.schemas)
		mu := manifest.Unit{Catalog: strings.ToUpper(u.catalog), Group: u.group, Stage: u.stage}
		// Sources in node order, as an exporter writes them, so "the first copy" is stable.
		for _, node := range slices.Sorted(maps.Keys(u.sources)) {
			rows := u.sources[node]
			sink := &diskSink{root: dir}
			prefix := fmt.Sprintf("nodes/%s/%s/%s/seg-%d/shard-0", node, u.catalog, u.group, ui)
			w := csvfmt.NewWriter(sink, csvfmt.WriterOptions{Layout: l, PathPrefix: prefix, Gzip: spec.gzip})
			for _, r := range rows {
				cells := l.NewRow()
				for i, h := range l.Header() {
					if v, ok := r[h]; ok {
						cells[i] = v
					}
				}
				if err := w.WriteRow(r["_name"], cells); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Sources = append(mu.Sources, manifest.UnitSource{Node: node, Shards: []manifest.Shard{{Files: sink.paths}}})
		}
		m.Units = append(m.Units, mu)
	}
	if err := manifest.Write(filepath.Join(dir, manifest.FileName), m); err != nil {
		t.Fatal(err)
	}
	return dir
}

func streamRow(id, ts, svc, latency string) map[string]string {
	return map[string]string{"_name": "sw", "_element_id": id, "_timestamp": ts, "searchable.svc": svc, "searchable.latency": latency}
}

func measureRow(version, svc, value string) map[string]string {
	return map[string]string{"_name": "cpm", "_timestamp": "1000", "_version": version, "default.svc": svc, "default.layer": "GENERAL", "_field.value": value}
}

func traceRow(traceID, spanID, svc string) map[string]string {
	return map[string]string{"_name": "segment", "trace_id": traceID, "span_id": spanID, "_span": "AQID", "ts": "1000", "svc": svc}
}

func propertyRow(id, rev, deleteTime, title string) map[string]string {
	return map[string]string{
		"_name": "ui", "_id": id, "_mod_revision": rev, "_create_revision": "1",
		"_updated_at": "5000", "_delete_time": deleteTime, "title": title,
	}
}

func allSchemas() []proto.Message {
	return []proto.Message{testStream("sg", false), testMeasure("mg"), testTrace("tg"), testProperty("pg")}
}

func baseUnits() []unitSpec {
	return []unitSpec{
		{catalog: catalogStream, group: "sg", stage: "hot", sources: map[string][]map[string]string{
			"n1": {streamRow("00000000000000a1", "1000", "a", "10"), streamRow("00000000000000a2", "2000", "b", "20")},
			"n2": {streamRow("00000000000000a1", "1000", "a", "10")},
		}},
		{catalog: catalogMeasure, group: "mg", stage: "hot", sources: map[string][]map[string]string{
			"n1": {measureRow("1", "a", "5"), measureRow("1", "b", "6")},
		}},
		{catalog: catalogTrace, group: "tg", stage: "hot", sources: map[string][]map[string]string{
			"n1": {traceRow("t1", "s1", "a"), traceRow("t1", "s2", "a")},
		}},
		{catalog: catalogProperty, group: "pg", stage: "", sources: map[string][]map[string]string{
			"n1": {propertyRow("p1", "1", "7000", "old"), propertyRow("p1", "2", "0", "new"), propertyRow("p2", "1", "0", "x")},
		}},
	}
}

func compare(t *testing.T, a, b artifactSpec, opts Options) *Report {
	t.Helper()
	rep, err := Compare(writeArtifact(t, a), writeArtifact(t, b), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func expectKinds(t *testing.T, rep *Report, kinds ...string) {
	t.Helper()
	got := make([]string, 0, len(rep.Diffs))
	for _, d := range rep.Diffs {
		got = append(got, d.Kind)
	}
	if strings.Join(got, ",") != strings.Join(kinds, ",") {
		t.Fatalf("diff kinds = %v, want %v; diffs:\n%s", got, kinds, render(rep))
	}
}

func render(rep *Report) string {
	var sb strings.Builder
	for _, d := range rep.Diffs {
		sb.WriteString(d.String())
		sb.WriteByte('\n')
	}
	return sb.String()
}

func TestIdentical(t *testing.T) {
	spec := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	rep := compare(t, spec, spec, Options{})
	if !rep.OK() {
		t.Fatalf("unexpected diffs:\n%s", render(rep))
	}
	want := map[string]int{catalogMeasure: 2, catalogProperty: 2, catalogStream: 2, catalogTrace: 2}
	if len(rep.Stats) != len(want) {
		t.Fatalf("stats = %+v", rep.Stats)
	}
	for _, st := range rep.Stats {
		if st.RowsA != want[st.Catalog] || st.RowsB != want[st.Catalog] {
			t.Fatalf("stat %+v, want %d rows", st, want[st.Catalog])
		}
	}
	if got := rep.StageCounts["property/pg/hot"]; got != [2]int{2, 2} {
		t.Fatalf("property stage count = %v", got)
	}
}

func TestMissingAndExtraRows(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	bUnits := baseUnits()
	bUnits[0].sources = map[string][]map[string]string{"n1": {streamRow("00000000000000a1", "1000", "a", "10")}}
	bUnits[2].sources["n1"] = append(bUnits[2].sources["n1"], traceRow("t2", "s1", "a"))
	rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: bUnits}, Options{})
	expectKinds(t, rep, KindMissingInB, KindStageCountMismatch, KindExtraInB, KindStageCountMismatch)
	if rep.Diffs[0].Catalog != catalogStream || rep.Diffs[0].Resource != "sw" || !strings.Contains(rep.Diffs[0].Key, "00000000000000a2") {
		t.Fatalf("unexpected missing diff %v", rep.Diffs[0])
	}
	if rep.Diffs[2].Catalog != catalogTrace || !strings.Contains(rep.Diffs[2].Key, `trace_id="t2"`) {
		t.Fatalf("unexpected extra diff %v", rep.Diffs[2])
	}
}

func TestValueMismatch(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	bUnits := baseUnits()
	bUnits[0].sources["n1"][1] = streamRow("00000000000000a2", "2000", "b", "21")
	rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: bUnits}, Options{})
	expectKinds(t, rep, KindValueMismatch)
	if d := rep.Diffs[0].Detail; !strings.Contains(d, `searchable.latency: A="20" B="21"`) {
		t.Fatalf("detail = %s", d)
	}
}

func TestMeasureVersionReduction(t *testing.T) {
	aUnits := baseUnits()
	aUnits[1].sources = map[string][]map[string]string{
		"n1": {measureRow("1", "a", "5"), measureRow("1", "b", "6")},
		"n2": {measureRow("3", "a", "50")},
	}
	bUnits := baseUnits()
	bUnits[1].sources = map[string][]map[string]string{
		"n1": {measureRow("3", "a", "50"), measureRow("1", "b", "6")},
	}
	a := artifactSpec{schemas: allSchemas(), units: aUnits}
	rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: bUnits}, Options{})
	if !rep.OK() {
		t.Fatalf("unexpected diffs:\n%s", render(rep))
	}
	// B keeping only the older version is a value mismatch on _version and the field.
	rep = compare(t, a, artifactSpec{schemas: allSchemas(), units: baseUnits()}, Options{})
	expectKinds(t, rep, KindValueMismatch)
	if d := rep.Diffs[0].Detail; !strings.Contains(d, `_version: A="3" B="1"`) || !strings.Contains(d, `_field.value: A="50" B="5"`) {
		t.Fatalf("detail = %s", d)
	}
}

func TestReplicaConflict(t *testing.T) {
	aUnits := baseUnits()
	aUnits[0].sources["n2"] = []map[string]string{streamRow("00000000000000a1", "1000", "a", "11")}
	aUnits[1].sources["n2"] = []map[string]string{measureRow("1", "a", "9")}
	rep := compare(t, artifactSpec{schemas: allSchemas(), units: aUnits}, artifactSpec{schemas: allSchemas(), units: baseUnits()}, Options{})
	expectKinds(t, rep, KindReplicaConflict, KindReplicaConflict)
	if rep.Diffs[0].Catalog != catalogMeasure || rep.Diffs[1].Catalog != catalogStream {
		t.Fatalf("diffs:\n%s", render(rep))
	}
}

// propertyUnits returns the base units with the property unit's sources replaced.
func propertyUnits(sources map[string][]map[string]string) []unitSpec {
	units := baseUnits()
	units[3].sources = sources
	return units
}

func TestPropertyComparesOnlyTheLatestRevision(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	latestOnly := propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p2", "1", "0", "x"), propertyRow("p1", "2", "0", "new")},
	})
	rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: latestOnly}, Options{})
	if !rep.OK() {
		t.Fatalf("older revisions only in A must be ignored:\n%s", render(rep))
	}
	if got := rep.StageCounts["property/pg/hot"]; got != [2]int{2, 2} {
		t.Fatalf("property stage count = %v, want the entity counts", got)
	}

	changed := propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p1", "2", "0", "changed"), propertyRow("p2", "1", "0", "x")},
	})
	rep = compare(t, a, artifactSpec{schemas: allSchemas(), units: changed}, Options{})
	expectKinds(t, rep, KindValueMismatch)
	if d := rep.Diffs[0].Detail; !strings.Contains(d, `title: A="new" B="changed"`) {
		t.Fatalf("detail = %s", d)
	}

	newer := propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p1", "2", "0", "new"), propertyRow("p1", "3", "0", "new"), propertyRow("p2", "1", "0", "x")},
	})
	rep = compare(t, a, artifactSpec{schemas: allSchemas(), units: newer}, Options{})
	expectKinds(t, rep, KindValueMismatch)
	if d := rep.Diffs[0].Detail; !strings.Contains(d, `_mod_revision: A="2" B="3"`) {
		t.Fatalf("detail = %s", d)
	}
}

func TestPropertyTombstone(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	// The latest revision tombstoned in B, by a copy of its own or next to a live copy.
	for _, rows := range [][]map[string]string{
		{propertyRow("p1", "2", "7500", "new"), propertyRow("p2", "1", "0", "x")},
		{propertyRow("p1", "2", "0", "new"), propertyRow("p1", "2", "7500", "new"), propertyRow("p2", "1", "0", "x")},
	} {
		rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: propertyUnits(map[string][]map[string]string{"n1": rows})}, Options{})
		expectKinds(t, rep, KindValueMismatch)
		if d := rep.Diffs[0].Detail; !strings.Contains(d, `_delete_time: A="live" B="tombstone"`) {
			t.Fatalf("detail = %s", d)
		}
	}
	// Replicas may tombstone the latest revision at different times.
	tombA := artifactSpec{schemas: allSchemas(), units: propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p1", "2", "8000", "new"), propertyRow("p2", "1", "0", "x")},
		"n2": {propertyRow("p1", "2", "9000", "new"), propertyRow("p2", "1", `\N`, "x")},
	})}
	tombB := artifactSpec{schemas: allSchemas(), units: propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p1", "1", "0", "old"), propertyRow("p1", "2", "7000", "new"), propertyRow("p2", "1", "0", "x")},
	})}
	if rep := compare(t, tombA, tombB, Options{}); !rep.OK() {
		t.Fatalf("tombstone times may differ:\n%s", render(rep))
	}
}

func TestPropertyReplicaConflictOnlyOnTheLatestRevision(t *testing.T) {
	b := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	olderDiffers := artifactSpec{schemas: allSchemas(), units: propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p1", "1", "7000", "old"), propertyRow("p1", "2", "0", "new"), propertyRow("p2", "1", "0", "x")},
		"n2": {propertyRow("p1", "1", "0", "other")},
	})}
	if rep := compare(t, olderDiffers, b, Options{}); !rep.OK() {
		t.Fatalf("an older revision's replicas are not compared:\n%s", render(rep))
	}
	latestDiffers := artifactSpec{schemas: allSchemas(), units: propertyUnits(map[string][]map[string]string{
		"n1": {propertyRow("p1", "1", "7000", "old"), propertyRow("p1", "2", "0", "new"), propertyRow("p2", "1", "0", "x")},
		"n2": {propertyRow("p1", "2", "0", "other")},
	})}
	expectKinds(t, compare(t, latestDiffers, b, Options{}), KindReplicaConflict)
}

func TestStageCountMismatch(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	bUnits := baseUnits()
	bUnits[2].stage = "warm"
	rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: bUnits}, Options{})
	expectKinds(t, rep, KindStageCountMismatch, KindStageCountMismatch)
	if rep.StageCounts["trace/tg/hot"] != [2]int{2, 0} || rep.StageCounts["trace/tg/warm"] != [2]int{0, 2} {
		t.Fatalf("stage counts = %v", rep.StageCounts)
	}
}

func TestGroupFilter(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	bUnits := baseUnits()
	bUnits[1].sources["n1"][0] = measureRow("1", "a", "999")
	b := artifactSpec{schemas: allSchemas(), units: bUnits}
	if rep := compare(t, a, b, Options{Groups: []string{"sg", "tg", "pg"}}); !rep.OK() {
		t.Fatalf("filtered comparison has diffs:\n%s", render(rep))
	}
	rep := compare(t, a, b, Options{Groups: []string{"mg"}})
	expectKinds(t, rep, KindValueMismatch)
	if len(rep.Stats) != 1 || rep.Stats[0].Catalog != catalogMeasure {
		t.Fatalf("stats = %+v", rep.Stats)
	}
}

func TestGroupsOnlyInBAreIgnored(t *testing.T) {
	aUnits := baseUnits()[:1]
	a := artifactSpec{schemas: allSchemas(), units: aUnits}
	if rep := compare(t, a, artifactSpec{schemas: allSchemas(), units: baseUnits()}, Options{}); !rep.OK() {
		t.Fatalf("unexpected diffs:\n%s", render(rep))
	}
}

func TestGzip(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits(), gzip: true}
	b := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	if rep := compare(t, a, b, Options{}); !rep.OK() {
		t.Fatalf("unexpected diffs:\n%s", render(rep))
	}
}

func TestLayoutMismatch(t *testing.T) {
	a := artifactSpec{schemas: allSchemas(), units: baseUnits()}
	bSchemas := allSchemas()
	bSchemas[0] = testStream("sg", true)
	rep := compare(t, a, artifactSpec{schemas: bSchemas, units: baseUnits()}, Options{})
	expectKinds(t, rep, KindLayoutMismatch)
	if d := rep.Diffs[0].Detail; !strings.Contains(d, "only in B: [searchable.extra]") {
		t.Fatalf("detail = %s", d)
	}
}

func TestReportSummary(t *testing.T) {
	rep := &Report{Diffs: []Diff{
		{Catalog: "stream", Group: "g", Resource: "a", Kind: KindMissingInB},
		{Catalog: "stream", Group: "g", Resource: "b", Kind: KindExtraInB},
		{Catalog: "stream", Group: "g", Resource: "c", Kind: KindValueMismatch},
	}}
	all := []string{rep.Diffs[0].String(), rep.Diffs[1].String(), rep.Diffs[2].String()}
	cases := []struct {
		want []string
		max  int
	}{
		{max: -1, want: all},
		{max: 3, want: all},
		{max: 10, want: all},
		{max: 2, want: []string{all[0], all[1], "... 1 more"}},
		{max: 0, want: []string{"... 3 more"}},
	}
	for _, tc := range cases {
		if got := rep.Summary(tc.max); !slices.Equal(got, tc.want) {
			t.Errorf("Summary(%d) = %q, want %q", tc.max, got, tc.want)
		}
	}
	if got := (&Report{}).Summary(5); len(got) != 0 {
		t.Errorf("Summary of an empty report = %q", got)
	}
}
