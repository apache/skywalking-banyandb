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

// Command checker validates a bydbctl CSV export artifact row by row and prints the result as
// yaml for the e2e expectations:
//
//	checker <stream|measure|trace|property> <artifact dir> [liaison host:grpc-port]
//	checker schema-only <artifact dir>
//
// Every row is checked against its group's schema, and the rows of known sources (the events
// report-events.sh reports, trace-mocker's services and their metrics, the UI template
// the setup creates) are printed under data, by resource, with their non-null columns, for the
// expectations to pin their values. The exported schema is printed under schema: the entries
// per kind, every group with its resource options and the schema of those known resources;
// the references between the entries are checked.
//
// The yaml on stdout carries no volatile values (ids, sizes, timestamps); counts that depend on
// the data OAP wrote are printed for go-template comparisons. Every check prints ok or a
// one-line reason, and the details of a failed check go to stderr. The exit code is 1 when a
// check failed or the manifest is missing.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

const (
	// eventUUIDPrefix starts the uuid of every event report-events.sh reports into sw_records.
	eventUUIDPrefix = "export-e2e-"
	// mockServicePrefix starts the names of trace-mocker's services.
	mockServicePrefix = "mock_"
	// uiTemplate is the name of the properties holding UI templates in sw_property, of which
	// the one with the e2eTemplateID is printed.
	uiTemplate = "ui_template"
	// e2eTemplateID is the id of the UI template the e2e setup creates through OAP's
	// ui-management; OAP 11 no longer seeds any template itself.
	e2eTemplateID = "transfer-e2e"
	// maxProblems bounds the diagnostics printed per check.
	maxProblems = 50
	// maxValueLength clips a printed value; a clipped value ends with "...".
	maxValueLength = 100
)

// failed records that a check failed, for the exit code.
var failed bool

func main() {
	if len(os.Args) != 3 && len(os.Args) != 4 {
		usage()
	}
	mode, dir := os.Args[1], os.Args[2]
	var liaison string
	if len(os.Args) == 4 {
		liaison = os.Args[3]
	}
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Println("manifest: missing")
		os.Exit(1)
	}
	if mode == "schema-only" {
		checkSchemaOnly(dir, m)
	} else {
		catalog, ok := transfer.CatalogByName(mode)
		if !ok {
			usage()
		}
		checkCatalog(dir, m, catalog, liaison)
	}
	if failed {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: checker <stream|measure|trace|property|schema-only> <artifact dir> [liaison host:grpc-port]")
	os.Exit(2)
}

// report collects the checks in the order they ran.
type report []struct{ name, value string }

func (r *report) check(name string, problems []string) {
	value := "ok"
	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "check %s failed:\n", name)
		for _, p := range problems[:min(len(problems), maxProblems)] {
			fmt.Fprintln(os.Stderr, "  "+p)
		}
		value = fmt.Sprintf("failed (%d problem(s), see stderr)", len(problems))
		failed = true
	}
	*r = append(*r, struct{ name, value string }{name, value})
}

func (r report) print(indent string) {
	for _, c := range r {
		fmt.Printf("%s%s: %s\n", indent, c.name, c.value)
	}
}

func checkSchemaOnly(dir string, m *manifest.Manifest) {
	fmt.Printf("format: %s\n", m.Format)
	var flag, units, nodes []string
	if !m.SchemaOnly {
		flag = []string{"manifest schemaOnly is false"}
	}
	if len(m.Units) > 0 {
		units = []string{fmt.Sprintf("%d unit(s) listed", len(m.Units))}
	}
	if _, err := os.Stat(filepath.Join(dir, "nodes")); err == nil {
		nodes = []string{"nodes/ exists"}
	}
	var r report
	r.check("schemaOnlyFlag", flag)
	r.check("noUnits", units)
	r.check("noNodeData", nodes)
	r.check("schemaFiles", schemaFileProblems(m))
	r.check("filesMatchManifest", fileProblems(dir, m.SchemaFiles))
	r.check("noWarnings", m.Warnings)
	r.check("schemaRefs", checkSchema(dir))
	fmt.Println("checks:")
	r.print("  ")
}

func schemaFileProblems(m *manifest.Manifest) []string {
	if len(m.SchemaFiles) == 0 {
		return []string{"manifest lists no schema files"}
	}
	var problems []string
	for _, f := range m.SchemaFiles {
		if !strings.HasPrefix(f.Path, schemasync.Dir+"/") {
			problems = append(problems, "schema file outside schema/: "+f.Path)
		}
	}
	return problems
}

// fileProblems checks that every listed file exists with its listed size and CRC32 and that
// nothing else but manifest.json is on disk.
func fileProblems(dir string, files []manifest.File) []string {
	var problems []string
	listed := map[string]bool{}
	for _, f := range files {
		listed[filepath.Clean(f.Path)] = true
		size, crc, err := sizeAndCRC(filepath.Join(dir, f.Path))
		if err != nil {
			problems = append(problems, "missing "+f.Path)
			continue
		}
		if size != f.Bytes {
			problems = append(problems, fmt.Sprintf("%s: %d bytes on disk, manifest says %d", f.Path, size, f.Bytes))
		}
		if crc != f.CRC32 {
			problems = append(problems, fmt.Sprintf("%s: crc32 %s on disk, manifest says %s", f.Path, crc, f.CRC32))
		}
	}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if rel != manifest.FileName && !listed[rel] {
			problems = append(problems, "unlisted file on disk "+rel)
		}
		return nil
	})
	if err != nil {
		problems = append(problems, "walk "+dir+": "+err.Error())
	}
	return problems
}

func sizeAndCRC(path string) (uint64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	h := crc32.NewIEEE()
	n, err := io.Copy(h, file)
	if err != nil {
		return 0, "", err
	}
	return uint64(n), manifest.FormatCRC32(h.Sum32()), nil
}

// groupFile is one data file of the checked catalog with the group and node it belongs to,
// and the row counts that cover it: its shard's or its source's segment files'.
type groupFile struct {
	counts  *rowCountSet
	unit    *manifest.Unit
	group   string
	node    string
	file    manifest.File
	segment bool
}

// rowCountSet pairs the rowCounts the manifest records for a set of files with the rows the
// files hold and, for a shard, the rows the session's snapshot stored: the row counts of its
// parts, or the documents of a property shard.
type rowCountSet struct {
	want      map[string]uint64
	got       map[string]uint64
	what      string
	group     string
	stored    uint64
	hasStored bool
}

// groupSchema is what the exported schema says about one group: its layout, its resources,
// the header every file must start with and, per column, the resources that may fill it.
type groupSchema struct {
	layout    *csvfmt.Layout
	resources map[string]bool
	header    []string
	owners    []map[string]bool
}

// groupResult is what the checks found in one group: data holds the rows of known sources by
// resource as printed, units the rows exported per unit and resource for the count queries,
// stored the rows the snapshot stored in the group's shards and queried the points the count
// queries answer.
type groupResult struct {
	nodes        map[string]bool
	data         map[string][][]printedValue
	units        map[*manifest.Unit]map[string]*unitRows
	header       []string
	cells        []string
	rowCounts    []string
	files        int
	segmentFiles int
	rows         uint64
	stored       uint64
	queried      uint64
}

func checkCatalog(dir string, m *manifest.Manifest, catalog commonv1.Catalog, liaison string) {
	label := strings.TrimPrefix(catalog.String(), "CATALOG_")
	var files []groupFile
	var sets []*rowCountSet
	var other []string
	for i := range m.Units {
		u := &m.Units[i]
		if u.Catalog != label {
			other = append(other, "unit of another catalog: "+u.Catalog+"/"+u.Group)
			continue
		}
		for _, src := range u.Sources {
			if len(src.SegmentFiles) > 0 {
				counts := &rowCountSet{
					want: src.RowCounts, got: map[string]uint64{}, group: u.Group,
					what: fmt.Sprintf("segment files of %s on %s", u.Segment, src.Node),
				}
				sets = append(sets, counts)
				for _, f := range src.SegmentFiles {
					files = append(files, groupFile{counts: counts, unit: u, group: u.Group, node: src.Node, segment: true, file: f})
				}
			}
			for _, sh := range src.Shards {
				counts := &rowCountSet{
					want: sh.RowCounts, got: map[string]uint64{}, group: u.Group,
					what: shardLabel(u, sh.Shard, src.Node), hasStored: true,
				}
				if catalog == commonv1.Catalog_CATALOG_PROPERTY {
					counts.stored = sh.DocCount
				}
				for _, p := range sh.Parts {
					counts.stored += p.TotalCount
				}
				sets = append(sets, counts)
				for _, f := range sh.Files {
					files = append(files, groupFile{counts: counts, unit: u, group: u.Group, node: src.Node, file: f})
				}
			}
		}
	}
	all := slices.Clone(m.SchemaFiles)
	for _, f := range files {
		all = append(all, f.file)
	}
	var version []string
	if m.ArtifactVersion != manifest.ArtifactVersion {
		version = []string{fmt.Sprintf("artifactVersion %d", m.ArtifactVersion)}
	}
	var r report
	r.check("artifactVersion", version)
	r.check("onlyThisCatalog", other)
	r.check("schemaFiles", schemaFileProblems(m))
	r.check("filesMatchManifest", fileProblems(dir, all))
	r.check("noWarnings", m.Warnings)

	schemas, schemaProblems := loadSchemas(dir, catalog)
	r.check("schemaLayouts", schemaProblems)
	results := map[string]*groupResult{}
	for _, f := range files {
		g := results[f.group]
		if g == nil {
			g = &groupResult{nodes: map[string]bool{}, data: map[string][][]printedValue{}, units: map[*manifest.Unit]map[string]*unitRows{}}
			results[f.group] = g
		}
		g.nodes[f.node] = true
		g.files++
		if f.segment {
			g.segmentFiles++
		}
		s := schemas[f.group]
		if s == nil {
			g.header = append(g.header, f.file.Path+": the schema has no layout for group "+f.group)
			continue
		}
		scanFile(filepath.Join(dir, f.file.Path), f, s, g)
	}
	for _, c := range sets {
		want := c.want
		if want == nil {
			want = map[string]uint64{}
		}
		if g := results[c.group]; g != nil && !maps.Equal(c.got, want) {
			g.rowCounts = append(g.rowCounts, fmt.Sprintf("%s: rows per resource %v, manifest rowCounts %v", c.what, c.got, want))
		}
	}
	mismatches := storedCountMismatches(catalog, sets, results)
	counted := liaison != "" && catalog == commonv1.Catalog_CATALOG_MEASURE
	if counted {
		mismatches = append(mismatches, queryCountMismatches(liaison, dir, m, results)...)
	}
	if len(mismatches) > 0 {
		failed = true
	}

	fmt.Printf("catalog: %s\nformat: %s\ncompress: %s\ngroups:\n", transfer.CatalogName(catalog), m.Format, m.Compress)
	for _, name := range slices.Sorted(maps.Keys(results)) {
		g := results[name]
		fmt.Printf("  - name: %s\n    nodes:\n", name)
		for _, node := range slices.Sorted(maps.Keys(g.nodes)) {
			fmt.Printf("      - %s\n", node)
		}
		var fixed string
		if s := schemas[name]; s != nil {
			fixed = strings.Join(fixedHeader(s.layout), ",")
		}
		fmt.Printf("    fixedColumns: %s\n    files: %d\n", fixed, g.files)
		if catalog == commonv1.Catalog_CATALOG_MEASURE {
			fmt.Printf("    segmentFiles: %d\n", g.segmentFiles)
		}
		fmt.Printf("    rows: %d\n", g.rows)
		// 0 for a group of segment-level files only, which have no stored count.
		fmt.Printf("    storedRows: %d\n", g.stored)
		if counted {
			fmt.Printf("    queriedRows: %d\n", g.queried)
		}
		var gr report
		gr.check("header", g.header)
		gr.check("cells", g.cells)
		gr.check("rowCounts", g.rowCounts)
		gr.print("    ")
		printData(g.data)
	}
	printList("countMismatches", mismatches)
	r.check("schemaRefs", checkSchema(dir))
	fmt.Println("checks:")
	r.print("  ")
}

// fixedHeader returns the headers of the model's fixed columns, the ones before any tag or field.
func fixedHeader(l *csvfmt.Layout) []string {
	var out []string
	for _, c := range l.Columns {
		if c.Kind == csvfmt.KindTag || c.Kind == csvfmt.KindField {
			break
		}
		out = append(out, c.Header)
	}
	return out
}

type named interface {
	proto.Message
	GetMetadata() *commonv1.Metadata
}

// loadSchemas reads the schema files once and builds the layout of every group of catalog the
// way a data node does.
func loadSchemas(dir string, catalog commonv1.Catalog) (map[string]*groupSchema, []string) {
	groups, err := readSchema(dir, schemasync.KindGroup, func() *commonv1.Group { return &commonv1.Group{} })
	if err != nil {
		return nil, []string{err.Error()}
	}
	var names []string
	for _, g := range groups {
		if g.GetCatalog() == catalog {
			names = append(names, g.GetMetadata().GetName())
		}
	}
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		return layoutsOf(dir, schemasync.KindStream, names, func() *databasev1.Stream { return &databasev1.Stream{} }, csvfmt.StreamLayout)
	case commonv1.Catalog_CATALOG_MEASURE:
		return layoutsOf(dir, schemasync.KindMeasure, names, func() *databasev1.Measure { return &databasev1.Measure{} }, csvfmt.MeasureLayout)
	case commonv1.Catalog_CATALOG_TRACE:
		return layoutsOf(dir, schemasync.KindTrace, names, func() *databasev1.Trace { return &databasev1.Trace{} }, csvfmt.TraceLayout)
	default:
		return layoutsOf(dir, schemasync.KindProperty, names, func() *databasev1.Property { return &databasev1.Property{} }, csvfmt.PropertyLayout)
	}
}

func layoutsOf[T named](dir, kind string, groups []string, newMsg func() T,
	build func([]T) (*csvfmt.Layout, error),
) (map[string]*groupSchema, []string) {
	all, err := readSchema(dir, kind, newMsg)
	if err != nil {
		return nil, []string{err.Error()}
	}
	byGroup := map[string][]T{}
	for _, x := range all {
		byGroup[x.GetMetadata().GetGroup()] = append(byGroup[x.GetMetadata().GetGroup()], x)
	}
	out := map[string]*groupSchema{}
	var problems []string
	for _, group := range groups {
		list := byGroup[group]
		l, buildErr := build(list)
		if buildErr != nil {
			problems = append(problems, fmt.Sprintf("group %s: %v", group, buildErr))
			continue
		}
		s := &groupSchema{layout: l, resources: map[string]bool{}, header: l.Header(), owners: make([]map[string]bool, len(l.Columns))}
		for _, x := range list {
			s.resources[x.GetMetadata().GetName()] = true
		}
		for i, c := range l.Columns {
			if c.Resources != nil {
				s.owners[i] = map[string]bool{}
				for _, res := range c.Resources {
					s.owners[i][res] = true
				}
			}
		}
		out[group] = s
	}
	return out, problems
}

// readSchema parses schema/<kind>.jsonl; a kind without a file has no entries.
func readSchema[T proto.Message](dir, kind string, newMsg func() T) ([]T, error) {
	raw, err := os.ReadFile(filepath.Join(dir, schemasync.Dir, kind+".jsonl"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []T
	for i, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		msg := newMsg()
		if err = protojson.Unmarshal([]byte(line), msg); err != nil {
			return nil, fmt.Errorf("%s/%s.jsonl line %d: %w", schemasync.Dir, kind, i+1, err)
		}
		out = append(out, msg)
	}
	return out, nil
}

// scanFile checks one CSV file against its group's schema: the header, every row's width and
// cells and the resource of every row, adds its rows per resource to its row counts and keeps
// the rows of known sources for printing.
func scanFile(path string, f groupFile, s *groupSchema, g *groupResult) {
	rel := f.file.Path
	rc, err := openData(path)
	if err != nil {
		g.header = append(g.header, rel+": "+err.Error())
		return
	}
	defer rc.Close()
	reader := csv.NewReader(bufio.NewReader(rc))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		g.header = append(g.header, rel+": no header: "+err.Error())
		return
	}
	if !slices.Equal(header, s.header) {
		g.header = append(g.header, fmt.Sprintf("%s: header has %d column(s), the schema layout %d; first difference at column %d",
			rel, len(header), len(s.header), firstDiff(header, s.header)))
	}
	columns := s.layout.Columns
	tsColumn := slices.IndexFunc(columns, func(c csvfmt.Column) bool { return c.Kind == csvfmt.KindTimestamp })
	perResource := map[string]uint64{}
	for line := 2; ; line++ {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			g.cells = append(g.cells, fmt.Sprintf("%s line %d: %v", rel, line, readErr))
			return
		}
		if len(row) != len(columns) {
			g.cells = append(g.cells, fmt.Sprintf("%s line %d: %d cell(s), header has %d", rel, line, len(row), len(columns)))
			continue
		}
		resource := row[0]
		perResource[resource]++
		g.rows++
		if tsColumn >= 0 {
			g.track(f.unit, resource, row[tsColumn])
		}
		if !s.resources[resource] {
			g.cells = append(g.cells, fmt.Sprintf("%s line %d: _name %q is not a resource of group %s", rel, line, resource, f.group))
		}
		for i, c := range columns {
			_, isNull, decodeErr := csvfmt.DecodeCell(row[i], c.Type)
			if decodeErr == nil && c.Kind == csvfmt.KindElementID {
				_, decodeErr = csvfmt.DecodeElementID(row[i])
			}
			switch {
			case decodeErr != nil:
				g.cells = append(g.cells, fmt.Sprintf("%s line %d column %s: %v", rel, line, c.Header, decodeErr))
			case c.Kind == csvfmt.KindName && isNull:
				g.cells = append(g.cells, fmt.Sprintf("%s line %d: _name is null", rel, line))
			case !isNull && s.owners[i] != nil && !s.owners[i][resource]:
				g.cells = append(g.cells, fmt.Sprintf("%s line %d: column %s belongs to %v, not to %s", rel, line, c.Header, c.Resources, resource))
			}
		}
		if values := printedRow(columns, row); knownSource(f.group, resource, values) {
			g.data[resource] = append(g.data[resource], values)
		}
	}
	for name, n := range perResource {
		f.counts.got[name] += n
	}
}

func openData(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".gz") {
		return file, nil
	}
	zr, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{zr, file}, nil
}

func firstDiff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i + 1
		}
	}
	return min(len(a), len(b)) + 1
}

// storedCountMismatches compares the rows exported from every shard with the rows the
// session's snapshot stored in it, as the Plan reported them into the manifest: a stream,
// trace or property shard is exported row for row, so the counts are equal; a measure keeps
// only the latest version of a point, so it exports at most the stored rows, and some when
// any are stored. Segment-level files (index-mode measures) have no stored count to compare.
func storedCountMismatches(catalog commonv1.Catalog, sets []*rowCountSet, results map[string]*groupResult) []string {
	var out []string
	for _, c := range sets {
		g := results[c.group]
		if !c.hasStored || g == nil {
			continue
		}
		var exported uint64
		for _, n := range c.got {
			exported += n
		}
		g.stored += c.stored
		switch {
		case catalog != commonv1.Catalog_CATALOG_MEASURE && exported != c.stored:
			out = append(out, fmt.Sprintf("%s: exported %d rows, the snapshot stored %d", c.what, exported, c.stored))
		case catalog == commonv1.Catalog_CATALOG_MEASURE && (exported > c.stored || c.stored > 0 && exported == 0):
			out = append(out, fmt.Sprintf("%s: exported %d rows of the %d point versions the snapshot stored", c.what, exported, c.stored))
		}
	}
	return out
}

// shardLabel names a shard of a unit in a diagnostic.
func shardLabel(u *manifest.Unit, shard uint32, node string) string {
	if u.Segment == "" {
		return fmt.Sprintf("group %s shard %d on %s", u.Group, shard, node)
	}
	return fmt.Sprintf("segment %s shard %d on %s", u.Segment, shard, node)
}

// printList prints a top-level yaml list of strings.
func printList(name string, items []string) {
	if len(items) == 0 {
		fmt.Printf("%s: []\n", name)
		return
	}
	fmt.Printf("%s:\n", name)
	for _, item := range items {
		fmt.Printf("  - %s\n", strconv.Quote(item))
	}
}
