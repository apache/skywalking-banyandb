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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

// Catalog labels used in diffs, stats and stage-count keys.
const (
	catalogStream   = "stream"
	catalogMeasure  = "measure"
	catalogTrace    = "trace"
	catalogProperty = "property"
)

type groupKey struct {
	catalog string
	group   string
}

// resourceCols describes how one resource's rows are keyed and compared.
type resourceCols struct {
	pos        map[string]int
	headers    []string // content column headers, layout order
	idx        []int    // layout indices of the content columns
	keyHeaders []string
	keyIdx     []int // layout indices of the key columns, after _name
	versionIdx int   // layout index of _version for measures and _mod_revision for properties; -1 otherwise
	deletePos  int   // position of _delete_time in headers for properties; -1 otherwise
}

type entry struct {
	stages   map[string]struct{}
	resource string
	key      string
	conflict string
	cells    []string
	version  int64
	// tombstone ranks a property tombstone above a live copy of its revision.
	tombstone bool
}

type groupData struct {
	resources map[string]*resourceCols
	rows      map[string]*entry
	layout    *csvfmt.Layout
	catalog   string
	name      string
}

type artifact struct {
	groups map[groupKey]*groupData
}

// loadArtifact reads the groups of the artifact at dir that scope accepts; a nil scope
// accepts every group.
func loadArtifact(ctx context.Context, dir string, scope map[string]bool) (*artifact, error) {
	art, err := importer.OpenArtifact(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	m := art.Manifest
	in := func(group string) bool { return scope == nil || scope[group] }
	a := &artifact{groups: map[groupKey]*groupData{}}
	ensure := func(catalog, group string) (*groupData, error) {
		k := groupKey{catalog: strings.ToLower(catalog), group: group}
		if g, ok := a.groups[k]; ok {
			return g, nil
		}
		g, buildErr := buildGroup(k, art)
		if buildErr != nil {
			return nil, fmt.Errorf("artifact %s group %s: %w", dir, group, buildErr)
		}
		a.groups[k] = g
		return g, nil
	}
	for _, g := range m.Groups {
		if !in(g.Name) {
			continue
		}
		if _, err = ensure(g.Catalog, g.Name); err != nil {
			return nil, err
		}
	}
	gzipAll := m.Compress == "gzip"
	for _, u := range m.Units {
		if !in(u.Group) {
			continue
		}
		g, ensureErr := ensure(u.Catalog, u.Group)
		if ensureErr != nil {
			return nil, ensureErr
		}
		stage := transfer.NormalizeStage(u.Stage)
		for _, src := range u.Sources {
			files := slices.Clone(src.SegmentFiles)
			for _, sh := range src.Shards {
				files = append(files, sh.Files...)
			}
			for _, f := range files {
				gz := gzipAll || strings.HasSuffix(f.Path, ".gz")
				if err = g.readFile(dir, f.Path, gz, stage); err != nil {
					return nil, fmt.Errorf("artifact %s file %s: %w", dir, f.Path, err)
				}
			}
		}
	}
	return a, nil
}

// contentKinds are the fixed columns compared per catalog, besides the resource's own
// tag and field columns.
var contentKinds = map[string][]csvfmt.ColumnKind{
	catalogStream:   nil,
	catalogMeasure:  {csvfmt.KindVersion},
	catalogTrace:    {csvfmt.KindSpan},
	catalogProperty: {csvfmt.KindModRevision, csvfmt.KindCreateRevision, csvfmt.KindUpdatedAt, csvfmt.KindDeleteTime},
}

// keyKinds are the fixed key columns per catalog, after _name; measures prepend their
// entity tags. A property is keyed by its entity (_name, _id) alone: only its latest
// revision is compared.
var keyKinds = map[string][]csvfmt.ColumnKind{
	catalogStream:   {csvfmt.KindElementID, csvfmt.KindTimestamp},
	catalogMeasure:  {csvfmt.KindTimestamp},
	catalogTrace:    {csvfmt.KindTraceID, csvfmt.KindSpanID},
	catalogProperty: {csvfmt.KindPropertyID},
}

func buildGroup(k groupKey, art *importer.Artifact) (*groupData, error) {
	catalog, ok := transfer.CatalogByName(k.catalog)
	if !ok {
		return nil, fmt.Errorf("unknown catalog %q", k.catalog)
	}
	layout, err := art.Layout(catalog, k.group)
	if err != nil {
		return nil, err
	}
	g := &groupData{catalog: k.catalog, name: k.group, layout: layout, resources: map[string]*resourceCols{}, rows: map[string]*entry{}}
	var names []string
	entities := map[string][][2]string{} // measure -> (family, tag) of each entity tag
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		names = namesIn(art.Streams, k.group)
	case commonv1.Catalog_CATALOG_MEASURE:
		names = namesIn(art.Measures, k.group)
		for _, name := range names {
			m := art.Measures[importer.ResourceKey{Group: k.group, Name: name}]
			for _, tag := range m.GetEntity().GetTagNames() {
				family, found := entityFamily(m, tag)
				if !found {
					return nil, fmt.Errorf("measure %q entity tag %q is not declared", name, tag)
				}
				entities[name] = append(entities[name], [2]string{family, tag})
			}
		}
	case commonv1.Catalog_CATALOG_TRACE:
		names = namesIn(art.Traces, k.group)
	default:
		names = namesIn(art.Properties, k.group)
	}
	for _, name := range names {
		rc := &resourceCols{pos: map[string]int{}, versionIdx: -1, deletePos: -1}
		for _, et := range entities[name] {
			idx, ok := g.layout.TagIndex(name, et[0], et[1])
			if !ok {
				return nil, fmt.Errorf("measure %q entity tag %s.%s has no column", name, et[0], et[1])
			}
			rc.keyIdx = append(rc.keyIdx, idx)
			rc.keyHeaders = append(rc.keyHeaders, g.layout.Columns[idx].Header)
		}
		for i := range g.layout.Columns {
			c := &g.layout.Columns[i]
			if slices.Contains(keyKinds[k.catalog], c.Kind) {
				rc.keyIdx = append(rc.keyIdx, i)
				rc.keyHeaders = append(rc.keyHeaders, c.Header)
				continue
			}
			owned := (c.Kind == csvfmt.KindTag || c.Kind == csvfmt.KindField) && slices.Contains(c.Resources, name)
			if !owned && !slices.Contains(contentKinds[k.catalog], c.Kind) {
				continue
			}
			switch c.Kind {
			case csvfmt.KindVersion, csvfmt.KindModRevision:
				rc.versionIdx = i
			case csvfmt.KindDeleteTime:
				rc.deletePos = len(rc.headers)
			default:
			}
			rc.pos[c.Header] = len(rc.headers)
			rc.headers = append(rc.headers, c.Header)
			rc.idx = append(rc.idx, i)
		}
		g.resources[name] = rc
	}
	return g, nil
}

// namesIn returns the sorted names of a group's resources.
func namesIn[T any](m map[importer.ResourceKey]T, group string) []string {
	var names []string
	for k := range m {
		if k.Group == group {
			names = append(names, k.Name)
		}
	}
	slices.Sort(names)
	return names
}

func entityFamily(m *databasev1.Measure, tag string) (string, bool) {
	for _, fam := range m.GetTagFamilies() {
		for _, t := range fam.GetTags() {
			if t.GetName() == tag {
				return fam.GetName(), true
			}
		}
	}
	return "", false
}

func (g *groupData) readFile(dir, rel string, gz bool, stage string) error {
	f, err := os.Open(filepath.Join(dir, filepath.FromSlash(rel)))
	if err != nil {
		return err
	}
	defer f.Close()
	rd, err := csvfmt.NewReader(bufio.NewReader(f), g.layout, csvfmt.ReaderOptions{Gzip: gz})
	if err != nil {
		return err
	}
	defer rd.Close()
	for {
		row, nextErr := rd.Next()
		if errors.Is(nextErr, io.EOF) {
			return nil
		}
		if nextErr != nil {
			return nextErr
		}
		if err = g.layout.CheckRow(row); err != nil {
			return fmt.Errorf("record %d: %w", rd.RecordNo(), err)
		}
		g.add(row, stage)
	}
}

func (g *groupData) add(row []csvfmt.Value, stage string) {
	name := row[0].Str
	rc := g.resources[name]
	cols := g.layout.Columns
	parts := make([]string, 0, len(rc.keyIdx))
	for i, li := range rc.keyIdx {
		parts = append(parts, rc.keyHeaders[i]+"="+strconv.Quote(row[li].Encode(cols[li].Type)))
	}
	key := strings.Join(parts, " ")
	cells := make([]string, len(rc.idx))
	for i, li := range rc.idx {
		cells[i] = row[li].Encode(cols[li].Type)
	}
	tombstone := false
	if rc.deletePos >= 0 {
		// Replicas may tombstone a revision at different times: only liveness is compared.
		v := row[rc.idx[rc.deletePos]]
		tombstone = !v.Null && v.Int != 0
		cells[rc.deletePos] = "live"
		if tombstone {
			cells[rc.deletePos] = "tombstone"
		}
	}
	version := int64(math.MinInt64)
	if rc.versionIdx >= 0 && !row[rc.versionIdx].Null {
		version = row[rc.versionIdx].Int
	}
	mapKey := name + "\x00" + key
	e, ok := g.rows[mapKey]
	if !ok {
		g.rows[mapKey] = &entry{resource: name, key: key, cells: cells, version: version, tombstone: tombstone, stages: map[string]struct{}{stage: {}}}
		return
	}
	e.stages[stage] = struct{}{}
	if rc.versionIdx >= 0 {
		// Only the newest copy is kept: the largest measure version, or the latest property
		// revision with a tombstone above a live copy of the same revision.
		if version > e.version || (version == e.version && tombstone && !e.tombstone) {
			e.cells, e.version, e.tombstone, e.conflict = cells, version, tombstone, ""
			return
		}
		if version < e.version || tombstone != e.tombstone {
			return
		}
	}
	if e.conflict == "" {
		e.conflict = compareCells(rc, e.cells, rc, cells)
	}
}

func (g *groupData) rowCount() int {
	if g == nil {
		return 0
	}
	return len(g.rows)
}

func (g *groupData) stageCounts() map[string]int {
	out := map[string]int{}
	if g == nil {
		return out
	}
	for _, e := range g.rows {
		for s := range e.stages {
			out[s]++
		}
	}
	return out
}

func (g *groupData) conflicts(side string) []Diff {
	if g == nil {
		return nil
	}
	var diffs []Diff
	for _, k := range slices.Sorted(maps.Keys(g.rows)) {
		e := g.rows[k]
		if e.conflict == "" {
			continue
		}
		diffs = append(diffs, Diff{
			Catalog: g.catalog, Group: g.name, Resource: e.resource, Key: e.key, Kind: KindReplicaConflict,
			Detail: "in " + side + " (A=first copy, B=other copy): " + e.conflict,
		})
	}
	return diffs
}
