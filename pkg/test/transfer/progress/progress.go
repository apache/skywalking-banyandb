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

// Package progress counts what the tests assert on in the progress files of a data export and a
// data import, read the way the exporter and the importer read them.
package progress

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// PlanPath is the side file next to the progress file at path that holds the plan.
func PlanPath(path string) string { return resume.SidePath(path, "plan") }

// Import is an import progress file as importer.ReadProgress reads it.
type Import struct {
	importer.ProgressView
}

// ReadImport reads the import progress file at path with importer.ReadProgress; a missing one is
// (nil, nil).
func ReadImport(path string) (*Import, error) {
	v, err := readUnlessGone(path, importer.ReadProgress)
	if v == nil || err != nil {
		return nil, err
	}
	return &Import{ProgressView: *v}, nil
}

// ReadExport reads the export progress file at path with exporter.ReadProgress; a missing one is
// (nil, nil).
func ReadExport(path string) (*exporter.ProgressView, error) {
	return readUnlessGone(path, exporter.ReadProgress)
}

// readUnlessGone reads the progress file at path with read; a missing one is (nil, nil). A run
// that finishes removes the progress file and then its side files, so a read that fails because
// the file went away meanwhile is (nil, nil) too.
func readUnlessGone[T any](path string, read func(string) (*T, error)) (*T, error) {
	if missing(path) {
		return nil, nil
	}
	v, err := read(path)
	if err != nil {
		if missing(path) {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

func missing(path string) bool {
	_, err := os.Stat(path)
	return errors.Is(err, fs.ErrNotExist)
}

// Complete counts the complete work items.
func (p *Import) Complete() int {
	var n int
	for _, it := range p.Done {
		if it.Complete {
			n++
		}
	}
	return n
}

// Started counts the CSV items started and not complete.
func (p *Import) Started() int {
	var n int
	for _, it := range p.Done {
		if it.Started {
			n++
		}
	}
	return n
}

// SentWindow sums the rows of the started CSV items sent after their cursors and not
// acknowledged: the rows a resume lists as ambiguous.
func (p *Import) SentWindow() uint64 {
	var n uint64
	for _, it := range p.Done {
		if it.Started {
			n += it.SentWindow
		}
	}
	return n
}

// InflightStreamParts counts the stream and trace parts in flight.
func (p *Import) InflightStreamParts() int {
	var n int
	for _, in := range p.Inflight {
		if in.Part && isStreamOrTrace(in.Key) {
			n++
		}
	}
	return n
}

// PushedRows sums the rows every run pushed.
func (p *Import) PushedRows() uint64 { return sumRows(p.Pushed, nil) }

// StreamTracePushed sums the stream and trace rows every run pushed.
func (p *Import) StreamTracePushed() uint64 { return sumRows(p.Pushed, isStreamOrTraceCatalog) }

// StreamTraceDuplicates sums the stream and trace rows recorded as pushed again, which may be
// stored twice.
func (p *Import) StreamTraceDuplicates() uint64 { return sumRows(p.Duplicates, isStreamOrTraceCatalog) }

// StreamTraceRowsOnce is how many stream and trace rows one uninterrupted import of the plan
// pushes from the artifact m: per planned work item, the rows of its chosen sources (of a
// native shard, its parts' rows; of a CSV shard, the rows of its files; of a segment item, the
// source's).
func (p *Import) StreamTraceRowsOnce(m *manifest.Manifest) uint64 {
	var n uint64
	for _, rows := range p.streamTraceRowsOnceByGroup(m) {
		n += rows
	}
	return n
}

// StreamTraceBoundViolations checks the stream and trace rows every run pushed per (catalog,
// group) against one uninterrupted import of the plan from the artifact m: every row was pushed,
// and no more rows above them than recorded as possible duplicates of that group. A CSV artifact
// pushes every row it acknowledged once, so its counts must be equal. It lists every group that
// breaks the bound, none when all hold. The stage is left out: the plan keys carry the source
// stage and the pushed counts the target stage, which differ when the target maps the stage.
func (p *Import) StreamTraceBoundViolations(m *manifest.Manifest) []string {
	once := p.streamTraceRowsOnceByGroup(m)
	pushed, dups := byGroup(p.Pushed), byGroup(p.Duplicates)
	keys := map[string]bool{}
	for _, mp := range []map[string]uint64{once, pushed, dups} {
		for k := range mp {
			keys[k] = true
		}
	}
	exact := m.Format != manifest.FormatNative
	var out []string
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		o, pu, d := once[k], pushed[k], dups[k]
		switch {
		case pu < o:
			out = append(out, fmt.Sprintf("%s: pushed %d < one import %d", k, pu, o))
		case exact && pu != o:
			out = append(out, fmt.Sprintf("%s: pushed %d != one import %d of a CSV artifact (%d possible duplicates)", k, pu, o, d))
		case pu-o > d:
			out = append(out, fmt.Sprintf("%s: pushed %d, one import %d: more than the %d possible duplicates", k, pu, o, d))
		}
	}
	return out
}

// streamTraceRowsOnceByGroup is StreamTraceRowsOnce per "<catalog>/<group>".
func (p *Import) streamTraceRowsOnceByGroup(m *manifest.Manifest) map[string]uint64 {
	native := m.Format == manifest.FormatNative
	rows := map[string]map[string]uint64{}
	add := func(key, node string, n uint64) {
		if rows[key] == nil {
			rows[key] = map[string]uint64{}
		}
		rows[key][node] += n
	}
	for _, u := range m.Units {
		catalog := strings.ToLower(u.Catalog)
		if !isStreamOrTraceCatalog(catalog) {
			continue
		}
		prefix := fmt.Sprintf("%s/%s/%s/%s/", catalog, u.Group, u.Stage, u.Segment)
		for _, src := range u.Sources {
			add(prefix+"segment", src.Node, src.TotalCount)
			for _, sh := range src.Shards {
				n := sh.TotalCount
				// A CSV shard lists the parts its rows were read from too; it pushes its files' rows.
				if native && len(sh.Parts) > 0 {
					n = 0
					for _, part := range sh.Parts {
						n += part.TotalCount
					}
				}
				add(fmt.Sprintf("%sshard-%d", prefix, sh.Shard), src.Node, n)
			}
		}
	}
	out := map[string]uint64{}
	for _, it := range p.Plan {
		parts := strings.SplitN(it.Key, "/", 4)
		if len(parts) < 4 || !isStreamOrTraceCatalog(parts[0]) {
			continue
		}
		group := parts[0] + "/" + parts[1]
		for _, src := range it.Sources {
			out[group] += rows[it.Key][src]
		}
	}
	return out
}

// byGroup sums the stream and trace counts per "<catalog>/<group>".
func byGroup(counts []importer.RowCountView) map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range counts {
		if isStreamOrTraceCatalog(c.Catalog) {
			out[c.Catalog+"/"+c.Group] += c.Rows
		}
	}
	return out
}

// ReadManifest reads the manifest of the artifact directory dir.
func ReadManifest(dir string) (*manifest.Manifest, error) {
	return manifest.Read(dir + string(os.PathSeparator) + manifest.FileName)
}

func sumRows(counts []importer.RowCountView, keep func(catalog string) bool) uint64 {
	var n uint64
	for _, c := range counts {
		if keep == nil || keep(c.Catalog) {
			n += c.Rows
		}
	}
	return n
}

func isStreamOrTraceCatalog(catalog string) bool { return catalog == "stream" || catalog == "trace" }

func isStreamOrTrace(key string) bool {
	return strings.HasPrefix(key, "stream/") || strings.HasPrefix(key, "trace/")
}
