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

package transfer

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// The groups and resources the data-export suites write (test/cases/<catalog>/data).
const (
	StreamGroup     = "default"
	StreamName      = "sw"
	MeasureGroup    = "sw_metric"
	MeasureName     = "service_cpm_minute"
	MeasureFile     = "service_cpm_minute_data.json"
	IndexModeGroup  = "index_mode"
	IndexModeName   = "service_traffic"
	IndexModeFile   = "service_traffic_data.json"
	TraceGroup      = "test-trace-group"
	TraceName       = "sw"
	PropertyGroup   = "sw"
	PropertyName    = "ui_menu"
	PropertyFile    = "sw1.json"
	topNResult      = "_top_n_result"
	propertyLiveID  = "1"
	deleteTimeLabel = "_delete_time"
)

// SuiteData is when and how densely the suites wrote their data.
type SuiteData struct {
	StreamBase  time.Time
	MeasureBase time.Time
	TraceBase   time.Time
	Interval    time.Duration
}

// Layouts computes the CSV layout of every group the artifact's schema holds, keyed
// "<catalog>/<group>", the way a data node does: resources sorted by name, _top_n_result left out.
func Layouts(dir string) map[string]*csvfmt.Layout {
	out := map[string]*csvfmt.Layout{}
	add := func(key string, l *csvfmt.Layout, err error) {
		gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred(), key)
		out[key] = l
	}
	for _, g := range groupNames(dir, "CATALOG_STREAM") {
		l, err := csvfmt.StreamLayout(StreamsOf(dir, g))
		add("stream/"+g, l, err)
	}
	for _, g := range groupNames(dir, "CATALOG_MEASURE") {
		l, err := csvfmt.MeasureLayout(MeasuresOf(dir, g))
		add("measure/"+g, l, err)
	}
	for _, g := range groupNames(dir, "CATALOG_TRACE") {
		l, err := csvfmt.TraceLayout(TracesOf(dir, g))
		add("trace/"+g, l, err)
	}
	for _, g := range groupNames(dir, "CATALOG_PROPERTY") {
		l, err := csvfmt.PropertyLayout(PropertiesOf(dir, g))
		add("property/"+g, l, err)
	}
	return out
}

func groupNames(dir, catalog string) []string {
	var out []string
	for _, g := range Schemas(dir, "group", func() *commonv1.Group { return &commonv1.Group{} }) {
		if g.GetCatalog().String() == catalog {
			out = append(out, g.GetMetadata().GetName())
		}
	}
	return out
}

// VerifyLayouts asserts that every file starts with its group's header and, when the
// artifact was written with annotations, with its #datatype line.
func (a *Artifact) VerifyLayouts() map[string]*csvfmt.Layout {
	layouts := Layouts(a.Dir)
	for _, f := range a.Files {
		l, ok := layouts[f.Catalog+"/"+f.Group]
		gomega.ExpectWithOffset(1, ok).To(gomega.BeTrue(), "the schema holds the group of %s", f.Path)
		gomega.ExpectWithOffset(1, f.Header).To(gomega.Equal(l.Header()), "header of %s", f.Path)
		if f.Datatype != nil {
			gomega.ExpectWithOffset(1, f.Datatype).To(gomega.Equal(l.DatatypeLine()), "#datatype of %s", f.Path)
		}
	}
	return layouts
}

// ExportedGroups lists the "<catalog>/<group>" keys that have at least one file.
func (a *Artifact) ExportedGroups() []string {
	seen := map[string]bool{}
	for _, f := range a.Files {
		seen[f.Catalog+"/"+f.Group] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// VerifySuiteData asserts that the artifact holds exactly the rows the suites wrote, cell by
// cell, in the files their group's layout describes. perNode reports for a "<catalog>/<group>"
// whether every node directory holds all of the group's rows (its replicas cover every node);
// otherwise the rows are split across the node directories.
func (a *Artifact) VerifySuiteData(d SuiteData, perNode func(group string) bool) {
	layouts := a.VerifyLayouts()
	gomega.ExpectWithOffset(1, a.ExportedGroups()).To(gomega.ConsistOf(
		"stream/"+StreamGroup, "measure/"+MeasureGroup, "measure/"+IndexModeGroup, "trace/"+TraceGroup, "property/"+PropertyGroup),
		"only the groups the suite wrote have files")
	check := func(catalog, group, name string, e Expected) {
		key := catalog + "/" + group
		byNode := a.RowsOf(catalog, group, name)
		if perNode(key) {
			gomega.ExpectWithOffset(2, byNode).To(gomega.HaveLen(len(a.Manifest.Source.Nodes)), "every node holds %s", key)
			for node, rows := range byNode {
				e.Match(rows, fmt.Sprintf("%s/%s on %s", key, name, node))
			}
			return
		}
		var all []map[string]string
		for _, rows := range byNode {
			all = append(all, rows...)
		}
		e.Match(all, key+"/"+name)
	}

	sw := StreamsOf(a.Dir, StreamGroup)
	check("stream", StreamGroup, StreamName, ExpectedStream(layouts["stream/"+StreamGroup], findByName(sw, StreamName), d.StreamBase, d.Interval))

	ml := layouts["measure/"+MeasureGroup]
	measure := findByName(MeasuresOf(a.Dir, MeasureGroup), MeasureName)
	em := ExpectedMeasure(ml, measure, MeasureFile, "_timestamp", d.MeasureBase, d.Interval)
	for _, row := range em.Rows {
		// The entity tag comes from the series and the indexed id tag from the series index:
		// neither is stored as a column. The expected rows must hold both, so the match below
		// proves the exporter recovered them.
		gomega.ExpectWithOffset(1, IsNull(row["default.entity_id"])).To(gomega.BeFalse())
		gomega.ExpectWithOffset(1, IsNull(row["default.id"])).To(gomega.BeFalse())
	}
	check("measure", MeasureGroup, MeasureName, em)

	il := layouts["measure/"+IndexModeGroup]
	traffic := findByName(MeasuresOf(a.Dir, IndexModeGroup), IndexModeName)
	gomega.ExpectWithOffset(1, traffic.GetIndexMode()).To(gomega.BeTrue())
	check("measure", IndexModeGroup, IndexModeName, ExpectedMeasure(il, traffic, IndexModeFile, "default.id", d.MeasureBase, d.Interval))
	for _, f := range a.FilesOf("measure", IndexModeGroup) {
		gomega.ExpectWithOffset(1, f.SegmentLevel).To(gomega.BeTrue(), "index-mode rows live only in segment-level files: %s", f.Path)
	}
	for _, f := range a.FilesOf("measure", MeasureGroup) {
		gomega.ExpectWithOffset(1, f.SegmentLevel).To(gomega.BeFalse(), "a group without index-mode measures has no segment-level file: %s", f.Path)
	}
	for _, f := range a.Files {
		for _, r := range f.Rows {
			gomega.ExpectWithOffset(1, r[0]).NotTo(gomega.Equal(topNResult), "%s never holds Top-N results", f.Path)
		}
	}

	tr := findByName(TracesOf(a.Dir, TraceGroup), TraceName)
	check("trace", TraceGroup, TraceName, ExpectedTrace(layouts["trace/"+TraceGroup], tr, d.TraceBase, d.Interval))

	pl := layouts["property/"+PropertyGroup]
	ep := ExpectedProperty(pl, PropertyFile)
	ep.Add(pl, TombstoneProperty(), 0)
	ep.Ignore = append(ep.Ignore, deleteTimeLabel)
	check("property", PropertyGroup, PropertyName, ep)
	for node, rows := range a.RowsOf("property", PropertyGroup, PropertyName) {
		for _, r := range rows {
			if r["_id"] == propertyLiveID {
				gomega.ExpectWithOffset(1, r[deleteTimeLabel]).To(gomega.Equal("0"), "a live property has no delete time on %s", node)
			} else {
				gomega.ExpectWithOffset(1, r[deleteTimeLabel]).NotTo(gomega.Equal("0"), "the tombstone carries its delete time on %s", node)
				gomega.ExpectWithOffset(1, IsNull(r[deleteTimeLabel])).To(gomega.BeFalse())
			}
		}
	}
	a.VerifyLiteralCells(d, layouts)
}

func findByName[T interface{ GetMetadata() *commonv1.Metadata }](list []T, name string) T {
	for _, x := range list {
		if x.GetMetadata().GetName() == name {
			return x
		}
	}
	gomega.ExpectWithOffset(2, false).To(gomega.BeTrue(), "the schema holds %s", name)
	var zero T
	return zero
}

// RowsCount counts the rows of one catalog and group across every node.
func RowsCount(a *Artifact, catalog, group string) int {
	var n int
	for _, f := range a.FilesOf(catalog, group) {
		n += len(f.Rows)
	}
	return n
}

// ExpectTopNSkippedSilently asserts that an exported measure group with a Top-N aggregation
// leaves its _top_n_result measure out without a warning: a target recomputes it.
func ExpectTopNSkippedSilently(a *Artifact) {
	aggs := Schemas(a.Dir, "topNAggregation", func() *databasev1.TopNAggregation { return &databasev1.TopNAggregation{} })
	withTopN := map[string]bool{}
	for _, agg := range aggs {
		withTopN[agg.GetMetadata().GetGroup()] = true
	}
	var checked int
	for _, key := range a.ExportedGroups() {
		group, isMeasure := strings.CutPrefix(key, "measure/")
		if !isMeasure || !withTopN[group] {
			continue
		}
		checked++
		gomega.ExpectWithOffset(1, a.Manifest.Warnings).NotTo(gomega.ContainElement(gomega.SatisfyAll(
			gomega.ContainSubstring("measure/"+group+"/"), gomega.ContainSubstring(topNResult))),
			"group %s has a Top-N aggregation", group)
	}
	gomega.ExpectWithOffset(1, checked).To(gomega.BeNumerically(">", 0), "an exported measure group has a Top-N aggregation")
}

// ExpectRolledFiles asserts the shape of an export written with --compress none and a
// one-byte --max-file-size: plain .csv files of one row each, every file of a writer
// numbered consecutively and starting with the header (VerifyLayouts checks its content).
func ExpectRolledFiles(a *Artifact) {
	byPrefix := map[string][]*CSVFile{}
	for _, f := range a.Files {
		gomega.ExpectWithOffset(1, f.Path).To(gomega.HaveSuffix(".csv"))
		gomega.ExpectWithOffset(1, f.Rows).To(gomega.HaveLen(1), "a one-byte limit rolls after every row: %s", f.Path)
		byPrefix[f.NodeDir+"|"+f.Prefix] = append(byPrefix[f.NodeDir+"|"+f.Prefix], f)
	}
	var rolled int
	for prefix, files := range byPrefix {
		sort.Slice(files, func(i, j int) bool { return files[i].Seq < files[j].Seq })
		for i, f := range files {
			gomega.ExpectWithOffset(1, f.Seq).To(gomega.Equal(files[0].Seq+i), "files of %s are numbered consecutively", prefix)
		}
		if len(files) > 1 {
			rolled++
		}
	}
	gomega.ExpectWithOffset(1, rolled).To(gomega.BeNumerically(">", 0), "at least one writer rolled")
}
