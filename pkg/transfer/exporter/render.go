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

package exporter

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// Render writes the report in the requested format: table (default), json or yaml.
func Render(w io.Writer, format string, rep *Report) error {
	if err := output.ValidateOutputFormat(format); err != nil {
		return err
	}
	return output.RenderAs(w, format, rep, func(tw io.Writer) error { return renderTable(tw, rep) })
}

func renderTable(w io.Writer, rep *Report) error {
	switch {
	case len(rep.MissingNodes) > 0:
		fmt.Fprintf(w, "NODES     the cluster reports %d data node(s) %v; no inventory came back from %v ⚠ (their data would not be exported)\n",
			len(rep.DataNodes), rep.DataNodes, rep.MissingNodes)
	case rep.Standalone:
		fmt.Fprintf(w, "NODES     standalone process %v\n", rep.DataNodes)
	default:
		fmt.Fprintf(w, "NODES     %d data node(s) %v, all answered\n", len(rep.DataNodes), rep.DataNodes)
	}
	if len(rep.MultiSource) > 0 {
		fmt.Fprintf(w, "MULTI-SRC %d unit(s) have more than one source; all of them are exported, the importer decides which to use\n", len(rep.MultiSource))
		for _, m := range rep.MultiSource {
			parts := make([]string, 0, len(m.Sources))
			for _, s := range m.Sources {
				parts = append(parts, fmt.Sprintf("%s(%s rows, %s)", s.Node, output.HumanCount(s.Rows), output.TimeRange(s.MinTimestamp, s.MaxTimestamp)))
			}
			fmt.Fprintf(w, "          %s  <- %s\n", m.Key, strings.Join(parts, " + "))
		}
	}
	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tCATALOG\tGROUP\tSTAGE\tSEGMENTS\tSHARDS\tPARTS\tEST-ROWS\tEST-SIZE(comp/raw)\tTIME-RANGE")
	for _, r := range rep.Rows {
		segments, parts := strconv.Itoa(r.Segments), strconv.Itoa(r.Parts)
		if r.Catalog == transfer.CatalogName(commonv1.Catalog_CATALOG_PROPERTY) {
			// Properties have no segments or parts; the counters are placeholders.
			segments, parts = "-", "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s / %s\t%s\n",
			r.Node, r.Catalog, r.Group, r.Stage, segments, r.Shards, parts,
			output.HumanCount(r.EstRows), output.HumanBytes(r.CompressedBytes), output.HumanBytes(r.UncompressedBytes), output.TimeRange(r.MinTimestamp, r.MaxTimestamp))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	node, size := rep.LargestNodeCompressedBytes()
	if node == "" {
		node = "-"
	}
	_, err := fmt.Fprintf(w, "\nSNAPSHOT  each data node pins its own snapshot; the largest node (%s) needs about %s; check df there first\n",
		node, output.HumanBytes(size))
	return err
}

// summaryRow is one (node, catalog, group, stage) line of the real-export summary.
type summaryRow struct {
	// key orders the rows: node, catalog, group and stage.
	key, node, catalog, group, stage string
	units, files, rows, bytes        uint64
	elapsed                          time.Duration
}

// elapsedKey keys the time a node spent on the units of one group; catalog is the manifest
// label (MEASURE). A node holds a group in one stage, so the stage is not needed.
func elapsedKey(node, catalog, group string) string {
	return strings.Join([]string{node, catalog, group}, "\x00")
}

// renderSummary prints what a finished export wrote, from its manifest: one row per node,
// catalog, group and stage with the time its units took (elapsed, keyed by elapsedKey),
// then the totals line with the time of the whole export.
func renderSummary(w io.Writer, dir string, m *manifest.Manifest, elapsed map[string]time.Duration, total time.Duration) error {
	byKey := map[string]*summaryRow{}
	var rows []*summaryRow
	for _, u := range m.Units {
		catalog := strings.ToLower(u.Catalog)
		for _, src := range u.Sources {
			key := strings.Join([]string{src.Node, catalog, u.Group, u.Stage}, "\x00")
			r, ok := byKey[key]
			if !ok {
				r = &summaryRow{
					key: key, node: src.Node, catalog: catalog, group: u.Group, stage: u.Stage,
					elapsed: elapsed[elapsedKey(src.Node, u.Catalog, u.Group)],
				}
				byKey[key] = r
				rows = append(rows, r)
			}
			r.units++
			r.rows += src.TotalCount
			files := slices.Clone(src.SegmentFiles)
			for _, sh := range src.Shards {
				files = append(files, sh.Files...)
				r.rows += sh.TotalCount
			}
			for _, f := range files {
				r.files, r.bytes = r.files+1, r.bytes+f.Bytes
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].key < rows[j].key })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tCATALOG\tGROUP\tSTAGE\tUNITS\tFILES\tROWS\tSIZE\tTIME")
	var files, bytes uint64
	for _, r := range rows {
		files, bytes = files+r.files, bytes+r.bytes
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\n", r.node, r.catalog, r.group, r.stage, r.units, r.files, r.rows,
			output.HumanBytes(r.bytes), output.FormatElapsed(r.elapsed))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "\nexport written to %s: %d unit(s), %d file(s), %s, %d warning(s) in %s\n",
		dir, len(m.Units), files, output.HumanBytes(bytes), len(m.Warnings), output.FormatElapsed(total))
	return err
}
