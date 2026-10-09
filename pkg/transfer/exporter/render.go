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
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

// ValidateOutputFormat accepts the dry-run renderings table (also ""), yaml and json and
// rejects anything else as a usage error.
func ValidateOutputFormat(format string) error {
	switch format {
	case "", "table", "yaml", "json":
		return nil
	default:
		return Exit(ExitUsage, "unsupported output format %q (table|yaml|json)", format)
	}
}

// Render writes the report in the requested format: table (default), json or yaml.
func Render(w io.Writer, format string, rep *Report) error {
	if err := ValidateOutputFormat(format); err != nil {
		return err
	}
	switch format {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	case "yaml":
		out, err := yaml.Marshal(rep)
		if err != nil {
			return err
		}
		_, err = w.Write(out)
		return err
	default:
		return renderTable(w, rep)
	}
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
				parts = append(parts, fmt.Sprintf("%s(%s rows, %s)", s.Node, humanCount(s.Rows), timeRange(s.MinTimestamp, s.MaxTimestamp)))
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
			humanCount(r.EstRows), humanBytes(r.CompressedBytes), humanBytes(r.UncompressedBytes), timeRange(r.MinTimestamp, r.MaxTimestamp))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	node, size := rep.LargestNodeCompressedBytes()
	if node == "" {
		node = "-"
	}
	fmt.Fprintf(w, "\nSNAPSHOT  each data node pins its own snapshot; the largest node (%s) needs about %s; check df there first\n",
		node, humanBytes(size))
	return nil
}

var (
	countSuffixes = []string{"", "K", "M", "G", "T"}
	byteSuffixes  = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
)

// humanCount renders a count with one decimal and a K/M/G/T suffix; a value that would
// round up to 1000.0 of a unit carries into the next one (999_999 -> 1.0M).
func humanCount(n uint64) string { return humanUnits(n, 1000, countSuffixes, "") }

// humanBytes renders bytes in IEC units with one decimal (humanize.IBytes rounds to whole
// units from 10 upwards, so 18.3 GiB would print as 18 GiB).
func humanBytes(n uint64) string { return humanUnits(n, 1024, byteSuffixes, " ") }

// humanUnits divides n by base until it fits one decimal below base and appends that
// unit's suffix; suffixes[0] names the base unit, in which n prints as a plain integer.
func humanUnits(n uint64, base float64, suffixes []string, sep string) string {
	carry := base - 0.05
	v := float64(n)
	for i := 0; ; i++ {
		if i == len(suffixes)-1 || v < carry {
			if i == 0 {
				return strconv.FormatUint(n, 10) + sep + suffixes[i]
			}
			return fmt.Sprintf("%.1f%s%s", v, sep, suffixes[i])
		}
		v /= base
	}
}

// timeRange renders the bounds as MM-DD, with the year when the bounds fall in different years.
func timeRange(minTS, maxTS int64) string {
	if minTS == 0 && maxTS == 0 {
		return "-"
	}
	lo, hi := time.Unix(0, minTS).UTC(), time.Unix(0, maxTS).UTC()
	layout := "01-02"
	if lo.Year() != hi.Year() {
		layout = "2006-01-02"
	}
	return lo.Format(layout) + " ~ " + hi.Format(layout)
}
