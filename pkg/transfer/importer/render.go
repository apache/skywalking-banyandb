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
	"bufio"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// Render writes the plan as json, yaml or, for any other format, a table; RunImport validates
// the format. The output depends only on the plan, so a dry run and the real run print the
// same bytes.
func Render(w io.Writer, format string, p *Plan) error {
	return output.RenderAs(w, format, p, func(tw io.Writer) error { return renderTable(tw, p) })
}

// tableRow is one (catalog, group, target stage) line of the main table.
type tableRow struct {
	units, nodes   map[string]struct{}
	targets        map[string]struct{}
	catalog, group string
	stage, key     string
	rows           uint64
	lo, hi         int64
	blocked, ttl   bool
}

func renderTable(out io.Writer, p *Plan) error {
	w := bufio.NewWriter(out)
	h := p.Header
	if h.Standalone {
		fmt.Fprintf(w, "TARGET    %s | standalone\n", h.Endpoint)
	} else {
		fmt.Fprintf(w, "TARGET    %s | data nodes %d\n", h.Endpoint, h.DataNodes)
	}
	fmt.Fprintf(w, "SOURCE    %s  %s  artifactVersion=%d  exported at %s\n", h.Input, h.Format, h.ArtifactVersion, h.ExportedAt)
	renderMultiSource(w, p)
	fmt.Fprintln(w)
	rows := tableRows(p)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CATALOG\tGROUP\tSTAGE\tUNITS\tSOURCES\tROWS\tSCHEMA\tCHANNEL\tTARGET-NODES\tSTATUS\tTIME-RANGE(UTC)")
	for _, r := range rows {
		channel := "row replay"
		if r.catalog == "property" {
			channel = "property/repair"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", r.catalog, r.group, stageLabel(r.stage), len(r.units), len(r.nodes),
			output.HumanCount(r.rows), groupDisposition(p.Schema, r.group), channel, joinSet(r.targets), r.gate(), output.TimeRange(r.lo, r.hi))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	var rowsTotal, blockedRows uint64
	seen, blockedSeen := map[string]struct{}{}, map[string]struct{}{}
	for _, e := range p.Entries {
		seen[unitKey(&e.Item)] = struct{}{}
		rowsTotal += e.Item.Rows()
	}
	for _, r := range p.Rejected {
		seen[unitKey(&r.Item)] = struct{}{}
		blockedSeen[unitKey(&r.Item)] = struct{}{}
		rowsTotal += r.Item.Rows()
		blockedRows += r.Item.Rows()
	}
	fmt.Fprintf(w, "TOTAL     %d unit(s), %d work item(s), %s rows\n", len(seen), len(p.Entries)+len(p.Rejected), output.HumanCount(rowsTotal))
	fmt.Fprintf(w, "          of which refused: %d unit(s), %d work item(s), %s rows\n", len(blockedSeen), len(p.Rejected), output.HumanCount(blockedRows))
	if rc := p.RowCheck; rc != nil {
		fmt.Fprintf(w, "ROWS      checked %d row(s) of %d work item(s) read-only", rc.Rows, rc.Items)
		if rc.Largest.Bytes > 0 {
			fmt.Fprintf(w, "; largest row %d bytes (file %s record %d)", rc.Largest.Bytes, rc.Largest.File, rc.Largest.Record)
		}
		fmt.Fprintln(w)
	}
	renderSchema(w, p)
	renderProblems(w, p)
	fmt.Fprintln(w)
	fmt.Fprintf(w, "RESULT    %s\n", result(p))
	return w.Flush()
}

// renderMultiSource lists the work items exported from more than one node: replicas of one
// shard, imported from one node unless the copies differ.
func renderMultiSource(w io.Writer, p *Plan) {
	if len(p.MultiSource) == 0 {
		return
	}
	fmt.Fprintf(w, "DUPLICATES  %d work item(s) were exported from more than one node; each is imported once unless the copies differ\n",
		len(p.MultiSource))
	for _, m := range p.MultiSource {
		var imported, skipped []string
		for _, d := range m.Decisions {
			switch {
			case d.Chosen:
				imported = append(imported, d.Node+chosenWhy(d))
			case d.Reason == ReasonSubset:
				skipped = append(skipped, fmt.Sprintf("%s (same data as %s)", d.Node, d.Ref))
			default:
			}
		}
		line := fmt.Sprintf("            %s: import from %s", m.Key, strings.Join(imported, ", "))
		if len(skipped) > 0 {
			line += "; skip " + strings.Join(skipped, ", ")
		}
		fmt.Fprintln(w, line)
	}
}

// chosenWhy says why an imported copy is imported besides the first, "" for the first.
func chosenWhy(d SourceDecision) string {
	switch d.Reason {
	case ReasonCountSpread:
		return fmt.Sprintf(" (its row count differs from %s)", d.Ref)
	case ReasonNoSingleCovering:
		return " (no single copy holds all the data)"
	case ReasonUnusableRange:
		return " (its time range is unknown)"
	case ReasonAllSources:
		return " (--all-sources)"
	default:
		return ""
	}
}

// dispositionOrder orders the dispositions of a group's RESULT, the problems first.
var dispositionOrder = []Disposition{
	DispositionConflict, DispositionInvalid, DispositionMissing, DispositionOverwrite, DispositionCreate, DispositionIgnore, DispositionOK,
}

// renderSchema summarizes the artifact's schema per group: how many resources the group holds
// and what the import does with them. Resources whose schema is not applied are counted apart
// from the identical ones; a compatible _top_n_result, which the target creates with its group,
// is not counted.
func renderSchema(w io.Writer, p *Plan) {
	if len(p.Schema) == 0 {
		return
	}
	counts := map[string]map[string]int{}
	for _, a := range p.Schema {
		if a.Name == csvfmt.TopNResultMeasure && a.Disposition == DispositionOK {
			continue
		}
		label := strings.ToLower(string(a.Disposition))
		if a.Disposition == DispositionOK && a.Reason != "" {
			label = "not applied"
		}
		if counts[a.Group] == nil {
			counts[a.Group] = map[string]int{}
		}
		counts[a.Group][label]++
	}
	labels := make([]string, 0, len(dispositionOrder)+1)
	for _, d := range dispositionOrder {
		labels = append(labels, strings.ToLower(string(d)))
	}
	labels = append(labels, "not applied")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "SCHEMA")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  GROUP\tRESOURCES\tRESULT")
	for _, g := range slices.Sorted(maps.Keys(counts)) {
		total := 0
		var parts []string
		for _, l := range labels {
			if n := counts[g][l]; n > 0 {
				total += n
				parts = append(parts, fmt.Sprintf("%d %s", n, l))
			}
		}
		fmt.Fprintf(tw, "  %s\t%d\t%s\n", g, total, strings.Join(parts, ", "))
	}
	_ = tw.Flush()
}

// renderProblems lists, one sentence each, what stops the import or needs attention: the
// schema resources that refuse it, the errors, the work items a gate refuses and the warnings.
// Nothing is printed when there is no problem.
func renderProblems(w io.Writer, p *Plan) {
	lines := schemaProblems(p.Schema)
	lines = append(lines, p.Errors...)
	lines = append(lines, p.Blockers...)
	lines = append(lines, p.GateDetails...)
	for _, warning := range p.Warnings {
		lines = append(lines, "warning: "+warning)
	}
	if len(lines) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "PROBLEMS")
	for _, l := range lines {
		fmt.Fprintf(w, "  - %s\n", l)
	}
}

// schemaProblems states the schema resources that refuse the import: the missing ones in one
// sentence, every invalid and conflicting one on its own, a conflict with its differences and
// the rule that would accept it. A resource whose rows lose or blank columns is a warning.
func schemaProblems(actions []SchemaAction) []string {
	var out []string
	missing, missingGroups := 0, map[string]struct{}{}
	for _, a := range actions {
		switch a.Disposition {
		case DispositionMissing:
			missing++
			missingGroups[a.Group] = struct{}{}
		case DispositionInvalid:
			out = append(out, fmt.Sprintf("%s is invalid: %s", a.ID(), a.Reason))
		case DispositionConflict:
			line := fmt.Sprintf("%s conflicts with the target: %s", a.ID(), a.Reason)
			for _, d := range a.Diffs {
				line += fmt.Sprintf("; %s is %s in the artifact, %s on the target", d.Path, d.Artifact, d.Target)
			}
			if a.Match != "" {
				line += fmt.Sprintf("; to accept it, add the schemaConflictPolicy rule \"match: %s\" with policy ignore or overwrite", a.Match)
			}
			out = append(out, line)
		default:
			if len(a.Notes) > 0 {
				out = append(out, fmt.Sprintf("warning: %s is written under the target schema: %s", a.ID(), strings.Join(a.Notes, "; ")))
			}
		}
	}
	if missing > 0 {
		out = append([]string{fmt.Sprintf("%d resource(s) of group(s) %s do not exist on the target, and the import does not create them "+
			"(--include-schema=false, or the artifact has no schema)", missing, strings.Join(slices.Sorted(maps.Keys(missingGroups)), ", "))}, out...)
	}
	return out
}

// result is the plan's conclusion in one line; PROBLEMS says why when it refuses the import.
func result(p *Plan) string {
	if p.Refused() {
		return fmt.Sprintf("refused, nothing written (exit %d)", exitcode.ExitPreflight)
	}
	if p.SchemaOnly {
		n := 0
		for _, a := range p.Schema {
			if a.Disposition == DispositionCreate || a.Disposition == DispositionOverwrite {
				n++
			}
		}
		return fmt.Sprintf("schema only: %d resource(s) to create or overwrite, no data sent", n)
	}
	var rows uint64
	for _, e := range p.Entries {
		rows += e.Item.Rows()
	}
	s := fmt.Sprintf("import %d work item(s), %s rows", len(p.Entries), output.HumanCount(rows))
	if len(p.Rejected) > 0 {
		s += fmt.Sprintf("; %d work item(s) skipped", len(p.Rejected))
	}
	return s
}

// refusalReasons explains why Refused holds, without the exit code or what was written.
func refusalReasons(p *Plan) string {
	var why []string
	if p.SchemaBlocked() {
		why = append(why, "a schema resource is CONFLICT, INVALID or MISSING")
	}
	why = append(why, p.Errors...)
	if !p.SchemaOnly {
		why = append(why, p.Blockers...)
		if p.rejectedBy(GateRoute) {
			why = append(why, "a target route cannot be written to")
		}
		if p.rejectedBy(GateRows) {
			why = append(why, "rows fail the row check")
		}
		if p.rejectedBy(GateCoverage) {
			why = append(why, "--strict-coverage refuses multi-source units")
		}
	}
	if len(why) == 0 {
		why = append(why, "every work item is rejected")
	}
	return strings.Join(why, "; ")
}

// unitKey identifies the manifest unit of an item.
func unitKey(item *WorkItem) string {
	return item.CatalogName() + "/" + item.Group + "/" + item.Stage + "/" + item.Segment
}

func tableRows(p *Plan) []*tableRow {
	byKey := map[string]*tableRow{}
	var rows []*tableRow
	add := func(item *WorkItem, stage string, targets []string) *tableRow {
		key := item.CatalogName() + "\x00" + item.Group + "\x00" + stage
		r, ok := byKey[key]
		if !ok {
			r = &tableRow{
				catalog: item.CatalogName(), group: item.Group, stage: stage, key: key,
				units: map[string]struct{}{}, nodes: map[string]struct{}{}, targets: map[string]struct{}{},
			}
			byKey[key] = r
			rows = append(rows, r)
		}
		r.units[unitKey(item)] = struct{}{}
		for _, s := range item.Sources {
			r.nodes[s.Node] = struct{}{}
		}
		lo, hi := item.TimeRange()
		transfer.MergeRange(&r.lo, &r.hi, lo, hi)
		r.rows += item.Rows()
		for _, n := range targets {
			r.targets[n] = struct{}{}
		}
		return r
	}
	for i := range p.Entries {
		e := &p.Entries[i]
		r := add(&e.Item, e.TargetStage, e.TargetNodes)
		r.ttl = r.ttl || e.TTLExpired
	}
	for i := range p.Rejected {
		rej := &p.Rejected[i]
		r := add(&rej.Item, rej.TargetStage, rej.TargetNodes)
		switch rej.Gate {
		case GateSchema, GateRoute, GateRows, GateCoverage:
			r.blocked = true
		case GateTTL:
			r.ttl = true
		}
	}
	slices.SortFunc(rows, func(a, b *tableRow) int { return strings.Compare(a.key, b.key) })
	return rows
}

func (r *tableRow) gate() string {
	switch {
	case r.blocked:
		return "refused"
	case r.ttl:
		return "expired"
	default:
		return "ok"
	}
}

// dispositionRank orders dispositions from the most to the least severe for the SCHEMA column.
var dispositionRank = []Disposition{
	DispositionInvalid, DispositionConflict, DispositionMissing, DispositionOverwrite, DispositionCreate, DispositionIgnore, DispositionOK,
}

// groupDisposition is the most severe disposition of a group's resources.
func groupDisposition(actions []SchemaAction, group string) string {
	best := len(dispositionRank) - 1
	for _, a := range actions {
		if a.Group != group {
			continue
		}
		if i := slices.Index(dispositionRank, a.Disposition); i >= 0 && i < best {
			best = i
		}
	}
	return string(dispositionRank[best])
}

func joinSet(set map[string]struct{}) string {
	if len(set) == 0 {
		return "-"
	}
	return strings.Join(slices.Sorted(maps.Keys(set)), ",")
}
