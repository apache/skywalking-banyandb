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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/stream"
)

func newVerifyCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Inspect a copy run: per-(entry, group) source vs target row counts, target segment grid alignment, sidx doc counts",
		Long: `verify reads the same plan.yaml that the copy run consumed and walks
each (entry, group) read-only:

  - sums source row count by opening every src/seg-*/shard-*/<partID>
    and totalling partMetadata.TotalCount
  - enumerates target/seg-*/, opens each part and the per-seg union sidx
  - flags whether each target seg's start time aligns to the entry
    stage's SegmentInterval grid (IntervalRule.Standard)
  - prints all numbers (source rows, target rows per seg, sidxDocs,
    aligned y/n) to stdout for the operator to inspect

verify never fails on mismatch — it just reports. Use it after a copy
run, optionally after the data-copy ↔ data swap, to confirm the
migration result.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			plan, err := migration.LoadCopyPlan(configPath)
			if err != nil {
				return err
			}

			executors := catalogExecutors()
			cls, clsErr := plan.ClassifyGroups(executors)
			if clsErr != nil {
				return clsErr
			}

			ctx, stop := signal.NotifyContext(context.Background(),
				os.Interrupt, syscall.SIGTERM)
			defer stop()

			tally := &verifyTally{}
			runErr := plan.RunVerify(ctx, cls, executors, func(report any) {
				switch r := report.(type) {
				case measure.EntryGroupReport:
					printOneReport(r)
					tally.absorb(r)
				case stream.EntryGroupReport:
					printOneStreamReport(r)
					tally.absorbStream(r)
				}
			})
			tally.printSummary()
			return runErr
		},
	}

	cmd.Flags().StringVar(&configPath, "copy-config", "",
		"path to the YAML migration copy plan that was used for `migration copy` (required)")
	_ = cmd.MarkFlagRequired("copy-config")
	return cmd
}

// verifyTally accumulates the per-(node, group) findings the callback
// stream emits so we can print a single roll-up SUMMARY block at the
// end of the run — the per-report stream prints itself.
// hasStream / hasMeasure track which catalogs reported, so printSummary
// can pick the right per-catalog messaging (a mixed plan prints both).
type verifyTally struct {
	mismatches        []verifyMismatch
	coverage          map[string]map[string]coverageState // node → group → (src/tgt presence)
	nodeOrder         []string                            // first-seen ordering
	groupOrder        []string                            // first-seen ordering
	srcRowsTotal      uint64
	tgtRowsTotal      uint64
	imSrcDocs         uint64 // index-mode source sidx docs
	imTgtDocs         uint64 // index-mode target sidx docs
	imValueMismatches int    // index-mode (segment, series) value-digest mismatches
	imDocIDIssues     int    // index-mode (entry, group) failing doc-id reconcile
	segsTotal         int
	segsMisaligned    int
	hasStream         bool
	hasMeasure        bool
	hasNormalMeasure  bool // a non-index-mode measure report was absorbed (part-row data)
	hasIndexMode      bool
}

// coverageState records whether SOURCE and TARGET independently hold
// any rows for one (node, group) pair. Four combinations exist, each
// surfaced by a distinct token in the coverage table:
//
//	src=true, tgt=true   → "✓"  (both present, normal copy success)
//	src=true, tgt=false  → "S"  (src has data but target is empty — copy lost this group)
//	src=false, tgt=true  → "T"  (target has data without a source — orphan / leftover)
//	src=false, tgt=false → "--" (neither — PVC hash sharding excluded this group, normal)
type coverageState struct {
	src bool
	tgt bool
}

// verifyMismatch records one (node, group) where source row count
// did NOT equal target row count — surfaced in the SUMMARY block so
// the operator immediately sees which PVCs / groups need follow-up.
type verifyMismatch struct {
	Stage    string
	NodeName string
	Group    string
	SrcRows  uint64
	TgtRows  uint64
}

// nodeLabel picks the coverage-table row label for one report. Entries
// usually carry exactly one node (live mode), in which case the label IS
// that node. When an entry references multiple nodes (backup-mode plans
// that fan one target out across several backup-node dirs), the report's
// SrcRows / TargetSegs are aggregated across all of them — so we join the
// node names with `,` to make it explicit that the row aggregates more
// than one node. Falls back to the entry stage when nodes is empty
// (defensive: validation requires at least one).
func nodeLabel(nodes []string, stage string) string {
	switch len(nodes) {
	case 0:
		return stage
	case 1:
		return nodes[0]
	default:
		return strings.Join(nodes, ",")
	}
}

// tallySeg is the catalog-neutral slice of one target-segment report the
// tally needs; the absorb adapters project both catalogs' reports onto it.
type tallySeg struct {
	rows    uint64
	aligned bool
}

// absorbCommon folds one (entry, group) report — already projected to the
// catalog-neutral shape — into the coverage / mismatch / alignment tallies.
func (t *verifyTally) absorbCommon(stage string, nodes []string, group string, srcRows uint64, segs []tallySeg) {
	node := nodeLabel(nodes, stage)
	if t.coverage == nil {
		t.coverage = make(map[string]map[string]coverageState)
	}
	if _, ok := t.coverage[node]; !ok {
		t.coverage[node] = make(map[string]coverageState)
		t.nodeOrder = append(t.nodeOrder, node)
	}
	if !slices.Contains(t.groupOrder, group) {
		t.groupOrder = append(t.groupOrder, group)
	}

	t.srcRowsTotal += srcRows
	var tgtRows uint64
	for _, s := range segs {
		tgtRows += s.rows
		if !s.aligned {
			t.segsMisaligned++
		}
	}
	t.tgtRowsTotal += tgtRows
	t.segsTotal += len(segs)

	t.coverage[node][group] = coverageState{
		src: srcRows > 0,
		tgt: tgtRows > 0,
	}

	if tgtRows != srcRows {
		t.mismatches = append(t.mismatches, verifyMismatch{
			Stage:    stage,
			NodeName: node,
			Group:    group,
			SrcRows:  srcRows,
			TgtRows:  tgtRows,
		})
	}
}

func (t *verifyTally) absorb(r measure.EntryGroupReport) {
	t.hasMeasure = true
	if r.IndexMode {
		t.absorbIndexMode(r)
		return
	}
	t.hasNormalMeasure = true
	segs := make([]tallySeg, len(r.TargetSegs))
	for i, s := range r.TargetSegs {
		segs[i] = tallySeg{rows: s.Rows, aligned: s.Aligned}
	}
	t.absorbCommon(r.EntryStage, r.EntryNodes, r.Group, r.SrcRows, segs)
}

// absorbIndexMode folds an index-mode (entry, group) report into the tally.
// Index-mode data lives in sidx documents, not part rows, and a coarser target
// grid legitimately merges several source docs of one series into one — so the
// doc COUNT differing src↔tgt is normal and must NOT be reported as a row-count
// mismatch. The real signals are the per-(segment, series) value-digest
// mismatches and the doc-id reconcile, tracked separately and folded into the
// final verdict.
func (t *verifyTally) absorbIndexMode(r measure.EntryGroupReport) {
	t.hasIndexMode = true
	node := nodeLabel(r.EntryNodes, r.EntryStage)
	if t.coverage == nil {
		t.coverage = make(map[string]map[string]coverageState)
	}
	if _, ok := t.coverage[node]; !ok {
		t.coverage[node] = make(map[string]coverageState)
		t.nodeOrder = append(t.nodeOrder, node)
	}
	if !slices.Contains(t.groupOrder, r.Group) {
		t.groupOrder = append(t.groupOrder, r.Group)
	}
	t.coverage[node][r.Group] = coverageState{src: r.SrcDocs > 0, tgt: r.TgtDocs > 0}
	t.imSrcDocs += r.SrcDocs
	t.imTgtDocs += r.TgtDocs
	t.imValueMismatches += len(r.ValueMismatches)
	if r.TgtDistinctIDs != r.ExpectDistinctIDs {
		t.imDocIDIssues++
	}
}

func (t *verifyTally) printSummary() {
	if t.hasStream && !t.hasMeasure {
		fmt.Println("== SUMMARY (stream) ==")
	} else {
		fmt.Println("== SUMMARY ==")
	}
	t.printCoverageTable()
	fmt.Println()
	// Part-row counters only mean something for stream / normal-measure data; a
	// pure index-mode run has none, so printing "0" there reads as "nothing was
	// copied". Suppress them unless such data was actually reported.
	hasRowData := t.hasStream || t.hasNormalMeasure
	if hasRowData {
		fmt.Printf("  target segments                : %d\n", t.segsTotal)
		fmt.Printf("  target segments misaligned     : %d\n", t.segsMisaligned)
		fmt.Printf("  source rows total              : %d\n", t.srcRowsTotal)
		fmt.Printf("  target rows total              : %d\n", t.tgtRowsTotal)
	}
	imClean := t.imValueMismatches == 0 && t.imDocIDIssues == 0
	if t.hasIndexMode {
		imTag := "OK"
		if !imClean {
			imTag = "MISMATCH"
		}
		fmt.Printf("  index-mode src docs            : %d\n", t.imSrcDocs)
		fmt.Printf("  index-mode tgt docs            : %d (tgt<src is normal: a coarser grid merges same-series docs)\n", t.imTgtDocs)
		fmt.Printf("  index-mode reconcile           : %d value mismatch(es), %d doc-id issue(s)  [%s]\n",
			t.imValueMismatches, t.imDocIDIssues, imTag)
	}
	if len(t.mismatches) == 0 && imClean {
		if hasRowData && t.tgtRowsTotal > t.srcRowsTotal {
			fmt.Printf("  diff (tgt - src)               : %+d (UNEXPECTED — target has MORE rows than source)\n",
				int64(t.tgtRowsTotal)-int64(t.srcRowsTotal))
		} else {
			switch {
			case t.hasStream && !t.hasMeasure:
				fmt.Println("  src == tgt  (stream invariant: no rows dropped)")
			case t.hasIndexMode && !t.hasNormalMeasure:
				fmt.Println("  src == tgt  (index-mode: every series preserved, max version per segment)")
			default:
				fmt.Println("  src == tgt")
			}
		}
		return
	}
	if !imClean {
		// Surface the index-mode failure regardless of whether a row-count
		// mismatch table also follows, so a mixed run never hides it.
		fmt.Println()
		fmt.Printf("  index-mode verify FAILED: %d value mismatch(es), %d doc-id issue(s) — "+
			"see the per-(entry, group) reports above for the offending (segment, series).\n",
			t.imValueMismatches, t.imDocIDIssues)
	}
	if len(t.mismatches) == 0 {
		return
	}
	fmt.Println()
	if t.hasStream && !t.hasMeasure {
		fmt.Println("  row-count mismatches (stream must have 0 diff — any non-zero is a BUG):")
	} else {
		fmt.Println("  row-count mismatches (tgt - src, negative = rows dropped by copy):")
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "    stage\tnode\tgroup\tsrc\ttgt\tdiff")
	var total int64
	for _, m := range t.mismatches {
		diff := int64(m.TgtRows) - int64(m.SrcRows)
		total += diff
		fmt.Fprintf(tw, "    %s\t%s\t%s\t%d\t%d\t%+d\n",
			m.Stage, m.NodeName, m.Group, m.SrcRows, m.TgtRows, diff)
	}
	fmt.Fprintf(tw, "    %s\t\t\t\t\t%+d\n", "TOTAL", total)
	_ = tw.Flush()
	if t.hasStream {
		fmt.Println()
		fmt.Println("  note: stream never deduplicates — any stream row-count diff indicates a")
		fmt.Println("        real migration bug. Run `migration analyze` for the exact missing rows.")
	}
	if t.hasMeasure && total < 0 {
		fmt.Println()
		fmt.Println("  note: rows dropped are caused by slow-path mustInitFromDataPoints")
		fmt.Println("        deduping (seriesID, timestamp) within each chunk flush — banyandb's")
		fmt.Println("        merger can leave duplicated boundary rows in source parts, and")
		fmt.Println("        only the latest version of each (sid, ts) survives in target.")
		fmt.Println("        Run `migration analyze --entry-idx <i> --group <g>` for the exact")
		fmt.Println("        missing (sid, ts, version) rows of any (entry, group) shown above.")
	}
}

// printCoverageTable renders a "✓ / --" grid of node × group so the
// operator can see at a glance which PVCs hold data for which groups
// (banyandb's hash sharding leaves some cells empty — that's normal).
func (t *verifyTally) printCoverageTable() {
	if len(t.nodeOrder) == 0 || len(t.groupOrder) == 0 {
		return
	}
	nodeWidth := len("node")
	for _, n := range t.nodeOrder {
		if w := len(n); w > nodeWidth {
			nodeWidth = w
		}
	}
	colWidth := make([]int, len(t.groupOrder))
	for i, g := range t.groupOrder {
		colWidth[i] = len(g)
		if colWidth[i] < 2 {
			colWidth[i] = 2
		}
	}
	fmt.Println("  data coverage per (node, group):")
	fmt.Println("    ✓  = src has rows AND tgt has rows  (normal copy success)")
	fmt.Println("    S  = src has rows, tgt is empty     (copy lost this group — investigate)")
	fmt.Println("    T  = tgt has rows, src is empty     (orphan target — investigate)")
	fmt.Println("    -- = neither side has rows          (PVC does not hold this group; hash-sharding normal)")
	border := func(left, mid, right, fill string) string {
		s := left + strings.Repeat(fill, nodeWidth+2)
		for _, w := range colWidth {
			s += mid + strings.Repeat(fill, w+2)
		}
		return s + right
	}
	fmt.Println("    " + border("┌", "┬", "┐", "─"))
	headerCells := []string{padString("node", nodeWidth)}
	for i, g := range t.groupOrder {
		headerCells = append(headerCells, padString(g, colWidth[i]))
	}
	fmt.Println("    │ " + strings.Join(headerCells, " │ ") + " │")
	fmt.Println("    " + border("├", "┼", "┤", "─"))
	for _, n := range t.nodeOrder {
		rowCells := []string{padString(n, nodeWidth)}
		for i, g := range t.groupOrder {
			s := t.coverage[n][g]
			var tok string
			switch {
			case s.src && s.tgt:
				tok = "✓"
			case s.src && !s.tgt:
				tok = "S"
			case !s.src && s.tgt:
				tok = "T"
			default:
				tok = "--"
			}
			rowCells = append(rowCells, padString(tok, colWidth[i]))
		}
		fmt.Println("    │ " + strings.Join(rowCells, " │ ") + " │")
	}
	fmt.Println("    " + border("└", "┴", "┘", "─"))
}

// padString right-pads s with spaces so it visually occupies w cells
// in a fixed-width terminal. We measure by RUNE count, not byte count,
// because the coverage cells include "✓" — 3 bytes in UTF-8 but only
// one terminal cell. Byte-based padding made the table jagged.
func padString(s string, w int) string {
	runeCount := len([]rune(s))
	if runeCount >= w {
		return s
	}
	return s + strings.Repeat(" ", w-runeCount)
}

// printOneReport renders a single (entry, group) report immediately.
// Both source paths (one per node) and the target group path are
// printed so the operator can match the row counts to a specific PVC.
func printOneReport(r measure.EntryGroupReport) {
	if r.IndexMode {
		printOneIndexModeReport(r)
		return
	}
	fmt.Printf("== entry stage=%s target=%s group=%s ==\n",
		r.EntryStage, r.EntryTarget, r.Group)
	if len(r.SrcRoots) == 0 {
		fmt.Printf("  src  : (no source dirs resolved for this entry/group)\n")
	} else {
		fmt.Printf("  src  : %d row(s) across %d part(s) in %d dir(s)\n",
			r.SrcRows, r.SrcParts, len(r.SrcRoots))
		for _, p := range r.SrcRoots {
			fmt.Printf("           %s\n", p)
		}
	}
	if len(r.TargetSegs) == 0 {
		fmt.Printf("  tgt  : (no segments under %s)\n", r.TargetGroup)
		return
	}
	var tgtRows uint64
	for _, s := range r.TargetSegs {
		tgtRows += s.Rows
	}
	fmt.Printf("  tgt  : %d row(s) across %d seg(s) under %s\n",
		tgtRows, len(r.TargetSegs), r.TargetGroup)
	for _, s := range r.TargetSegs {
		alignTag := "aligned"
		if !s.Aligned {
			alignTag = "MISALIGNED"
		}
		sidxTag := "no-sidx"
		if s.SidxOpened {
			sidxTag = fmt.Sprintf("sidxDocs=%d", s.SidxDocCount)
		}
		fmt.Printf("    %-22s %s shards=%d parts=%d rows=%d %s\n",
			s.Seg, alignTag, s.Shards, s.Parts, s.Rows, sidxTag)
	}
}

// printOneIndexModeReport renders a single index-mode (entry, group) report:
// document counts, distinct doc-id reconciliation and per-(segment, series)
// value-digest mismatches. Index-mode groups carry data in the segment sidx, so
// they are reconciled at the document level rather than by part rows.
func printOneIndexModeReport(r measure.EntryGroupReport) {
	fmt.Printf("== entry stage=%s target=%s group=%s (index-mode) ==\n",
		r.EntryStage, r.EntryTarget, r.Group)
	if len(r.SrcRoots) == 0 {
		fmt.Printf("  src  : (no source dirs resolved for this entry/group)\n")
	} else {
		fmt.Printf("  src  : %d doc(s), %d distinct doc-id across %d dir(s)\n",
			r.SrcDocs, r.SrcDistinctIDs, len(r.SrcRoots))
		for _, p := range r.SrcRoots {
			fmt.Printf("           %s\n", p)
		}
	}
	fmt.Printf("  tgt  : %d doc(s), %d distinct doc-id under %s\n",
		r.TgtDocs, r.TgtDistinctIDs, r.TargetGroup)
	idTag := "OK"
	if r.TgtDistinctIDs != r.ExpectDistinctIDs {
		idTag = "MISMATCH"
	}
	fmt.Printf("  doc-id reconcile : expected(union of src)=%d tgt=%d  [%s]\n",
		r.ExpectDistinctIDs, r.TgtDistinctIDs, idTag)
	printIndexModeSegAligns(r.SegAligns)
	if len(r.ValueMismatches) == 0 {
		fmt.Printf("  value reconcile  : all (segment, series) digests match  [OK]\n")
		return
	}
	fmt.Printf("  value reconcile  : %d (segment, series) key(s) MISMATCH:\n", len(r.ValueMismatches))
	for _, m := range r.ValueMismatches {
		fmt.Printf("    %-9s seg=%s sid=%d\n", m.Kind, m.Segment, m.SeriesID)
	}
}

// printIndexModeSegAligns renders the per-target-segment alignment breakdown: for
// each segment whether it was a clean 1:1 byte-copy candidate (one source segment,
// no collapse) or merged/split, plus the source-doc -> target-doc collapse. This
// shows the operator why a segment did or did not take the byte-copy fast path.
func printIndexModeSegAligns(segs []measure.IndexModeSegAlign) {
	if len(segs) == 0 {
		return
	}
	aligned := 0
	for _, s := range segs {
		if s.Aligned {
			aligned++
		}
	}
	fmt.Printf("  segment alignment: %d/%d target seg(s) are 1:1 aligned (byte-copy candidates); the rest were merged/split:\n",
		aligned, len(segs))
	for _, s := range segs {
		tag := "merged/split"
		if s.Aligned {
			tag = "aligned 1:1"
		}
		fmt.Printf("    %-16s %-13s src-segs=%d src-docs=%d -> tgt-docs=%d (collapsed %d)\n",
			s.Segment, tag, s.SrcSegs, s.SrcDocs, s.TgtDocs, s.SrcDocs-s.TgtDocs)
	}
}

// absorbStream records one stream (entry, group) report into the tally.
func (t *verifyTally) absorbStream(r stream.EntryGroupReport) {
	t.hasStream = true
	segs := make([]tallySeg, len(r.TargetSegs))
	for i, s := range r.TargetSegs {
		segs[i] = tallySeg{rows: s.Rows, aligned: s.Aligned}
	}
	t.absorbCommon(r.EntryStage, r.EntryNodes, r.Group, r.SrcRows, segs)
}

// printOneStreamReport renders a single (entry, group) stream report.
func printOneStreamReport(r stream.EntryGroupReport) {
	fmt.Printf("== entry stage=%s target=%s group=%s (stream) ==\n",
		r.EntryStage, r.EntryTarget, r.Group)
	if len(r.SrcRoots) == 0 {
		fmt.Printf("  src  : (no source dirs resolved for this entry/group)\n")
	} else {
		fmt.Printf("  src  : %d row(s) across %d part(s) in %d dir(s)\n",
			r.SrcRows, r.SrcParts, len(r.SrcRoots))
		for _, p := range r.SrcRoots {
			fmt.Printf("           %s\n", p)
		}
	}
	if len(r.TargetSegs) == 0 {
		fmt.Printf("  tgt  : (no segments under %s)\n", r.TargetGroup)
		return
	}
	var tgtRows uint64
	for _, s := range r.TargetSegs {
		tgtRows += s.Rows
	}
	fmt.Printf("  tgt  : %d row(s) across %d seg(s) under %s\n",
		tgtRows, len(r.TargetSegs), r.TargetGroup)
	for _, s := range r.TargetSegs {
		alignTag := "aligned"
		if !s.Aligned {
			alignTag = "MISALIGNED"
		}
		sidxTag := "no-sidx"
		if s.SidxOpened {
			sidxTag = fmt.Sprintf("sidxDocs=%d", s.SidxDocCount)
		}
		fmt.Printf("    %-22s %s shards=%d parts=%d rows=%d %s\n",
			s.Seg, alignTag, len(s.Shards), s.Parts, s.Rows, sidxTag)
		for _, sh := range s.Shards {
			idxTag := "no-idx"
			if sh.IdxOpened {
				idxTag = fmt.Sprintf("idxDocs=%d", sh.IdxDocCount)
			}
			fmt.Printf("      %-20s rows=%d parts=%d %s\n",
				sh.Shard, sh.Rows, sh.Parts, idxTag)
		}
	}
}
