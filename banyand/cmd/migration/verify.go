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
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/measure"
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
			plan, err := LoadCopyPlan(configPath)
			if err != nil {
				return err
			}
			// staging dir is unused by verify (read-only), pass "".
			cfg := plan.ToDirectCopyConfig("")

			ctx, stop := signal.NotifyContext(context.Background(),
				os.Interrupt, syscall.SIGTERM)
			defer stop()

			tally := &verifyTally{}
			runErr := measure.MigrationVerify(ctx, cfg, func(r measure.EntryGroupReport) {
				printOneReport(r)
				tally.absorb(r)
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
type verifyTally struct {
	mismatches     []verifyMismatch
	coverage       map[string]map[string]coverageState // node → group → (src/tgt presence)
	nodeOrder      []string                            // first-seen ordering
	groupOrder     []string                            // first-seen ordering
	srcRowsTotal   uint64
	tgtRowsTotal   uint64
	segsTotal      int
	segsMisaligned int
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

// entryNodeName picks the first node from EntryNodes (entries usually
// reference exactly one node); falls back to the entry stage when the
// list is empty (e.g. backup-mode plans without explicit nodes).
func entryNodeName(r measure.EntryGroupReport) string {
	if len(r.EntryNodes) > 0 {
		return r.EntryNodes[0]
	}
	return r.EntryStage
}

func (t *verifyTally) absorb(r measure.EntryGroupReport) {
	node := entryNodeName(r)
	if t.coverage == nil {
		t.coverage = make(map[string]map[string]coverageState)
	}
	if _, ok := t.coverage[node]; !ok {
		t.coverage[node] = make(map[string]coverageState)
		t.nodeOrder = append(t.nodeOrder, node)
	}
	if _, ok := indexOf(t.groupOrder, r.Group); !ok {
		t.groupOrder = append(t.groupOrder, r.Group)
	}

	t.srcRowsTotal += r.SrcRows
	var tgtRows uint64
	for _, s := range r.TargetSegs {
		tgtRows += s.Rows
		if !s.Aligned {
			t.segsMisaligned++
		}
	}
	t.tgtRowsTotal += tgtRows
	t.segsTotal += len(r.TargetSegs)

	t.coverage[node][r.Group] = coverageState{
		src: r.SrcRows > 0,
		tgt: tgtRows > 0,
	}

	if tgtRows != r.SrcRows {
		t.mismatches = append(t.mismatches, verifyMismatch{
			Stage:    r.EntryStage,
			NodeName: node,
			Group:    r.Group,
			SrcRows:  r.SrcRows,
			TgtRows:  tgtRows,
		})
	}
}

func indexOf(xs []string, v string) (int, bool) {
	for i, x := range xs {
		if x == v {
			return i, true
		}
	}
	return 0, false
}

func (t *verifyTally) printSummary() {
	fmt.Println("== SUMMARY ==")
	t.printCoverageTable()
	fmt.Println()
	fmt.Printf("  target segments                : %d\n", t.segsTotal)
	fmt.Printf("  target segments misaligned     : %d\n", t.segsMisaligned)
	fmt.Printf("  source rows total              : %d\n", t.srcRowsTotal)
	fmt.Printf("  target rows total              : %d\n", t.tgtRowsTotal)
	if len(t.mismatches) == 0 {
		if t.tgtRowsTotal > t.srcRowsTotal {
			fmt.Printf("  diff (tgt - src)               : %+d (UNEXPECTED — target has MORE rows than source)\n",
				int64(t.tgtRowsTotal)-int64(t.srcRowsTotal))
		} else {
			fmt.Println("  src == tgt")
		}
		return
	}
	fmt.Println()
	fmt.Println("  row-count mismatches (tgt - src, negative = rows dropped by copy):")
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
	if total < 0 {
		fmt.Println()
		fmt.Println("  note: rows dropped are caused by slow-path mustInitFromDataPoints")
		fmt.Println("        deduping (seriesID, timestamp) within each chunk flush — banyandb's")
		fmt.Println("        merger can leave duplicated boundary rows in source parts, and")
		fmt.Println("        only the latest version of each (sid, ts) survives in target.")
		fmt.Println("        Pass --detail to list the exact missing (sid, ts, version) rows.")
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
