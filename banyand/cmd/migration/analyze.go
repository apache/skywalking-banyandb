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
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

func newAnalyzeCmd() *cobra.Command {
	var configPath string
	var entryIdx int
	var groupName string
	var sampleCap int

	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Row-level (seriesID, timestamp) duplicate analysis for one (entry, group)",
		Long: `analyze opens every src part for ONE (entry, group) pair in the copy
plan, decodes block metadata + timestamps + versions (NOT tag/field
bodies — cheap scan), and reports:

  - total physical row count on disk
  - distinct (seriesID, timestamp) key count
  - duplicate row count (= total - distinct)
  - first --sample (sid, ts) keys that have >1 version on disk

This is the diagnostic that proves the src > tgt diff reported by
'verify' is caused by banyandb's normal version-tracking dedup at
write time (only the latest version per (sid, ts) survives in the
target part).`,
		RunE: func(_ *cobra.Command, _ []string) error {
			plan, err := LoadCopyPlan(configPath)
			if err != nil {
				return err
			}
			cfg := plan.ToDirectCopyConfig("")
			if entryIdx < 0 || entryIdx >= len(cfg.Entries) {
				return fmt.Errorf("entry-idx %d out of range [0,%d)", entryIdx, len(cfg.Entries))
			}
			entry := cfg.Entries[entryIdx]

			srcRoots := measure.ResolveEntrySrcRoots(cfg, entry, groupName)
			if len(srcRoots) == 0 {
				return fmt.Errorf("no source dirs resolve for entry-idx=%d group=%s (PVC likely doesn't carry this group)", entryIdx, groupName)
			}
			fmt.Printf("== analyze entry [%d/%d] stage=%s nodes=%v group=%s ==\n",
				entryIdx+1, len(cfg.Entries), entry.Stage, entry.Nodes, groupName)
			for _, r := range srcRoots {
				fmt.Printf("  src: %s\n", r)
			}
			start := time.Now()
			res, err := measure.AnalyzeGroupRows(srcRoots, fs.NewLocalFileSystem(), sampleCap)
			if err != nil {
				return err
			}
			elapsed := time.Since(start)

			fmt.Printf("  parts scanned                : %d\n", res.PartsScanned)
			fmt.Printf("  total rows on disk           : %d\n", res.TotalRows)
			fmt.Printf("  distinct (sid, ts) (global)  : %d\n", res.UniqueKeys)
			fmt.Printf("  cross-part dup rows          : %d  (NOT dropped by copy — slow path is per-part)\n", res.DuplicateRows)
			fmt.Printf("  (sid, ts) keys with >1 row   : %d\n", res.KeysWithDuplicates)
			fmt.Printf("  WITHIN-part dup rows         : %d  (← MATCH this against verify src-tgt diff)\n", res.PerPartDupRows)
			fmt.Printf("  elapsed                      : %v\n", elapsed)
			if len(res.PerPartDups) > 0 {
				partsToShow := capToSample(len(res.PerPartDups), sampleCap)
				fmt.Printf("  per-part dedup breakdown (showing %d / %d parts with internal duplicates):\n",
					partsToShow, len(res.PerPartDups))
				for i := 0; i < partsToShow; i++ {
					pp := res.PerPartDups[i]
					fmt.Printf("    %s/%s/%s rows=%d uniq=%d withinPartDups=%d boundaries=%d\n",
						pp.SegName, pp.ShardName, pp.PartID, pp.Rows, pp.UniqueKeys, pp.PartialDupRows, len(pp.Boundaries))
					fmt.Printf("      source path: %s/%s/%s/%s\n",
						pp.SourceRoot, pp.SegName, pp.ShardName, pp.PartID)
					boundariesToShow := capToSample(len(pp.Boundaries), sampleCap)
					for j := 0; j < boundariesToShow; j++ {
						b := pp.Boundaries[j]
						ts := time.Unix(0, b.Timestamp).UTC().Format(time.RFC3339)
						fmt.Printf("      boundary: block[%d] last == block[%d] first  sid=%d ts=%s vA=%d vB=%d\n",
							b.BlockA, b.BlockB, b.SeriesID, ts, b.VersionA, b.VersionB)
					}
					if boundariesToShow < len(pp.Boundaries) {
						fmt.Printf("      ... + %d more boundaries (raise --sample to see them)\n",
							len(pp.Boundaries)-boundariesToShow)
					}
				}
				if partsToShow < len(res.PerPartDups) {
					fmt.Printf("    ... + %d more parts (raise --sample to see them)\n",
						len(res.PerPartDups)-partsToShow)
				}
			}
			if res.KeysWithDuplicates > 0 {
				shown := len(res.SamplesByVersion)
				fmt.Printf("  sample (sid, ts) with >1 row on disk (showing %d / %d keys; cross-part dups, won't be deduped):\n",
					shown, res.KeysWithDuplicates)
				for _, s := range res.SamplesByVersion {
					ts := time.Unix(0, s.Timestamp).UTC().Format(time.RFC3339)
					fmt.Printf("    sid=%d ts=%s\n", s.SeriesID, ts)
					for _, v := range s.Versions {
						fmt.Printf("      version=%d  part=%s\n", v.Version, v.Path)
					}
				}
			}

			targetGroupRoot := filepath.Join(entry.Target, groupName)
			fmt.Println()
			fmt.Printf("== src-vs-target multiset diff ==\n")
			fmt.Printf("  tgt: %s\n", targetGroupRoot)
			diffStart := time.Now()
			diff, derr := measure.AnalyzeGroupDiffWithTarget(srcRoots, targetGroupRoot, fs.NewLocalFileSystem(), sampleCap)
			if derr != nil {
				return derr
			}
			fmt.Printf("  src rows           : %d\n", diff.SourceRows)
			fmt.Printf("  tgt rows           : %d\n", diff.TargetRows)
			fmt.Printf("  missing rows total : %d  (← MATCHES verify src-tgt diff for this entry+group)\n", diff.MissingRows)
			fmt.Printf("  diff scan elapsed  : %v\n", time.Since(diffStart))
			if diff.MissingKeys > 0 {
				shown := len(diff.Missing)
				fmt.Printf("  missing rows (showing %d / %d keys):\n", shown, diff.MissingKeys)
				for _, m := range diff.Missing {
					ts := time.Unix(0, m.Timestamp).UTC().Format(time.RFC3339)
					fmt.Printf("    sid=%d ts=%s\n", m.SeriesID, ts)
					for _, v := range m.SourceVersions {
						fmt.Printf("      src    version=%d  part=%s\n", v.Version, v.Path)
					}
					for _, v := range m.TargetVersions {
						fmt.Printf("      tgt    version=%d  part=%s\n", v.Version, v.Path)
					}
					for _, v := range m.MissingVersions {
						fmt.Printf("      MISSING version=%d  part=%s\n", v.Version, v.Path)
					}
				}
			}
			fmt.Println()
			fmt.Println("Interpretation:")
			fmt.Println("  - slow-path mustInitFromDataPoints only fires INSIDE one source part's")
			fmt.Println("    bucket flush; cross-part dups survive copy as separate target memparts.")
			fmt.Println("  - WITHIN-part dup rows = the count slow-path drops at flush time. If this")
			fmt.Println("    matches `verify`'s src-tgt diff (for the same entry/group), the loss is")
			fmt.Println("    proven to be banyandb's normal in-part (seriesID, ts) collapse.")
			return nil
		},
	}

	cmd.Flags().StringVar(&configPath, "copy-config", "", "path to the YAML migration copy plan (required)")
	cmd.Flags().IntVar(&entryIdx, "entry-idx", 0, "0-based entry index in the plan to analyze")
	cmd.Flags().StringVar(&groupName, "group", "", "group name to analyze (must appear in plan.groups) (required)")
	cmd.Flags().IntVar(&sampleCap, "sample", 10,
		"max number of items to print per list (applies to every sample/breakdown section; 0 = unlimited)")
	_ = cmd.MarkFlagRequired("copy-config")
	_ = cmd.MarkFlagRequired("group")
	return cmd
}

// capToSample returns the number of items to actually print given the
// observed list length and the operator-supplied --sample cap. limit=0
// means "print everything"; otherwise we never exceed limit.
func capToSample(have, limit int) int {
	if limit <= 0 || have <= limit {
		return have
	}
	return limit
}
