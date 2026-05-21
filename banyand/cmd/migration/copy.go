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
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
)

func newCopyCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "copy",
		Short: "Direct-file copy of measure parts into local measure root with row-level grid alignment",
		Long: `copy walks every source part, reads each row's actual timestamp, and routes
the row to the grid-aligned target segment via SegmentInterval.Standard.
A single source part can fan out to multiple target segments (one new
part each).

The run is fully driven by a YAML config (--copy-config). One config
carries one source (backup snapshot or live PVC mounts), one group list,
and one or more (stage, target) entries — schemas and per-group union
sidx are loaded/built once and reused across every entry.

Per group, a single union sidx is built in the staging directory by
deduplicating every source seg-*/sidx doc by SeriesID; that union sidx
is then broadcast (byte-copied) into every aligned target segment that
received any rows. A fresh .snp is written per (target segment, shard)
so tsTable.loadSnapshot picks up the new parts on next startup.

Schemas (measure tag families + IndexMode bit) and group resource
opts (SegmentInterval per LifecycleStage) are read directly from the
source's schema-property bluge catalog — no liaison access is needed.
Groups containing any IndexMode measure are rejected up front, since
their fields live inside sidx and the broadcast strategy would corrupt
cross-node deduplication at the liaison.

The staging directory is removed in full when the run finishes
(success or failure). Run inside a data pod whose BanyanDB process is
NOT writing to any of the entry targets. Source and target shard
topology must match.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			applyGoMemLimit()

			plan, err := LoadCopyPlan(configPath)
			if err != nil {
				return err
			}
			stagingDir, err := PrepareStagingDir(plan.StagingDir)
			if err != nil {
				return err
			}
			defer func() {
				if rmErr := os.RemoveAll(stagingDir); rmErr != nil {
					fmt.Fprintf(os.Stderr,
						"warning: failed to clean staging dir %s: %v\n",
						stagingDir, rmErr)
				}
			}()

			cfg := plan.ToDirectCopyConfig(stagingDir)

			ctx, stop := signal.NotifyContext(context.Background(),
				os.Interrupt, syscall.SIGTERM)
			defer stop()

			res, runErr := measure.MigrationCopy(ctx, cfg)
			printCopyResult(res)
			return runErr
		},
	}

	cmd.Flags().StringVar(&configPath, "copy-config", "",
		"path to the YAML migration copy plan (required)")
	_ = cmd.MarkFlagRequired("copy-config")
	return cmd
}

func printCopyResult(res measure.DirectCopyResult) {
	fmt.Println("DONE in", res.Duration)
	fmt.Printf("   target segments   : %d\n", res.Segments)
	fmt.Printf("   source parts      : %d\n", res.SourceParts)
	fmt.Printf("   target mem-parts  : %d (pre-merge; banyandb's merge loop will compact)\n", res.TargetParts)
	fmt.Printf("   rows copied       : %d\n", res.Rows)
	fmt.Printf("   bytes written     : %d\n", res.Bytes)
	fmt.Printf("   fast-path parts   : %d\n", measure.FastPathHits())
	fmt.Printf("   slow-path parts   : %d\n", measure.SlowPathHits())
	fmt.Printf("   slow-path rows    : %d\n", measure.SlowPathRows())
}

// applyGoMemLimit auto-sets the Go runtime's heap soft-cap (GOMEMLIMIT)
// from the process cgroup memory limit when the operator did not pass
// one explicitly.
//
// Why this is necessary:
//   - Go's runtime sizes the heap from live set + GOGC by default; it
//     does NOT read cgroup limits on its own. On a memory-constrained
//     pod a momentary heap spike during slow-path zstd flush can sail
//     right past the pod limit and trip the kernel OOMKiller (or
//     macOS jetsam on kind), wiping the whole migration mid-run.
//   - Historical evidence from this codebase: an unconstrained run on
//     a 16 GiB GKE pod OOM'd at 99 % through the warm tier; raising
//     the limit to 20 GiB AND setting GOMEMLIMIT to 17 GiB (85 %) was
//     the fix. The shared flush-pool + Phase A union-sidx refactor
//     reduced peak memory, but no run has been validated WITHOUT
//     GOMEMLIMIT post-refactor, so we still ship the safety net.
//
// Why we don't rely on operators setting GOMEMLIMIT manually:
//   - Go 1.19+ honors a GOMEMLIMIT env at startup, so an operator who
//     remembered to set it gets the same effect. This helper is a UX
//     fallback for the common case where it was forgotten — we read
//     the cgroup limit (works on kind, GKE, plain docker run) and
//     apply 85 % of it. The 15 % headroom is for the runtime's own
//     overhead + the Linux page cache.
//
// Resolution order: GOMEMLIMIT env wins (we just log and return);
// otherwise read cgroup limit and call debug.SetMemoryLimit; otherwise
// log "not set" and let the process run uncapped (bare-host case where
// the operator presumably has enough RAM).
func applyGoMemLimit() {
	if raw := os.Getenv("GOMEMLIMIT"); raw != "" {
		// Honor whatever the env already set — Go's runtime parsed it
		// at startup. Just log.
		fmt.Fprintf(os.Stdout, "[migration] GOMEMLIMIT honored from env: %s\n", raw)
		return
	}
	limit, err := cgroups.MemoryLimit()
	if err != nil || limit <= 0 {
		// Fall back to 85% of host total memory. cgroups.MemoryLimit
		// returns -1 for "max" (unlimited) on a non-containerised host.
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		if ms.Sys == 0 {
			fmt.Fprintf(os.Stdout, "[migration] GOMEMLIMIT not set (no cgroup limit, runtime.MemStats unavailable)\n")
			return
		}
		// MemStats.Sys reports OS memory reserved for the runtime, not
		// host RAM. As a heuristic we don't set a limit here.
		fmt.Fprintf(os.Stdout, "[migration] GOMEMLIMIT not set (cgroup limit unavailable; running on bare host)\n")
		return
	}
	memLimit := int64(float64(limit) * 0.85)
	debug.SetMemoryLimit(memLimit)
	fmt.Fprintf(os.Stdout, "[migration] GOMEMLIMIT set to %s (85%% of cgroup limit %s) at %s\n",
		humanBytes(memLimit), humanBytes(limit), time.Now().Format("15:04:05"))
}

func humanBytes(n int64) string {
	if n < 0 {
		return strconv.FormatInt(n, 10)
	}
	const (
		KiB = 1 << 10
		MiB = 1 << 20
		GiB = 1 << 30
	)
	switch {
	case n >= GiB:
		return fmt.Sprintf("%.1fGiB", float64(n)/GiB)
	case n >= MiB:
		return fmt.Sprintf("%.1fMiB", float64(n)/MiB)
	case n >= KiB:
		return fmt.Sprintf("%.1fKiB", float64(n)/KiB)
	}
	return fmt.Sprintf("%dB", n)
}
