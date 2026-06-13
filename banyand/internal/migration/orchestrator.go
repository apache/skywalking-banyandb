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

package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

// Result aggregates a whole run across every catalog the plan touched.
type Result struct {
	PerCatalog map[commonv1.Catalog]EntryGroupResult
	Duration   time.Duration
	Totals     EntryGroupResult
}

func (r *EntryGroupResult) add(other EntryGroupResult) {
	r.Rows += other.Rows
	r.Bytes += other.Bytes
	r.SourceParts += other.SourceParts
	r.TargetParts += other.TargetParts
	r.Segments += other.Segments
}

// resolveStageInterval picks the SegmentInterval that should drive the
// target grid for one (group, stage) pair. Mirroring lifecycle's parseGroup:
// a stage listed under opts.GetStages() uses that stage's SegmentInterval;
// any other name addresses the implicit initial (hot) tier — which is never
// listed as a named stage — and uses the group's default SegmentInterval.
func resolveStageInterval(opts *commonv1.ResourceOpts, stage string) (storage.IntervalRule, error) {
	var zero storage.IntervalRule
	for _, st := range opts.GetStages() {
		if st.GetName() == stage {
			if st.GetSegmentInterval() == nil {
				return zero, fmt.Errorf("stage %q has no segmentInterval", stage)
			}
			return intervalRuleFromProto(st.GetSegmentInterval())
		}
	}
	if opts.GetSegmentInterval() == nil {
		return zero, fmt.Errorf("stage %q maps to the group's default SegmentInterval but the group has none", stage)
	}
	return intervalRuleFromProto(opts.GetSegmentInterval())
}

func intervalRuleFromProto(ir *commonv1.IntervalRule) (storage.IntervalRule, error) {
	var zero storage.IntervalRule
	if ir.GetNum() <= 0 {
		return zero, fmt.Errorf("segmentInterval.num must be > 0, got %d", ir.GetNum())
	}
	switch ir.GetUnit() {
	case commonv1.IntervalRule_UNIT_DAY:
		return storage.IntervalRule{Unit: storage.DAY, Num: int(ir.GetNum())}, nil
	case commonv1.IntervalRule_UNIT_HOUR:
		return storage.IntervalRule{Unit: storage.HOUR, Num: int(ir.GetNum())}, nil
	}
	return zero, fmt.Errorf("unsupported segmentInterval.unit %v", ir.GetUnit())
}

// resolveGroupInterval resolves the target grid for one (group, stage) from
// the classified schema context.
func (c *Classified) resolveGroupInterval(group, stage string) (storage.IntervalRule, error) {
	g := c.Groups[group]
	if g.GetResourceOpts() == nil {
		return storage.IntervalRule{}, fmt.Errorf("group %q has no ResourceOpts in the schema-property catalog", group)
	}
	return resolveStageInterval(g.GetResourceOpts(), stage)
}

// dirtyTargets returns every (entry, group) target dir this run would
// write into that already holds files. Pairs whose sources resolve to no
// dirs are skipped — Phase B skips them too. The caller aggregates across
// catalogs so the operator sees the FULL list up front instead of one
// error at a time mid-run.
func (p *CopyPlan) dirtyTargets(catalog commonv1.Catalog, entries []ResolvedEntry, groups []string) ([]string, error) {
	var dirty []string
	for _, entry := range entries {
		for _, group := range groups {
			if len(p.ResolveEntrySrcRoots(catalog, entry, group)) == 0 {
				continue
			}
			dir := filepath.Join(entry.Target, group)
			ents, err := os.ReadDir(dir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("inspect target %s: %w", dir, err)
			}
			if len(ents) > 0 {
				dirty = append(dirty, dir)
			}
		}
	}
	return dirty, nil
}

// RunCopy drives one copy run: for every executor whose catalog bucket is
// non-empty it loads the catalog's schema context, pre-flights every
// (entry, group) stage interval, builds the per-group union sidx once
// (Phase A), then walks entry × group rewriting source parts onto the
// target grid (Phase B). Executors run sequentially in slice order; the
// first error aborts the run.
func (p *CopyPlan) RunCopy(ctx context.Context, stagingDir string, cls *Classified, executors []CatalogExecutor) (res Result, err error) {
	res = Result{PerCatalog: map[commonv1.Catalog]EntryGroupResult{}}
	if stagingDir == "" {
		return res, errors.New("stagingDir is required (caller owns its lifecycle)")
	}
	if cls.SchemaRoot == "" {
		return res, errors.New("copy requires a schema-property catalog in the source " +
			"(segment alignment and index rebuilds depend on it)")
	}
	start := time.Now()
	defer func() { res.Duration = time.Since(start) }()
	entries := p.ResolvedEntries()

	// Pre-flight across EVERY executor before any expensive work starts:
	// each (entry, group) must resolve to a SegmentInterval, and no target
	// group dir this run would write into may already hold files. Failing
	// here saves an expensive union-sidx build (or a fully-copied first
	// catalog) only to abort mid-run on a broken schema or a dirty target.
	var dirty []string
	for _, exec := range executors {
		groups := cls.Buckets[exec.Catalog()]
		for entryIdx, entry := range entries {
			for _, group := range groups {
				if _, irErr := cls.resolveGroupInterval(group, entry.Stage); irErr != nil {
					return res, fmt.Errorf("entry %d (target=%s) group %s: %w", entryIdx, entry.Target, group, irErr)
				}
			}
		}
		execDirty, dtErr := p.dirtyTargets(exec.Catalog(), entries, groups)
		if dtErr != nil {
			return res, dtErr
		}
		dirty = append(dirty, execDirty...)
	}
	if len(dirty) > 0 {
		return res, fmt.Errorf("%d target group dir(s) are not empty; remove them before re-running:\n  %s",
			len(dirty), strings.Join(dirty, "\n  "))
	}

	for _, exec := range executors {
		groups := cls.Buckets[exec.Catalog()]
		if len(groups) == 0 {
			continue
		}
		prefix := exec.LogPrefix()

		if err := exec.Prepare(ctx, cls.SchemaRoot, groups); err != nil {
			return res, err
		}

		// Phase A: build one union sidx per group, sourced from EVERY entry's
		// roots merged (dedup by path). Each (entry, group) in Phase B reuses
		// the same staged sidx so two invariants hold:
		//   - Cross-replica broadcast invariant: the same series-ID set ends
		//     up under every target seg's sidx subdir, which is what liaison
		//     relies on to dedup distributed query fan-outs.
		//   - We don't rescan the same source bluge index N times per group.
		groupUnionSidx := make(map[string]string, len(groups))
		for _, group := range groups {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			srcRoots := p.CollectAllSrcGroupRoots(exec.Catalog(), group)
			if len(srcRoots) == 0 {
				logStep(prefix, "group %s: no source dirs across any entry — skipping union sidx build", group)
				continue
			}
			logStep(prefix, "group %s: building union sidx from %d source dir(s) merged across all entries",
				group, len(srcRoots))
			unionSidxPath, buildErr := BuildGroupUnionSidx(ctx, srcRoots,
				filepath.Join(stagingDir, "groups", group, sidxDirName),
				func(format string, args ...any) {
					logStep(prefix, "group "+group+": "+format, args...)
				})
			if buildErr != nil {
				return res, fmt.Errorf("group %s: build union sidx: %w", group, buildErr)
			}
			groupUnionSidx[group] = unionSidxPath
		}

		// Phase B: per entry × group, align source rows by stage's
		// SegmentInterval and write parts under the entry's target group
		// root, broadcasting the pre-built union sidx (from Phase A) into
		// every aligned segment.
		catalogTotal := res.PerCatalog[exec.Catalog()]
		for entryIdx, entry := range entries {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			entryTag := fmt.Sprintf("entry [%d/%d]", entryIdx+1, len(entries))
			logStep(prefix, "%s stage=%s nodes=%v target=%s",
				entryTag, entry.Stage, entry.Nodes, entry.Target)
			for _, group := range groups {
				if ctx.Err() != nil {
					return res, ctx.Err()
				}
				srcRoots := p.ResolveEntrySrcRoots(exec.Catalog(), entry, group)
				if len(srcRoots) == 0 {
					if len(entry.Source) > 0 {
						logStep(prefix, "%s stage=%s group %s: skipped (none of source paths %v contain this group)",
							entryTag, entry.Stage, group, entry.Source)
					} else {
						logStep(prefix, "%s stage=%s group %s: skipped (none of nodes %v carry this group)",
							entryTag, entry.Stage, group, entry.Nodes)
					}
					continue
				}
				ir, err := cls.resolveGroupInterval(group, entry.Stage)
				if err != nil {
					return res, fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
				}
				in := EntryGroupInput{
					EntryTag:        entryTag,
					Group:           group,
					Entry:           entry,
					SrcRoots:        srcRoots,
					TargetGroupRoot: filepath.Join(entry.Target, group),
					Interval:        ir,
					UnionSidxPath:   groupUnionSidx[group],
				}
				groupRes, err := exec.CopyEntryGroup(ctx, in)
				if err != nil {
					return res, fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
				}
				logStep(prefix, "%s stage=%s group %s: done rows=%d segs=%d srcParts=%d targetParts=%d",
					entryTag, entry.Stage, group,
					groupRes.Rows, groupRes.Segments, groupRes.SourceParts, groupRes.TargetParts)
				// Return idle heap to the OS so compressed-memory pressure
				// doesn't accumulate across (entry, group).
				debug.FreeOSMemory()
				catalogTotal.add(groupRes)
				res.Totals.add(groupRes)
			}
		}
		res.PerCatalog[exec.Catalog()] = catalogTotal
	}
	return res, nil
}

// RunVerify drives one read-only verify run: same entry × group walk as
// RunCopy, but no staging, no union sidx, and no target mutation.
func (p *CopyPlan) RunVerify(ctx context.Context, cls *Classified, executors []CatalogExecutor, onReport func(report any)) error {
	// Unlike RunCopy, verify deliberately does NOT skip (entry, group) pairs
	// with zero source roots: enumerating the target there surfaces orphan
	// data left behind by a prior partial run.
	entries := p.ResolvedEntries()
	for _, exec := range executors {
		groups := cls.Buckets[exec.Catalog()]
		if len(groups) == 0 {
			continue
		}
		if err := exec.Prepare(ctx, cls.SchemaRoot, groups); err != nil {
			return err
		}
		for entryIdx, entry := range entries {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			entryTag := fmt.Sprintf("entry [%d/%d]", entryIdx+1, len(entries))
			for _, group := range groups {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				ir, err := cls.resolveGroupInterval(group, entry.Stage)
				if err != nil {
					return fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
				}
				in := EntryGroupInput{
					EntryTag:        entryTag,
					Group:           group,
					Entry:           entry,
					SrcRoots:        p.ResolveEntrySrcRoots(exec.Catalog(), entry, group),
					TargetGroupRoot: filepath.Join(entry.Target, group),
					Interval:        ir,
				}
				if err := exec.VerifyEntryGroup(ctx, in, onReport); err != nil {
					return fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
				}
			}
		}
	}
	return nil
}

func logStep(prefix, format string, args ...any) {
	fmt.Fprintf(os.Stdout, time.Now().Format("2006/01/02 15:04:05")+" "+prefix+" "+format+"\n", args...)
}
