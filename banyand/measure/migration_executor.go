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

package measure

import (
	"context"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// MigrationExecutor is the measure implementation of
// migration.CatalogExecutor: it copies / verifies measure groups one
// (entry, group) at a time under the shared orchestrator.
type MigrationExecutor struct {
	schemas         map[string]map[string]*measureSchemaInfo
	indexModeGroups map[string]bool
	ruleByID        map[uint32]indexRuleInfo
}

// NewMigrationExecutor returns the measure catalog executor.
func NewMigrationExecutor() *MigrationExecutor {
	return &MigrationExecutor{}
}

// Catalog reports CATALOG_MEASURE.
func (e *MigrationExecutor) Catalog() commonv1.Catalog { return commonv1.Catalog_CATALOG_MEASURE }

// LogPrefix tags this executor's progress lines.
func (e *MigrationExecutor) LogPrefix() string { return measureMigrationLogPrefix }

// Prepare loads the measure schemas of the given groups from the
// schema-property catalog and classifies each group: an index-mode group
// (its data lives in the segment sidx, routed per timestamp) opts out of the
// union-sidx broadcast, while a group mixing index-mode measures with real
// normal measures is unsupported and rejected up front.
func (e *MigrationExecutor) Prepare(_ context.Context, schemaRoot string, groups []string) error {
	logStep("loading measure schemas")
	//nolint:contextcheck // bluge reader.Search inside reader.WalkShard already uses its own context.
	schemas, err := loadMeasureSchemas(schemaRoot, groups) //nolint:contextcheck // offline bluge schema read, no cancellation
	if err != nil {
		return fmt.Errorf("load measure schemas: %w", err)
	}
	// Refuse to proceed when a requested group resolved to zero
	// measures: the slow path would otherwise silently write target
	// parts with an empty tag projection (no tag columns), which is
	// indistinguishable from "no tags" at read time and corrupts the
	// query result. Typical cause: typo in --groups or stale backup.
	for _, g := range groups {
		if len(schemas[g]) == 0 {
			return fmt.Errorf("group %q has no measures in backup schema-property catalog — typo in groups or empty group?", g)
		}
	}
	indexModeGroups := make(map[string]bool, len(groups))
	for _, g := range groups {
		isIndexMode, classifyErr := classifyGroup(g, schemas[g])
		if classifyErr != nil {
			return classifyErr
		}
		indexModeGroups[g] = isIndexMode
	}
	ruleByID, err := loadIndexRuleInfoByID(schemaRoot, groups) //nolint:contextcheck // offline bluge schema read, no cancellation
	if err != nil {
		return fmt.Errorf("load index rules: %w", err)
	}
	e.schemas = schemas
	e.indexModeGroups = indexModeGroups
	e.ruleByID = ruleByID
	// Reset path-hit counters so a previous in-process run's totals don't
	// bleed into this one.
	fastPathHits.Store(0)
	slowPathHits.Store(0)
	slowPathRows.Store(0)
	return nil
}

// SkipUnionSidx reports whether the orchestrator should skip the Phase A
// union-sidx build + broadcast for the given group. Index-mode measure groups
// opt out: their sidx docs are data points routed per timestamp, not
// series-index metadata to broadcast.
func (e *MigrationExecutor) SkipUnionSidx(group string) bool { return e.indexModeGroups[group] }

// CopyEntryGroup migrates one (entry, group): discovers source parts and
// rewrites them onto the target grid.
func (e *MigrationExecutor) CopyEntryGroup(ctx context.Context, in migration.EntryGroupInput) (migration.EntryGroupResult, error) {
	if e.indexModeGroups[in.Group] {
		return copyIndexModeGroup(ctx, in, e.ruleByID, e.schemas[in.Group])
	}
	var zero migration.EntryGroupResult
	tasks, err := discoverPartTasks(ctx, in.SrcRoots)
	if err != nil {
		return zero, fmt.Errorf("discover parts: %w", err)
	}
	if len(tasks) == 0 {
		logStep("%s stage=%s group %s: skipped (no parts in selected nodes)",
			in.EntryTag, in.Entry.Stage, in.Group)
		return zero, nil
	}
	logStep("%s stage=%s group %s: %d source dir(s), %d parts, interval=%v×%d — writing target (target=%q, union sidx=%q)",
		in.EntryTag, in.Entry.Stage, in.Group, len(in.SrcRoots), len(tasks), in.Interval.Unit, in.Interval.Num,
		in.TargetGroupRoot, in.UnionSidxPath)
	return directCopyGroup(ctx,
		in.EntryTag,
		in.Group, in.Entry.Stage,
		in.TargetGroupRoot,
		in.Interval, e.schemas[in.Group],
		tasks, in.UnionSidxPath)
}

// VerifyEntryGroup re-reads one (entry, group) read-only: it sums source
// rows, enumerates target segments and emits an EntryGroupReport.
func (e *MigrationExecutor) VerifyEntryGroup(ctx context.Context, in migration.EntryGroupInput, onReport func(report any)) error {
	if e.indexModeGroups[in.Group] {
		report, err := verifyIndexModeGroup(ctx, in, e.ruleByID, e.schemas[in.Group])
		if err != nil {
			return fmt.Errorf("index-mode verify: %w", err)
		}
		onReport(report)
		return nil
	}
	fileSystem := fs.NewLocalFileSystem()
	ir := in.Interval
	targetGroup := in.TargetGroupRoot
	if len(in.SrcRoots) == 0 {
		// Confirm whether the target carries any parts so the
		// operator can still see them (rare but possible after
		// a partial copy). Empty src + empty tgt is silenced.
		segs, segErr := EnumerateGroupTarget(targetGroup, ir, fileSystem)
		if segErr != nil {
			return fmt.Errorf("target: %w", segErr)
		}
		if len(segs) == 0 {
			return nil
		}
		onReport(EntryGroupReport{
			Group:       in.Group,
			EntryStage:  in.Entry.Stage,
			EntryTarget: in.Entry.Target,
			EntryNodes:  in.Entry.Nodes,
			SrcRoots:    nil,
			TargetGroup: targetGroup,
			TargetSegs:  segs,
		})
		return nil
	}

	srcRows, srcParts, srcErr := SumGroupSourceRows(in.SrcRoots, fileSystem)
	if srcErr != nil {
		return fmt.Errorf("src: %w", srcErr)
	}

	segs, segErr := EnumerateGroupTarget(targetGroup, ir, fileSystem)
	if segErr != nil {
		return fmt.Errorf("target: %w", segErr)
	}
	onReport(EntryGroupReport{
		Group:       in.Group,
		EntryStage:  in.Entry.Stage,
		EntryTarget: in.Entry.Target,
		EntryNodes:  in.Entry.Nodes,
		SrcRoots:    in.SrcRoots,
		SrcRows:     srcRows,
		SrcParts:    srcParts,
		TargetGroup: targetGroup,
		TargetSegs:  segs,
	})
	return nil
}
