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

package stream

import (
	"context"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/reader"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// MigrationExecutor is the stream implementation of
// migration.CatalogExecutor: it copies / verifies stream groups one
// (entry, group) at a time under the shared orchestrator.
type MigrationExecutor struct {
	schemas  map[string]map[string]*streamSchemaInfo
	locators map[string]map[string]*streamIndexLocator
}

// NewMigrationExecutor returns the stream catalog executor.
func NewMigrationExecutor() *MigrationExecutor {
	return &MigrationExecutor{}
}

// Catalog reports CATALOG_STREAM.
func (e *MigrationExecutor) Catalog() commonv1.Catalog { return commonv1.Catalog_CATALOG_STREAM }

// LogPrefix tags this executor's progress lines.
func (e *MigrationExecutor) LogPrefix() string { return streamMigrationLogPrefix }

// Prepare loads the stream schemas and per-stream index locators of the
// given groups from the schema-property catalog.
func (e *MigrationExecutor) Prepare(_ context.Context, schemaRoot string, groups []string) error {
	logStreamStep("loading stream schemas")
	//nolint:contextcheck // bluge reader.Search inside reader.WalkShard already uses its own context.
	sc, err := reader.LoadStreamSchemaContext(schemaRoot, groups)
	if err != nil {
		return fmt.Errorf("load stream schemas: %w", err)
	}
	schemas := buildStreamSchemas(sc.Streams, groups)
	for _, g := range groups {
		if len(schemas[g]) == 0 {
			return fmt.Errorf("group %q has no streams in backup schema-property catalog — typo in groups or empty group?", g)
		}
	}
	e.schemas = schemas
	e.locators = buildStreamIndexLocators(sc, groups)
	// Reset path-hit counters so a previous in-process run's totals don't
	// bleed into this one.
	streamFastPathHits.Store(0)
	streamSlowPathHits.Store(0)
	streamSlowPathRows.Store(0)
	return nil
}

// CopyEntryGroup migrates one (entry, group): discovers source parts,
// rewrites them onto the target grid and finalizes the element index.
func (e *MigrationExecutor) CopyEntryGroup(ctx context.Context, in migration.EntryGroupInput) (migration.EntryGroupResult, error) {
	var zero migration.EntryGroupResult
	tasks, err := discoverStreamPartTasks(ctx, in.SrcRoots)
	if err != nil {
		return zero, fmt.Errorf("discover parts: %w", err)
	}
	if len(tasks) == 0 {
		logStreamStep("%s stage=%s group %s: skipped (no parts in selected nodes)",
			in.EntryTag, in.Entry.Stage, in.Group)
		return zero, nil
	}
	logStreamStep("%s stage=%s group %s: %d source dir(s), %d parts, interval=%v×%d — writing target (target=%q, union sidx=%q)",
		in.EntryTag, in.Entry.Stage, in.Group, len(in.SrcRoots), len(tasks), in.Interval.Unit, in.Interval.Num,
		in.TargetGroupRoot, in.UnionSidxPath)
	tagProjection := buildStreamTagProjectionFromGroupSchemas(e.schemas[in.Group])
	return directCopyStreamGroup(ctx,
		in.EntryTag,
		in.Group, in.Entry.Stage,
		in.TargetGroupRoot,
		in.Interval, tagProjection,
		tasks, in.UnionSidxPath,
		e.locators[in.Group])
}

// VerifyEntryGroup re-reads one (entry, group) read-only: it sums source
// rows, enumerates target segments (including per-shard idx/ doc counts),
// and emits a EntryGroupReport.
//
// Core invariant: stream never deduplicates, so srcRows == tgtRows strictly.
func (e *MigrationExecutor) VerifyEntryGroup(_ context.Context, in migration.EntryGroupInput, onReport func(report any)) error {
	fileSystem := fs.NewLocalFileSystem()
	ir := in.Interval
	targetGroup := in.TargetGroupRoot
	if len(in.SrcRoots) == 0 {
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
