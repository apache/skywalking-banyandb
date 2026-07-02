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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

// EntryGroupInput carries everything a catalog executor needs to process
// one (entry, group) pair. The orchestrator resolves source roots, the
// target group root, the stage's segment interval, and the pre-built
// union sidx before invoking the executor.
type EntryGroupInput struct {
	// EntryTag is the operator-facing "entry [i/n]" label for log lines.
	EntryTag string
	Group    string
	// TargetGroupRoot is `<entry.Target>/<group>`.
	TargetGroupRoot string
	// UnionSidxPath is the staged per-group union sidx to broadcast, or ""
	// when the group's sources carried no sidx docs.
	UnionSidxPath string
	// SrcRoots are the per-source `<root>/<group>` directories feeding this
	// entry; never empty when the executor is invoked.
	SrcRoots []string
	Entry    ResolvedEntry
	// Interval is the target grid resolved from the entry stage's
	// SegmentInterval.
	Interval storage.IntervalRule
}

// EntryGroupResult totals one executor invocation.
type EntryGroupResult struct {
	Rows        int64
	Bytes       int64
	SourceParts int
	TargetParts int
	Segments    int
}

// CatalogExecutor is the per-catalog migration backend. banyand/measure and
// banyand/stream each provide one; the orchestrator owns plan handling,
// catalog classification, union-sidx staging and the entry × group loops.
type CatalogExecutor interface {
	// Catalog reports which catalog this executor handles.
	Catalog() commonv1.Catalog
	// LogPrefix is the tag used on this executor's progress lines,
	// e.g. "[migration/measure]" (measure) or "[migration/stream]".
	LogPrefix() string
	// Prepare loads the catalog-specific schema context (schemas, index
	// locators, …) for the given groups from the resolved `_schema` root.
	// It is called once per run, before any Copy/Verify invocation.
	Prepare(ctx context.Context, schemaRoot string, groups []string) error
	// CopyEntryGroup migrates one (entry, group): discovers source parts
	// under in.SrcRoots, rewrites them onto the target grid, and finalizes
	// the group tree under in.TargetGroupRoot.
	CopyEntryGroup(ctx context.Context, in EntryGroupInput) (EntryGroupResult, error)
	// VerifyEntryGroup re-reads one (entry, group) read-only and emits its
	// catalog-specific report through onReport.
	VerifyEntryGroup(ctx context.Context, in EntryGroupInput, onReport func(report any)) error
}
