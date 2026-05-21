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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// directCopyDayFormat / directCopyHourFormat mirror segmentController.format
// in banyand/internal/storage/segment.go.
const (
	directCopyDayFormat   = "20060102"
	directCopyHourFormat  = "2006010215"
	directCopySegPrefix   = "seg-"
	directCopyShardPrefix = "shard-"
	directCopySidxDirName = "sidx"
	directCopySnpSuffix   = ".snp"
)

var directCopyPartDirPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// nofsyncFS wraps a local FileSystem and skips per-file fsync /
// directory sync calls. Migration is a one-shot bulk import: a crash
// mid-run leaves the target measure-root in an unusable state anyway
// and the operator re-runs with --clean. Per-part fsyncs were the
// dominant wall-time cost (60%+ of CPU samples in syscall.syscall on
// open/write/fsync) precisely because each chunked target part fans
// out 7-10 small files. Skipping fsync trades zero-crash-safety for
// throughput, which is the right trade-off here.
type nofsyncFS struct {
	fs.FileSystem
}

func (f nofsyncFS) Write(buffer []byte, name string, permission fs.Mode) (int, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", name, err)
	}
	n, err := file.Write(buffer)
	if cerr := file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (f nofsyncFS) WriteAtomic(buffer []byte, name string, permission fs.Mode) (int, error) {
	return f.Write(buffer, name, permission)
}

func (nofsyncFS) SyncPath(string) {}

// FastPathHits / SlowPathHits / SlowPathRows expose copy-path
// instrumentation to the CLI so it can print a quick fast-vs-slow
// breakdown after each RunDirectCopy.
func FastPathHits() int64 { return fastPathHits.Load() }

// SlowPathHits returns the number of source parts that went through the
// row-level rewrite path.
func SlowPathHits() int64 { return slowPathHits.Load() }

// SlowPathRows returns the total source-row count across all parts that
// were forced onto the slow path.
func SlowPathRows() int64 { return slowPathRows.Load() }

// DirectCopyConfig configures a one-shot direct-file copy migration of
// measure data from a backup snapshot into one or more local target
// roots. The caller (e.g. banyand/cmd/migration) is expected to have
// stopped the BanyanDB process writing to any of the Entries' Target
// directories before invoking MigrationCopy.
//
// Both measure schemas (tag families + IndexMode bit) and group resource
// options (SegmentInterval per LifecycleStage) are read straight from
// the backup's schema-property bluge catalog — no liaison access needed.
//
// Each Entry names one (Stage, Target) destination; the same source
// rows are aligned by that stage's SegmentInterval and written under
// Target. Schemas, ResourceOpts and per-group union sidx are loaded /
// built once and shared across every entry.
type DirectCopyConfig struct {
	BackupDir string
	Date      string

	// SchemaPropertyPath, when set, points the schema loader at a
	// schema-property catalog directory directly (e.g. a live cluster's
	// schema-property PVC mounted into the runner pod). When empty, the
	// loader falls back to discovering the catalog under BackupDir using
	// the backup snapshot's standard layout.
	SchemaPropertyPath string

	// SidxStagingDir is where per-group union sidx artifacts are built
	// before being broadcast into every aligned target segment. Required:
	// the caller owns the directory's lifecycle (creation and cleanup).
	SidxStagingDir string

	Groups  []string
	Entries []DirectCopyEntry
}

// DirectCopyEntry names one fan-out destination. Each entry is
// processed in turn; for every group, the tool reads source parts from
// either the explicit Source paths (preferred) or, when Source is
// empty, derives the per-node source roots from BackupDir + Date + Nodes
// using the backup snapshot's standard layout. Rows are aligned by
// Stage's SegmentInterval and written under the target group root.
//
// Each Source entry must be a measure-data root — a directory whose
// children are the per-group segment trees that BanyanDB writes during
// normal operation. All Source entries are traversed and merged when
// building the per-group union sidx.
type DirectCopyEntry struct {
	Stage  string
	Target string
	Source []string
	Nodes  []string
}

// DirectCopyResult summarizes one MigrationCopy invocation, with totals
// aggregated across every (entry, group) the run touched.
type DirectCopyResult struct {
	Duration    time.Duration
	Rows        int64
	Bytes       int64
	SourceParts int
	TargetParts int
	Segments    int
}

// MigrationCopy is the package-public entry point for the migration's
// copy strategy. See banyand/cmd/migration/copy.go for the CLI driver.
//
// Flow:
//  1. Load schemas + group resource opts directly from the backup's
//     schema-property bluge catalog (once for the whole run).
//  2. Reject up front if any requested group contains an IndexMode
//     measure — broadcasting a union sidx would let queries fan their
//     full result set across data nodes and corrupt cross-node dedup.
//  3. Resolve every group's source directories once; build one union
//     sidx per group under SidxStagingDir — the union is source-only
//     and gets reused across every entry.
//  4. For each (entry, group): align rows by entry.Stage's
//     SegmentInterval and write parts under the entry's target group
//     root. After all parts of one (entry, group) land, broadcast that
//     group's union sidx into every aligned target segment.
//
// Entries run sequentially; the first error aborts the run.
func MigrationCopy(ctx context.Context, cfg DirectCopyConfig) (DirectCopyResult, error) {
	var res DirectCopyResult
	if err := validateDirectCopyConfig(&cfg); err != nil {
		return res, err
	}
	if cfg.SidxStagingDir == "" {
		return res, errors.New("sidxStagingDir is required (caller owns its lifecycle)")
	}
	// Reset path-hit counters so a previous in-process run's totals don't
	// bleed into this one (CLI invokes MigrationCopy once, but tests and
	// future callers may invoke it multiple times within one binary).
	fastPathHits.Store(0)
	slowPathHits.Store(0)
	slowPathRows.Store(0)
	start := time.Now()

	logStep("loading measure schemas")
	//nolint:contextcheck // bluge reader.Search inside walkSchemaPropertyShard already uses its own context.
	schemas, err := loadMeasureSchemasFromSchemaCatalog(cfg.BackupDir, cfg.SchemaPropertyPath, cfg.Groups)
	if err != nil {
		return res, fmt.Errorf("load measure schemas: %w", err)
	}
	// Refuse to proceed when a requested group resolved to zero
	// measures: the slow path would otherwise silently write target
	// parts with an empty tag projection (no tag columns), which is
	// indistinguishable from "no tags" at read time and corrupts the
	// query result. Typical cause: typo in --groups or stale backup.
	for _, g := range cfg.Groups {
		if len(schemas[g]) == 0 {
			return res, fmt.Errorf("group %q has no measures in backup schema-property catalog — typo in groups or empty group?", g)
		}
	}
	if rejectErr := rejectIndexModeGroups(cfg.Groups, schemas); rejectErr != nil {
		return res, rejectErr
	}

	//nolint:contextcheck // bluge reader.Search inside walkSchemaPropertyShard already uses its own context.
	resourceOpts, err := loadGroupResourceOptsFromSchema(cfg.BackupDir, cfg.SchemaPropertyPath, cfg.Groups)
	if err != nil {
		return res, fmt.Errorf("load group resource opts: %w", err)
	}

	// Pre-flight: every (entry, group) must resolve to a SegmentInterval
	// up front. Failing here saves an expensive union-sidx build only to
	// abort mid-run on a typo (warn vs warm has been the historic gotcha).
	for entryIdx, entry := range cfg.Entries {
		for _, group := range cfg.Groups {
			opts, ok := resourceOpts[group]
			if !ok || opts == nil {
				return res, fmt.Errorf("group %s: ResourceOpts not found in backup schema-property catalog", group)
			}
			if _, err := resolveStageInterval(opts, entry.Stage); err != nil {
				return res, fmt.Errorf("entry %d (target=%s) group %s: %w",
					entryIdx, entry.Target, group, err)
			}
		}
	}

	// Phase A: build one union sidx per group, sourced from EVERY entry's
	// roots merged (dedup by path). Each (entry, group) in Phase B reuses
	// the same staged sidx so two invariants hold:
	//   - Cross-replica broadcast invariant: the same series-ID set ends
	//     up under every target seg's sidx subdir, which is what liaison
	//     relies on to dedup distributed query fan-outs.
	//   - We don't rescan the same source bluge index N times per group
	//     (was 5 entries × 3 groups = 15 builds on a 5-PVC GKE plan).
	groupUnionSidx := make(map[string]string, len(cfg.Groups))
	for _, group := range cfg.Groups {
		if ctx.Err() != nil {
			res.Duration = time.Since(start)
			return res, ctx.Err()
		}
		srcRoots := collectAllSrcGroupRoots(cfg, group)
		if len(srcRoots) == 0 {
			logStep("group %s: no source dirs across any entry — skipping union sidx build", group)
			continue
		}
		logStep("group %s: building union sidx from %d source dir(s) merged across all entries",
			group, len(srcRoots))
		unionSidxPath, err := buildGroupUnionSidx(ctx, srcRoots,
			filepath.Join(cfg.SidxStagingDir, "groups", group, directCopySidxDirName))
		if err != nil {
			res.Duration = time.Since(start)
			return res, fmt.Errorf("group %s: build union sidx: %w", group, err)
		}
		groupUnionSidx[group] = unionSidxPath
	}

	// Phase B: per entry × group, align source rows by stage's
	// SegmentInterval and write parts under the entry's target group
	// root, broadcasting the pre-built union sidx (from Phase A) into
	// every aligned segment.
	for entryIdx, entry := range cfg.Entries {
		if ctx.Err() != nil {
			res.Duration = time.Since(start)
			return res, ctx.Err()
		}
		entryTag := fmt.Sprintf("entry [%d/%d]", entryIdx+1, len(cfg.Entries))
		logStep("%s stage=%s nodes=%v target=%s",
			entryTag, entry.Stage, entry.Nodes, entry.Target)
		for _, group := range cfg.Groups {
			if ctx.Err() != nil {
				res.Duration = time.Since(start)
				return res, ctx.Err()
			}
			srcRoots := resolveEntrySrcRoots(cfg, entry, group)
			if len(srcRoots) == 0 {
				if len(entry.Source) > 0 {
					logStep("%s stage=%s group %s: skipped (none of source paths %v contain this group)",
						entryTag, entry.Stage, group, entry.Source)
				} else {
					logStep("%s stage=%s group %s: skipped (none of nodes %v carry this group)",
						entryTag, entry.Stage, group, entry.Nodes)
				}
				continue
			}
			tasks, err := discoverPartTasks(ctx, srcRoots)
			if err != nil {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("%s stage=%s group %s: discover parts: %w",
					entryTag, entry.Stage, group, err)
			}
			if len(tasks) == 0 {
				logStep("%s stage=%s group %s: skipped (no parts in selected nodes)",
					entryTag, entry.Stage, group)
				continue
			}
			opts, ok := resourceOpts[group]
			if !ok || opts == nil {
				return res, fmt.Errorf("group %s: ResourceOpts not found in backup schema-property catalog", group)
			}
			ir, err := resolveStageInterval(opts, entry.Stage)
			if err != nil {
				return res, fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
			}
			unionSidxPath := groupUnionSidx[group]
			targetGroupRoot := filepath.Join(entry.Target, group)
			logStep("%s stage=%s group %s: %d source dir(s), %d parts, interval=%v×%d — writing target (target=%q, union sidx=%q)",
				entryTag, entry.Stage, group, len(srcRoots), len(tasks), ir.Unit, ir.Num,
				targetGroupRoot, unionSidxPath)
			groupRes, err := directCopyGroup(ctx,
				entryTag,
				group, entry,
				targetGroupRoot,
				ir, schemas[group],
				tasks, unionSidxPath)
			if err != nil {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("%s stage=%s group %s: %w", entryTag, entry.Stage, group, err)
			}
			logStep("%s stage=%s group %s: done rows=%d segs=%d srcParts=%d targetParts=%d",
				entryTag, entry.Stage, group,
				groupRes.Rows, groupRes.Segments, groupRes.SourceParts, groupRes.TargetParts)
			// Return idle heap to the OS so compressed-memory pressure
			// doesn't accumulate across (entry, group) — macOS jetsam
			// killed earlier runs at 224 GB compressed.
			debug.FreeOSMemory()
			res.SourceParts += groupRes.SourceParts
			res.TargetParts += groupRes.TargetParts
			res.Rows += groupRes.Rows
			res.Bytes += groupRes.Bytes
			res.Segments += groupRes.Segments
		}
	}
	res.Duration = time.Since(start)
	return res, nil
}

func logStep(format string, args ...any) {
	fmt.Fprintf(os.Stdout, time.Now().Format("2006/01/02 15:04:05")+" [migration] "+format+"\n", args...)
}

// StageHot is the conventional sentinel for "use the group's default
// (top-level) SegmentInterval". BanyanDB models the hot tier as the
// implicit destination of new writes, so groups rarely list "hot" as a
// named stage; users still need a way to migrate INTO the hot tier, and
// this constant is that way.
const StageHot = "hot"

// resolveStageInterval picks the SegmentInterval that should drive the
// target grid for one (group, stage) pair. stage must either be the
// reserved "hot" sentinel (which selects opts.SegmentInterval) or
// exactly match one of opts.GetStages() entries — silent fallback to
// default on an unknown stage has bitten operators in the past
// (warn-vs-warm typo wrote multi-day source rows into a 1-day grid).
func resolveStageInterval(opts *commonv1.ResourceOpts, stage string) (storage.IntervalRule, error) {
	var zero storage.IntervalRule
	if stage == StageHot {
		if opts.GetSegmentInterval() == nil {
			return zero, fmt.Errorf("stage %q maps to default SegmentInterval but the group has none", stage)
		}
		return intervalRuleFromProto(opts.GetSegmentInterval())
	}
	for _, st := range opts.GetStages() {
		if st.GetName() == stage {
			if st.GetSegmentInterval() == nil {
				return zero, fmt.Errorf("stage %q has no segmentInterval", stage)
			}
			return intervalRuleFromProto(st.GetSegmentInterval())
		}
	}
	available := make([]string, 0, len(opts.GetStages())+1)
	available = append(available, StageHot+" (default)")
	for _, st := range opts.GetStages() {
		available = append(available, st.GetName())
	}
	return zero, fmt.Errorf("stage %q not found in group schema; available: %s",
		stage, strings.Join(available, ", "))
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

// resolveEntrySrcRoots picks the source group dirs for one (entry,
// group) pair. When entry.Source is set, treat each entry as a
// measure-data root and look for "<source>/<group>"; otherwise fall
// back to the backup-snapshot layout via Nodes. Either path can yield
// zero dirs — the caller logs and skips.
func resolveEntrySrcRoots(cfg DirectCopyConfig, entry DirectCopyEntry, group string) []string {
	if len(entry.Source) > 0 {
		var roots []string
		for _, p := range entry.Source {
			candidate := filepath.Join(p, group)
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				roots = append(roots, candidate)
			}
		}
		sort.Strings(roots)
		return roots
	}
	return collectEntrySrcGroupRoots(cfg.BackupDir, cfg.Date, group, entry.Nodes)
}

// collectEntrySrcGroupRoots returns the `<backupDir>/<node>/<date>/measure/<group>`
// directories that exist for the explicit set of nodes the entry asked
// for. Nodes that don't carry the group (or don't exist at all) are
// silently dropped — that's a legitimate "this stage's machines don't
// own this group" case. Returned paths are sorted.
func collectEntrySrcGroupRoots(backupDir, date, group string, nodes []string) []string {
	var roots []string
	for _, node := range nodes {
		if node == "" {
			continue
		}
		candidate := filepath.Join(backupDir, node, date, "measure", group)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			roots = append(roots, candidate)
		}
	}
	sort.Strings(roots)
	return roots
}

// collectAllSrcGroupRoots merges resolveEntrySrcRoots across every
// entry in cfg (de-duped) so Phase A can build one union sidx per group
// that covers the whole plan, not just one entry's node slice. The
// returned list is sorted for determinism.
func collectAllSrcGroupRoots(cfg DirectCopyConfig, group string) []string {
	seen := make(map[string]struct{})
	var roots []string
	for _, entry := range cfg.Entries {
		for _, r := range resolveEntrySrcRoots(cfg, entry, group) {
			if _, dup := seen[r]; dup {
				continue
			}
			seen[r] = struct{}{}
			roots = append(roots, r)
		}
	}
	sort.Strings(roots)
	return roots
}

// rejectIndexModeGroups errors out if any requested group contains at
// least one IndexMode measure. The union-sidx broadcast strategy is
// only safe for non-IndexMode measures (whose query result rows come
// from part data, not from the sidx itself); silently letting an
// IndexMode group through would corrupt cross-node dedup at the
// liaison.
func rejectIndexModeGroups(groups []string, schemas map[string]map[string]*measureSchemaInfo) error {
	var offenders []string
	for _, g := range groups {
		for _, m := range schemas[g] {
			if m.IndexMode {
				offenders = append(offenders, fmt.Sprintf("%s/%s", g, m.Name))
			}
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)
	return fmt.Errorf("refusing to copy IndexMode measures (their data lives inside sidx; "+
		"the union-sidx broadcast strategy would break query correctness): %s",
		strings.Join(offenders, ", "))
}

func validateDirectCopyConfig(cfg *DirectCopyConfig) error {
	if cfg.SchemaPropertyPath == "" {
		if cfg.BackupDir == "" {
			return errors.New("backupDir is required when SchemaPropertyPath is unset")
		}
		if cfg.Date == "" {
			return errors.New("date is required when SchemaPropertyPath is unset")
		}
	}
	// SidxStagingDir is checked in MigrationCopy specifically — verify
	// is read-only and does not stage anything.
	if len(cfg.Groups) == 0 {
		return errors.New("groups is required")
	}
	if len(cfg.Entries) == 0 {
		return errors.New("entries is required")
	}
	seenTarget := make(map[string]int, len(cfg.Entries))
	for i, e := range cfg.Entries {
		if e.Stage == "" {
			return fmt.Errorf("entries[%d].Stage is required", i)
		}
		if e.Target == "" {
			return fmt.Errorf("entries[%d].Target is required", i)
		}
		if len(e.Source) == 0 && len(e.Nodes) == 0 {
			return fmt.Errorf("entries[%d]: at least one of Source or Nodes must be set", i)
		}
		if prev, dup := seenTarget[e.Target]; dup {
			return fmt.Errorf("entries[%d].Target %q duplicates Entries[%d].Target", i, e.Target, prev)
		}
		seenTarget[e.Target] = i
	}
	return nil
}

// fastPathHits / slowPathHits / slowPathRows track how often each
// processOneSourcePart branch fires across one RunDirectCopy. Surfaced
// in the final summary so operators can see whether their backup is
// already grid-aligned (fast path dominates) or needs row-level
// rewrite (slow path dominates).
var (
	fastPathHits atomic.Int64
	slowPathHits atomic.Int64
	slowPathRows atomic.Int64
)

type partTask struct {
	srcSegName string
	shardName  string
	shardDir   string
	partIDStr  string
}

// discoverPartTasks walks every <srcGroupRoot>/seg-*/shard-*/<partID>
// directory across all supplied roots and produces a flat task list
// for the worker pool to consume without further IO contention.
func discoverPartTasks(ctx context.Context, srcGroupRoots []string) ([]partTask, error) {
	var tasks []partTask
	for _, srcGroupRoot := range srcGroupRoots {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		segEntries, err := os.ReadDir(srcGroupRoot)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", srcGroupRoot, err)
		}
		for _, segE := range segEntries {
			if !segE.IsDir() || !strings.HasPrefix(segE.Name(), directCopySegPrefix) {
				continue
			}
			srcSegName := segE.Name()
			srcSegDir := filepath.Join(srcGroupRoot, srcSegName)
			shardEntries, err := os.ReadDir(srcSegDir)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", srcSegDir, err)
			}
			for _, shardE := range shardEntries {
				if !shardE.IsDir() || !strings.HasPrefix(shardE.Name(), directCopyShardPrefix) {
					continue
				}
				shardName := shardE.Name()
				shardDir := filepath.Join(srcSegDir, shardName)
				partEntries, err := os.ReadDir(shardDir)
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", shardDir, err)
				}
				for _, partE := range partEntries {
					if !partE.IsDir() || !directCopyPartDirPattern.MatchString(partE.Name()) {
						continue
					}
					tasks = append(tasks, partTask{
						srcSegName: srcSegName,
						shardName:  shardName,
						shardDir:   shardDir,
						partIDStr:  partE.Name(),
					})
				}
			}
		}
	}
	return tasks, nil
}

// segStateForGroup tracks bookkeeping needed at end-of-group to write
// segment-level metadata.json and per-shard .snp files.
type segStateForGroup struct {
	alignedTime time.Time
	shards      map[string][]uint64 // shard-N -> partIDs landed here
}

// segStateRegistry is a goroutine-safe registry of segStateForGroup,
// used so the per-part workers can register their landed (alignedSeg,
// shard, partID) tuples concurrently without stepping on each other.
type segStateRegistry struct {
	m  map[string]*segStateForGroup
	mu sync.Mutex
}

func newSegStateRegistry() *segStateRegistry {
	return &segStateRegistry{m: map[string]*segStateForGroup{}}
}

// register attaches one target partID to (alignedSegName, shardName) and
// guarantees a segStateForGroup exists for alignedSegName with the
// supplied alignedTime. Concurrent callers serialize on the registry's
// mutex; the critical section is tiny (slice append + map lookup) so
// contention stays well below worker compute cost.
func (r *segStateRegistry) register(alignedSegName string, alignedTime time.Time, shardName string, partID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ss, ok := r.m[alignedSegName]
	if !ok {
		ss = &segStateForGroup{
			alignedTime: alignedTime,
			shards:      map[string][]uint64{},
		}
		r.m[alignedSegName] = ss
	}
	ss.shards[shardName] = append(ss.shards[shardName], partID)
}

// snapshot returns the underlying map for serial end-of-group
// processing (metadata.json + .snp writes). Callers may only iterate
// it after every worker has finished.
func (r *segStateRegistry) snapshot() map[string]*segStateForGroup {
	return r.m
}

// directCopyGroup walks every source part for one group, splits each
// source part's rows by aligned target segment, flushes one memPart per
// (alignedSeg, sourcePart) bucket, then writes per-segment metadata and
// per-shard .snp at the end. The group's union sidx (pre-built in
// staging) is broadcast 1:1 into every aligned target segment that
// received any rows; an empty unionSidxPath skips the sidx broadcast.
func directCopyGroup(ctx context.Context,
	entryTag string,
	group string, entry DirectCopyEntry, dstGroupRoot string,
	ir storage.IntervalRule, groupSchemas map[string]*measureSchemaInfo,
	tasks []partTask, unionSidxPath string,
) (DirectCopyResult, error) {
	var res DirectCopyResult

	if err := directCopyPrepareTarget(dstGroupRoot); err != nil {
		return res, err
	}

	tagProjection := buildTagProjectionFromGroupSchemas(groupSchemas)

	segStates := newSegStateRegistry()
	// partIDGen issues monotonic partIDs (1, 2, 3, ...) shared across
	// every srcGroupRoot and every worker so concurrent writes never
	// collide. Starting at 0 is safe because directCopyPrepareTarget
	// asserts the target group dir is empty and the migration runs
	// while banyandb is not writing to it; on restart banyandb seeds
	// its own counter past the highest partID we wrote.
	var partIDGen atomic.Uint64
	fileSystem := nofsyncFS{FileSystem: fs.NewLocalFileSystem()}

	totalTasks := len(tasks)

	// Worker pool: N = NumCPU, each worker owns a fresh BytesBlockDecoder
	// (the decoder caches state across reads and is not safe for concurrent
	// use). Workers fan out over the task list; the segStateRegistry,
	// partIDGen and result counters are the only shared mutable state.
	workerCount := runtime.NumCPU()
	if workerCount > totalTasks {
		workerCount = totalTasks
	}
	if workerCount < 1 {
		workerCount = 1
	}

	// Shared bounded flush pool: a fixed set of consumers drain
	// flushCh and do the heavy encode + zstd + write per bucket. Outer
	// workers (per source part) post one job per bucket per chunk and
	// wait on a per-chunk WaitGroup; this caps simultaneous encode+zstd
	// at flushWorkerCount rather than `outerWorkers × bucketsPerChunk`
	// (which had no bound and caused OOM on cold-tier fan-out workloads).
	flushWorkerCount := workerCount
	flushCh := make(chan flushJob, flushWorkerCount*2)
	var flushWg sync.WaitGroup
	for i := 0; i < flushWorkerCount; i++ {
		flushWg.Add(1)
		go func() {
			defer flushWg.Done()
			runFlushWorker(flushCh, fileSystem)
		}()
	}
	defer func() {
		close(flushCh)
		flushWg.Wait()
	}()

	// Buffered so producers don't rendezvous with each consumer pull.
	taskCh := make(chan partTask, workerCount*2)
	var (
		wg             sync.WaitGroup
		resMu          sync.Mutex
		sourceParts    int
		targetParts    int
		rowsCopied     int64
		bytesWritten   int64
		firstErr       error
		errMu          sync.Mutex
		partsCompleted atomic.Int64
	)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decoder := &encoding.BytesBlockDecoder{}
			for task := range taskCh {
				if workerCtx.Err() != nil {
					return
				}
				partResult, err := func() (pr processPartResult, err error) {
					// processOneSourcePart's mustReadMetadata / mustOpenFilePart /
					// b.mustReadFrom panic on corrupted parts. Convert panics into
					// firstErr so one bad source part doesn't tear down the whole
					// migration before the deferred staging-dir cleanup runs.
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic processing %s/%s/%s: %v",
								task.srcSegName, task.shardName, task.partIDStr, r)
						}
					}()
					return processOneSourcePart(
						processPartInput{
							ir:            ir,
							decoder:       decoder,
							fileSystem:    fileSystem,
							tagProjection: tagProjection,
							shardName:     task.shardName,
							shardDir:      task.shardDir,
							partIDStr:     task.partIDStr,
							dstGroupRoot:  dstGroupRoot,
							segStates:     segStates,
							partIDGen:     &partIDGen,
							flushCh:       flushCh,
						},
					)
				}()
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancelWorkers()
					}
					errMu.Unlock()
					return
				}
				done := partsCompleted.Add(1)
				logStep("%s stage=%s group %s: part %d/%d (%.1f%%) done %s/%s/%s rows=%d targetParts=%d",
					entryTag, entry.Stage, group, done, totalTasks,
					100.0*float64(done)/float64(totalTasks),
					task.srcSegName, task.shardName, task.partIDStr,
					partResult.rows, partResult.targetParts)
				resMu.Lock()
				sourceParts++
				targetParts += partResult.targetParts
				rowsCopied += partResult.rows
				bytesWritten += partResult.bytes
				resMu.Unlock()
			}
		}()
	}

	for _, t := range tasks {
		select {
		case taskCh <- t:
		case <-workerCtx.Done():
		}
	}
	close(taskCh)
	wg.Wait()
	if firstErr != nil {
		return res, firstErr
	}
	res.SourceParts = sourceParts
	res.TargetParts = targetParts
	res.Rows = rowsCopied
	res.Bytes = bytesWritten

	unionSidxAvailable := false
	if unionSidxPath != "" {
		if info, statErr := os.Stat(unionSidxPath); statErr == nil && info.IsDir() {
			unionSidxAvailable = true
		}
	}
	segSnapshot := segStates.snapshot()
	logStep("%s stage=%s group %s: finalizing %d aligned target segments (writing metadata + snp + union sidx)",
		entryTag, entry.Stage, group, len(segSnapshot))
	for alignedSeg, ss := range segSnapshot {
		res.Segments++
		segDir := filepath.Join(dstGroupRoot, alignedSeg)
		endTime := ir.NextTime(ss.alignedTime)
		metaBytes, err := writeDirectCopySegmentMetadata(segDir, endTime)
		if err != nil {
			return res, fmt.Errorf("seg %s metadata: %w", alignedSeg, err)
		}
		res.Bytes += metaBytes
		for shardName, partIDs := range ss.shards {
			sort.Slice(partIDs, func(i, j int) bool { return partIDs[i] < partIDs[j] })
			names := make([]string, len(partIDs))
			for i, pid := range partIDs {
				names[i] = fmt.Sprintf("%016x", pid)
			}
			snpBytes, err := writeDirectCopySnp(
				filepath.Join(segDir, shardName), names,
			)
			if err != nil {
				return res, fmt.Errorf("seg %s shard %s snp: %w", alignedSeg, shardName, err)
			}
			res.Bytes += snpBytes
		}
		if unionSidxAvailable {
			sidxBytes, err := directCopyDir(
				unionSidxPath,
				filepath.Join(segDir, directCopySidxDirName),
			)
			if err != nil {
				return res, fmt.Errorf("seg %s union sidx: %w", alignedSeg, err)
			}
			res.Bytes += sidxBytes
		}
	}

	return res, nil
}

// buildTagProjectionFromGroupSchemas merges every measure's tag families
// in the group into a single tagProjection set. Reading more tags than
// the part actually contains is safe — block.mustReadFrom silently skips
// families/tags that aren't in the block.
func buildTagProjectionFromGroupSchemas(measures map[string]*measureSchemaInfo) []model.TagProjection {
	if len(measures) == 0 {
		return nil
	}
	families := map[string]map[string]bool{} // family -> set(tag)
	for _, m := range measures {
		for _, tf := range m.TagFamilies {
			if families[tf.Name] == nil {
				families[tf.Name] = map[string]bool{}
			}
			for _, t := range tf.Tags {
				families[tf.Name][t] = true
			}
		}
	}
	out := make([]model.TagProjection, 0, len(families))
	famNames := make([]string, 0, len(families))
	for name := range families {
		famNames = append(famNames, name)
	}
	sort.Strings(famNames)
	for _, name := range famNames {
		tagSet := families[name]
		tags := make([]string, 0, len(tagSet))
		for t := range tagSet {
			tags = append(tags, t)
		}
		sort.Strings(tags)
		out = append(out, model.TagProjection{Family: name, Names: tags})
	}
	return out
}

// processPartInput is the closed-over context passed to processOneSourcePart;
// keeping the helper free of long signatures. Field order is readability-first
// for an internal helper; fieldalignment churn here would obscure intent.
//
//nolint:govet // internal-only helper, readability > minor padding savings
type processPartInput struct {
	decoder       *encoding.BytesBlockDecoder
	fileSystem    fs.FileSystem
	segStates     *segStateRegistry
	partIDGen     *atomic.Uint64
	flushCh       chan<- flushJob
	tagProjection []model.TagProjection
	shardName     string
	shardDir      string
	partIDStr     string
	dstGroupRoot  string
	ir            storage.IntervalRule
}

type processPartResult struct {
	rows        int64
	bytes       int64
	targetParts int
}

// processOneSourcePart processes one source part. When the part is
// entirely inside one aligned target segment (the common case for
// already-grid-aligned backups), it takes the fast path: copy the
// whole part directory file-for-file and skip the decode / re-encode
// loop. Otherwise it falls back to the slow row-level split path
// that re-routes each row by timestamp.
func processOneSourcePart(in processPartInput) (processPartResult, error) {
	var pr processPartResult
	srcPartID, err := strconv.ParseUint(in.partIDStr, 16, 64)
	if err != nil {
		return pr, fmt.Errorf("parse partID %s: %w", in.partIDStr, err)
	}
	srcPartDir := filepath.Join(in.shardDir, in.partIDStr)

	// Cheap metadata-only probe to decide between fast and slow paths.
	var srcMeta partMetadata
	srcMeta.mustReadMetadata(in.fileSystem, srcPartDir)

	alignedMin := in.ir.Standard(time.Unix(0, srcMeta.MinTimestamp))
	alignedMax := in.ir.Standard(time.Unix(0, srcMeta.MaxTimestamp))
	if alignedMin.Equal(alignedMax) {
		// Fast path: entire part lands in a single target aligned segment.
		alignedSegName := formatDirectCopySegName(alignedMin, in.ir.Unit)
		fastPathHits.Add(1)
		return fastCopyOnePart(in, srcPartDir, alignedSegName, alignedMin, srcMeta.TotalCount)
	}

	slowPathHits.Add(1)
	slowPathRows.Add(int64(srcMeta.TotalCount))
	return slowCopyOnePart(in, srcPartID)
}

// slowCopyArena is a per-call slab that holds every nameValue and the
// supporting []*nameValue / []nameValues backing arrays produced by
// the row-level rewrite. Holding all of them in pre-sized contiguous
// arrays — instead of doing one heap allocation per (row, family,
// column) — was worth ~40-50% of the wall time on a cold-grid
// workload by collapsing the GC scan footprint.
//
// The arena is reset (length truncated to 0; cap retained) every time
// a chunk is flushed, so the same underlying memory serves the next
// chunk without further allocator pressure. Buckets that still hold
// *nameValue pointers into the arena are flushed before reset.
type slowCopyArena struct {
	nvs      []nameValue
	nvPtrs   []*nameValue
	famSlots []nameValues
}

func (a *slowCopyArena) reset() {
	a.nvs = a.nvs[:0]
	a.nvPtrs = a.nvPtrs[:0]
	a.famSlots = a.famSlots[:0]
}

// allocNameValue appends a nameValue to the arena and returns a stable
// pointer into the slab. The caller must guarantee cap(nvs) is large
// enough that the append doesn't reallocate (i.e. arena was sized for
// the chunk). On overflow we fall back to a fresh heap alloc to stay
// correct under pathological inputs.
func (a *slowCopyArena) allocNameValue(name string, value []byte, valueType pbv1.ValueType) *nameValue {
	if len(a.nvs)+1 > cap(a.nvs) {
		return &nameValue{name: name, value: value, valueType: valueType}
	}
	a.nvs = append(a.nvs, nameValue{name: name, value: value, valueType: valueType})
	return &a.nvs[len(a.nvs)-1]
}

// arenaTakePtrs carves out a contiguous []*nameValue of length n from
// the arena's pointer backing array. Like allocNameValue, it falls back
// to a fresh slice if the arena would have to grow (which would
// invalidate pointers we handed out earlier in the chunk).
func arenaTakePtrs(a *slowCopyArena, n int) []*nameValue {
	if len(a.nvPtrs)+n > cap(a.nvPtrs) {
		return make([]*nameValue, n)
	}
	start := len(a.nvPtrs)
	a.nvPtrs = a.nvPtrs[:start+n]
	return a.nvPtrs[start : start+n : start+n]
}

// arenaTakeFamSlots carves out a contiguous []nameValues of length n
// for the per-row tag-family slot table.
func arenaTakeFamSlots(a *slowCopyArena, n int) []nameValues {
	if len(a.famSlots)+n > cap(a.famSlots) {
		return make([]nameValues, n)
	}
	start := len(a.famSlots)
	a.famSlots = a.famSlots[:start+n]
	return a.famSlots[start : start+n : start+n]
}

// slowCopyOnePartChunkRows caps how many rows the slow path keeps
// in-memory across all buckets before forcing a flush. Tuned on the
// cold-grid workload:
//
//   - 100K rows: ~36s wall but ~26 GB RSS peak with NumCPU=16
//     workers — overran a kind-on-Mac 32 GB Docker VM.
//   - 50K rows: ~50s wall (~40% slower) but ~13 GB RSS peak — fits
//     comfortably in a 20 GB pod limit and leaves headroom for the
//     in-flight flush-pool encoders.
//
// The arena slab below is sized off this constant; halving it directly
// halves the long-lived per-worker allocation.
const slowCopyOnePartChunkRows = 50_000

// slowCopyArenaPool pools per-call slowCopyArena instances so
// successive parts processed by the same worker don't re-allocate the
// nameValue / *nameValue / nameValues backing arrays. The pool retains
// each arena's underlying cap; reset() truncates len to 0 but keeps
// cap so the next part reuses the slab in-place.
var slowCopyArenaPool = sync.Pool{
	New: func() any {
		// SkyWalking metric measures have 4-8 tag/field columns per row
		// in practice. Sizing the slab at 8 (was 16) halves the long-
		// lived per-worker allocation; pathological inputs fall back
		// to fresh heap allocs in allocNameValue / arenaTakePtrs.
		const estColumnsPerRow = 8
		return &slowCopyArena{
			nvs:      make([]nameValue, 0, slowCopyOnePartChunkRows*estColumnsPerRow),
			nvPtrs:   make([]*nameValue, 0, slowCopyOnePartChunkRows*estColumnsPerRow),
			famSlots: make([]nameValues, 0, slowCopyOnePartChunkRows*2),
		}
	},
}

func acquireSlowCopyArena() *slowCopyArena {
	a := slowCopyArenaPool.Get().(*slowCopyArena)
	a.reset()
	return a
}

func releaseSlowCopyArena(a *slowCopyArena) {
	a.reset()
	slowCopyArenaPool.Put(a)
}

// appendBlockRowToBuckets routes one row of one block to the correct
// per-aligned-segment bucket and writes its (seriesID, ts, version,
// tagFamilies, field) tuple via the arena. Block buffers are aliased
// directly into nameValue.value — the block must outlive the bucket
// until flush completes, which slowCopyOnePart guarantees by deferring
// releaseBlock until after flushChunk.
//
// Lives outside slowCopyOnePart so when banyandb's block struct gains
// or drops a field, only this helper needs updating.
// alignedSegCache memoises the last (alignedSegName, segment window)
// pair so the row loop avoids ir.Standard + formatDirectCopySegName
// on consecutive rows that share a target segment — typically the
// common case once banyandb data is roughly time-clustered per block.
type alignedSegCache struct {
	name       string
	startNanos int64 // inclusive
	endNanos   int64 // exclusive
}

func (c *alignedSegCache) segNameFor(ir storage.IntervalRule, ts int64) string {
	if c.name != "" && ts >= c.startNanos && ts < c.endNanos {
		return c.name
	}
	start := ir.Standard(time.Unix(0, ts))
	c.name = formatDirectCopySegName(start, ir.Unit)
	c.startNanos = start.UnixNano()
	c.endNanos = ir.NextTime(start).UnixNano()
	return c.name
}

func appendBlockRowToBuckets(
	ir storage.IntervalRule,
	b *block,
	seriesID common.SeriesID,
	k uint64,
	arena *slowCopyArena,
	buckets map[string]*dataPoints,
	segCache *alignedSegCache,
) {
	ts := b.timestamps[k]
	alignedSegName := segCache.segNameFor(ir, ts)

	dp, exists := buckets[alignedSegName]
	if !exists {
		dp = generateDataPoints()
		dp.reset()
		buckets[alignedSegName] = dp
	}
	dp.seriesIDs = append(dp.seriesIDs, seriesID)
	dp.timestamps = append(dp.timestamps, ts)
	dp.versions = append(dp.versions, b.versions[k])

	rowTagFamilies := arenaTakeFamSlots(arena, len(b.tagFamilies))
	for fi := range b.tagFamilies {
		cf := &b.tagFamilies[fi]
		ptrs := arenaTakePtrs(arena, len(cf.columns))
		for ci := range cf.columns {
			c := &cf.columns[ci]
			var v []byte
			if uint64(len(c.values)) > k {
				v = c.values[k]
			}
			ptrs[ci] = arena.allocNameValue(c.name, v, c.valueType)
		}
		rowTagFamilies[fi] = nameValues{name: cf.name, values: ptrs}
	}
	dp.tagFamilies = append(dp.tagFamilies, rowTagFamilies)

	fieldPtrs := arenaTakePtrs(arena, len(b.field.columns))
	for ci := range b.field.columns {
		c := &b.field.columns[ci]
		var v []byte
		if uint64(len(c.values)) > k {
			v = c.values[k]
		}
		fieldPtrs[ci] = arena.allocNameValue(c.name, v, c.valueType)
	}
	dp.fields = append(dp.fields, nameValues{name: b.field.name, values: fieldPtrs})
}

// slowCopyOnePart handles the row-level rewrite path: opens the source
// part, decodes blocks, and routes rows into per-aligned-target
// buckets. Buckets are flushed when the cumulative row count reaches
// slowCopyOnePartChunkRows or at end of part. Live blocks are held
// alive across multiple block iterations so bucket nameValue.value
// slices can alias the block buffers (zero-copy) — they are all
// released only after the buckets that reference them have been
// flushed.
//
// The primaryBlockMetadata walk + b.mustReadFrom pattern intentionally
// mirrors migration_reader.go:readPartRows (the MigrationReplay sibling).
// partIter (used by query / merge paths) carries a sids-filter + heap
// merge that adds no value when we want every block of one part — and
// would force an upfront scan to enumerate seriesIDs. Keep the two
// migration_* functions in lockstep: if banyandb changes its block
// layout, both update together.
func slowCopyOnePart(in processPartInput, srcPartID uint64) (processPartResult, error) {
	var pr processPartResult
	p := mustOpenFilePart(srcPartID, in.shardDir, in.fileSystem)
	defer p.close()

	flushBuckets := func(buckets map[string]*dataPoints) error {
		if len(buckets) == 0 {
			return nil
		}
		// Submit one job per bucket to the shared flush pool and wait
		// on a per-chunk WaitGroup. The shared pool caps concurrent
		// encode + zstd at flushWorkerCount instead of spawning one
		// goroutine per (outerWorker, bucket) — that unbounded fan-out
		// ballooned heap to 224 GB compressed on cold-tier loads and
		// got jetsam-killed.
		var (
			chunkWg    sync.WaitGroup
			chunkErrMu sync.Mutex
			chunkErr   error
			chunkBytes atomic.Int64
		)
		setErr := func(err error) {
			chunkErrMu.Lock()
			if chunkErr == nil {
				chunkErr = err
			}
			chunkErrMu.Unlock()
		}
		addBytes := func(b int64) { chunkBytes.Add(b) }
		for alignedSegName, dp := range buckets {
			chunkWg.Add(1)
			targetPartID := in.partIDGen.Add(1)
			targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
			dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
			in.flushCh <- flushJob{
				dp:           dp,
				alignedSeg:   alignedSegName,
				dstPart:      dstPart,
				targetPartID: targetPartID,
				shardName:    in.shardName,
				ir:           in.ir,
				segStates:    in.segStates,
				chunkWg:      &chunkWg,
				setErr:       setErr,
				addBytes:     addBytes,
			}
		}
		chunkWg.Wait()
		pr.bytes += chunkBytes.Load()
		pr.targetParts += len(buckets)
		return chunkErr
	}

	buckets := map[string]*dataPoints{}
	liveBlocks := make([]*block, 0, 8)
	chunkRows := 0
	arena := acquireSlowCopyArena()
	defer releaseSlowCopyArena(arena)
	var segCache alignedSegCache

	releaseLive := func() {
		for _, b := range liveBlocks {
			releaseBlock(b)
		}
		liveBlocks = liveBlocks[:0]
	}
	// Guarantee live blocks return to the pool even on flush errors,
	// chunk-walk errors, or early returns below. Successful flushChunk
	// calls drain liveBlocks first; this defer is a safety net.
	defer releaseLive()
	// Safety net for unflushed bucket dp's on early-return error paths
	// (decompress / unmarshal failures below). Once flushBuckets has been
	// called the dp's are owned by the workers (directCopyFlushBucket
	// defers releaseDataPoints), so flushChunk clears the map afterwards
	// to keep this defer a no-op in the normal path.
	defer func() {
		for _, dp := range buckets {
			releaseDataPoints(dp)
		}
	}()
	flushChunk := func() error {
		if chunkRows == 0 {
			return nil
		}
		err := flushBuckets(buckets)
		// Workers have already returned every submitted dp to the pool
		// via directCopyFlushBucket's defer (regardless of per-job
		// error). Drop our map references so the outer defer doesn't
		// double-release.
		buckets = map[string]*dataPoints{}
		if err != nil {
			return err
		}
		releaseLive()
		arena.reset()
		chunkRows = 0
		return nil
	}

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)
	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		compressed = bytes.ResizeOver(compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), compressed)
		var err error
		raw, err = zstd.Decompress(raw[:0], compressed)
		if err != nil {
			releaseLive()
			return pr, fmt.Errorf("decompress primary block: %w", err)
		}
		bms, err = unmarshalBlockMetadata(bms[:0], raw)
		if err != nil {
			releaseLive()
			return pr, fmt.Errorf("unmarshal block metadata: %w", err)
		}
		for j := range bms {
			bm := &bms[j]
			bm.tagProjection = in.tagProjection
			b := generateBlock()
			b.mustReadFrom(in.decoder, p, *bm)
			liveBlocks = append(liveBlocks, b)

			for k := uint64(0); k < bm.count; k++ {
				appendBlockRowToBuckets(in.ir, b, bm.seriesID, k, arena, buckets, &segCache)
				pr.rows++
				chunkRows++
			}
			if chunkRows >= slowCopyOnePartChunkRows {
				if err := flushChunk(); err != nil {
					return pr, err
				}
			}
		}
	}
	if err := flushChunk(); err != nil {
		return pr, err
	}
	return pr, nil
}

// fastCopyOnePart handles the case where every row in a source part
// already lands in the same target aligned segment. We can skip all
// decoding / re-encoding and just copy the part directory verbatim
// (under a fresh target partID so concurrent sources don't collide).
// The caller pre-resolved alignedSegName + alignedTime from the part
// metadata so this function does no further timestamp math.
func fastCopyOnePart(
	in processPartInput,
	srcPartDir, alignedSegName string,
	alignedTime time.Time,
	totalCount uint64,
) (processPartResult, error) {
	var pr processPartResult
	targetPartID := in.partIDGen.Add(1)
	targetPartIDStr := fmt.Sprintf("%016x", targetPartID)
	dstPart := filepath.Join(in.dstGroupRoot, alignedSegName, in.shardName, targetPartIDStr)
	pr.rows = int64(totalCount)
	pr.targetParts = 1

	if err := os.MkdirAll(filepath.Dir(dstPart), storage.DirPerm); err != nil {
		return pr, fmt.Errorf("mkdir %s: %w", filepath.Dir(dstPart), err)
	}
	bytesCopied, err := directCopyDir(srcPartDir, dstPart)
	if err != nil {
		return pr, fmt.Errorf("fast-copy %s -> %s: %w", srcPartDir, dstPart, err)
	}
	pr.bytes = bytesCopied

	in.segStates.register(alignedSegName, alignedTime, in.shardName, targetPartID)
	return pr, nil
}

// flushJob is one bucket's worth of work submitted to the shared flush
// pool: write dp out as a target part, register the segment, and report
// bytes / errors back to the submitting chunk.
type flushJob struct {
	dp           *dataPoints
	chunkWg      *sync.WaitGroup
	setErr       func(error)
	addBytes     func(int64)
	segStates    *segStateRegistry
	alignedSeg   string
	dstPart      string
	shardName    string
	targetPartID uint64
	ir           storage.IntervalRule
}

// runFlushWorker drains flushCh until it is closed. The flush pool size
// is the only cap on concurrent encode + zstd + write — sized to NumCPU
// in directCopyGroup so the host can't be drowned by a chunked slow path.
func runFlushWorker(flushCh <-chan flushJob, fileSystem fs.FileSystem) {
	for job := range flushCh {
		sz, err := directCopyFlushBucket(job.dp, fileSystem, job.dstPart)
		if err != nil {
			job.setErr(fmt.Errorf("flush %s: %w", job.dstPart, err))
			job.chunkWg.Done()
			continue
		}
		job.addBytes(sz)
		alignedTime, err := parseDirectCopySegStart(job.alignedSeg, job.ir.Unit)
		if err != nil {
			job.setErr(fmt.Errorf("parse seg start %s: %w", job.alignedSeg, err))
			job.chunkWg.Done()
			continue
		}
		job.segStates.register(job.alignedSeg, alignedTime, job.shardName, job.targetPartID)
		job.chunkWg.Done()
	}
}

// directCopyFlushBucket builds a memPart from dp and flushes it to disk at
// partPath, then releases pooled objects. mustFlush internally calls
// MkdirPanicIfExist(partPath), so the caller must only pre-create the
// parent shard dir; this helper handles that. dp is always returned to
// the pool — including on the MkdirAll error path.
func directCopyFlushBucket(dp *dataPoints, fileSystem fs.FileSystem, partPath string) (int64, error) {
	defer releaseDataPoints(dp)
	if err := os.MkdirAll(filepath.Dir(partPath), storage.DirPerm); err != nil {
		return 0, err
	}
	mp := generateMemPart()
	mp.mustInitFromDataPoints(dp)
	mp.mustFlush(fileSystem, partPath)
	sz := int64(mp.partMetadata.CompressedSizeBytes)
	releaseMemPart(mp)
	return sz, nil
}

// directCopyPrepareTarget refuses to write into a non-empty group dir.
// The operator must remove (or relocate) the previous target manually
// before re-running.
func directCopyPrepareTarget(dstGroupRoot string) error {
	entries, err := os.ReadDir(dstGroupRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dstGroupRoot, storage.DirPerm)
		}
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("target %s is not empty; remove it before re-running", dstGroupRoot)
	}
	return nil
}

// formatDirectCopySegName mirrors segmentController.format.
func formatDirectCopySegName(t time.Time, unit storage.IntervalUnit) string {
	switch unit {
	case storage.HOUR:
		return directCopySegPrefix + t.Format(directCopyHourFormat)
	case storage.DAY:
		return directCopySegPrefix + t.Format(directCopyDayFormat)
	}
	panic(fmt.Sprintf("formatDirectCopySegName: unsupported interval unit %v", unit))
}

// parseDirectCopySegStart inverts formatDirectCopySegName.
func parseDirectCopySegStart(segName string, unit storage.IntervalUnit) (time.Time, error) {
	suffix := strings.TrimPrefix(segName, directCopySegPrefix)
	switch unit {
	case storage.HOUR:
		return time.ParseInLocation(directCopyHourFormat, suffix, time.Local)
	case storage.DAY:
		return time.ParseInLocation(directCopyDayFormat, suffix, time.Local)
	}
	return time.Time{}, fmt.Errorf("unrecognized interval unit %v", unit)
}

// writeDirectCopySegmentMetadata writes <segDir>/metadata matching the
// runtime's segmentMeta layout (banyand/internal/storage/version.go).
// The filename, JSON tags, and Version string all come from the exported
// storage helpers so a banyandb runtime that reads compatibleVersions
// from versions.yml will accept these files without modification.
func writeDirectCopySegmentMetadata(segDir string, endTime time.Time) (int64, error) {
	if err := os.MkdirAll(segDir, storage.DirPerm); err != nil {
		return 0, err
	}
	body := storage.SegmentMetadata{
		Version: storage.CurrentSegmentVersion,
		EndTime: endTime.Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(
		filepath.Join(segDir, storage.SegmentMetadataFilename),
		data, storage.FilePerm,
	); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// writeDirectCopySnp writes a <epoch-hex16>.snp file listing the part
// names for one (alignedSeg, shard) so tsTable.loadSnapshot finds them
// at next startup.
func writeDirectCopySnp(dstShard string, partNames []string) (int64, error) {
	if err := os.MkdirAll(dstShard, storage.DirPerm); err != nil {
		return 0, err
	}
	data, err := json.Marshal(partNames)
	if err != nil {
		return 0, err
	}
	snpPath := filepath.Join(dstShard,
		fmt.Sprintf("%016x%s", time.Now().UnixNano(), directCopySnpSuffix))
	if err := os.WriteFile(snpPath, data, storage.FilePerm); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// directCopyDir replicates src into dst via byte-level file copy,
// returning total bytes materialized. Used both for the union-sidx
// broadcast and for the fast-path whole-part copy.
//
// Earlier versions of this helper attempted a recursive hard-link via
// fs.LocalFileSystem.CreateHardLink first, falling back to byte copy
// on cross-FS failure. Every observed deployment path (staging on
// emptyDir → target on PVC) trips cross-FS and forces the byte path
// anyway, so the hardlink attempt was pure overhead + noisy log
// lines. We now always go straight to the byte copy.
func directCopyDir(src, dst string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dst), storage.DirPerm); err != nil {
		return 0, err
	}
	return directCopyDirByteCopy(src, dst)
}

// directCopyDirByteCopy walks src and byte-copies every file into dst.
// Called from directCopyDir for both the union-sidx broadcast and the
// fast-path whole-part copy.
func directCopyDirByteCopy(src, dst string) (int64, error) {
	var total int64
	if err := os.MkdirAll(dst, storage.DirPerm); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		srcPath := filepath.Join(src, e.Name())
		dstPath := filepath.Join(dst, e.Name())
		if e.IsDir() {
			sub, recErr := directCopyDirByteCopy(srcPath, dstPath)
			if recErr != nil {
				return total, recErr
			}
			total += sub
			continue
		}
		n, copyErr := directCopyFile(srcPath, dstPath)
		if copyErr != nil {
			return total, copyErr
		}
		total += n
	}
	return total, nil
}

func directCopyFile(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, storage.FilePerm)
	if err != nil {
		return 0, err
	}
	// No out.Sync(): consistent with nofsyncFS, migration is one-shot
	// bulk import and operator re-runs with --clean on crash.
	defer out.Close()
	n, copyErr := io.Copy(out, in)
	return n, copyErr
}

// Field names copied verbatim from pkg/index/inverted/inverted.go. The
// migration tool deliberately does not depend on that package's
// (writer-flavored) constants because we open every source sidx
// read-only via raw bluge — no banyandb store lifecycle is needed.
const (
	unionSidxDocIDField     = "_id"
	unionSidxTimestampField = "_timestamp"
	unionSidxVersionField   = "_version"

	// Larger batches mean fewer bluge merger ticks; the merger loop
	// dominates CPU at small batch sizes. 50k keeps the merger work
	// off the hot path while still fitting in the per-process heap.
	unionSidxBatchSize = 50000
)

// buildGroupUnionSidx walks every srcGroupRoot/seg-*/sidx/ directory
// across every supplied group root, scans every series-index doc,
// deduplicates by SeriesID, and re-emits the surviving docs into a
// fresh bluge index rooted at stagingPath.
//
// The returned path is stagingPath when at least one doc was written;
// it is "" (without error) when no source sidx contained any doc — the
// caller treats this as "no sidx to broadcast" and skips the per-target
// copy. SeriesID is recovered by unmarshaling the source doc's EntityValues
// (the bluge _id field) into pbv1.Series and reading series.ID.
//
// We intentionally bypass pkg/index/inverted's store wrapper here: that
// wrapper starts background flush goroutines and registers metrics, both
// of which are unnecessary for a one-shot offline build. Raw bluge
// readers/writers keep the staging artifact byte-for-byte compatible
// with what BanyanDB writes at runtime — the wrapper itself ultimately
// produces the same .seg/.snp layout.
func buildGroupUnionSidx(ctx context.Context, srcGroupRoots []string, stagingPath string) (string, error) {
	if err := os.MkdirAll(stagingPath, storage.DirPerm); err != nil {
		return "", fmt.Errorf("mkdir staging %q: %w", stagingPath, err)
	}

	writer, err := bluge.OpenWriter(bluge.DefaultConfig(stagingPath))
	if err != nil {
		return "", fmt.Errorf("open union sidx writer at %q: %w", stagingPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()

	// Collect every source sidx directory across all node + segment
	// roots up front so the worker pool can fan out cleanly.
	var sidxPaths []string
	for _, srcGroupRoot := range srcGroupRoots {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		segEntries, err := os.ReadDir(srcGroupRoot)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("read src group root %q: %w", srcGroupRoot, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), directCopySegPrefix) {
				continue
			}
			srcSidxPath := filepath.Join(srcGroupRoot, se.Name(), directCopySidxDirName)
			info, statErr := os.Stat(srcSidxPath)
			if statErr != nil || !info.IsDir() {
				continue
			}
			sidxPaths = append(sidxPaths, srcSidxPath)
		}
	}

	seen := make(map[common.SeriesID]struct{}, 1_000_000)
	var seenMu sync.Mutex
	var writerMu sync.Mutex
	var insertedAtomic atomic.Int64
	var firstErr atomic.Pointer[error]

	workerCount := runtime.NumCPU()
	if workerCount > len(sidxPaths) {
		workerCount = len(sidxPaths)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	pathCh := make(chan string)
	var wg sync.WaitGroup
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for srcSidxPath := range pathCh {
				if workerCtx.Err() != nil {
					return
				}
				count, mergeErr := mergeOneSourceSidxInto(workerCtx, srcSidxPath, writer, seen, &seenMu, &writerMu)
				if mergeErr != nil {
					e := fmt.Errorf("merge %s: %w", srcSidxPath, mergeErr)
					if firstErr.CompareAndSwap(nil, &e) {
						cancelWorkers()
					}
					return
				}
				insertedAtomic.Add(int64(count))
			}
		}()
	}
	for _, p := range sidxPaths {
		select {
		case pathCh <- p:
		case <-workerCtx.Done():
		}
	}
	close(pathCh)
	wg.Wait()
	if errPtr := firstErr.Load(); errPtr != nil {
		return "", *errPtr
	}
	inserted := int(insertedAtomic.Load())

	closed = true
	if closeErr := writer.Close(); closeErr != nil {
		return "", fmt.Errorf("close union sidx writer: %w", closeErr)
	}
	if inserted == 0 {
		// Nothing to broadcast. Remove the empty bluge directory so the
		// caller can cleanly signal "no sidx for this group" by checking
		// for an empty return path. Leaving a half-baked empty index
		// behind would otherwise confuse a future bluge open at the
		// same path.
		_ = os.RemoveAll(stagingPath)
		return "", nil
	}
	return stagingPath, nil
}

// mergeOneSourceSidxInto opens one source sidx, scans every doc, and
// inserts unseen-SeriesID docs into dst in batched flushes. Concurrent
// callers serialize on seenMu (dedup map) and writerMu (single bluge
// writer); cross-source readers run in parallel.
func mergeOneSourceSidxInto(
	ctx context.Context,
	srcPath string,
	dst *bluge.Writer,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
	writerMu *sync.Mutex,
) (int, error) {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(srcPath))
	if err != nil {
		// A sidx directory may exist on disk while carrying no committed
		// bluge snapshot — e.g. a fresh segment created by the runtime
		// whose sidx writer never received a doc. Treat that as "no
		// sidx to merge for this seg" instead of aborting the whole
		// per-group union build.
		if strings.Contains(err.Error(), "unable to find a usable snapshot") {
			return 0, nil
		}
		return 0, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	if err != nil {
		return 0, fmt.Errorf("search: %w", err)
	}

	batch := bluge.NewBatch()
	batched := 0
	inserted := 0

	flush := func() error {
		writerMu.Lock()
		defer writerMu.Unlock()
		return dst.Batch(batch)
	}

	for {
		next, nextErr := dmi.Next()
		if nextErr != nil {
			return inserted, fmt.Errorf("iterate docs: %w", nextErr)
		}
		if next == nil {
			break
		}

		doc, dup, buildErr := buildDocFromMatchLocked(next, seen, seenMu)
		if buildErr != nil {
			return inserted, buildErr
		}
		if dup || doc == nil {
			continue
		}
		batch.Insert(doc)
		batched++
		inserted++
		if batched >= unionSidxBatchSize {
			if err := flush(); err != nil {
				return inserted, fmt.Errorf("flush batch: %w", err)
			}
			batch = bluge.NewBatch()
			batched = 0
		}
	}
	if batched > 0 {
		if err := flush(); err != nil {
			return inserted, fmt.Errorf("flush tail batch: %w", err)
		}
	}
	return inserted, nil
}

// buildDocFromMatchLocked is the concurrency-safe variant: the caller
// passes the dedup map's mutex; the seen check + insert is a single
// critical section so two workers never insert the same SeriesID.
func buildDocFromMatchLocked(
	match *search.DocumentMatch,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
) (*bluge.Document, bool, error) {
	var entityValues []byte
	type storedField struct {
		name  string
		value []byte
	}
	var fields []storedField
	visitErr := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case unionSidxDocIDField:
			entityValues = append([]byte(nil), value...)
		default:
			fields = append(fields, storedField{
				name:  field,
				value: append([]byte(nil), value...),
			})
		}
		return true
	})
	if visitErr != nil {
		return nil, false, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	if len(entityValues) == 0 {
		return nil, false, nil
	}
	var series pbv1.Series
	if err := series.Unmarshal(entityValues); err != nil {
		return nil, false, nil
	}
	seenMu.Lock()
	if _, dup := seen[series.ID]; dup {
		seenMu.Unlock()
		return nil, true, nil
	}
	seen[series.ID] = struct{}{}
	seenMu.Unlock()

	doc := bluge.NewDocument(string(entityValues))
	for _, f := range fields {
		switch f.name {
		case unionSidxTimestampField:
			ts, decErr := bluge.DecodeDateTime(f.value)
			if decErr != nil {
				return nil, false, fmt.Errorf("decode timestamp on series %d: %w", series.ID, decErr)
			}
			doc.AddField(bluge.NewDateTimeField(f.name, ts).StoreValue())
		case unionSidxVersionField:
			doc.AddField(bluge.NewStoredOnlyField(f.name, f.value))
		default:
			doc.AddField(bluge.NewKeywordFieldBytes(f.name, f.value).StoreValue())
		}
	}
	return doc, false, nil
}
