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

package exporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// nodesDir is the directory below the artifact root holding one subtree per source node.
const nodesDir = "nodes"

// Env is what RunExport works with besides the settings: where it prints, what the preflight
// learned and the liaison connections.
type Env struct {
	// Log receives warnings and hints (stderr); Out receives the progress and the final summary.
	Log io.Writer
	Out io.Writer
	// Terminal is nil when Out is not a terminal. Otherwise it returns the terminal's width and
	// height (0 when unknown), and the progress redraws one live line per exporting node.
	Terminal func() (width, height int)
	// Confirm asks the operator a yes/no question and reports a yes; nil when nobody can
	// answer, and then a cluster held by another session refuses the export.
	Confirm  func(ctx context.Context, question string) bool
	Cluster  *cluster.ClusterInfo
	Liaisons *cluster.Liaisons
	// Groups is the full group list from cluster.FetchGroups; RunExport picks the selected ones.
	Groups []*commonv1.Group
}

// CheckOutputDir accepts an output directory that does not exist or is empty. Anything else
// is refused: this step has no resume, and a fresh export must not mix with leftovers.
func CheckOutputDir(dir string) error {
	if dir == "" {
		return exitcode.Exit(exitcode.ExitUsage, "no output directory: give --output or export.output in plan.yaml")
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "output directory %s: %w", dir, err)
	}
	if len(entries) > 0 {
		return exitcode.Exit(exitcode.ExitUsage, "output directory %s is not empty; give a new or empty directory", dir)
	}
	return nil
}

// SelectedGroups returns the groups the selectors cover, in the given order: all groups
// for no selector, every group of the catalog for a selector without groups.
func SelectedGroups(groups []*commonv1.Group, selectors []*transferv1.Selector) []*commonv1.Group {
	if len(selectors) == 0 {
		return groups
	}
	var out []*commonv1.Group
	for _, g := range groups {
		for _, s := range selectors {
			if s.GetCatalog() == g.GetCatalog() && (len(s.GetGroups()) == 0 || slices.Contains(s.GetGroups(), g.GetMetadata().GetName())) {
				out = append(out, g)
				break
			}
		}
	}
	return out
}

// RunExport performs a real export: schema, session, one Export
// stream per node, manifest, release. Schema, Plan and Sessions go through the primary
// liaison, the node streams are spread over every liaison. Once the session exists any
// failure keeps it and tells the operator how to release it; the error is an exitcode.ExitError
// (exit 3 unless the cause carries its own code).
func RunExport(ctx context.Context, env Env, opts config.Export) error {
	if opts.Format != transferv1.Format_FORMAT_CSV {
		return exitcode.Exit(exitcode.ExitUsage, "native export format is not implemented yet")
	}
	if opts.SchemaOnly && !opts.IncludeSchema {
		return exitcode.Exit(exitcode.ExitUsage, "a schema-only export needs the schema; it cannot be combined with --include-schema=false")
	}
	start := time.Now()
	prog := output.NewProgress(env.Out, env.Terminal, "node(s) exporting")
	// Workers and the heartbeat watcher print concurrently; every Fprintf is one Write, and
	// it goes through the progress so that it never tears the live lines.
	env.Log = prog.Wrap(env.Log)
	if err := CheckOutputDir(opts.Output); err != nil {
		return err
	}
	if err := os.MkdirAll(opts.Output, 0o700); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "create output directory: %w", err)
	}
	conn := env.Liaisons.Primary()
	groups := SelectedGroups(env.Groups, opts.Selectors)
	var schema *schemasync.Result
	if opts.IncludeSchema {
		var err error
		if schema, err = schemasync.Export(ctx, conn, groups, opts.Output); err != nil {
			// Leave the output directory empty again, so the same command can run next.
			_ = os.RemoveAll(filepath.Join(opts.Output, schemasync.Dir))
			return exitcode.Exit(exitcode.ExitCodeFor(err), "export schema: %w", err)
		}
		prog.Printf("schema of %d group(s) written to %s", len(groups), filepath.Join(opts.Output, schemasync.Dir))
	}
	m := baseManifest(opts.Compress, groups, schema)
	if opts.SchemaOnly {
		m.SchemaOnly = true
		if err := manifest.Write(filepath.Join(opts.Output, manifest.FileName), m); err != nil {
			return exitcode.Exit(exitcode.ExitRuntime, "write manifest: %w", err)
		}
		fmt.Fprintf(env.Out, "schema-only export written to %s\n", opts.Output)
		return nil
	}

	sessOpts := SessionOptions{
		Preempt:   opts.Preempt,
		OnCreated: func(id string) { prog.Printf("export session %s created", id) },
	}
	if env.Confirm != nil {
		sessOpts.Confirm = func(occupants string) bool {
			return env.Confirm(ctx, "another export session is held:\n"+occupants+
				"Take over the session(s) above? An export still running on them fails on its next read. [y/N] ")
		}
	}
	sess, plan, err := CreateSession(ctx, conn, opts.Selectors, sessOpts)
	if err != nil {
		// No data was exported: leave the output directory empty again, so the same command
		// (with --preempt, say) can run without emptying it first.
		_ = os.RemoveAll(filepath.Join(opts.Output, schemasync.Dir))
		if ctx.Err() != nil {
			// Interrupted, at the takeover question included: no session was created.
			return exitcode.Exit(exitcode.ExitRuntime, "export interrupted before its session was created: %w", context.Cause(ctx))
		}
		return err
	}
	if len(sess.Preempted) > 0 {
		fmt.Fprintf(env.Log, "WARN ⚠ this export preempted the existing export session(s) %v: an export still running on them fails on its next read\n",
			sess.Preempted)
	}
	elapsed, err := exportSession(ctx, env, prog, opts, schema, sess, plan, m)
	if err != nil {
		fmt.Fprintf(env.Log, "export session %s kept; release it with: bydbctl data release-session --id %s\n", sess.ID, sess.ID)
		var exitErr *exitcode.ExitError
		if errors.As(err, &exitErr) {
			return err
		}
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionsCallTimeout)
	defer cancel()
	result, err := ReleaseSession(releaseCtx, conn, sess.ID)
	switch {
	case err != nil:
		fmt.Fprintf(env.Log, "WARN release of export session %s failed (its lease expires on its own): %v\n", sess.ID, err)
	case len(result.Failed) > 0:
		for _, f := range result.Failed {
			fmt.Fprintf(env.Log, "WARN release of export session %s failed on %s: %s\n", sess.ID, f.GetNodeId(), f.GetError())
		}
	}
	if err != nil || len(result.Failed) > 0 {
		fmt.Fprintf(env.Log, "release it with: bydbctl data release-session --id %s\n", sess.ID)
	}
	if err = renderSummary(env.Out, opts.Output, m, elapsed, time.Since(start)); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "render summary: %w", err)
	}
	return nil
}

// exportSession runs the per-node workers under the heartbeat and writes the manifest. The
// nodes, in id order, are dealt round robin over the liaisons. It returns the time the units
// took, keyed by elapsedKey.
func exportSession(ctx context.Context, env Env, prog *output.Progress, opts config.Export, schema *schemasync.Result,
	sess *Session, plan *cluster.PlanResult, m *manifest.Manifest,
) (map[string]time.Duration, error) {
	jobs, err := buildJobs(plan)
	if err != nil {
		return nil, err
	}
	conn := env.Liaisons.Primary()
	l := logger.GetLogger("exporter")
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	stopHeartbeat, lost := StartHeartbeat(runCtx, conn, sess.ID, 0, l, env.Log)
	defer stopHeartbeat()
	run.Go(runCtx, "export-heartbeat-watch", l, func(ctx context.Context) {
		select {
		case lostErr := <-lost:
			cancelRun(exitcode.Exit(exitcode.ExitRuntime, "%w", lostErr))
		case <-ctx.Done():
		}
	})

	parallelism, _ := config.ClampParallelism(max(opts.Parallelism, 1), len(jobs))
	prog.Printf("exporting %d node(s), parallelism %d, via %d liaison(s)", len(jobs), parallelism, len(env.Liaisons.All))
	init := &transferv1.ExportRequest_Init{
		SessionId:             sess.ID,
		Format:                opts.Format,
		Compress:              opts.Compress,
		MaxFileSize:           opts.MaxFileSize,
		RateLimit:             ratelimit.Proto(opts.NodeRateLimit, opts.NodeRowRateLimit),
		CsvAnnotations:        opts.CSVAnnotations,
		LiaisonBytesPerSecond: ratelimit.PerSecond(opts.LiaisonRateLimit),
	}
	if rl := init.GetRateLimit(); rl.GetBytesPerSecond() > 0 || rl.GetRowsPerSecond() > 0 || init.GetLiaisonBytesPerSecond() > 0 {
		prog.Printf("rate limits: node %d bytes/s, %d rows/s; liaison %d bytes/s per liaison (0 = unlimited)",
			rl.GetBytesPerSecond(), rl.GetRowsPerSecond(), init.GetLiaisonBytesPerSecond())
	}
	ids := make([]string, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.nodeID)
	}
	width := nodeWidth(ids)
	prog.Start(runCtx)
	g, gctx := errgroup.WithContext(runCtx)
	g.SetLimit(parallelism)
	for i, job := range jobs {
		nodeInit := proto.Clone(init).(*transferv1.ExportRequest_Init)
		nodeInit.TargetNode = job.nodeID
		w := &nodeExport{
			job: job, init: nodeInit, schema: schema, log: env.Log,
			liaisons: env.Liaisons.All,
			next:     i,
			root:     filepath.Join(opts.Output, nodesDir, job.dir),
			relRoot:  nodesDir + "/" + job.dir,
			retryFor: opts.SendRetryTimeout,
			idleFor:  opts.NodeIdleTimeout,
		}
		g.Go(func() error {
			w.progress = newNodeProgress(prog, job.nodeID, len(job.units), width)
			runErr := w.run(gctx)
			w.progress.finish(runErr == nil)
			return runErr
		})
	}
	err = g.Wait()
	prog.Close()
	if cause := context.Cause(runCtx); cause != nil && ctx.Err() == nil && !errors.Is(cause, context.Canceled) {
		err = cause // the heartbeat lost the session; the workers only saw the cancellation
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	fillManifest(m, env.Cluster, jobs)
	m.Warnings = append(m.Warnings, StageWarnings(env.Groups, plan)...)
	for _, w := range m.Warnings {
		fmt.Fprintln(env.Log, "WARN "+w)
	}
	if err = manifest.Write(filepath.Join(opts.Output, manifest.FileName), m); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}
	elapsed := map[string]time.Duration{}
	for _, job := range jobs {
		for _, u := range job.units {
			elapsed[elapsedKey(job.nodeID, catalogLabel(u.catalog()), u.group())] += u.elapsed
		}
	}
	return elapsed, nil
}

// buildJobs folds the Plan frames into one job per node, each unit kept as the node
// reported it, and derives each node's directory, refusing two nodes that map to one.
func buildJobs(plan *cluster.PlanResult) ([]*nodeJob, error) {
	byNode := map[string]*nodeJob{}
	for _, f := range plan.Frames {
		job, ok := byNode[f.GetNodeId()]
		if !ok {
			job = &nodeJob{nodeID: f.GetNodeId(), dir: manifest.NodeDir(f.GetNodeId())}
			byNode[f.GetNodeId()] = job
		}
		for _, inv := range f.GetUnits() {
			u := &plannedUnit{stage: transfer.NormalizeStage(f.GetStage()), segment: inv.GetSegment(), property: inv.GetProperty()}
			if u.segment == nil && u.property == nil {
				return nil, exitcode.Exit(exitcode.ExitRuntime, "plan: node %s reported a unit of no kind", f.GetNodeId())
			}
			job.units = append(job.units, u)
		}
	}
	jobs := make([]*nodeJob, 0, len(byNode))
	dirs := map[string]string{}
	for _, job := range byNode {
		if other, dup := dirs[job.dir]; dup {
			return nil, exitcode.Exit(exitcode.ExitRuntime, "node ids %s and %s map to the same directory %s", other, job.nodeID, job.dir)
		}
		dirs[job.dir] = job.nodeID
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].nodeID < jobs[j].nodeID })
	return jobs, nil
}

// catalogLabel is the manifest spelling of a catalog: STREAM, MEASURE, TRACE, PROPERTY.
func catalogLabel(c commonv1.Catalog) string {
	return strings.TrimPrefix(c.String(), "CATALOG_")
}

// schemaSkippedWarning is the manifest warning of an export without the schema.
const schemaSkippedWarning = "the schema was not exported (--include-schema=false), so groups[].indexMode is unknown and recorded as false"

// baseManifest is the manifest without units and nodes: format, schema files and groups. The
// groups come from the group list; their index mode from the exported measures, or it is
// unknown when schema is nil, which a manifest warning says.
func baseManifest(compress transferv1.Compress, groups []*commonv1.Group, schema *schemasync.Result) *manifest.Manifest {
	m := &manifest.Manifest{
		ArtifactVersion: manifest.ArtifactVersion,
		Format:          "csv",
		Compress:        compressLabel(compress),
		ExportedAt:      time.Now().UTC().Format(time.RFC3339),
		SchemaFiles:     []manifest.File{},
	}
	if schema == nil {
		m.Warnings = append(m.Warnings, schemaSkippedWarning)
	} else {
		for _, f := range schema.Files {
			m.SchemaFiles = append(m.SchemaFiles, manifest.File{Path: f.Path, Bytes: f.Bytes, CRC32: manifest.FormatCRC32(f.CRC32)})
		}
	}
	for _, g := range groups {
		name := g.GetMetadata().GetName()
		mg := manifest.Group{Name: name, Catalog: catalogLabel(g.GetCatalog())}
		if schema != nil {
			for _, ms := range schema.Measures {
				if ms.GetMetadata().GetGroup() == name && ms.GetIndexMode() {
					mg.IndexMode = true
				}
			}
		}
		ro := g.GetResourceOpts()
		mg.Stages = append(mg.Stages, manifest.Stage{
			Name: transfer.DefaultStageName, ShardNum: ro.GetShardNum(), Replicas: ro.GetReplicas(),
			SegmentInterval: output.IntervalLabel(ro.GetSegmentInterval()), TTL: output.IntervalLabel(ro.GetTtl()),
		})
		for _, st := range ro.GetStages() {
			mg.Stages = append(mg.Stages, stageOf(st))
		}
		m.Groups = append(m.Groups, mg)
	}
	return m
}

// stageOf is a lifecycle stage as the manifest records it: the stage's own options, its
// ttl not added up with the earlier stages'.
func stageOf(st *commonv1.LifecycleStage) manifest.Stage {
	return manifest.Stage{
		Name: st.GetName(), ShardNum: st.GetShardNum(), Replicas: st.GetReplicas(),
		SegmentInterval: output.IntervalLabel(st.GetSegmentInterval()), TTL: output.IntervalLabel(st.GetTtl()),
	}
}

// compressLabel is the manifest spelling of a compression; RunExport has already resolved
// UNSPECIFIED to the csv default gzip, and it is labeled that way here too.
func compressLabel(c transferv1.Compress) string {
	if c == transferv1.Compress_COMPRESS_NONE {
		return "none"
	}
	return "gzip"
}

// fillManifest adds the source nodes, the units and the unit warnings. Segment
// units are keyed by (catalog, group, stage, segment) with one source per node. A property
// unit's key degrades to (catalog, group): every node's shards of the group land in one unit,
// whose stage is the stage of its first source (jobs are in node order). Shards come from the
// Plan statistics, their totalCount and rowCounts from the exported files.
func fillManifest(m *manifest.Manifest, info *cluster.ClusterInfo, jobs []*nodeJob) {
	tz := map[string]string{}
	if info != nil {
		for _, n := range info.DataNodes {
			tz[n.GetMetadata().GetName()] = n.GetTzName()
		}
	}
	units := map[string]*manifest.Unit{}
	for _, job := range jobs {
		m.Source.Nodes = append(m.Source.Nodes, manifest.Node{ID: job.nodeID, Dir: job.dir, Timezone: tz[job.nodeID]})
		for _, u := range job.units {
			key := strings.Join([]string{u.catalog().String(), u.group()}, "\x00")
			if u.segment != nil {
				key = strings.Join([]string{key, u.stage, u.segment.GetUnit().GetSegmentSuffix()}, "\x00")
			}
			mu, ok := units[key]
			if !ok {
				mu = &manifest.Unit{
					Catalog: catalogLabel(u.catalog()), Group: u.group(), Stage: u.stage,
					Segment: u.segment.GetUnit().GetSegmentSuffix(), SegmentVersion: u.segment.GetSegmentVersion(),
				}
				units[key] = mu
			}
			src := unitSource(job.nodeID, u)
			for _, sh := range src.Shards {
				transfer.MergeRange(&mu.MinTimestamp, &mu.MaxTimestamp, sh.MinTimestamp, sh.MaxTimestamp)
			}
			mu.Sources = append(mu.Sources, src)
			for _, w := range u.warnings {
				m.Warnings = append(m.Warnings, fmt.Sprintf("node %s unit %s: %s", job.nodeID, u.label(), w))
			}
		}
	}
	for _, mu := range units {
		sort.Slice(mu.Sources, func(i, j int) bool { return mu.Sources[i].Node < mu.Sources[j].Node })
		m.Units = append(m.Units, *mu)
	}
	sort.Slice(m.Units, func(i, j int) bool {
		a, b := m.Units[i], m.Units[j]
		return strings.Join([]string{a.Catalog, a.Group, a.Stage, a.Segment}, "\x00") < strings.Join([]string{b.Catalog, b.Group, b.Stage, b.Segment}, "\x00")
	})
}

// unitSource is one node's contribution to a unit.
func unitSource(nodeID string, u *plannedUnit) manifest.UnitSource {
	src := manifest.UnitSource{Node: nodeID, Shards: []manifest.Shard{}}
	shards := map[uint32]*manifest.Shard{}
	shard := func(id uint32) *manifest.Shard {
		s, ok := shards[id]
		if !ok {
			s = &manifest.Shard{Shard: id, RowCounts: map[string]uint64{}, Parts: []manifest.Part{}, Files: []manifest.File{}}
			shards[id] = s
		}
		return s
	}
	for _, st := range u.segment.GetShards() {
		s := shard(st.GetShardId())
		s.MinTimestamp, s.MaxTimestamp = st.GetMinTimestamp(), st.GetMaxTimestamp()
		s.EstimatedCompressedBytes, s.EstimatedUncompressedBytes = st.GetEstimatedCompressedBytes(), st.GetEstimatedUncompressedBytes()
		for _, p := range st.GetParts() {
			s.Parts = append(s.Parts, manifest.Part{ID: p.GetId(), MinTimestamp: p.GetMinTimestamp(), MaxTimestamp: p.GetMaxTimestamp(), TotalCount: p.GetTotalCount()})
		}
	}
	for _, st := range u.property.GetShards() {
		s := shard(st.GetShardId())
		s.DocCount = st.GetDocCount()
		s.EstimatedCompressedBytes, s.EstimatedUncompressedBytes = st.GetEstimatedBytes(), st.GetEstimatedBytes()
	}
	for _, f := range u.files {
		if f.segmentLevel {
			src.SegmentFiles = append(src.SegmentFiles, f.file)
			if src.RowCounts == nil {
				src.RowCounts = map[string]uint64{}
			}
			for name, n := range f.rowCounts {
				src.RowCounts[name] += n
				src.TotalCount += n
			}
			continue
		}
		s := shard(f.shardID)
		s.Files = append(s.Files, f.file)
		for name, n := range f.rowCounts {
			s.RowCounts[name] += n
			s.TotalCount += n
		}
	}
	for _, s := range shards {
		src.Shards = append(src.Shards, *s)
	}
	sort.Slice(src.Shards, func(i, j int) bool { return src.Shards[i].Shard < src.Shards[j].Shard })
	return src
}
