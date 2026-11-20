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
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// nodesDir is the directory below the artifact root holding one subtree per source node.
const nodesDir = "nodes"

// Env is what RunExport works with besides the settings: where it prints, what the preflight
// learned and the liaison connections.
type Env struct {
	// Log receives the information, warnings and hints and Out the progress and the final
	// summary, both stdout; Err receives what explains a failure before its error (stderr), Log
	// when nil.
	Log io.Writer
	Out io.Writer
	Err io.Writer
	// Terminal is nil when Out is not a terminal. Otherwise it returns the terminal's width and
	// height (0 when unknown), and the progress redraws one live line per exporting node.
	Terminal func() (width, height int)
	// Confirm asks the operator a yes/no question and reports a yes; nil when stdin is not a
	// terminal or none of stdout and stderr is one to ask on (stderr only when stdout is
	// redirected, with the lines Log and Out got so far shown there first), and then a cluster
	// held by another session refuses the export, and a progress file to resume needs --resume.
	Confirm  func(ctx context.Context, question string) bool
	Cluster  *cluster.Info
	Liaisons *cluster.Liaisons
	// Groups is the full group list from cluster.FetchGroups; RunExport picks the selected ones.
	Groups []*commonv1.Group
}

// CheckOutputDir accepts an output directory that does not exist or is empty, or one that
// holds the progress file at progressPath (ProgressPath): that export is resumed. The lock file
// of the progress file does not count. It also accepts what an export killed before its first
// progress write leaves when the progress file belongs in the directory (leftover), which the
// export removes (cleanLeftover) once it holds the lock. Anything else is refused, naming what the
// directory holds: a fresh export must not mix with leftovers. A progress file in a subdirectory
// of the output directory is refused: that subdirectory would end up in the artifact.
func CheckOutputDir(dir, progressPath string) error {
	if dir == "" {
		return exitcode.Exit(exitcode.ExitUsage, "no output directory: give --output or export.output in plan.yaml")
	}
	if info, err := os.Stat(dir); err == nil && !info.IsDir() {
		return exitcode.Exit(exitcode.ExitUsage, "output directory %s is not a directory", dir)
	}
	if progressPath != "" {
		if subdirOf(dir, filepath.Dir(progressPath)) {
			return exitcode.Exit(exitcode.ExitUsage, "the progress file %s (--checkpoint / export.checkpoint) is in a subdirectory of the output directory %s; "+
				"put it directly in %s or outside it", progressPath, dir, dir)
		}
		if resume.Exists(progressPath) {
			return nil
		}
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "output directory %s: %w", dir, err)
	}
	inDir := progressPath != "" && config.SameDir(filepath.Dir(progressPath), dir)
	var names []string
	isLeftover := true
	for _, e := range entries {
		name := e.Name()
		if inDir && name == filepath.Base(resume.LockPath(progressPath)) {
			continue
		}
		names = append(names, name)
		if !inDir || !leftover(dir, progressPath, e) {
			isLeftover = false
		}
	}
	if len(names) == 0 || isLeftover {
		return nil
	}
	return exitcode.Exit(exitcode.ExitUsage, "output directory %s is not empty and holds no export to resume (it holds %s); "+
		"give a new or empty directory, or delete it first with: rm -rf %s", dir, listed(names), dir)
}

// listed joins names, the first five of them and how many more there are.
func listed(names []string) string {
	shown := names
	if len(shown) > 5 {
		shown = append(slices.Clone(shown[:5]), fmt.Sprintf("and %d more", len(names)-5))
	}
	return strings.Join(shown, ", ")
}

// leftover reports whether e, an entry of the output directory dir that holds the progress file
// at progressPath, is what an export killed before its first progress write may leave: schema/
// with nothing but schema files in it, the plan side file of the progress file, and the
// temporary files of either.
func leftover(dir, progressPath string, e os.DirEntry) bool {
	name := e.Name()
	side := resume.SidePath(progressPath, planSide)
	if e.Type().IsRegular() {
		return name == filepath.Base(side) || resume.IsTemp(progressPath, name) || resume.IsTemp(side, name)
	}
	if name != schemasync.Dir || !e.IsDir() {
		return false
	}
	files, err := os.ReadDir(filepath.Join(dir, name))
	if err != nil {
		return false
	}
	for _, f := range files {
		if !f.Type().IsRegular() || !slices.ContainsFunc(schemasync.Kinds, func(kind string) bool { return f.Name() == kind+".jsonl" }) {
			return false
		}
	}
	return true
}

// subdirOf reports whether sub is a directory strictly below dir, compared as absolute paths.
func subdirOf(dir, sub string) bool {
	absDir, errDir := filepath.Abs(dir)
	absSub, errSub := filepath.Abs(sub)
	if errDir != nil || errSub != nil {
		return false
	}
	rel, err := filepath.Rel(absDir, absSub)
	return err == nil && rel != "." && filepath.IsLocal(rel)
}

// cleanLeftover removes what CheckOutputDir accepts as left by an export killed before its first
// progress write (leftover), naming it first. It runs under the lock, before a fresh export
// writes anything. The lock is that of the progress file, not of the output directory: an export
// into the same directory with another --checkpoint is not kept out by it.
func cleanLeftover(w io.Writer, dir, progressPath string) error {
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return exitcode.Exit(exitcode.ExitUsage, "output directory %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.Name() != filepath.Base(resume.LockPath(progressPath)) {
			names = append(names, e.Name())
		}
	}
	if len(names) > 0 {
		removingLeftovers(w, dir, listed(names))
		if err = os.RemoveAll(filepath.Join(dir, schemasync.Dir)); err != nil {
			return exitcode.Exit(exitcode.ExitUsage, "remove %s: %w", filepath.Join(dir, schemasync.Dir), err)
		}
	}
	// No progress file is at progressPath: this removes the plan side file and the temporary
	// files of both.
	if err = resume.Remove(progressPath, planSide); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	return nil
}

// lockProgress takes the lock of the progress file at path for the whole run, on success,
// failure and interrupt alike, a stale lock taken over with a warning on log; another bydbctl
// holding it exits exitcode.ExitUsage.
func lockProgress(log io.Writer, path string) (func(), error) {
	unlock, err := resume.LockWith(path, log)
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	return func() { _ = unlock() }, nil
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
// (exit 3 unless the cause carries its own code). The progress file (ProgressPath) records the
// session, its plan and every finished unit until the manifest is written; an output directory
// that holds one is resumed instead.
func RunExport(ctx context.Context, env Env, opts config.Export) error {
	if err := CheckFormat(&opts); err != nil {
		return err
	}
	if opts.SchemaOnly && !opts.IncludeSchema {
		return exitcode.Exit(exitcode.ExitUsage, "a schema-only export needs the schema; it cannot be combined with --include-schema=false")
	}
	start := time.Now()
	prog := output.NewProgress(env.Out, env.Terminal, "node(s) exporting")
	// Workers and the heartbeat watcher print concurrently; every Fprintf is one Write, and
	// it goes through the progress so that it never tears the live lines.
	if env.Err == nil {
		env.Err = env.Log
	}
	env.Log, env.Err = prog.Wrap(env.Log), prog.Wrap(env.Err)
	path := ProgressPath(opts)
	if err := resume.CheckSideCollision(path); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	// First without the lock, which creates the lock file and its directory: an output directory
	// that is refused is refused for what it holds, and gets nothing added.
	if err := CheckOutputDir(opts.Output, path); err != nil {
		return err
	}
	// Only a fresh export checks its selectors against the groups (exit 1): a resume compares them
	// with the recorded ones and its session with the recorded plan, so a group deleted since
	// refuses with how to start over (exit 6), and does not matter when only the manifest is left.
	// Before the lock, which creates the output directory: a refused export leaves nothing behind.
	if !resume.Exists(path) {
		if err := ValidateSelectors(env.Groups, opts.Selectors); err != nil {
			return exitcode.Exit(exitcode.ExitUsage, "%w", err)
		}
	}
	// Under the lock no other bydbctl writes the progress file: the checks below and the cleanup
	// of leftovers cannot race one. Taking the lock also creates the directory of the progress
	// file, so an unusable --checkpoint is refused before the cluster is touched.
	unlock, err := lockProgress(env.Log, path)
	if err != nil {
		return err
	}
	defer unlock()
	if err = CheckOutputDir(opts.Output, path); err != nil {
		return err
	}
	if resume.Exists(path) {
		if opts.SchemaOnly {
			return exitcode.Exit(exitcode.ExitUsage, "a previous export left %s; a schema-only export needs a new or empty directory", path)
		}
		return resumeExport(ctx, env, prog, opts, path)
	}
	if err = cleanLeftover(env.Log, opts.Output, path); err != nil {
		return err
	}
	if err = resume.MkdirAll(opts.Output, 0o700); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "create output directory: %w", err)
	}
	conn := env.Liaisons.Primary()
	groups := SelectedGroups(env.Groups, opts.Selectors)
	var schema *schemasync.Result
	if opts.IncludeSchema {
		if schema, err = schemasync.Export(ctx, conn, groups, opts.Output); err != nil {
			// Leave the output directory empty again, so the same command can run next.
			_ = os.RemoveAll(filepath.Join(opts.Output, schemasync.Dir))
			return exitcode.Exit(exitcode.For(err), "export schema: %w", err)
		}
		prog.Printf("schema of %d group(s) written to %s", len(groups), filepath.Join(opts.Output, schemasync.Dir))
	}
	m := baseManifest(opts.Format, opts.Compress, groups, schema)
	// The progress file records start as the export time, which a resume writes the manifest
	// with: the manifest a crash before the progress file is deleted gets written again is then
	// the same bytes.
	m.ExportedAt = start.UTC().Format(time.RFC3339)
	if opts.SchemaOnly {
		m.SchemaOnly = true
		if err = manifest.Write(filepath.Join(opts.Output, manifest.FileName), m); err != nil {
			return exitcode.Exit(exitcode.ExitRuntime, "write manifest: %w", err)
		}
		schemaOnlyWritten(env.Out, opts.Output)
		return nil
	}
	outputDir, err := filepath.Abs(opts.Output)
	if err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "output directory %s: %w", opts.Output, err)
	}
	state := &exportProgress{
		Header:    resume.Header{Kind: progressKind, Version: progressVersion},
		Format:    formatLabel(opts.Format),
		StartedAt: start, OutputDir: outputDir, Bindings: bindingsOf(opts, env.Cluster),
		SourceTimezones: nodeTimezones(env.Cluster, nil),
		SchemaFiles:     m.SchemaFiles, Groups: m.Groups,
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	track := startTracker(ctx, path, state, start, cancelRun)
	defer func() { _ = track.close() }()

	// createdErr is the failure to record the session id; OnCreated runs before CreateSession returns.
	// announced is every id the attempts of the create announced, the last of which the progress file records.
	var createdErr error
	var announced []string
	sessOpts := SessionOptions{
		Preempt: opts.Preempt,
		OnCreated: func(id string) {
			announced = append(announced, id)
			prog.Printf("export session %s created", id)
			// The id is on disk before anything else; a failure cancels the create.
			createdErr = track.sync(func(s *exportProgress) { s.Session.ID = id })
		},
	}
	if env.Confirm != nil {
		sessOpts.Confirm = func(occupants string) bool {
			return env.Confirm(ctx, "another export session is held:\n"+occupants+
				"Take over the session(s) above? An export still running on them fails on its next read. [y/N] ")
		}
	}
	sess, plan, err := CreateSession(runCtx, conn, opts.Selectors, sessOpts)
	if err != nil {
		// No data was exported: leave the output directory empty again, so the same command
		// (with --preempt, say) can run without emptying it first.
		trackErr := track.close()
		// The persister has stopped. A refused create (exit 2) was rolled back; one given up
		// after an announcement (exit 3) may stand: every id its attempts announced is released
		// here, before the progress file that records the last one is deleted, so that a run
		// stopped while it waits for the release leaves that id on disk.
		if len(announced) > 0 && exitcode.For(err) == exitcode.ExitRuntime {
			err = abandonedCreate(ctx, env.Log, conn, announced, runErr(ctx, runCtx, err))
			removeCreated(path, opts.Output)
			return err
		}
		removeCreated(path, opts.Output)
		if trackErr != nil {
			return exitcode.Exit(exitcode.ExitRuntime, "%w; %w", trackErr, err)
		}
		if ctx.Err() != nil {
			// Interrupted, at the takeover question included, before any session id was announced.
			return exitcode.Exit(exitcode.ExitRuntime, "export interrupted before its session was created: %w", context.Cause(ctx))
		}
		return err
	}
	if len(sess.Preempted) > 0 {
		warnPreempted(env.Log, sess.Preempted)
	}
	// The id must be on disk before the session is used: a failed write of it may not have
	// stopped the create in time.
	if err = createdErr; err != nil {
		_ = track.close()
		err = abandonedCreate(ctx, env.Log, conn, []string{sess.ID}, exitcode.Exit(exitcode.ExitRuntime, "%w", err))
		removeCreated(path, opts.Output)
		return err
	}
	jobs, err := buildJobs(plan)
	if err != nil {
		keptHint(env.Err, sess.ID, path, opts.Output)
		return err
	}
	return runSession(ctx, runCtx, env, prog, opts, track, sessionRun{schema: schema, sessID: sess.ID, plan: plan, jobs: jobs, m: m, start: start})
}

// removeCreated removes what an export whose session was not created wrote: the progress file at
// path, its plan side file and the schema files in output. No data was exported: the same command
// (with --preempt, say) can run again without emptying output first.
func removeCreated(path, output string) {
	_ = resume.Remove(path, planSide)
	_ = os.RemoveAll(filepath.Join(output, schemasync.Dir))
}

// runErr is err, or, when a failed write of the progress file stopped the run (runCtx ended and
// ctx did not), that failure: what it stopped saw only that. An interrupt (ctx ended) is reported
// as such: with its cause when err is only the bare cancellation of a node stream it canceled,
// and with err otherwise, so that a lost session, a changed schema or an announced session that
// may still be held keeps its text.
func runErr(ctx, runCtx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		if cause := context.Cause(ctx); !errors.Is(err, cause) && bareCancel(err) {
			err = cause
		}
		return fmt.Errorf("export interrupted: %w", err)
	}
	if err != nil && runCtx.Err() != nil {
		if cause := context.Cause(runCtx); cause != nil {
			return cause
		}
	}
	return err
}

// bareCancel reports whether err is nothing but a cancellation: neither a lost session, a changed
// schema nor an announced session carries it.
func bareCancel(err error) bool {
	var lost sessionLostError
	var changed schemaChangedError
	var announced *announcedError
	if errors.As(err, &lost) || errors.As(err, &changed) || errors.As(err, &announced) {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.Canceled
}

// sessionRun is what runSession exports: the session sessID with its plan and the units of it,
// the schema and the base manifest m, when this run started, and what the runs before a resumed
// export did (prev, nil for a fresh one). planRecorded tells that the plan is on disk already.
type sessionRun struct {
	start        time.Time
	schema       *schemasync.Result
	plan         *cluster.PlanResult
	m            *manifest.Manifest
	prev         *resumedRun
	sessID       string
	jobs         []*nodeJob
	planRecorded bool
}

// runSession records the plan unless run.planRecorded, exports the units not done yet, writes the
// manifest, deletes the progress file and releases the session. ctx is the caller's context,
// runCtx the one a progress-file failure cancels.
func runSession(ctx, runCtx context.Context, env Env, prog *output.Progress, opts config.Export, track *tracker, run sessionRun) error {
	var err error
	if !run.planRecorded {
		var rec *recordedPlan
		if rec, err = recordPlan(run.plan); err == nil {
			// No Export stream opens before the plan is on disk.
			err = track.recordPlan(rec)
		}
	}
	if err == nil {
		holders := planNodes(run.plan)
		if run.prev != nil {
			// A resume needs nothing more of a node whose units are all done (checkMembers).
			holders = nodesLeft(run.jobs)
		}
		err = exportSession(runCtx, env, prog, opts, run.schema, run.sessID, holders, run.prev != nil, run.jobs, track)
	}
	err = runErr(ctx, runCtx, err)
	if trackErr := track.close(); trackErr != nil {
		switch {
		case err == nil:
			err = exitcode.Exit(exitcode.ExitRuntime, "%w", trackErr)
		case !errors.Is(err, trackErr):
			// The first error leads: what it is (a lost session, a changed schema) decides the hint.
			err = exitcode.Exit(exitcode.ExitRuntime, "%w; %w", err, trackErr)
		}
	}
	var elapsed map[string]time.Duration
	if err == nil {
		elapsed, err = writeManifest(env, opts, run.plan, run.jobs, run.m, track.state.SourceTimezones)
	}
	if err != nil {
		// The persister has stopped: its state can be read.
		failureHint(env.Err, err, run.sessID, track.path, opts.Output, len(track.state.Done), unitCount(run.jobs))
		var exitErr *exitcode.ExitError
		if errors.As(err, &exitErr) {
			return err
		}
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	return finishExport(ctx, env, opts, track.path, run.sessID, run.m, elapsed, run.start, run.prev)
}

// failureHint tells the operator what is left of a failed export and the ways on: resume it, or
// give it up. A session that is gone, or a schema that changed under finished CSV units, leaves
// only the second, unless every unit is done: the manifest is then written without the session.
// done counts the finished units, total all units of the plan.
func failureHint(w io.Writer, err error, sessID, path, outputDir string, done, total int) {
	var lost sessionLostError
	var changed schemaChangedError
	switch {
	case total > 0 && done == total:
		keptHint(w, sessID, path, outputDir)
	case errors.As(err, &lost):
		sessionLostHint(w, sessID, giveUpFiles(path, outputDir))
	case errors.As(err, &changed) && done > 0:
		schemaChangedHint(w, done, sessID, giveUpFiles(path, outputDir))
	default:
		keptHint(w, sessID, path, outputDir)
	}
}

// giveUpFiles names what to delete to give up the export the progress file at path records:
// what is in outputDir, the progress file with it, or the progress file and its plan side file
// too when they are elsewhere (--checkpoint); without outputDir only those two.
func giveUpFiles(path, outputDir string) string {
	switch {
	case outputDir == "":
		return path + " and " + resume.SidePath(path, planSide)
	case config.SameDir(filepath.Dir(path), outputDir):
		return "what is in " + outputDir + " (the progress file " + path + " included)"
	default:
		return path + ", " + resume.SidePath(path, planSide) + " and what is in " + outputDir
	}
}

// unitCount counts the units of jobs.
func unitCount(jobs []*nodeJob) int {
	n := 0
	for _, job := range jobs {
		n += len(job.units)
	}
	return n
}

// nodesLeft names the nodes of jobs with a unit not done yet; never nil, which would mean every
// node to the heartbeat.
func nodesLeft(jobs []*nodeJob) []string {
	nodes := []string{}
	for _, job := range jobs {
		if slices.ContainsFunc(job.units, func(u *plannedUnit) bool { return !u.done }) {
			nodes = append(nodes, job.nodeID)
		}
	}
	return nodes
}

// startOverHint is how to give up the export the progress file at path records and begin again:
// release its session id ("" when none was recorded), delete the progress file and what is in
// outputDir, and export again.
func startOverHint(id, path, outputDir string) string {
	files := "what is in " + outputDir
	if !config.SameDir(filepath.Dir(path), outputDir) {
		files = path + ", " + resume.SidePath(path, planSide) + " and what is in " + outputDir
	}
	if id == "" {
		return "to start over: delete " + files + ", then export again"
	}
	return "to start over: bydbctl data release-session --id " + id + ", then delete " + files + " and export again"
}

// finishExport ends an export whose manifest is written: it releases the session, whose failure
// is only a warning, deletes the progress file and prints the summary. In this order a crash in
// between leaves the progress file, and the same command finishes the export again without the
// session; the other order could leave a session nothing on disk names.
func finishExport(ctx context.Context, env Env, opts config.Export, path, sessID string, m *manifest.Manifest,
	elapsed map[string]time.Duration, start time.Time, prev *resumedRun,
) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionsCallTimeout)
	defer cancel()
	result, err := ReleaseSession(releaseCtx, env.Liaisons.Primary(), sessID)
	switch {
	case err != nil:
		warnReleaseFailed(env.Log, sessID, err)
	case len(result.Failed) > 0:
		for _, f := range result.Failed {
			warnReleaseFailedOn(env.Log, sessID, f.GetNodeId(), f.GetError())
		}
	}
	if err != nil || len(result.Failed) > 0 {
		releaseHint(env.Log, sessID)
	}
	if err = resume.Remove(path, planSide); err != nil {
		warnProgressNotDeleted(env.Log, err, path, resume.SidePath(path, planSide))
	}
	if err = renderSummary(env.Out, opts.Output, m, elapsed, time.Since(start), prev); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "render summary: %w", err)
	}
	return nil
}

// abandonedCreate handles a create whose attempts announced the session ids ids but did not
// complete, interrupted or failed (err is why, what CreateSession returned): the liaison may not
// roll those sessions back, and nothing on disk records all of them, so each is released once, and
// when that fails the operator is told how to. An interrupted export says on log that it waits
// for the release.
func abandonedCreate(ctx context.Context, log io.Writer, conn *grpc.ClientConn, ids []string, err error) error {
	ids = uniqueIDs(ids)
	label, they, was := "session "+ids[0], "it", "was"
	if len(ids) > 1 {
		label, they, was = "sessions "+strings.Join(ids, ", "), "they", "were"
	}
	what, why := "creating export "+label+" failed after "+they+" "+was+" announced", err
	if ctx.Err() != nil {
		what, why = "export interrupted while its "+label+" "+was+" being created", context.Cause(ctx)
		stoppingRelease(log, ids[0], sessionsCallTimeout)
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionsCallTimeout)
	defer cancel()
	var released, hints []string
	var releaseErrs []error
	for _, id := range ids {
		result, releaseErr := ReleaseSession(releaseCtx, conn, id)
		switch {
		case releaseErr == nil && len(result.Failed) == 0:
			released = append(released, result.Released...)
			continue
		case releaseErr == nil:
			releaseErr = fmt.Errorf("it failed on %d data node(s)", len(result.Failed))
		}
		releaseErrs = append(releaseErrs, releaseErr)
		hints = append(hints, "bydbctl data release-session --id "+id)
	}
	if len(releaseErrs) == 0 {
		outcome := "no data node holds it"
		if len(released) > 0 {
			outcome = fmt.Sprintf("it was released on %v", transfer.UniqueSorted(released))
		}
		// Released: the create's own advice to release the announced session no longer applies.
		var announced *announcedError
		if errors.As(why, &announced) {
			why = fmt.Errorf("create export session: %w", announced.err)
		}
		return exitcode.Exit(exitcode.ExitRuntime, "%s: %w; %s", what, why, outcome)
	}
	if ctx.Err() != nil {
		why = fmt.Errorf("%w (%w)", why, err)
	}
	return exitcode.Exit(exitcode.ExitRuntime, "%s: %w; releasing it failed: %w; release it with: %s", what, why, errors.Join(releaseErrs...), strings.Join(hints, "; "))
}

// uniqueIDs is ids without repeats, in the order they came.
func uniqueIDs(ids []string) []string {
	var out []string
	for _, id := range ids {
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// sessionLostError marks the error of an export whose heartbeat found the session gone.
type sessionLostError struct{ error }

func (e sessionLostError) Unwrap() error { return e.error }

// schemaChangedError marks the error of a CSV file a node encoded with another schema than the
// exported one. A resume checks its files against that same schema, so once a unit is done it
// cannot get past the change.
type schemaChangedError struct{ error }

func (e schemaChangedError) Unwrap() error { return e.error }

// exportSession runs the per-node workers under the heartbeat for the units not done yet;
// holders are the data nodes that must hold the session. On a resume (resumed) a node leaves them
// once its worker exported every unit of it, so the heartbeat stops requiring its lease; a fresh
// export requires every holder throughout. The nodes, in id order, are dealt round robin over
// the liaisons. Every finished unit is on disk when it returns nil.
func exportSession(ctx context.Context, env Env, prog *output.Progress, opts config.Export, schema *schemasync.Result,
	sessID string, holders []string, resumed bool, jobs []*nodeJob, track *tracker,
) error {
	var pending []*nodeJob
	for _, job := range jobs {
		left := &nodeJob{nodeID: job.nodeID, dir: job.dir}
		for _, u := range job.units {
			if !u.done {
				left.units = append(left.units, u)
			}
		}
		if len(left.units) > 0 {
			pending = append(pending, left)
		}
	}
	conn := env.Liaisons.Primary()
	l := logger.GetLogger("exporter")
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	var holdersMu sync.Mutex
	holders = slices.Clone(holders)
	holdersNow := func() []string {
		holdersMu.Lock()
		defer holdersMu.Unlock()
		return slices.Clone(holders)
	}
	stopHeartbeat, lost := StartHeartbeat(runCtx, conn, sessID, holdersNow, 0, l, env.Log)
	defer stopHeartbeat()
	run.Go(runCtx, "export-heartbeat-watch", l, func(ctx context.Context) {
		select {
		case lostErr := <-lost:
			cancelRun(exitcode.Exit(exitcode.ExitRuntime, "%w", sessionLostError{lostErr}))
		case <-ctx.Done():
		}
	})

	parallelism, _ := config.ClampParallelism(max(opts.Parallelism, 1), len(pending))
	prog.Printf("exporting %d node(s), parallelism %d, via %d liaison(s)", len(pending), parallelism, len(env.Liaisons.All))
	init := &transferv1.ExportRequest_Init{
		SessionId:             sessID,
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
	ids := make([]string, 0, len(pending))
	for _, job := range pending {
		ids = append(ids, job.nodeID)
	}
	width := nodeWidth(ids)
	prog.Start(runCtx)
	g, gctx := errgroup.WithContext(runCtx)
	g.SetLimit(parallelism)
	for i, job := range pending {
		nodeInit := proto.Clone(init).(*transferv1.ExportRequest_Init)
		nodeInit.TargetNode = job.nodeID
		w := &nodeExport{
			job: job, init: nodeInit, schema: schema, log: env.Log, track: track,
			liaisons: env.Liaisons.All,
			next:     i,
			root:     filepath.Join(opts.Output, nodesDir, job.dir),
			relRoot:  nodesDir + "/" + job.dir,
			retryFor: opts.SendRetryTimeout,
			idleFor:  opts.NodeIdleTimeout,
		}
		g.Go(func() error {
			w.progress = newNodeProgress(prog, job.nodeID, len(job.units), width, init.GetFormat() != transferv1.Format_FORMAT_NATIVE)
			runErr := w.run(gctx)
			w.progress.finish(runErr == nil)
			if runErr == nil && resumed {
				holdersMu.Lock()
				holders = slices.DeleteFunc(holders, func(n string) bool { return n == job.nodeID })
				holdersMu.Unlock()
			}
			return runErr
		})
	}
	err := g.Wait()
	prog.Close()
	if cause := context.Cause(runCtx); err != nil && cause != nil && ctx.Err() == nil && !errors.Is(cause, context.Canceled) {
		// The heartbeat lost the session; the workers only saw the cancellation. A loss after
		// every worker succeeded does not count: every unit is on disk, and the manifest needs
		// no session.
		err = cause
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		// Every finished unit is on disk before the manifest says the export is complete.
		err = track.close()
	}
	return err
}

// writeManifest writes the manifest of an export whose units are all done, from every unit of
// jobs. It returns the time the units took, keyed by elapsedKey.
// The time zones of the source nodes are those recorded (the first run's), and the cluster's for
// a node none is recorded of.
func writeManifest(env Env, opts config.Export, plan *cluster.PlanResult, jobs []*nodeJob, m *manifest.Manifest,
	recordedTZ map[string]string,
) (map[string]time.Duration, error) {
	fillManifest(m, nodeTimezones(env.Cluster, recordedTZ), jobs)
	// The stages are those the manifest records, which a resumed export took from its first run.
	m.Warnings = append(m.Warnings, stageWarnings(manifestStages(m.Groups), plan)...)
	for _, w := range m.Warnings {
		warnManifest(env.Log, w)
	}
	if err := manifest.Write(filepath.Join(opts.Output, manifest.FileName), m); err != nil {
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

// manifestStages maps the name of each group to the names of the stages the manifest records.
func manifestStages(groups []manifest.Group) map[string][]string {
	out := make(map[string][]string, len(groups))
	for _, g := range groups {
		for _, st := range g.Stages {
			out[g.Name] = append(out[g.Name], st.Name)
		}
	}
	return out
}

// buildJobs folds the Plan frames into one job per node, each unit kept as the node
// reported it, and derives each node's directory, refusing two nodes that map to one.
// cluster.CollectPlanFrames has already merged the pieces of a segment inventory split
// across Plan frames, so each unit is kept as reported.
func buildJobs(plan *cluster.PlanResult) ([]*nodeJob, error) {
	byNode := map[string]*nodeJob{}
	for _, f := range plan.Frames {
		job, ok := byNode[f.GetNodeId()]
		if !ok {
			job = &nodeJob{nodeID: f.GetNodeId(), dir: manifest.NodeDir(f.GetNodeId())}
			byNode[f.GetNodeId()] = job
		}
		stage := transfer.NormalizeStage(f.GetStage())
		for _, inv := range f.GetUnits() {
			u := &plannedUnit{stage: stage, segment: inv.GetSegment(), property: inv.GetProperty()}
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

// catalogLabel is the manifest spelling of a catalog (STREAM, MEASURE, TRACE, PROPERTY): the
// upper-cased selector name that the importer parses back with transfer.CatalogByName.
func catalogLabel(c commonv1.Catalog) string {
	return strings.ToUpper(transfer.CatalogName(c))
}

// schemaSkippedWarning is the manifest warning of an export without the schema.
const schemaSkippedWarning = "the schema was not exported (--include-schema=false), so groups[].indexMode is unknown and recorded as false"

// CheckFormat checks the settings a format takes: a native artifact is never compressed, so an
// unspecified compression becomes none and gzip is refused, and its volume limit must leave
// room for a container. Any format but csv and native is refused.
func CheckFormat(opts *config.Export) error {
	switch opts.Format {
	case transferv1.Format_FORMAT_CSV:
		return nil
	case transferv1.Format_FORMAT_NATIVE:
	default:
		return exitcode.Exit(exitcode.ExitUsage, "unsupported artifact format %s", opts.Format)
	}
	switch opts.Compress {
	case transferv1.Compress_COMPRESS_UNSPECIFIED:
		opts.Compress = transferv1.Compress_COMPRESS_NONE
	case transferv1.Compress_COMPRESS_NONE:
	default:
		return exitcode.Exit(exitcode.ExitUsage, "a native export is not compressed: its column files already are; use --compress none")
	}
	if bnc.CheckFileSize(opts.MaxFileSize) != nil {
		return exitcode.Exit(exitcode.ExitUsage, "a native export needs --max-file-size 0 or at least %d bytes, got %d", bnc.MinFileSize, opts.MaxFileSize)
	}
	return nil
}

// formatLabel is the manifest spelling of an artifact format: csv or native.
func formatLabel(f transferv1.Format) string {
	if f == transferv1.Format_FORMAT_NATIVE {
		return manifest.FormatNative
	}
	return manifest.FormatCSV
}

// baseManifest is the manifest without units and nodes: format, schema files and groups. The
// groups come from the group list; their index mode from the exported measures, or it is
// unknown when schema is nil, which a manifest warning says.
func baseManifest(format transferv1.Format, compress transferv1.Compress, groups []*commonv1.Group, schema *schemasync.Result) *manifest.Manifest {
	m := &manifest.Manifest{
		ArtifactVersion: manifest.ArtifactVersion,
		Format:          formatLabel(format),
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

// compressLabel is the manifest spelling of a compression. The option default is gzip and
// CheckFormat turns an unspecified native compression into none; an UNSPECIFIED left here is
// labeled gzip.
func compressLabel(c transferv1.Compress) string {
	if c == transferv1.Compress_COMPRESS_NONE {
		return manifest.CompressNone
	}
	return manifest.CompressGzip
}

// nodeTimezones maps every data node of info to its time zone, overridden by recorded: a node may
// have left the cluster since the first run of a resumed export, or be restarting.
func nodeTimezones(info *cluster.Info, recorded map[string]string) map[string]string {
	tz := map[string]string{}
	if info != nil {
		for _, n := range info.DataNodes {
			tz[n.GetMetadata().GetName()] = n.GetTzName()
		}
	}
	for node, zone := range recorded {
		tz[node] = zone
	}
	return tz
}

// fillManifest adds the source nodes, with their time zones from tz, the units and the unit
// warnings. Segment
// units are keyed by (catalog, group, stage, segment) with one source per node. A property
// unit's key degrades to (catalog, group): every node's shards of the group land in one unit,
// whose stage is the stage of its first source (jobs are in node order). Shards come from the
// Plan statistics; unitSource says where their totalCount and rowCounts come from.
func fillManifest(m *manifest.Manifest, tz map[string]string, jobs []*nodeJob) {
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
			src := unitSource(job.nodeID, u, m.Format == manifest.FormatNative)
			for _, sh := range src.Shards {
				transfer.MergeRange(&mu.MinTimestamp, &mu.MaxTimestamp, sh.MinTimestamp, sh.MaxTimestamp)
			}
			// The rows of an index-mode group's series index; 0 for every other unit.
			sl := u.segment.GetSegmentLevel()
			transfer.MergeRange(&mu.MinTimestamp, &mu.MaxTimestamp, sl.GetMinTimestamp(), sl.GetMaxTimestamp())
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

// unitSource is one node's contribution to a unit. The row counts of a CSV unit come from its
// files; a native file counts no rows, so a native unit takes them from the Plan statistics:
// a shard's total count is its Plan aggregate, a property shard's its document count, and
// the segment-level count that of an index-mode series index. The last two are document
// counts, upper bounds of the rows.
func unitSource(nodeID string, u *plannedUnit, native bool) manifest.UnitSource {
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
		if native {
			s.TotalCount = st.GetTotalCount()
		}
	}
	if native {
		src.TotalCount = u.segment.GetSegmentLevel().GetDocCount()
	}
	for _, st := range u.property.GetShards() {
		s := shard(st.GetShardId())
		s.DocCount = st.GetDocCount()
		if native {
			s.TotalCount = st.GetDocCount()
		}
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
