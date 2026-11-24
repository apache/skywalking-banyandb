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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// loadProgress reads and checks the progress file at path against this invocation and
// restores its plan into jobs (nil while no plan is recorded). Anything unusable exits
// exitcode.ExitResume.
func loadProgress(path string, opts config.Export) (*exportProgress, *cluster.PlanResult, []*nodeJob, error) {
	state := &exportProgress{}
	if err := resume.Read(path, progressKind, progressVersion, state); err != nil {
		if version := new(resume.VersionError); errors.As(err, &version) {
			// The session id is there when the file decodes at all.
			return nil, nil, nil, exitcode.Exit(exitcode.ExitResume, "an earlier export left the progress file %s, but another version of bydbctl wrote it "+
				"(progress file version %d, this bydbctl reads version %d), so it cannot be resumed; nothing was deleted. "+
				"Resume it with the bydbctl version that wrote it; or %s", path, version.Found, version.Want, giveUpHint(state, path, opts))
		}
		// What the file records cannot be read, the session id included: name only the files.
		return nil, nil, nil, exitcode.Exit(exitcode.ExitResume, "%w; nothing was deleted. To start over: if it records an export session, "+
			"release it with: bydbctl data release-session --id <id>; then delete %s and export again", err, giveUpFiles(path, opts.Output))
	}
	over := startOverHint(state.Session.ID, path, outputOf(opts, state))
	rec, err := readPlan(path, state)
	var plan *cluster.PlanResult
	if err == nil && rec != nil {
		plan, err = rec.result()
	}
	if err != nil {
		return nil, nil, nil, exitcode.Exit(exitcode.ExitResume, "the progress file %s is unusable: %w; nothing was deleted. %s", path, err, over)
	}
	if diff := checkBindings(state, opts, path); diff != "" {
		return nil, nil, nil, exitcode.Exit(exitcode.ExitResume,
			"the progress file %s records an export this command differs from; nothing was deleted. It differs in:\n%s"+
				"run the first export's command again to resume it; %s", path, diff, over)
	}
	if state.Session.ID == "" {
		return nil, nil, nil, exitcode.Exit(exitcode.ExitResume, "the progress file %s records no export session; nothing was deleted. %s", path, over)
	}
	if plan == nil {
		if len(state.Done) > 0 {
			// The plan is recorded before any unit is exported: without it the finished units
			// would be exported again over their own files.
			return nil, nil, nil, exitcode.Exit(exitcode.ExitResume,
				"the progress file %s is unusable: it records finished units but no plan; nothing was deleted. %s", path, over)
		}
		return state, nil, nil, nil
	}
	jobs, err := buildJobs(plan)
	if err == nil {
		if err = markDone(jobs, state.Done); err == nil {
			return state, plan, jobs, nil
		}
	}
	return nil, nil, nil, exitcode.Exit(exitcode.ExitResume, "the progress file %s is unusable: %w; nothing was deleted. %s", path, err, over)
}

// outputOf is the output directory of a run over the progress file state: the given one, or
// without it (a dry run that names only the progress file) the recorded one.
func outputOf(opts config.Export, state *exportProgress) string {
	if opts.Output != "" {
		return opts.Output
	}
	return state.OutputDir
}

// giveUpHint is how to give up the export a progress file records that this bydbctl cannot read:
// with the session id when the file holds one, else the general form.
func giveUpHint(state *exportProgress, path string, opts config.Export) string {
	output := outputOf(opts, state)
	if state.Session.ID == "" {
		return "to give up: if it records an export session, release it with: bydbctl data release-session --id <id>; then delete " +
			giveUpFiles(path, output) + " and export again, or export into a new output directory"
	}
	return startOverHint(state.Session.ID, path, output) + ", or export into a new output directory once the session is released"
}

// checkMembers refuses (exitcode.ExitResume) a resume while a data node that still has units to
// export is not a data node of the cluster; without a recorded plan (jobs nil) every source node
// recorded counts. A node whose units are all done, or one the cluster gained, does not count.
// over ends the refusal with how to start over.
func checkMembers(state *exportProgress, jobs []*nodeJob, info *cluster.Info, over string) error {
	needed := state.Bindings.SourceNodes
	if jobs != nil {
		needed = nil
		for _, job := range jobs {
			if slices.ContainsFunc(job.units, func(u *plannedUnit) bool { return !u.done }) {
				needed = append(needed, job.nodeID)
			}
		}
	}
	missing := resume.Missing(needed, info.DataNodeNames())
	if len(missing) == 0 {
		return nil
	}
	return exitcode.Exit(exitcode.ExitResume, "data node(s) %s still have units of this export to export but are not registered in the cluster now; "+
		"nothing was deleted. If a node is restarting, run the same command again later; only if it is gone for good, give the export up: %s",
		strings.Join(missing, ", "), over)
}

// DryRunResume is `data export --dry-run` over the output directory of an interrupted export:
// it runs the checks a resume makes without the cluster (the bindings, the files of the
// finished units, the schema they were exported with), prints the resume summary to env.Log
// and the plan of what is left to env.Out, and reports whether a progress file was there. It
// does not take the lock, so it reads an export that is running too, and it writes and deletes
// nothing. The export session is not contacted, which a NOTE says.
func DryRunResume(env Env, opts config.Export) (bool, error) {
	path := ProgressPath(opts)
	if path == "" || !resume.Exists(path) {
		return false, nil
	}
	if err := CheckFormat(&opts); err != nil {
		return true, err
	}
	state, plan, jobs, err := loadProgress(path, opts)
	if err != nil {
		return true, err
	}
	done := len(state.Done)
	sum := summarize(path, state, jobs)
	rep := leftReport(env, plan, jobs)
	sum.LeftShards = leftShards(jobs)
	if err = sum.Render(env.Log); err != nil {
		return true, exitcode.Exit(exitcode.ExitRuntime, "render the resume summary: %w", err)
	}
	if opts.Output == "" {
		summaryOutputDir(env.Log, state.OutputDir)
	}
	if _, _, err = localChecks(env, opts, path, state, jobs, done == sum.Total && done > 0); err != nil {
		return true, err
	}
	rebindMaxFileSize(env.Log, state, opts, sum.Total-done)
	switch {
	case done > 0 && done == sum.Total:
		noteManifestOnly(env.Log, state.Session.ID)
	case plan == nil:
		noteSessionNotContacted(env.Log, state.Session.ID)
		planReadOnResume(env.Log)
	default:
		noteSessionNotContacted(env.Log, state.Session.ID)
	}
	if plan == nil {
		// No units are known: the report has none.
		rep = BuildReport(env.Cluster, &cluster.PlanResult{})
	}
	rep.Resumed = &ResumedProgress{Path: path, Done: done, Total: sum.Total}
	if (done > 0 && done == sum.Total) || plan == nil {
		// Nothing is left to list: a table would say nothing the notes did not, while json and
		// yaml must still print a document for whoever parses them.
		if opts.OutputFormat != "json" && opts.OutputFormat != "yaml" {
			return true, nil
		}
	}
	if err = Render(env.Out, opts.OutputFormat, rep); err != nil {
		return true, exitcode.Exit(exitcode.ExitRuntime, "render report: %w", err)
	}
	return true, nil
}

// leftShards counts the shards of the units of jobs not done yet, each shard of a group once
// however many nodes hold a replica of it; jobs nil (no plan recorded) counts none.
func leftShards(jobs []*nodeJob) int {
	seen := map[string]struct{}{}
	for _, job := range jobs {
		for _, u := range job.units {
			if u.done {
				continue
			}
			for _, id := range u.shardIDs() {
				seen[fmt.Sprintf("%s/%s/%d", transfer.CatalogName(u.catalog()), u.group(), id)] = struct{}{}
			}
		}
	}
	return len(seen)
}

// leftReport is the plan of the units not done, from the plan the progress file records; nil
// when no plan is recorded yet. It needs neither the cluster nor the export session.
func leftReport(env Env, plan *cluster.PlanResult, jobs []*nodeJob) *Report {
	if plan == nil {
		return nil
	}
	left := &cluster.PlanResult{AnsweredNodes: plan.AnsweredNodes}
	for _, job := range jobs {
		for _, u := range job.units {
			if u.done {
				continue
			}
			inv := &transferv1.UnitInventory{}
			if u.property != nil {
				inv.Kind = &transferv1.UnitInventory_Property{Property: u.property}
			} else {
				inv.Kind = &transferv1.UnitInventory_Segment{Segment: u.segment}
			}
			left.Frames = append(left.Frames, &transferv1.UnitFrame{NodeId: job.nodeID, Stage: u.stage, Units: []*transferv1.UnitInventory{inv}})
		}
	}
	return BuildReport(env.Cluster, left)
}

// localChecks makes the checks of a resume that need neither the cluster nor the session: the
// files of the finished units, the schema they were exported with (returned, nil when none was
// recorded or none is needed) and, unless allDone, that the data nodes with units left are still
// in the cluster. A refusal exits exitcode.ExitResume.
func localChecks(env Env, opts config.Export, path string, state *exportProgress, jobs []*nodeJob, allDone bool) (*schemasync.Result, *nodesTree, error) {
	dir := outputOf(opts, state)
	over := startOverHint(state.Session.ID, path, dir)
	if err := checkDoneFiles(dir, state.Done, over); err != nil {
		return nil, nil, err
	}
	// What the cleanup would refuse, refused before the question and before the session is read.
	tree, err := walkNodes(dir, over)
	if err != nil {
		return nil, nil, err
	}
	var schema *schemasync.Result
	if opts.IncludeSchema && len(state.Done) > 0 {
		if schema, err = recordedSchema(dir, state, over); err != nil {
			return nil, nil, err
		}
	}
	if !allDone {
		if err = checkMembers(state, jobs, env.Cluster, over); err != nil {
			return nil, nil, err
		}
	}
	return schema, tree, nil
}

// resumeExport continues the export recorded in the progress file at path: bindings and
// summary; the local checks (localChecks); the confirmation; with finished units the live schema
// (checkLiveSchema); a Plan{read} of the session compared with the recorded plan; the
// cleanup of every file no finished unit lists; the schema; and then the units left. Every
// refusal (exit 6) comes before anything is deleted. An export whose units are all done only
// lacks its manifest: it is written without the session, which may be gone by now.
func resumeExport(ctx context.Context, env Env, prog *output.Progress, opts config.Export, path string) error {
	state, plan, jobs, err := loadProgress(path, opts)
	if err != nil {
		return err
	}
	done := len(state.Done)
	sessID := state.Session.ID
	over := startOverHint(sessID, path, opts.Output)
	sum := summarize(path, state, jobs)
	rep := leftReport(env, plan, jobs)
	sum.LeftShards = leftShards(jobs)
	if err = sum.Render(env.Log); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "render the resume summary: %w", err)
	}
	if rep != nil && len(rep.Rows) > 0 {
		rep.Resumed = &ResumedProgress{Path: path, Done: done, Total: sum.Total}
		// One Write, as the terminal's progress writer would overwrite a tabwriter's cells.
		var buf bytes.Buffer
		buf.WriteByte('\n')
		if err = renderTable(&buf, rep); err != nil {
			return exitcode.Exit(exitcode.ExitRuntime, "render what is left: %w", err)
		}
		_, _ = env.Log.Write(buf.Bytes())
	}
	allDone := done > 0 && done == sum.Total
	if opts.Preempt {
		notePreemptIgnored(env.Log, sessID)
	}
	// The local checks before the question: they only read, so a refusal neither asks first, nor
	// renews the session's lease, nor deletes anything.
	// tree is what the cleanup deletes from: nodes/ is walked once.
	schema, tree, err := localChecks(env, opts, path, state, jobs, allDone)
	if err != nil {
		return err
	}
	rebindMaxFileSize(env.Log, state, opts, sum.Total-done)
	if err = resume.ConfirmResume(ctx, opts.Resume, env.Confirm, path, over); err != nil {
		return err
	}
	// This run starts after the question: the time spent on it is not the export's.
	start := time.Now()
	prev := &resumedRun{units: done, prior: time.Duration(state.ElapsedMs) * time.Millisecond, startedAt: state.StartedAt}
	// A failure from here on is worth another try: tell how, and how to give up instead.
	retryable := func(err error) error {
		if exitcode.For(err) != exitcode.ExitResume {
			keptHint(env.Err, sessID, path, opts.Output)
		}
		return err
	}
	if allDone {
		return finishResumed(ctx, env, opts, path, state, schema, tree, plan, jobs, start, prev)
	}
	if schema != nil {
		// Before the session is read, which renews its lease, and before anything is deleted.
		if err = checkLiveSchema(ctx, env, opts, state, schema, jobs, over); err != nil {
			return retryable(err)
		}
	}
	if opts.IncludeSchema && len(state.Done) == 0 {
		// The schema is written again below, of the recorded groups the cluster still has.
		if err = checkGroupsExist(env, opts, state, over); err != nil {
			return err
		}
	}
	// Only the nodes with units left must still hold the session, as only they must still be
	// members of the cluster (checkMembers).
	holders := state.Bindings.SourceNodes
	if plan != nil {
		holders = nodesLeft(jobs)
	}
	current := plan
	if plan == nil || len(holders) > 0 {
		// A plan without a unit left needs no node to hold the session: an empty required_nodes
		// would mean every node to the liaison.
		if current, err = readSession(ctx, env.Liaisons.Primary(), opts.Selectors, sessID, holders, over); err != nil {
			return retryable(err)
		}
	}
	if plan == nil {
		plan = current
		if jobs, err = buildJobs(plan); err != nil {
			return exitcode.Exit(exitcode.ExitResume, "the inventory of export session %s cannot be exported: %w; nothing was deleted. %s", sessID, err, over)
		}
	} else if diff := diffPlans(plan, current, doneKeys(state.Done)); diff != "" {
		return exitcode.Exit(exitcode.ExitResume, "the inventory of export session %s differs from the recorded plan; nothing was deleted:\n%s%s",
			sessID, diff, over)
	}
	if err = cleanTree(opts.Output, path, state.Done, tree); err != nil {
		if exitcode.For(err) == exitcode.ExitResume {
			return err
		}
		return retryable(exitcode.Exit(exitcode.ExitRuntime, "clean up the unfinished units: %w", err))
	}
	if opts.IncludeSchema && len(state.Done) == 0 {
		// No unit was checked against the schema yet: write it again, so a schema changed since
		// the first run is the one the units are checked against now.
		if schema, err = rewriteSchema(ctx, env, opts, state); err != nil {
			return retryable(err)
		}
	}
	m := resumedManifest(opts, state, schema)
	prog.Printf("resuming export session %s: %d unit(s) done, %d left", sessID, done, unitsLeft(jobs))
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	planRecorded := state.PlanDigest != ""
	// What the earlier runs had in flight is theirs: this run records its own.
	state.Inflight = map[string]inflightUnit{}
	track := startTracker(ctx, path, state, start, cancelRun)
	defer func() { _ = track.close() }()
	return runSession(ctx, runCtx, env, prog, opts, track, sessionRun{
		schema: schema, sessID: sessID, plan: plan, planRecorded: planRecorded, jobs: jobs, m: m, start: start, prev: prev,
	})
}

// resumedRun is what a resumed export carries over from the runs before it.
type resumedRun struct {
	// startedAt is when the first run started, the export time of the manifest.
	startedAt time.Time
	// prior is the time the earlier runs took, up to their last write of the progress file.
	prior time.Duration
	// units counts the units they finished.
	units int
}

// resumedManifest is the base manifest of a resumed export: the groups the progress file
// records and the time the first run started as the export time.
func resumedManifest(opts config.Export, state *exportProgress, schema *schemasync.Result) *manifest.Manifest {
	m := baseManifest(opts.Format, opts.Compress, nil, schema)
	m.Groups = state.Groups
	m.ExportedAt = state.StartedAt.UTC().Format(time.RFC3339)
	return m
}

// checkLiveSchema compares the schema the registries hold now with the one the finished units
// were exported with (schema), for an export with finished units: a difference exits
// exitcode.ExitResume, nothing deleted; over ends the refusal with how to start over.
//
// A native export binds no file to the schema, so nothing would catch a change later: every
// resource of the recorded groups is compared. A resource changed or removed since refuses; one
// added does not, since the units left come from the session's snapshot taken in the first run
// and the exported schema, which lacks it, still describes them.
//
// A CSV export checks the mod revisions a node encoded each file with against the exported
// schema, and a changed schema fails the first unit left, after the session was read and the
// unfinished files deleted. The same check is made here first, on the resources the CSV files
// of the units left bind to (schemasync.DataKinds of their groups): changed, removed and added
// ones alike, since a node encodes with every resource of those kinds in the group.
func checkLiveSchema(ctx context.Context, env Env, opts config.Export, state *exportProgress, schema *schemasync.Result,
	jobs []*nodeJob, over string,
) error {
	groups := recordedGroups(env, opts, state)
	native := opts.Format == transferv1.Format_FORMAT_NATIVE
	var left []string
	if !native {
		// The groups with units left as recorded, not as the cluster has them now: one deleted
		// since is a removed resource, not one to leave out.
		left = recordedGroupsLeft(state, jobs)
		if len(left) == 0 {
			return nil
		}
		groups = slices.DeleteFunc(groups, func(g *commonv1.Group) bool { return !slices.Contains(left, g.GetMetadata().GetName()) })
	}
	live, err := schemasync.Live(ctx, env.Liaisons.Primary(), groups)
	if err != nil {
		return exitcode.Exit(resumedCode(err), "read the schema to compare it with the one the finished units were exported with: %w", err)
	}
	recorded := schema.Fingerprints()
	if native {
		var added []string
		live, added = schemasync.Restrict(live, recorded)
		if len(added) > 0 {
			noteResourcesAdded(env.Log, added)
		}
	} else {
		recorded = schemasync.Select(recorded, schemasync.DataKinds, left)
		live = schemasync.Select(live, schemasync.DataKinds, left)
	}
	if diff := schemasync.DiffFingerprints(recorded, live); len(diff) > 0 {
		return exitcode.Exit(exitcode.ExitResume, "the schema changed after %d unit(s) were exported with the first one, so this export cannot be resumed; "+
			"nothing was deleted:\n  %s\n%s", len(state.Done), strings.Join(diff, "\n  "), over)
	}
	return nil
}

// recordedGroupsLeft names the groups the progress file records with a unit of jobs not done
// yet; all of them when no plan is recorded (jobs nil).
func recordedGroupsLeft(state *exportProgress, jobs []*nodeJob) []string {
	left := map[string]bool{}
	for _, job := range jobs {
		for _, u := range job.units {
			if !u.done {
				left[catalogLabel(u.catalog())+"\x00"+u.group()] = true
			}
		}
	}
	var out []string
	for _, g := range state.Groups {
		if (jobs == nil || left[g.Catalog+"\x00"+g.Name]) && !slices.Contains(out, g.Name) {
			out = append(out, g.Name)
		}
	}
	return out
}

// resumedCode is the exit code of a server error in a resumed export: a refusal the server made
// is no preflight rejection there (exit 2 says nothing was exported), the earlier runs having
// exported units, so it fails at run time like any other error, and the same command can run
// again.
func resumedCode(err error) int {
	if code := exitcode.For(err); code != exitcode.ExitPreflight {
		return code
	}
	return exitcode.ExitRuntime
}

// recordedGroups are the selected groups of the cluster that the progress file records.
func recordedGroups(env Env, opts config.Export, state *exportProgress) []*commonv1.Group {
	recorded := map[string]bool{}
	for _, g := range state.Groups {
		recorded[g.Catalog+"\x00"+g.Name] = true
	}
	var groups []*commonv1.Group
	for _, g := range SelectedGroups(env.Groups, opts.Selectors) {
		if recorded[catalogLabel(g.GetCatalog())+"\x00"+g.GetMetadata().GetName()] {
			groups = append(groups, g)
		}
	}
	return groups
}

// unitsLeft counts the units of jobs not done yet.
func unitsLeft(jobs []*nodeJob) int {
	n := 0
	for _, job := range jobs {
		for _, u := range job.units {
			if !u.done {
				n++
			}
		}
	}
	return n
}

// finishResumed completes an export whose units were all done when it stopped: it writes the
// manifest from the progress file, releases the session, if it is still held, and deletes the
// progress file.
func finishResumed(ctx context.Context, env Env, opts config.Export, path string, state *exportProgress, schema *schemasync.Result,
	tree *nodesTree, plan *cluster.PlanResult, jobs []*nodeJob, start time.Time, prev *resumedRun,
) error {
	if err := cleanTree(opts.Output, path, state.Done, tree); err != nil {
		if exitcode.For(err) == exitcode.ExitResume {
			return err
		}
		keptHint(env.Err, state.Session.ID, path, opts.Output)
		return exitcode.Exit(exitcode.ExitRuntime, "clean up the output directory: %w", err)
	}
	m := resumedManifest(opts, state, schema)
	writingManifestOnly(env.Log, state.Session.ID, prev.units)
	elapsed, err := writeManifest(env, opts, plan, jobs, m, state.SourceTimezones)
	if err != nil {
		keptHint(env.Err, state.Session.ID, path, opts.Output)
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	return finishExport(ctx, env, opts, path, state.Session.ID, m, elapsed, start, prev)
}

// recordedSchema loads the schema the first run wrote and checks that its files are those the
// progress file records: the finished units were checked against that schema. A difference
// exits exitcode.ExitResume; over ends the refusal with how to start over.
func recordedSchema(dir string, state *exportProgress, over string) (*schemasync.Result, error) {
	schemaDir := filepath.Join(dir, schemasync.Dir)
	schema, err := schemasync.Load(dir)
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitResume, "the schema under %s, which the finished units were exported with, is unusable: %w; nothing was deleted. %s",
			schemaDir, err, over)
	}
	if diff := diffSchemaFiles(schema, state); diff != "" {
		return nil, exitcode.Exit(exitcode.ExitResume, "the files under %s, which the finished units were exported with, no longer match the progress file; "+
			"nothing was deleted:\n%s%s", schemaDir, diff, over)
	}
	return schema, nil
}

// checkGroupsExist refuses (exitcode.ExitResume) a resume while a group the progress file records
// is not a group of the cluster now: the schema written again would lack it, and the manifest
// would list units of the session's snapshot from a group it does not have. over ends the refusal
// with how to start over.
func checkGroupsExist(env Env, opts config.Export, state *exportProgress, over string) error {
	now := map[string]bool{}
	for _, g := range recordedGroups(env, opts, state) {
		now[catalogLabel(g.GetCatalog())+"\x00"+g.GetMetadata().GetName()] = true
	}
	var gone []string
	for _, g := range state.Groups {
		if !now[g.Catalog+"\x00"+g.Name] {
			gone = append(gone, strings.ToLower(g.Catalog)+" "+g.Name)
		}
	}
	if len(gone) == 0 {
		return nil
	}
	return exitcode.Exit(exitcode.ExitResume, "group(s) %s the first run recorded no longer exist, so this export cannot be resumed; nothing was deleted. %s",
		strings.Join(gone, ", "), over)
}

// rewriteSchema writes the schema of the groups the progress file records again and records its
// files and groups in state; every one of them exists (checkGroupsExist).
func rewriteSchema(ctx context.Context, env Env, opts config.Export, state *exportProgress) (*schemasync.Result, error) {
	groups := recordedGroups(env, opts, state)
	if err := os.RemoveAll(filepath.Join(opts.Output, schemasync.Dir)); err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "remove the schema: %w", err)
	}
	schema, err := schemasync.Export(ctx, env.Liaisons.Primary(), groups, opts.Output)
	if err != nil {
		return nil, exitcode.Exit(resumedCode(err), "export schema: %w", err)
	}
	m := baseManifest(opts.Format, opts.Compress, groups, schema)
	state.SchemaFiles, state.Groups = m.SchemaFiles, m.Groups
	return schema, nil
}

// diffSchemaFiles names every schema file whose path, size or CRC-32 differs from what the
// progress file records, one per line, "" when none does.
func diffSchemaFiles(schema *schemasync.Result, state *exportProgress) string {
	now := map[string]manifest.File{}
	for _, f := range baseManifest(transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE, nil, schema).SchemaFiles {
		now[f.Path] = f
	}
	var lines []string
	for _, r := range state.SchemaFiles {
		f, ok := now[r.Path]
		switch {
		case !ok:
			lines = append(lines, fmt.Sprintf("  %s: missing", r.Path))
		case f.Bytes != r.Bytes || f.CRC32 != r.CRC32:
			lines = append(lines, fmt.Sprintf("  %s: recorded %d bytes crc32 %s, now %d bytes crc32 %s", r.Path, r.Bytes, r.CRC32, f.Bytes, f.CRC32))
		}
		delete(now, r.Path)
	}
	for p := range now {
		lines = append(lines, fmt.Sprintf("  %s: not recorded", p))
	}
	if len(lines) == 0 {
		return ""
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

// readSession runs Plan{read{id}}, which renews the session's lease on every node. holders are
// the data nodes that must still hold the session, sent as required_nodes; the liaison asks every
// data node of the cluster, and one it gained since, which never held the session, or one whose
// lease expired but which has no unit left, is reported unreachable: that does not count. A
// session that is gone, expired or incomplete on a holder exits exitcode.ExitResume: an export
// against a new point in time cannot be mixed with this one. over ends a refusal with how to
// start over.
func readSession(ctx context.Context, conn *grpc.ClientConn, selectors []*transferv1.Selector, id string, holders []string, over string,
) (*cluster.PlanResult, error) {
	gone := func(why string) error {
		return exitcode.Exit(exitcode.ExitResume, "export session %s cannot be resumed: %s; nothing was deleted. %s", id, why, over)
	}
	stream, err := transferv1.NewExportServiceClient(conn).Plan(ctx, &transferv1.PlanRequest{
		Selectors: selectors,
		Session:   &transferv1.PlanRequest_Read{Read: &transferv1.ReadSession{Id: id, RequiredNodes: holders}},
	})
	var result *cluster.PlanResult
	if err == nil {
		result, err = cluster.CollectPlanFrames(stream)
	}
	if err != nil {
		if code := status.Code(err); code == codes.NotFound || code == codes.FailedPrecondition {
			return nil, gone(err.Error())
		}
		if ctx.Err() != nil {
			return nil, exitcode.Exit(exitcode.ExitRuntime, "resume interrupted while reading export session %s: %w", id, context.Cause(ctx))
		}
		return nil, exitcode.Exit(exitcode.ExitRuntime, "read export session %s: %w", id, cluster.Unsupported(err))
	}
	var unread []string
	for _, n := range result.UnreachableNodes {
		if slices.Contains(holders, n) {
			unread = append(unread, n)
		}
	}
	if len(unread) > 0 {
		// The liaison reports a node that no longer holds the session like one it cannot reach.
		return nil, exitcode.Exit(exitcode.ExitResume, "export session %s could not be read on data node(s) %v: they no longer hold it or cannot be reached; "+
			"nothing was deleted. If a node is restarting or unreachable, run the same command again later; only if the session is lost, %s",
			id, unread, over)
	}
	return result, nil
}

// diffPlans compares two inventories of a session: the same nodes, units, shards and part
// statistics. A recorded unit in done (doneKeys) may be missing now: it is not read again. It
// returns one line per difference, "" when they are equal.
func diffPlans(recorded, current *cluster.PlanResult, done map[string]bool) string {
	a, b := inventoryOf(recorded), inventoryOf(current)
	var lines []string
	for k, v := range a {
		w, ok := b[k]
		switch {
		case !ok && done[k]:
		case !ok:
			lines = append(lines, "  "+k+": no longer in the session")
		case !proto.Equal(v, w):
			lines = append(lines, "  "+k+": its shards or parts differ")
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			lines = append(lines, "  "+k+": not in the recorded plan")
		}
	}
	if len(lines) == 0 {
		return ""
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

// doneKeys keys the finished units like inventoryOf.
func doneKeys(done []doneUnit) map[string]bool {
	out := make(map[string]bool, len(done))
	for _, d := range done {
		out["node "+d.Node+" unit "+d.Unit] = true
	}
	return out
}

// inventoryOf keys every unit of a plan by node and unit, each with its parts sorted so that
// the order the pieces of a shard arrived in does not count.
func inventoryOf(plan *cluster.PlanResult) map[string]*transferv1.UnitInventory {
	out := map[string]*transferv1.UnitInventory{}
	for _, f := range plan.Frames {
		stage := transfer.NormalizeStage(f.GetStage())
		for _, inv := range f.GetUnits() {
			u := &plannedUnit{stage: stage, segment: inv.GetSegment(), property: inv.GetProperty()}
			c := proto.Clone(inv).(*transferv1.UnitInventory)
			for _, s := range c.GetSegment().GetShards() {
				slices.SortFunc(s.Parts, func(x, y *transferv1.PartStat) int { return cmp.Compare(x.GetId(), y.GetId()) })
			}
			out["node "+f.GetNodeId()+" unit "+u.key()] = c
		}
	}
	return out
}

// checkDoneFiles checks that every file of a finished unit is still there with its size; its
// CRC-32 was checked when it was written and is not read again. Neither nodes/ nor any directory
// below it on the way to a file, nor the file, may be a symlink: the cleanup of a resume walks
// nodes/ without following links, so the files behind one would drop out of the export. over
// ends a refusal with how to start over.
func checkDoneFiles(dir string, done []doneUnit, over string) error {
	checked := map[string]bool{}
	// notLink refuses p as a symlink; what it found of p is returned unless p was checked before
	// or cannot be read.
	notLink := func(p string) (os.FileInfo, error) {
		if checked[p] {
			return nil, nil
		}
		info, err := os.Lstat(p)
		if err != nil {
			info = nil
		} else if info.Mode()&fs.ModeSymlink != 0 {
			return nil, exitcode.Exit(exitcode.ExitResume, "%s is a symlink, which a resume does not follow: move what it points to in its place; nothing was deleted. %s",
				p, over)
		}
		checked[p] = true
		return info, nil
	}
	root := filepath.Join(dir, nodesDir)
	if _, err := notLink(root); err != nil {
		return err
	}
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			if _, err = notLink(filepath.Join(root, e.Name())); err != nil {
				return err
			}
		}
	}
	for _, d := range done {
		for _, f := range d.Files {
			if !strings.HasPrefix(f.Path, nodesDir+"/") || strings.Contains(f.Path, `\`) || !filepath.IsLocal(filepath.FromSlash(f.Path)) {
				return exitcode.Exit(exitcode.ExitResume, "the progress file lists %q, which is not a file below %s/; nothing was deleted. %s", f.Path, nodesDir, over)
			}
			p := filepath.Join(dir, filepath.FromSlash(f.Path))
			for sub := filepath.Dir(p); len(sub) > len(root); sub = filepath.Dir(sub) {
				if _, err := notLink(sub); err != nil {
					return err
				}
			}
			// Not a symlink: what Lstat found is what Stat finds.
			info, err := notLink(p)
			if err != nil {
				return err
			}
			if info == nil {
				if info, err = os.Stat(p); err != nil {
					return exitcode.Exit(exitcode.ExitResume, "finished unit %s of node %s: %v; nothing was deleted. %s", d.Unit, d.Node, err, over)
				}
			}
			if uint64(info.Size()) != f.Bytes {
				return exitcode.Exit(exitcode.ExitResume, "finished unit %s of node %s: %s has %d bytes, %d were written; nothing was deleted. %s",
					d.Unit, d.Node, f.Path, info.Size(), f.Bytes, over)
			}
		}
	}
	return nil
}

// nodesTree is what walkNodes found below dir/nodes: every directory and every regular file.
type nodesTree struct {
	dirs, files []string
}

// walkNodes walks dir/nodes without following links and returns every regular file and directory
// below it. Anything else (a symlink) refuses with exitcode.ExitResume, which over ends with how
// to start over; a missing dir/nodes is walked as empty.
func walkNodes(dir, over string) (*nodesTree, error) {
	tree := &nodesTree{}
	root := filepath.Join(dir, nodesDir)
	err := filepath.WalkDir(root, func(p string, e fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) && p == root {
				return filepath.SkipDir
			}
			return exitcode.Exit(exitcode.ExitResume, "walk %s: %w; nothing was deleted. %s", p, walkErr, over)
		}
		if !e.IsDir() && !e.Type().IsRegular() {
			return exitcode.Exit(exitcode.ExitResume, "%s is not a regular file or directory (%s), which a resume does not delete nor follow; nothing was deleted. %s",
				p, e.Type(), over)
		}
		if e.IsDir() {
			tree.dirs = append(tree.dirs, p)
		} else {
			tree.files = append(tree.files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tree, nil
}

// cleanNodes deletes every file below dir/nodes that no finished unit lists (.tmp files, the
// volumes of unfinished units) and then the directories left empty, a manifest temporary file a
// crash left in dir, and the temporary files a crash left of the progress file at path. What it
// keeps comes from the progress file only, never from what the interrupted run may have had in
// flight. It removes only regular files and directories: anything else below dir/nodes (a
// symlink) refuses with exitcode.ExitResume before anything is deleted, which over ends with how
// to start over.
func cleanNodes(dir, path string, done []doneUnit, over string) error {
	tree, err := walkNodes(dir, over)
	if err != nil {
		return err
	}
	return cleanTree(dir, path, done, tree)
}

// cleanTree is cleanNodes over tree, what walkNodes found below dir/nodes: a resume walks it once,
// before its question, under the lock that keeps any other bydbctl from writing there meanwhile.
func cleanTree(dir, path string, done []doneUnit, tree *nodesTree) error {
	keep := map[string]struct{}{}
	for _, d := range done {
		for _, f := range d.Files {
			keep[filepath.Join(dir, filepath.FromSlash(f.Path))] = struct{}{}
		}
	}
	for _, p := range tree.files {
		if _, ok := keep[p]; ok {
			continue
		}
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	// Deepest first, so a parent is empty once its children are gone. A directory that is not
	// empty stays; one that is and cannot be removed fails (not every system says ENOTEMPTY).
	dirs := slices.Clone(tree.dirs)
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, d := range dirs {
		if err := os.Remove(d); err != nil {
			if entries, readErr := os.ReadDir(d); readErr == nil && len(entries) == 0 {
				return err
			}
		}
	}
	if err := resume.RemoveTemps(filepath.Join(dir, manifest.FileName)); err != nil {
		return err
	}
	return resume.RemoveTemps(path, planSide)
}
