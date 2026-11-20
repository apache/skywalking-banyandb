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

package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// schemaWaitTimeout bounds the wait for a written schema to be applied everywhere; tests
// shorten it.
var schemaWaitTimeout = 2 * time.Minute

// Env is what RunImport works with besides the settings: where it prints, what the
// preflight learned and the liaison connections.
type Env struct {
	// Log receives the information, warnings, progress and a verify report that passes, Out the
	// plan; with a table output both are stdout, with json or yaml Log is stderr and Out stays the
	// rendering. Err receives what explains a failure before its error, such as the work items an
	// interrupted push left and a verify report that fails or was interrupted (stderr), Log when
	// nil.
	Log io.Writer
	Out io.Writer
	Err io.Writer
	// Hints receives the lines about the connection and the settings in use (info, parallelism,
	// rate limits), Log when nil: a structured rendering of the plan leaves them out.
	Hints    io.Writer
	Cluster  *cluster.Info
	Liaisons *cluster.Liaisons
	// Hook, when set, is called after every acknowledged batch (see PushOptions.Hook).
	Hook func(event string)
	// Now is the reference time of the TTL gate; nil uses time.Now.
	Now func() time.Time
	// Terminal reports the size of the terminal Log is, nil when it is not one: the push then
	// redraws a live line per group in place.
	Terminal func() (width, height int)
	// Confirm asks the operator a yes/no question; nil when stdin is not a terminal or none of the
	// streams it can ask on is, and a progress file then needs --resume. The question goes to Log's
	// stream when that is a terminal (stdout, or stderr with json or yaml output); otherwise to
	// stderr when that is one, after the lines written so far are shown there too.
	Confirm func(ctx context.Context, question string) bool
	// DuplicationSource names where resumeDuplicationPolicy was set, for the messages: the flag,
	// the plan file or the variable; "" is taken as the flag.
	DuplicationSource string
	// VerifyExplicit records that --verify was given on the command line: only then is a finished
	// import verified again. plan.yaml and BYDBCTL_IMPORT_VERIFY set a default, not a request.
	VerifyExplicit bool
	// ResumeFromEnv records that --resume came from BYDBCTL_IMPORT_RESUME, not the flag: it
	// resumes a progress file without asking when there is one, and starts a fresh import when
	// there is none, where the flag exits 1.
	ResumeFromEnv bool
}

// hints is where the lines about the connection and the settings go: Hints, else Log.
func (env Env) hints() io.Writer {
	if env.Hints != nil {
		return env.Hints
	}
	return env.Log
}

// errOut is where the explanation of a failure goes: Err, else Log.
func (env Env) errOut() io.Writer {
	if env.Err != nil {
		return env.Err
	}
	return env.Log
}

// withDefaults fills the writers and the clock left unset.
func (env Env) withDefaults() Env {
	if env.Log == nil {
		env.Log = io.Discard
	}
	if env.Out == nil {
		env.Out = io.Discard
	}
	if env.Now == nil {
		env.Now = time.Now
	}
	return env
}

// Result is what RunImport did; fields after the failing phase stay empty.
type Result struct {
	// Plan is the plan decided before anything was written, with every work item; the plan
	// printed leaves out the items a progress file records as complete.
	Plan *Plan
	// Recheck is the plan re-decided after the schema write; nil when not reached.
	Recheck *Plan
	Push    *PushResult
	Verify  *VerifyReport
	// PlanChanges are the differences between Plan and Recheck.
	PlanChanges []string
	// SchemaWrites counts the resources created or updated.
	SchemaWrites int
}

// logSchemaSource logs where the schema of the import comes from: the artifact's schema
// files, or the target for an artifact exported without its schema, which --schema-only
// cannot import.
func logSchemaSource(log io.Writer, art *Artifact, opts *config.Import) error {
	if art.HasSchema() {
		infoSchemaFromArtifact(log, len(art.Manifest.SchemaFiles), opts.Input)
		return nil
	}
	if opts.SchemaOnly {
		return exitcode.Exit(exitcode.ExitUsage, "--schema-only needs a schema, and artifact %s was exported without one", opts.Input)
	}
	if art.Native() {
		infoNativeWithoutSchema(log, opts.Input)
		return nil
	}
	infoCSVWithoutSchema(log, opts.Input)
	return nil
}

// RunImport imports the artifact at opts.Input: it checks the artifact,
// compares the schema with the target, asks Targets, decides and prints the plan. A dry run
// stops there (exit 0, or 2 when the import would be refused). A real run then writes the
// schema in dependency order and waits until every node applied it, re-decides and prints
// what changed (--abort-on-plan-change stops with exit 5), pushes the rows and, with
// --verify, reconciles the target's counts (exit 4 on a mismatch). Errors before the schema
// write keep the exit code of their cause (2 for a refusal: nothing was written); errors from
// the schema write on are exit 3, except a verify failure (4), a plan change (5), a resume
// refused by a plan that differs from the recorded one (6) and a resume halted by
// resumeDuplicationPolicy block (7). Once the progress file exists, an exit 3 names it: the same
// command resumes. Every error is an exitcode.ExitError.
func RunImport(ctx context.Context, env Env, opts config.Import) (*Result, error) {
	env = env.withDefaults()
	now := env.Now
	// A schema-only import reads no data file, so it checks their sizes only.
	art, err := OpenArtifact(ctx, opts.Input, !opts.SchemaOnly)
	if err != nil {
		return nil, err
	}
	if err = logSchemaSource(env.hints(), art, &opts); err != nil {
		return nil, err
	}
	conn := env.Liaisons.Primary()
	path := ProgressPath(opts)
	unlock, err := claimProgress(env.Log, &opts, env.ResumeFromEnv, path)
	if err != nil {
		return nil, err
	}
	defer unlock()
	bindings, state, err := openRun(env, &opts, path)
	if err != nil {
		return nil, err
	}
	d := decider{
		liaisons: env.Liaisons.All, cluster: env.Cluster, art: art, opts: &opts, endpoint: env.Cluster.Liaison + " (ROLE_LIAISON)",
		complete: completeItems(state),
	}
	wait := phaseWaiter{log: env.Log, terminal: env.Terminal}
	var first *decision
	err = wait.run(ctx, "reading the target cluster and comparing the schema", func() (decideErr error) {
		// Native parts are relayed as they are, so there are no rows to check. Nor are there when
		// the run is refused for lack of --resume after the summary: the rows are read for nothing.
		first, decideErr = d.decide(ctx, now(), !opts.SchemaOnly && !art.Native() && !refusedForResume(env, &opts, state))
		return decideErr
	})
	if err != nil {
		return nil, exitcode.Exit(exitcode.For(err), "%w", err)
	}
	// What the target's state says (a barrier probe that was skipped, for one) is not a setting: it
	// is kept in a structured output too, on stderr.
	for _, l := range first.state.Info {
		infoLine(env.Log, l)
	}
	res := &Result{Plan: first.plan}
	shown, err := openProgress(env.Log, &opts, env.DuplicationSource, path, state, first)
	if err != nil {
		return res, err
	}
	if state != nil {
		// The target must still hold what the progress file records as imported, a dry run too.
		if err = checkPresence(ctx, env.Log, conn, art, path, state, first, now()); err != nil {
			return res, err
		}
	}
	if err = renderPlan(env.Out, &opts, state, shown); err != nil {
		return res, exitcode.Exit(exitcode.ExitRuntime, "render the import plan: %w", err)
	}
	warnUnneededForce(env.Log, &opts, first)
	if finished(state, first.plan) {
		return res, verifyFinished(ctx, env, conn, &opts, res, path, state, first)
	}
	if first.plan.Refused() {
		if !first.plan.refusedForProblems() {
			return res, exitcode.Exit(exitcode.ExitPreflight, "the import is refused: every work item is rejected by a gate (see STATUS); nothing was written")
		}
		return res, exitcode.Exit(exitcode.ExitPreflight, "the import is refused (see PROBLEMS); nothing was written")
	}
	if !opts.SchemaOnly {
		// The progress file is first written after the schema: find out before the target changes.
		if err = probeWritable(path); err != nil {
			return res, err
		}
	}
	if opts.DryRun {
		return res, nil
	}
	resumed := state != nil
	switch {
	case resumed && state.FinishedAt != nil && onlyNewItemsLeft(state, first.plan):
		// Finished, with items a gate refused then and no progress of them: nothing of the earlier
		// runs is pushed again.
		finishedWithAcceptedItems(env.Log, *state.FinishedAt, len(remaining(first.plan, state).Entries))
	case resumed && opts.Resume:
		resumeWithoutAsking(env.Log, path, resumeSource(env.ResumeFromEnv))
	case resumed:
		if err = resume.ConfirmResume(ctx, false, env.Confirm, path, importStartOverHint(path)); err != nil {
			return res, err
		}
	}
	run := &realImport{env: env, opts: opts, art: art, conn: conn, path: path, state: state, resumed: resumed, res: res}
	defer run.closeTracker()
	err = run.run(ctx, &d, wait, first, bindings)
	var named *progressNamedError
	if err != nil && run.track != nil && exitcode.For(err) == exitcode.ExitRuntime && !errors.As(err, &named) {
		err = exitcode.Exit(exitcode.ExitRuntime, "%w; the progress file %s is kept: %s", err, path, resumeHint)
	}
	return res, err
}

// openRun reads the progress file of an earlier import of this artifact into this target, which
// this run resumes, if there is one, before the target is read: a progress file that cannot be
// resumed stops the run first. One that needs --resume when not at a terminal is refused later,
// after the resume summary is printed.
func openRun(env Env, opts *config.Import, path string) (importBindings, *progressState, error) {
	if opts.SchemaOnly && (opts.Resume || opts.Restart) {
		noteSchemaOnlyNoProgress(env.Log, path)
	}
	if opts.DryRun && opts.Restart && !opts.SchemaOnly && resume.Exists(path) {
		noteDryRunRestart(env.Log, path)
	}
	bindings, err := currentBindings(opts, env.Cluster)
	if err != nil {
		return bindings, nil, err
	}
	state, err := readProgress(opts, path, bindings)
	if err != nil {
		return bindings, nil, err
	}
	return bindings, state, nil
}

// refusedForResume tells whether a run is certain to be refused after its summary for a progress
// file left by an unfinished import: nobody can be asked and --resume is not given.
func refusedForResume(env Env, opts *config.Import, state *progressState) bool {
	return state != nil && state.FinishedAt == nil && !opts.Resume && !opts.DryRun && env.Confirm == nil
}

// resumeHint tells the operator how to resume an import from its progress file.
const resumeHint = "run the same command again to resume (add --resume when not at a terminal)"

// progressNamedError marks a failure whose message already names the progress file or one of its
// side files, as one that says the file is kept or one writing it: RunImport adds no resume hint.
type progressNamedError struct{ err error }

func (e *progressNamedError) Error() string { return e.err.Error() }

func (e *progressNamedError) Unwrap() error { return e.err }

// namingProgress marks err, which names the progress file, as a progressNamedError; nil stays nil.
func namingProgress(err error) error {
	if err == nil {
		return nil
	}
	return &progressNamedError{err: err}
}

// readProgress reads the progress file of an earlier import of this artifact, if there is one,
// and checks it against this invocation (exit 6, a dry run too): nil without one. A schema-only
// import reads none, and --restart sets it aside unread, so that a dry run with it previews a
// fresh import.
func readProgress(opts *config.Import, path string, bindings importBindings) (*progressState, error) {
	if opts.SchemaOnly || opts.Restart || !resume.Exists(path) {
		return nil, nil
	}
	return loadProgress(path, bindings)
}

// completeItems are the work items state records as complete: their rows need no check.
func completeItems(state *progressState) map[string]bool {
	if state == nil {
		return nil
	}
	out := map[string]bool{}
	for key, r := range state.Done {
		if r.Complete {
			out[key] = true
		}
	}
	return out
}

// finished tells whether the import state records is finished for plan: a run pushed its last
// item, and every item plan holds is complete. An item a gate refused then and accepts now, such as
// with --force-ttl, is pushed by a resume.
func finished(state *progressState, plan *Plan) bool {
	return state != nil && state.FinishedAt != nil && len(remaining(plan, state).Entries) == 0
}

// onlyNewItemsLeft tells whether every item of plan left to push is one the recorded plan of state
// lacks, a gate having refused it then: such an item has no progress, so pushing it repeats
// nothing. An item of the recorded plan left, such as one a gate refused on the run that finished
// the import, may have payloads in flight: it is resumed, after --resume or the question.
func onlyNewItemsLeft(state *progressState, plan *Plan) bool {
	left := remaining(plan, state).Entries
	return len(newItems(state, left)) == len(left)
}

// importStartOverHint tells the operator how to import from scratch instead of resuming the
// progress file at path.
func importStartOverHint(path string) string {
	return "give --restart to import from scratch (" + path + " is kept as a .bak file); stream and trace data already imported is then stored twice"
}

// claimProgress checks that a progress file is at path when the --resume flag asks for one (with
// fromEnv, --resume came from BYDBCTL_IMPORT_RESUME, which without one starts a fresh import), takes
// its lock for the whole run and removes the temporary files a crash left of it. A dry run and a
// schema-only import use no progress file.
func claimProgress(log io.Writer, opts *config.Import, fromEnv bool, path string) (func(), error) {
	if opts.DryRun || opts.SchemaOnly {
		return func() {}, nil
	}
	switch {
	case !opts.Resume || resume.Exists(path):
	case fromEnv:
		noteResumeEnvWithoutProgress(log, path)
	case opts.Resume:
		return nil, exitcode.Exit(exitcode.ExitUsage, "--resume given but no progress file at %s; without --resume a fresh import starts", path)
	}
	// Under the lock no other bydbctl reads or writes the progress file meanwhile.
	unlock, err := lockProgress(log, path)
	if err != nil {
		return nil, err
	}
	if err = resume.RemoveTemps(path, progressSides...); err != nil {
		unlock()
		return nil, unwritable(path, err)
	}
	return unlock, nil
}

// resumeSource names where --resume came from.
func resumeSource(fromEnv bool) string {
	if fromEnv {
		return "BYDBCTL_IMPORT_RESUME"
	}
	return "--resume"
}

// lockProgress takes the lock of the progress file at path for the whole run; another bydbctl
// holding it exits 1, and so does a lock that cannot be taken, such as in a read-only directory.
// The takeover of a stale lock is warned about on log, the import's log. A directory that does
// not exist is not created: probeWritable refuses it before anything is written.
func lockProgress(log io.Writer, path string) (func(), error) {
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		return func() {}, nil
	}
	unlock, err := resume.LockWith(path, log)
	if errors.Is(err, resume.ErrLocked) {
		return nil, exitcode.Exit(exitcode.ExitUsage, "%w; %s", err, anotherClusterHint)
	}
	if err != nil {
		// err names the progress file already.
		return nil, exitcode.Exit(exitcode.ExitUsage, "%w; nothing was written. Give --checkpoint <a path in a writable directory>", err)
	}
	return func() { _ = unlock() }, nil
}

// unwritable is the refusal of a progress file that cannot be written or locked.
func unwritable(path string, err error) error {
	return exitcode.Exit(exitcode.ExitUsage, "the progress file %s cannot be written (%w); nothing was written. "+
		"Give --checkpoint <a path in a writable directory>", path, err)
}

// progressSides are the side files of a progress file.
var progressSides = []string{cursorSide, planSide}

// restartProgress keeps the progress file at path and its side files as <name>.<timestamp>.bak
// for a fresh import, after a warning that says what of the earlier import this one may store
// twice (restartWarning). The side files, and the ones a version 1 file left (legacySidePaths), are
// moved first and the progress file last: a failure before the last move leaves the progress file
// in place, and what is kept is a whole set. All of them get one timestamp, and a .bak of the same
// name is never overwritten.
func restartProgress(log io.Writer, path string, plan *Plan, bindings importBindings, native bool, now time.Time) error {
	base := now.UTC().Format("20060102T150405Z")
	stamp := base
	for n := 1; bakExists(path, stamp); n++ {
		stamp = fmt.Sprintf("%s-%d", base, n)
	}
	bak := path + "." + stamp + ".bak"
	warnRestart(log, path, bak, restartWarning(path, plan, bindings, native))
	for _, from := range slices.Concat(sidePaths(path), legacySidePaths(path)) {
		if err := os.Rename(from, from+"."+stamp+".bak"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return exitcode.Exit(exitcode.ExitRuntime, "--restart: keep %s: %w; no data was sent", from, err)
		}
	}
	if err := os.Rename(path, bak); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "--restart: keep %s as %s: %w; no data was sent", path, bak, err)
	}
	if err := resume.RemoveTemps(path, progressSides...); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "--restart: %w; no data was sent", err)
	}
	return nil
}

// restartWarning says what an import started over may store twice of what the progress file at
// path records, read best-effort: nothing when it records another import (another artifact,
// target or options) or nothing pushed; else the stream and trace data of the groups it records
// rows or payloads of (storedTwice). Measures and properties overwrite what is there.
func restartWarning(path string, plan *Plan, bindings importBindings, native bool) string {
	old := &progressState{}
	if err := resume.Read(path, progressKind, progressVersion, old); err != nil {
		return fmt.Sprintf("it cannot be read (%v), so what it records is unknown: if an earlier run imported this artifact into this cluster, %s",
			err, storedTwice(plan, native, nil))
	}
	if diff := diffBindings(old, bindings); diff != "" {
		return "it records the import of another artifact, target or options, so nothing it records is stored twice by this one (it differs in: " +
			strings.Join(strings.Fields(diff), " ") + ")"
	}
	pushed := map[string]bool{}
	for _, c := range old.Pushed {
		if c.Rows > 0 {
			pushed[c.Catalog+" "+c.Group] = true
		}
	}
	for key, r := range old.Done {
		if len(r.Parts)+len(r.Blobs)+len(r.Landed) > 0 || r.Complete || r.Cursor != nil {
			pushed[groupOfKey(key)] = true
		}
	}
	for _, in := range old.Inflight {
		pushed[groupOfKey(in.Key)] = true
	}
	if len(pushed) == 0 {
		return "it records nothing pushed yet, so nothing is stored twice"
	}
	return storedTwice(plan, native, pushed)
}

// groupOfKey is the "<catalog> <group>" of a work item key.
func groupOfKey(key string) string {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) < 2 {
		return key
	}
	return parts[0] + " " + parts[1]
}

// storedTwice says what an import started over stores twice of what an earlier run imported:
// the stream and trace parts and element indexes of a native artifact, the stream and trace rows
// of a CSV one, of the groups of plan; of those only in pushed ("<catalog> <group>") when it is
// not nil. Measures and properties overwrite what is there.
func storedTwice(plan *Plan, native bool, pushed map[string]bool) string {
	var catalogs []string
	for _, e := range plan.Entries {
		name := e.Item.CatalogName() + " " + e.Item.Group
		if storesDuplicates(e.Item.Catalog) && (pushed == nil || pushed[name]) {
			catalogs = append(catalogs, name)
		}
	}
	if len(catalogs) == 0 {
		return "nothing of this plan is stored twice: measures and properties overwrite what is there"
	}
	what := "rows"
	if native {
		what = "parts and element indexes"
	}
	verb := "are"
	if pushed == nil {
		verb = "may be"
	}
	return fmt.Sprintf("the stream and trace %s an earlier run already imported %s stored twice, in %s", what, verb, strings.Join(transfer.UniqueSorted(catalogs), ", "))
}

// sidePaths are the side files of the progress file at path.
func sidePaths(path string) []string {
	out := make([]string, len(progressSides))
	for i, side := range progressSides {
		out[i] = resume.SidePath(path, side)
	}
	return out
}

// legacySidePaths are the side files a version 1 progress file at path keeps: "<path>.plan",
// "<path>.cursor" and "<path>.baseline", and the baseline's own "<path without .json>-baseline.json".
// Version 2 uses none of them, and --restart keeps them with the file.
func legacySidePaths(path string) []string {
	return []string{path + ".plan", path + ".cursor", path + ".baseline", resume.SidePath(path, "baseline")}
}

// bakExists tells whether a .bak file of the progress file at path or of a side file has stamp.
func bakExists(path, stamp string) bool {
	for _, name := range slices.Concat([]string{path}, sidePaths(path), legacySidePaths(path)) {
		if _, err := os.Lstat(name + "." + stamp + ".bak"); err == nil {
			return true
		}
	}
	return false
}

// realImport is a real import from the schema write on.
type realImport struct {
	env     Env
	conn    *grpc.ClientConn
	art     *Artifact
	state   *progressState
	track   *tracker
	res     *Result
	path    string
	opts    config.Import
	resumed bool
}

// closeTracker closes the tracker on the way out of a failed run; a write failure is only
// warned about, the run's own error being the one reported.
func (r *realImport) closeTracker() {
	if err := r.track.close(); err != nil {
		warnError(r.env.Log, err)
	}
}

// run writes the schema, re-checks the plan, pushes and verifies. From the schema write on the
// target is being changed: every failure is a runtime failure. A failure to record the progress
// stops the run: the next resume would be wrong without it.
func (r *realImport) run(ctx context.Context, d *decider, wait phaseWaiter, first *decision, bindings importBindings) (err error) {
	env, opts, res := r.env, &r.opts, r.res
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	startTracking := func() error {
		var startErr error
		r.track, startErr = startProgress(ctx, r.path, r.state, opts.CheckpointEveryBatches, cancelRun)
		if startErr == nil {
			progressFile(env.Log, r.path)
		}
		return startErr
	}
	if r.resumed {
		if err = startTracking(); err != nil {
			return err
		}
	}
	if res.SchemaWrites, err = writeAndAwaitSchema(ctx, r.conn, first.diff, r.art, opts, env.Log, wait); err != nil {
		return err
	}
	if opts.SchemaOnly {
		schemaOnlyDone(env.Log, res.SchemaWrites)
		return nil
	}
	// The rows were checked against the predicted schema before anything was written.
	second, err := recheck(ctx, d, wait, env.Now(), first, opts, env.Log, res)
	if err != nil {
		return err
	}
	entries := second.plan.Entries
	if !r.resumed {
		// --restart sets the progress file aside only now: a run refused before keeps it in place.
		if opts.Restart && resume.Exists(r.path) {
			if err = restartProgress(env.Log, r.path, first.plan, bindings, r.art.Native(), env.Now()); err != nil {
				return err
			}
		}
		// A fresh import records its progress from here: nothing was refused or sent so far, and the
		// schema write is idempotent, so a failure before leaves nothing to resume.
		// A cursor side file left without its progress file belongs to an earlier import: it goes
		// before the first write, as any FoldSeq of it is not lower than the fresh file's.
		side := resume.SidePath(r.path, cursorSide)
		if err = removeCursors(side); err != nil && !errors.Is(err, os.ErrNotExist) {
			return exitcode.Exit(exitcode.ExitRuntime, "remove %s: %w; no data was sent", side, err)
		}
		r.state = newProgress(r.art, bindings)
		r.state.Plan = planOf(entries)
		// The plan side file is written before the progress file that names it.
		if r.state.PlanDigest, err = writePlan(r.path, r.state.Plan, nil); err != nil {
			return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
		}
		if err = startTracking(); err != nil {
			return err
		}
	}
	pre, rec, err := r.preparePush(ctx, wait, second)
	if err != nil {
		return err
	}
	sampled := mergeSampledGroups(second.state.Groups)
	start := time.Now()
	push, rp, err := pushWithProgress(runCtx, env, r.art, *opts, &second.state.Schemas, entries, progressRun{
		state: r.state, track: r.track, rec: rec, sampled: sampled, resumed: r.resumed, rejected: second.plan.Rejected,
	})
	res.Push = push
	if err != nil {
		return err
	}
	verifying := (opts.EffectiveVerify() || (rp != nil && len(rp.skipped) > 0)) && len(entries) > 0
	if err = r.track.sync(func(s *progressState) {
		finished := time.Now()
		s.FinishedAt = &finished
		// Until a verify passes, a later run verifies again instead of only saying it is done.
		// A verify pending from an earlier run stays pending: only a verify that passes clears it.
		s.VerifyPending = (s.VerifyPending || verifying) && pre != nil
	}); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	logImported(env.Log, r.art, push, start)
	if !verifying {
		return r.closeChecked()
	}
	if pre == nil {
		return exitcode.Exit(exitcode.ExitVerify, "verify is not possible: the first run of this import could not count the target before its push "+
			"(it needed no baseline then), so there is nothing to compare against%s; --restart imports it again with a new baseline", skippedNote(rp))
	}
	// pushed counts every run: the groups of recorded items a gate refuses now are verified too.
	if err = runVerify(ctx, env, r.conn, opts, res, pre, recordedDecision(second, r.state), r.track.pushedTotals(), rp); err != nil {
		return err
	}
	// What was pushed, and the payloads skip left out, are checked now: later runs need not verify.
	if err = r.track.sync(func(s *progressState) { s.SkippedPayloads, s.VerifyPending = nil, false }); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	return r.closeChecked()
}

// preparePush checks the re-checked plan against the recorded one, records the baseline (and the
// groups of the items appended to the plan), and builds the reconciler of native stream and trace
// parts, nil without them or without a baseline.
func (r *realImport) preparePush(ctx context.Context, wait phaseWaiter, second *decision) (*Counts, *reconciler, error) {
	opts, entries := &r.opts, second.plan.Entries
	if r.resumed {
		if diff := comparePlans(r.state, second.plan, true); diff != "" {
			return nil, nil, planDiffers(r.path, diff)
		}
	}
	// What skip leaves out is verified, so a baseline that cannot be counted then fails the import.
	verify := opts.EffectiveVerify() || opts.HasDuplicationSkip()
	pre, err := r.takeBaseline(ctx, wait, entries, verify)
	if err != nil {
		return nil, nil, err
	}
	if added := newItems(r.state, entries); r.resumed && len(added) > 0 {
		// Counted before anything of them is sent, like the baseline of the first run.
		if err = r.coverNewGroups(ctx, wait, pre, added); err != nil {
			return nil, nil, err
		}
		// Recorded before anything of them is sent: a later resume compares them like the others.
		if err = r.track.appendPlan(r.state, planOf(added)); err != nil {
			return nil, nil, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
		}
		planItemsAdded(r.env.Log, len(added))
	}
	if pre == nil || !r.art.Native() || !pushesReconcilableParts(entries) {
		return pre, nil, nil
	}
	selectors := VerifySelectors(entries)
	rec := newReconciler(pre, func(ctx context.Context) (*Counts, error) { return Snapshot(ctx, r.conn, selectors) })
	rec.sampled = mergeSampledGroups(second.state.Groups)
	return pre, rec, nil
}

// coverNewGroups counts, before anything of them is pushed, the groups of the items appended to
// the plan that no recorded item covers, so the baseline the first run recorded lacks them, and
// records their counts with it (a new digest): verify and the reconciliation compare those groups
// against what they held before this run. Nothing to do without a baseline. Every target node of
// the plan is one pre counted already (coverNewNodes runs first), so its nodes stay as they are.
func (r *realImport) coverNewGroups(ctx context.Context, wait phaseWaiter, pre *Counts, added []Entry) error {
	if pre == nil {
		return nil
	}
	covered := map[string]bool{}
	for _, p := range r.state.Plan {
		covered[groupOfKey(p.Key)] = true
	}
	groups := map[string]commonv1.Catalog{}
	var fresh []Entry
	for _, e := range added {
		if !covered[e.Item.CatalogName()+" "+e.Item.Group] {
			fresh = append(fresh, e)
			groups[e.Item.Group] = e.Item.Catalog
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	err := r.extendBaselineWith(ctx, wait, "counting the groups the appended items add, before the push", groupSelectors(groups), fresh, pre,
		func(extra *Counts) *Counts {
			return mergeCounts(pre, extra, func(k CountKey) bool {
				c, ok := groups[k.Group]
				return ok && c == k.Catalog
			}, func(g string) bool { return groups[g] == commonv1.Catalog_CATALOG_PROPERTY })
		})
	if err != nil {
		return err
	}
	names := slices.Sorted(maps.Keys(groups))
	baselineGroupsAdded(r.env.Log, names)
	return nil
}

// noDataSent is the failure of a count of the target before the first push. An interruption says
// no more: the resume hint of the progress file follows it.
func noDataSent(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "%w; no data was sent", err)
	}
	return exitcode.Exit(exitcode.ExitRuntime, "%w; no data was sent: run the same command again once the target answers", err)
}

// extendBaselineWith counts the groups of selectors of entries on the target, before anything of
// them is pushed (label says so while it waits), and records what merge makes of pre and that count
// as the baseline (a new digest), which pre becomes too.
func (r *realImport) extendBaselineWith(ctx context.Context, wait phaseWaiter, label string, selectors []*transferv1.Selector, entries []Entry,
	pre *Counts, merge func(extra *Counts) *Counts,
) error {
	var extra *Counts
	err := wait.run(ctx, label, func() (snapErr error) {
		extra, snapErr = snapshotBaseline(ctx, r.conn, selectors, entries)
		return snapErr
	})
	if err != nil {
		return noDataSent(ctx, err)
	}
	merged := merge(extra)
	if err = r.track.extendBaseline(r.state, merged); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	*pre = *merged
	return nil
}

// mergeCounts is pre with the shard and segment counts of extra whose key keep accepts and the
// property counts of extra whose group keepProperty (nil: none) accepts; the rest of pre stays.
func mergeCounts(pre, extra *Counts, keep func(CountKey) bool, keepProperty func(group string) bool) *Counts {
	out := *pre
	out.Shards, out.Segments, out.Properties = maps.Clone(pre.Shards), maps.Clone(pre.Segments), maps.Clone(pre.Properties)
	for _, m := range []struct{ dst, src map[CountKey]map[string]int64 }{{out.Shards, extra.Shards}, {out.Segments, extra.Segments}} {
		for k, segs := range m.src {
			if keep(k) {
				m.dst[k] = maps.Clone(segs)
			}
		}
	}
	for g, n := range extra.Properties {
		if keepProperty == nil || !keepProperty(g) {
			continue
		}
		if out.Properties == nil {
			out.Properties = map[string]int64{}
		}
		out.Properties[g] = n
	}
	return &out
}

// closeChecked closes the tracker and reports a write failure.
func (r *realImport) closeChecked() error {
	if err := r.track.close(); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	return nil
}

// probeWritable checks that the progress file at path can be written, by creating and removing
// a file next to it: a read-only artifact directory, or a --checkpoint whose directory does not
// exist, exits 1 before the target is changed.
func probeWritable(path string) error {
	// Named as a temporary file of the progress file, so a crash leaves nothing RemoveTemps misses.
	f, err := os.CreateTemp(filepath.Dir(path), resume.TempPattern(path))
	if err != nil {
		return unwritable(path, err)
	}
	name := f.Name()
	closeErr := f.Close()
	// A real run starting meanwhile removes it as a temporary file of the progress file.
	if err = os.Remove(name); err == nil || errors.Is(err, os.ErrNotExist) {
		err = closeErr
	}
	if err != nil {
		return unwritable(path, err)
	}
	return nil
}

// openProgress prints the resume summary of the progress file state was read from, if any, and
// checks the plan against the recorded one (exit 6, a dry run too); shown is the plan of what is
// left (the whole plan without one).
func openProgress(log io.Writer, opts *config.Import, policySource, path string, state *progressState, first *decision) (*Plan, error) {
	if state == nil {
		return first.plan, nil
	}
	var amb []*ambiguousPayload
	done := finished(state, first.plan)
	if !done {
		amb = classify(state, first.plan.Entries, opts, mergeSampledGroups(first.state.Groups), policySource)
	}
	sum := summarize(path, state, first.plan.Entries, amb)
	if done {
		sum.Finished = state.FinishedAt
	}
	if err := sum.Render(log); err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "render the resume summary: %w", err)
	}
	if diff := comparePlans(state, first.plan, false); diff != "" {
		return nil, planDiffers(path, diff)
	}
	left := remaining(first.plan, state)
	left.Resumed = &ResumedProgress{Path: path, Done: sum.Done, Total: sum.Total, FinishedAt: state.FinishedAt}
	return left, nil
}

// renderPlan writes the plan to out: after the summary of a resume, its dry run included, what is
// left only (renderResumed); else the whole plan.
func renderPlan(out io.Writer, opts *config.Import, state *progressState, p *Plan) error {
	if state != nil {
		return renderResumed(out, opts.OutputFormat, p)
	}
	return Render(out, opts.OutputFormat, p)
}

// startProgress starts recording the progress of the import in path, writing the file. A write
// that fails later cancels the run.
func startProgress(ctx context.Context, path string, state *progressState, every int, cancelRun context.CancelCauseFunc) (*tracker, error) {
	track, err := startTracker(ctx, path, state, every, func(writeErr error) {
		cancelRun(exitcode.Exit(exitcode.ExitRuntime, "record the progress: %w", writeErr))
	})
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	// The cursors of a side file that was read are exact: no run is sending, so their send limit
	// goes, and a later resume that finds the side file removed below does not take it for lost.
	// The cursors of a lost side file keep theirs: their window is only known by it.
	if err = track.sync(func(s *progressState) {
		for _, r := range s.Done {
			if r.Cursor != nil && !r.Cursor.limitWindow {
				r.Cursor.SendLimit = 0
			}
		}
	}); err != nil {
		return track, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	// The cursors of the side file are in the progress file now.
	side := resume.SidePath(path, cursorSide)
	if err = removeCursors(side); err != nil && !errors.Is(err, os.ErrNotExist) {
		return track, namingProgress(exitcode.Exit(exitcode.ExitRuntime, "remove %s: %w", side, err))
	}
	return track, nil
}

// recheck re-decides the plan after the schema write and prints what changed: with
// --abort-on-plan-change a change exits 5, and a refusal now is exit 3.
func recheck(ctx context.Context, d *decider, wait phaseWaiter, now time.Time, first *decision, opts *config.Import, log io.Writer, res *Result,
) (*decision, error) {
	var second *decision
	err := wait.run(ctx, "re-checking the plan after the schema write", func() (decideErr error) {
		second, decideErr = d.decide(ctx, now, false)
		return decideErr
	})
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "re-check the plan after the schema write: %w", err)
	}
	res.Recheck = second.plan
	res.PlanChanges = ComparePlans(first.plan, second.plan)
	for _, w := range second.plan.Warnings {
		if !slices.Contains(first.plan.Warnings, w) {
			warnRecheck(log, w)
		}
	}
	if len(res.PlanChanges) > 0 {
		fmt.Fprintln(log, "the plan re-checked after the schema write differs:")
		for _, l := range res.PlanChanges {
			fmt.Fprintf(log, "  %s\n", l)
		}
		if opts.AbortOnPlanChange {
			return nil, exitcode.Exit(exitcode.ExitPlanChanged, "the plan changed after the schema write (--abort-on-plan-change); the schema is written, no data was sent")
		}
	}
	if second.plan.Refused() {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "the plan re-checked after the schema write refuses the import: %s; the schema is written, no data was sent",
			refusalReasons(second.plan))
	}
	return second, nil
}

// logImported prints what this run pushed.
func logImported(log io.Writer, art *Artifact, push *PushResult, start time.Time) {
	var rows uint64
	for _, n := range push.Rows {
		rows += n
	}
	elapsed := output.FormatElapsed(time.Since(start))
	if art.Native() {
		// Series-index, element-index and property payloads count no rows.
		fmt.Fprintf(log, "IMPORTED  %d work item(s) of native files, %d rows in the imported parts, in %s\n", len(push.Done), rows, elapsed)
	} else {
		fmt.Fprintf(log, "IMPORTED  %d rows of %d work item(s) in %s\n", rows, len(push.Done), elapsed)
	}
}

// verifyFinished handles a run over a finished import: nothing is pushed, and what a query of the
// target returns is not compared with the artifact (only the verify of the run that pushed the
// data does). Only an explicit --verify, a verify the finishing run was to do and none has passed
// since (unless --verify=false), or payloads skip left out that no verify has checked yet, verify
// the counts again, against the baseline the first run recorded (exit 4); otherwise, unless
// --verify=false, a note says why it is not done. A dry run only says it is done.
func verifyFinished(ctx context.Context, env Env, conn *grpc.ClientConn, opts *config.Import, res *Result, path string,
	state *progressState, first *decision,
) error {
	alreadyImported(env.Log, *state.FinishedAt, targetNames(state.Bindings))
	if opts.DryRun {
		return nil
	}
	baseline := "the first run recorded no baseline"
	if state.BaselineAt != nil {
		baseline = "the baseline was taken at " + state.BaselineAt.Local().Format(time.RFC3339)
	}
	pending := state.VerifyPending && opts.EffectiveVerify()
	forced := len(state.SkippedPayloads) > 0 || pending
	if !forced && (!env.VerifyExplicit || !opts.EffectiveVerify()) {
		if opts.EffectiveVerify() {
			noteVerifySkipped(env.Log, baseline)
		}
		return nil
	}
	pre := state.baseline
	if pre == nil {
		return exitcode.Exit(exitcode.ExitVerify, "verify is not possible: %s (its count of the target before the push failed, and it needed none then); "+
			"--restart imports it again with a new baseline", baseline)
	}
	if pending {
		verifyPending(env.Log)
	}
	verifyAgainstBaseline(env.Log, baseline)
	// loadProgress validated the pushed counts.
	pushed, err := pushedMap(state.Pushed)
	if err != nil {
		return exitcode.Exit(exitcode.ExitResume, "the progress file %s is unusable: %w", path, err)
	}
	rp := &resumePlan{tolerance: toleranceOf(state.Duplicates), skipped: state.SkippedPayloads}
	if err = runVerify(ctx, env, conn, opts, res, pre, recordedDecision(first, state), pushed, rp); err != nil {
		return err
	}
	if forced {
		state.SkippedPayloads, state.VerifyPending = nil, false
		state.UpdatedAt = env.Now()
		if err = writeProgress(path, state); err != nil {
			return exitcode.Exit(exitcode.ExitRuntime, "record the progress: %w", err)
		}
	}
	return nil
}

// recordedDecision is first with the items the progress file records that a gate refuses now
// planned again, so that verify counts every group the import pushed rows into.
func recordedDecision(first *decision, state *progressState) *decision {
	recorded := state.plannedKeys()
	plan := *first.plan
	plan.Entries = slices.Clone(plan.Entries)
	for _, r := range first.plan.Rejected {
		if recorded[r.Item.Key()] {
			plan.Entries = append(plan.Entries, Entry{Item: r.Item, TargetStage: r.TargetStage})
		}
	}
	return &decision{state: first.state, diff: first.diff, plan: &plan}
}

// skippedNote names what resumeDuplicationPolicy skip left out, for a verify failure.
func skippedNote(rp *resumePlan) string {
	if rp == nil || len(rp.skipped) == 0 {
		return ""
	}
	return fmt.Sprintf("; resumeDuplicationPolicy skip left out: %s", strings.Join(rp.skipped, "; "))
}

// runVerify checks the target's counts against the baseline and every row pushed (exit 4 on a
// mismatch, naming what resumeDuplicationPolicy skip left out) over the plan of second.
func runVerify(ctx context.Context, env Env, conn *grpc.ClientConn, opts *config.Import, res *Result, pre *Counts, second *decision,
	pushed map[PushKey]uint64, rp *resumePlan,
) error {
	if !opts.EffectiveVerify() && rp != nil && len(rp.skipped) > 0 {
		verifyForcedBySkip(env.Log)
	}
	vprog := output.NewProgress(env.Log, env.Terminal, "")
	vprog.Start(ctx)
	vopts := VerifyOptions{Log: vprog.Wrap(env.Log), Progress: vprog, Timeout: opts.VerifyTimeout, Sampled: mergeSampledGroups(second.state.Groups)}
	if rp != nil {
		vopts.Tolerance = rp.tolerance
	}
	res.Verify = Verify(ctx, conn, pre, second.plan, second.state.Routes, pushed, vopts)
	vprog.Close()
	report := env.Log
	if res.Verify.Failed() || ctx.Err() != nil {
		report = env.errOut()
	}
	res.Verify.Render(report)
	if ctx.Err() != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "import interrupted during verify: %w", context.Cause(ctx))
	}
	if !res.Verify.Failed() {
		return nil
	}
	return exitcode.Exit(exitcode.ExitVerify, "verify failed: %d check(s) not completed, %d count mismatch(es)%s",
		len(res.Verify.Incomplete), len(res.Verify.Mismatches), skippedNote(rp))
}

// progressRun is how a push records its progress and what it resumes.
type progressRun struct {
	state   *progressState
	track   *tracker
	rec     *reconciler
	sampled map[string]bool
	// rejected are the items a gate refused this time; the progress file may record them.
	rejected []Rejection
	resumed  bool
}

// pushWithProgress pushes the entries, recording the progress. A resumed push first resolves the
// ambiguous set once and then pushes only what is left: the items not complete,
// without the payloads and CSV rows already stored.
func pushWithProgress(ctx context.Context, env Env, art *Artifact, opts config.Import, target *Schemas, entries []Entry, run progressRun,
) (*PushResult, *resumePlan, error) {
	var rp *resumePlan
	todo := entries
	if run.resumed {
		amb := classify(run.state, entries, &opts, run.sampled, env.DuplicationSource)
		if err := locateLimitWindows(ctx, art, target, amb); err != nil {
			return nil, nil, err
		}
		var err error
		if rp, err = resolveResume(ctx, env.Log, run.track, run.state, entries, run.rejected, amb, run.rec); err != nil {
			return nil, nil, err
		}
		todo = remainingEntries(entries, run.state)
		fmt.Fprintf(env.Log, "RESUMED   %d of %d work item(s) done by the previous run(s); %d left\n", len(entries)-len(todo), len(entries), len(todo))
	}
	push, err := pushEntries(ctx, env, art, opts, target, todo, PushOptions{reconciler: run.rec, track: run.track, resume: rp})
	return push, rp, err
}

// planDiffers is the refusal of a resume whose plan differs from the recorded one.
func planDiffers(path, diff string) error {
	return exitcode.Exit(exitcode.ExitResume, "the plan differs from the one recorded in %s; no data was sent (the schema may be written):\n%s\n"+
		"run the first run's command again (same --input, options and target, before a gate such as the TTL refuses more items) to resume, or "+
		separateHint, path, diff)
}

// baselineRetryTimeout bounds how long the baseline Plan is retried; tests shorten it.
var baselineRetryTimeout = time.Minute

// takeBaseline counts the imported groups on the target before the first push: for --verify,
// for the reconciliation of native stream and trace parts, and for a later resume, so it is
// recorded once, in a side file whose digest the progress file holds, and read back from it by a
// resumed run, which first counts the target nodes it lacks (coverNewNodes). The Plan is retried
// with the transfer back-off; when neither verify nor reconciliation needs it, a failure is a
// warning and the progress file records that there is no baseline.
func (r *realImport) takeBaseline(ctx context.Context, wait phaseWaiter, entries []Entry, verify bool) (*Counts, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	reconcile := r.art.Native() && pushesReconcilableParts(entries)
	switch {
	case r.state.baseline != nil:
		// loadProgress read it.
		pre := r.state.baseline
		if err := r.coverNewNodes(ctx, wait, pre, entries); err != nil {
			return nil, err
		}
		return pre, nil
	case r.state.NoBaseline:
		// The first run recorded that it had none; a count taken now would hold its rows.
		if verify {
			warnNoBaselineForVerify(r.env.Log)
		}
		return nil, nil
	}
	var pre *Counts
	err := wait.run(ctx, "counting the target before the push", func() (snapErr error) {
		pre, snapErr = snapshotBaseline(ctx, r.conn, VerifySelectors(entries), entries)
		return snapErr
	})
	if err != nil {
		if ctx.Err() != nil || verify || reconcile {
			return nil, noDataSent(ctx, err)
		}
		warnBaselineNotNeeded(r.env.Log, err)
		if err = r.track.sync(func(s *progressState) { s.NoBaseline = true }); err != nil {
			return nil, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
		}
		return nil, nil
	}
	if err = r.track.recordBaseline(r.state, pre, r.env.Now()); err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	return pre, nil
}

// coverNewNodes counts, before anything is pushed, the target nodes of entries the recorded
// baseline pre did not count, such as a data node added to the target since the first run, and
// records them with it (a new digest): verify and the reconciliation then compare what the import
// pushes there against what they held before. A node is counted only while the progress file
// records nothing pushed to it, so that its count holds none of the import's rows; any other is
// left uncounted, and the verify says so.
func (r *realImport) coverNewNodes(ctx context.Context, wait phaseWaiter, pre *Counts, entries []Entry) error {
	var nodes []string
	used := r.state.nodesUsed()
	for _, n := range uncountedTargets(pre, entries) {
		if used[n] {
			warnNodeLeftUncounted(r.env.Log, n)
			continue
		}
		nodes = append(nodes, n)
	}
	if len(nodes) == 0 {
		return nil
	}
	err := r.extendBaselineWith(ctx, wait, "counting the target nodes the baseline lacks, before the push", VerifySelectors(entries), entries, pre,
		func(extra *Counts) *Counts { return mergeNodes(pre, extra, nodes) })
	if err != nil {
		return err
	}
	baselineNodesAdded(r.env.Log, nodes)
	return nil
}

// nodesUsed are the target nodes the progress file records anything of this import pushed to,
// or to be pushed to by an item with progress.
func (s *progressState) nodesUsed() map[string]bool {
	out := map[string]bool{}
	for _, p := range s.Plan {
		if hasProgress(s, p.Key) {
			for _, n := range p.TargetNodes {
				out[n] = true
			}
		}
	}
	for _, in := range s.Inflight {
		for _, n := range in.Nodes {
			out[n] = true
		}
	}
	for _, r := range s.Done {
		for n := range r.Landed {
			out[n] = true
		}
		for _, n := range r.Tainted {
			out[n] = true
		}
		r.eachPayload(func(_ string, p *payloadRecord, _ bool) {
			for _, n := range slices.Concat(p.Nodes, p.Missing, p.CompletionSent, p.Skipped, p.Repushed) {
				out[n] = true
			}
		})
	}
	return out
}

// mergeNodes is pre with the counts extra holds on nodes added: what pre counted stays as it is.
func mergeNodes(pre, extra *Counts, nodes []string) *Counts {
	out := mergeCounts(pre, extra, func(k CountKey) bool { return slices.Contains(nodes, k.Node) }, nil)
	out.Unreachable = slices.DeleteFunc(slices.Clone(pre.Unreachable), func(n string) bool { return slices.Contains(nodes, n) })
	// A baseline recorded without the nodes it answered keeps saying so: counted tells from its
	// Unreachable only, and a list of just the new nodes would uncount the others.
	if pre.Answered != nil {
		out.Answered = transfer.UniqueSorted(slices.Concat(pre.Answered, nodes))
	}
	return out
}

// snapshotBaseline counts the groups of entries on the target, retrying a failed Plan and one that
// did not count a target node with the transfer back-off until baselineRetryTimeout: the baseline is
// recorded once, and a target node it does not count would stay uncounted for every later run.
func snapshotBaseline(ctx context.Context, conn *grpc.ClientConn, selectors []*transferv1.Selector, entries []Entry) (*Counts, error) {
	deadline := time.Now().Add(baselineRetryTimeout)
	backoff := transfer.RetryBase
	for {
		pre, err := Snapshot(ctx, conn, selectors)
		if err == nil {
			missing := uncountedTargets(pre, entries)
			if len(missing) == 0 {
				return pre, nil
			}
			err = fmt.Errorf("the baseline Plan of the target did not count target node(s) %s", strings.Join(missing, ", "))
		} else {
			err = fmt.Errorf("baseline Plan of the target: %w", err)
		}
		if ctx.Err() != nil || time.Now().Add(backoff).After(deadline) {
			return nil, err
		}
		if sleepErr := transfer.SleepCtx(ctx, backoff); sleepErr != nil {
			return nil, err
		}
		backoff = transfer.NextBackoff(backoff)
	}
}

// uncountedTargets are the target nodes of entries a Plan did not count: unreachable, or not asked.
func uncountedTargets(c *Counts, entries []Entry) []string {
	var out []string
	for _, e := range entries {
		for _, n := range e.TargetNodes {
			if !c.counted(n) {
				out = append(out, n)
			}
		}
	}
	return transfer.UniqueSorted(out)
}

// pushEntries logs the parallelism and the rates and pushes the entries: the part files of a
// native artifact, the rows of a CSV one. target is the target schema the rows are assembled for.
func pushEntries(ctx context.Context, env Env, art *Artifact, opts config.Import, target *Schemas, entries []Entry, pushOpts PushOptions) (*PushResult, error) {
	workers, reason := WorkerCount(opts.Parallelism, entries)
	if art.Native() {
		parallelismNative(env.hints(), workers, reason, opts.PartChunkSize>>10)
	} else {
		window := max(1, opts.WriteInflightBatches)
		parallelismCSV(env.hints(), workers, reason, window, float64(uint64(window)*opts.WriteBatchBytes)/(1<<20))
	}
	logRates(env.hints(), env.Log, &opts, art.Native())
	prog := output.NewProgress(env.Log, env.Terminal, "group(s) importing")
	prog.Start(ctx)
	defer prog.Close()
	// Workers print concurrently; through the progress no line tears the live lines.
	pushOpts.Log, pushOpts.Err, pushOpts.Hook, pushOpts.Import, pushOpts.Progress = prog.Wrap(env.Log), prog.Wrap(env.errOut()), env.Hook, opts, prog
	if art.Native() {
		return PushParts(ctx, env.Liaisons.All, art, entries, pushOpts)
	}
	return Push(ctx, env.Liaisons.All, art, target, entries, pushOpts)
}

// pushesReconcilableParts tells whether the entries push stream or trace parts.
func pushesReconcilableParts(entries []Entry) bool {
	return slices.ContainsFunc(entries, func(e Entry) bool {
		return e.Item.Kind == ShardItem && reconcilable(e.Item.Catalog, nativefmt.KindPart)
	})
}

// logRates logs the rate limits of the push when any is set, the wire values the liaisons and
// the data nodes apply. A native push applies only the part limit and a CSV push only the
// others, so a limit the artifact's format ignores is warned about instead, on warn: a structured
// output keeps the warning although it leaves the settings (log) out.
func logRates(log, warn io.Writer, opts *config.Import, native bool) {
	part := ratelimit.PerSecond(opts.PartRateLimit)
	node := ratelimit.Proto(opts.NodeRateLimit, opts.NodeRowRateLimit)
	liaison, client := ratelimit.PerSecond(opts.LiaisonRateLimit), ratelimit.PerSecond(opts.WriteRateLimit)
	rowLimits := node.GetBytesPerSecond() != 0 || node.GetRowsPerSecond() != 0 || liaison != 0 || client != 0
	if native {
		if part > 0 {
			rateLimitsNative(log, part)
		}
		if rowLimits {
			warnRowLimitsOnNative(warn)
		}
		return
	}
	if part > 0 {
		warnPartLimitOnCSV(warn)
	}
	if !rowLimits {
		return
	}
	rateLimitsCSV(log, node.GetBytesPerSecond(), node.GetRowsPerSecond(), liaison, client)
}

// writeAndAwaitSchema writes the planned schema changes and waits until every node applied
// them; it reports how many resources were written.
func writeAndAwaitSchema(ctx context.Context, conn *grpc.ClientConn, diff *SchemaDiff, art *Artifact, opts *config.Import, log io.Writer,
	wait phaseWaiter,
) (int, error) {
	writes := diff.Writes()
	if !opts.IncludeSchema || len(writes) == 0 {
		return 0, nil
	}
	written := 0
	err := wait.run(ctx, fmt.Sprintf("writing %d schema resource(s) and waiting for every node to apply them", len(writes)), func() error {
		req, writeErr := WriteSchema(ctx, conn, writes, &art.Schemas, art.Native())
		if writeErr != nil {
			return writeErr
		}
		written = len(writes)
		return AwaitSchema(ctx, conn, req, schemaWaitTimeout)
	})
	if err != nil {
		return written, err
	}
	schemaWritten(log, len(writes))
	return len(writes), nil
}

// decision is one collect + diff + decide round.
type decision struct {
	state *TargetState
	diff  *SchemaDiff
	plan  *Plan
}

type decider struct {
	// complete are the work items a progress file records as complete: their rows are not checked.
	complete map[string]bool
	cluster  *cluster.Info
	art      *Artifact
	opts     *config.Import
	endpoint string
	liaisons []*cluster.Liaison
}

// checkRowsFn is CheckRows; tests count its calls.
var checkRowsFn = CheckRows

// decide collects the target, compares the schema and decides; with checkRows it also checks
// every row of the planned work items against the predicted target schema, unless the plan
// is already refused for its schema or artifact.
func (d *decider) decide(ctx context.Context, now time.Time, checkRows bool) (*decision, error) {
	// An artifact exported without its schema has nothing to compare or write.
	schemaOpts := SchemaOptions{
		Default: d.opts.OnSchemaConflict, Rules: d.opts.ConflictRules,
		IncludeSchema: d.opts.IncludeSchema && d.art.HasSchema(), Force: d.opts.Force,
	}
	state, err := Collect(ctx, d.liaisons, d.cluster, d.art, schemaOpts)
	if err != nil {
		return nil, err
	}
	diff := DiffSchemas(&d.art.Schemas, state, schemaOpts)
	predicted := PredictState(state, diff.Actions)
	ruleIDs := make(map[ResourceKey]uint32, len(state.IndexRules))
	for k, r := range state.IndexRules {
		ruleIDs[k] = r.GetMetadata().GetId()
	}
	plan := Decide(Inputs{
		Now: now, Artifact: d.art, Target: predicted, Endpoint: d.endpoint, Actions: diff.Actions, TargetIndexRuleIDs: ruleIDs,
		Flags: Flags{ForceTTL: d.opts.ForceTTL, AllSources: d.opts.AllSources, StrictCoverage: d.opts.StrictCoverage, SchemaOnly: d.opts.SchemaOnly},
	})
	if checkRows && !plan.SchemaBlocked() && len(plan.Errors) == 0 && len(plan.Entries) > 0 {
		check := plan.Entries
		if len(d.complete) > 0 {
			check = slices.DeleteFunc(slices.Clone(check), func(e Entry) bool { return d.complete[e.Item.Key()] })
		}
		rc, checkErr := checkRowsFn(ctx, d.art, &predicted.Schemas, check, d.opts.WriteBatchBytes)
		switch {
		case checkErr == nil:
			ApplyRowCheck(plan, rc, d.opts.WriteBatchBytes)
		case ctx.Err() != nil:
			return nil, checkErr
		default:
			plan.Errors = append(plan.Errors, checkErr.Error())
		}
	}
	return &decision{state: state, diff: diff, plan: plan}, nil
}

// warnUnneededForce warns when --force confirms an overwrite of groups that are all known
// to be empty, so it changed nothing.
func warnUnneededForce(log io.Writer, opts *config.Import, d *decision) {
	if !opts.Force {
		return
	}
	var groups []string
	for _, a := range d.diff.Actions {
		if a.Disposition == DispositionOverwrite && !slices.Contains(groups, a.Group) {
			groups = append(groups, a.Group)
		}
	}
	if len(groups) == 0 {
		return
	}
	for _, g := range groups {
		if st := d.state.NonEmptyGroups[g]; !st.Known || st.NonEmpty {
			return
		}
	}
	warnForceNotNeeded(log, groups)
}
