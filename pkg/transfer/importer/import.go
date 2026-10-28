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
	"fmt"
	"io"
	"slices"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// schemaWaitTimeout bounds the wait for a written schema to be applied everywhere; tests
// shorten it.
var schemaWaitTimeout = 2 * time.Minute

// Env is what RunImport works with besides the settings: where it prints, what the
// preflight learned and the liaison connections.
type Env struct {
	// Log receives warnings, progress and the verify report (stderr); Out the plan (stdout).
	Log      io.Writer
	Out      io.Writer
	Cluster  *cluster.Info
	Liaisons *cluster.Liaisons
	// Hook, when set, is called after every acknowledged batch (see PushOptions.Hook).
	Hook func(event string)
	// Now is the reference time of the TTL gate; nil uses time.Now.
	Now func() time.Time
	// Terminal reports the size of the terminal Log is, nil when it is not one: the push then
	// redraws a live line per group in place.
	Terminal func() (width, height int)
}

// Result is what RunImport did; fields after the failing phase stay empty.
type Result struct {
	// Plan is the plan printed before anything was written.
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
		fmt.Fprintf(log, "info: the schema is read from the %d schema file(s) of artifact %s\n", len(art.Manifest.SchemaFiles), opts.Input)
		return nil
	}
	if opts.SchemaOnly {
		return exitcode.Exit(exitcode.ExitUsage, "--schema-only needs a schema, and artifact %s was exported without one", opts.Input)
	}
	fmt.Fprintf(log, "info: artifact %s carries no schema; its CSV columns are read with the target's schema\n", opts.Input)
	return nil
}

// RunImport imports the artifact at opts.Input: it checks the artifact,
// compares the schema with the target, asks Targets, decides and prints the plan. A dry run
// stops there (exit 0, or 2 when the import would be refused). A real run then writes the
// schema in dependency order and waits until every node applied it, re-decides and prints
// what changed (--abort-on-plan-change stops with exit 5), pushes the rows and, with
// --verify, reconciles the target's counts (exit 4 on a mismatch). Errors before the schema
// write keep the exit code of their cause (2 for a refusal: nothing was written); any error
// from the schema write on is exit 3. Every error is an exitcode.ExitError.
func RunImport(ctx context.Context, env Env, opts config.Import) (*Result, error) {
	if env.Log == nil {
		env.Log = io.Discard
	}
	if env.Out == nil {
		env.Out = io.Discard
	}
	now := time.Now
	if env.Now != nil {
		now = env.Now
	}
	art, err := OpenArtifact(ctx, opts.Input, true)
	if err != nil {
		return nil, err
	}
	if err = logSchemaSource(env.Log, art, &opts); err != nil {
		return nil, err
	}
	conn := env.Liaisons.Primary()
	d := decider{liaisons: env.Liaisons.All, cluster: env.Cluster, art: art, opts: &opts, endpoint: env.Cluster.Liaison + " (ROLE_LIAISON)"}
	first, err := d.decide(ctx, now(), !opts.SchemaOnly)
	if err != nil {
		return nil, exitcode.Exit(exitcode.For(err), "%w", err)
	}
	for _, l := range first.state.Info {
		fmt.Fprintf(env.Log, "info: %s\n", l)
	}
	res := &Result{Plan: first.plan}
	if err = Render(env.Out, opts.OutputFormat, first.plan); err != nil {
		return res, exitcode.Exit(exitcode.ExitRuntime, "render the import plan: %w", err)
	}
	warnUnneededForce(env.Log, &opts, first)
	if first.plan.Refused() {
		return res, exitcode.Exit(exitcode.ExitPreflight, "the import is refused (see PROBLEMS); nothing was written")
	}
	if opts.DryRun {
		return res, nil
	}

	// From here on the target is being changed: every failure is a runtime failure.
	if res.SchemaWrites, err = writeAndAwaitSchema(ctx, conn, first.diff, art, &opts, env.Log); err != nil {
		return res, err
	}
	if opts.SchemaOnly {
		fmt.Fprintf(env.Log, "schema-only import done: %d resource(s) written, no data sent\n", res.SchemaWrites)
		return res, nil
	}
	// The rows were checked against the predicted schema before anything was written.
	second, err := d.decide(ctx, now(), false)
	if err != nil {
		return res, exitcode.Exit(exitcode.ExitRuntime, "re-check the plan after the schema write: %w", err)
	}
	res.Recheck = second.plan
	res.PlanChanges = ComparePlans(first.plan, second.plan)
	for _, w := range second.plan.Warnings {
		if !slices.Contains(first.plan.Warnings, w) {
			fmt.Fprintf(env.Log, "WARN (re-check after the schema write) %s\n", w)
		}
	}
	if len(res.PlanChanges) > 0 {
		fmt.Fprintln(env.Log, "the plan re-checked after the schema write differs:")
		for _, l := range res.PlanChanges {
			fmt.Fprintf(env.Log, "  %s\n", l)
		}
		if opts.AbortOnPlanChange {
			return res, exitcode.Exit(exitcode.ExitPlanChanged, "the plan changed after the schema write (--abort-on-plan-change); the schema is written, no data was sent")
		}
	}
	if second.plan.Refused() {
		return res, exitcode.Exit(exitcode.ExitRuntime, "the plan re-checked after the schema write refuses the import: %s; the schema is written, no data was sent",
			refusalReasons(second.plan))
	}
	entries := second.plan.Entries
	var pre *Counts
	// With no entries there is nothing to check, and empty selectors would plan every group.
	verify := opts.EffectiveVerify() && len(entries) > 0
	if verify {
		if pre, err = Snapshot(ctx, conn, VerifySelectors(entries)); err != nil {
			return res, exitcode.Exit(exitcode.ExitRuntime, "verify baseline: %w", err)
		}
	}
	workers, reason := WorkerCount(opts.Parallelism, entries)
	window := max(1, opts.WriteInflightBatches)
	fmt.Fprintf(env.Log, "parallelism: %d (%s); up to %d batch(es) in flight per stream, so at most %.1f MiB of rows per worker\n",
		workers, reason, window, float64(uint64(window)*opts.WriteBatchBytes)/(1<<20))
	logRates(env.Log, &opts)
	start := time.Now()
	prog := output.NewProgress(env.Log, env.Terminal, "group(s) importing")
	prog.Start(ctx)
	// Workers print concurrently; through the progress no line tears the live lines.
	push, err := Push(ctx, env.Liaisons.All, art, &second.state.Schemas, entries,
		PushOptions{Log: prog.Wrap(env.Log), Hook: env.Hook, Import: opts, Progress: prog})
	prog.Close()
	res.Push = push
	if err != nil {
		return res, err
	}
	var rows uint64
	for _, n := range push.Rows {
		rows += n
	}
	fmt.Fprintf(env.Log, "IMPORTED  %d rows of %d work item(s) in %s\n", rows, len(push.Done), output.FormatElapsed(time.Since(start)))
	if !verify {
		return res, nil
	}
	vprog := output.NewProgress(env.Log, env.Terminal, "")
	vprog.Start(ctx)
	res.Verify = Verify(ctx, conn, pre, second.plan, second.state.Routes, push.Rows,
		VerifyOptions{Log: vprog.Wrap(env.Log), Progress: vprog, Timeout: opts.VerifyTimeout})
	vprog.Close()
	res.Verify.Render(env.Log)
	if ctx.Err() != nil {
		return res, exitcode.Exit(exitcode.ExitRuntime, "import interrupted during verify: %w", context.Cause(ctx))
	}
	if res.Verify.Failed() {
		return res, exitcode.Exit(exitcode.ExitVerify, "verify failed: %d check(s) not completed, %d count mismatch(es)",
			len(res.Verify.Incomplete), len(res.Verify.Mismatches))
	}
	return res, nil
}

// logRates logs the rate limits of the push when any is set, the wire values the liaisons and
// the data nodes apply.
func logRates(log io.Writer, opts *config.Import) {
	node := ratelimit.Proto(opts.NodeRateLimit, opts.NodeRowRateLimit)
	liaison, client := ratelimit.PerSecond(opts.LiaisonRateLimit), ratelimit.PerSecond(opts.WriteRateLimit)
	if node.GetBytesPerSecond() == 0 && node.GetRowsPerSecond() == 0 && liaison == 0 && client == 0 {
		return
	}
	fmt.Fprintf(log, "rate limits: node %d bytes/s, %d rows/s per target node; "+
		"liaison %d bytes/s per liaison; client %d rows/s (0 = unlimited)\n",
		node.GetBytesPerSecond(), node.GetRowsPerSecond(), liaison, client)
}

// writeAndAwaitSchema writes the planned schema changes and waits until every node applied
// them; it reports how many resources were written.
func writeAndAwaitSchema(ctx context.Context, conn *grpc.ClientConn, diff *SchemaDiff, art *Artifact, opts *config.Import, log io.Writer) (int, error) {
	writes := diff.Writes()
	if !opts.IncludeSchema || len(writes) == 0 {
		return 0, nil
	}
	req, err := WriteSchema(ctx, conn, writes, &art.Schemas)
	if err != nil {
		return 0, err
	}
	if err = AwaitSchema(ctx, conn, req, schemaWaitTimeout); err != nil {
		return len(writes), err
	}
	fmt.Fprintf(log, "schema: %d resource(s) written and applied on every node\n", len(writes))
	return len(writes), nil
}

// decision is one collect + diff + decide round.
type decision struct {
	state *TargetState
	diff  *SchemaDiff
	plan  *Plan
}

type decider struct {
	cluster  *cluster.Info
	art      *Artifact
	opts     *config.Import
	endpoint string
	liaisons []*cluster.Liaison
}

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
	plan := Decide(Inputs{
		Now: now, Artifact: d.art, Target: predicted, Endpoint: d.endpoint, Actions: diff.Actions,
		Flags: Flags{ForceTTL: d.opts.ForceTTL, AllSources: d.opts.AllSources, StrictCoverage: d.opts.StrictCoverage, SchemaOnly: d.opts.SchemaOnly},
	})
	if checkRows && !plan.SchemaBlocked() && len(plan.Errors) == 0 && len(plan.Entries) > 0 {
		rc, checkErr := CheckRows(ctx, d.art, &predicted.Schemas, plan.Entries, d.opts.WriteBatchBytes)
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
	fmt.Fprintf(log, "WARN --force was not needed: every group to overwrite (%v) is empty\n", groups)
}
