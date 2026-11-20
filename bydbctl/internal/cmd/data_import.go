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

package cmd

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

// dryRunIgnoredImportSettings are the import settings a dry run does not use: they only shape
// the writes, the push and the verification. --write-batch-bytes, --resume-duplication-policy and
// --restart are the exceptions: the dry run plan warns about rows larger than the first, its
// resume summary names the policy of every ambiguous payload, and with --restart it previews a
// fresh import. The settings are still range-checked under
// --dry-run, except --write-batch-size, --write-inflight-batches and --checkpoint-every-batches.
func dryRunIgnoredImportSettings(o *config.Import) []any {
	return []any{
		&o.AbortOnPlanChange, &o.Verify, &o.VerifyTimeout, &o.WriteBatchSize, &o.WriteInflightBatches, &o.WriteRateLimit,
		&o.SendRetryTimeout, &o.LiaisonRateLimit, &o.NodeRateLimit, &o.NodeRowRateLimit, &o.Parallelism,
		&o.PartRateLimit, &o.PartChunkSize, &o.CheckpointEveryBatches, &o.Resume,
	}
}

func newDataImportCmd() *cobra.Command {
	conn, opts := &config.Connection{}, &config.Import{}
	c := &cobra.Command{
		Use:           "import",
		Short:         "Import a CSV or native artifact written by data export into a cluster, or print the import plan with --dry-run",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
	}
	config.Bind(c.Flags(), conn, opts)
	c.Flags().Lookup(config.PlanFlag).Usage = "plan.yaml describing connection and import options"
	var sources config.Sources
	c.PreRunE = func(cmd *cobra.Command, _ []string) error {
		var err error
		if sources, err = resolveSettings(cmd, conn, opts); err != nil {
			return err
		}
		return validateImport(cmd, sources, opts)
	}
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		return runImport(cmd, conn, opts, sources)
	}
	return c
}

// validateImport checks the resolved settings before anything is contacted; every failure is
// a usage error (exit 1). Under --dry-run the settings that only shape the writes are
// ignored, with one notice line naming those given explicitly.
func validateImport(cmd *cobra.Command, sources config.Sources, opts *config.Import) error {
	if err := validateOutputFormat(sources, &opts.OutputFormat); err != nil {
		return err
	}
	if opts.Input == "" {
		return exitcode.Exit(exitcode.ExitUsage, "no artifact: give --input or import.input in plan.yaml")
	}
	if err := opts.Validate(sources); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	if !opts.DryRun {
		return nil
	}
	var ignored []string
	for _, setting := range dryRunIgnoredImportSettings(opts) {
		if sources.Explicit(setting) {
			ignored = append(ignored, sources.Label(setting))
		}
	}
	if len(ignored) > 0 {
		fmt.Fprintf(streamsOf(cmd, opts.OutputFormat).info, "NOTE --dry-run writes nothing and ignores %s\n", strings.Join(ignored, ", "))
	}
	return nil
}

// runImport prints the liaisons it uses and where they came from, connects and hands over to
// importer.RunImport. Ctrl-C or SIGTERM stops the import, which then fails with exit 3 and
// lists the work items done; the progress file lets the same command resume it. Everything but
// the explanation of a failure and its error goes to stdout, the questions too; with -o json|yaml
// stdout holds the plan only (streamsOf).
func runImport(cmd *cobra.Command, conn *config.Connection, opts *config.Import, sources config.Sources) error {
	s := streamsOf(cmd, opts.OutputFormat)
	fmt.Fprintf(s.hints, "liaisons: %s (from %s)\n", strings.Join(conn.Nodes, ", "), sources.Label(&conn.Nodes))
	lc, err := connectLiaisons(cmd, conn, 3*time.Minute, s)
	if err != nil {
		return err
	}
	defer lc.close()
	env := sourcedEnv(sources, opts)
	var wrap func(io.Writer) io.Writer
	env.Confirm, wrap = terminalConfirm(cmd.InOrStdin(), s.info, cmd.ErrOrStderr())
	env.Log, env.Out, env.Err, env.Hints, env.Cluster, env.Liaisons = wrap(s.info), wrap(s.out), cmd.ErrOrStderr(), wrap(s.hints), lc.cluster, lc.liaisons
	env.Terminal = terminalSize(s.info)
	_, err = importer.RunImport(lc.ctx, env, *opts)
	return err
}

// sourcedEnv is the part of the importer.Env that where the settings came from decides. Only the
// flag asks to verify a finished import again; plan.yaml and the variable set a default.
func sourcedEnv(sources config.Sources, opts *config.Import) importer.Env {
	return importer.Env{
		VerifyExplicit: sources.FromFlag(&opts.Verify), ResumeFromEnv: sources.FromEnv(&opts.Resume),
		DuplicationSource: sources.Label(&opts.DuplicationPolicy),
	}
}
