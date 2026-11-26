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
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

func newDataExportCmd() *cobra.Command {
	conn, opts := &config.Connection{}, &config.Export{}
	c := &cobra.Command{
		Use:           "export",
		Short:         "Export data from a BanyanDB cluster in the --format artifact format, or inventory it with --dry-run",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
	}
	config.Bind(c.Flags(), conn, opts)
	c.PreRunE = func(cmd *cobra.Command, _ []string) error {
		sources, err := resolveSettings(cmd, conn, opts)
		if err != nil {
			return err
		}
		return validateExport(sources, opts)
	}
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		if !opts.DryRun {
			return runRealExport(cmd, conn, opts)
		}
		return runDryRun(cmd, conn, opts)
	}
	return c
}

// validateExport checks the combinations of resolved settings before anything is contacted.
// The artifact format is checked in both modes: a native export defaults to no compression
// and refuses gzip or a volume limit too small for a container, with or without --dry-run.
func validateExport(sources config.Sources, opts *config.Export) error {
	if err := validateOutputFormat(sources, &opts.OutputFormat); err != nil {
		return err
	}
	if opts.Format == transferv1.Format_FORMAT_NATIVE {
		// A setting a native export does not use is refused when given as a flag or in
		// plan.yaml, and is ignored when it comes from the environment.
		for _, s := range []struct {
			setting any
			reset   func()
			why     string
			set     bool
		}{
			{
				&opts.Compress, func() { opts.Compress = transferv1.Compress_COMPRESS_NONE }, "a native export is not compressed; use --compress none",
				opts.Compress != transferv1.Compress_COMPRESS_NONE,
			},
			{
				&opts.NodeRowRateLimit, func() { opts.NodeRowRateLimit = 0 },
				"a native export moves part bytes and has no rows to limit; it applies to --format csv only", opts.NodeRowRateLimit > 0,
			},
			{&opts.CSVAnnotations, func() { opts.CSVAnnotations = false }, "a native export has no CSV headers; it applies to --format csv only", opts.CSVAnnotations},
		} {
			if !s.set {
				continue
			}
			if sources.Explicit(s.setting) {
				return exitcode.Exit(exitcode.ExitUsage, "%s: %s", sources.Label(s.setting), s.why)
			}
			s.reset()
		}
	}
	if err := exporter.CheckFormat(opts); err != nil {
		return err
	}
	if err := exporter.CheckResume(sources, opts); err != nil {
		return err
	}
	if opts.DryRun {
		// A real-export setting given explicitly, as a flag or in plan.yaml, contradicts a
		// dry run; one from the environment is simply unused.
		for _, s := range []struct {
			setting *bool
			set     bool
		}{{&opts.Preempt, opts.Preempt}, {&opts.SchemaOnly, opts.SchemaOnly}} {
			if sources.FromFlag(s.setting) || (sources.FromPlan(s.setting) && s.set) {
				return exitcode.Exit(exitcode.ExitUsage, "%s cannot be combined with --dry-run, which writes nothing", sources.Label(s.setting))
			}
		}
		return nil
	}
	if sources.FromFlag(&opts.StrictCoverage) {
		return exitcode.Exit(exitcode.ExitUsage, "--strict-coverage applies to --dry-run only; a real export fails on any unreachable node")
	}
	if err := config.CheckSchemaOnly(sources, &opts.SchemaOnly, &opts.IncludeSchema); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	return exporter.CheckOutputDir(opts.Output, exporter.ProgressPath(*opts))
}

// runRealExport connects, lists the groups and hands over to exporter.RunExport. No overall
// deadline applies: progress is bounded by --node-idle-timeout instead.
func runRealExport(cmd *cobra.Command, conn *config.Connection, opts *config.Export) error {
	// Ctrl-C or SIGTERM cancels the export so that it stops cleanly and prints how to release
	// the session it keeps. Everything but the explanation of a failure and its error goes to
	// stdout, the questions too.
	out := cmd.OutOrStdout()
	lc, err := connectLiaisons(cmd, conn, 3*time.Minute, dataStreams{info: out, hints: out, out: out})
	if err != nil {
		return err
	}
	defer lc.close()
	groups, err := cluster.FetchGroups(lc.bounded, lc.liaisons.Primary())
	if err != nil {
		return err
	}
	ask, wrap := terminalConfirm(cmd.InOrStdin(), out, cmd.ErrOrStderr())
	return exporter.RunExport(lc.ctx, exporter.Env{
		Log:      wrap(out),
		Out:      wrap(out),
		Err:      cmd.ErrOrStderr(),
		Terminal: terminalSize(out),
		Confirm:  ask,
		Cluster:  lc.cluster,
		Liaisons: lc.liaisons,
		Groups:   groups,
	}, *opts)
}

// terminalSize returns the size reader of the terminal w is, or nil when w is not one.
func terminalSize(w io.Writer) func() (width, height int) {
	f, ok := w.(*os.File)
	if !ok || !term.IsTerminal(int(f.Fd())) {
		return nil
	}
	return func() (width, height int) {
		width, height, err := term.GetSize(int(f.Fd()))
		if err != nil {
			return 0, 0
		}
		return width, height
	}
}

// runDryRun is the read-only inventory of `data export --dry-run`.
func runDryRun(cmd *cobra.Command, conn *config.Connection, opts *config.Export) error {
	s := streamsOf(cmd, opts.OutputFormat)
	lc, err := connectLiaisons(cmd, conn, 10*time.Minute, s)
	if err != nil {
		return err
	}
	defer lc.close()
	ctx, info, primary := lc.bounded, lc.cluster, lc.liaisons.Primary()

	groups, err := cluster.FetchGroups(ctx, primary)
	if err != nil {
		return err
	}
	resumed, err := exporter.DryRunResume(exporter.Env{
		Log: s.info, Out: s.out, Cluster: info, Liaisons: lc.liaisons, Groups: groups,
	}, *opts)
	if resumed || err != nil {
		return err
	}
	// A preview of a resume does not check the selectors: the resume compares them with the
	// recorded ones.
	if err = exporter.ValidateSelectors(groups, opts.Selectors); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	result, err := cluster.RunPlan(ctx, primary, opts.Selectors)
	if err != nil {
		return exitcode.Exit(exitcode.For(err), "%w", err)
	}
	report := exporter.BuildReport(info, result)
	for _, w := range exporter.StageWarnings(groups, result) {
		fmt.Fprintln(s.info, "WARN "+w)
	}

	if len(report.AnsweredNodes) > 0 {
		effective, clamped := config.ClampParallelism(opts.Parallelism, len(report.AnsweredNodes))
		if clamped {
			fmt.Fprintf(s.hints, "parallelism: %s -> %d (clamped by node count)\n", config.FormatParallelism(opts.Parallelism), effective)
		} else {
			fmt.Fprintf(s.hints, "parallelism: %d\n", effective)
		}
	}
	if err = exporter.Render(s.out, opts.OutputFormat, report); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "render report: %w", err)
	}
	if len(report.MissingNodes) > 0 {
		msg := fmt.Sprintf("%d data node(s) returned no inventory: %s", len(report.MissingNodes), strings.Join(report.MissingNodes, ", "))
		if opts.StrictCoverage {
			return exitcode.Exit(exitcode.ExitPreflight, "coverage gap: %s", msg)
		}
		fmt.Fprintln(s.info, "WARN "+msg)
	}
	return nil
}
