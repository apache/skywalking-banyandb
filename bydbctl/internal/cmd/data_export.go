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
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
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
// The artifact format is checked in both modes, so --format native is refused the same way
// with or without --dry-run.
func validateExport(sources config.Sources, opts *config.Export) error {
	if err := validateOutputFormat(sources, opts.OutputFormat); err != nil {
		return err
	}
	if opts.Format == transferv1.Format_FORMAT_NATIVE {
		return exitcode.Exit(exitcode.ExitUsage, "native export format is not implemented yet")
	}
	if opts.DryRun {
		// A real-export setting given explicitly, as a flag or in plan.yaml, contradicts a
		// dry run; one from the environment is simply unused.
		for _, s := range []struct {
			flag string
			set  bool
		}{{"output", opts.Output != ""}, {"preempt", opts.Preempt}, {"schema-only", opts.SchemaOnly}} {
			if sources.FromFlag(s.flag) || (sources.FromPlan(s.flag) && s.set) {
				return exitcode.Exit(exitcode.ExitUsage, "%s cannot be combined with --dry-run, which writes nothing", sources.Label(s.flag))
			}
		}
		return nil
	}
	if sources.FromFlag("strict-coverage") {
		return exitcode.Exit(exitcode.ExitUsage, "--strict-coverage applies to --dry-run only; a real export fails on any unreachable node")
	}
	if err := config.CheckSchemaOnly(sources, opts.SchemaOnly, opts.IncludeSchema); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	return exporter.CheckOutputDir(opts.Output)
}

// runRealExport connects, lists the groups and hands over to exporter.RunExport. No overall
// deadline applies: progress is bounded by --node-idle-timeout instead.
func runRealExport(cmd *cobra.Command, conn *config.Connection, opts *config.Export) error {
	// Ctrl-C or SIGTERM cancels the export so that it stops cleanly and prints how to release
	// the session it keeps.
	out := cmd.OutOrStdout()
	lc, err := connectLiaisons(cmd, conn, 3*time.Minute, out)
	if err != nil {
		return err
	}
	defer lc.close()
	groups, err := cluster.FetchGroups(lc.bounded, lc.liaisons.Primary())
	if err != nil {
		return err
	}
	if err = exporter.ValidateSelectors(groups, opts.Selectors); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	return exporter.RunExport(lc.ctx, exporter.Env{
		Log:      cmd.ErrOrStderr(),
		Out:      out,
		Terminal: terminalSize(out),
		Confirm:  terminalConfirm(cmd.InOrStdin(), out),
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

// terminalConfirm returns the question asker of an operator at a terminal, or nil when the
// input or the output is not a terminal: a script or a pipe is never asked.
func terminalConfirm(in io.Reader, out io.Writer) func(context.Context, string) bool {
	inFile, inOK := in.(*os.File)
	outFile, outOK := out.(*os.File)
	if !inOK || !outOK || !term.IsTerminal(int(inFile.Fd())) || !term.IsTerminal(int(outFile.Fd())) {
		return nil
	}
	return func(ctx context.Context, question string) bool { return askYes(ctx, in, out, question) }
}

// askYes prints question and reads one line: y or yes, in any case, is a yes; anything else,
// the end of the input, or ctx ending first (Ctrl-C while asked) is a no.
func askYes(ctx context.Context, in io.Reader, out io.Writer, question string) bool {
	fmt.Fprint(out, question)
	answer := make(chan string, 1)
	// The read cannot be interrupted; once ctx ends the process exits and the reader with it.
	run.Go(ctx, "bydbctl-export-confirm", logger.GetLogger("bydbctl"), func(context.Context) {
		line, _ := bufio.NewReader(in).ReadString('\n')
		answer <- line
	})
	select {
	case <-ctx.Done():
		fmt.Fprintln(out)
		return false
	case line := <-answer:
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "y", "yes":
			return true
		}
		return false
	}
}

// runDryRun is the read-only inventory of `data export --dry-run`.
func runDryRun(cmd *cobra.Command, conn *config.Connection, opts *config.Export) error {
	errOut := cmd.ErrOrStderr()
	lc, err := connectLiaisons(cmd, conn, 10*time.Minute, errOut)
	if err != nil {
		return err
	}
	defer lc.close()
	ctx, info, primary := lc.bounded, lc.cluster, lc.liaisons.Primary()

	groups, err := cluster.FetchGroups(ctx, primary)
	if err != nil {
		return err
	}
	if err = exporter.ValidateSelectors(groups, opts.Selectors); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	result, err := cluster.RunPlan(ctx, primary, opts.Selectors)
	if err != nil {
		return exitcode.Exit(exitcode.ExitCodeFor(err), "%w", err)
	}
	report := exporter.BuildReport(info, result)
	for _, w := range exporter.StageWarnings(groups, result) {
		fmt.Fprintln(errOut, "WARN "+w)
	}

	if len(report.AnsweredNodes) > 0 {
		effective, clamped := config.ClampParallelism(opts.Parallelism, len(report.AnsweredNodes))
		if clamped {
			fmt.Fprintf(errOut, "parallelism: %s -> %d (clamped by node count)\n", config.FormatParallelism(opts.Parallelism), effective)
		} else {
			fmt.Fprintf(errOut, "parallelism: %d\n", effective)
		}
	}
	if err = exporter.Render(cmd.OutOrStdout(), opts.OutputFormat, report); err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "render report: %w", err)
	}
	if len(report.MissingNodes) > 0 {
		msg := fmt.Sprintf("%d data node(s) returned no inventory: %s", len(report.MissingNodes), strings.Join(report.MissingNodes, ", "))
		if opts.StrictCoverage {
			return exitcode.Exit(exitcode.ExitPreflight, "coverage gap: %s", msg)
		}
		fmt.Fprintln(errOut, "WARN "+msg)
	}
	return nil
}
