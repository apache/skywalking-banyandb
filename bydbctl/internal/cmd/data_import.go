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

// dryRunIgnoredImportFlags are the import settings a dry run does not use: they only shape
// the writes, the push and the verification. --write-batch-bytes is not one of them: the dry
// run plan warns about rows larger than it, so it is range-checked under --dry-run too.
var dryRunIgnoredImportFlags = []string{
	"abort-on-plan-change", "verify", "verify-timeout", "write-batch-size", "write-inflight-batches",
	"write-rate-limit",
	"send-retry-timeout", "liaison-rate-limit", "node-rate-limit", "node-row-rate-limit", "parallelism",
}

func newDataImportCmd() *cobra.Command {
	conn, opts := &config.Connection{}, &config.Import{}
	c := &cobra.Command{
		Use:           "import",
		Short:         "Import a CSV artifact written by data export into a cluster, or print the import plan with --dry-run",
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
		return validateImport(cmd.ErrOrStderr(), sources, opts)
	}
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		return runImport(cmd, conn, opts, sources)
	}
	return c
}

// validateImport checks the resolved settings before anything is contacted; every failure is
// a usage error (exit 1). Under --dry-run the settings that only shape the writes are
// ignored, with one notice line naming those given explicitly.
func validateImport(errOut io.Writer, sources config.Sources, opts *config.Import) error {
	if err := validateOutputFormat(sources, opts.OutputFormat); err != nil {
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
	for _, f := range dryRunIgnoredImportFlags {
		if sources.FromFlag(f) || sources.FromPlan(f) {
			ignored = append(ignored, sources.Label(f))
		}
	}
	if len(ignored) > 0 {
		fmt.Fprintf(errOut, "NOTE --dry-run writes nothing and ignores %s\n", strings.Join(ignored, ", "))
	}
	return nil
}

// runImport prints the liaisons it uses and where they came from, connects and hands over to
// importer.RunImport. Ctrl-C or SIGTERM stops the import, which then fails with exit 3 and
// lists the work items done.
func runImport(cmd *cobra.Command, conn *config.Connection, opts *config.Import, sources config.Sources) error {
	errOut := cmd.ErrOrStderr()
	fmt.Fprintf(errOut, "liaisons: %s (from %s)\n", strings.Join(conn.Nodes, ", "), sources.Label("nodes"))
	lc, err := connectLiaisons(cmd, conn, 3*time.Minute, errOut)
	if err != nil {
		return err
	}
	defer lc.close()
	_, err = importer.RunImport(lc.ctx, importer.Env{
		Log:      errOut,
		Out:      cmd.OutOrStdout(),
		Cluster:  lc.cluster,
		Liaisons: lc.liaisons,
		Terminal: terminalSize(errOut),
	}, *opts)
	return err
}
