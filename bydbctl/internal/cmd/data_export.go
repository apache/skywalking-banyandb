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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

type dataExportFlags struct {
	outputFormat   string
	plan           planFlags
	dryRun         bool
	strictCoverage bool
}

func newDataExportCmd() *cobra.Command {
	f := &dataExportFlags{}
	c := &cobra.Command{
		Use:           "export",
		Short:         "Export data from a BanyanDB cluster (currently the --dry-run inventory only)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
	}
	bindPlanFlags(c, &f.plan)
	c.Flags().BoolVar(&f.dryRun, "dry-run", false,
		"inventory only: list units and estimated sizes without creating snapshots or writing files (required: the real export is not implemented yet)")
	c.Flags().StringVarP(&f.outputFormat, "output-format", "o", "table", "dry-run rendering: table|yaml|json")
	c.Flags().BoolVar(&f.strictCoverage, "strict-coverage", false, "treat a data node that returned no inventory as an error (exit 2)")

	var plan *resolvedPlan
	c.PreRunE = func(cmd *cobra.Command, _ []string) error {
		var err error
		if plan, err = resolvePlan(cmd, &f.plan); err != nil {
			return err
		}
		if err = exporter.ValidateOutputFormat(f.outputFormat); err != nil {
			return exporter.Exit(exporter.ExitUsage, "--output-format: %w", err)
		}
		if !f.dryRun {
			return exporter.Exit(exporter.ExitUsage, "data export without --dry-run is not implemented yet; run with --dry-run to inventory the cluster")
		}
		return nil
	}
	c.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Minute)
		defer cancel()
		conn, cluster, err := exporter.Preflight(ctx, plan.nodes, connectOptions())
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		errOut := cmd.ErrOrStderr()
		fmt.Fprintf(errOut, "liaison: %s (standalone=%v), data nodes: %d\n", cluster.Liaison, cluster.Standalone, len(cluster.DataNodes))

		groups, err := exporter.FetchGroups(ctx, conn)
		if err != nil {
			return err
		}
		if err = exporter.ValidateSelectors(groups, plan.selectors); err != nil {
			return exporter.Exit(exporter.ExitUsage, "%w", err)
		}
		result, err := exporter.RunPlan(ctx, conn, plan.selectors)
		if err != nil {
			return exporter.Exit(exporter.ExitCodeFor(err), "%w", err)
		}
		report := exporter.BuildReport(cluster, result)
		for _, w := range exporter.StageWarnings(groups, result) {
			fmt.Fprintln(errOut, "WARN "+w)
		}

		if len(report.AnsweredNodes) > 0 {
			effective, clamped := transfer.ClampParallelism(plan.parallelism, len(report.AnsweredNodes))
			if clamped {
				fmt.Fprintf(errOut, "parallelism: %s -> %d (clamped by node count)\n", plan.parallelismRaw, effective)
			} else {
				fmt.Fprintf(errOut, "parallelism: %d\n", effective)
			}
		}
		if err = exporter.Render(cmd.OutOrStdout(), f.outputFormat, report); err != nil {
			return exporter.Exit(exporter.ExitRuntime, "render report: %w", err)
		}
		if len(report.MissingNodes) > 0 {
			msg := fmt.Sprintf("%d data node(s) returned no inventory: %s", len(report.MissingNodes), strings.Join(report.MissingNodes, ", "))
			if f.strictCoverage {
				return exporter.Exit(exporter.ExitPreflight, "coverage gap: %s", msg)
			}
			fmt.Fprintln(errOut, "WARN "+msg)
		}
		return nil
	}
	return c
}
