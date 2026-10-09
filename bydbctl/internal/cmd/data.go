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
	"github.com/spf13/viper"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

func newDataCmd() *cobra.Command {
	dataCmd := &cobra.Command{
		Use:           "data",
		Short:         "Export data through the liaison gRPC endpoint",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	dataCmd.AddCommand(newDataExportCmd(), newReleaseSessionCmd())
	return dataCmd
}

// newReleaseSessionCmd is `data release-session`: it releases an export session snapshot on
// every data node.
func newReleaseSessionCmd() *cobra.Command {
	var id string
	f := &connectionFlags{}
	c := &cobra.Command{
		Use:           "release-session",
		Short:         "Release an export session snapshot on every data node",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// The request's own validation rules make a malformed --id a usage error here
			// rather than an INVALID_ARGUMENT round trip; the rule admits the empty id
			// ACTION_LIST ignores, so emptiness is checked explicitly.
			if id == "" {
				return exporter.Exit(exporter.ExitUsage, "--id must be a session id (1-64 lowercase hex characters)")
			}
			req := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id}
			if err := req.ValidateAll(); err != nil {
				return exporter.Exit(exporter.ExitUsage, "--id %q is not a session id (1-64 lowercase hex characters): %w", id, err)
			}
			_, nodes, err := resolveConnection(cmd, f)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 3*time.Minute)
			defer cancel()
			conn, cluster, err := exporter.Preflight(ctx, nodes, connectOptions())
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			result, err := exporter.ReleaseSession(ctx, conn, id)
			if err != nil {
				return exporter.Exit(exporter.ExitCodeFor(err), "%w", err)
			}
			var failed []string
			for _, frame := range result.Failed {
				failed = append(failed, frame.GetNodeId())
				fmt.Fprintln(cmd.ErrOrStderr(), "release failed on "+frame.GetNodeId()+": "+frame.GetError())
			}
			if len(failed) > 0 {
				if len(result.Released) == 0 {
					return exporter.Exit(exporter.ExitRuntime, "session %s could not be released on %v (via %s); retry later", id, failed, cluster.Liaison)
				}
				return exporter.Exit(exporter.ExitRuntime, "session %s released except on %v (via %s); retry later", id, failed, cluster.Liaison)
			}
			if len(result.Released) == 0 {
				return exporter.Exit(exporter.ExitPreflight, "export session %s is not held on any data node", id)
			}
			if len(result.NotHeld) > 0 {
				fmt.Fprintf(cmd.ErrOrStderr(), "export session %s was not held on %v\n", id, result.NotHeld)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "export session %s released\n", id)
			return nil
		},
	}
	c.Flags().StringVar(&id, "id", "", "export session id (from the first Plan frame)")
	_ = c.MarkFlagRequired("id")
	bindConnectionFlags(c, f)
	return c
}

// isDataCommand reports whether c is the data command or one of its subcommands, which do
// not use the bydbctl config file.
func isDataCommand(c *cobra.Command) bool {
	for ; c != nil && c.HasParent(); c = c.Parent() {
		if c.Name() == "data" && !c.Parent().HasParent() {
			return true
		}
	}
	return false
}

// rejectRootScopeFlags refuses an explicit root -a/--addr, -g/--group or --config flag: data
// commands take liaison gRPC addresses through --nodes, their scope through --selector and
// their settings through --plan; they never read the config file. A group set in the
// environment is simply ignored.
func rejectRootScopeFlags() error {
	if explicitRootFlags["addr"] {
		return exporter.Exit(exporter.ExitUsage, "-a/--addr is the liaison HTTP endpoint; data commands take liaison gRPC addresses via --nodes")
	}
	if explicitRootFlags["group"] {
		return exporter.Exit(exporter.ExitUsage, "-g/--group does not apply to data commands; the scope comes from --selector")
	}
	if explicitRootFlags["config"] {
		return exporter.Exit(exporter.ExitUsage, "--config does not apply to data commands; give their settings with flags, --plan or BYDBCTL_* variables")
	}
	return nil
}

// connectOptions assembles the dial options shared by every data command.
func connectOptions() exporter.ConnectOptions {
	return exporter.ConnectOptions{
		EnableTLS: enableTLS,
		Insecure:  insecure,
		Cert:      cert,
		Username:  viper.GetString("username"),
		Password:  viper.GetString("password"),
		Timeout:   10 * time.Second,
	}
}

// resolveString applies the precedence for one scalar setting:
// explicit flag > plan.yaml > BYDBCTL_* environment.
func resolveString(cmd *cobra.Command, flag, flagValue, planValue, viperKey, def string) string {
	if cmd.Flags().Changed(flag) {
		return flagValue
	}
	if planValue != "" {
		return planValue
	}
	if v := viper.GetString(viperKey); v != "" {
		return v
	}
	return def
}

// connectionFlags are the connection flags every data command takes: the plan file itself,
// the liaison addresses (plan.yaml connection.nodes) and the logging flags, which plan.yaml
// does not cover (the TLS flags are the root ones).
type connectionFlags struct {
	planFile     string
	loggingLevel string
	loggingEnv   string
	nodes        []string
}

// bindConnectionFlags registers --plan, --nodes, the logging flags and the TLS flags.
func bindConnectionFlags(c *cobra.Command, f *connectionFlags) {
	c.Flags().StringVar(&f.planFile, "plan", "", "plan.yaml describing connection, scope and parallelism")
	c.Flags().StringSliceVar(&f.nodes, "nodes", nil, "liaison gRPC addresses (host:17912), tried in order")
	c.Flags().StringVar(&f.loggingLevel, "logging-level", "info", "log level")
	c.Flags().StringVar(&f.loggingEnv, "logging-env", "prod", "log environment: prod|dev")
	bindTLSRelatedFlag(c)
}

// resolveConnection initializes logging, rejects the root -a/-g flags, reads plan.yaml when
// given, resolves the liaison address list and lets each connection.nodesTLS setting take
// effect unless its flag was given explicitly.
func resolveConnection(cmd *cobra.Command, f *connectionFlags) (*transfer.Plan, []string, error) {
	if err := logger.Init(logger.Logging{Env: f.loggingEnv, Level: f.loggingLevel}); err != nil {
		return nil, nil, exporter.Exit(exporter.ExitUsage, "init logger: %w", err)
	}
	if err := rejectRootScopeFlags(); err != nil {
		return nil, nil, err
	}
	plan := &transfer.Plan{}
	if f.planFile != "" {
		var err error
		if plan, err = transfer.LoadPlanFile(f.planFile); err != nil {
			return nil, nil, exporter.Exit(exporter.ExitUsage, "%w", err)
		}
	}
	nodes := resolveNodes(cmd, f.nodes, plan.Connection.Nodes)
	if len(nodes) == 0 {
		return nil, nil, exporter.Exit(exporter.ExitUsage, "no liaison address: give --nodes or connection.nodes in plan.yaml")
	}
	mergeNodesTLS(cmd, plan.Connection.NodesTLS)
	return plan, nodes, nil
}

// planFlags are every plan.yaml setting a data export takes: the connection plus the scope
// (--selector) and the node parallelism.
type planFlags struct {
	parallelism string
	selectors   []string
	connection  connectionFlags
}

// bindPlanFlags registers the connection flags plus --selector and --parallelism.
func bindPlanFlags(c *cobra.Command, f *planFlags) {
	bindConnectionFlags(c, &f.connection)
	c.Flags().StringArrayVar(&f.selectors, "selector", nil, "scope, repeatable: catalog=<stream|measure|trace|property>[,groups=<g1>;<g2>]")
	c.Flags().StringVar(&f.parallelism, "parallelism", "max", "number of data nodes exported concurrently: max or an integer >= 1")
}

// resolvedPlan is a plan whose every setting went through the flag > plan.yaml > BYDBCTL_*
// environment precedence.
type resolvedPlan struct {
	parallelismRaw string
	nodes          []string
	selectors      []*transferv1.Selector
	parallelism    int
}

// resolvePlan resolves the connection, the parallelism and the selectors of a data export.
func resolvePlan(cmd *cobra.Command, f *planFlags) (*resolvedPlan, error) {
	plan, nodes, err := resolveConnection(cmd, &f.connection)
	if err != nil {
		return nil, err
	}
	out := &resolvedPlan{nodes: nodes}
	out.parallelismRaw = resolveString(cmd, "parallelism", f.parallelism, plan.Export.Parallelism.String(), "parallelism", "max")
	if out.parallelism, err = transfer.ParseParallelism(out.parallelismRaw); err != nil {
		source := "BYDBCTL_PARALLELISM"
		switch {
		case cmd.Flags().Changed("parallelism"):
			source = "--parallelism"
		case plan.Export.Parallelism != "":
			source = "plan.yaml export.parallelism"
		}
		return nil, exporter.Exit(exporter.ExitUsage, "%s: %w", source, err)
	}
	cfgs, source := plan.Export.Selectors, "plan.yaml export.selectors"
	if cmd.Flags().Changed("selector") {
		cfgs, source = nil, "--selector"
		for _, raw := range f.selectors {
			s, parseErr := transfer.ParseSelector(raw)
			if parseErr != nil {
				return nil, exporter.Exit(exporter.ExitUsage, "--selector: %w", parseErr)
			}
			cfgs = append(cfgs, s)
		}
	}
	for i := range cfgs {
		s, protoErr := cfgs[i].ToProto()
		if protoErr != nil {
			return nil, exporter.Exit(exporter.ExitUsage, "%s[%d]: %w", source, i, protoErr)
		}
		out.selectors = append(out.selectors, s)
	}
	return out, nil
}

// mergeNodesTLS applies plan.yaml connection.nodesTLS flag by flag: an explicit --enable-tls,
// --insecure or --cert keeps its value, the others take the file's.
func mergeNodesTLS(cmd *cobra.Command, tls transfer.TLSConfig) {
	if !cmd.Flags().Changed("enable-tls") {
		enableTLS = tls.Enable
	}
	if !cmd.Flags().Changed("insecure") {
		insecure = tls.Insecure
	}
	if !cmd.Flags().Changed("cert") {
		cert = tls.Cert
	}
}

// resolveNodes applies the same precedence to the liaison address list. The environment
// value is comma separated.
func resolveNodes(cmd *cobra.Command, flagValue, planValue []string) []string {
	if cmd.Flags().Changed("nodes") {
		return flagValue
	}
	if len(planValue) > 0 {
		return planValue
	}
	var out []string
	for _, item := range viper.GetStringSlice("nodes") {
		for _, n := range strings.Split(item, ",") {
			if n = strings.TrimSpace(n); n != "" {
				out = append(out, n)
			}
		}
	}
	return out
}
