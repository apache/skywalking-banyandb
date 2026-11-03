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
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
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
	conn := &config.Connection{}
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
				return exporter.Exit(exporter.ExitUsage, "--id must be a session id (32 lowercase hex characters)")
			}
			req := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id}
			if err := req.ValidateAll(); err != nil {
				return exporter.Exit(exporter.ExitUsage, "--id %q is not a session id (32 lowercase hex characters): %w", id, err)
			}
			if _, err := resolveSettings(cmd, conn); err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 3*time.Minute)
			defer cancel()
			liaisons, cluster, err := exporter.Preflight(ctx, conn.Nodes, connectOptions(cmd, conn))
			if err != nil {
				return err
			}
			defer func() { _ = liaisons.Close() }()
			result, err := exporter.ReleaseSession(ctx, liaisons.Primary(), id)
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
					return exporter.Exit(exporter.ExitRuntime, "export session %s could not be released on %v (via %s); retry later", id, failed, cluster.Liaison)
				}
				return exporter.Exit(exporter.ExitRuntime, "export session %s released except on %v (via %s); retry later", id, failed, cluster.Liaison)
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
	config.Bind(c.Flags(), conn)
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

// connectOptions assembles the dial options of a data command from its resolved connection.
// Skipped liaisons are reported on stderr.
func connectOptions(cmd *cobra.Command, conn *config.Connection) exporter.ConnectOptions {
	return exporter.ConnectOptions{
		Log:       cmd.ErrOrStderr(),
		EnableTLS: conn.EnableTLS,
		Insecure:  conn.Insecure,
		Cert:      conn.Cert,
		Username:  viper.GetString("username"),
		Password:  viper.GetString("password"),
		Timeout:   10 * time.Second,
	}
}

// resolveSettings rejects the root -a/-g/--config flags, resolves the option structs (the
// first is the connection) with flag > plan.yaml > BYDBCTL_* > default, initializes logging
// and requires a liaison address. Every failure is a usage error.
func resolveSettings(cmd *cobra.Command, conn *config.Connection, opts ...any) (config.Sources, error) {
	if err := rejectRootScopeFlags(); err != nil {
		return nil, err
	}
	sources, err := config.Resolve(cmd.Flags(), os.LookupEnv, append([]any{conn}, opts...)...)
	if err != nil {
		return nil, exporter.Exit(exporter.ExitUsage, "%w", err)
	}
	if err = logger.Init(logger.Logging{Env: conn.LoggingEnv, Level: conn.LoggingLevel}); err != nil {
		return nil, exporter.Exit(exporter.ExitUsage, "init logger: %w", err)
	}
	if len(conn.Nodes) == 0 {
		return nil, exporter.Exit(exporter.ExitUsage, "no liaison address: give --nodes or connection.nodes in plan.yaml")
	}
	return sources, nil
}
