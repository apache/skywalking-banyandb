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
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// validateOutputFormat checks --output-format as a usage error naming its source; export and
// import both run it first.
func validateOutputFormat(sources config.Sources, format string) error {
	if err := output.ValidateOutputFormat(format); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%s: %w", sources.Label("output-format"), err)
	}
	return nil
}

func newDataCmd() *cobra.Command {
	dataCmd := &cobra.Command{
		Use:           "data",
		Short:         "Export data from or import data into a cluster through the liaison gRPC endpoint",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	dataCmd.AddCommand(newDataExportCmd(), newDataImportCmd(), newReleaseSessionCmd())
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
				return exitcode.Exit(exitcode.ExitUsage, "--id must be a session id (32 lowercase hex characters)")
			}
			req := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id}
			if err := req.ValidateAll(); err != nil {
				return exitcode.Exit(exitcode.ExitUsage, "--id %q is not a session id (32 lowercase hex characters): %w", id, err)
			}
			if _, err := resolveSettings(cmd, conn); err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 3*time.Minute)
			defer cancel()
			liaisons, info, err := cluster.Preflight(ctx, conn.Nodes, connectOptions(cmd, conn))
			if err != nil {
				return err
			}
			defer func() { _ = liaisons.Close() }()
			result, err := exporter.ReleaseSession(ctx, liaisons.Primary(), id)
			if err != nil {
				return exitcode.Exit(exitcode.For(err), "%w", err)
			}
			var failed []string
			for _, frame := range result.Failed {
				failed = append(failed, frame.GetNodeId())
				fmt.Fprintln(cmd.ErrOrStderr(), "release failed on "+frame.GetNodeId()+": "+frame.GetError())
			}
			if len(failed) > 0 {
				if len(result.Released) == 0 {
					return exitcode.Exit(exitcode.ExitRuntime, "export session %s could not be released on %v (via %s); retry later", id, failed, info.Liaison)
				}
				return exitcode.Exit(exitcode.ExitRuntime, "export session %s released except on %v (via %s); retry later", id, failed, info.Liaison)
			}
			if len(result.Released) == 0 {
				return exitcode.Exit(exitcode.ExitPreflight, "export session %s is not held on any data node", id)
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
// commands take liaison gRPC addresses through --nodes, their scope through --selector
// (export) or the artifact (import) and their settings through --plan; they never read the
// config file. A group set in the environment is simply ignored. command names the data
// command for the message.
func rejectRootScopeFlags(command string) error {
	if explicitRootFlags["addr"] {
		return exitcode.Exit(exitcode.ExitUsage, "-a/--addr is the liaison HTTP endpoint; data %s takes liaison gRPC addresses via --nodes", command)
	}
	if explicitRootFlags["group"] {
		scope := "the scope comes from --selector"
		switch command {
		case "import":
			scope = "the groups come from the artifact (--input)"
		case "release-session":
			scope = "a session covers the groups it was created for"
		}
		return exitcode.Exit(exitcode.ExitUsage, "-g/--group does not apply to data commands; %s", scope)
	}
	if explicitRootFlags["config"] {
		return exitcode.Exit(exitcode.ExitUsage, "--config does not apply to data commands; give the settings of data %s with flags, --plan or BYDBCTL_* variables", command)
	}
	return nil
}

// connectOptions assembles the dial options of a data command from its resolved connection.
// Skipped liaisons are reported on stderr.
func connectOptions(cmd *cobra.Command, conn *config.Connection) cluster.ConnectOptions {
	return cluster.ConnectOptions{
		Log:       cmd.ErrOrStderr(),
		EnableTLS: conn.EnableTLS,
		Insecure:  conn.Insecure,
		Cert:      conn.Cert,
		Username:  viper.GetString("username"),
		Password:  viper.GetString("password"),
		Timeout:   10 * time.Second,
	}
}

// liaisonConn is a data command's connection to its liaisons.
type liaisonConn struct {
	// ctx ends on Ctrl-C or SIGTERM; bounded is ctx limited to the connect timeout.
	ctx      context.Context
	bounded  context.Context
	liaisons *cluster.Liaisons
	cluster  *cluster.Info
	cancel   context.CancelFunc
	stop     context.CancelFunc
}

// close closes the liaison connections and stops listening for signals.
func (c *liaisonConn) close() {
	_ = c.liaisons.Close()
	c.cancel()
	c.stop()
}

// connectLiaisons runs the preflight of a data command: Ctrl-C or SIGTERM cancels ctx, the
// preflight runs within timeout, and the liaison it reached is printed on out. The caller
// must close the connection.
func connectLiaisons(cmd *cobra.Command, conn *config.Connection, timeout time.Duration, out io.Writer) (*liaisonConn, error) {
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	bounded, cancel := context.WithTimeout(ctx, timeout)
	liaisons, info, err := cluster.Preflight(bounded, conn.Nodes, connectOptions(cmd, conn))
	if err != nil {
		cancel()
		stop()
		return nil, err
	}
	fmt.Fprintf(out, "liaison: %s (standalone=%v), data nodes: %d\n", info.Liaison, info.Standalone, len(info.DataNodes))
	return &liaisonConn{ctx: ctx, bounded: bounded, liaisons: liaisons, cluster: info, cancel: cancel, stop: stop}, nil
}

// resolveSettings rejects the root -a/-g/--config flags, resolves the option structs (the
// first is the connection) with flag > plan.yaml > BYDBCTL_* > default, initializes logging
// and requires a liaison address. Every failure is a usage error.
func resolveSettings(cmd *cobra.Command, conn *config.Connection, opts ...any) (config.Sources, error) {
	if err := rejectRootScopeFlags(cmd.Name()); err != nil {
		return nil, err
	}
	sources, err := config.Resolve(cmd.Flags(), os.LookupEnv, append([]any{conn}, opts...)...)
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	if err = logger.Init(logger.Logging{Env: conn.LoggingEnv, Level: conn.LoggingLevel}); err != nil {
		return nil, exitcode.Exit(exitcode.ExitUsage, "init logger: %w", err)
	}
	if len(conn.Nodes) == 0 {
		return nil, exitcode.Exit(exitcode.ExitUsage, "no liaison address: give --nodes or connection.nodes in plan.yaml")
	}
	return sources, nil
}
