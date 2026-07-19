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
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/codex"
	tuiapp "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/app"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	tuibysql "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const agentProviderCodex = "codex"

var errCodexCommandRequired = errors.New("--codex-command is required")

func newAgentCmd() *cobra.Command {
	var codexCommand string
	var initialGoal string
	var initialStart string
	var initialEnd string
	var queryTimeout time.Duration
	var logDir string
	agentCmd := &cobra.Command{
		Use:     "agent",
		Version: version.Build(),
		Short:   "Open the interactive BYDBQL agent TUI",
		RunE: func(_ *cobra.Command, _ []string) error {
			if strings.TrimSpace(codexCommand) == "" {
				return errCodexCommandRequired
			}
			workingDirectory, wdErr := os.MkdirTemp("", "bydbctl-agent-cwd-")
			if wdErr != nil {
				return fmt.Errorf("failed to create isolated agent working directory: %w", wdErr)
			}
			defer func() {
				_ = os.RemoveAll(workingDirectory)
			}()
			executor := tools.NewHTTPExecutor(tools.HTTPConfig{
				Addr:      viper.GetString("addr"),
				Username:  viper.GetString("username"),
				Password:  viper.GetString("password"),
				EnableTLS: enableTLS,
				Insecure:  insecure,
				Cert:      cert,
				Timeout:   queryTimeout,
			})
			approvals := approval.NewController()
			toolBridge := bridge.New(bridge.Config{
				Approvals: approvals,
				Executor:  executor,
				Validator: tuibysql.NewSemanticValidator(),
			})
			bridgeServer, bridgeErr := bridge.StartSocketServer(toolBridge)
			if bridgeErr != nil {
				return fmt.Errorf("failed to start controlled tool bridge: %w", bridgeErr)
			}
			defer func() {
				_ = bridgeServer.Close()
			}()
			executable, executableErr := os.Executable()
			if executableErr != nil {
				return fmt.Errorf("failed to locate bydbctl executable: %w", executableErr)
			}
			agentGateway, gatewayErr := newAgentGateway(codexCommand, workingDirectory, bridgeServer.MCPServerConfig(executable))
			if gatewayErr != nil {
				return gatewayErr
			}
			defer func() {
				_ = agentGateway.Close()
			}()
			sessionLog, logErr := applog.New(logDir)
			if logErr != nil {
				return fmt.Errorf("failed to create agent session log: %w", logErr)
			}
			defer func() {
				_ = sessionLog.Close()
			}()
			model := tuiapp.NewModel(tuiapp.Config{
				AgentGateway: agentGateway,
				Executor:     executor,
				Approvals:    approvals,
				ToolBridge:   toolBridge,
				SessionLog:   sessionLog,
				Provider:     agentProviderCodex,
				Goal:         initialGoal,
				Start:        initialStart,
				End:          initialEnd,
			})
			program := tea.NewProgram(model, tea.WithAltScreen())
			if _, runErr := program.Run(); runErr != nil {
				return fmt.Errorf("failed to run agent TUI: %w", runErr)
			}
			fmt.Fprintf(os.Stderr, "agent session log: %s\n", sessionLog.Path())
			return nil
		},
	}
	agentCmd.Flags().StringVar(&codexCommand, "codex-command", "codex", "path to the Codex CLI executable")
	agentCmd.Flags().StringVar(&initialGoal, "goal", "", "initial natural language query goal")
	agentCmd.Flags().StringVar(&initialStart, "start", "-30m", "initial BYDBQL time start")
	agentCmd.Flags().StringVar(&initialEnd, "end", "", "initial BYDBQL time end")
	agentCmd.Flags().DurationVar(&queryTimeout, "query-timeout", 3*time.Second, "timeout for one approved BYDBQL query")
	agentCmd.Flags().StringVar(&logDir, "log-dir", "", "directory for agent session logs; default is $HOME/.bydbctl/logs")
	bindTLSRelatedFlag(agentCmd)
	return agentCmd
}

func newAgentGateway(codexCommand, workingDirectory string, mcpServer agent.ControlledMCPServer) (agent.Gateway, error) {
	if strings.TrimSpace(codexCommand) == "" {
		return nil, errCodexCommandRequired
	}
	return codex.NewGateway(codex.Config{
		Command:             codexCommand,
		WorkingDirectory:    workingDirectory,
		ControlledMCPServer: mcpServer,
	}), nil
}

func newAgentToolBridgeCmd() *cobra.Command {
	var socketPath string
	toolBridgeCmd := &cobra.Command{
		Use:    "agent-tool-bridge",
		Hidden: true,
		Short:  "Run the internal bydbctl agent tool bridge",
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if serveErr := bridge.ServeMCP(socketPath, cmd.InOrStdin(), cmd.OutOrStdout()); serveErr != nil {
				return fmt.Errorf("failed to serve controlled MCP tools: %w", serveErr)
			}
			return nil
		},
	}
	toolBridgeCmd.Flags().StringVar(&socketPath, "socket", "", "private bydbctl tool bridge socket")
	return toolBridgeCmd
}
