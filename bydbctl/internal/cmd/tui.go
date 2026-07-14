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
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/acp"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/builtin"
	tuiapp "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/app"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	tuibysql "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	agentProviderBuiltin    = "builtin"
	agentProviderACP        = "acp"
	agentProviderCodexACP   = "codex-acp"
)

func newAgentCmd() *cobra.Command {
	var agentProvider string
	var acpCommand string
	var acpArgs []string
	var agentModel string
	var agentBaseURL string
	var mcpConfig string
	var initialGoal string
	var initialStart string
	var initialEnd string
	var queryTimeout time.Duration
	var logDir string
	agentCmd := &cobra.Command{
		Use:     "agent",
		Version: version.Build(),
		Short:   "Open the interactive BYDBQL agent TUI",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("mcp-config") {
				return fmt.Errorf("--mcp-config is no longer supported: bydbctl agent only exposes its built-in controlled tools")
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
			var mcpServers any
			if usesACPToolBridge(agentProvider) {
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
				mcpServers = bridgeServer.MCPServerConfig(executable)
			}
			agentGateway, gatewayErr := newAgentGateway(agentGatewayOptions{
				provider:         agentProvider,
				acpCommand:       acpCommand,
				acpArgs:          acpArgs,
				workingDirectory: workingDirectory,
				mcpServers:       mcpServers,
				toolBridge:       toolBridge,
				model:            strings.TrimSpace(agentModel),
				baseURL:          strings.TrimSpace(agentBaseURL),
				apiKey:           strings.TrimSpace(os.Getenv("BYDBCTL_AGENT_API_KEY")),
			})
			if gatewayErr != nil {
				return gatewayErr
			}
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
				Provider:     agentProvider,
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
	agentCmd.Flags().StringVar(&agentProvider, "agent", agentProviderBuiltin, "agent adapter: builtin, codex-acp, or acp")
	agentCmd.Flags().StringVar(&acpCommand, "acp-command", "", "ACP-compatible stdio command used by --agent acp")
	agentCmd.Flags().StringArrayVar(&acpArgs, "acp-arg", nil, "argument passed to --acp-command; may be repeated")
	agentCmd.Flags().StringVar(&agentModel, "agent-model", "", "chat model for --agent builtin; default from BYDBCTL_AGENT_MODEL or gpt-4o-mini")
	agentCmd.Flags().StringVar(&agentBaseURL, "agent-base-url", "", "OpenAI-compatible base URL for --agent builtin; default from BYDBCTL_AGENT_BASE_URL")
	agentCmd.Flags().StringVar(&mcpConfig, "mcp-config", "", "deprecated: external MCP configuration is rejected")
	agentCmd.Flags().StringVar(&initialGoal, "goal", "", "initial natural language query goal")
	agentCmd.Flags().StringVar(&initialStart, "start", "-30m", "initial BYDBQL time start")
	agentCmd.Flags().StringVar(&initialEnd, "end", "", "initial BYDBQL time end")
	agentCmd.Flags().DurationVar(&queryTimeout, "query-timeout", 3*time.Second, "timeout for one approved BYDBQL query")
	agentCmd.Flags().StringVar(&logDir, "log-dir", "", "directory for agent session logs; default is $HOME/.bydbctl/logs")
	bindTLSRelatedFlag(agentCmd)
	return agentCmd
}

type agentGatewayOptions struct {
	provider         string
	acpCommand       string
	acpArgs          []string
	workingDirectory string
	mcpServers       any
	toolBridge       *bridge.ToolBridge
	model            string
	baseURL          string
	apiKey           string
}

func usesACPToolBridge(provider string) bool {
	switch provider {
	case agentProviderACP, agentProviderCodexACP:
		return true
	default:
		return false
	}
}

func newAgentGateway(opts agentGatewayOptions) (agent.Gateway, error) {
	switch opts.provider {
	case agentProviderBuiltin, "":
		modelName := opts.model
		if modelName == "" {
			modelName = strings.TrimSpace(os.Getenv("BYDBCTL_AGENT_MODEL"))
		}
		baseURL := opts.baseURL
		if baseURL == "" {
			baseURL = strings.TrimSpace(os.Getenv("BYDBCTL_AGENT_BASE_URL"))
		}
		chatModel, modelErr := builtin.NewOpenAIChatModel(builtin.ModelConfig{
			APIKey:  opts.apiKey,
			BaseURL: baseURL,
			Model:   modelName,
		})
		if modelErr != nil {
			return nil, modelErr
		}
		return builtin.NewGateway(builtin.Config{ToolBridge: opts.toolBridge, Model: chatModel})
	case agentProviderACP:
		if strings.TrimSpace(opts.acpCommand) == "" {
			return nil, fmt.Errorf("--acp-command is required when --agent acp")
		}
		return acp.NewGateway(opts.acpCommand, opts.acpArgs...).WithWorkingDirectory(opts.workingDirectory).WithMCPServers(opts.mcpServers), nil
	case agentProviderCodexACP:
		return acp.NewGateway("npx", "-y", "@agentclientprotocol/codex-acp").WithWorkingDirectory(opts.workingDirectory).WithMCPServers(opts.mcpServers), nil
	default:
		return nil, fmt.Errorf("unsupported agent provider %q; use builtin, codex-acp, or acp", opts.provider)
	}
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
