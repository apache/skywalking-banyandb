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
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/claude"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent/codex"
	tuiapp "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/app"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	tuibysql "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bydbql"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	agentProviderCodex  = "codex"
	agentProviderClaude = "claude"
)

var (
	errCodexCommandRequired  = errors.New("--codex-command is required when --provider=codex")
	errClaudeCommandRequired = errors.New("--claude-command is required when --provider=claude")
)

func newAgentCmd() *cobra.Command {
	var provider string
	var codexCommand string
	var initialGoal string
	var initialStart string
	var initialEnd string
	var queryTimeout time.Duration
	var logDir string
	var claudeCommand string
	var claudeModel string
	var claudeAPIKey string
	var claudeBaseURL string
	var claudeMaxTurns int
	agentCmd := &cobra.Command{
		Use:     "agent",
		Version: version.Build(),
		Short:   "Open the interactive BYDBQL agent TUI",
		RunE: func(_ *cobra.Command, _ []string) (runErr error) {
			switch strings.TrimSpace(provider) {
			case agentProviderCodex, agentProviderClaude:
			default:
				return fmt.Errorf("unknown agent provider %q", provider)
			}
			if provider == agentProviderCodex && strings.TrimSpace(codexCommand) == "" {
				return errCodexCommandRequired
			}
			if provider == agentProviderClaude && strings.TrimSpace(claudeCommand) == "" {
				return errClaudeCommandRequired
			}
			workingDirectory, wdErr := os.MkdirTemp("", "bydbctl-agent-cwd-")
			if wdErr != nil {
				return fmt.Errorf("failed to create isolated agent working directory: %w", wdErr)
			}
			defer func() {
				if removeErr := os.RemoveAll(workingDirectory); removeErr != nil {
					runErr = errors.Join(runErr, fmt.Errorf("failed to remove isolated agent working directory: %w", removeErr))
				}
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
			toolBridge := bridge.New(bridge.Config{
				Executor:  executor,
				Validator: tuibysql.NewSemanticValidator(),
			})
			bridgeServer, bridgeErr := bridge.StartSocketServer(toolBridge)
			if bridgeErr != nil {
				return fmt.Errorf("failed to start controlled tool bridge: %w", bridgeErr)
			}
			defer func() {
				if closeErr := bridgeServer.Close(); closeErr != nil {
					runErr = errors.Join(runErr, fmt.Errorf("failed to close controlled tool bridge: %w", closeErr))
				}
			}()
			executable, executableErr := os.Executable()
			if executableErr != nil {
				return fmt.Errorf("failed to locate bydbctl executable: %w", executableErr)
			}
			mcpServer := bridgeServer.MCPServerConfig(executable)
			agentGateway, gatewayErr := newAgentGateway(provider, codexCommand, workingDirectory, mcpServer, claude.Config{
				Command:             claudeCommand,
				Model:               claudeModel,
				APIKey:              claudeAPIKey,
				BaseURL:             claudeBaseURL,
				MaxTurns:            claudeMaxTurns,
				WorkingDirectory:    workingDirectory,
				ControlledMCPServer: mcpServer,
			})
			if gatewayErr != nil {
				return gatewayErr
			}
			defer func() {
				if closeErr := agentGateway.Close(); closeErr != nil {
					runErr = errors.Join(runErr, fmt.Errorf("failed to close agent gateway: %w", closeErr))
				}
			}()
			sessionLog, logErr := applog.New(logDir)
			if logErr != nil {
				return fmt.Errorf("failed to create agent session log: %w", logErr)
			}
			defer func() {
				if closeErr := sessionLog.Close(); closeErr != nil {
					runErr = errors.Join(runErr, fmt.Errorf("failed to close agent session log: %w", closeErr))
				}
			}()
			model := tuiapp.NewModel(tuiapp.Config{
				AgentGateway: agentGateway,
				Executor:     executor,
				ToolBridge:   toolBridge,
				SessionLog:   sessionLog,
				Provider:     provider,
				Goal:         initialGoal,
				Start:        initialStart,
				End:          initialEnd,
			})
			program := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseCellMotion())
			if _, programErr := program.Run(); programErr != nil {
				return fmt.Errorf("failed to run agent TUI: %w", programErr)
			}
			// Reported after the alt screen is restored, so the path is the last thing left on screen.
			if _, writeErr := fmt.Fprintf(os.Stderr,
				"\nagent session log: %s\nreplay it with: tail -n +1 %s\n", sessionLog.Path(), sessionLog.Path()); writeErr != nil {
				return fmt.Errorf("failed to report agent session log path: %w", writeErr)
			}
			return nil
		},
	}
	agentCmd.Flags().StringVar(&provider, "provider", agentProviderCodex, "agent provider: codex|claude")
	agentCmd.Flags().StringVar(&codexCommand, "codex-command", "codex", "path to the Codex CLI executable (provider=codex)")
	agentCmd.Flags().StringVar(&claudeCommand, "claude-command", "claude", "path to the Claude CLI executable (provider=claude)")
	agentCmd.Flags().StringVar(&claudeModel, "claude-model", "sonnet", "Claude model id or alias (provider=claude)")
	agentCmd.Flags().StringVar(&claudeAPIKey, "claude-api-key", "", "optional ANTHROPIC_API_KEY override for Claude CLI (provider=claude)")
	agentCmd.Flags().StringVar(&claudeBaseURL, "claude-base-url", "", "optional ANTHROPIC_BASE_URL override for Claude CLI (provider=claude)")
	agentCmd.Flags().IntVar(&claudeMaxTurns, "claude-max-turns", 12, "maximum agentic turns per Claude CLI invocation (provider=claude)")
	agentCmd.Flags().StringVar(&initialGoal, "goal", "", "initial natural language query goal")
	agentCmd.Flags().StringVar(&initialStart, "start", "-30m", "initial BYDBQL time start")
	agentCmd.Flags().StringVar(&initialEnd, "end", "", "initial BYDBQL time end")
	agentCmd.Flags().DurationVar(&queryTimeout, "query-timeout", 3*time.Second, "timeout for one approved BYDBQL query")
	agentCmd.Flags().StringVar(&logDir, "log-dir", "", "directory for agent session logs; default is $HOME/.bydbctl/logs")
	bindTLSRelatedFlag(agentCmd)
	return agentCmd
}

func newAgentGateway(provider, codexCommand, workingDirectory string, mcpServer agent.ControlledMCPServer, claudeCfg claude.Config) (agent.Gateway, error) {
	switch provider {
	case agentProviderCodex:
		if strings.TrimSpace(codexCommand) == "" {
			return nil, errCodexCommandRequired
		}
		return codex.NewGateway(codex.Config{
			Command:             codexCommand,
			WorkingDirectory:    workingDirectory,
			ControlledMCPServer: mcpServer,
		}), nil
	case agentProviderClaude:
		if strings.TrimSpace(claudeCfg.Command) == "" {
			return nil, errClaudeCommandRequired
		}
		claudeCfg.WorkingDirectory = workingDirectory
		claudeCfg.ControlledMCPServer = mcpServer
		return claude.NewGateway(claudeCfg), nil
	default:
		return nil, fmt.Errorf("unknown agent provider %q", provider)
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
