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

// Package app implements the Bubble Tea user interface for bydbctl agent.
package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/applog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

const (
	defaultWidth  = 120
	defaultHeight = 36
)

const (
	focusCatalog = iota
	focusCatalogFilter
	focusChat
	focusMessage
	focusStart
	focusEnd
	focusQuery
	focusActivity
	focusExecution
	focusCount
)

// Config configures the TUI model.
type Config struct {
	AgentGateway agent.Gateway
	Executor     tools.Executor
	Approvals    *approval.Controller
	ToolBridge   *bridge.ToolBridge
	SessionLog   *applog.Logger
	LogDir       string
	Provider     string
	Goal         string
	Start        string
	End          string
}

// Model is the Bubble Tea state for the bydbctl agent TUI.
type Model struct {
	runner                *workflow.Runner
	executor              tools.Executor
	querySession          *session.QuerySession
	catalog               catalogBrowser
	selectedSchema        session.SchemaSnapshot
	catalogFilter         textinput.Model
	message               textarea.Model
	query                 textarea.Model
	start                 textinput.Model
	end                   textinput.Model
	provider              string
	status                string
	events                []string
	sessionLog            *applog.Logger
	logPathDisplay        string
	width                 int
	height                int
	catalogHeight         int
	activeTab             appTab
	activityLog           []activityEntry
	activityScroll        int
	activityCursor        int
	activityDetailScroll  int
	executionDetailScroll int
	executionRowCursor    int
	showExecutionRaw      bool
	executionExportPath   string
	detailScroll          int
	chatScroll            int
	chatCursor            int
	chatDetailScroll      int
	focus                 int
	busy                  bool
	showReasoning         bool
	executionPolicy       approval.ExecutionPolicy
	pendingApproval       *approval.Request
	turnCancel            context.CancelFunc
	turnEvents            []agent.Event
	queuedMessage         string
	liveResponse          string
	queryRevision         int
}

// NewModel creates a TUI model with the configured agent gateway.
func NewModel(config Config) Model {
	agentGateway := config.AgentGateway
	provider := strings.TrimSpace(config.Provider)
	if provider == "" {
		provider = "unconfigured"
	}
	catalogFilter := newTextInput("", "filter groups/resources")
	catalogFilter.Width = 24
	message := textarea.New()
	message.Placeholder = "Ask about schemas or describe the query you need…"
	message.ShowLineNumbers = false
	message.SetHeight(5)
	message.SetValue(config.Goal)
	query := textarea.New()
	query.Placeholder = "BYDBQL candidate"
	query.ShowLineNumbers = false
	query.SetHeight(10)
	start := newTextInput(config.Start, "time start, for example -30m")
	end := newTextInput(config.End, "optional time end")
	sessionLog := config.SessionLog
	if sessionLog == nil {
		createdLog, createErr := applog.New(config.LogDir)
		if createErr == nil {
			sessionLog = createdLog
		}
	}
	model := Model{
		runner: workflow.NewRunner(workflow.Config{
			AgentGateway: agentGateway,
			Executor:     config.Executor,
			Approvals:    config.Approvals,
			ToolBridge:   config.ToolBridge,
		}),
		executor:        config.Executor,
		catalog:         newCatalogBrowser(),
		catalogFilter:   catalogFilter,
		message:         message,
		query:           query,
		start:           start,
		end:             end,
		provider:        provider,
		status:          "ready",
		sessionLog:      sessionLog,
		logPathDisplay:  applog.DisplayPath(sessionLogPath(sessionLog)),
		executionPolicy: approval.PolicyAskEveryTime,
		width:           defaultWidth,
		height:          defaultHeight,
		activeTab:       tabQuery,
		focus:           focusMessage,
	}
	if sessionLog != nil {
		sessionLog.Write("session", fmt.Sprintf("provider=%s addr=workflow", provider))
	}
	model.addEvent("ready: browse Schema Browser, message agent with Ctrl+A")
	model.resize(defaultWidth, defaultHeight)
	model.syncFocus()
	model.syncExecutionPolicy()
	return model
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return tea.Batch(m.loadCatalogCmd(), m.waitApprovalCmd())
}

// Update implements tea.Model.
func (m Model) Update(teaMsg tea.Msg) (tea.Model, tea.Cmd) {
	switch typedMsg := teaMsg.(type) {
	case tea.WindowSizeMsg:
		m.resize(typedMsg.Width, typedMsg.Height)
		return m, nil
	case approvalMsg:
		m.pendingApproval = &typedMsg.request
		m.status = "execution approval required"
		m.recordActivity("approval", "approval required", formatApprovalRequest(typedMsg.request))
		m.logWrite("approval", formatApprovalAudit(typedMsg.request))
		return m, m.waitApprovalCmd()
	case agentStartedMsg:
		if typedMsg.startErr != nil {
			m.busy = false
			m.turnCancel = nil
			m.status = typedMsg.startErr.Error()
			m.addUIEvent(summarizeError("agent", typedMsg.startErr.Error()))
			m.logWriteError("agent", typedMsg.startErr)
			return m, nil
		}
		if typedMsg.querySession != nil {
			m.querySession = typedMsg.querySession
			m.syncQuerySession()
			m.queuedMessage = ""
		}
		m.message.SetValue("")
		m.liveResponse = ""
		return m, m.nextAgentUpdateCmd(typedMsg.updates)
	case agentTurnUpdateMsg:
		if typedMsg.update.Event != nil {
			event := *typedMsg.update.Event
			m.turnEvents = append(m.turnEvents, event)
			m.recordAgentActivities([]agent.Event{event})
			if event.Kind == agent.EventKindMessageDelta {
				if m.showReasoning {
					m.liveResponse += event.Message
					m.status = "agent is reasoning"
				}
			} else if summary := summarizeAgentEvent(event); summary != "" {
				m.addUIEvent(summary)
			}
		}
		if !typedMsg.update.Done {
			return m, m.nextAgentUpdateCmd(typedMsg.updates)
		}
		m.busy = false
		m.turnCancel = nil
		m.querySession = typedMsg.update.QuerySession
		m.syncQuerySession()
		m.logAgentTurn(m.turnEvents)
		m.turnEvents = nil
		m.liveResponse = ""
		if typedMsg.update.Err != nil {
			m.status = typedMsg.update.Err.Error()
			m.addUIEvent(summarizeError("agent", typedMsg.update.Err.Error()))
			m.logWriteError("agent", typedMsg.update.Err)
			return m, nil
		}
		m.message.SetValue("")
		m.status = "agent turn complete"
		m.addUIEvent("agent: turn complete")
		m.logQuerySession(m.querySession)
		return m, nil
	case queryDebounceMsg:
		if typedMsg.revision != m.queryRevision || m.busy || strings.TrimSpace(m.query.Value()) == "" {
			return m, nil
		}
		m.busy = true
		m.status = "validating edited query"
		return m, m.validateCmd()
	case tea.KeyMsg:
		command, handled := m.handleKey(typedMsg)
		if handled {
			return m, command
		}
	case catalogMsg:
		m.busy = false
		if typedMsg.loadErr != nil {
			m.catalog.setLoadError(typedMsg.loadErr.Error())
			m.status = "BanyanDB connection failed: " + typedMsg.loadErr.Error()
			m.addUIEvent(m.status)
			m.logWriteError("catalog", typedMsg.loadErr)
			return m, nil
		}
		m.catalog.setCatalog(typedMsg.catalog)
		m.status = fmt.Sprintf("catalog loaded: %d resources in %d groups", len(typedMsg.catalog.Entries), len(typedMsg.catalog.Groups))
		m.addUIEvent(m.status)
		m.logWrite("catalog", m.status)
		return m, nil
	case schemaDetailMsg:
		if typedMsg.loadErr != nil {
			m.addUIEvent("schema detail: " + typedMsg.loadErr.Error())
			m.logWriteError("schema", typedMsg.loadErr)
			return m, nil
		}
		m.selectedSchema = typedMsg.snapshot
		if typedMsg.snapshot.Loaded {
			m.detailScroll = 0
		}
		return m, nil
	case workflowMsg:
		m.busy = false
		m.turnCancel = nil
		if typedMsg.querySession != nil {
			m.querySession = typedMsg.querySession
			m.syncQuerySession()
		}
		m.addAgentEvents(typedMsg.events)
		m.recordAgentActivities(typedMsg.events)
		if typedMsg.clearTurnHint {
			m.message.SetValue("")
		}
		if m.querySession != nil {
			if validationHint := formatValidationHint(m.querySession.Validation.Message); validationHint != "" {
				m.addUIEvent(validationHint)
				m.logWrite("validation", m.querySession.Validation.Message)
			}
			if currentCandidate := m.querySession.CurrentCandidate(); currentCandidate != nil && strings.TrimSpace(currentCandidate.Query) != "" {
				if !m.querySession.Validation.Valid {
					if invalidHint := formatInvalidCandidateHint(currentCandidate.Query); invalidHint != "" {
						m.addUIEvent(invalidHint)
					}
					m.logWrite("candidate", currentCandidate.Query)
				}
			}
			m.logQuerySession(m.querySession)
			if typedMsg.status == "execution complete" {
				m.executionDetailScroll = 0
				m.showExecutionRaw = false
				m.executionExportPath = ""
				if len(m.querySession.ExecutionResult.Preview) > 0 {
					m.executionRowCursor = 0
				} else {
					m.executionRowCursor = -1
				}
				m.recordExecutionActivity(m.querySession)
				m.switchTab(tabRun)
				m.focus = focusExecution
			}
		}
		if typedMsg.err != nil {
			m.status = typedMsg.err.Error()
			m.addUIEvent(summarizeError("error", typedMsg.err.Error()))
			m.logWriteError("workflow", typedMsg.err)
			return m, nil
		}
		if typedMsg.status != "" {
			m.addUIEvent(summarizeStatusEvent(typedMsg.status))
			m.logWrite("workflow", typedMsg.status)
			m.status = typedMsg.status
		} else if m.querySession != nil && !m.querySession.Validation.Valid && m.querySession.CurrentCandidate() != nil {
			m.status = "invalid candidate — send another message and press Ctrl+A"
			m.addUIEvent("validation: send another message and press Ctrl+A to refine")
		}
		if typedMsg.switchRunTab {
			m.switchTab(tabRun)
		}
		return m, nil
	}
	inputCmd := m.updateFocused(teaMsg)
	return m, inputCmd
}

// View implements tea.Model.
func (m Model) View() string {
	contentWidth := clamp(m.width-4, 48, 200)
	bodyHeight := clamp(m.height-8, 18, 40)
	tabBar := m.renderTabBar(contentWidth)
	header := tabBar
	if m.catalog.loadError != "" {
		connectionError := truncate("BanyanDB connection failed: "+singleLine(m.catalog.loadError), contentWidth)
		header = lipgloss.JoinVertical(lipgloss.Left, tabBar, badStyle.Render(connectionError))
		bodyHeight = maxInt(bodyHeight-1, 18)
	}
	var body string
	switch m.activeTab {
	case tabSchema:
		body = m.renderSchemaTab(contentWidth, bodyHeight)
	case tabRun:
		body = m.renderRunTab(contentWidth, bodyHeight)
	default:
		body = m.renderQueryTab(contentWidth, bodyHeight)
	}
	return lipgloss.JoinVertical(lipgloss.Left, header, body, m.renderFooter(contentWidth))
}

type catalogMsg struct {
	catalog session.SchemaCatalog
	loadErr error
}

type schemaDetailMsg struct {
	snapshot session.SchemaSnapshot
	loadErr  error
}

type workflowMsg struct {
	querySession  *session.QuerySession
	events        []agent.Event
	err           error
	status        string
	clearTurnHint bool
	switchRunTab  bool
}

type approvalMsg struct {
	request approval.Request
}

type agentStartedMsg struct {
	querySession *session.QuerySession
	updates      <-chan workflow.TurnUpdate
	startErr     error
}

type agentTurnUpdateMsg struct {
	update  workflow.TurnUpdate
	updates <-chan workflow.TurnUpdate
}

type queryDebounceMsg struct {
	revision int
}

func (m Model) waitApprovalCmd() tea.Cmd {
	requests := m.runner.ApprovalRequests()
	return func() tea.Msg {
		request := <-requests
		return approvalMsg{request: request}
	}
}

func (m Model) nextAgentUpdateCmd(updates <-chan workflow.TurnUpdate) tea.Cmd {
	return func() tea.Msg {
		update, open := <-updates
		if !open {
			return agentTurnUpdateMsg{update: workflow.TurnUpdate{Done: true, Err: fmt.Errorf("agent stream closed unexpectedly")}, updates: updates}
		}
		return agentTurnUpdateMsg{update: update, updates: updates}
	}
}

func (m Model) queryDebounceCmd(revision int) tea.Cmd {
	return tea.Tick(350*time.Millisecond, func(time.Time) tea.Msg {
		return queryDebounceMsg{revision: revision}
	})
}

func (m *Model) syncQuerySession() {
	if m.querySession == nil {
		return
	}
	if m.querySession.CandidateSuperseded {
		m.query.SetValue("")
		return
	}
	if currentCandidate := m.querySession.CurrentCandidate(); currentCandidate != nil {
		m.query.SetValue(currentCandidate.Query)
	}
	if strings.TrimSpace(m.querySession.SchemaSnapshot.Name) != "" {
		m.selectedSchema = m.querySession.SchemaSnapshot
	}
	m.syncChatCursor(true)
}

func formatApprovalRequest(request approval.Request) string {
	return strings.Join([]string{
		"statement: " + request.Query,
		"resource: " + fallback(request.Resource, "-"),
		"groups: " + fallback(strings.Join(request.Groups, ", "), "-"),
		"time range: " + fallback(request.TimeRange, "-"),
		"limit: " + fallback(request.Limit, "-"),
		"timeout: " + request.Timeout.String(),
		fmt.Sprintf("preview rows: %d", request.PreviewRows),
		"source: " + string(request.Source),
	}, "\n")
}

func formatApprovalAudit(request approval.Request) string {
	return fmt.Sprintf("source=%s resource=%s groups=%s time_range=%s limit=%s timeout=%s preview_rows=%d query=%q",
		request.Source, request.Resource, strings.Join(request.Groups, ","), request.TimeRange, request.Limit, request.Timeout, request.PreviewRows, request.Query)
}

func newTextInput(value, placeholder string) textinput.Model {
	input := textinput.New()
	input.Placeholder = placeholder
	input.SetValue(value)
	input.Prompt = ""
	input.Width = 24
	return input
}

func (m *Model) handleKey(keyMsg tea.KeyMsg) (tea.Cmd, bool) {
	if m.pendingApproval != nil {
		switch keyMsg.String() {
		case "y":
			return m.resolvePendingApproval(true)
		case "n":
			return m.resolvePendingApproval(false)
		case "e":
			request := *m.pendingApproval
			if _, handled := m.resolvePendingApproval(false); handled {
				m.cancelActive()
				m.query.SetValue(request.Query)
				m.status = "editing rejected statement"
				m.switchTab(tabQuery)
				m.focus = focusQuery
				return m.syncFocus(), true
			}
		}
	}
	switch keyMsg.String() {
	case "ctrl+c", "esc":
		if m.busy || m.pendingApproval != nil {
			m.cancelActive()
			return nil, true
		}
		return tea.Quit, true
	case "f1":
		m.switchTab(tabSchema)
		return m.syncFocus(), true
	case "f2":
		m.switchTab(tabQuery)
		return m.syncFocus(), true
	case "f3":
		m.switchTab(tabRun)
		return m.syncFocus(), true
	case "ctrl+]":
		m.cycleTab(1)
		return m.syncFocus(), true
	case "tab":
		m.cycleFocus(1)
		return m.syncFocus(), true
	case "shift+tab":
		m.cycleFocus(-1)
		return m.syncFocus(), true
	case "ctrl+l":
		if m.busy {
			return nil, true
		}
		m.busy = true
		m.catalog.setLoading()
		m.status = "refreshing catalog"
		return m.loadCatalogCmd(), true
	case "/":
		if m.activeTab == tabSchema && (m.focus == focusCatalog || m.focus == focusCatalogFilter) {
			m.catalog.cycleTypeFilter()
			return nil, true
		}
		return nil, false
	case "[", "]":
		delta := -1
		if keyMsg.String() == "]" {
			delta = 1
		}
		m.cycleTab(delta)
		return m.syncFocus(), true
	case "ctrl+left", "ctrl+right":
		if m.querySession == nil || len(m.querySession.Candidates) == 0 {
			return nil, true
		}
		delta := -1
		if keyMsg.String() == "ctrl+right" {
			delta = 1
		}
		if m.querySession.SelectCandidate(m.querySession.SelectedCandidateIndex() + delta) {
			m.syncQuerySession()
			m.status = "selected candidate version"
		}
		return nil, true
	case "up", "down":
		if m.activeTab == tabSchema {
			if m.focus == focusCatalog {
				delta := 1
				if keyMsg.String() == "up" {
					delta = -1
				}
				m.catalog.moveCursor(delta, m.catalogListHeight())
				return m.loadSchemaDetailCmdForCursor(), true
			}
			delta := 1
			if keyMsg.String() == "up" {
				delta = -1
			}
			m.scrollSchemaDetail(delta, m.schemaDetailHeight())
			return nil, true
		}
		if m.activeTab == tabRun && m.focus == focusExecution {
			delta := 1
			if keyMsg.String() == "up" {
				delta = -1
			}
			m.moveExecutionRowCursor(delta)
			return nil, true
		}
		if m.activeTab == tabRun && m.focus == focusActivity {
			delta := 1
			if keyMsg.String() == "up" {
				delta = -1
			}
			m.moveActivityCursor(delta, 8)
			return nil, true
		}
		if m.activeTab == tabQuery && m.focus == focusChat {
			delta := 1
			if keyMsg.String() == "up" {
				delta = -1
			}
			m.moveChatCursor(delta, m.chatListViewportHeight())
			return nil, true
		}
		return nil, false
	case "pgup", "pgdown":
		if m.activeTab == tabRun && m.focus == focusExecution {
			delta := 8
			if keyMsg.String() == "pgup" {
				delta = -8
			}
			m.scrollExecutionDetail(delta, m.executionDetailViewportHeight())
			return nil, true
		}
		if m.activeTab == tabRun && m.focus == focusActivity {
			delta := 8
			if keyMsg.String() == "pgup" {
				delta = -8
			}
			if m.scrollActivityDetail(delta, 8) {
				return nil, true
			}
			if m.scrollExecutionDetail(delta, m.executionDetailViewportHeight()) {
				return nil, true
			}
			m.moveActivityCursor(delta, 8)
			return nil, true
		}
		if m.activeTab == tabQuery && m.focus == focusChat {
			delta := 8
			if keyMsg.String() == "pgup" {
				delta = -8
			}
			m.moveChatDetailScroll(delta, chatDetailViewportHeight(m.chatPanelHeight(clamp(m.height-8, 18, 40))))
			return nil, true
		}
		return nil, false
	case "enter":
		if m.focus == focusCatalog {
			return m.loadSchemaDetailCmdForCursor(), true
		}
		return nil, false
	case "ctrl+a":
		if m.busy {
			return nil, true
		}
		messageValue := strings.TrimSpace(m.message.Value())
		if messageValue == "" {
			m.addUIEvent("message required before asking agent")
			return nil, true
		}
		m.syncExecutionPolicy()
		m.queuedMessage = messageValue
		m.message.SetValue("")
		m.syncChatCursor(true)
		m.busy = true
		m.status = "asking agent"
		m.logWrite("action", fmt.Sprintf("ctrl+a agent message=%q", messageValue))
		turnCtx, cancelTurn := context.WithCancel(context.Background())
		m.turnCancel = cancelTurn
		return m.agentCmd(turnCtx, messageValue), true
	case "ctrl+v":
		if m.busy {
			return nil, true
		}
		m.busy = true
		m.status = "validating query"
		m.logWrite("action", "ctrl+v validate query")
		return m.validateCmd(), true
	case "ctrl+e":
		if m.busy {
			return nil, true
		}
		m.busy = true
		m.status = "executing query"
		m.logWrite("action", "ctrl+e execute query")
		executeCtx, cancelExecute := context.WithCancel(context.Background())
		m.turnCancel = cancelExecute
		return m.executeCmd(executeCtx), true
	case "ctrl+p":
		m.executionPolicy = m.executionPolicy.Next()
		m.syncExecutionPolicy()
		m.status = "execution policy: " + m.executionPolicy.Label()
		m.addUIEvent("policy: " + m.executionPolicy.Label())
		return nil, true
	case "ctrl+r":
		m.showReasoning = !m.showReasoning
		if m.showReasoning {
			m.status = "agent reasoning stream visible"
		} else {
			m.status = "agent reasoning stream hidden"
		}
		return nil, true
	case "ctrl+o":
		if m.activeTab == tabRun && m.querySession != nil && strings.TrimSpace(m.querySession.ExecutionResult.Response) != "" {
			exportPath, exportErr := exportExecutionResult(m.querySession.ExecutionResult)
			if exportErr != nil {
				m.status = exportErr.Error()
				m.addUIEvent("export failed: " + exportErr.Error())
				return nil, true
			}
			m.executionExportPath = exportPath
			m.status = "exported execution response"
			m.addUIEvent("exported: " + exportPath)
			return nil, true
		}
		return nil, false
	case "ctrl+j":
		if m.activeTab == tabRun && m.querySession != nil && m.querySession.ExecutionResult.Summary != "" {
			m.showExecutionRaw = !m.showExecutionRaw
			m.executionDetailScroll = 0
			if m.showExecutionRaw {
				m.status = "showing raw JSON response"
			} else {
				m.status = "hiding raw JSON response"
			}
			return nil, true
		}
		return nil, false
	default:
		return nil, false
	}
}

func (m *Model) resolvePendingApproval(approved bool) (tea.Cmd, bool) {
	if m.pendingApproval == nil {
		return nil, false
	}
	request := *m.pendingApproval
	m.pendingApproval = nil
	if resolveErr := m.runner.ResolveApproval(request.ID, approved); resolveErr != nil {
		m.status = resolveErr.Error()
		m.addUIEvent(summarizeError("approval", resolveErr.Error()))
		return nil, true
	}
	decision := "rejected"
	if approved {
		decision = "approved"
	}
	m.status = "execution " + decision
	m.recordActivity("approval", "execution "+decision, formatApprovalRequest(request))
	m.logWrite("approval", fmt.Sprintf("id=%s decision=%s", request.ID, decision))
	return nil, true
}

func (m *Model) cancelActive() {
	if m.turnCancel != nil {
		m.turnCancel()
		m.turnCancel = nil
	}
	m.runner.CancelApprovals()
	if stopErr := m.runner.StopAgentTurn(context.Background(), m.querySession); stopErr != nil {
		m.logWriteError("agent", stopErr)
	}
	m.pendingApproval = nil
	m.status = "cancelled"
	m.busy = false
	m.addUIEvent("workflow: cancelled")
}

func (m *Model) syncFocus() tea.Cmd {
	m.message.Blur()
	m.catalogFilter.Blur()
	m.start.Blur()
	m.end.Blur()
	m.query.Blur()
	switch m.focus {
	case focusCatalog:
		return nil
	case focusCatalogFilter:
		return m.catalogFilter.Focus()
	case focusChat:
		return nil
	case focusMessage:
		return m.message.Focus()
	case focusStart:
		return m.start.Focus()
	case focusEnd:
		return m.end.Focus()
	case focusQuery:
		return m.query.Focus()
	case focusActivity:
		return nil
	case focusExecution:
		return nil
	default:
		return nil
	}
}

func (m *Model) updateFocused(teaMsg tea.Msg) tea.Cmd {
	if m.busy {
		return nil
	}
	var updateCmd tea.Cmd
	previousQuery := m.query.Value()
	switch m.focus {
	case focusCatalog:
		return nil
	case focusCatalogFilter:
		m.catalogFilter, updateCmd = m.catalogFilter.Update(teaMsg)
		if strings.TrimSpace(m.catalogFilter.Value()) != m.catalog.filter {
			m.catalog.setFilter(m.catalogFilter.Value())
		}
	case focusChat:
		return nil
	case focusMessage:
		m.message, updateCmd = m.message.Update(teaMsg)
	case focusStart:
		m.start, updateCmd = m.start.Update(teaMsg)
	case focusEnd:
		m.end, updateCmd = m.end.Update(teaMsg)
	case focusQuery:
		m.query, updateCmd = m.query.Update(teaMsg)
	case focusActivity:
		return nil
	case focusExecution:
		return nil
	}
	if m.focus == focusQuery && previousQuery != m.query.Value() {
		m.queryRevision++
		return tea.Batch(updateCmd, m.queryDebounceCmd(m.queryRevision))
	}
	return updateCmd
}

func (m *Model) resize(width, height int) {
	m.width = width
	m.height = height
	contentWidth := clamp(width-4, 48, 200)
	catalogWidth := clamp(contentWidth*28/100, 28, 44)
	queryLeftWidth, queryRightWidth := queryTabWidths(contentWidth)
	timeInputWidth := maxInt(10, (queryLeftWidth-24)/2)
	m.catalogHeight = clamp(height-6, 20, 40)
	m.catalogFilter.Width = maxInt(12, catalogWidth-8)
	m.message.SetWidth(maxInt(18, queryLeftWidth-4))
	m.query.SetWidth(maxInt(18, queryRightWidth-4))
	m.start.Width = timeInputWidth
	m.end.Width = timeInputWidth
	if contentWidth < 100 {
		m.query.SetWidth(maxInt(18, contentWidth-4))
	}
	queryHeight := clamp(height-18, 8, 16)
	m.query.SetHeight(queryHeight)
	m.message.SetHeight(clamp(height/12, 3, 5))
}

func (m Model) chatPanelHeight(totalHeight int) int {
	return clamp(totalHeight-8, 16, totalHeight-6)
}

func (m Model) chatListViewportHeight() int {
	panelHeight := m.chatPanelHeight(clamp(m.height-8, 18, 40))
	detailBudget := 0
	if entries := chatEntries(m.querySession, m.showReasoning, m.liveResponse, m.queuedMessage); m.chatCursor >= 0 &&
		m.chatCursor < len(entries) && strings.TrimSpace(entries[m.chatCursor].detail) != "" {
		detailBudget = chatDetailViewportHeight(panelHeight) + 2
	}
	return maxInt(panelHeight-8-detailBudget, 4)
}

func (m Model) agentCmd(ctx context.Context, messageValue string) tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(ctx, runner, querySession, options, query)
		if ensureErr != nil {
			return agentStartedMsg{startErr: ensureErr}
		}
		updates, startErr := runner.StartAgentTurn(ctx, updatedSession, messageValue)
		return agentStartedMsg{querySession: updatedSession, updates: updates, startErr: startErr}
	}
}

func (m Model) validateCmd() tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(context.Background(), runner, querySession, options, query)
		if ensureErr != nil {
			return workflowMsg{err: ensureErr}
		}
		if strings.TrimSpace(query) == "" {
			if currentCandidate := updatedSession.CurrentCandidate(); currentCandidate != nil {
				query = currentCandidate.Query
			}
		}
		if validateErr := runner.ValidateManualQuery(context.Background(), updatedSession, query); validateErr != nil {
			return workflowMsg{
				querySession: updatedSession,
				err:          validateErr,
			}
		}
		return workflowMsg{
			querySession: updatedSession,
			status:       "validation complete",
		}
	}
}

func (m Model) executeCmd(ctx context.Context) tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(ctx, runner, querySession, options, query)
		if ensureErr != nil {
			return workflowMsg{err: ensureErr}
		}
		if executeErr := runner.ExecuteCurrent(ctx, updatedSession); executeErr != nil {
			return workflowMsg{
				querySession: updatedSession,
				err:          executeErr,
			}
		}
		return workflowMsg{
			querySession: updatedSession,
			status:       "execution complete",
		}
	}
}

func (m *Model) startOptions() workflow.StartOptions {
	return workflow.StartOptions{
		TimeRange: session.TimeRange{
			Start: m.start.Value(),
			End:   m.end.Value(),
		},
		Goal:            m.currentGoal(),
		ExecutionPolicy: m.executionPolicy,
	}
}

func (m Model) currentGoal() string {
	if m.querySession != nil && strings.TrimSpace(m.querySession.UserGoal) != "" {
		return m.querySession.UserGoal
	}
	if queuedMessage := strings.TrimSpace(m.queuedMessage); queuedMessage != "" {
		return queuedMessage
	}
	return strings.TrimSpace(m.message.Value())
}

func (m *Model) syncExecutionPolicy() {
	m.runner.SetExecutionPolicy(m.executionPolicy)
	if m.querySession != nil {
		m.querySession.ExecutionPolicy = m.executionPolicy
	}
}

func (m Model) loadCatalogCmd() tea.Cmd {
	executor := m.executor
	return func() tea.Msg {
		if executor == nil {
			return catalogMsg{loadErr: fmt.Errorf("schema executor is not configured")}
		}
		catalog, catalogErr := executor.DiscoverCatalog(context.Background())
		if catalogErr != nil {
			return catalogMsg{loadErr: catalogErr}
		}
		return catalogMsg{catalog: catalog}
	}
}

func (m Model) loadSchemaDetailCmd(entry session.CatalogEntry) tea.Cmd {
	executor := m.executor
	return func() tea.Msg {
		if executor == nil {
			return schemaDetailMsg{loadErr: fmt.Errorf("schema executor is not configured")}
		}
		snapshot, schemaErr := executor.DiscoverSchema(context.Background(), tools.SchemaRequest{
			Type:   entry.Type,
			Name:   entry.Name,
			Groups: []string{entry.Group},
		})
		if schemaErr != nil {
			return schemaDetailMsg{loadErr: schemaErr}
		}
		return schemaDetailMsg{snapshot: snapshot}
	}
}

func (m Model) catalogListHeight() int {
	return clamp(m.catalogHeight-16, 8, 22)
}

func (m *Model) scrollActivityDetail(delta, viewportHeight int) bool {
	if m.activityCursor < 0 || m.activityCursor >= len(m.activityLog) {
		return false
	}
	selected := m.activityLog[m.activityCursor]
	if strings.TrimSpace(selected.detail) == "" {
		return false
	}
	detailLines := formatActivityDetailText(selected.detail, clamp(m.width-8, 48, 200))
	maxScroll := maxInt(len(detailLines)-viewportHeight, 0)
	if maxScroll == 0 {
		return false
	}
	m.activityDetailScroll += delta
	if m.activityDetailScroll < 0 {
		m.activityDetailScroll = 0
	}
	if m.activityDetailScroll > maxScroll {
		m.activityDetailScroll = maxScroll
	}
	return true
}

func (m Model) executionDetailViewportHeight() int {
	return minInt(maxInt(m.height/2, 10), 22)
}

func (m *Model) scrollExecutionDetail(delta, viewportHeight int) bool {
	if m.querySession == nil || m.querySession.ExecutionResult.Summary == "" {
		return false
	}
	bodyLines := m.executionBodyLines(clamp(m.width-8, 48, 200))
	maxScroll := maxInt(len(bodyLines)-viewportHeight, 0)
	if maxScroll == 0 {
		return false
	}
	m.executionDetailScroll += delta
	if m.executionDetailScroll < 0 {
		m.executionDetailScroll = 0
	}
	if m.executionDetailScroll > maxScroll {
		m.executionDetailScroll = maxScroll
	}
	return true
}

func (m *Model) moveExecutionRowCursor(delta int) {
	if m.querySession == nil || len(m.querySession.ExecutionResult.Preview) == 0 {
		m.executionRowCursor = -1
		return
	}
	if m.executionRowCursor < 0 {
		m.executionRowCursor = 0
	}
	m.executionRowCursor += delta
	if m.executionRowCursor < 0 {
		m.executionRowCursor = 0
	}
	previewLength := len(m.querySession.ExecutionResult.Preview)
	if m.executionRowCursor >= previewLength {
		m.executionRowCursor = previewLength - 1
	}
	m.executionDetailScroll = 0
}

func (m Model) executionBodyLines(width int) []string {
	if m.querySession == nil {
		return nil
	}
	return executionDetailLines(m.querySession.ExecutionResult, executionDisplayOptions{
		width:       width,
		selectedRow: m.executionRowCursor,
		showRaw:     m.showExecutionRaw,
	})
}

func (m Model) schemaDetailHeight() int {
	return clamp(m.height-14, 10, 28)
}

func (m *Model) moveActivityCursor(delta, viewportHeight int) {
	if len(m.activityLog) == 0 {
		m.activityCursor = 0
		m.activityScroll = 0
		return
	}
	m.activityCursor += delta
	if m.activityCursor < 0 {
		m.activityCursor = 0
	}
	if m.activityCursor >= len(m.activityLog) {
		m.activityCursor = len(m.activityLog) - 1
	}
	if m.activityCursor < m.activityScroll {
		m.activityScroll = m.activityCursor
	}
	if m.activityCursor >= m.activityScroll+viewportHeight {
		m.activityScroll = m.activityCursor - viewportHeight + 1
	}
	m.activityDetailScroll = 0
}

func (m *Model) syncChatCursor(scrollToEnd bool) {
	entryCount := chatEntryCount(m.querySession, m.showReasoning, m.liveResponse, m.queuedMessage)
	if entryCount == 0 {
		m.chatCursor = 0
		m.chatScroll = 0
		return
	}
	if scrollToEnd {
		m.chatCursor = entryCount - 1
	}
	m.chatDetailScroll = 0
	if m.chatCursor < 0 {
		m.chatCursor = 0
	}
	if m.chatCursor >= entryCount {
		m.chatCursor = entryCount - 1
	}
}

func (m *Model) moveChatCursor(delta, viewportHeight int) {
	entryCount := chatEntryCount(m.querySession, m.showReasoning, m.liveResponse, m.queuedMessage)
	if entryCount == 0 {
		m.chatCursor = 0
		m.chatScroll = 0
		return
	}
	m.chatCursor += delta
	if m.chatCursor < 0 {
		m.chatCursor = 0
	}
	if m.chatCursor >= entryCount {
		m.chatCursor = entryCount - 1
	}
	if m.chatCursor < m.chatScroll {
		m.chatScroll = m.chatCursor
	}
	if m.chatCursor >= m.chatScroll+viewportHeight {
		m.chatScroll = m.chatCursor - viewportHeight + 1
	}
	m.chatDetailScroll = 0
}

func (m *Model) moveChatDetailScroll(delta, viewportHeight int) {
	entries := chatEntries(m.querySession, m.showReasoning, m.liveResponse, m.queuedMessage)
	if m.chatCursor < 0 || m.chatCursor >= len(entries) {
		return
	}
	detailLines := formatChatDetailLines(entries[m.chatCursor].detail, maxInt(m.width/2, 40))
	if len(detailLines) == 0 {
		m.chatDetailScroll = 0
		return
	}
	m.chatDetailScroll += delta
	maxScroll := maxInt(len(detailLines)-viewportHeight, 0)
	if m.chatDetailScroll < 0 {
		m.chatDetailScroll = 0
	}
	if m.chatDetailScroll > maxScroll {
		m.chatDetailScroll = maxScroll
	}
}

func (m Model) loadSchemaDetailCmdForCursor() tea.Cmd {
	entry, ok := m.catalog.selectedEntry()
	if !ok {
		return nil
	}
	return m.loadSchemaDetailCmd(entry)
}

func ensureSession(
	ctx context.Context,
	runner *workflow.Runner,
	querySession *session.QuerySession,
	options workflow.StartOptions,
	query string,
) (*session.QuerySession, error) {
	updatedSession := querySession
	if updatedSession == nil {
		var startErr error
		updatedSession, startErr = runner.StartSession(ctx, options)
		if startErr != nil {
			return nil, startErr
		}
	} else {
		var syncErr error
		updatedSession, syncErr = runner.SyncSession(ctx, updatedSession, options)
		if syncErr != nil {
			return nil, syncErr
		}
	}
	currentCandidate := updatedSession.CurrentCandidate()
	if strings.TrimSpace(query) != "" && (currentCandidate == nil || strings.TrimSpace(currentCandidate.Query) != strings.TrimSpace(query)) {
		if validateErr := runner.ValidateManualQuery(ctx, updatedSession, query); validateErr != nil {
			return nil, validateErr
		}
	}
	return updatedSession, nil
}

func (m *Model) addAgentEvents(events []agent.Event) {
	for _, event := range events {
		if uiEvent := summarizeAgentEvent(event); shouldShowAgentEvent(event) && uiEvent != "" {
			m.addUIEvent(uiEvent)
		}
	}
	m.logAgentTurn(events)
}

func shouldShowAgentEvent(event agent.Event) bool {
	switch event.Kind {
	case agent.EventKindMessageDelta:
		return false
	case agent.EventKindPermissionRequest:
		return !strings.Contains(strings.ToLower(event.Message), "granted")
	default:
		return true
	}
}

func (m *Model) addEvent(event string) {
	m.addUIEvent(summarizeStatusEvent(event))
	m.logWrite("event", event)
}

func (m *Model) addUIEvent(event string) {
	if strings.TrimSpace(event) == "" {
		return
	}
	m.events = append(m.events, event)
	m.recordActivity("workflow", event, "")
	if len(m.events) > maxVisibleEvents {
		m.events = m.events[len(m.events)-maxVisibleEvents:]
	}
}

func (m *Model) logWrite(category, message string) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.Write(category, message)
}

func (m *Model) logWriteError(category string, err error) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteError(category, err)
}

func (m *Model) logAgentTurn(events []agent.Event) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteAgentTurn(events)
}

func (m *Model) logQuerySession(querySession *session.QuerySession) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteQuerySession(querySession)
}

func sessionLogPath(sessionLog *applog.Logger) string {
	if sessionLog == nil {
		return ""
	}
	return sessionLog.Path()
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func clamp(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
