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
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tools"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuitext"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

const (
	defaultWidth  = 120
	defaultHeight = 36
)

const (
	focusChat = iota
	focusMessage
	focusStart
	focusEnd
	focusLimit
	focusQuery
	focusExecution
	focusCount
)

// Config configures the TUI model.
type Config struct {
	AgentGateway agent.Gateway
	Executor     tools.Executor
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
	message                    textarea.Model
	query                      textarea.Model
	turnStartedAt              time.Time
	executor                   tools.Executor
	composerReference          *session.CatalogEntry
	querySession               *session.QuerySession
	runner                     *workflow.Runner
	schemaCache                map[string]session.SchemaSnapshot
	schemaLoads                map[string]struct{}
	turnCancel                 context.CancelFunc
	sessionLog                 *applog.Logger
	queuedMessage              string
	schemaSearchValue          string
	provider                   string
	status                     string
	executionExportPath        string
	liveResponse               string
	turnEvents                 []agent.Event
	activityLog                []activityEntry
	panelRegions               []panelRegion
	messageHistory             editorHistory
	limit                      textinput.Model
	end                        textinput.Model
	start                      textinput.Model
	selectedSchema             session.SchemaSnapshot
	catalog                    catalogBrowser
	focus                      int
	schemaSearchCursor         int
	chatDetailScroll           int
	chatScroll                 int
	helpScroll                 int
	progressOperation          progressOperation
	schemaDetailScroll         int
	executionPreviewOffset     int
	executionRowCursor         int
	executionDetailScroll      int
	candidateCountAtTurnStart  int
	autoExecutedCandidateIndex int
	chatCursor                 int
	height                     int
	evidenceMode               evidenceMode
	width                      int
	regionsStale               bool
	busy                       bool
	helpVisible                bool
	quitConfirmPending         bool
	editingQuery               bool
	schemaSearchDismissed      bool
	hasAutoExecutedCandidate   bool
}

// NewModel creates a TUI model with the configured agent gateway.
func NewModel(config Config) Model {
	agentGateway := config.AgentGateway
	provider := strings.TrimSpace(config.Provider)
	if provider == "" {
		provider = "unconfigured"
	}
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
	limit := newTextInput("", "limit")
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
			ToolBridge:   config.ToolBridge,
		}),
		executor:    config.Executor,
		catalog:     newCatalogBrowser(),
		schemaCache: make(map[string]session.SchemaSnapshot),
		schemaLoads: make(map[string]struct{}),
		message:     message,
		query:       query,
		start:       start,
		end:         end,
		limit:       limit,
		provider:    provider,
		status:      "ready",
		sessionLog:  sessionLog,
		width:       defaultWidth,
		height:      defaultHeight,
		focus:       focusMessage,
	}
	if sessionLog != nil {
		sessionLog.Write("session", fmt.Sprintf("provider=%s addr=workflow", provider))
	}
	model.addEvent("ready: use @ to browse the local schema catalog, then Enter to ask the agent")
	if logPath := applog.DisplayPath(sessionLog.Path()); logPath != "" {
		model.addEvent("session log: " + logPath)
	}
	model.resize(defaultWidth, defaultHeight)
	model.syncFocus()
	return model
}

// Init implements tea.Model.
func (m Model) Init() tea.Cmd {
	return m.loadCatalogCmd()
}

// Update implements tea.Model.
//
// Click regions are marked stale rather than recomputed here, so one keystroke renders the view once.
func (m Model) Update(teaMsg tea.Msg) (tea.Model, tea.Cmd) {
	previousEvidence := m.showsSchemaEvidence()
	updatedModel, command := m.update(teaMsg)
	updatedModel.regionsStale = true
	// A schema leaving the slot is logged with the message that took it, since the panel is derived
	// state: nothing else in the log would say which update dropped it.
	if previousEvidence != updatedModel.showsSchemaEvidence() {
		updatedModel.logViewState(fmt.Sprintf("evidence panel changed on %T", teaMsg))
	}
	return updatedModel, command
}

func (m Model) update(teaMsg tea.Msg) (Model, tea.Cmd) {
	switch typedMsg := teaMsg.(type) {
	case tea.WindowSizeMsg:
		m.resize(typedMsg.Width, typedMsg.Height)
		return m, nil
	case tea.MouseMsg:
		command, _ := m.handleMouse(typedMsg)
		return m, command
	case tea.KeyMsg:
		if command, handled := m.handleKey(typedMsg); handled {
			return m, command
		}
	case agentStartedMsg:
		return m.applyAgentStarted(typedMsg)
	case agentTurnUpdateMsg:
		return m.applyAgentTurnUpdate(typedMsg)
	case catalogMsg:
		return m.applyCatalog(typedMsg)
	case schemaDetailMsg:
		return m.applySchemaDetail(typedMsg)
	case workflowMsg:
		return m.applyWorkflow(typedMsg)
	case turnTimeoutMsg:
		if !m.busy || typedMsg.startedAt != m.turnStartedAt {
			return m, nil
		}
		m.status = "still working — Esc stops the run"
		m.addUIEvent("workflow: still working")
		return m, m.turnTimeoutCmd(typedMsg.startedAt)
	}
	return m, m.updateFocused(teaMsg)
}

// applyAgentStarted records the session created for a new agent turn.
func (m Model) applyAgentStarted(startedMsg agentStartedMsg) (Model, tea.Cmd) {
	if startedMsg.startErr != nil {
		m.finishTurn()
		m.status = startedMsg.startErr.Error()
		m.addUIEvent(summarizeError("agent", startedMsg.startErr.Error()))
		m.logWriteError("agent", startedMsg.startErr)
		return m, nil
	}
	if startedMsg.querySession != nil {
		m.querySession = startedMsg.querySession
		m.candidateCountAtTurnStart = len(startedMsg.querySession.Candidates)
		m.syncQuerySession()
		m.queuedMessage = ""
	}
	m.message.SetValue("")
	m.composerReference = nil
	m.evidenceMode = evidenceModeData
	m.progressOperation = progressOperationPreparing
	m.turnEvents = nil
	m.liveResponse = ""
	return m, m.nextAgentUpdateCmd(startedMsg.updates)
}

// applyAgentTurnUpdate folds one streamed agent event, or the turn result, into the workspace.
func (m Model) applyAgentTurnUpdate(updateMsg agentTurnUpdateMsg) (Model, tea.Cmd) {
	if updateMsg.update.Event != nil {
		event := *updateMsg.update.Event
		m.turnEvents = append(m.turnEvents, event)
		if updateMsg.update.QuerySession != nil {
			m.querySession = updateMsg.update.QuerySession
			m.syncQuerySession()
		}
		m.applyTurnEvidenceMode()
		m.recordAgentActivities([]agent.Event{event})
		if event.Kind == agent.EventKindMessageDelta {
			m.liveResponse += event.Message
			m.status = "agent output streaming"
			m.syncChatCursor()
		} else if summary := summarizeAgentEvent(event); summary != "" {
			m.addUIEvent(summary)
		}
	}
	if !updateMsg.update.Done {
		return m, m.nextAgentUpdateCmd(updateMsg.updates)
	}
	m.finishTurn()
	m.querySession = updateMsg.update.QuerySession
	m.executionPreviewOffset = 0
	m.syncQuerySession()
	m.applyTurnEvidenceMode()
	m.logAgentTurn(m.turnEvents)
	m.liveResponse = ""
	if updateMsg.update.Err != nil {
		m.status = updateMsg.update.Err.Error()
		m.addUIEvent(summarizeError("agent", updateMsg.update.Err.Error()))
		m.logWriteError("agent", updateMsg.update.Err)
		return m, nil
	}
	m.message.SetValue("")
	m.status = "agent turn complete"
	m.addUIEvent("agent: turn complete")
	m.logQuerySession(m.querySession)
	if m.shouldExecuteGeneratedCandidate(true) {
		return m, m.executeGeneratedCandidate()
	}
	return m, nil
}

// shouldExecuteGeneratedCandidate reports whether the current candidate is a valid generated query that has not run automatically.
func (m Model) shouldExecuteGeneratedCandidate(requireNewAgentCandidate bool) bool {
	if m.querySession == nil {
		return false
	}
	if requireNewAgentCandidate && len(m.querySession.Candidates) <= m.candidateCountAtTurnStart {
		return false
	}
	candidateIndex := m.querySession.SelectedCandidateIndex()
	currentCandidate := m.querySession.CurrentCandidate()
	if currentCandidate == nil || currentCandidate.Source != session.CandidateSourceAgent || !currentCandidate.Validation.Valid {
		return false
	}
	if m.hasAutoExecutedCandidate && candidateIndex == m.autoExecutedCandidateIndex {
		return false
	}
	generatedQuery := strings.TrimSpace(currentCandidate.Query)
	if generatedQuery == "" || strings.TrimSpace(m.query.Value()) != generatedQuery {
		return false
	}
	if requireNewAgentCandidate && m.agentExecutedCandidate(generatedQuery) {
		return false
	}
	return true
}

// agentExecutedCandidate reports whether this agent turn already executed the generated query.
func (m Model) agentExecutedCandidate(query string) bool {
	for _, event := range m.turnEvents {
		if event.Kind != agent.EventKindToolResult || event.Origin != agent.EventOriginToolBridge ||
			event.ToolName != bridge.ToolExecuteBydbQL || event.Status != agent.EventStatusSucceeded {
			continue
		}
		if strings.TrimSpace(event.Candidate) == query {
			return true
		}
	}
	return false
}

// executeGeneratedCandidate runs a newly generated candidate without reinterpreting editor text as a manual edit.
func (m *Model) executeGeneratedCandidate() tea.Cmd {
	m.autoExecutedCandidateIndex = m.querySession.SelectedCandidateIndex()
	m.hasAutoExecutedCandidate = true
	return m.startOperation(progressOperationExecute, "executing generated query", "automatically execute generated query", m.executeGeneratedCmd)
}

// applyCatalog records a catalog refresh or its connection failure.
func (m Model) applyCatalog(catalogResult catalogMsg) (Model, tea.Cmd) {
	m.busy = false
	if catalogResult.loadErr != nil {
		m.catalog.setLoadError(catalogResult.loadErr.Error())
		m.status = "BanyanDB connection failed: " + catalogResult.loadErr.Error()
		m.addUIEvent(m.status)
		m.logWriteError("catalog", catalogResult.loadErr)
		return m, nil
	}
	m.catalog.setCatalog(catalogResult.catalog)
	m.status = fmt.Sprintf("catalog loaded: %d resources in %d groups", len(catalogResult.catalog.Entries), len(catalogResult.catalog.Groups))
	m.addUIEvent(m.status)
	m.logWrite("catalog", m.status)
	return m, nil
}

// applySchemaDetail caches one discovered resource schema for the evidence panel.
func (m Model) applySchemaDetail(detailMsg schemaDetailMsg) (Model, tea.Cmd) {
	m.clearSchemaLoad(detailMsg.entry)
	if detailMsg.loadErr != nil {
		m.addUIEvent("schema detail: " + detailMsg.loadErr.Error())
		m.logWriteError("schema", detailMsg.loadErr)
		return m, nil
	}
	m.cacheSchema(detailMsg.snapshot)
	if !m.schemaSearchOpen() || m.isCurrentSchemaSearchEntry(detailMsg.entry) {
		m.selectedSchema = detailMsg.snapshot
	}
	if detailMsg.snapshot.Loaded {
		m.schemaDetailScroll = 0
	}
	return m, nil
}

// applyWorkflow folds a completed validation or execution into the workspace.
func (m Model) applyWorkflow(workflowResult workflowMsg) (Model, tea.Cmd) {
	m.finishTurn()
	if workflowResult.querySession != nil {
		m.querySession = workflowResult.querySession
		m.syncQuerySession()
	}
	m.addAgentEvents(workflowResult.events)
	m.recordAgentActivities(workflowResult.events)
	if workflowResult.clearTurnHint {
		m.message.SetValue("")
	}
	// A schema lookup carries no candidate, so the validation bookkeeping below has nothing to report.
	if m.querySession != nil && !workflowResult.schemaAnswer {
		m.recordWorkflowSessionState(workflowResult.status)
	}
	if workflowResult.err != nil {
		m.status = workflowResult.err.Error()
		m.addUIEvent(summarizeError("error", workflowResult.err.Error()))
		m.logWriteError("workflow", workflowResult.err)
		return m, nil
	}
	if workflowResult.schemaAnswer {
		m.applySchemaAnswer()
		// The reference belonged to the sent message; leaving it set would pin the next turn too.
		m.composerReference = nil
	}
	if workflowResult.status != "" {
		m.addUIEvent(summarizeStatusEvent(workflowResult.status))
		m.logWrite("workflow", workflowResult.status)
		m.status = workflowResult.status
		if workflowResult.status == statusExecutionComplete && m.shouldExecuteGeneratedCandidate(false) {
			return m, m.executeGeneratedCandidate()
		}
	} else if m.querySession != nil && !m.querySession.Validation.Valid && m.querySession.CurrentCandidate() != nil {
		m.status = "invalid candidate — Ctrl+G lets Agent fix it, or send a message"
		m.addUIEvent("validation: Ctrl+G lets Agent fix the candidate")
	}
	return m, nil
}

// recordWorkflowSessionState logs validation hints and focuses fresh execution results.
func (m *Model) recordWorkflowSessionState(status string) {
	if validationHint := formatValidationHint(m.querySession.Validation.Message); validationHint != "" {
		m.addUIEvent(validationHint)
		m.logWrite("validation", m.querySession.Validation.Message)
	}
	currentCandidate := m.querySession.CurrentCandidate()
	if currentCandidate != nil && strings.TrimSpace(currentCandidate.Query) != "" && !m.querySession.Validation.Valid {
		if invalidHint := formatInvalidCandidateHint(currentCandidate.Query); invalidHint != "" {
			m.addUIEvent(invalidHint)
		}
		m.logWrite("candidate", currentCandidate.Query)
	}
	m.logQuerySession(m.querySession)
	if status != statusExecutionComplete {
		return
	}
	m.executionDetailScroll = 0
	m.executionPreviewOffset = 0
	m.executionExportPath = ""
	m.evidenceMode = evidenceModeData
	m.executionRowCursor = -1
	if len(m.querySession.ExecutionResult.Preview) > 0 {
		m.executionRowCursor = 0
	}
	m.recordExecutionActivity(m.querySession)
	m.focus = focusExecution
}

// applySchemaAnswer points the workspace at the schema a direct lookup just read.
//
// The turn produced no rows, so leaving focus on Data Preview would show a stale result beside a
// message describing something else.
func (m *Model) applySchemaAnswer() {
	if m.querySession == nil {
		return
	}
	m.selectedSchema = m.querySession.SchemaSnapshot
	m.evidenceMode = evidenceModeSchemaPinned
	m.schemaDetailScroll = 0
	m.schemaSearchDismissed = true
	// The placeholder that showed the message while the turn ran is now a duplicate of the recorded
	// one, and it holds the cursor on an entry with no detail instead of on the description.
	m.queuedMessage = ""
	m.syncChatCursor()
	if m.focus == focusExecution {
		m.focus = focusChat
	}
	m.logSchemaAnswer()
	m.logQuerySession(m.querySession)
}

// focusEvidencePanel enters the evidence slot without changing which panel occupies it.
//
// Focusing is how a schema gets read and scrolled, and on a narrow terminal it is the only way to
// reach one at all, so it closes the live search preview but keeps a schema the turn looked up.
func (m *Model) focusEvidencePanel() {
	m.schemaSearchDismissed = true
	if m.evidenceMode == evidenceModeSchema {
		m.evidenceMode = evidenceModeData
	}
}

// finishTurn clears the bookkeeping shared by every way a turn can end.
func (m *Model) finishTurn() {
	m.busy = false
	m.turnStartedAt = time.Time{}
	m.turnCancel = nil
}

// View implements tea.Model.
func (m Model) View() string {
	view, _ := m.renderView()
	return view
}

// renderView renders the whole screen and reports the click target of every visible panel.
func (m Model) renderView() (string, []panelRegion) {
	if m.width < minTerminalWidth || m.height < minTerminalHeight {
		return m.renderTooSmall(), nil
	}
	header, footer, contentWidth, bodyHeight := m.workspaceFrame()
	if m.helpVisible {
		return lipgloss.JoinVertical(lipgloss.Left,
			header, m.renderHelpOverlay(contentWidth, bodyHeight), footer), nil
	}
	body, regions := m.renderWorkspaceWithRegions(contentWidth, bodyHeight, lipgloss.Height(header))
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer), regions
}

// workspaceFrame renders the persistent chrome and returns the space left for the workspace panels.
func (m Model) workspaceFrame() (string, string, int, int) {
	contentWidth := clamp(m.width-4, minTerminalWidth-4, 200)
	header := m.renderWorkspaceHeader(contentWidth)
	if m.catalog.loadError != "" {
		connectionError := truncate(glyphFailed+" BanyanDB connection failed: "+tuitext.SingleLine(m.catalog.loadError), contentWidth)
		header = lipgloss.JoinVertical(lipgloss.Left, header,
			badStyle.Render(connectionError),
			mutedStyle.Render("Ctrl+L retries the catalog load"))
	}
	footer := m.renderFooter(contentWidth)
	bodyHeight := max(m.height-lipgloss.Height(header)-lipgloss.Height(footer), 1)
	return header, footer, contentWidth, bodyHeight
}

// Minimum terminal size below which the workspace cannot render legibly.
const (
	minTerminalWidth  = 60
	minTerminalHeight = 18
)

// renderTooSmall replaces the workspace with an actionable message instead of a broken layout.
func (m Model) renderTooSmall() string {
	width := max(m.width, 1)
	rows := []string{
		titleStyle.Render(truncate("bydbctl · text2bydbQL", width)),
		warnStyle.Render(truncate(glyphWarn+" Terminal too small", width)),
		mutedStyle.Render(truncate(fmt.Sprintf("Need %d×%d, have %d×%d",
			minTerminalWidth, minTerminalHeight, m.width, m.height), width)),
		mutedStyle.Render(truncate("Resize, or press Esc twice to quit", width)),
	}
	return lipgloss.JoinVertical(lipgloss.Left, fitRows(rows, max(m.height, 1))...)
}

// refreshPanelRegions recomputes click targets so a click maps to what the user currently sees.
func (m *Model) refreshPanelRegions() {
	_, regions := m.renderView()
	m.panelRegions = regions
	m.regionsStale = false
}

// statusAskingAgent is the status shown while a turn is in flight.
const statusAskingAgent = "asking agent"

// statusSchemaComplete is the status of a turn answered straight from the schema catalog.
const statusSchemaComplete = "schema lookup complete"

// statusExecutionComplete is the status of a successful candidate execution.
const statusExecutionComplete = "execution complete"

// cancelActive stops the in-flight turn on both sides: the local context and the provider session.
func (m *Model) cancelActive() {
	if m.turnCancel != nil {
		m.turnCancel()
		m.turnCancel = nil
	}
	if stopErr := m.runner.StopAgentTurn(context.Background(), m.querySession); stopErr != nil {
		m.logWriteError("agent", stopErr)
	}
	m.status = "stopped"
	m.busy = false
	m.turnStartedAt = time.Time{}
	m.addUIEvent("workflow: stopped by user")
}

func (m *Model) startOperation(operation progressOperation, status, logMessage string, command func(context.Context) tea.Cmd) tea.Cmd {
	m.busy = true
	m.turnEvents = nil
	m.progressOperation = operation
	m.status = status
	m.turnStartedAt = time.Now()
	m.logWrite("action", logMessage)
	operationCtx, cancelOperation := context.WithCancel(context.Background())
	m.turnCancel = cancelOperation
	return tea.Batch(command(operationCtx), m.turnTimeoutCmd(m.turnStartedAt))
}

func (m *Model) sendComposerMessage() (tea.Cmd, bool) {
	if m.busy {
		return nil, true
	}
	messageValue := strings.TrimSpace(m.message.Value())
	if messageValue == "" {
		m.addUIEvent("message required before asking agent")
		return nil, true
	}
	describeRequest, describesSchema := m.resolveDescribeTarget(messageValue)
	m.queuedMessage = messageValue
	m.messageHistory.record(messageValue)
	m.liveResponse = ""
	m.editingQuery = false
	m.message.SetValue("")
	m.updateSchemaSearch()
	m.syncChatCursor()
	if describesSchema {
		logMessage := fmt.Sprintf("describe schema=%s/%s type=%s question=%q",
			describeRequest.Group, describeRequest.Name, describeRequest.ResourceType, messageValue)
		command := func(operationCtx context.Context) tea.Cmd {
			return m.describeCmd(operationCtx, describeRequest, messageValue)
		}
		return m.startOperation(progressOperationSchema, "reading schema from BanyanDB", logMessage, command), true
	}
	logMessage := fmt.Sprintf("send agent message=%q reference=%s", messageValue, describeReferenceLabel(m.composerReference))
	command := func(operationCtx context.Context) tea.Cmd {
		return m.agentCmd(operationCtx, messageValue)
	}
	return m.startOperation(progressOperationPreparing, statusAskingAgent, logMessage, command), true
}

// describeReferenceLabel names the composer reference for the session log, or reports its absence.
func describeReferenceLabel(reference *session.CatalogEntry) string {
	if reference == nil {
		return "none"
	}
	return reference.Group + "/" + reference.Name
}

// resolveDescribeTarget reports whether the composed message is a schema lookup bydbctl can serve.
//
// A question about the shape of one named resource is answered by the same BanyanDB schema call the
// agent would make, so it is served directly: no provider round trip, and no BYDBQL candidate.
func (m Model) resolveDescribeTarget(messageValue string) (workflow.DescribeRequest, bool) {
	if m.executor == nil {
		return workflow.DescribeRequest{}, false
	}
	entries := m.catalog.catalog.Entries
	if len(entries) == 0 && m.querySession != nil {
		entries = m.querySession.SchemaSnapshot.Catalog
	}
	return workflow.ResolveDescribeTarget(messageValue, m.composerReference, entries)
}

func (m *Model) repairCurrentCandidate() (tea.Cmd, bool) {
	if m.busy {
		return nil, true
	}
	if m.querySession == nil {
		m.status = "an invalid candidate is required before Agent repair"
		return nil, true
	}
	currentCandidate := m.querySession.CurrentCandidate()
	if currentCandidate == nil || currentCandidate.Validation.Valid {
		m.status = "an invalid candidate is required before Agent repair"
		return nil, true
	}
	const repairRequest = "Repair the current invalid BYDBQL candidate using the validation error."
	m.editingQuery = false
	m.queuedMessage = repairRequest
	m.liveResponse = ""
	m.syncChatCursor()
	command := func(operationCtx context.Context) tea.Cmd {
		return m.agentCmd(operationCtx, repairRequest)
	}
	return m.startOperation(progressOperationPreparing, "asking Agent to repair candidate", "ctrl+g repair invalid candidate", command), true
}

func (m Model) exportResult() (session.ExecutionResult, bool) {
	if m.querySession == nil {
		return session.ExecutionResult{}, false
	}
	executionResult := m.querySession.ExecutionResult
	if strings.TrimSpace(executionResult.Response) == "" && len(executionResult.Preview) == 0 {
		return session.ExecutionResult{}, false
	}
	return executionResult, true
}

func (m *Model) syncFocus() tea.Cmd {
	m.message.Blur()
	m.start.Blur()
	m.end.Blur()
	m.limit.Blur()
	m.query.Blur()
	switch m.focus {
	case focusChat:
		return nil
	case focusMessage:
		return m.message.Focus()
	case focusStart:
		return m.start.Focus()
	case focusEnd:
		return m.end.Focus()
	case focusLimit:
		return m.limit.Focus()
	case focusQuery:
		return m.query.Focus()
	case focusExecution:
		return nil
	default:
		return nil
	}
}

// updateFocused routes input to the focused control.
//
// Editing stays live while a background turn runs so a validation or agent round trip never swallows keystrokes.
func (m *Model) updateFocused(teaMsg tea.Msg) tea.Cmd {
	var updateCmd tea.Cmd
	previousQuery := m.query.Value()
	switch m.focus {
	case focusChat:
		return nil
	case focusMessage:
		m.message, updateCmd = m.message.Update(teaMsg)
		m.updateSchemaSearch()
		updateCmd = tea.Batch(updateCmd, m.loadSchemaDetailForSearch())
	case focusStart:
		m.start, updateCmd = m.start.Update(teaMsg)
		m.applyCandidateTimeRange()
	case focusEnd:
		m.end, updateCmd = m.end.Update(teaMsg)
		m.applyCandidateTimeRange()
	case focusLimit:
		m.limit, updateCmd = m.limit.Update(teaMsg)
		if m.limit.Value() != extractCandidateLimit(previousQuery) {
			m.applyCandidateLimit()
		}
	case focusQuery:
		m.query, updateCmd = m.query.Update(teaMsg)
		if previousQuery != m.query.Value() {
			m.limit.SetValue(extractCandidateLimit(m.query.Value()))
			start, end := extractCandidateTimeRange(m.query.Value())
			m.start.SetValue(start)
			m.end.SetValue(end)
		}
	case focusExecution:
		return nil
	}
	if (m.focus == focusStart || m.focus == focusEnd || m.focus == focusLimit || m.focus == focusQuery) && previousQuery != m.query.Value() {
		m.editingQuery = true
	}
	return updateCmd
}

func (m *Model) resize(width, height int) {
	m.width = width
	m.height = height
	contentWidth := clamp(width-4, 48, 200)
	queryLeftWidth, _ := workspaceWidths(contentWidth)
	timeInputWidth := max(10, (queryLeftWidth-24)/2)
	m.message.SetWidth(max(18, queryLeftWidth-4))
	m.query.SetWidth(max(18, queryLeftWidth-4))
	m.start.Width = timeInputWidth
	m.end.Width = timeInputWidth
	m.limit.Width = 8
	if contentWidth < 100 {
		m.query.SetWidth(max(18, contentWidth-4))
	}
	queryHeight := clamp(height-18, 8, 16)
	m.query.SetHeight(queryHeight)
	m.message.SetHeight(clamp(height/12, 3, 5))
}

// addAgentEvents surfaces the events worth showing and records the whole turn to the session log.
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
	m.recordActivity("workflow", event, "")
}

// clamp bounds value to the inclusive range between minValue and maxValue.
func clamp(value, minValue, maxValue int) int {
	return min(max(value, minValue), maxValue)
}
