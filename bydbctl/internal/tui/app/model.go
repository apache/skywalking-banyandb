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
	"regexp"
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
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

const (
	defaultWidth            = 120
	defaultHeight           = 36
	queryValidationDebounce = 350 * time.Millisecond
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
	message                textarea.Model
	query                  textarea.Model
	turnStartedAt          time.Time
	executor               tools.Executor
	composerReference      *session.CatalogEntry
	querySession           *session.QuerySession
	runner                 *workflow.Runner
	schemaCache            map[string]session.SchemaSnapshot
	schemaLoads            map[string]struct{}
	turnCancel             context.CancelFunc
	sessionLog             *applog.Logger
	queuedMessage          string
	schemaSearchValue      string
	provider               string
	status                 string
	executionExportPath    string
	liveResponse           string
	turnEvents             []agent.Event
	activityLog            []activityEntry
	panelRegions           []panelRegion
	messageHistory         editorHistory
	limit                  textinput.Model
	end                    textinput.Model
	start                  textinput.Model
	selectedSchema         session.SchemaSnapshot
	catalog                catalogBrowser
	focus                  int
	schemaSearchCursor     int
	chatDetailScroll       int
	chatScroll             int
	helpScroll             int
	progressOperation      progressOperation
	schemaDetailScroll     int
	executionPreviewOffset int
	executionRowCursor     int
	executionDetailScroll  int
	queryRevision          int
	chatCursor             int
	height                 int
	evidenceMode           evidenceMode
	width                  int
	regionsStale           bool
	busy                   bool
	helpVisible            bool
	quitConfirmPending     bool
	editingQuery           bool
	schemaSearchDismissed  bool
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
	case queryDebounceMsg:
		if typedMsg.revision != m.queryRevision || m.busy || strings.TrimSpace(m.query.Value()) == "" {
			return m, nil
		}
		m.busy = true
		m.progressOperation = progressOperationValidate
		m.status = "validating edited query"
		return m, m.validateCmd()
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
	return m, nil
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
	if status != "execution complete" {
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
	contentWidth := clamp(m.width-4, minTerminalWidth-4, 200)
	header := m.renderWorkspaceHeader(contentWidth)
	if m.catalog.loadError != "" {
		connectionError := truncate(glyphFailed+" BanyanDB connection failed: "+singleLine(m.catalog.loadError), contentWidth)
		header = lipgloss.JoinVertical(lipgloss.Left, header,
			badStyle.Render(connectionError),
			mutedStyle.Render("Ctrl+L retries the catalog load"))
	}
	footer := m.renderFooter(contentWidth)
	headerHeight := lipgloss.Height(header)
	bodyHeight := maxInt(m.height-headerHeight-lipgloss.Height(footer), 1)
	if m.helpVisible {
		return lipgloss.JoinVertical(lipgloss.Left,
			header, m.renderHelpOverlay(contentWidth, bodyHeight), footer), nil
	}
	body, regions := m.renderWorkspaceWithRegions(contentWidth, bodyHeight, headerHeight)
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer), regions
}

// Minimum terminal size below which the workspace cannot render legibly.
const (
	minTerminalWidth  = 60
	minTerminalHeight = 18
)

// renderTooSmall replaces the workspace with an actionable message instead of a broken layout.
func (m Model) renderTooSmall() string {
	width := maxInt(m.width, 1)
	rows := []string{
		titleStyle.Render(truncate("bydbctl · text2bydbQL", width)),
		warnStyle.Render(truncate(glyphWarn+" Terminal too small", width)),
		mutedStyle.Render(truncate(fmt.Sprintf("Need %d×%d, have %d×%d",
			minTerminalWidth, minTerminalHeight, m.width, m.height), width)),
		mutedStyle.Render(truncate("Resize, or press Esc twice to quit", width)),
	}
	return lipgloss.JoinVertical(lipgloss.Left, fitRows(rows, maxInt(m.height, 1))...)
}

// refreshPanelRegions recomputes click targets so a click maps to what the user currently sees.
func (m *Model) refreshPanelRegions() {
	_, regions := m.renderView()
	m.panelRegions = regions
	m.regionsStale = false
}

type catalogMsg struct {
	loadErr error
	catalog session.SchemaCatalog
}

type schemaDetailMsg struct {
	loadErr  error
	entry    session.CatalogEntry
	snapshot session.SchemaSnapshot
}

type workflowMsg struct {
	err           error
	querySession  *session.QuerySession
	status        string
	events        []agent.Event
	clearTurnHint bool
	// schemaAnswer marks a turn answered from the schema catalog rather than by running a query.
	schemaAnswer bool
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

type turnTimeoutMsg struct {
	startedAt time.Time
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
	return tea.Tick(queryValidationDebounce, func(time.Time) tea.Msg {
		return queryDebounceMsg{revision: revision}
	})
}

// syncQuerySession mirrors session state into the editor without discarding an in-progress manual edit.
func (m *Model) syncQuerySession() {
	if m.querySession == nil {
		return
	}
	if !m.editingQuery {
		if m.querySession.CandidateSuperseded {
			m.query.SetValue("")
			m.limit.SetValue("")
			m.syncChatCursor()
			return
		}
		if currentCandidate := m.querySession.CurrentCandidate(); currentCandidate != nil {
			m.setQueryValue(currentCandidate.Query)
		}
	}
	if strings.TrimSpace(m.querySession.SchemaSnapshot.Name) != "" {
		m.cacheSchema(m.querySession.SchemaSnapshot)
		for _, cachedSchema := range m.querySession.Schemas {
			m.cacheSchema(cachedSchema)
		}
		m.selectedSchema = m.querySession.SchemaSnapshot
	}
	m.syncChatCursor()
}

// setQueryValue replaces the editor contents and refreshes the derived time and limit slots.
func (m *Model) setQueryValue(query string) {
	if m.query.Value() == query {
		return
	}
	m.query.SetValue(query)
	m.limit.SetValue(extractCandidateLimit(query))
	start, end := extractCandidateTimeRange(query)
	m.start.SetValue(start)
	m.end.SetValue(end)
}

var (
	candidateLimitPattern = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	candidateTimePattern  = regexp.MustCompile(`(?i)\bTIME\s+(?:BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'|([><]=?)\s+'([^']+)')`)
)

func extractCandidateLimit(query string) string {
	matches := candidateLimitPattern.FindStringSubmatch(query)
	if len(matches) != 2 {
		return ""
	}
	return matches[1]
}

func extractCandidateTimeRange(query string) (string, string) {
	matches := candidateTimePattern.FindStringSubmatch(query)
	if len(matches) != 5 {
		return "", ""
	}
	if matches[1] != "" || matches[2] != "" {
		return matches[1], matches[2]
	}
	if strings.HasPrefix(matches[3], ">") {
		return matches[4], ""
	}
	return "", matches[4]
}

func (m *Model) applyCandidateLimit() {
	query := strings.TrimSpace(m.query.Value())
	if query == "" {
		return
	}
	limitValue := strings.TrimSpace(m.limit.Value())
	if candidateLimitPattern.MatchString(query) {
		if limitValue == "" {
			m.query.SetValue(strings.TrimSpace(candidateLimitPattern.ReplaceAllString(query, "")))
		} else {
			m.query.SetValue(candidateLimitPattern.ReplaceAllString(query, "LIMIT "+limitValue))
		}
		return
	}
	if limitValue != "" {
		m.query.SetValue(query + " LIMIT " + limitValue)
	}
}

func (m *Model) applyCandidateTimeRange() {
	query := strings.TrimSpace(m.query.Value())
	if query == "" || !candidateTimePattern.MatchString(query) {
		return
	}
	start := strings.TrimSpace(m.start.Value())
	end := strings.TrimSpace(m.end.Value())
	if start == "" && end == "" {
		return
	}
	timeClause := ""
	switch {
	case start != "" && end != "":
		timeClause = fmt.Sprintf("TIME BETWEEN '%s' AND '%s'", start, end)
	case start != "":
		timeClause = fmt.Sprintf("TIME > '%s'", start)
	default:
		timeClause = fmt.Sprintf("TIME < '%s'", end)
	}
	m.query.SetValue(candidateTimePattern.ReplaceAllString(query, timeClause))
}

func newTextInput(value, placeholder string) textinput.Model {
	input := textinput.New()
	input.Placeholder = placeholder
	input.SetValue(value)
	input.Prompt = ""
	input.Width = 24
	return input
}

// statusAskingAgent is the status shown while a turn is in flight.
const statusAskingAgent = "asking agent"

// statusSchemaComplete is the status of a turn answered straight from the schema catalog.
const statusSchemaComplete = "schema lookup complete"

const (
	keyPageUp        = "pgup"
	keyArrowUp       = "up"
	pageScrollStep   = 8
	chatPanelPadding = 8
)

// chatPanelChrome counts the non-message rows of the conversation panel: title, activity, progress,
// the message counter, and the panel frame.
const chatPanelChrome = 6

// handleKey dispatches one key press, resolving overlays before navigation and workspace actions.
func (m *Model) handleKey(keyMsg tea.KeyMsg) (tea.Cmd, bool) {
	key := keyMsg.String()
	if m.quitConfirmPending {
		return m.resolveQuitConfirm(key)
	}
	if m.helpVisible {
		return m.resolveHelpKey(key)
	}
	if command, handled := m.handleNavigationKey(key); handled {
		return command, true
	}
	return m.handleActionKey(key)
}

// resolveHelpKey keeps the help overlay modal, closing it on the keys a user expects to dismiss with.
func (m *Model) resolveHelpKey(key string) (tea.Cmd, bool) {
	switch key {
	case "?", "esc", "q", "enter":
		m.helpVisible = false
		m.helpScroll = 0
		return nil, true
	case keyPageUp:
		m.scrollHelp(-pageScrollStep)
		return nil, true
	case "pgdown":
		m.scrollHelp(pageScrollStep)
		return nil, true
	case keyArrowUp, "k":
		m.scrollHelp(-1)
		return nil, true
	case "down", "j":
		m.scrollHelp(1)
		return nil, true
	default:
		return nil, true
	}
}

// handleNavigationKey moves focus, selection, and scroll position without starting work.
func (m *Model) handleNavigationKey(key string) (tea.Cmd, bool) {
	switch key {
	case "tab":
		m.cycleFocus(1)
		return m.syncFocus(), true
	case "shift+tab":
		m.cycleFocus(-1)
		return m.syncFocus(), true
	case "1", "2", "3", "4":
		if m.acceptsTextInput() {
			return nil, false
		}
		return m.focusPanelByNumber(key)
	case "alt+1", "alt+2", "alt+3", "alt+4":
		return m.focusPanelByNumber(strings.TrimPrefix(key, "alt+"))
	case "j", "k":
		if m.acceptsTextInput() {
			return nil, false
		}
		if key == "k" {
			return m.handleVerticalKey(keyArrowUp)
		}
		return m.handleVerticalKey("down")
	case "left", "right":
		if m.focus != focusExecution {
			return nil, false
		}
		delta := previewHorizontalScrollStep
		if key == "left" {
			delta = -previewHorizontalScrollStep
		}
		m.moveExecutionPreviewOffset(delta)
		return nil, true
	case keyArrowUp, "down":
		return m.handleVerticalKey(key)
	case keyPageUp, "pgdown":
		return m.handlePageKey(key)
	default:
		return nil, false
	}
}

// acceptsTextInput reports whether the focused control consumes plain characters.
//
// Vim-style navigation must not steal letters from an editor the user is typing into.
func (m Model) acceptsTextInput() bool {
	switch m.focus {
	case focusMessage, focusQuery, focusStart, focusEnd, focusLimit:
		return true
	default:
		return false
	}
}

// focusPanelByNumber jumps straight to a panel so no feature is more than one keypress away.
//
// A bare digit only reaches here outside an editor; Alt+digit works from anywhere.
func (m *Model) focusPanelByNumber(key string) (tea.Cmd, bool) {
	panelFocus, ok := map[string]int{
		"1": focusChat,
		"2": focusQuery,
		"3": focusMessage,
		"4": focusExecution,
	}[key]
	if !ok {
		return nil, false
	}
	m.focus = panelFocus
	if panelFocus == focusExecution {
		m.focusEvidencePanel()
	}
	m.status = m.focusLabel() + " focused"
	return m.syncFocus(), true
}

// handleVerticalKey moves the cursor of whichever list owns the focus.
func (m *Model) handleVerticalKey(key string) (tea.Cmd, bool) {
	delta := 1
	if key == keyArrowUp {
		delta = -1
	}
	if m.focus == focusMessage && m.schemaSearchOpen() {
		m.moveSchemaSearchCursor(delta)
		return m.loadSchemaDetailForSearch(), true
	}
	switch m.focus {
	case focusExecution:
		m.moveExecutionRowCursor(delta)
		return nil, true
	case focusChat:
		m.moveChatCursor(delta, m.chatListViewportHeight())
		return nil, true
	case focusMessage:
		return m.recallMessageHistory(key)
	default:
		return nil, false
	}
}

// recallMessageHistory restores an earlier composer message once the cursor leaves the draft.
func (m *Model) recallMessageHistory(key string) (tea.Cmd, bool) {
	recalled, ok := recallHistoryValue(&m.messageHistory, m.message, key)
	if !ok {
		return nil, false
	}
	m.message.SetValue(recalled)
	m.message.CursorEnd()
	m.updateSchemaSearch()
	m.status = m.messageHistory.statusLabel("message")
	return m.loadSchemaDetailForSearch(), true
}

// handlePageKey scrolls the detail view of whichever panel owns the focus.
func (m *Model) handlePageKey(key string) (tea.Cmd, bool) {
	delta := pageScrollStep
	if key == keyPageUp {
		delta = -pageScrollStep
	}
	switch m.focus {
	case focusExecution:
		if m.evidenceMode.showsSchema() {
			m.scrollSchemaDetail(delta)
			return nil, true
		}
		m.scrollExecutionDetail(delta, m.executionDetailViewportHeight())
		return nil, true
	case focusChat:
		m.moveChatDetailScroll(delta, chatDetailViewportHeight(m.chatPanelHeight(clamp(m.height-chatPanelPadding, 18, 40))))
		return nil, true
	default:
		if m.evidenceMode.showsSchema() {
			m.scrollSchemaDetail(delta)
			return nil, true
		}
		return nil, false
	}
}

// scrollSchemaDetail moves the schema evidence viewport, which can be taller than the panel.
func (m *Model) scrollSchemaDetail(delta int) {
	lineCount := len(schemaDetailLines(m.selectedSchema))
	if lineCount == 0 {
		m.schemaDetailScroll = 0
		return
	}
	m.schemaDetailScroll = clamp(m.schemaDetailScroll+delta, 0, maxInt(lineCount-1, 0))
}

// handleActionKey runs workspace commands that can start or stop work.
func (m *Model) handleActionKey(key string) (tea.Cmd, bool) {
	switch key {
	case "ctrl+c", "esc":
		return m.handleEscape()
	case "?":
		if m.acceptsTextInput() {
			return nil, false
		}
		m.helpVisible = true
		return nil, true
	case "/":
		if m.acceptsTextInput() {
			return nil, false
		}
		return m.openCatalogFilter()
	case "ctrl+l":
		if m.busy {
			return nil, true
		}
		m.busy = true
		m.catalog.setLoading()
		m.progressOperation = progressOperationCatalog
		m.status = "refreshing catalog"
		return m.loadCatalogCmd(), true
	case "ctrl+left", "ctrl+right":
		return m.selectCandidateVersion(key)
	case "enter":
		return m.handleEnterKey()
	case "ctrl+e":
		return m.executeCurrentCandidate()
	case "ctrl+g":
		return m.repairCurrentCandidate()
	case "ctrl+o":
		return m.exportCurrentResult()
	default:
		return nil, false
	}
}

// handleEscape unwinds one layer at a time so Esc never quits while something is still open.
func (m *Model) handleEscape() (tea.Cmd, bool) {
	if m.schemaSearchOpen() {
		m.schemaSearchDismissed = true
		// A search that matched nothing never took the slot, so a pinned schema still owns it.
		if m.evidenceMode == evidenceModeSchema {
			m.evidenceMode = evidenceModeData
		}
		m.status = "schema search closed"
		return nil, true
	}
	if m.busy {
		m.cancelActive()
		return nil, true
	}
	m.quitConfirmPending = true
	m.status = "quit? y confirms · any other key keeps working"
	return nil, true
}

// openCatalogFilter focuses the composer on a fresh schema search, the conventional filter entry point.
func (m *Model) openCatalogFilter() (tea.Cmd, bool) {
	m.focus = focusMessage
	m.schemaSearchDismissed = false
	if !strings.HasSuffix(m.message.Value(), "@") {
		m.message.SetValue(m.message.Value() + "@")
	}
	m.updateSchemaSearch()
	m.status = "searching the local catalog"
	return tea.Batch(m.syncFocus(), m.loadSchemaDetailForSearch()), true
}

// selectCandidateVersion steps back to a query the session recorded earlier.
func (m *Model) selectCandidateVersion(key string) (tea.Cmd, bool) {
	if m.querySession == nil || len(m.querySession.Candidates) == 0 {
		return nil, true
	}
	delta := -1
	if key == "ctrl+right" {
		delta = 1
	}
	if m.querySession.SelectCandidate(m.querySession.SelectedCandidateIndex() + delta) {
		m.editingQuery = false
		m.syncQuerySession()
		m.status = "loaded a previous query"
	}
	return nil, true
}

// handleEnterKey inserts a schema reference or sends the composed message.
func (m *Model) handleEnterKey() (tea.Cmd, bool) {
	if m.focus == focusMessage && m.schemaSearchOpen() {
		m.insertSchemaReference()
		return nil, true
	}
	if m.focus == focusMessage {
		return m.sendComposerMessage()
	}
	if m.busy && strings.Contains(m.status, "still working") {
		m.status = "still working — Esc stops the run"
		return nil, true
	}
	return nil, false
}

// executeCurrentCandidate runs the editor contents immediately.
func (m *Model) executeCurrentCandidate() (tea.Cmd, bool) {
	if m.busy {
		return nil, true
	}
	m.busy = true
	m.turnEvents = nil
	m.progressOperation = progressOperationExecute
	m.status = "executing full query"
	m.turnStartedAt = time.Now()
	m.logWrite("action", "ctrl+e full execute query")
	executeCtx, cancelExecute := context.WithCancel(context.Background())
	m.turnCancel = cancelExecute
	return tea.Batch(m.executeCmd(executeCtx), m.turnTimeoutCmd(m.turnStartedAt)), true
}

// exportCurrentResult writes the visible execution result to a local file.
func (m *Model) exportCurrentResult() (tea.Cmd, bool) {
	exportResult, ok := m.exportResult()
	if !ok {
		return nil, false
	}
	exportPath, exportErr := exportExecutionResult(exportResult)
	if exportErr != nil {
		m.status = exportErr.Error()
		m.addUIEvent("export failed: " + exportErr.Error())
		return nil, true
	}
	m.executionExportPath = exportPath
	m.status = "exported preview"
	m.addUIEvent("exported: " + exportPath)
	return nil, true
}

// resolveQuitConfirm answers the exit confirmation prompt.
func (m *Model) resolveQuitConfirm(key string) (tea.Cmd, bool) {
	switch key {
	case "y", "Y", "ctrl+c":
		return tea.Quit, true
	default:
		m.quitConfirmPending = false
		m.status = "quit canceled"
		return nil, true
	}
}

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
	m.turnEvents = nil
	m.liveResponse = ""
	m.editingQuery = false
	m.message.SetValue("")
	m.updateSchemaSearch()
	m.syncChatCursor()
	m.busy = true
	m.turnStartedAt = time.Now()
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	m.turnCancel = cancelTurn
	if describesSchema {
		m.progressOperation = progressOperationSchema
		m.status = "reading schema from BanyanDB"
		m.logWrite("action", fmt.Sprintf("describe schema=%s/%s type=%s question=%q",
			describeRequest.Group, describeRequest.Name, describeRequest.ResourceType, messageValue))
		return tea.Batch(m.describeCmd(turnCtx, describeRequest, messageValue), m.turnTimeoutCmd(m.turnStartedAt)), true
	}
	m.progressOperation = progressOperationPreparing
	m.status = statusAskingAgent
	m.logWrite("action", fmt.Sprintf("send agent message=%q reference=%s", messageValue, describeReferenceLabel(m.composerReference)))
	return tea.Batch(m.agentCmd(turnCtx, messageValue), m.turnTimeoutCmd(m.turnStartedAt)), true
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
	m.turnEvents = nil
	m.liveResponse = ""
	m.syncChatCursor()
	m.busy = true
	m.progressOperation = progressOperationPreparing
	m.status = "asking Agent to repair candidate"
	m.turnStartedAt = time.Now()
	m.logWrite("action", "ctrl+g repair invalid candidate")
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	m.turnCancel = cancelTurn
	return tea.Batch(m.agentCmd(turnCtx, repairRequest), m.turnTimeoutCmd(m.turnStartedAt)), true
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
		m.queryRevision++
		m.editingQuery = true
		return tea.Batch(updateCmd, m.queryDebounceCmd(m.queryRevision))
	}
	return updateCmd
}

func (m *Model) resize(width, height int) {
	m.width = width
	m.height = height
	contentWidth := clamp(width-4, 48, 200)
	queryLeftWidth, _ := workspaceWidths(contentWidth)
	timeInputWidth := maxInt(10, (queryLeftWidth-24)/2)
	m.message.SetWidth(maxInt(18, queryLeftWidth-4))
	m.query.SetWidth(maxInt(18, queryLeftWidth-4))
	m.start.Width = timeInputWidth
	m.end.Width = timeInputWidth
	m.limit.Width = 8
	if contentWidth < 100 {
		m.query.SetWidth(maxInt(18, contentWidth-4))
	}
	queryHeight := clamp(height-18, 8, 16)
	m.query.SetHeight(queryHeight)
	m.message.SetHeight(clamp(height/12, 3, 5))
}

func (m Model) turnTimeoutCmd(startedAt time.Time) tea.Cmd {
	return tea.Tick(20*time.Second, func(time.Time) tea.Msg {
		return turnTimeoutMsg{startedAt: startedAt}
	})
}

func (m Model) chatPanelHeight(totalHeight int) int {
	return clamp(totalHeight-8, 16, totalHeight-6)
}

func (m Model) chatListViewportHeight() int {
	panelHeight := m.chatPanelHeight(clamp(m.height-chatPanelPadding, 18, 40))
	detailBudget := 0
	if entries := chatEntries(m.querySession, m.liveResponse, m.queuedMessage); m.chatCursor >= 0 &&
		m.chatCursor < len(entries) && strings.TrimSpace(entries[m.chatCursor].detail) != "" {
		detailBudget = chatDetailViewportHeight(panelHeight)
	}
	return maxInt(panelHeight-chatPanelChrome-detailBudget, 3)
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

// describeCmd reads one resource schema and records it as a direct catalog answer.
func (m Model) describeCmd(ctx context.Context, request workflow.DescribeRequest, messageValue string) tea.Cmd {
	runner := m.runner
	options := m.startOptions()
	query := m.query.Value()
	querySession := m.querySession
	return func() tea.Msg {
		updatedSession, ensureErr := ensureSession(ctx, runner, querySession, options, query)
		if ensureErr != nil {
			return workflowMsg{err: ensureErr}
		}
		if describeErr := runner.DescribeResource(ctx, updatedSession, request, messageValue); describeErr != nil {
			return workflowMsg{querySession: updatedSession, err: describeErr}
		}
		return workflowMsg{
			querySession:  updatedSession,
			status:        statusSchemaComplete,
			clearTurnHint: true,
			schemaAnswer:  true,
		}
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
	options := workflow.StartOptions{
		TimeRange: session.TimeRange{
			Start: m.start.Value(),
			End:   m.end.Value(),
		},
		Goal: m.currentGoal(),
	}
	if m.composerReference != nil {
		options.ResourceType = m.composerReference.Type
		options.ResourceName = m.composerReference.Name
		options.Groups = []string{m.composerReference.Group}
		options.NameProvided = true
		options.GroupsProvided = true
		options.TypeProvided = true
	}
	return options
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
			return schemaDetailMsg{entry: entry, loadErr: fmt.Errorf("schema executor is not configured")}
		}
		snapshot, schemaErr := executor.DiscoverSchema(context.Background(), tools.SchemaRequest{
			Type:   entry.Type,
			Name:   entry.Name,
			Groups: []string{entry.Group},
		})
		if schemaErr != nil {
			return schemaDetailMsg{entry: entry, loadErr: schemaErr}
		}
		return schemaDetailMsg{entry: entry, snapshot: snapshot}
	}
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
	preview, ok := m.currentPreviewData()
	if !ok || len(preview.preview) == 0 {
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
	previewLength := len(preview.preview)
	if m.executionRowCursor >= previewLength {
		m.executionRowCursor = previewLength - 1
	}
	m.executionDetailScroll = 0
}

func (m *Model) moveExecutionPreviewOffset(delta int) {
	tableLines := m.dataPreviewTableLines()
	maxOffset := previewTableMaxHorizontalOffset(tableLines, m.dataPreviewViewportWidth())
	m.executionPreviewOffset = clamp(m.executionPreviewOffset+delta, 0, maxOffset)
}

func (m Model) dataPreviewTableLines() []string {
	data, ok := m.currentPreviewData()
	if !ok || data.errorText != "" || len(data.preview) == 0 {
		return nil
	}
	displayColumns := selectDisplayColumns(data.columns)
	projectedRows := projectPreviewRows(data.preview, data.columns, displayColumns)
	return formatPreviewTable(displayColumns, projectedRows, 0, m.executionRowCursor)
}

func (m Model) dataPreviewViewportWidth() int {
	contentWidth := clamp(m.width-4, 48, 200)
	_, previewWidth := workspaceWidths(contentWidth)
	return maxInt(previewWidth-4, 1)
}

func (m Model) executionBodyLines(width int) []string {
	if m.querySession == nil {
		return nil
	}
	return executionDetailLines(m.querySession.ExecutionResult, executionDisplayOptions{
		width:       width,
		selectedRow: m.executionRowCursor,
	})
}

// executionRowDetailLines renders the field-by-field detail of the selected result row.
//
// Without this the row cursor moves but the columns dropped from the table stay invisible.
func (m Model) executionRowDetailLines(width int) []string {
	data, ok := m.currentPreviewData()
	if !ok || m.executionRowCursor < 0 || m.executionRowCursor >= len(data.preview) {
		return nil
	}
	lines := []string{fmt.Sprintf("row %d/%d · %s",
		m.executionRowCursor+1, len(data.preview), fallback(data.resource, "result"))}
	for columnIndex, column := range data.columns {
		value := ""
		if columnIndex < len(data.preview[m.executionRowCursor]) {
			value = data.preview[m.executionRowCursor][columnIndex]
		}
		lines = append(lines, wrapRunes(column+": "+value, maxInt(width-2, 24))...)
	}
	return lines
}

// syncChatCursor selects the newest message and resets its detail scroll.
func (m *Model) syncChatCursor() {
	entryCount := chatEntryCount(m.querySession, m.liveResponse, m.queuedMessage)
	if entryCount == 0 {
		m.chatCursor = 0
		m.chatScroll = 0
		return
	}
	m.chatCursor = entryCount - 1
	m.chatDetailScroll = 0
	if m.chatCursor < 0 {
		m.chatCursor = 0
	}
	if m.chatCursor >= entryCount {
		m.chatCursor = entryCount - 1
	}
}

func (m *Model) moveChatCursor(delta, viewportHeight int) {
	entryCount := chatEntryCount(m.querySession, m.liveResponse, m.queuedMessage)
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
	entries := chatEntries(m.querySession, m.liveResponse, m.queuedMessage)
	if m.chatCursor < 0 || m.chatCursor >= len(entries) {
		return
	}
	detailLines := entries[m.chatCursor].detailLines(maxInt(m.width/2, 40))
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
	m.recordActivity("workflow", event, "")
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
	m.sessionLog.WriteSchemaSnapshot("schema_snapshot", querySession.SchemaSnapshot)
	m.sessionLog.WriteChatMessages(querySession.ChatMessages)
}

// logSchemaAnswer records the schema a direct lookup put on screen, and the state that keeps it there.
func (m *Model) logSchemaAnswer() {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteSchemaSnapshot("schema_answer", m.selectedSchema)
	m.logViewState("schema answer applied")
}

// logViewState records which evidence panel owns the slot, and why.
//
// A panel that renders once and disappears leaves two of these lines with different owners, which
// names the transition that dropped it.
func (m *Model) logViewState(reason string) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteViewState(fmt.Sprintf(
		"%s · evidence=%s shows_schema=%t search_open=%t search_dismissed=%t search_value=%q focus=%s selected_schema=%s/%s loaded=%t schema_lines=%d phase=%s busy=%t",
		reason,
		m.evidenceMode,
		m.showsSchemaEvidence(),
		m.schemaSearchOpen(),
		m.schemaSearchDismissed,
		m.schemaSearchValue,
		m.focusLabel(),
		strings.Join(m.selectedSchema.Groups, "|"),
		m.selectedSchema.Name,
		m.selectedSchema.Loaded,
		len(schemaDetailLines(m.selectedSchema)),
		m.currentPhaseLabel(),
		m.busy,
	))
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
