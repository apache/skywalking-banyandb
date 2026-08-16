// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package app

import (
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

const (
	schemaSearchSelectionPrefix      = "› "
	schemaSearchTruncationMarker     = "…"
	minSchemaSearchLabelWidth        = 1
	schemaSearchMinimumTruncateWidth = 3
	maxColdStartGroups               = 3
)

const (
	schemaSearchNameScore = iota
	schemaSearchExactTokenScore
	schemaSearchPrefixScore
	schemaSearchSubstringScore
	schemaSearchGroupScore
	schemaSearchTypeScore = schemaSearchGroupScore * 2

	schemaSearchRowsPerViewport   = 6
	maxSchemaSearchVisibleResults = 12

	minWorkspaceChatHeight      = 4
	minWorkspaceEditorHeight    = 1
	previewHorizontalScrollStep = 8
	// minEditorWidth keeps a textarea usable on the narrowest terminal the workspace renders in.
	minEditorWidth = 18
)

type evidenceMode int

const (
	evidenceModeData evidenceMode = iota
	// evidenceModeSchema is a schema the composer is previewing while an @ search is open.
	evidenceModeSchema
	// evidenceModeSchemaPinned is a schema a completed turn looked up, which outlives that search.
	//
	// Closing the search clears a preview, but the answer to a describe turn has to stay on screen:
	// the composer keeps ticking its cursor after the turn, and each tick reaches updateSchemaSearch.
	evidenceModeSchemaPinned
)

// showsSchema reports whether the mode puts a schema in the evidence slot, however it got there.
func (mode evidenceMode) showsSchema() bool {
	return mode == evidenceModeSchema || mode == evidenceModeSchemaPinned
}

// String names the mode for the session log.
func (mode evidenceMode) String() string {
	switch mode {
	case evidenceModeSchema:
		return "schema-preview"
	case evidenceModeSchemaPinned:
		return "schema-pinned"
	case evidenceModeData:
		return "data"
	default:
		return "unknown"
	}
}

// applyTurnEvidenceMode selects the evidence panel that matches the last completed controlled tool.
func (m *Model) applyTurnEvidenceMode() {
	for eventIndex := len(m.turnEvents) - 1; eventIndex >= 0; eventIndex-- {
		event := m.turnEvents[eventIndex]
		if event.Status != agent.EventStatusSucceeded ||
			(event.Kind != agent.EventKindToolResult && event.Kind != agent.EventKindCandidate) {
			continue
		}
		switch event.ToolName {
		case bridge.ToolDescribeSchema:
			if m.querySession != nil && strings.TrimSpace(m.querySession.SchemaSnapshot.Name) != "" {
				m.selectedSchema = m.querySession.SchemaSnapshot
			}
			m.evidenceMode = evidenceModeSchemaPinned
			if m.focus == focusExecution {
				m.focus = focusChat
			}
			return
		case bridge.ToolProposeQueryPlan, bridge.ToolExecuteBydbQL:
			m.evidenceMode = evidenceModeData
			return
		}
	}
}

type turnProgressState int

const (
	turnProgressPending turnProgressState = iota
	turnProgressRunning
	turnProgressSucceeded
	turnProgressFailed
)

type turnProgressStage struct {
	label string
	state turnProgressState
}

type turnProgressStageID int

const (
	turnProgressStageCatalog turnProgressStageID = iota
	turnProgressStageDescribeSchema
	turnProgressStageCompilePlan
	turnProgressStageValidate
	turnProgressStageExecute
	turnProgressStageCount
)

type progressOperation int

const (
	progressOperationPreparing progressOperation = iota
	progressOperationCatalog
	progressOperationValidate
	progressOperationExecute
	progressOperationSchema
)

func (operation progressOperation) label() string {
	switch operation {
	case progressOperationCatalog:
		return "catalog"
	case progressOperationValidate:
		return "validate"
	case progressOperationExecute:
		return "execute"
	case progressOperationSchema:
		return "describe schema"
	default:
		return "preparing"
	}
}

func (m Model) renderTurnProgress() string {
	if !m.busy && len(m.turnEvents) == 0 {
		return ""
	}
	stages := [turnProgressStageCount]turnProgressStage{
		turnProgressStageCatalog:        {label: "catalog"},
		turnProgressStageDescribeSchema: {label: "describe schema"},
		turnProgressStageCompilePlan:    {label: "compile plan"},
		turnProgressStageValidate:       {label: "validate"},
		turnProgressStageExecute:        {label: "execute"},
	}
	observedStages := [turnProgressStageCount]bool{}
	for _, event := range m.turnEvents {
		stageIndex, ok := progressStageForEvent(event)
		if !ok {
			continue
		}
		observedStages[stageIndex] = true
		stages[stageIndex].state = progressStateForEvent(event)
	}
	parts := make([]string, 0, len(stages))
	for stageIndex, stage := range stages {
		if !observedStages[stageIndex] {
			continue
		}
		parts = append(parts, renderTurnProgressStage(stage))
	}
	if len(parts) == 0 && m.busy {
		return mutedStyle.Render("Steps  " + warnStyle.Render("⟳ "+m.progressOperation.label()))
	}
	if len(parts) == 0 {
		return ""
	}
	return mutedStyle.Render("Steps  " + strings.Join(parts, " · "))
}

func progressStageForEvent(event agent.Event) (turnProgressStageID, bool) {
	switch event.ToolName {
	case bridge.ToolListGroupsSchemas:
		return turnProgressStageCatalog, true
	case bridge.ToolDescribeSchema:
		return turnProgressStageDescribeSchema, true
	case bridge.ToolProposeQueryPlan:
		return turnProgressStageCompilePlan, true
	case bridge.ToolValidateBydbQL:
		return turnProgressStageValidate, true
	case bridge.ToolExecuteBydbQL:
		return turnProgressStageExecute, true
	default:
		return turnProgressStageCatalog, false
	}
}

func progressStateForEvent(event agent.Event) turnProgressState {
	if event.Kind == agent.EventKindToolCall || event.Status == agent.EventStatusRunning || event.Status == agent.EventStatusWaiting {
		return turnProgressRunning
	}
	if event.Status == agent.EventStatusFailed || event.Err != nil {
		return turnProgressFailed
	}
	if event.Status == agent.EventStatusSucceeded {
		return turnProgressSucceeded
	}
	return turnProgressPending
}

func renderTurnProgressStage(stage turnProgressStage) string {
	switch stage.state {
	case turnProgressRunning:
		return warnStyle.Render("⟳ " + stage.label)
	case turnProgressSucceeded:
		return okStyle.Render("✓ " + stage.label)
	case turnProgressFailed:
		return badStyle.Render("! " + stage.label)
	default:
		return mutedStyle.Render("○ " + stage.label)
	}
}

func (m Model) coldStartGuidance(width int) []string {
	if m.querySession != nil {
		return nil
	}
	rows := []string{titleStyle.Render("Welcome to text2bydbQL")}
	if len(m.catalog.catalog.Groups) == 0 {
		catalogStatus := "Type @ to browse groups and resources."
		if m.catalog.loading {
			catalogStatus = "Loading the BanyanDB schema catalog…"
		}
		return append(rows,
			mutedStyle.Render(wrapText(catalogStatus, width)),
			mutedStyle.Render(wrapText("Try: Which resource should I use to inspect errors?", width)),
			mutedStyle.Render(wrapText("Try: Show the latest 10 rows for the last 30 minutes.", width)),
		)
	}
	groups := append([]string(nil), m.catalog.catalog.Groups...)
	sort.Strings(groups)
	groupLabel := strings.Join(groups, ", ")
	if len(groups) > maxColdStartGroups {
		groupLabel = strings.Join(groups[:maxColdStartGroups], ", ") + fmt.Sprintf(" (+%d more)", len(groups)-maxColdStartGroups)
	}
	rows = append(rows, mutedStyle.Render(wrapText("Available groups: "+groupLabel, width)))
	if len(m.catalog.catalog.Entries) == 0 {
		return append(rows, mutedStyle.Render(wrapText("Type @ to search a group or resource, then ask a question.", width)))
	}
	entries := append([]session.CatalogEntry(nil), m.catalog.catalog.Entries...)
	sort.Slice(entries, func(leftIndex, rightIndex int) bool {
		leftEntry := entries[leftIndex]
		rightEntry := entries[rightIndex]
		if leftEntry.Group != rightEntry.Group {
			return leftEntry.Group < rightEntry.Group
		}
		if leftEntry.Name != rightEntry.Name {
			return leftEntry.Name < rightEntry.Name
		}
		return leftEntry.Type < rightEntry.Type
	})
	reference := "@" + entries[0].Group + "/" + entries[0].Name
	rows = append(rows,
		mutedStyle.Render(wrapText("Try: What fields does "+reference+" have?", width)),
		mutedStyle.Render(wrapText("Try: Show the latest 10 rows from "+reference+" for the last 30 minutes.", width)),
		mutedStyle.Render(wrapText("Try: Which resource should I use to inspect errors?", width)),
	)
	return rows
}

type previewData struct {
	resource  string
	query     string
	errorText string
	columns   []string
	preview   [][]string
	totalRows int
	truncated bool
}

type workspaceLeftLayout struct {
	chatHeight        int
	queryHeight       int
	messageHeight     int
	schemaResultLimit int
}

type schemaSearchResult struct {
	entry session.CatalogEntry
	score int
}

// stackedLayoutWidth is the content width below which the evidence panel stacks under the left column.
const stackedLayoutWidth = 100

func workspaceWidths(width int) (int, int) {
	if width < stackedLayoutWidth {
		return width, width
	}
	leftWidth := clamp(width*52/100, 52, 104)
	return leftWidth, width - leftWidth - 2
}

// workspaceIsStacked reports whether the evidence panel sits under the left column instead of beside it.
func workspaceIsStacked(width int) bool {
	return width < stackedLayoutWidth
}

func (m Model) focusOrder() []int {
	return []int{focusChat, focusMessage, focusStart, focusEnd, focusLimit, focusQuery, focusExecution}
}

func (m *Model) cycleFocus(delta int) {
	order := m.focusOrder()
	if len(order) == 0 {
		return
	}
	currentIndex := 0
	for index, focusValue := range order {
		if focusValue == m.focus {
			currentIndex = index
			break
		}
	}
	nextIndex := (currentIndex + delta) % len(order)
	if nextIndex < 0 {
		nextIndex += len(order)
	}
	m.focus = order[nextIndex]
}

// renderWorkspaceHeader names the tool and provider on a single row.
//
// A bordered provider chip would cost three rows of the height budget to show one word.
func (m Model) renderWorkspaceHeader(width int) string {
	header := titleStyle.Render("bydbctl · text2bydbQL") +
		mutedStyle.Render("   provider "+m.provider)
	if resourceLabel := m.headerResourceLabel(); resourceLabel != "" {
		header += mutedStyle.Render("   " + resourceLabel)
	}
	return lipgloss.NewStyle().Width(width).Render(truncate(header, maxInt(width, 8)))
}

// headerResourceLabel names the resource the session is working against, when one is known.
func (m Model) headerResourceLabel() string {
	if m.querySession == nil {
		return ""
	}
	resourceName := strings.TrimSpace(m.querySession.ResourceName)
	if resourceName == "" {
		return ""
	}
	if resourceType := strings.TrimSpace(string(m.querySession.ResourceType)); resourceType != "" {
		return resourceType + " " + resourceName
	}
	return resourceName
}

// renderWorkspaceWithRegions renders the workspace and reports where each panel landed on screen.
//
// originY is the first screen row occupied by the workspace body, so click coordinates map directly to panels.
func (m Model) renderWorkspaceWithRegions(width, height, originY int) (string, []panelRegion) {
	if workspaceIsStacked(width) {
		return m.renderStackedWorkspace(width, height, originY)
	}
	return m.renderSideBySideWorkspace(width, height, originY)
}

// renderSideBySideWorkspace places the evidence panel beside the left column, both bounded by height.
//
// A turn that answered in words has no evidence to show, so the conversation takes the full width
// rather than reading in half of it beside an empty panel.
func (m Model) renderSideBySideWorkspace(width, height, originY int) (string, []panelRegion) {
	if !m.evidencePanelHasContent() && m.focus != focusExecution {
		left, layout := m.renderWorkspaceLeft(width, height)
		return left, m.leftPanelRegions(width, layout, originY)
	}
	leftWidth, rightWidth := workspaceWidths(width)
	left, layout := m.renderWorkspaceLeft(leftWidth, height)
	right := m.renderEvidencePanel(rightWidth, height)
	regions := m.leftPanelRegions(leftWidth, layout, originY)
	if right != "" {
		regions = append(regions, panelRegion{
			focus:  focusExecution,
			top:    originY,
			bottom: originY + lipgloss.Height(right) - 1,
			left:   leftWidth + 2,
			right:  leftWidth + 2 + rightWidth - 1,
		})
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, left, "  ", right), regions
}

// renderStackedWorkspace shows one column at a time on a narrow terminal.
//
// Splitting the height between two stacked columns leaves both too short to use, so the focused
// evidence panel takes the whole body and the left column takes it back when focus leaves.
func (m Model) renderStackedWorkspace(width, height, originY int) (string, []panelRegion) {
	if m.focus == focusExecution && m.evidencePanelHasContent() {
		evidence := m.renderEvidencePanel(width, height-1)
		if evidence != "" {
			return lipgloss.JoinVertical(lipgloss.Left, evidence, m.stackedReturnHint(width)), []panelRegion{{
				focus:  focusExecution,
				top:    originY,
				bottom: originY + lipgloss.Height(evidence) - 1,
				left:   0,
				right:  width - 1,
			}}
		}
	}
	left, layout := m.renderWorkspaceLeft(width, height-1)
	regions := m.leftPanelRegions(width, layout, originY)
	return lipgloss.JoinVertical(lipgloss.Left, left, m.stackedEvidenceHint(width)), regions
}

// stackedReturnHint tells the user how to get back to the conversation from a full-screen panel.
func (m Model) stackedReturnHint(width int) string {
	return lipgloss.NewStyle().Width(width).Render(
		keyStyle.Render("1") + mutedStyle.Render(" back to the conversation"))
}

// stackedEvidenceHint points at the results panel that the narrow layout keeps off screen.
func (m Model) stackedEvidenceHint(width int) string {
	hint := mutedStyle.Render("no results yet")
	if m.evidencePanelHasContent() {
		// This hint is the only way to reach the panel on a narrow terminal, so it names what is in it.
		hint = keyStyle.Render("4") + mutedStyle.Render(" open "+m.evidencePanelLabel())
	}
	return lipgloss.NewStyle().Width(width).Render(hint)
}

// evidencePanelLabel describes the waiting panel by its contents, not just by its kind.
func (m Model) evidencePanelLabel() string {
	if !m.evidenceMode.showsSchema() {
		return "results"
	}
	if schemaName := strings.TrimSpace(m.selectedSchema.Name); schemaName != "" {
		return "schema " + schemaName
	}
	return "schema"
}

// evidencePanelHasContent reports whether the evidence panel has anything worth reserving rows for.
func (m Model) evidencePanelHasContent() bool {
	if _, ok := m.currentPreviewData(); ok {
		return true
	}
	return len(schemaDetailLines(m.selectedSchema)) > 0
}

// leftPanelRegions measures the stacked left-column panels in render order.
func (m Model) leftPanelRegions(width int, layout workspaceLeftLayout, originY int) []panelRegion {
	sections := m.leftSections(width, layout)
	regions := make([]panelRegion, 0, len(sections))
	row := originY
	for _, section := range sections {
		if section.content == "" {
			continue
		}
		sectionHeight := lipgloss.Height(section.content)
		if section.focus >= 0 {
			regions = append(regions, panelRegion{
				focus:  section.focus,
				top:    row,
				bottom: row + sectionHeight - 1,
				left:   0,
				right:  width - 1,
			})
		}
		row += sectionHeight
	}
	return regions
}

// leftSection is one stacked panel of the left column and the focus target it owns.
//
// A focus of noPanelFocus means the panel occupies rows but cannot be focused.
type leftSection struct {
	content string
	focus   int
}

// noPanelFocus marks a rendered section that is not a focus target.
const noPanelFocus = -1

// leftSections renders the left column once, in order, under the resolved layout.
//
// Both the visible view and the click regions are derived from this list so they cannot drift apart.
// The receiver is a value, so sizing the editors to the column stays local to this render.
func (m Model) leftSections(width int, layout workspaceLeftLayout) []leftSection {
	editorWidth := maxInt(width-panelStyle.GetHorizontalFrameSize()-2, minEditorWidth)
	m.query.SetWidth(editorWidth)
	m.message.SetWidth(editorWidth)
	m.query.SetHeight(layout.queryHeight)
	m.message.SetHeight(layout.messageHeight)
	return []leftSection{
		{focus: focusChat, content: m.renderChat(width, layout.chatHeight)},
		{focus: focusQuery, content: m.renderCandidateCard(width)},
		{focus: focusMessage, content: m.renderSchemaSearch(width, layout.schemaResultLimit)},
		{focus: focusMessage, content: m.renderMessage(width)},
		{focus: noPanelFocus, content: m.renderStatusLine(width)},
	}
}

func (m Model) renderWorkspaceLeft(width, height int) (string, workspaceLeftLayout) {
	schemaSearchResultLimit := schemaSearchViewportLimit(height, len(m.schemaSearchEntries()))
	layout := m.allocateLeftColumn(height)
	layout.schemaResultLimit = schemaSearchResultLimit
	for {
		left := m.renderWorkspaceLeftWithLayout(width, layout)
		heightOverflow := lipgloss.Height(left) - height
		if heightOverflow <= 0 {
			layout.chatHeight -= heightOverflow
			return m.renderWorkspaceLeftWithLayout(width, layout), layout
		}
		if layout.queryHeight > minWorkspaceEditorHeight {
			layout.queryHeight -= minInt(heightOverflow, layout.queryHeight-minWorkspaceEditorHeight)
			continue
		}
		if layout.chatHeight > minWorkspaceChatHeight {
			layout.chatHeight -= minInt(heightOverflow, layout.chatHeight-minWorkspaceChatHeight)
			continue
		}
		if layout.messageHeight > minWorkspaceEditorHeight {
			layout.messageHeight -= minInt(heightOverflow, layout.messageHeight-minWorkspaceEditorHeight)
			continue
		}
		if layout.schemaResultLimit > 1 {
			layout.schemaResultLimit--
			continue
		}
		// Every panel is at its minimum, so drop the trailing rows rather than overflow the terminal.
		return strings.Join(fitRows(strings.Split(left, "\n"), height), "\n"), layout
	}
}

// allocateLeftColumn divides the body height between the conversation, the QL editor, and the composer.
//
// The conversation is the primary content, so the editors give up their optional rows to it first.
func (m Model) allocateLeftColumn(height int) workspaceLeftLayout {
	messageHeight := clamp(m.message.Height(), minWorkspaceEditorHeight, maxComposerHeight)
	if m.candidateCardIsCollapsed() {
		// The collapsed card renders one row inside its frame, so the editor and slot rows come back.
		chatHeight := height - messageHeight - fixedLeftColumnRows + minCandidateEditorHeight + 1
		return workspaceLeftLayout{
			chatHeight:    maxInt(chatHeight, minWorkspaceChatHeight),
			queryHeight:   minCandidateEditorHeight,
			messageHeight: messageHeight,
		}
	}
	queryHeight := m.candidateEditorHeight()
	chatHeight := height - queryHeight - messageHeight - fixedLeftColumnRows
	for chatHeight < preferredChatHeight {
		switch {
		case queryHeight > minCandidateEditorHeight:
			queryHeight--
		case messageHeight > minWorkspaceEditorHeight:
			messageHeight--
		default:
			return workspaceLeftLayout{
				chatHeight:    maxInt(chatHeight, minWorkspaceChatHeight),
				queryHeight:   queryHeight,
				messageHeight: messageHeight,
			}
		}
		chatHeight++
	}
	return workspaceLeftLayout{chatHeight: chatHeight, queryHeight: queryHeight, messageHeight: messageHeight}
}

// Left-column sizing preferences.
const (
	// preferredChatHeight is the conversation height the layout tries to protect.
	preferredChatHeight = 12
	// maxComposerHeight caps the composer so it never crowds out the conversation.
	maxComposerHeight = 4
)

// candidateEditorHeight sizes the QL editor to the query it holds, plus room to keep typing.
//
// A fixed tall editor spends the height budget on blank rows the conversation panel needs.
func (m Model) candidateEditorHeight() int {
	editorWidth := maxInt(m.query.Width(), 1)
	lineCount := 1
	for _, queryLine := range strings.Split(m.query.Value(), "\n") {
		lineCount += maxInt((lipgloss.Width(queryLine)-1)/editorWidth, 0)
		lineCount++
	}
	return clamp(lineCount, minCandidateEditorHeight, maxCandidateEditorHeight)
}

// Candidate editor sizing bounds, and the rows the left column spends outside the chat and QL panels.
const (
	minCandidateEditorHeight = 3
	maxCandidateEditorHeight = 10
	// fixedLeftColumnRows counts the three panel frames, the candidate title and slot rows,
	// the composer title, and the status bar.
	fixedLeftColumnRows = 10
)

func schemaSearchViewportLimit(height, resultCount int) int {
	if resultCount == 0 {
		return 0
	}
	return minInt(resultCount, clamp(height/schemaSearchRowsPerViewport, 1, maxSchemaSearchVisibleResults))
}

func (m Model) renderWorkspaceLeftWithLayout(width int, layout workspaceLeftLayout) string {
	sections := m.leftSections(width, layout)
	rendered := make([]string, 0, len(sections))
	for _, section := range sections {
		if section.content == "" {
			continue
		}
		rendered = append(rendered, section.content)
	}
	return lipgloss.JoinVertical(lipgloss.Left, rendered...)
}

// renderCandidateCard renders the BYDBQL editor, or a single collapsed row while there is no query.
//
// Not every turn produces a query, so an empty editor would otherwise hold rows the conversation
// needs in order to show an answer.
func (m Model) renderCandidateCard(width int) string {
	if m.candidateCardIsCollapsed() {
		return m.renderCollapsedCandidateCard(width)
	}
	report := session.ValidationReport{Message: statusNotChecked}
	if m.querySession != nil {
		report = m.querySession.Validation
	}
	status := report.Status()
	statusStyle := badStyle
	if report.Valid {
		statusStyle = okStyle
	} else if status == statusNotChecked {
		statusStyle = mutedStyle
	}
	titleRow := m.panelTitle(focusQuery, "Candidate QL ") + statusStyle.Render(statusGlyphFor(report)+" "+status)
	if m.editingQuery {
		titleRow += warnStyle.Render("  edited locally")
	}
	rows := []string{
		titleRow,
		m.query.View(),
		lipgloss.JoinHorizontal(lipgloss.Top,
			m.slotLabel(focusStart, "Time"),
			m.start.View(),
			mutedStyle.Render(" → "),
			m.end.View(),
			m.slotLabel(focusLimit, "  Limit"),
			m.limit.View(),
		),
	}
	if !report.Valid && report.Message != "" && report.Message != statusNotChecked {
		rows = append(rows,
			badStyle.Render(glyphFailed+" "+truncate(report.Message, width-16)),
			mutedStyle.Render("Ctrl+G let Agent fix"),
		)
	}
	return m.candidatePanelStyle().Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

// candidateCardIsCollapsed reports whether the editor is empty and not being used.
//
// Focus expands it again, so a query can still be written by hand at any point.
func (m Model) candidateCardIsCollapsed() bool {
	switch m.focus {
	case focusQuery, focusStart, focusEnd, focusLimit:
		return false
	}
	if strings.TrimSpace(m.query.Value()) != "" {
		return false
	}
	return m.querySession == nil || len(m.querySession.Candidates) == 0
}

// renderCollapsedCandidateCard states why there is no query and how to get one.
func (m Model) renderCollapsedCandidateCard(width int) string {
	label := "Candidate QL"
	hint := "  no query yet · ask for data, or press "
	if m.querySession != nil && m.querySession.Phase == session.PhaseClarifying {
		hint = "  waiting on your reply · or press "
	}
	if m.querySession != nil && m.querySession.Phase == session.PhaseSchema {
		hint = "  schema lookup only · ask for data, or press "
	}
	row := titleStyle.Render("  "+label) + mutedStyle.Render(hint) +
		keyStyle.Render("Alt+2") + mutedStyle.Render(" to write one")
	return panelStyle.Width(width).Render(truncate(row, maxInt(width-panelStyle.GetHorizontalFrameSize(), 8)))
}

// statusGlyphFor pairs the validation status with a glyph so it reads without color.
func statusGlyphFor(report session.ValidationReport) string {
	switch {
	case report.Valid:
		return glyphOK
	case report.Message == "" || report.Message == statusNotChecked:
		return glyphPending
	default:
		return glyphFailed
	}
}

// candidatePanelStyle highlights the candidate card while any of its editable slots holds focus.
func (m Model) candidatePanelStyle() lipgloss.Style {
	switch m.focus {
	case focusQuery, focusStart, focusEnd, focusLimit:
		return activePanelStyle
	default:
		return panelStyle
	}
}

// slotLabel highlights an individual time or limit slot label when it holds focus.
//
// The marker stays on one line so moving focus between slots never changes the card height.
func (m Model) slotLabel(slotFocus int, label string) string {
	if m.focus == slotFocus {
		return focusStyle.Render(label) + " "
	}
	return mutedStyle.Render(label) + " "
}

func (m Model) renderSchemaSearch(width, resultLimit int) string {
	if !m.schemaSearchOpen() {
		return ""
	}
	entries := m.schemaSearchEntries()
	rows := []string{titleStyle.Render("@ search · local catalog")}
	if len(entries) == 0 {
		rows = append(rows, mutedStyle.Render("No group or resource matches · continue typing"))
		return panelStyle.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
	}
	labelWidth := maxInt(width-panelStyle.GetHorizontalFrameSize()-selectionPrefixWidth, minSchemaSearchLabelWidth)
	visibleStart := 0
	if resultLimit < len(entries) && m.schemaSearchCursor >= resultLimit {
		visibleStart = m.schemaSearchCursor - resultLimit + 1
	}
	visibleEnd := minInt(visibleStart+maxInt(resultLimit, 1), len(entries))
	for entryIndex := visibleStart; entryIndex < visibleEnd; entryIndex++ {
		entry := entries[entryIndex]
		label := truncateSchemaSearchLabel(
			fmt.Sprintf("%s/%s · %s", entry.Group, entry.Name, shortTypeLabel(entry.Type)), labelWidth)
		if entryIndex == m.schemaSearchCursor {
			rows = append(rows, focusStyle.Render(glyphSelected+" "+label))
			continue
		}
		rows = append(rows, mutedStyle.Render("  "+label))
	}
	searchHint := "↑↓ preview · Enter insert · Esc close"
	if visibleEnd-visibleStart < len(entries) {
		searchHint = fmt.Sprintf("%d-%d/%d · %s", visibleStart+1, visibleEnd, len(entries), searchHint)
	}
	rows = append(rows, mutedStyle.Render(searchHint))
	return panelStyle.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

func truncateSchemaSearchLabel(label string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	if maxWidth <= schemaSearchMinimumTruncateWidth && lipgloss.Width(label) > maxWidth {
		return schemaSearchTruncationMarker
	}
	return truncate(label, maxWidth)
}

func (m Model) renderEvidencePanel(width, height int) string {
	if height <= panelStyle.GetVerticalFrameSize() {
		return ""
	}
	if m.showsSchemaEvidence() {
		return m.renderSchemaEvidence(width, height)
	}
	return m.renderDataPreview(width, height)
}

// showsSchemaEvidence decides which evidence panel occupies the slot.
//
// Focusing the slot dismisses the live preview of an open schema search, but a schema the turn
// actually looked up stays until the next data tool, so it can be focused and scrolled.
func (m Model) showsSchemaEvidence() bool {
	if m.schemaSearchOpen() {
		return m.focus != focusExecution
	}
	return m.evidenceMode.showsSchema()
}

func (m Model) renderDataPreview(width, height int) string {
	panel := m.panelStyleFor(focusExecution)
	contentHeight := panelContentHeight(height)
	rows := []string{m.panelTitle(focusExecution, "Data Preview")}
	data, ok := m.currentPreviewData()
	if !ok {
		rows = append(rows, m.renderPreviewEmptyState(width)...)
		return panel.Width(width).Height(contentHeight).
			Render(lipgloss.JoinVertical(lipgloss.Left, fitRows(rows, contentHeight)...))
	}
	rows = append(rows, mutedStyle.Render(fmt.Sprintf("%s · %s · %d/%s rows",
		fallback(data.resource, "current query"), previewLabel(data.query), len(data.preview), formatCount(data.totalRows))))
	switch {
	case data.errorText != "":
		rows = append(rows, badStyle.Render(glyphFailed+" "+truncate(data.errorText, width-12)))
		if m.executionExportPath != "" {
			rows = append(rows, mutedStyle.Render("exported "+m.executionExportPath))
		}
	case len(data.preview) == 0:
		rows = append(rows,
			mutedStyle.Render("The query matched no rows in this time range."),
			mutedStyle.Render("Widen the time range in the candidate card, then Ctrl+E to run again."))
	default:
		rows = append(rows, m.renderPreviewBody(data, width, contentHeight-len(rows))...)
	}
	return panel.Width(width).Height(contentHeight).
		Render(lipgloss.JoinVertical(lipgloss.Left, fitRows(rows, contentHeight)...))
}

// renderPreviewEmptyState explains what will appear here and how to get it.
func (m Model) renderPreviewEmptyState(width int) []string {
	rows := []string{mutedStyle.Render("No results yet.")}
	if m.querySession != nil && m.querySession.Validation.Valid {
		return append(rows, mutedStyle.Render(wrapText("The candidate is valid. Press Ctrl+E to run it.", width-4)))
	}
	return append(rows, mutedStyle.Render(wrapText(
		"Ask the agent for data in the composer, or write a query in the candidate card and press Ctrl+E.", width-4)))
}

// renderPreviewBody renders the result table and, below it, the detail of the selected row.
func (m Model) renderPreviewBody(data previewData, width, availableHeight int) []string {
	tableLines := m.dataPreviewTableLines()
	visibleTableLines := previewTableViewport(tableLines, width-4, m.executionPreviewOffset)
	rows := append([]string(nil), visibleTableLines...)
	if data.truncated {
		rows = append(rows, mutedStyle.Render(glyphTruncate+" preview truncated; total row count shown above"))
	}
	detailLines := m.executionRowDetailLines(width - 4)
	if len(detailLines) == 0 {
		return rows
	}
	detailHeight := maxInt(availableHeight-len(rows)-1, 0)
	if detailHeight < minPreviewDetailHeight {
		return rows
	}
	rows = append(rows, titleStyle.Render("Row detail · pgup/pgdn scroll"))
	detailEnd := minInt(m.executionDetailScroll+detailHeight, len(detailLines))
	for lineIndex := m.executionDetailScroll; lineIndex < detailEnd; lineIndex++ {
		rows = append(rows, mutedStyle.Render(truncate(detailLines[lineIndex], width-4)))
	}
	if len(detailLines) > detailHeight {
		rows = append(rows, mutedStyle.Render(fmt.Sprintf("detail %d-%d/%d lines",
			m.executionDetailScroll+1, detailEnd, len(detailLines))))
	}
	return rows
}

// minPreviewDetailHeight is the smallest row-detail viewport worth rendering.
const minPreviewDetailHeight = 3

func (m Model) renderSchemaEvidence(width, height int) string {
	contentHeight := panelContentHeight(height)
	rows := []string{m.panelTitle(focusExecution, "Schema") + m.schemaEvidenceSourceLabel()}
	detailLines := schemaDetailLines(m.selectedSchema)
	if len(detailLines) == 0 {
		rows = append(rows,
			mutedStyle.Render("No resource selected."),
			mutedStyle.Render(wrapText("Type @ in the composer to search groups and resources.", width-4)))
		return m.panelStyleFor(focusExecution).Width(width).Height(contentHeight).
			Render(lipgloss.JoinVertical(lipgloss.Left, fitRows(rows, contentHeight)...))
	}
	footerRows := m.schemaEvidenceFooter(len(detailLines), contentHeight)
	viewportHeight := maxInt(contentHeight-len(rows)-len(footerRows), 1)
	scroll := clamp(m.schemaDetailScroll, 0, maxInt(len(detailLines)-viewportHeight, 0))
	endIndex := minInt(scroll+viewportHeight, len(detailLines))
	for lineIndex := scroll; lineIndex < endIndex; lineIndex++ {
		rows = append(rows, truncate(detailLines[lineIndex], width-4))
	}
	return m.panelStyleFor(focusExecution).Width(width).Height(contentHeight).
		Render(lipgloss.JoinVertical(lipgloss.Left, fitRows(append(rows, footerRows...), contentHeight)...))
}

// schemaEvidenceSourceLabel credits the catalog when the panel answers a schema lookup.
//
// Both this panel and Data Preview render columns, so the title says which one the user is reading.
func (m Model) schemaEvidenceSourceLabel() string {
	if m.querySession == nil || m.querySession.Phase != session.PhaseSchema {
		return ""
	}
	return mutedStyle.Render("  read from the catalog · no query run")
}

// schemaEvidenceFooter reports load state and, when the schema overflows, the visible line range.
func (m Model) schemaEvidenceFooter(lineCount, contentHeight int) []string {
	var footerRows []string
	if !m.selectedSchema.Loaded {
		if m.schemaSearchLoading() {
			footerRows = append(footerRows, warnStyle.Render(glyphRunning+" loading typed columns from BanyanDB"))
		} else {
			footerRows = append(footerRows, mutedStyle.Render("Typed columns are not available for this resource."))
		}
	}
	if lineCount > contentHeight-len(footerRows)-1 {
		footerRows = append(footerRows, mutedStyle.Render(fmt.Sprintf("%d lines · pgup/pgdn scroll", lineCount)))
	}
	return footerRows
}

func panelContentHeight(totalHeight int) int {
	return maxInt(totalHeight-panelStyle.GetVerticalFrameSize(), 1)
}

func (m Model) schemaSearchLoading() bool {
	if !m.schemaSearchOpen() || m.schemaLoads == nil {
		return false
	}
	entries := m.schemaSearchEntries()
	if m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return false
	}
	entry := entries[m.schemaSearchCursor]
	_, loading := m.schemaLoads[schemaEntryKey(entry)]
	return loading
}

func (m Model) currentPreviewData() (previewData, bool) {
	if m.querySession == nil {
		return previewData{}, false
	}
	executionResult := m.querySession.ExecutionResult
	if executionResult.Summary == "" && len(executionResult.Preview) == 0 && executionResult.Error == "" {
		return previewData{}, false
	}
	return previewData{
		columns:   executionResult.Columns,
		preview:   executionResult.Preview,
		resource:  m.querySession.ResourceName,
		query:     executionResult.Query,
		errorText: executionResult.Error,
		totalRows: executionResult.Rows,
		truncated: executionResult.Truncated || len(executionResult.Preview) < executionResult.Rows,
	}, true
}

func previewLabel(query string) string {
	if strings.TrimSpace(query) == "" {
		return "preview"
	}
	return "current query"
}

func formatCount(value int) string {
	if value < 1000 {
		return fmt.Sprintf("%d", value)
	}
	formatted := fmt.Sprintf("%d", value)
	firstGroup := len(formatted) % 3
	if firstGroup == 0 {
		firstGroup = 3
	}
	var builder strings.Builder
	builder.WriteString(formatted[:firstGroup])
	for index := firstGroup; index < len(formatted); index += 3 {
		builder.WriteByte(',')
		builder.WriteString(formatted[index : index+3])
	}
	return builder.String()
}

func (m *Model) updateSchemaSearch() {
	if m.message.Value() != m.schemaSearchValue {
		m.schemaSearchValue = m.message.Value()
		m.schemaSearchDismissed = false
	}
	if !m.schemaSearchOpen() {
		// Only a preview is retracted here. This runs on every composer message, cursor blinks
		// included, so clearing a pinned schema would drop a describe answer a blink after it lands.
		if m.evidenceMode == evidenceModeSchema {
			m.evidenceMode = evidenceModeData
		}
		m.schemaSearchCursor = 0
		return
	}
	entries := m.schemaSearchEntries()
	if len(entries) == 0 {
		m.schemaSearchCursor = 0
		return
	}
	if m.schemaSearchCursor >= len(entries) {
		m.schemaSearchCursor = len(entries) - 1
	}
	if m.schemaSearchCursor < 0 {
		m.schemaSearchCursor = 0
	}
	m.previewSchemaSearchEntry(entries[m.schemaSearchCursor])
}

func (m Model) schemaSearchOpen() bool {
	_, ok := m.schemaSearchTerm()
	return ok && !m.schemaSearchDismissed
}

func (m Model) schemaSearchTerm() (string, bool) {
	messageValue := m.message.Value()
	atIndex := strings.LastIndex(messageValue, "@")
	if atIndex < 0 {
		return "", false
	}
	term := messageValue[atIndex+1:]
	if strings.ContainsAny(term, " \t\n") {
		return "", false
	}
	return strings.ToLower(strings.TrimSpace(term)), true
}

func (m Model) schemaSearchEntries() []session.CatalogEntry {
	term, open := m.schemaSearchTerm()
	if !open {
		return nil
	}
	if normalizeSchemaSearchTerm(term) == "" && term != "" {
		return nil
	}
	results := make([]schemaSearchResult, 0, len(m.catalog.catalog.Entries))
	for _, entry := range m.catalog.catalog.Entries {
		score, matches := schemaSearchScore(entry, term)
		if !matches {
			continue
		}
		results = append(results, schemaSearchResult{entry: entry, score: score})
	}
	sort.Slice(results, func(leftIndex, rightIndex int) bool {
		leftResult := results[leftIndex]
		rightResult := results[rightIndex]
		if leftResult.score != rightResult.score {
			return leftResult.score < rightResult.score
		}
		leftEntry := leftResult.entry
		rightEntry := rightResult.entry
		if leftEntry.Group != rightEntry.Group {
			return leftEntry.Group < rightEntry.Group
		}
		if leftEntry.Name != rightEntry.Name {
			return leftEntry.Name < rightEntry.Name
		}
		return leftEntry.Type < rightEntry.Type
	})
	entries := make([]session.CatalogEntry, len(results))
	for index, result := range results {
		entries[index] = result.entry
	}
	return entries
}

func schemaSearchScore(entry session.CatalogEntry, term string) (int, bool) {
	groupTerm, resourceTerm, hasResourceTerm := strings.Cut(term, "/")
	if hasResourceTerm {
		return schemaSearchPathScore(entry, groupTerm, resourceTerm)
	}
	normalizedTerm := normalizeSchemaSearchTerm(term)
	if normalizedTerm == "" {
		return 0, true
	}
	if score, matches := schemaSearchFieldScore(entry.Name, normalizedTerm, schemaSearchNameScore); matches {
		return score, true
	}
	if score, matches := schemaSearchFieldScore(entry.Group, normalizedTerm, schemaSearchGroupScore); matches {
		return score, true
	}
	return schemaSearchFieldScore(entry.Type.String(), normalizedTerm, schemaSearchTypeScore)
}

func schemaSearchPathScore(entry session.CatalogEntry, groupTerm, resourceTerm string) (int, bool) {
	normalizedGroupTerm := normalizeSchemaSearchTerm(groupTerm)
	if normalizedGroupTerm == "" {
		return 0, false
	}
	groupScore, matchesGroup := schemaSearchFieldScore(entry.Group, normalizedGroupTerm, schemaSearchNameScore)
	if !matchesGroup {
		return 0, false
	}
	normalizedResourceTerm := normalizeSchemaSearchTerm(resourceTerm)
	if normalizedResourceTerm == "" {
		return groupScore, true
	}
	resourceScore, matchesResource := schemaSearchFieldScore(entry.Name, normalizedResourceTerm, schemaSearchNameScore)
	if !matchesResource {
		return 0, false
	}
	return groupScore + resourceScore, true
}

func schemaSearchFieldScore(value, normalizedTerm string, baseScore int) (int, bool) {
	normalizedValue := normalizeSchemaSearchTerm(value)
	if normalizedValue == normalizedTerm {
		return baseScore, true
	}
	tokens := schemaSearchTokens(value)
	bestScore := 0
	matches := false
	recordMatch := func(score int) {
		if !matches || score < bestScore {
			bestScore = score
			matches = true
		}
	}
	for tokenIndex, token := range tokens {
		switch {
		case token == normalizedTerm:
			recordMatch(baseScore + schemaSearchExactTokenScore)
		case strings.HasPrefix(token, normalizedTerm):
			recordMatch(baseScore + schemaSearchPrefixScore)
		case strings.Contains(token, normalizedTerm):
			recordMatch(baseScore + schemaSearchSubstringScore)
		}
		var sequence strings.Builder
		for sequenceEnd := tokenIndex; sequenceEnd < len(tokens); sequenceEnd++ {
			sequence.WriteString(tokens[sequenceEnd])
			if sequence.Len() > len(normalizedTerm) {
				break
			}
			if sequence.String() != normalizedTerm {
				continue
			}
			matchScore := baseScore + schemaSearchSubstringScore
			if tokenIndex == 0 {
				matchScore = baseScore + schemaSearchPrefixScore
			}
			recordMatch(matchScore)
			break
		}
	}
	return bestScore, matches
}

func normalizeSchemaSearchTerm(value string) string {
	return strings.Join(schemaSearchTokens(value), "")
}

func schemaSearchTokens(value string) []string {
	tokens := make([]string, 0, 1)
	var normalized strings.Builder
	appendToken := func() {
		if normalized.Len() == 0 {
			return
		}
		tokens = append(tokens, normalized.String())
		normalized.Reset()
	}
	for _, valueRune := range strings.ToLower(value) {
		if (valueRune >= 'a' && valueRune <= 'z') || (valueRune >= '0' && valueRune <= '9') {
			normalized.WriteRune(valueRune)
			continue
		}
		appendToken()
	}
	appendToken()
	return tokens
}

func (m *Model) moveSchemaSearchCursor(delta int) {
	entries := m.schemaSearchEntries()
	if len(entries) == 0 || delta == 0 {
		return
	}
	m.schemaSearchCursor += delta
	if m.schemaSearchCursor < 0 {
		m.schemaSearchCursor = 0
	}
	if m.schemaSearchCursor >= len(entries) {
		m.schemaSearchCursor = len(entries) - 1
	}
	m.previewSchemaSearchEntry(entries[m.schemaSearchCursor])
}

func (m *Model) previewSchemaSearchEntry(entry session.CatalogEntry) {
	// A preview replaces a pinned schema, since the user is now steering the panel by hand.
	m.evidenceMode = evidenceModeSchema
	if sameSchemaResource(m.selectedSchema, entry) {
		return
	}
	if cachedSchema, ok := m.cachedSchema(entry); ok {
		m.selectedSchema = cachedSchema
		return
	}
	if m.querySession != nil {
		if cachedSchema, ok := m.querySession.CachedSchema(entry.Type, entry.Name, []string{entry.Group}); ok {
			m.selectedSchema = cachedSchema
			return
		}
	}
	m.selectedSchema = session.SchemaSnapshot{
		Type:   entry.Type,
		Name:   entry.Name,
		Groups: []string{entry.Group},
	}
}

func (m Model) cachedSchema(entry session.CatalogEntry) (session.SchemaSnapshot, bool) {
	if m.schemaCache == nil {
		return session.SchemaSnapshot{}, false
	}
	snapshot, ok := m.schemaCache[schemaEntryKey(entry)]
	return snapshot, ok
}

func (m *Model) cacheSchema(snapshot session.SchemaSnapshot) {
	if strings.TrimSpace(snapshot.Name) == "" {
		return
	}
	if m.schemaCache == nil {
		m.schemaCache = make(map[string]session.SchemaSnapshot)
	}
	key := session.SchemaKey(snapshot.Type, snapshot.Name, snapshot.Groups)
	m.schemaCache[key] = snapshot
}

func (m *Model) clearSchemaLoad(entry session.CatalogEntry) {
	if m.schemaLoads == nil {
		return
	}
	delete(m.schemaLoads, schemaEntryKey(entry))
}

func (m Model) isCurrentSchemaSearchEntry(entry session.CatalogEntry) bool {
	entries := m.schemaSearchEntries()
	if m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return false
	}
	return entries[m.schemaSearchCursor] == entry
}

func (m *Model) loadSchemaDetailForSearch() tea.Cmd {
	if !m.schemaSearchOpen() {
		return nil
	}
	entries := m.schemaSearchEntries()
	if m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return nil
	}
	entry := entries[m.schemaSearchCursor]
	if _, ok := m.cachedSchema(entry); ok {
		return nil
	}
	key := schemaEntryKey(entry)
	if m.schemaLoads == nil {
		m.schemaLoads = make(map[string]struct{})
	}
	if _, loading := m.schemaLoads[key]; loading {
		return nil
	}
	m.schemaLoads[key] = struct{}{}
	return m.loadSchemaDetailCmd(entry)
}

func sameSchemaResource(snapshot session.SchemaSnapshot, entry session.CatalogEntry) bool {
	return snapshot.Type == entry.Type && snapshot.Name == entry.Name && len(snapshot.Groups) == 1 && snapshot.Groups[0] == entry.Group
}

func schemaEntryKey(entry session.CatalogEntry) string {
	return session.SchemaKey(entry.Type, entry.Name, []string{entry.Group})
}

func (m *Model) insertSchemaReference() {
	entries := m.schemaSearchEntries()
	if len(entries) == 0 || m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return
	}
	entry := entries[m.schemaSearchCursor]
	term, open := m.schemaSearchTerm()
	if !open {
		return
	}
	messageValue := m.message.Value()
	referenceStart := strings.LastIndex(messageValue, "@")
	if referenceStart < 0 {
		return
	}
	chip := "@" + entry.Group + "/" + entry.Name
	m.message.SetValue(messageValue[:referenceStart] + chip + messageValue[referenceStart+len(term)+1:])
	m.schemaSearchValue = m.message.Value()
	m.schemaSearchDismissed = true
	m.composerReference = &session.CatalogEntry{Group: entry.Group, Type: entry.Type, Name: entry.Name}
	m.previewSchemaSearchEntry(entry)
}
