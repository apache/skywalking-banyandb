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
	return lipgloss.NewStyle().Width(width).Render(truncate(header, max(width, 8)))
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
	editorWidth := max(width-panelStyle.GetHorizontalFrameSize()-2, minEditorWidth)
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
			layout.queryHeight -= min(heightOverflow, layout.queryHeight-minWorkspaceEditorHeight)
			continue
		}
		if layout.chatHeight > minWorkspaceChatHeight {
			layout.chatHeight -= min(heightOverflow, layout.chatHeight-minWorkspaceChatHeight)
			continue
		}
		if layout.messageHeight > minWorkspaceEditorHeight {
			layout.messageHeight -= min(heightOverflow, layout.messageHeight-minWorkspaceEditorHeight)
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
			chatHeight:    max(chatHeight, minWorkspaceChatHeight),
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
				chatHeight:    max(chatHeight, minWorkspaceChatHeight),
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
	editorWidth := max(m.query.Width(), 1)
	lineCount := 1
	for _, queryLine := range strings.Split(m.query.Value(), "\n") {
		lineCount += max((lipgloss.Width(queryLine)-1)/editorWidth, 0)
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
	return min(resultCount, clamp(height/schemaSearchRowsPerViewport, 1, maxSchemaSearchVisibleResults))
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
	return panelStyle.Width(width).Render(truncate(row, max(width-panelStyle.GetHorizontalFrameSize(), 8)))
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
	labelWidth := max(width-panelStyle.GetHorizontalFrameSize()-selectionPrefixWidth, minSchemaSearchLabelWidth)
	visibleStart := 0
	if resultLimit < len(entries) && m.schemaSearchCursor >= resultLimit {
		visibleStart = m.schemaSearchCursor - resultLimit + 1
	}
	visibleEnd := min(visibleStart+max(resultLimit, 1), len(entries))
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
