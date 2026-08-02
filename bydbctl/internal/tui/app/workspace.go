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

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
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

	minWorkspaceChatHeight   = 4
	minWorkspaceEditorHeight = 1
)

type evidenceMode int

const (
	evidenceModeData evidenceMode = iota
	evidenceModeSchema
)

type previewData struct {
	columns   []string
	preview   [][]string
	resource  string
	query     string
	errorText string
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

func workspaceWidths(width int) (int, int) {
	if width < 100 {
		return width, width
	}
	leftWidth := clamp(width*52/100, 52, 104)
	return leftWidth, width - leftWidth - 2
}

func (m Model) focusOrder() []int {
	return []int{focusChat, focusMessage, focusStart, focusEnd, focusLimit, focusQuery, focusExecution, focusActivity}
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

func (m Model) renderWorkspaceHeader(width int) string {
	phase := m.currentPhaseLabel()
	return lipgloss.NewStyle().Width(width).Render(lipgloss.JoinHorizontal(
		lipgloss.Top,
		titleStyle.Render("bydbctl · text2bydbQL"),
		"  ",
		chipStyle.Render("provider "+m.provider),
		"  ",
		mutedStyle.Render(phase),
		"  ",
		mutedStyle.Render("@ local schema search"),
	))
}

func (m Model) renderWorkspace(width, height int) string {
	leftWidth, rightWidth := workspaceWidths(width)
	left := m.renderWorkspaceLeft(leftWidth, height)
	right := m.renderEvidencePanel(rightWidth, height)
	if width < 100 {
		return lipgloss.JoinVertical(lipgloss.Left, left, right)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, left, "  ", right)
}

func (m Model) renderWorkspaceLeft(width, height int) string {
	schemaSearchResultLimit := schemaSearchViewportLimit(height, len(m.schemaSearchEntries()))
	layout := workspaceLeftLayout{
		chatHeight:        clamp(height-24, minWorkspaceChatHeight, 16),
		queryHeight:       m.query.Height(),
		messageHeight:     m.message.Height(),
		schemaResultLimit: schemaSearchResultLimit,
	}
	for {
		left := m.renderWorkspaceLeftWithLayout(width, layout)
		heightOverflow := lipgloss.Height(left) - height
		if heightOverflow <= 0 {
			layout.chatHeight -= heightOverflow
			return m.renderWorkspaceLeftWithLayout(width, layout)
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
		return left
	}
}

func schemaSearchViewportLimit(height, resultCount int) int {
	if resultCount == 0 {
		return 0
	}
	return minInt(resultCount, clamp(height/schemaSearchRowsPerViewport, 1, maxSchemaSearchVisibleResults))
}

func (m Model) renderWorkspaceLeftWithLayout(width int, layout workspaceLeftLayout) string {
	m.query.SetHeight(layout.queryHeight)
	m.message.SetHeight(layout.messageHeight)
	return lipgloss.JoinVertical(lipgloss.Left,
		m.renderChat(width, layout.chatHeight),
		m.renderCandidateCard(width),
		m.renderSchemaSearch(width, layout.schemaResultLimit),
		m.renderMessage(width),
		m.renderStatusLine(width),
	)
}

func (m Model) renderCandidateCard(width int) string {
	report := session.ValidationReport{Message: "not checked"}
	versionLabel := "v0"
	if m.querySession != nil {
		report = m.querySession.Validation
		if selectedCandidate := m.querySession.SelectedCandidateIndex(); selectedCandidate >= 0 {
			versionLabel = fmt.Sprintf("v%d", selectedCandidate+1)
			if currentCandidate := m.querySession.CurrentCandidate(); currentCandidate != nil && currentCandidate.Source == session.CandidateSourceManual {
				versionLabel += " (edited)"
			}
		}
	}
	status := report.Status()
	statusStyle := badStyle
	if report.Valid {
		statusStyle = okStyle
	} else if status == "not checked" {
		statusStyle = mutedStyle
	}
	rows := []string{
		titleStyle.Render("Candidate QL " + versionLabel + " " + statusStyle.Render(status)),
		m.query.View(),
		lipgloss.JoinHorizontal(lipgloss.Top,
			mutedStyle.Render("Time "),
			m.start.View(),
			mutedStyle.Render(" → "),
			m.end.View(),
		),
		lipgloss.JoinHorizontal(lipgloss.Top, mutedStyle.Render("Limit "), m.limit.View()),
		mutedStyle.Render(fmt.Sprintf(
			"edit inline · validation pauses %s · Ctrl+E run · Ctrl+←/→ history",
			queryValidationDebounce,
		)),
	}
	if !report.Valid && report.Message != "" && report.Message != "not checked" {
		rows = append(rows, badStyle.Render("Validation: "+truncate(report.Message, width-16)))
	}
	return panelStyle.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
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
	visibleStart := 0
	if resultLimit < len(entries) && m.schemaSearchCursor >= resultLimit {
		visibleStart = m.schemaSearchCursor - resultLimit + 1
	}
	visibleEnd := minInt(visibleStart+maxInt(resultLimit, 1), len(entries))
	for entryIndex := visibleStart; entryIndex < visibleEnd; entryIndex++ {
		entry := entries[entryIndex]
		label := fmt.Sprintf("%s/%s · %s", entry.Group, entry.Name, shortTypeLabel(entry.Type))
		if entryIndex == m.schemaSearchCursor {
			rows = append(rows, activeChipStyle.Render(truncate(label, width-4)))
			continue
		}
		rows = append(rows, mutedStyle.Render(truncate(label, width-4)))
	}
	searchHint := "↑↓ preview schema · Enter insert resource"
	if visibleEnd-visibleStart < len(entries) {
		searchHint = fmt.Sprintf("results %d-%d/%d · %s", visibleStart+1, visibleEnd, len(entries), searchHint)
	}
	rows = append(rows, mutedStyle.Render(searchHint))
	return panelStyle.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

func (m Model) renderEvidencePanel(width, height int) string {
	approvalPanel := ""
	evidenceHeight := height
	if m.pendingApproval != nil {
		approvalPanel = m.renderApproval(width)
		evidenceHeight -= lipgloss.Height(approvalPanel)
	}
	var evidence string
	if evidenceHeight > panelStyle.GetVerticalFrameSize() {
		if m.schemaSearchOpen() || m.evidenceMode == evidenceModeSchema {
			evidence = m.renderSchemaEvidence(width, evidenceHeight)
		} else {
			evidence = m.renderDataPreview(width, evidenceHeight)
		}
	}
	if approvalPanel == "" {
		return evidence
	}
	if evidence == "" {
		return approvalPanel
	}
	return lipgloss.JoinVertical(lipgloss.Left, evidence, approvalPanel)
}

func (m Model) renderDataPreview(width, height int) string {
	data, ok := m.currentPreviewData()
	rows := []string{titleStyle.Render("Data Preview")}
	if !ok {
		rows = append(rows, mutedStyle.Render("A validated query probe or execution result will appear here."))
		return panelStyle.Width(width).Height(panelContentHeight(height)).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
	}
	resource := fallback(data.resource, "current query")
	previewCount := len(data.preview)
	rows = append(rows, fmt.Sprintf("%s · %s · %d/%s rows", resource, previewLabel(data.query), previewCount, formatCount(data.totalRows)))
	if data.errorText != "" {
		rows = append(rows, badStyle.Render("Probe: "+truncate(data.errorText, width-12)))
	} else if previewCount == 0 {
		rows = append(rows, mutedStyle.Render("The query returned no preview rows."))
	} else {
		displayColumns := selectDisplayColumns(data.columns)
		projectedRows := projectPreviewRows(data.preview, data.columns, displayColumns)
		rows = append(rows, formatPreviewTable(displayColumns, projectedRows, width-4, m.executionRowCursor)...)
	}
	if data.truncated {
		rows = append(rows, mutedStyle.Render("… preview is truncated; total row count shown above"))
	}
	if m.showExecutionRaw && m.querySession != nil && strings.TrimSpace(m.querySession.ExecutionResult.Response) != "" {
		rows = append(rows, titleStyle.Render("Full response"))
		rows = append(rows, formatJSONResponsePreview(m.querySession.ExecutionResult.Response, width-4, maxExecutionResponseLines)...)
	}
	rows = append(rows, mutedStyle.Render("Ctrl+O export · Ctrl+J see full response · ↑↓ row"))
	return panelStyle.Width(width).Height(panelContentHeight(height)).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

func (m Model) renderSchemaEvidence(width, height int) string {
	rows := []string{titleStyle.Render("Schema")}
	detailLines := schemaDetailLines(m.selectedSchema)
	if len(detailLines) == 0 {
		rows = append(rows, mutedStyle.Render("Use @ in the composer to search groups and resources."))
		return panelStyle.Width(width).Height(panelContentHeight(height)).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
	}
	viewportHeight := maxInt(height-4, 6)
	endIndex := minInt(m.detailScroll+viewportHeight, len(detailLines))
	for lineIndex := m.detailScroll; lineIndex < endIndex; lineIndex++ {
		rows = append(rows, truncate(detailLines[lineIndex], width-4))
	}
	if !m.selectedSchema.Loaded {
		if m.schemaSearchLoading() {
			rows = append(rows, mutedStyle.Render("Loading typed columns from BanyanDB…"))
		} else {
			rows = append(rows, mutedStyle.Render("Typed columns are not available from BanyanDB for this resource."))
		}
	}
	return panelStyle.Width(width).Height(panelContentHeight(height)).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
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
	if executionResult.Summary != "" || len(executionResult.Preview) > 0 {
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
	currentCandidate := m.querySession.CurrentCandidate()
	if currentCandidate == nil || currentCandidate.Probe == nil {
		return previewData{}, false
	}
	probe := currentCandidate.Probe
	return previewData{
		columns:   probe.Columns,
		preview:   probe.Preview,
		resource:  m.querySession.ResourceName,
		query:     probe.Query,
		errorText: probe.Error,
		totalRows: probe.Rows,
		truncated: len(probe.Preview) < probe.Rows,
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
		m.evidenceMode = evidenceModeData
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
