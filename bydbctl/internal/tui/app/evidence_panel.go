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
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// One slot on the right shows either a schema or the latest result rows. Which one it holds is the
// evidence mode; these renderers only draw whatever the mode already decided.

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
	detailLines := m.executionRowDetailLines(width - 4)
	layout := previewBodyLayoutFor(len(tableLines), len(detailLines), availableHeight, data.truncated)
	verticalTableLines, firstRow, lastRow := previewTableVerticalViewport(tableLines, layout.tableHeight, m.executionRowCursor)
	visibleTableLines := previewTableViewport(verticalTableLines, width-4, m.executionPreviewOffset)
	rows := append([]string(nil), visibleTableLines...)
	if layout.tableOverflows {
		rows = append(rows, mutedStyle.Render(fmt.Sprintf("rows %d-%d/%d · ↑↓ row", firstRow+1, lastRow, len(data.preview))))
	}
	if data.truncated {
		rows = append(rows, mutedStyle.Render(glyphTruncate+" preview truncated; total row count shown above"))
	}
	if layout.detailViewportHeight == 0 {
		return rows
	}
	rows = append(rows, titleStyle.Render("Row detail · pgup/pgdn scroll"))
	detailScroll := clamp(m.executionDetailScroll, 0, max(len(detailLines)-layout.detailViewportHeight, 0))
	detailEnd := min(detailScroll+layout.detailViewportHeight, len(detailLines))
	for lineIndex := detailScroll; lineIndex < detailEnd; lineIndex++ {
		rows = append(rows, mutedStyle.Render(truncate(detailLines[lineIndex], width-4)))
	}
	if layout.detailOverflows {
		rows = append(rows, mutedStyle.Render(fmt.Sprintf("detail %d-%d/%d lines",
			detailScroll+1, detailEnd, len(detailLines))))
	}
	return rows
}

// minPreviewDetailHeight is the smallest row-detail viewport worth rendering.
const minPreviewDetailHeight = 3

type previewBodyLayout struct {
	tableHeight          int
	tableOverflows       bool
	detailViewportHeight int
	detailOverflows      bool
}

// previewBodyLayoutFor divides the available panel rows between table rows, their footers, and selected-row detail.
func previewBodyLayoutFor(tableLineCount, detailLineCount, availableHeight int, truncated bool) previewBodyLayout {
	footerHeight := 0
	if truncated {
		footerHeight++
	}
	tableHeight := max(availableHeight-footerHeight, previewTableHeaderLines+1)
	tableOverflows := tableLineCount > tableHeight
	if tableOverflows {
		footerHeight++
		tableHeight = max(availableHeight-footerHeight, previewTableHeaderLines+1)
	}
	layout := previewBodyLayout{tableHeight: tableHeight, tableOverflows: tableOverflows}
	if detailLineCount == 0 {
		return layout
	}
	renderedTableHeight := min(tableLineCount, tableHeight)
	detailViewportHeight := availableHeight - renderedTableHeight - footerHeight - 1
	if detailViewportHeight < minPreviewDetailHeight {
		return layout
	}
	if detailLineCount > detailViewportHeight {
		detailViewportHeight--
		if detailViewportHeight < minPreviewDetailHeight {
			return layout
		}
		layout.detailOverflows = true
	}
	layout.detailViewportHeight = detailViewportHeight
	return layout
}

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
	viewportHeight := max(contentHeight-len(rows)-len(footerRows), 1)
	scroll := clamp(m.schemaDetailScroll, 0, max(len(detailLines)-viewportHeight, 0))
	endIndex := min(scroll+viewportHeight, len(detailLines))
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
	return max(totalHeight-panelStyle.GetVerticalFrameSize(), 1)
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
