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

package app

import (
	"fmt"
	"strings"
)

// Each panel owns a cursor and a scroll offset that are clamped against content the view derives on
// every frame, so a result shrinking under the cursor can never leave it pointing past the end.

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
	return max(panelHeight-chatPanelChrome-detailBudget, 3)
}

func (m *Model) scrollExecutionDetail(delta int) bool {
	data, ok := m.currentPreviewData()
	if !ok {
		m.executionDetailScroll = 0
		return false
	}
	width, availableHeight := m.executionPreviewBodyDimensions()
	detailLines := m.executionRowDetailLines(width - 4)
	layout := previewBodyLayoutFor(len(m.dataPreviewTableLines()), len(detailLines), availableHeight, data.truncated)
	maxScroll := max(len(detailLines)-layout.detailViewportHeight, 0)
	if maxScroll == 0 {
		m.executionDetailScroll = 0
		return false
	}
	m.executionDetailScroll = clamp(m.executionDetailScroll+delta, 0, maxScroll)
	return true
}

// executionPreviewBodyDimensions returns the exact space available to the data table and row detail.
func (m Model) executionPreviewBodyDimensions() (int, int) {
	_, _, contentWidth, bodyHeight := m.workspaceFrame()
	if workspaceIsStacked(contentWidth) {
		return contentWidth, max(panelContentHeight(bodyHeight-1)-2, 0)
	}
	_, previewWidth := workspaceWidths(contentWidth)
	return previewWidth, max(panelContentHeight(bodyHeight)-2, 0)
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
	return max(previewWidth-4, 1)
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
		lines = append(lines, wrapRunes(column+": "+value, max(width-2, 24))...)
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
	detailLines := entries[m.chatCursor].detailLines(max(m.width/2, 40))
	if len(detailLines) == 0 {
		m.chatDetailScroll = 0
		return
	}
	m.chatDetailScroll += delta
	maxScroll := max(len(detailLines)-viewportHeight, 0)
	if m.chatDetailScroll < 0 {
		m.chatDetailScroll = 0
	}
	if m.chatDetailScroll > maxScroll {
		m.chatDetailScroll = maxScroll
	}
}
