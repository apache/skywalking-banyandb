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
	"context"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

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
	m.schemaDetailScroll = clamp(m.schemaDetailScroll+delta, 0, max(lineCount-1, 0))
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
