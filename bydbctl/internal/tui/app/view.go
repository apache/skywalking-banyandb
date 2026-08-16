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

	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

func (m Model) currentPhaseLabel() string {
	if m.querySession == nil || m.querySession.Phase == "" {
		return "intent"
	}
	return m.querySession.Phase.String()
}

// renderStatusLine reports run state, validation, and focus in unframed rows.
//
// The status bar carries no border of its own so it reads as chrome instead of as one more panel.
func (m Model) renderStatusLine(width int) string {
	rows := []string{m.renderStatusSummary(width)}
	switch {
	case m.quitConfirmPending:
		rows = append(rows, warnStyle.Render(glyphWarn+" Quit bydbctl agent? ")+
			keyStyle.Render("y")+mutedStyle.Render(" quits · any other key keeps working"))
	case m.busy:
		rows = append(rows, badStyle.Render(glyphStop+" Stop")+mutedStyle.Render(" · ")+
			keyStyle.Render("Esc")+mutedStyle.Render(" stops this run"))
	}
	return lipgloss.NewStyle().Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

// renderStatusSummary lays out the persistent state fields, pairing each state with a glyph.
func (m Model) renderStatusSummary(width int) string {
	summaryStyle := mutedStyle
	stateGlyph := glyphActive
	switch {
	case m.busy:
		summaryStyle = warnStyle
		stateGlyph = glyphRunning
	case m.catalog.loadError != "":
		summaryStyle = badStyle
		stateGlyph = glyphFailed
	}
	summary := fmt.Sprintf("%s Status: %s%s · Focus: %s · %s",
		stateGlyph, m.status, m.validationField(), m.focusLabel(), m.currentPhaseLabel())
	return summaryStyle.Render(truncate(summary, maxInt(width, 8)))
}

// validationField reports candidate validation, and nothing at all when there is no candidate.
//
// A turn that answered in words has nothing to validate, so the field would only add noise.
func (m Model) validationField() string {
	if m.querySession == nil || len(m.querySession.Candidates) == 0 {
		if strings.TrimSpace(m.query.Value()) == "" {
			return ""
		}
	}
	validation := statusNotChecked
	if m.querySession != nil && m.querySession.Validation.Message != "" {
		validation = m.querySession.Validation.Status()
	}
	return " · Validation: " + validation
}

// focusLabel names the panel that currently receives input.
func (m Model) focusLabel() string {
	switch m.focus {
	case focusChat:
		return "conversation"
	case focusMessage:
		return "composer"
	case focusStart:
		return "time start"
	case focusEnd:
		return "time end"
	case focusLimit:
		return "limit"
	case focusQuery:
		return "candidate QL"
	case focusExecution:
		// The slot holds either panel, so naming the wrong one would misreport where input goes.
		if m.showsSchemaEvidence() {
			return "schema"
		}
		return "data preview"
	default:
		return "composer"
	}
}

// panelStyleFor highlights the border of the panel that owns the current focus.
func (m Model) panelStyleFor(panelFocus int) lipgloss.Style {
	if m.focus == panelFocus {
		return activePanelStyle
	}
	return panelStyle
}

// panelTitle marks the focused panel with weight and a glyph rather than a bordered chip.
//
// A bordered marker would add rows to the panel and shift the layout every time focus moved.
func (m Model) panelTitle(panelFocus int, title string) string {
	if m.focus == panelFocus {
		return focusStyle.Render(glyphSelected + " " + title)
	}
	return titleStyle.Render("  " + title)
}

func (m Model) renderMessage(width int) string {
	return m.panelStyleFor(focusMessage).Width(width).Render(lipgloss.JoinVertical(
		lipgloss.Left,
		m.panelTitle(focusMessage, "Message"),
		m.message.View(),
	))
}

// renderChat renders the conversation, dropping its own chrome first when the panel is short.
//
// A short panel must spend its rows on messages, not on the activity and progress lines.
func (m Model) renderChat(width, panelHeight int) string {
	contentHeight := panelContentHeight(panelHeight)
	rows := []string{m.panelTitle(focusChat, "Conversation")}
	if contentHeight >= chatChromeBudget {
		if m.busy {
			rows = append(rows, warnStyle.Render(glyphRunning+" "+m.status))
		} else if len(m.activityLog) > 0 {
			lastActivity := m.activityLog[len(m.activityLog)-1]
			rows = append(rows, mutedStyle.Render(truncate(lastActivity.title, maxInt(width-6, 8))))
		}
		if progress := m.renderTurnProgress(); progress != "" {
			rows = append(rows, progress)
		}
	}
	entries := chatEntries(m.querySession, m.liveResponse, m.queuedMessage)
	if len(entries) == 0 {
		rows = append(rows, m.renderChatEmptyState(width)...)
	} else {
		rows = append(rows, m.renderChatEntries(entries, width, panelHeight, contentHeight-len(rows))...)
	}
	return m.panelStyleFor(focusChat).Width(width).Height(contentHeight).
		Render(lipgloss.JoinVertical(lipgloss.Left, fitRows(rows, contentHeight)...))
}

const (
	// chatChromeBudget is the content height below which the conversation panel hides its status rows.
	chatChromeBudget = 8
	// chatDetailChrome counts the detail title and its line-range footer.
	chatDetailChrome = 2
)

// renderChatEmptyState guides a fresh session instead of leaving the panel blank.
func (m Model) renderChatEmptyState(width int) []string {
	if guidance := m.coldStartGuidance(width - 4); len(guidance) > 0 {
		return guidance
	}
	return []string{mutedStyle.Render("Start a conversation. Your sent message appears here immediately.")}
}

// renderChatEntries renders the message list and, when one is selected, its detail body.
//
// availableRows is what is left of the panel after its chrome, so the two viewports share it.
func (m Model) renderChatEntries(entries []chatEntryView, width, panelHeight, availableRows int) []string {
	detailViewportHeight := 0
	detailLines := []string(nil)
	if m.chatCursor >= 0 && m.chatCursor < len(entries) {
		if selected := entries[m.chatCursor]; strings.TrimSpace(selected.detail) != "" {
			detailViewportHeight = minInt(chatDetailViewportHeight(panelHeight), maxInt(availableRows/2, 0))
			if detailViewportHeight > 0 {
				detailLines = selected.detailLines(width - 4)
			}
		}
	}
	listViewportHeight := maxInt(availableRows-detailViewportHeight-chatDetailChrome-1, 1)
	if detailViewportHeight == 0 {
		listViewportHeight = maxInt(availableRows-1, 1)
	}
	startIdx, endIdx := chatListWindow(m.chatScroll, m.chatCursor, listViewportHeight, len(entries))
	rows := make([]string, 0, listViewportHeight+detailViewportHeight+3)
	for entryIdx := startIdx; entryIdx < endIdx; entryIdx++ {
		rows = append(rows, m.renderChatEntryLine(entries[entryIdx], entryIdx, width))
	}
	if len(detailLines) > 0 {
		rows = append(rows, m.renderChatDetail(detailLines, detailViewportHeight, width)...)
	}
	if startIdx > 0 {
		rows = append(rows, mutedStyle.Render(fmt.Sprintf("%d-%d/%d messages", startIdx+1, endIdx, len(entries))))
	} else {
		rows = append(rows, mutedStyle.Render(fmt.Sprintf("%d/%d messages", endIdx, len(entries))))
	}
	return rows
}

// chatListWindow picks the visible slice of the message list, keeping the cursor inside it.
//
// The render viewport is smaller than the one the scroll keys assume, so the window is clamped here
// rather than trusting the stored scroll offset to already contain the cursor.
func chatListWindow(scroll, cursor, viewportHeight, entryCount int) (int, int) {
	if viewportHeight >= entryCount {
		return 0, entryCount
	}
	start := clamp(scroll, 0, maxInt(entryCount-viewportHeight, 0))
	if cursor >= 0 && cursor < entryCount {
		if cursor < start {
			start = cursor
		}
		if cursor >= start+viewportHeight {
			start = cursor - viewportHeight + 1
		}
	}
	return start, minInt(start+viewportHeight, entryCount)
}

// renderChatEntryLine styles one message row by role, marking the cursor with a fixed-width prefix.
func (m Model) renderChatEntryLine(entry chatEntryView, entryIdx, width int) string {
	suffix := ""
	if kindLabel := chatKindLabel(entry.kind); kindLabel != "" {
		suffix = mutedStyle.Render("  · " + kindLabel)
	}
	headlineWidth := maxInt(width-12-lipgloss.Width(suffix), 8)
	if entryIdx == m.chatCursor {
		return focusStyle.Render(glyphSelected+" "+truncate(entry.headline, headlineWidth)) + suffix
	}
	lineStyle := chatEntryStyle(entry)
	return lineStyle.Render("  "+truncate(entry.headline, headlineWidth)) + suffix
}

// chatEntryStyle colors one message row by what produced it.
//
// A schema lookup takes the schema panel's own color rather than the agent's, so the two kinds of
// column listing are told apart at a glance instead of only by their suffix.
func chatEntryStyle(entry chatEntryView) lipgloss.Style {
	if entry.kind == session.ChatMessageKindSchema {
		return titleStyle
	}
	switch entry.role {
	case session.ChatRoleUser:
		return titleStyle
	case session.ChatRoleTool:
		return warnStyle
	case session.ChatRoleAssistant:
		return okStyle
	case session.ChatRoleSystem:
		return mutedStyle
	default:
		return mutedStyle
	}
}

// renderChatDetail renders the scrollable body of the selected message.
func (m Model) renderChatDetail(detailLines []string, viewportHeight, width int) []string {
	rows := []string{titleStyle.Render("Detail · pgup/pgdn scroll")}
	detailEnd := minInt(m.chatDetailScroll+viewportHeight, len(detailLines))
	for lineIdx := m.chatDetailScroll; lineIdx < detailEnd; lineIdx++ {
		line := renderChatDetailLine(detailLines[lineIdx])
		if lipgloss.Width(line) > width-4 {
			line = truncate(line, width-4)
		}
		rows = append(rows, line)
	}
	if len(detailLines) > viewportHeight {
		rows = append(rows, mutedStyle.Render(fmt.Sprintf(
			"detail %d-%d/%d lines", m.chatDetailScroll+1, detailEnd, len(detailLines))))
	}
	return rows
}

type chatEntryView struct {
	role     session.ChatRole
	kind     session.ChatMessageKind
	headline string
	detail   string
	// exactDetail marks a body bydbctl composed from BanyanDB data rather than from agent prose.
	exactDetail bool
}

// detailLines renders the body of one message, repairing agent prose but never exact schema text.
func (entry chatEntryView) detailLines(width int) []string {
	if entry.exactDetail {
		return formatExactDetailLines(entry.detail, width)
	}
	return formatChatDetailLines(entry.detail, width)
}

func chatEntryCount(querySession *session.QuerySession, liveResponse, queuedMessage string) int {
	return len(chatEntries(querySession, liveResponse, queuedMessage))
}

func chatEntries(querySession *session.QuerySession, liveResponse, queuedMessage string) []chatEntryView {
	chatMessageCount := 0
	if querySession != nil {
		chatMessageCount = len(querySession.ChatMessages)
	}
	entries := make([]chatEntryView, 0, chatMessageCount+2)
	if querySession != nil {
		for _, message := range querySession.ChatMessages {
			entries = append(entries, chatEntryFromMessage(message))
		}
	}
	if queued := strings.TrimSpace(queuedMessage); queued != "" {
		entries = append(entries, chatEntryView{
			role:     session.ChatRoleUser,
			headline: "You › " + queued,
		})
	}
	if strings.TrimSpace(liveResponse) != "" {
		entries = append(entries, chatEntryView{
			role:     session.ChatRoleAssistant,
			headline: "live output: " + truncateRunes(singleLine(liveResponse), 96),
			detail:   strings.TrimSpace(liveResponse),
		})
	}
	return entries
}

// chatEntryFromMessage splits one chat message into its one-line headline and its full detail body.
//
// The detail keeps its original line breaks: the detail renderer normalizes line by line, so
// flattening here would destroy the headings and lists before it could format them.
func chatEntryFromMessage(message session.ChatMessage) chatEntryView {
	content := strings.TrimSpace(message.Content)
	roleLabel := chatMessageLabel(message)
	headline := roleLabel + singleLine(workflow.NormalizeAgentDisplayText(content))
	detail := content
	if structuredDetail := strings.TrimSpace(message.Detail); structuredDetail != "" {
		detail = structuredDetail
	}
	if message.Kind == session.ChatMessageKindSchema {
		// A described schema is already exact text from BanyanDB, so the fragmented-output repair
		// that agent prose needs would only corrupt its column names.
		return chatEntryView{
			role:        message.Role,
			kind:        message.Kind,
			headline:    roleLabel + singleLine(content),
			detail:      detail,
			exactDetail: true,
		}
	}
	if message.ToolName != "" {
		headline = roleLabel + message.ToolName + ": " + singleLine(content)
	}
	if strings.TrimSpace(message.Candidate) != "" {
		status := "unchecked"
		if message.Validation != nil {
			status = message.Validation.Status()
		}
		candidate := strings.TrimSpace(message.Candidate)
		candidateLine := chatRoleLabel(message.Role) + "candidate [" + status + "]: " + singleLine(candidate)
		compactDetail := strings.ReplaceAll(strings.ReplaceAll(detail, " ", ""), "\n", "")
		compactCandidate := strings.ReplaceAll(candidate, " ", "")
		if detail == "" {
			headline = candidateLine
			detail = candidate
		} else if !strings.Contains(compactDetail, compactCandidate) {
			detail = appendCandidateDetail(detail, candidate, status)
		}
		if message.Role == session.ChatRoleAssistant && strings.TrimSpace(content) == "" {
			headline = candidateLine
		}
	}
	if headline == chatRoleLabel(message.Role) {
		headline = chatRoleLabel(message.Role) + "(empty)"
		detail = ""
	}
	return chatEntryView{role: message.Role, kind: message.Kind, headline: headline, detail: detail}
}

// chatKindLabel names what an assistant message expects next, for turns that produced no query.
//
// Without it an answer and a question look alike, leaving the user unsure whether to reply or wait.
func chatKindLabel(kind session.ChatMessageKind) string {
	switch kind {
	case session.ChatMessageKindClarification:
		return "needs your reply"
	case session.ChatMessageKindAnswer:
		return "answered, no query"
	case session.ChatMessageKindSchema:
		return schemaLookupLabel
	default:
		return ""
	}
}

// schemaLookupLabel marks a message read straight from the schema catalog rather than generated.
//
// A described schema and an executed query both come back as columns, so the source has to be on
// screen: this answer never ran BYDBQL and never touched stored rows.
const schemaLookupLabel = "schema catalog · no query run"

func chatRoleLabel(role session.ChatRole) string {
	switch role {
	case session.ChatRoleUser:
		return "You › "
	case session.ChatRoleTool:
		return "  ↳ "
	case session.ChatRoleSystem:
		return "System › "
	default:
		return "Agent › "
	}
}

// chatMessageLabel names the author of a message, crediting the catalog for a direct schema lookup.
//
// Attributing a schema read to the agent would suggest a model produced those columns.
func chatMessageLabel(message session.ChatMessage) string {
	if message.Kind == session.ChatMessageKindSchema {
		return "Schema › "
	}
	return chatRoleLabel(message.Role)
}

func fallback(value, fallbackValue string) string {
	if strings.TrimSpace(value) == "" {
		return fallbackValue
	}
	return value
}

func truncate(value string, maxWidth int) string {
	if maxWidth <= 3 {
		return value
	}
	if lipgloss.Width(value) <= maxWidth {
		return value
	}
	runes := []rune(stripANSI(value))
	for len(runes) > 0 && lipgloss.Width(string(runes)) > maxWidth-3 {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + "..."
}
