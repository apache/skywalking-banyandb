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

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/approval"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

var (
	borderColor = lipgloss.Color("#3B454B")
	tealColor   = lipgloss.Color("#3FD0BD")
	amberColor  = lipgloss.Color("#E9B85D")
	redColor    = lipgloss.Color("#F0766D")
	greenColor  = lipgloss.Color("#84CC72")
	mutedColor  = lipgloss.Color("#B4ADA0")
	panelStyle  = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor).
			Padding(0, 1)
	titleStyle = lipgloss.NewStyle().
			Foreground(tealColor).
			Bold(true)
	mutedStyle = lipgloss.NewStyle().
			Foreground(mutedColor)
	chipStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor).
			Padding(0, 1)
	activeChipStyle = chipStyle.Copy().
			BorderForeground(tealColor).
			Foreground(tealColor)
	warnStyle = lipgloss.NewStyle().
			Foreground(amberColor)
	okStyle = lipgloss.NewStyle().
		Foreground(greenColor)
	badStyle = lipgloss.NewStyle().
			Foreground(redColor)
)

func (m Model) currentPhaseLabel() string {
	if m.querySession == nil {
		return "phase intent"
	}
	return "phase " + m.querySession.Phase.String()
}

func (m Model) renderStatusLine(width int) string {
	status := m.status
	if m.busy {
		status = warnStyle.Render(status)
	}
	validation := "not checked"
	if m.querySession != nil && m.querySession.Validation.Message != "" {
		validation = m.querySession.Validation.Status()
	}
	reasoning := "off"
	if m.showReasoning {
		reasoning = "on"
	}
	statusLine := mutedStyle.Render(fmt.Sprintf(
		"Status: %s · Validation: %s · Policy: %s (Ctrl+P) · Reasoning: %s (Ctrl+R)",
		status,
		validation,
		m.executionPolicy.Label(),
		reasoning,
	))
	if m.trustSessionSuggested {
		statusLine = lipgloss.JoinVertical(lipgloss.Left,
			statusLine,
			warnStyle.Render(fmt.Sprintf(
				"%d clean reads completed · Ctrl+P can enable trust session",
				trustSessionCleanReadThreshold,
			)),
		)
	}
	return panelStyle.Width(width).Render(statusLine)
}

func (m Model) renderMessage(width int) string {
	return panelStyle.Width(width).Render(lipgloss.JoinVertical(
		lipgloss.Left,
		titleStyle.Render("Message · Enter to send"),
		m.message.View(),
		mutedStyle.Render("Ask a follow-up, refine the QL, or type @ to pin a schema resource."),
	))
}

func (m Model) renderChat(width, panelHeight int) string {
	rows := []string{
		titleStyle.Render("Conversation · activity"),
		mutedStyle.Render("↑↓ messages · pgup/pgdn detail · Tab composer"),
	}
	if m.pendingApproval != nil {
		rows = append(rows, warnStyle.Render("▸ execution waiting for approval"))
	} else if m.busy {
		rows = append(rows, warnStyle.Render("▸ workflow in progress · "+m.status))
	} else if len(m.activityLog) > 0 {
		lastActivity := m.activityLog[len(m.activityLog)-1]
		rows = append(rows, mutedStyle.Render("▸ "+truncate(lastActivity.title, width-8)))
	}
	entries := chatEntries(m.querySession, m.showReasoning, m.liveResponse, m.queuedMessage)
	if len(entries) == 0 {
		rows = append(rows, mutedStyle.Render("Start a conversation. Your sent message appears here immediately."))
	} else {
		detailViewportHeight := 0
		detailLines := []string(nil)
		if m.chatCursor >= 0 && m.chatCursor < len(entries) {
			selected := entries[m.chatCursor]
			if strings.TrimSpace(selected.detail) != "" {
				detailViewportHeight = chatDetailViewportHeight(panelHeight)
				detailLines = formatChatDetailLines(selected.detail, width-4)
			}
		}
		listViewportHeight := maxInt(panelHeight-8-detailViewportHeight-2, 4)
		endIdx := minInt(m.chatScroll+listViewportHeight, len(entries))
		for entryIdx := m.chatScroll; entryIdx < endIdx; entryIdx++ {
			entry := entries[entryIdx]
			lineStyle := mutedStyle
			prefix := " "
			if entryIdx == m.chatCursor {
				prefix = ">"
				lineStyle = activeChipStyle
			}
			switch entry.role {
			case session.ChatRoleUser:
				if entryIdx != m.chatCursor {
					lineStyle = titleStyle
				}
			case session.ChatRoleTool:
				if entryIdx != m.chatCursor {
					lineStyle = warnStyle
				}
			case session.ChatRoleAssistant:
				if entryIdx != m.chatCursor {
					lineStyle = okStyle
				}
			}
			rows = append(rows, lineStyle.Render(prefix+truncate(entry.headline, width-12)))
		}
		if len(detailLines) > 0 {
			rows = append(rows, titleStyle.Render("Detail · pgup/pgdn scroll"))
			detailEnd := minInt(m.chatDetailScroll+detailViewportHeight, len(detailLines))
			for lineIdx := m.chatDetailScroll; lineIdx < detailEnd; lineIdx++ {
				line := renderChatDetailLine(detailLines[lineIdx])
				if lipgloss.Width(line) > width-4 {
					line = truncateANSI(line, width-4)
				}
				rows = append(rows, line)
			}
			if len(detailLines) > detailViewportHeight {
				rows = append(rows, mutedStyle.Render(fmt.Sprintf(
					"detail %d-%d/%d lines",
					m.chatDetailScroll+1,
					detailEnd,
					len(detailLines),
				)))
			}
		}
		rows = append(rows, mutedStyle.Render(fmt.Sprintf("%d/%d messages", endIdx, len(entries))))
	}
	if m.pendingApproval != nil {
		rows = append(rows, warnStyle.Render("execution waiting for approval…"))
	} else if m.busy {
		rows = append(rows, warnStyle.Render("workflow in progress…"))
	}
	return panelStyle.Width(width).Height(panelHeight).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

type chatEntryView struct {
	role     session.ChatRole
	headline string
	detail   string
}

func chatEntryCount(querySession *session.QuerySession, showReasoning bool, liveResponse, queuedMessage string) int {
	return len(chatEntries(querySession, showReasoning, liveResponse, queuedMessage))
}

func chatEntries(querySession *session.QuerySession, showReasoning bool, liveResponse, queuedMessage string) []chatEntryView {
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
	if showReasoning && strings.TrimSpace(liveResponse) != "" {
		entries = append(entries, chatEntryView{
			role:     session.ChatRoleAssistant,
			headline: "reasoning: " + truncateRunes(singleLine(liveResponse), 96),
			detail:   workflow.NormalizeAgentDisplayText(liveResponse),
		})
	}
	return entries
}

func chatEntryFromMessage(message session.ChatMessage) chatEntryView {
	content := workflow.NormalizeAgentDisplayText(strings.TrimSpace(message.Content))
	headline := chatRoleLabel(message.Role) + singleLine(content)
	detail := content
	if structuredDetail := strings.TrimSpace(message.Detail); structuredDetail != "" {
		detail = workflow.NormalizeAgentDisplayText(structuredDetail)
	}
	if message.ToolName != "" {
		headline = chatRoleLabel(message.Role) + message.ToolName + ": " + singleLine(content)
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
	return chatEntryView{role: message.Role, headline: headline, detail: detail}
}

func chatLines(querySession *session.QuerySession, showReasoning bool, liveResponse string) []string {
	entries := chatEntries(querySession, showReasoning, liveResponse, "")
	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		lines = append(lines, entry.headline)
	}
	return lines
}

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

func (m Model) renderApproval(width int) string {
	if m.pendingApproval == nil {
		return panelStyle.Width(width).Render(mutedStyle.Render("Read-only BYDBQL runs automatically. Mutating statements still require approval."))
	}
	request := *m.pendingApproval
	rows := []string{
		titleStyle.Render("Execution approval required"),
		"Exact statement:",
		wrapText(request.Query, width-4),
		"Resource: " + fallback(request.Resource, "-"),
		"Groups: " + fallback(strings.Join(request.Groups, ", "), "-"),
		"Time range: " + fallback(request.TimeRange, "-"),
		"Limit: " + fallback(request.Limit, "-"),
		"Timeout: " + request.Timeout.String(),
		fmt.Sprintf("Preview cap: %d rows", request.PreviewRows),
		approvalScanEstimate(request),
		"Source: " + string(request.Source),
		warnStyle.Render("y execute once · n reject · e copy to editor and revise"),
	}
	return panelStyle.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

func approvalScanEstimate(request approval.Request) string {
	if request.Limit == "-" || request.TimeRange == "" {
		return warnStyle.Render("Estimated scan: unbounded from query text; approval is required")
	}
	return "Estimated scan: bounded by the displayed time range and LIMIT " + request.Limit
}

func (m Model) renderFooter(width int) string {
	commands := []string{
		"@ schema", "Ctrl+A send", "Ctrl+V validate", "Ctrl+E run", "Ctrl+F repair", "Ctrl+P policy", "Ctrl+R reasoning",
		"Ctrl+←/→ versions", "Ctrl+O export", "Ctrl+J full response", "Tab focus", "Esc stop/quit",
	}
	return lipgloss.NewStyle().Width(width).Foreground(mutedColor).Render(strings.Join(commands, "  "))
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

func truncateANSI(value string, maxWidth int) string {
	return truncate(value, maxWidth)
}
