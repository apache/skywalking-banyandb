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
)

// keyHint pairs a key with the action it performs so the footer can style the two halves apart.
type keyHint struct {
	key    string
	action string
}

// keyHintSeparator spaces footer hints far enough apart to read as separate items.
const keyHintSeparator = "   "

// globalKeyHints are available from every panel.
var globalKeyHints = []keyHint{
	{key: "Tab", action: "focus"},
	{key: "Alt+1-4", action: "panel"},
	{key: "?", action: "help"},
	{key: "Esc", action: "quit"},
}

// contextKeyHints lists the bindings that act on the focused panel, most useful first.
func (m Model) contextKeyHints() []keyHint {
	if m.busy {
		return []keyHint{{key: "Esc", action: "stop run"}}
	}
	switch m.focus {
	case focusChat:
		return []keyHint{
			{key: "↑↓", action: "message"},
			{key: "pgup/pgdn", action: "detail"},
		}
	case focusMessage:
		return []keyHint{
			{key: "Enter", action: "send"},
			{key: "↑↓", action: "history"},
			{key: "@", action: "schema"},
			{key: "/", action: "catalog"},
		}
	case focusQuery, focusStart, focusEnd, focusLimit:
		return []keyHint{
			{key: "Ctrl+E", action: "run"},
			{key: "Ctrl+G", action: "agent fix"},
			{key: "Ctrl+←/→", action: "previous query"},
		}
	case focusExecution:
		// A schema has no rows to select, no columns to scroll, and nothing to export.
		if m.showsSchemaEvidence() {
			return []keyHint{
				{key: "pgup/pgdn", action: "scroll schema"},
				{key: "@", action: "another schema"},
			}
		}
		return []keyHint{
			{key: "↑↓", action: "row"},
			{key: "←/→", action: "scroll"},
			{key: "pgup/pgdn", action: "detail"},
			{key: "Ctrl+O", action: "export"},
		}
	default:
		return nil
	}
}

// renderFooter shows the bindings that apply right now, then the global ones.
//
// Showing every binding at once would make the relevant keys harder to find than showing none.
func (m Model) renderFooter(width int) string {
	if m.helpVisible {
		return mutedStyle.Width(width).Render(renderKeyHints([]keyHint{{key: "?", action: "close help"}}, width))
	}
	hints := append(m.contextKeyHints(), globalKeyHints...)
	return lipgloss.NewStyle().Width(width).Render(renderKeyHints(hints, width))
}

// renderKeyHints wraps hints onto as many lines as the width requires.
func renderKeyHints(hints []keyHint, width int) string {
	var lines []string
	currentLine := ""
	currentWidth := 0
	for _, hint := range hints {
		rendered := keyStyle.Render(hint.key) + mutedStyle.Render(" "+hint.action)
		hintWidth := lipgloss.Width(rendered)
		if currentLine != "" && currentWidth+lipgloss.Width(keyHintSeparator)+hintWidth > width {
			lines = append(lines, currentLine)
			currentLine = rendered
			currentWidth = hintWidth
			continue
		}
		if currentLine != "" {
			currentLine += keyHintSeparator
			currentWidth += lipgloss.Width(keyHintSeparator)
		}
		currentLine += rendered
		currentWidth += hintWidth
	}
	if currentLine != "" {
		lines = append(lines, currentLine)
	}
	return strings.Join(lines, "\n")
}

// helpSection groups the bindings of one context on the help screen.
type helpSection struct {
	title string
	hints []keyHint
}

// helpSections lists every binding, the focused panel first, as the help overlay contents.
func (m Model) helpSections() []helpSection {
	return []helpSection{
		{title: "Focused panel · " + m.focusLabel(), hints: m.contextKeyHints()},
		{title: "Move focus", hints: []keyHint{
			{key: "Tab / Shift+Tab", action: "next or previous control"},
			{key: "Alt+1 / 1", action: "conversation"},
			{key: "Alt+2 / 2", action: "candidate QL"},
			{key: "Alt+3 / 3", action: "composer"},
			{key: "Alt+4 / 4", action: "data preview"},
			{key: "click", action: "focus the panel under the cursor"},
			{key: "wheel", action: "scroll the focused panel"},
		}},
		{title: "Conversation", hints: []keyHint{
			{key: "↑↓ / j k", action: "select a message"},
			{key: "pgup / pgdn", action: "scroll the selected detail"},
		}},
		{title: "Candidate QL", hints: []keyHint{
			{key: "Ctrl+E", action: "run the query in the editor"},
			{key: "Ctrl+G", action: "ask the agent to repair an invalid candidate"},
			{key: "Ctrl+← / Ctrl+→", action: "step back to a previous or next query"},
		}},
		{title: "Composer", hints: []keyHint{
			{key: "Enter", action: "send the message to the agent"},
			{key: "↑ / ↓", action: "recall an earlier message past the first or last line"},
			{key: "@", action: "search the local schema catalog"},
			{key: "/", action: "filter the catalog browser"},
		}},
		{title: "Data preview", hints: []keyHint{
			{key: "↑↓ / j k", action: "select a row"},
			{key: "← / →", action: "scroll columns"},
			{key: "pgup / pgdn", action: "scroll the row detail"},
			{key: "Ctrl+O", action: "export the result to a file"},
		}},
		{title: "Global", hints: []keyHint{
			{key: "Ctrl+L", action: "reload the schema catalog"},
			{key: "?", action: "toggle this help"},
			{key: "Esc", action: "close an overlay, stop a run, or quit"},
			{key: "Ctrl+C", action: "stop a run, or quit"},
		}},
	}
}

// renderHelpOverlay renders the keybinding reference, scrolled to fit the terminal.
func (m Model) renderHelpOverlay(width, height int) string {
	lines := m.helpLines()
	contentHeight := panelContentHeight(height)
	header := titleStyle.Render("Keyboard reference")
	footer := mutedStyle.Render("? or Esc closes this help")
	viewportHeight := maxInt(contentHeight-2, 1)
	maxScroll := maxInt(len(lines)-viewportHeight, 0)
	scroll := clamp(m.helpScroll, 0, maxScroll)
	if maxScroll > 0 {
		footer = mutedStyle.Render(fmt.Sprintf("%d-%d/%d · pgup/pgdn scroll · ? or Esc closes",
			scroll+1, minInt(scroll+viewportHeight, len(lines)), len(lines)))
	}
	rows := append([]string{header}, lines[scroll:minInt(scroll+viewportHeight, len(lines))]...)
	return activePanelStyle.Width(width).Height(contentHeight).
		Render(lipgloss.JoinVertical(lipgloss.Left, append(rows, footer)...))
}

// helpLines flattens every section into the scrollable body of the help overlay.
func (m Model) helpLines() []string {
	var lines []string
	for _, section := range m.helpSections() {
		if len(section.hints) == 0 {
			continue
		}
		if len(lines) > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, focusStyle.Render(section.title))
		for _, hint := range section.hints {
			lines = append(lines, "  "+keyStyle.Render(padDisplayWidth(hint.key, helpKeyColumnWidth))+
				mutedStyle.Render(" "+hint.action))
		}
	}
	return lines
}

// scrollHelp moves the help viewport, clamped to the reference length.
func (m *Model) scrollHelp(delta int) {
	m.helpScroll = clamp(m.helpScroll+delta, 0, maxInt(len(m.helpLines())-1, 0))
}

// helpKeyColumnWidth aligns the action column of the help overlay.
const helpKeyColumnWidth = 16
