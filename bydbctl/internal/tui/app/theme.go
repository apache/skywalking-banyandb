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
	"github.com/charmbracelet/glamour/ansi"
	"github.com/charmbracelet/lipgloss"
)

// Selection and status glyphs pair with color so state never depends on color alone.
const (
	glyphSelected = "▸"
	glyphOK       = "✓"
	glyphFailed   = "✗"
	glyphRunning  = "⟳"
	glyphPending  = "○"
	glyphActive   = "●"
	glyphStop     = "■"
	glyphWarn     = "⚠"
	glyphTruncate = "…"
)

// selectionPrefixWidth keeps selected and unselected rows the same width so the layout never shifts.
const selectionPrefixWidth = 2

// statusNotChecked is the validation status of a candidate that has not been validated yet.
const statusNotChecked = "not checked"

// fitRows truncates panel content to the rows it was budgeted, marking the cut.
//
// Every panel must obey its height budget, or a small terminal renders more rows than it has.
func fitRows(rows []string, contentHeight int) []string {
	if contentHeight <= 0 {
		return nil
	}
	if len(rows) <= contentHeight {
		return rows
	}
	if contentHeight == 1 {
		return []string{mutedStyle.Render(glyphTruncate)}
	}
	fitted := make([]string, 0, contentHeight)
	fitted = append(fitted, rows[:contentHeight-1]...)
	return append(fitted, mutedStyle.Render(glyphTruncate+" more"))
}

var (
	borderColor = lipgloss.Color("#3B454B")
	tealColor   = lipgloss.Color("#3FD0BD")
	amberColor  = lipgloss.Color("#E9B85D")
	redColor    = lipgloss.Color("#F0766D")
	greenColor  = lipgloss.Color("#84CC72")
	mutedColor  = lipgloss.Color("#B4ADA0")
)

var (
	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor).
			Padding(0, 1)
	activePanelStyle = panelStyle.
				BorderForeground(tealColor)
	titleStyle = lipgloss.NewStyle().
			Foreground(tealColor).
			Bold(true)
	mutedStyle = lipgloss.NewStyle().
			Foreground(mutedColor)
	warnStyle = lipgloss.NewStyle().
			Foreground(amberColor)
	okStyle = lipgloss.NewStyle().
		Foreground(greenColor)
	badStyle = lipgloss.NewStyle().
			Foreground(redColor)
	// focusStyle marks the focused control with weight as well as color.
	//
	// Under NO_COLOR lipgloss drops both, so the glyph prefix is what carries focus there.
	focusStyle = lipgloss.NewStyle().
			Foreground(tealColor).
			Bold(true)
	// keyStyle renders the key half of a footer hint; the action half stays muted.
	keyStyle = lipgloss.NewStyle().
			Foreground(tealColor)
)

// Theme colors as glamour needs them: hex strings rather than lipgloss colors.
const (
	tealHex  = "#3FD0BD"
	amberHex = "#E9B85D"
	mutedHex = "#B4ADA0"
	redHex   = "#F0766D"
)

// agentMarkdownStyle renders agent markdown in the workspace palette, without decorative chrome.
//
// The panels supply their own frame, indentation, and blank-line budget, so every glamour margin
// and heading prefix is cleared; a rule that reserved rows here would come out of the message list.
func agentMarkdownStyle() ansi.StyleConfig {
	noMargin := uint(0)
	bold := true
	return ansi.StyleConfig{
		Document:  ansi.StyleBlock{Margin: &noMargin},
		Paragraph: ansi.StyleBlock{},
		BlockQuote: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: markdownColor(mutedHex), BlockPrefix: "▏ "},
			Indent:         &noMargin,
		},
		List:        ansi.StyleList{StyleBlock: ansi.StyleBlock{Indent: &noMargin}, LevelIndent: 2},
		Item:        ansi.StylePrimitive{BlockPrefix: "• "},
		Enumeration: ansi.StylePrimitive{BlockPrefix: ". "},
		Heading: ansi.StyleBlock{
			StylePrimitive: ansi.StylePrimitive{Color: markdownColor(tealHex), Bold: &bold},
			Margin:         &noMargin,
		},
		H1:             ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: ""}},
		H2:             ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: ""}},
		H3:             ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: ""}},
		H4:             ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: ""}},
		H5:             ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: ""}},
		H6:             ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Prefix: ""}},
		Strong:         ansi.StylePrimitive{Color: markdownColor(tealHex), Bold: &bold},
		Emph:           ansi.StylePrimitive{Color: markdownColor(mutedHex)},
		HorizontalRule: ansi.StylePrimitive{Color: markdownColor(mutedHex), Format: "─"},
		Code:           ansi.StyleBlock{StylePrimitive: ansi.StylePrimitive{Color: markdownColor(amberHex)}},
		CodeBlock: ansi.StyleCodeBlock{
			StyleBlock: ansi.StyleBlock{
				StylePrimitive: ansi.StylePrimitive{Color: markdownColor(amberHex)},
				Margin:         &noMargin,
			},
		},
		Link:      ansi.StylePrimitive{Color: markdownColor(tealHex)},
		LinkText:  ansi.StylePrimitive{Color: markdownColor(tealHex)},
		Image:     ansi.StylePrimitive{Color: markdownColor(mutedHex)},
		ImageText: ansi.StylePrimitive{Color: markdownColor(mutedHex)},
		Table: ansi.StyleTable{
			StyleBlock:      ansi.StyleBlock{Margin: &noMargin},
			CenterSeparator: markdownColor("┼"),
			ColumnSeparator: markdownColor("│"),
			RowSeparator:    markdownColor("─"),
		},
		DefinitionTerm:        ansi.StylePrimitive{Color: markdownColor(tealHex)},
		DefinitionDescription: ansi.StylePrimitive{BlockPrefix: "  "},
		Text:                  ansi.StylePrimitive{},
		Strikethrough:         ansi.StylePrimitive{CrossedOut: &bold},
	}
}

// markdownColor adapts a theme value to the pointer fields of the glamour style config.
func markdownColor(value string) *string {
	return &value
}
