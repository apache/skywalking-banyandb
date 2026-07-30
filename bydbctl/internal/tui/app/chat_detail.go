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
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/workflow"
)

var structuredDetailKeyPattern = regexp.MustCompile(`^(?i)(plan|workflow|query|parameters|input|output|candidate|message|hint|command|path|rows|summary|error)\s*[:=]\s*`)

var (
	inlineCodePattern = regexp.MustCompile("`([^`]+)`")
	boldPattern       = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	headerPattern     = regexp.MustCompile(`^#{1,3}\s+`)
	tableRowPattern   = regexp.MustCompile(`^\|.*\|$`)
)

var (
	codeDetailStyle = lipgloss.NewStyle().Foreground(amberColor)
	boldDetailStyle = lipgloss.NewStyle().Bold(true).Foreground(tealColor)
)

func chatDetailViewportHeight(panelHeight int) int {
	return clamp(panelHeight/3, 6, 14)
}

func formatChatDetailLines(content string, width int) []string {
	content = strings.TrimSpace(workflow.NormalizeAgentDisplayText(content))
	if content == "" || width <= 0 {
		return nil
	}
	if structuredLines := formatStructuredDetailContent(content, width); len(structuredLines) > 0 {
		return structuredLines
	}
	var lines []string
	inCodeBlock := false
	var codeBlockLang string
	var codeBlockLines []string
	for _, rawLine := range strings.Split(content, "\n") {
		trimmedLine := strings.TrimSpace(rawLine)
		if strings.HasPrefix(trimmedLine, "```") {
			if inCodeBlock {
				lines = append(lines, renderCodeBlock(codeBlockLang, codeBlockLines, width)...)
				codeBlockLang = ""
				codeBlockLines = nil
				inCodeBlock = false
				continue
			}
			inCodeBlock = true
			codeBlockLang = strings.TrimPrefix(trimmedLine, "```")
			continue
		}
		if inCodeBlock {
			codeBlockLines = append(codeBlockLines, rawLine)
			continue
		}
		if trimmedLine == "" {
			lines = append(lines, "")
			continue
		}
		if tableRowPattern.MatchString(trimmedLine) || strings.HasPrefix(trimmedLine, "|---") {
			lines = append(lines, mutedStyle.Render(truncate(trimmedLine, width)))
			continue
		}
		lines = append(lines, renderMarkdownLine(trimmedLine, width)...)
	}
	if inCodeBlock && len(codeBlockLines) > 0 {
		lines = append(lines, renderCodeBlock(codeBlockLang, codeBlockLines, width)...)
	}
	return compactDetailLines(lines)
}

func renderMarkdownLine(line string, width int) []string {
	prefix := ""
	styledPrefix := ""
	switch {
	case headerPattern.MatchString(line):
		line = headerPattern.ReplaceAllString(line, "")
		styledPrefix = titleStyle.Render("## ")
	case strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* "):
		line = strings.TrimSpace(line[2:])
		styledPrefix = mutedStyle.Render("• ")
	case len(line) > 2 && line[0] >= '0' && line[0] <= '9' && strings.Contains(line, ". "):
		if dotIdx := strings.Index(line, ". "); dotIdx > 0 && dotIdx < 4 {
			prefix = line[:dotIdx+2]
			line = strings.TrimSpace(line[dotIdx+2:])
			styledPrefix = mutedStyle.Render(prefix)
		}
	}
	plainLine := stripInlineMarkdown(line)
	wrapped := wrapRunes(plainLine, maxInt(width-lipgloss.Width(styledPrefix), 8))
	if len(wrapped) == 0 {
		return nil
	}
	rendered := make([]string, 0, len(wrapped))
	for lineIdx, wrappedLine := range wrapped {
		styledLine := renderInlineMarkdown(wrappedLine)
		if lineIdx == 0 {
			rendered = append(rendered, styledPrefix+styledLine)
			continue
		}
		rendered = append(rendered, strings.Repeat(" ", lipgloss.Width(styledPrefix))+styledLine)
	}
	return rendered
}

func renderInlineMarkdown(line string) string {
	line = inlineCodePattern.ReplaceAllStringFunc(line, func(match string) string {
		innerText := strings.Trim(match, "`")
		return codeDetailStyle.Render(innerText)
	})
	line = boldPattern.ReplaceAllStringFunc(line, func(match string) string {
		innerText := strings.Trim(match, "*")
		return boldDetailStyle.Render(innerText)
	})
	return line
}

func stripInlineMarkdown(line string) string {
	line = inlineCodePattern.ReplaceAllString(line, "$1")
	line = boldPattern.ReplaceAllString(line, "$1")
	line = strings.ReplaceAll(line, "**", "")
	return strings.TrimSpace(line)
}

func renderCodeBlock(language string, codeLines []string, width int) []string {
	label := strings.TrimSpace(language)
	if label == "" {
		label = "code"
	}
	rows := []string{titleStyle.Render(label)}
	for _, codeLine := range codeLines {
		for _, wrappedLine := range wrapRunes(strings.TrimRight(codeLine, " "), maxInt(width-2, 8)) {
			rows = append(rows, codeDetailStyle.Render(" "+wrappedLine))
		}
	}
	return rows
}

func compactDetailLines(lines []string) []string {
	compacted := make([]string, 0, len(lines))
	blankPending := false
	for _, line := range lines {
		if strings.TrimSpace(stripANSI(line)) == "" {
			if !blankPending && len(compacted) > 0 {
				blankPending = true
				compacted = append(compacted, "")
			}
			continue
		}
		blankPending = false
		compacted = append(compacted, line)
	}
	return compacted
}

func stripANSI(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	insideEscape := false
	for _, valueRune := range value {
		if insideEscape {
			if valueRune == 'm' {
				insideEscape = false
			}
			continue
		}
		if valueRune == '\x1b' {
			insideEscape = true
			continue
		}
		builder.WriteRune(valueRune)
	}
	return builder.String()
}

func wrapRunes(text string, width int) []string {
	if width <= 0 {
		return []string{text}
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return nil
	}
	var lines []string
	var current []rune
	currentWidth := 0
	for _, textRune := range runes {
		if textRune == '\n' {
			lines = append(lines, string(current))
			current = nil
			currentWidth = 0
			continue
		}
		runeWidth := lipgloss.Width(string(textRune))
		if currentWidth+runeWidth > width && len(current) > 0 {
			lines = append(lines, string(current))
			current = []rune{textRune}
			currentWidth = runeWidth
			continue
		}
		current = append(current, textRune)
		currentWidth += runeWidth
	}
	if len(current) > 0 {
		lines = append(lines, string(current))
	}
	return lines
}

func formatStructuredDetailContent(content string, width int) []string {
	if looksLikeStructuredDetail(content) {
		return formatStructuredDetailLines(content, width)
	}
	return nil
}

func looksLikeStructuredDetail(content string) bool {
	trimmedContent := strings.TrimSpace(content)
	if trimmedContent == "" {
		return false
	}
	if strings.HasPrefix(trimmedContent, "{") || strings.HasPrefix(trimmedContent, "[") {
		return true
	}
	if structuredDetailKeyPattern.MatchString(trimmedContent) {
		return true
	}
	for _, line := range strings.Split(trimmedContent, "\n") {
		if structuredDetailKeyPattern.MatchString(strings.TrimSpace(line)) {
			return true
		}
	}
	return strings.Contains(trimmedContent, "plan=") ||
		strings.Contains(trimmedContent, "workflow=") ||
		strings.Contains(trimmedContent, "query=")
}

func formatStructuredDetailLines(content string, width int) []string {
	lines := make([]string, 0, 16)
	for _, rawLine := range strings.Split(content, "\n") {
		trimmedLine := strings.TrimSpace(rawLine)
		if trimmedLine == "" {
			lines = append(lines, "")
			continue
		}
		if sectionLabel, sectionBody, ok := splitStructuredDetailSection(trimmedLine); ok {
			lines = append(lines, titleStyle.Render(sectionLabel))
			lines = append(lines, formatStructuredDetailBody(sectionBody, width)...)
			continue
		}
		if strings.HasPrefix(trimmedLine, "{") || strings.HasPrefix(trimmedLine, "[") {
			lines = append(lines, formatStructuredDetailBody(trimmedLine, width)...)
			continue
		}
		lines = append(lines, wrapRunes(trimmedLine, width)...)
	}
	return compactDetailLines(lines)
}

func splitStructuredDetailSection(line string) (string, string, bool) {
	if keyValue := strings.SplitN(line, "=", 2); len(keyValue) == 2 && isStructuredDetailKey(keyValue[0]) {
		return strings.TrimSpace(keyValue[0]), strings.TrimSpace(keyValue[1]), true
	}
	if keyValue := strings.SplitN(line, ":", 2); len(keyValue) == 2 && isStructuredDetailKey(keyValue[0]) {
		return strings.TrimSpace(keyValue[0]), strings.TrimSpace(keyValue[1]), true
	}
	return "", "", false
}

func isStructuredDetailKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "plan", "workflow", "query", "parameters", "input", "output", "candidate", "message", "hint", "command", "path", "rows", "summary", "error":
		return true
	default:
		return false
	}
}

func formatStructuredDetailBody(body string, width int) []string {
	trimmedBody := strings.TrimSpace(body)
	if trimmedBody == "" {
		return nil
	}
	if strings.HasPrefix(trimmedBody, "SELECT ") || strings.HasPrefix(trimmedBody, "SHOW ") {
		return renderCodeBlock("bydbql", strings.Split(trimmedBody, "\n"), width)
	}
	if strings.HasPrefix(trimmedBody, "{") || strings.HasPrefix(trimmedBody, "[") {
		return formatJSONResponsePreview(trimmedBody, width, maxExecutionResponseLines)
	}
	return wrapRunes(trimmedBody, width)
}

func renderChatDetailLine(line string) string {
	if strings.Contains(line, "\x1b[") {
		return line
	}
	plainLine := strings.TrimSpace(stripANSI(line))
	switch {
	case plainLine == "":
		return line
	case strings.HasPrefix(plainLine, "Error:") || strings.HasPrefix(plainLine, "error:"):
		return badStyle.Render(line)
	case strings.HasPrefix(plainLine, "Hint:") || strings.HasPrefix(plainLine, "hint="):
		return warnStyle.Render(line)
	case isStructuredDetailKey(strings.TrimSuffix(plainLine, ":")):
		return titleStyle.Render(line)
	case strings.HasPrefix(plainLine, "…"):
		return mutedStyle.Render(line)
	default:
		if strings.TrimSpace(line) == "" {
			return line
		}
		return mutedStyle.Render(line)
	}
}

func appendCandidateDetail(detail, candidate, status string) string {
	block := "\n\n**BYDBQL candidate** [" + status + "]\n\n```bydbql\n" + strings.TrimSpace(candidate) + "\n```"
	if strings.TrimSpace(detail) == "" {
		return strings.TrimSpace(block)
	}
	return detail + block
}
