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

var headerPattern = regexp.MustCompile(`^#{1,3}\s+`)

var codeDetailStyle = lipgloss.NewStyle().Foreground(amberColor)

func chatDetailViewportHeight(panelHeight int) int {
	return clamp(panelHeight/3, 6, 14)
}

// formatChatDetailLines renders one agent message body into styled, width-bounded lines.
//
// A tool argument or result arrives as labeled JSON rather than prose, so it keeps its own
// key-and-body layout; everything else is agent markdown and goes through the shared renderer.
func formatChatDetailLines(content string, width int) []string {
	return formatDetailLines(normalizeDetailContent(content), width)
}

// formatExactDetailLines renders a body bydbctl composed itself, without repairing its text.
//
// The repair applied to agent prose rejoins tokens it reads as fragmented, which would silently
// rewrite an exact BanyanDB identifier such as a column whose name contains a space.
func formatExactDetailLines(content string, width int) []string {
	return formatDetailLines(strings.TrimSpace(content), width)
}

// formatDetailLines renders one already-prepared detail body.
func formatDetailLines(content string, width int) []string {
	if content == "" || width <= 0 {
		return nil
	}
	if structuredLines := formatStructuredDetailContent(content, width); len(structuredLines) > 0 {
		return structuredLines
	}
	return compactDetailLines(renderMarkdownDetail(content, width))
}

// normalizeDetailContent tidies agent text without collapsing the line structure.
//
// The shared normalizer joins every line into one, which would flatten code fences and headings
// into the body text before the markdown parser below could recognize them.
func normalizeDetailContent(content string) string {
	lines := strings.Split(content, "\n")
	normalized := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			normalized = append(normalized, "")
			continue
		}
		marker, body := splitLineMarker(line)
		normalized = append(normalized, marker+workflow.NormalizeAgentDisplayText(body))
	}
	return strings.TrimSpace(strings.Join(normalized, "\n"))
}

// listMarkerPattern matches the bullet or number that opens a markdown list item.
var listMarkerPattern = regexp.MustCompile(`^\s*(?:[-*+]|\d{1,3}[.)])\s+`)

// splitLineMarker separates a leading list or heading marker from the text it introduces.
//
// The shared normalizer rejoins fragmented tokens by deleting spaces around hyphens, which would
// otherwise consume a "- " bullet and turn the list item into part of the previous word.
func splitLineMarker(line string) (string, string) {
	if headerMatch := headerPattern.FindString(line); headerMatch != "" {
		return headerMatch, line[len(headerMatch):]
	}
	if listMatch := listMarkerPattern.FindString(line); listMatch != "" {
		return listMatch, line[len(listMatch):]
	}
	return "", line
}

// renderCodeBlock renders a fenced block, labeling it only when the language adds information.
func renderCodeBlock(language string, codeLines []string, width int) []string {
	var rows []string
	if label := strings.TrimSpace(language); label != "" && !strings.EqualFold(label, "bydbql") {
		rows = append(rows, titleStyle.Render(label))
	}
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
