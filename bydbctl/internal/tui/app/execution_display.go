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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

const (
	maxExecutionResponseLines = 40
	maxDisplayStringRunes     = 120
	maxDisplayArrayItems      = 3
	maxExecutionTableColumns  = 5
)

var executionTableColumnOrder = []string{
	"timestamp",
	"trace_id",
	"endpoint_id",
	"content",
	"service_id",
	"service_instance_id",
	"span_id",
	"tags",
	"elementId",
}

type executionDisplayOptions struct {
	width       int
	selectedRow int
	showRaw     bool
}

func executionDetailLines(executionResult session.ExecutionResult, opts executionDisplayOptions) []string {
	lines := []string{
		"Resource type: " + fallback(executionResult.ResourceType, "-"),
		"Duration: " + executionResult.Duration.String(),
		"Command: " + fallback(executionResult.Command, "-"),
		"Path: " + fallback(executionResult.Path, "-"),
		fmt.Sprintf("Rows: %d", executionResult.Rows),
		"Summary: " + executionResult.Summary,
	}
	if executionResult.Error != "" {
		lines = append(lines, "Error: "+executionResult.Error)
	}
	if executionResult.Hint != "" {
		lines = append(lines, "Hint: "+executionResult.Hint)
	}
	if len(executionResult.Preview) > 0 {
		displayColumns := selectDisplayColumns(executionResult.Columns)
		projectedRows := projectPreviewRows(executionResult.Preview, executionResult.Columns, displayColumns)
		lines = append(lines, "Table preview · ↑↓ row · Ctrl+O export")
		lines = append(lines, formatPreviewTable(displayColumns, projectedRows, opts.width, opts.selectedRow)...)
		if len(executionResult.Columns) > len(displayColumns) {
			lines = append(lines, fmt.Sprintf("… %d more columns in row detail below", len(executionResult.Columns)-len(displayColumns)))
		}
		if executionResult.Truncated {
			lines = append(lines, fmt.Sprintf("… showing first %d rows", len(executionResult.Preview)))
		}
		if opts.selectedRow >= 0 && opts.selectedRow < len(executionResult.Preview) {
			lines = append(lines, formatExecutionRowDetail(
				executionResult.Columns,
				executionResult.Preview[opts.selectedRow],
				opts.selectedRow+1,
				len(executionResult.Preview),
				opts.width,
			)...)
		}
	}
	if strings.TrimSpace(executionResult.Response) == "" {
		return lines
	}
	if len(executionResult.Preview) > 0 && !opts.showRaw {
		lines = append(lines, fmt.Sprintf(
			"Full JSON hidden (%s). Ctrl+O exports it to a file.",
			formatByteCount(len(executionResult.Response)),
		))
		return lines
	}
	lines = append(lines, "Response preview")
	lines = append(lines, formatJSONResponsePreview(executionResult.Response, opts.width, maxExecutionResponseLines)...)
	return lines
}

func selectDisplayColumns(columns []string) []string {
	if len(columns) <= maxExecutionTableColumns {
		return append([]string(nil), columns...)
	}
	selected := make([]string, 0, maxExecutionTableColumns)
	seen := make(map[string]struct{}, len(columns))
	for _, column := range executionTableColumnOrder {
		if !containsColumn(columns, column) {
			continue
		}
		selected = append(selected, column)
		seen[column] = struct{}{}
		if len(selected) >= maxExecutionTableColumns {
			return selected
		}
	}
	rest := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, alreadyUsed := seen[column]; alreadyUsed {
			continue
		}
		rest = append(rest, column)
	}
	sort.Strings(rest)
	for _, column := range rest {
		selected = append(selected, column)
		if len(selected) >= maxExecutionTableColumns {
			break
		}
	}
	return selected
}

func projectPreviewRows(preview [][]string, columns []string, displayColumns []string) [][]string {
	columnIndex := make(map[string]int, len(columns))
	for idx, column := range columns {
		columnIndex[column] = idx
	}
	projected := make([][]string, 0, len(preview))
	for _, row := range preview {
		projectedRow := make([]string, 0, len(displayColumns))
		for _, column := range displayColumns {
			sourceIdx, exists := columnIndex[column]
			if !exists || sourceIdx >= len(row) {
				projectedRow = append(projectedRow, "")
				continue
			}
			projectedRow = append(projectedRow, row[sourceIdx])
		}
		projected = append(projected, projectedRow)
	}
	return projected
}

func containsColumn(columns []string, target string) bool {
	for _, column := range columns {
		if column == target {
			return true
		}
	}
	return false
}

func formatExecutionRowDetail(columns []string, row []string, rowNumber, rowTotal, width int) []string {
	lines := []string{fmt.Sprintf("Row detail %d/%d", rowNumber, rowTotal)}
	for idx, column := range columns {
		value := ""
		if idx < len(row) {
			value = row[idx]
		}
		lines = append(lines, wrapRunes(column+": "+value, max(width-2, 24))...)
	}
	return lines
}

func formatPreviewTable(columns []string, preview [][]string, width, selectedRow int) []string {
	if len(columns) == 0 || len(preview) == 0 {
		return nil
	}
	colWidths := previewColumnWidths(columns, preview, width)
	header := "  " + renderPreviewRow(columns, colWidths)
	separatorWidth := sumInts(colWidths) + 3*(len(columns)-1)
	separator := "  " + strings.Repeat("─", max(separatorWidth, 8))
	lines := []string{header, separator}
	for rowIdx, row := range preview {
		line := renderPreviewRow(row, colWidths)
		if rowIdx == selectedRow {
			line = "> " + line
		} else {
			line = "  " + line
		}
		lines = append(lines, line)
	}
	return lines
}

func previewColumnWidths(columns []string, preview [][]string, totalWidth int) []int {
	widths := make([]int, len(columns))
	for idx, column := range columns {
		widths[idx] = lipgloss.Width(column)
	}
	for _, row := range preview {
		for idx, cell := range row {
			if idx >= len(widths) {
				break
			}
			cellWidth := lipgloss.Width(cell)
			if cellWidth > widths[idx] {
				widths[idx] = cellWidth
			}
		}
	}
	for idx := range widths {
		if widths[idx] < 3 {
			widths[idx] = 3
		}
	}
	if totalWidth <= 0 {
		return widths
	}
	const maxColumnWidth = 56
	for idx := range widths {
		if widths[idx] > maxColumnWidth {
			widths[idx] = maxColumnWidth
		}
	}
	usedWidth := sumInts(widths) + 3*len(widths) + 2
	for usedWidth > totalWidth {
		widestIdx := 0
		for idx := 1; idx < len(widths); idx++ {
			if widths[idx] > widths[widestIdx] {
				widestIdx = idx
			}
		}
		if widths[widestIdx] <= 8 {
			break
		}
		widths[widestIdx]--
		usedWidth = sumInts(widths) + 3*len(widths) + 2
	}
	return widths
}

func renderPreviewRow(cells []string, widths []int) string {
	parts := make([]string, 0, len(cells))
	for idx, cell := range cells {
		columnWidth := 12
		if idx < len(widths) {
			columnWidth = widths[idx]
		}
		parts = append(parts, padDisplayWidth(cell, columnWidth))
	}
	return strings.Join(parts, " │ ")
}

func padDisplayWidth(value string, width int) string {
	valueWidth := lipgloss.Width(value)
	if valueWidth > width {
		return truncate(value, width)
	}
	return value + strings.Repeat(" ", max(width-valueWidth, 0))
}

// previewTableViewport clamps the horizontal offset and returns the visible slice of each table row.
func previewTableViewport(lines []string, width, offset int) []string {
	offset = clamp(offset, 0, previewTableMaxHorizontalOffset(lines, width))
	visible := make([]string, 0, len(lines))
	for _, line := range lines {
		prefix, content := previewTableLinePrefix(line)
		visibleWidth := max(width-lipgloss.Width(prefix), 1)
		visible = append(visible, prefix+horizontalViewport(content, offset, visibleWidth))
	}
	return visible
}

func previewTableMaxHorizontalOffset(lines []string, width int) int {
	maxWidth := 0
	for _, line := range lines {
		prefix, content := previewTableLinePrefix(line)
		visibleWidth := max(width-lipgloss.Width(prefix), 1)
		maxWidth = max(maxWidth, lipgloss.Width(content)-visibleWidth)
	}
	return max(maxWidth, 0)
}

func previewTableLinePrefix(line string) (string, string) {
	if len(line) < 2 {
		return "", line
	}
	return line[:2], line[2:]
}

func horizontalViewport(value string, offset, width int) string {
	if width <= 0 {
		return ""
	}
	var visible strings.Builder
	position := 0
	visibleWidth := 0
	for _, valueRune := range value {
		runeWidth := lipgloss.Width(string(valueRune))
		runeEnd := position + runeWidth
		if runeEnd <= offset {
			position = runeEnd
			continue
		}
		if position < offset {
			position = runeEnd
			continue
		}
		if visibleWidth+runeWidth > width {
			break
		}
		visible.WriteRune(valueRune)
		visibleWidth += runeWidth
		position = runeEnd
	}
	return visible.String()
}

func formatJSONResponsePreview(response string, width, maxLines int) []string {
	trimmedResponse := strings.TrimSpace(response)
	if trimmedResponse == "" {
		return nil
	}
	var parsed any
	if unmarshalErr := json.Unmarshal([]byte(trimmedResponse), &parsed); unmarshalErr != nil {
		return truncateLines(wrapRunes(trimmedResponse, width), maxLines)
	}
	sanitized := sanitizeJSONValue(parsed, 0)
	formatted, marshalErr := json.MarshalIndent(sanitized, "", "  ")
	if marshalErr != nil {
		return truncateLines(wrapRunes(trimmedResponse, width), maxLines)
	}
	return truncateLines(wrapRunes(string(formatted), width), maxLines)
}

func sanitizeJSONValue(value any, depth int) any {
	switch typedValue := value.(type) {
	case map[string]any:
		sanitized := make(map[string]any, len(typedValue))
		for key, nestedValue := range typedValue {
			if key == "binaryData" || key == "tags_raw_data" {
				sanitized[key] = "<omitted>"
				continue
			}
			sanitized[key] = sanitizeJSONValue(nestedValue, depth+1)
		}
		return sanitized
	case []any:
		if depth > 0 && len(typedValue) > maxDisplayArrayItems {
			sanitized := make([]any, maxDisplayArrayItems)
			for idx := 0; idx < maxDisplayArrayItems; idx++ {
				sanitized[idx] = sanitizeJSONValue(typedValue[idx], depth+1)
			}
			return sanitized
		}
		sanitized := make([]any, len(typedValue))
		for idx, nestedValue := range typedValue {
			sanitized[idx] = sanitizeJSONValue(nestedValue, depth+1)
		}
		return sanitized
	case string:
		return truncateDisplayString(typedValue)
	default:
		return typedValue
	}
}

func truncateDisplayString(value string) string {
	runes := []rune(value)
	if len(runes) <= maxDisplayStringRunes {
		return value
	}
	return string(runes[:maxDisplayStringRunes-1]) + "…"
}

func truncateLines(lines []string, maxLines int) []string {
	if maxLines <= 0 || len(lines) <= maxLines {
		return lines
	}
	remaining := len(lines) - maxLines + 1
	truncated := append([]string{}, lines[:maxLines-1]...)
	truncated = append(truncated, fmt.Sprintf("… %d more lines", remaining))
	return truncated
}

func formatByteCount(size int) string {
	switch {
	case size >= 1_048_576:
		return fmt.Sprintf("%.1f MB", float64(size)/1_048_576)
	case size >= 1024:
		return fmt.Sprintf("%.1f KB", float64(size)/1024)
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func sumInts(values []int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}

func exportExecutionResult(executionResult session.ExecutionResult) (string, error) {
	homeDir, homeErr := os.UserHomeDir()
	if homeErr != nil {
		return "", fmt.Errorf("failed to resolve home directory: %w", homeErr)
	}
	exportDir := filepath.Join(homeDir, ".bydbctl", "exports")
	if mkdirErr := os.MkdirAll(exportDir, 0o750); mkdirErr != nil {
		return "", fmt.Errorf("failed to create export directory: %w", mkdirErr)
	}
	filename := fmt.Sprintf("execution-%s.json", time.Now().Format("20060102-150405"))
	exportPath := filepath.Join(exportDir, filename)
	payload := buildExecutionExportPayload(executionResult)
	// An export holds queried rows, so it stays readable only by the user who ran the query.
	if writeErr := os.WriteFile(exportPath, payload, 0o600); writeErr != nil {
		return "", fmt.Errorf("failed to write export file: %w", writeErr)
	}
	return exportPath, nil
}

func buildExecutionExportPayload(executionResult session.ExecutionResult) []byte {
	exportEnvelope := map[string]any{
		"exportedAt": executionResult.CheckedAt.UTC().Format(time.RFC3339Nano),
		"query":      executionResult.Query,
		"summary":    executionResult.Summary,
		"rows":       executionResult.Rows,
		"columns":    executionResult.Columns,
		"preview":    executionResult.Preview,
	}
	var parsedResponse any
	if unmarshalErr := json.Unmarshal([]byte(executionResult.Response), &parsedResponse); unmarshalErr == nil {
		exportEnvelope["response"] = parsedResponse
	} else {
		exportEnvelope["responseText"] = executionResult.Response
	}
	encoded, marshalErr := json.MarshalIndent(exportEnvelope, "", "  ")
	if marshalErr != nil {
		return []byte(executionResult.Response)
	}
	return encoded
}
