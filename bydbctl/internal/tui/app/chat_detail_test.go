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
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

func TestRenderChatWrapsCJKDetailWithoutEllipsis(t *testing.T) {
	detail := "受控目录中，没有发现：精确资源；请确认，资源名称。"
	model := NewModel(Config{})
	model.activityLog = nil
	model.querySession = &session.QuerySession{ChatMessages: []session.ChatMessage{{
		Role:    session.ChatRoleAssistant,
		Content: "查询结果",
		Detail:  detail,
	}}}
	model.chatCursor = 0

	view := stripANSI(model.renderChat(40, 16))
	if strings.Contains(view, "...") {
		t.Fatalf("expected CJK detail to wrap without truncation:\n%s", view)
	}
	formattedLines := formatChatDetailLines(detail, 36)
	for lineIndex, line := range formattedLines {
		formattedLines[lineIndex] = stripANSI(line)
	}
	if formattedDetail := strings.Join(formattedLines, ""); formattedDetail != detail {
		t.Fatalf("expected complete CJK detail, got %q", formattedDetail)
	}
}

func TestFormatChatDetailLinesRendersMarkdown(t *testing.T) {
	content := "你好！\n\n**查询资源：** `meter_vm_cpu_average_used_hour`\n\n- 最近30分钟\n- 限制10条\n\n```bydbql\nSELECT value FROM MEASURE cpu IN g TIME > '-30m' LIMIT 10\n```"
	lines := formatChatDetailLines(content, 60)
	if len(lines) == 0 {
		t.Fatal("expected formatted detail lines")
	}
	joined := strings.Join(lines, "\n")
	if strings.Contains(joined, "**") || strings.Contains(joined, "```") {
		t.Fatalf("expected markdown markers to be rendered, got:\n%s", joined)
	}
	if !strings.Contains(stripANSI(joined), "查询资源") {
		t.Fatalf("expected bold content, got:\n%s", joined)
	}
	if !strings.Contains(stripANSI(joined), "SELECT value FROM MEASURE cpu") {
		t.Fatalf("expected code block content, got:\n%s", joined)
	}
}

func TestFormatChatDetailLinesFormatsToolPlanJSON(t *testing.T) {
	content := "plan:\n{\"resource\":{\"groups\":[\"sw_metadata\"],\"name\":\"_top_n_result\",\"type\":\"MEASURE\"},\"limit\":10}"
	lines := formatChatDetailLines(content, 72)
	if len(lines) == 0 {
		t.Fatal("expected formatted detail lines")
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(stripANSI(joined), "plan") {
		t.Fatalf("expected plan section header, got:\n%s", joined)
	}
	if !strings.Contains(stripANSI(joined), "\"_top_n_result\"") {
		t.Fatalf("expected pretty-printed JSON, got:\n%s", joined)
	}
	if strings.Contains(joined, "plan={") {
		t.Fatalf("expected expanded JSON instead of inline plan=, got:\n%s", joined)
	}
}

func TestFormatChatDetailLinesFormatsQueryArgument(t *testing.T) {
	content := "query:\nSELECT endpoint, MEAN(latency) FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
	lines := formatChatDetailLines(content, 80)
	joined := strings.Join(lines, "\n")
	if !strings.Contains(stripANSI(joined), "bydbql") {
		t.Fatalf("expected bydbql code block label, got:\n%s", joined)
	}
	if !strings.Contains(stripANSI(joined), "SELECT endpoint") {
		t.Fatalf("expected query body, got:\n%s", joined)
	}
}

func TestFormatChatDetailLinesCollapsesBlankLines(t *testing.T) {
	lines := formatChatDetailLines("line one\n\n\n\nline two", 40)
	blankCount := 0
	for _, line := range lines {
		if strings.TrimSpace(stripANSI(line)) == "" {
			blankCount++
		}
	}
	if blankCount > 1 {
		t.Fatalf("expected at most one blank separator, got %d in %#v", blankCount, lines)
	}
}

func TestWrapRunesHandlesCJK(t *testing.T) {
	lines := wrapRunes("你好世界这是一个测试", 8)
	if len(lines) < 2 {
		t.Fatalf("expected wrapped cjk lines, got %#v", lines)
	}
}
