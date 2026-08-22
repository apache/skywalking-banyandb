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

	"github.com/charmbracelet/lipgloss"
)

func TestRenderMarkdownDetailKeepsEveryLineInsideTheWidth(t *testing.T) {
	const width = 40
	body := "## 查询资源\n\n受控目录中，没有发现：精确资源；请确认，资源名称或刷新 BanyanDB catalog 后重试。\n\n" +
		"- a very long english bullet that has to wrap because it does not fit on one line\n" +
		"- 第二条\n\n```bydbql\nSELECT error_rate FROM MEASURE service_cpm IN sw_metrics TIME > '-30m' LIMIT 10\n```"
	for _, line := range renderMarkdownDetail(body, width) {
		if lineWidth := lipgloss.Width(line); lineWidth > width {
			t.Fatalf("rendered a %d-column line at width %d: %q", lineWidth, width, line)
		}
	}
}

func TestRenderMarkdownDetailStylesHeadingsListsAndCode(t *testing.T) {
	lines := renderMarkdownDetail("## Tags\n\n- trace_id (string)\n\n`service_cpm`", 60)
	joined := strings.Join(lines, "\n")
	if stripANSI(joined) == joined {
		t.Fatalf("expected styled output to carry ANSI sequences:\n%q", joined)
	}
	plain := stripANSI(joined)
	for _, expected := range []string{"Tags", "• trace_id (string)", "service_cpm"} {
		if !strings.Contains(plain, expected) {
			t.Fatalf("expected %q in rendered markdown:\n%s", expected, plain)
		}
	}
	for _, marker := range []string{"##", "`", "- trace_id"} {
		if strings.Contains(plain, marker) {
			t.Fatalf("expected markdown marker %q to be rendered away:\n%s", marker, plain)
		}
	}
}

func TestRenderMarkdownDetailRendersTablesAsAlignedColumns(t *testing.T) {
	lines := renderMarkdownDetail("| Column | Kind |\n| --- | --- |\n| service | tag |\n| value | field |", 60)
	plain := stripANSI(strings.Join(lines, "\n"))
	if strings.Contains(plain, "| --- |") {
		t.Fatalf("expected the table separator row to be rendered, got:\n%s", plain)
	}
	for _, expected := range []string{"Column", "service", "field", "│"} {
		if !strings.Contains(plain, expected) {
			t.Fatalf("expected %q in the rendered table:\n%s", expected, plain)
		}
	}
}

// A heading split across several ANSI spans still reads correctly on screen, but no longer matches a
// plain substring search, which is how both the tests and the panel truncation inspect it.
func TestRenderMarkdownDetailKeepsMultiWordHeadingsSearchable(t *testing.T) {
	lines := renderMarkdownDetail("## Sortable index rules\n\n- start_time", 60)
	if !strings.Contains(strings.Join(lines, "\n"), "Sortable index rules") {
		t.Fatalf("expected the heading to survive as contiguous text:\n%q", lines)
	}
}

func TestRenderMarkdownDetailIsStableAcrossRepeatedRenders(t *testing.T) {
	const body = "## Columns\n\n- `service` tag\n- `value` field"
	first := renderMarkdownDetail(body, 48)
	second := renderMarkdownDetail(body, 48)
	if strings.Join(first, "\n") != strings.Join(second, "\n") {
		t.Fatalf("expected a cached render to be identical:\n%q\n%q", first, second)
	}
	// The cache must hand out copies, or a caller mutating its slice would corrupt later frames.
	first[0] = "mutated"
	if third := renderMarkdownDetail(body, 48); third[0] == "mutated" {
		t.Fatal("the render cache must not expose its stored slice")
	}
}

func TestCollapseRedundantSGRPreservesVisibleText(t *testing.T) {
	styled := "\x1b[;1m\x1b[0m\x1b[;1mSortable index\x1b[0m\x1b[;1m rules\x1b[0m"
	collapsed := collapseRedundantSGR(styled)
	if stripANSI(collapsed) != "Sortable index rules" {
		t.Fatalf("collapsing must not change the visible text, got %q", stripANSI(collapsed))
	}
	if len(collapsed) >= len(styled) {
		t.Fatalf("expected redundant sequences to be dropped: %q", collapsed)
	}
}

func TestCollapseRedundantSGRKeepsDistinctStyles(t *testing.T) {
	styled := "\x1b[31mred\x1b[0m\x1b[32mgreen\x1b[0m"
	if collapsed := collapseRedundantSGR(styled); collapsed != styled {
		t.Fatalf("distinct styles must survive collapsing, got %q", collapsed)
	}
}
