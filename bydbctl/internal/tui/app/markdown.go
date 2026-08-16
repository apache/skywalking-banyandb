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
	"sync"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
	xansi "github.com/charmbracelet/x/ansi"
)

// Bounds for the width glamour is asked to wrap agent markdown at.
const (
	minMarkdownWidth = 8
	maxMarkdownWidth = 200
	// maxMarkdownCacheEntries bounds the memo shared by the render and scroll passes of one frame.
	maxMarkdownCacheEntries = 64
)

// markdownCacheKey identifies one rendered markdown body at one wrap width.
type markdownCacheKey struct {
	content string
	width   int
}

var (
	markdownMu        sync.Mutex
	markdownRenderers = map[int]*glamour.TermRenderer{}
	markdownCache     = map[markdownCacheKey][]string{}
)

// renderMarkdownDetail renders agent markdown into styled terminal lines that fit width.
//
// A frame renders the same body several times, once to lay the panel out and again to bound the
// scroll offset, so the result is memoized per width rather than re-parsed on every keystroke.
func renderMarkdownDetail(content string, width int) []string {
	wrapWidth := clamp(width, minMarkdownWidth, maxMarkdownWidth)
	markdownMu.Lock()
	defer markdownMu.Unlock()
	key := markdownCacheKey{content: content, width: wrapWidth}
	cached, ok := markdownCache[key]
	if !ok {
		cached = renderMarkdownLines(content, wrapWidth)
		if len(markdownCache) >= maxMarkdownCacheEntries {
			markdownCache = make(map[markdownCacheKey][]string, maxMarkdownCacheEntries)
		}
		markdownCache[key] = cached
	}
	return append([]string(nil), cached...)
}

// renderMarkdownLines converts one markdown body into wrapped, styled lines.
//
// Glamour wraps on spaces, which leaves CJK text unbroken because it holds none, so every line it
// produces is measured and hard-wrapped again before it can overflow the panel.
func renderMarkdownLines(content string, wrapWidth int) []string {
	renderer, rendererErr := markdownRenderer(wrapWidth)
	if rendererErr != nil {
		return wrapRunes(content, wrapWidth)
	}
	rendered, renderErr := renderer.Render(content)
	if renderErr != nil {
		return wrapRunes(content, wrapWidth)
	}
	var lines []string
	for _, renderedLine := range strings.Split(strings.Trim(rendered, "\n"), "\n") {
		styledLine := collapseRedundantSGR(strings.TrimRight(renderedLine, " "))
		if lipgloss.Width(styledLine) <= wrapWidth {
			lines = append(lines, styledLine)
			continue
		}
		lines = append(lines, strings.Split(xansi.Hardwrap(styledLine, wrapWidth, true), "\n")...)
	}
	return lines
}

// markdownRenderer returns the cached glamour renderer for one wrap width.
//
// Building a renderer compiles the syntax-highlighting styles, which is far too slow to repeat per
// frame, and a renderer is bound to the width it was built with.
func markdownRenderer(wrapWidth int) (*glamour.TermRenderer, error) {
	if renderer, ok := markdownRenderers[wrapWidth]; ok {
		return renderer, nil
	}
	renderer, rendererErr := glamour.NewTermRenderer(
		glamour.WithStyles(agentMarkdownStyle()),
		glamour.WithWordWrap(wrapWidth),
		glamour.WithColorProfile(lipgloss.ColorProfile()),
	)
	if rendererErr != nil {
		return nil, rendererErr
	}
	markdownRenderers[wrapWidth] = renderer
	return renderer, nil
}

var sgrPattern = regexp.MustCompile("\x1b\\[[0-9;]*m")

// collapseRedundantSGR drops style sequences that restate the style already in effect.
//
// Glamour re-emits a full style sequence around every word it wraps, which splits one styled
// heading into several spans. The visible text is unchanged, but a plain substring search for that
// heading no longer matches it, and the redundant sequences waste terminal bandwidth.
func collapseRedundantSGR(line string) string {
	matches := sgrPattern.FindAllStringIndex(line, -1)
	if len(matches) < 2 {
		return line
	}
	var builder strings.Builder
	builder.Grow(len(line))
	active := ""
	position := 0
	for matchIndex, match := range matches {
		builder.WriteString(line[position:match[0]])
		position = match[1]
		sequence := line[match[0]:match[1]]
		if sequence == active {
			continue
		}
		if sgrResetsStyle(sequence) && matchIndex+1 < len(matches) &&
			matches[matchIndex+1][0] == match[1] && sgrResetsStyle(line[matches[matchIndex+1][0]:matches[matchIndex+1][1]]) {
			// A reset immediately replaced by another self-resetting style never reaches the screen.
			continue
		}
		active = sequence
		builder.WriteString(sequence)
	}
	builder.WriteString(line[position:])
	return builder.String()
}

// sgrResetsStyle reports whether a sequence clears the previous style before applying its own.
//
// Only such a sequence can absorb a preceding reset without changing what the terminal renders.
func sgrResetsStyle(sequence string) bool {
	parameters := strings.TrimSuffix(strings.TrimPrefix(sequence, "\x1b["), "m")
	if parameters == "" {
		return true
	}
	firstParameter, _, _ := strings.Cut(parameters, ";")
	return firstParameter == "" || strings.Trim(firstParameter, "0") == ""
}
