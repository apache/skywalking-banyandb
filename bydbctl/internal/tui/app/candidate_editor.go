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
	"regexp"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
)

// The candidate editor keeps the BYDBQL text area and the derived time and limit slots consistent in
// both directions: a new candidate fills the slots, and editing a slot rewrites the clause in place.

// syncQuerySession mirrors session state into the editor without discarding an in-progress manual edit.
func (m *Model) syncQuerySession() {
	if m.querySession == nil {
		return
	}
	if !m.editingQuery {
		if m.querySession.CandidateSuperseded {
			m.query.SetValue("")
			m.limit.SetValue("")
			m.syncChatCursor()
			return
		}
		if currentCandidate := m.querySession.CurrentCandidate(); currentCandidate != nil {
			m.setQueryValue(currentCandidate.Query)
		}
	}
	if strings.TrimSpace(m.querySession.SchemaSnapshot.Name) != "" {
		m.cacheSchema(m.querySession.SchemaSnapshot)
		for _, cachedSchema := range m.querySession.Schemas {
			m.cacheSchema(cachedSchema)
		}
		m.selectedSchema = m.querySession.SchemaSnapshot
	}
	m.syncChatCursor()
}

// setQueryValue replaces the editor contents and refreshes the derived time and limit slots.
func (m *Model) setQueryValue(query string) {
	if m.query.Value() == query {
		return
	}
	m.query.SetValue(query)
	m.limit.SetValue(extractCandidateLimit(query))
	start, end := extractCandidateTimeRange(query)
	m.start.SetValue(start)
	m.end.SetValue(end)
}

var (
	candidateLimitPattern = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	candidateTimePattern  = regexp.MustCompile(`(?i)\bTIME\s+(?:BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'|([><]=?)\s+'([^']+)')`)
)

func extractCandidateLimit(query string) string {
	matches := candidateLimitPattern.FindStringSubmatch(query)
	if len(matches) != 2 {
		return ""
	}
	return matches[1]
}

func extractCandidateTimeRange(query string) (string, string) {
	matches := candidateTimePattern.FindStringSubmatch(query)
	if len(matches) != 5 {
		return "", ""
	}
	if matches[1] != "" || matches[2] != "" {
		return matches[1], matches[2]
	}
	if strings.HasPrefix(matches[3], ">") {
		return matches[4], ""
	}
	return "", matches[4]
}

func (m *Model) applyCandidateLimit() {
	query := strings.TrimSpace(m.query.Value())
	if query == "" {
		return
	}
	limitValue := strings.TrimSpace(m.limit.Value())
	if candidateLimitPattern.MatchString(query) {
		if limitValue == "" {
			m.query.SetValue(strings.TrimSpace(candidateLimitPattern.ReplaceAllString(query, "")))
		} else {
			m.query.SetValue(candidateLimitPattern.ReplaceAllString(query, "LIMIT "+limitValue))
		}
		return
	}
	if limitValue != "" {
		m.query.SetValue(query + " LIMIT " + limitValue)
	}
}

func (m *Model) applyCandidateTimeRange() {
	query := strings.TrimSpace(m.query.Value())
	if query == "" || !candidateTimePattern.MatchString(query) {
		return
	}
	start := strings.TrimSpace(m.start.Value())
	end := strings.TrimSpace(m.end.Value())
	if start == "" && end == "" {
		return
	}
	timeClause := ""
	switch {
	case start != "" && end != "":
		timeClause = fmt.Sprintf("TIME BETWEEN '%s' AND '%s'", start, end)
	case start != "":
		timeClause = fmt.Sprintf("TIME > '%s'", start)
	default:
		timeClause = fmt.Sprintf("TIME < '%s'", end)
	}
	m.query.SetValue(candidateTimePattern.ReplaceAllString(query, timeClause))
}

func newTextInput(value, placeholder string) textinput.Model {
	input := textinput.New()
	input.Placeholder = placeholder
	input.SetValue(value)
	input.Prompt = ""
	input.Width = 24
	return input
}
