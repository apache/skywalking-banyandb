// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package app

import (
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// Typing `@term` in the composer opens a schema search over the discovered catalog. The highlighted
// entry is previewed in the evidence panel, and Enter turns it into the turn's pinned reference.

func (m *Model) updateSchemaSearch() {
	if m.message.Value() != m.schemaSearchValue {
		m.schemaSearchValue = m.message.Value()
		m.schemaSearchDismissed = false
	}
	if !m.schemaSearchOpen() {
		// Only a preview is retracted here. This runs on every composer message, cursor blinks
		// included, so clearing a pinned schema would drop a describe answer a blink after it lands.
		if m.evidenceMode == evidenceModeSchema {
			m.evidenceMode = evidenceModeData
		}
		m.schemaSearchCursor = 0
		return
	}
	entries := m.schemaSearchEntries()
	if len(entries) == 0 {
		m.schemaSearchCursor = 0
		return
	}
	if m.schemaSearchCursor >= len(entries) {
		m.schemaSearchCursor = len(entries) - 1
	}
	if m.schemaSearchCursor < 0 {
		m.schemaSearchCursor = 0
	}
	m.previewSchemaSearchEntry(entries[m.schemaSearchCursor])
}

func (m Model) schemaSearchOpen() bool {
	_, ok := m.schemaSearchTerm()
	return ok && !m.schemaSearchDismissed
}

func (m Model) schemaSearchTerm() (string, bool) {
	messageValue := m.message.Value()
	atIndex := strings.LastIndex(messageValue, "@")
	if atIndex < 0 {
		return "", false
	}
	term := messageValue[atIndex+1:]
	if strings.ContainsAny(term, " \t\n") {
		return "", false
	}
	return strings.ToLower(strings.TrimSpace(term)), true
}

func (m Model) schemaSearchEntries() []session.CatalogEntry {
	term, open := m.schemaSearchTerm()
	if !open {
		return nil
	}
	if normalizeSchemaSearchTerm(term) == "" && term != "" {
		return nil
	}
	results := make([]schemaSearchResult, 0, len(m.catalog.catalog.Entries))
	for _, entry := range m.catalog.catalog.Entries {
		score, matches := schemaSearchScore(entry, term)
		if !matches {
			continue
		}
		results = append(results, schemaSearchResult{entry: entry, score: score})
	}
	sort.Slice(results, func(leftIndex, rightIndex int) bool {
		leftResult := results[leftIndex]
		rightResult := results[rightIndex]
		if leftResult.score != rightResult.score {
			return leftResult.score < rightResult.score
		}
		leftEntry := leftResult.entry
		rightEntry := rightResult.entry
		if leftEntry.Group != rightEntry.Group {
			return leftEntry.Group < rightEntry.Group
		}
		if leftEntry.Name != rightEntry.Name {
			return leftEntry.Name < rightEntry.Name
		}
		return leftEntry.Type < rightEntry.Type
	})
	entries := make([]session.CatalogEntry, len(results))
	for index, result := range results {
		entries[index] = result.entry
	}
	return entries
}

func schemaSearchScore(entry session.CatalogEntry, term string) (int, bool) {
	groupTerm, resourceTerm, hasResourceTerm := strings.Cut(term, "/")
	if hasResourceTerm {
		return schemaSearchPathScore(entry, groupTerm, resourceTerm)
	}
	normalizedTerm := normalizeSchemaSearchTerm(term)
	if normalizedTerm == "" {
		return 0, true
	}
	if score, matches := schemaSearchFieldScore(entry.Name, normalizedTerm, schemaSearchNameScore); matches {
		return score, true
	}
	if score, matches := schemaSearchFieldScore(entry.Group, normalizedTerm, schemaSearchGroupScore); matches {
		return score, true
	}
	return schemaSearchFieldScore(entry.Type.String(), normalizedTerm, schemaSearchTypeScore)
}

func schemaSearchPathScore(entry session.CatalogEntry, groupTerm, resourceTerm string) (int, bool) {
	normalizedGroupTerm := normalizeSchemaSearchTerm(groupTerm)
	if normalizedGroupTerm == "" {
		return 0, false
	}
	groupScore, matchesGroup := schemaSearchFieldScore(entry.Group, normalizedGroupTerm, schemaSearchNameScore)
	if !matchesGroup {
		return 0, false
	}
	normalizedResourceTerm := normalizeSchemaSearchTerm(resourceTerm)
	if normalizedResourceTerm == "" {
		return groupScore, true
	}
	resourceScore, matchesResource := schemaSearchFieldScore(entry.Name, normalizedResourceTerm, schemaSearchNameScore)
	if !matchesResource {
		return 0, false
	}
	return groupScore + resourceScore, true
}

func schemaSearchFieldScore(value, normalizedTerm string, baseScore int) (int, bool) {
	normalizedValue := normalizeSchemaSearchTerm(value)
	if normalizedValue == normalizedTerm {
		return baseScore, true
	}
	tokens := schemaSearchTokens(value)
	bestScore := 0
	matches := false
	recordMatch := func(score int) {
		if !matches || score < bestScore {
			bestScore = score
			matches = true
		}
	}
	for tokenIndex, token := range tokens {
		switch {
		case token == normalizedTerm:
			recordMatch(baseScore + schemaSearchExactTokenScore)
		case strings.HasPrefix(token, normalizedTerm):
			recordMatch(baseScore + schemaSearchPrefixScore)
		case strings.Contains(token, normalizedTerm):
			recordMatch(baseScore + schemaSearchSubstringScore)
		}
		var sequence strings.Builder
		for sequenceEnd := tokenIndex; sequenceEnd < len(tokens); sequenceEnd++ {
			sequence.WriteString(tokens[sequenceEnd])
			if sequence.Len() > len(normalizedTerm) {
				break
			}
			if sequence.String() != normalizedTerm {
				continue
			}
			matchScore := baseScore + schemaSearchSubstringScore
			if tokenIndex == 0 {
				matchScore = baseScore + schemaSearchPrefixScore
			}
			recordMatch(matchScore)
			break
		}
	}
	return bestScore, matches
}

func normalizeSchemaSearchTerm(value string) string {
	return strings.Join(schemaSearchTokens(value), "")
}

func schemaSearchTokens(value string) []string {
	tokens := make([]string, 0, 1)
	var normalized strings.Builder
	appendToken := func() {
		if normalized.Len() == 0 {
			return
		}
		tokens = append(tokens, normalized.String())
		normalized.Reset()
	}
	for _, valueRune := range strings.ToLower(value) {
		if (valueRune >= 'a' && valueRune <= 'z') || (valueRune >= '0' && valueRune <= '9') {
			normalized.WriteRune(valueRune)
			continue
		}
		appendToken()
	}
	appendToken()
	return tokens
}

func (m *Model) moveSchemaSearchCursor(delta int) {
	entries := m.schemaSearchEntries()
	if len(entries) == 0 || delta == 0 {
		return
	}
	m.schemaSearchCursor += delta
	if m.schemaSearchCursor < 0 {
		m.schemaSearchCursor = 0
	}
	if m.schemaSearchCursor >= len(entries) {
		m.schemaSearchCursor = len(entries) - 1
	}
	m.previewSchemaSearchEntry(entries[m.schemaSearchCursor])
}

func (m *Model) previewSchemaSearchEntry(entry session.CatalogEntry) {
	// A preview replaces a pinned schema, since the user is now steering the panel by hand.
	m.evidenceMode = evidenceModeSchema
	if sameSchemaResource(m.selectedSchema, entry) {
		return
	}
	if cachedSchema, ok := m.cachedSchema(entry); ok {
		m.selectedSchema = cachedSchema
		return
	}
	if m.querySession != nil {
		if cachedSchema, ok := m.querySession.CachedSchema(entry.Type, entry.Name, []string{entry.Group}); ok {
			m.selectedSchema = cachedSchema
			return
		}
	}
	m.selectedSchema = session.SchemaSnapshot{
		Type:   entry.Type,
		Name:   entry.Name,
		Groups: []string{entry.Group},
	}
}

func (m Model) cachedSchema(entry session.CatalogEntry) (session.SchemaSnapshot, bool) {
	if m.schemaCache == nil {
		return session.SchemaSnapshot{}, false
	}
	snapshot, ok := m.schemaCache[schemaEntryKey(entry)]
	return snapshot, ok
}

func (m *Model) cacheSchema(snapshot session.SchemaSnapshot) {
	if strings.TrimSpace(snapshot.Name) == "" {
		return
	}
	if m.schemaCache == nil {
		m.schemaCache = make(map[string]session.SchemaSnapshot)
	}
	key := session.SchemaKey(snapshot.Type, snapshot.Name, snapshot.Groups)
	m.schemaCache[key] = snapshot
}

func (m *Model) clearSchemaLoad(entry session.CatalogEntry) {
	if m.schemaLoads == nil {
		return
	}
	delete(m.schemaLoads, schemaEntryKey(entry))
}

func (m Model) isCurrentSchemaSearchEntry(entry session.CatalogEntry) bool {
	entries := m.schemaSearchEntries()
	if m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return false
	}
	return entries[m.schemaSearchCursor] == entry
}

func (m *Model) loadSchemaDetailForSearch() tea.Cmd {
	if !m.schemaSearchOpen() {
		return nil
	}
	entries := m.schemaSearchEntries()
	if m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return nil
	}
	entry := entries[m.schemaSearchCursor]
	if _, ok := m.cachedSchema(entry); ok {
		return nil
	}
	key := schemaEntryKey(entry)
	if m.schemaLoads == nil {
		m.schemaLoads = make(map[string]struct{})
	}
	if _, loading := m.schemaLoads[key]; loading {
		return nil
	}
	m.schemaLoads[key] = struct{}{}
	return m.loadSchemaDetailCmd(entry)
}

func sameSchemaResource(snapshot session.SchemaSnapshot, entry session.CatalogEntry) bool {
	return snapshot.Type == entry.Type && snapshot.Name == entry.Name && len(snapshot.Groups) == 1 && snapshot.Groups[0] == entry.Group
}

func schemaEntryKey(entry session.CatalogEntry) string {
	return session.SchemaKey(entry.Type, entry.Name, []string{entry.Group})
}

func (m *Model) insertSchemaReference() {
	entries := m.schemaSearchEntries()
	if len(entries) == 0 || m.schemaSearchCursor < 0 || m.schemaSearchCursor >= len(entries) {
		return
	}
	entry := entries[m.schemaSearchCursor]
	term, open := m.schemaSearchTerm()
	if !open {
		return
	}
	messageValue := m.message.Value()
	referenceStart := strings.LastIndex(messageValue, "@")
	if referenceStart < 0 {
		return
	}
	chip := "@" + entry.Group + "/" + entry.Name
	m.message.SetValue(messageValue[:referenceStart] + chip + messageValue[referenceStart+len(term)+1:])
	m.schemaSearchValue = m.message.Value()
	m.schemaSearchDismissed = true
	m.composerReference = &session.CatalogEntry{Group: entry.Group, Type: entry.Type, Name: entry.Name}
	m.previewSchemaSearchEntry(entry)
}
