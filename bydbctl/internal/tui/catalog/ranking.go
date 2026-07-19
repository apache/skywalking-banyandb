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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package catalog provides deterministic BanyanDB resource ranking and matching.
package catalog

import (
	"regexp"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

const (
	defaultCandidateLimit = 10
	minimumMatchScore     = 12
	minimumScoreMargin    = 4
)

var (
	tokenPattern           = regexp.MustCompile(`[a-z0-9]+`)
	resourceMentionPattern = regexp.MustCompile(`(?i)[a-z][a-z0-9_]{5,}`)
)

// Match is a confidence-aware catalog resolution result.
type Match struct {
	Group     string
	Name      string
	Type      session.ResourceType
	Score     int
	Matched   bool
	Ambiguous bool
}

type rankedEntry struct {
	entry session.CatalogEntry
	score int
}

// InferResourceType derives a weak resource-type preference from a user goal.
func InferResourceType(goal string) session.ResourceType {
	normalizedGoal := strings.ToLower(goal)
	switch {
	case strings.Contains(normalizedGoal, "trace") || strings.Contains(normalizedGoal, "链路"):
		return session.ResourceTypeTrace
	case strings.Contains(normalizedGoal, "property") || strings.Contains(normalizedGoal, "属性"):
		return session.ResourceTypeProperty
	case strings.Contains(normalizedGoal, "stream") || strings.Contains(normalizedGoal, "log") || strings.Contains(normalizedGoal, "日志"):
		return session.ResourceTypeStream
	case strings.Contains(normalizedGoal, "top") || strings.Contains(normalizedGoal, "最高") || strings.Contains(normalizedGoal, "最低"):
		return session.ResourceTypeTopN
	default:
		return session.ResourceTypeMeasure
	}
}

// MatchGoal resolves a catalog entry only when the best candidate is sufficiently confident and unambiguous.
func MatchGoal(
	goal string,
	schemaCatalog session.SchemaCatalog,
	preferredType session.ResourceType,
	preferredName string,
	preferredGroups []string,
) Match {
	entries := filterEntries(schemaCatalog.Entries, preferredType, preferredName, preferredGroups)
	if len(entries) == 0 {
		return Match{}
	}
	if preferredName != "" {
		return matchExactPreferredEntry(entries, len(preferredGroups) > 0)
	}
	ranked := rank(goal, entries, InferResourceType(goal), uniqueGroupCount(schemaCatalog.Entries))
	if len(ranked) == 0 || ranked[0].score < minimumMatchScore {
		return Match{}
	}
	result := Match{
		Group:   ranked[0].entry.Group,
		Name:    ranked[0].entry.Name,
		Type:    ranked[0].entry.Type,
		Score:   ranked[0].score,
		Matched: true,
	}
	if len(ranked) > 1 && ranked[0].score-ranked[1].score < minimumScoreMargin {
		result.Matched = false
		result.Ambiguous = true
	}
	return result
}

func matchExactPreferredEntry(entries []session.CatalogEntry, groupsProvided bool) Match {
	selectedEntry := entries[0]
	for _, entry := range entries[1:] {
		if entry.Type != selectedEntry.Type || (!groupsProvided && entry.Group != selectedEntry.Group) {
			return Match{Ambiguous: true}
		}
	}
	return Match{
		Group:   selectedEntry.Group,
		Name:    selectedEntry.Name,
		Type:    selectedEntry.Type,
		Score:   100,
		Matched: true,
	}
}

// Rank returns catalog candidates ordered by relevance to the current user goal.
func Rank(goal string, entries []session.CatalogEntry, limit int) []session.CatalogEntry {
	if limit <= 0 {
		limit = defaultCandidateLimit
	}
	ranked := rank(goal, entries, InferResourceType(goal), uniqueGroupCount(entries))
	candidates := make([]session.CatalogEntry, 0, min(limit, len(ranked)))
	for _, rankedItem := range ranked {
		if rankedItem.score <= 0 && len(candidates) > 0 {
			break
		}
		candidates = append(candidates, rankedItem.entry)
		if len(candidates) >= limit {
			break
		}
	}
	return candidates
}

// FindExplicit matches only exact resource identifiers, including identifiers fragmented by whitespace.
func FindExplicit(goal string, entries []session.CatalogEntry) *session.CatalogEntry {
	repairedGoal := strings.ToLower(strings.TrimSpace(goal))
	compactGoal := strings.ReplaceAll(repairedGoal, " ", "")
	mentions := resourceMentionPattern.FindAllString(repairedGoal, -1)
	mentions = append(mentions, resourceMentionPattern.FindAllString(compactGoal, -1)...)
	var matchedEntry *session.CatalogEntry
	for _, mention := range mentions {
		mention = strings.ToLower(strings.TrimSpace(mention))
		if len(mention) < 8 || isGenericMention(mention) {
			continue
		}
		mentionCompact := strings.ReplaceAll(mention, "_", "")
		for entryIndex := range entries {
			entry := &entries[entryIndex]
			entryName := strings.ToLower(strings.TrimSpace(entry.Name))
			entryCompact := strings.ReplaceAll(entryName, "_", "")
			if mention != entryName && mentionCompact != entryCompact {
				continue
			}
			if matchedEntry != nil && !sameEntry(*matchedEntry, *entry) {
				return nil
			}
			matchedEntry = entry
		}
	}
	return matchedEntry
}

// Ensure includes entry in candidates when missing, keeping the requested maximum length.
func Ensure(candidates []session.CatalogEntry, entry session.CatalogEntry, limit int) []session.CatalogEntry {
	if limit <= 0 {
		limit = defaultCandidateLimit
	}
	for _, candidate := range candidates {
		if sameEntry(candidate, entry) {
			return candidates
		}
	}
	updated := append([]session.CatalogEntry{entry}, candidates...)
	if len(updated) > limit {
		updated = updated[:limit]
	}
	return updated
}

func rank(goal string, entries []session.CatalogEntry, preferredType session.ResourceType, groupCount int) []rankedEntry {
	goalTokens := tokens(goal)
	ranked := make([]rankedEntry, 0, len(entries))
	for _, entry := range entries {
		if shouldSkip(entry) {
			continue
		}
		ranked = append(ranked, rankedEntry{
			entry: entry,
			score: scoreEntry(goal, goalTokens, entry, preferredType, groupCount),
		})
	}
	sort.SliceStable(ranked, func(leftIndex, rightIndex int) bool {
		if ranked[leftIndex].score != ranked[rightIndex].score {
			return ranked[leftIndex].score > ranked[rightIndex].score
		}
		if ranked[leftIndex].entry.Group != ranked[rightIndex].entry.Group {
			return ranked[leftIndex].entry.Group < ranked[rightIndex].entry.Group
		}
		if ranked[leftIndex].entry.Type != ranked[rightIndex].entry.Type {
			return ranked[leftIndex].entry.Type < ranked[rightIndex].entry.Type
		}
		return ranked[leftIndex].entry.Name < ranked[rightIndex].entry.Name
	})
	return ranked
}

func scoreEntry(goal string, goalTokens []string, entry session.CatalogEntry, preferredType session.ResourceType, groupCount int) int {
	normalizedGoal := strings.ToLower(goal)
	entryName := strings.ToLower(entry.Name)
	entryGroup := strings.ToLower(entry.Group)
	score := 0
	for _, goalToken := range goalTokens {
		for _, nameToken := range tokens(entryName) {
			if goalToken == nameToken {
				score += 12
			}
		}
		for _, groupToken := range tokens(entryGroup) {
			if goalToken == groupToken {
				score += 6
			}
		}
	}
	if strings.Contains(normalizedGoal, entryName) {
		score += 20
	}
	if strings.Contains(normalizedGoal, entryGroup) {
		score += 10
	}
	if entry.Type == preferredType {
		score += 8
	}
	if entry.Group == "default" && groupCount > 1 {
		score -= 8
	}
	if strings.Contains(entryGroup, "metric") && containsAny(normalizedGoal, "metric", "endpoint", "latency", "指标", "端点", "延迟") {
		score += 6
	}
	score += typeKeywordScore(normalizedGoal, entry.Type)
	if strings.Contains(entryName, "latency") && containsAny(normalizedGoal, "slow", "latency", "慢", "延迟") {
		score += 8
	}
	if strings.Contains(entryName, "endpoint") && containsAny(normalizedGoal, "endpoint", "payment", "端点", "支付") {
		score += 8
	}
	if strings.Contains(entryName, "cpu") && containsAny(normalizedGoal, "cpu", "处理器") {
		score += 12
	}
	return score
}

func typeKeywordScore(goal string, resourceType session.ResourceType) int {
	switch resourceType {
	case session.ResourceTypeStream:
		if containsAny(goal, "log", "stream", "日志") {
			return 6
		}
	case session.ResourceTypeTrace:
		if containsAny(goal, "trace", "span", "链路") {
			return 6
		}
	case session.ResourceTypeProperty:
		if containsAny(goal, "property", "属性") {
			return 6
		}
	case session.ResourceTypeTopN:
		if containsAny(goal, "top", "最高", "最低") {
			return 6
		}
	case session.ResourceTypeMeasure:
		if containsAny(goal, "measure", "metric", "latency", "指标", "延迟") {
			return 4
		}
	}
	return 0
}

func filterEntries(entries []session.CatalogEntry, preferredType session.ResourceType, preferredName string, preferredGroups []string) []session.CatalogEntry {
	filtered := make([]session.CatalogEntry, 0, len(entries))
	for _, entry := range entries {
		if shouldSkip(entry) {
			continue
		}
		if preferredName != "" && entry.Name != preferredName {
			continue
		}
		if len(preferredGroups) > 0 && !containsString(preferredGroups, entry.Group) {
			continue
		}
		if preferredType != "" && entry.Type != preferredType {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func tokens(value string) []string {
	normalizedValue := strings.ToLower(strings.ReplaceAll(value, "_", " "))
	return compactStrings(tokenPattern.FindAllString(normalizedValue, -1))
}

func uniqueGroupCount(entries []session.CatalogEntry) int {
	groups := make(map[string]struct{})
	for _, entry := range entries {
		if !shouldSkip(entry) {
			groups[entry.Group] = struct{}{}
		}
	}
	return len(groups)
}

func shouldSkip(entry session.CatalogEntry) bool {
	return strings.HasPrefix(entry.Group, "_") || strings.TrimSpace(entry.Name) == ""
}

func isGenericMention(mention string) bool {
	switch mention {
	case "minute", "minutes", "metrics", "metric", "schema", "schemas", "groups", "group", "query", "queries":
		return true
	default:
		return false
	}
}

func sameEntry(left, right session.CatalogEntry) bool {
	return left.Type == right.Type && left.Name == right.Name && left.Group == right.Group
}

func containsAny(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if strings.Contains(value, fragment) {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func compactStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	compactedValues := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		compactedValues = append(compactedValues, value)
	}
	return compactedValues
}
