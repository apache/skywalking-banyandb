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
	"slices"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuitext"
)

const (
	defaultCandidateLimit = 10
	minimumMatchScore     = 12
	minimumScoreMargin    = 4
)

var resourceMentionPattern = regexp.MustCompile(`(?i)[a-z][a-z0-9_]{5,}`)

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
	scoring := newScoringContext(goal, preferredType, groupCount)
	ranked := make([]rankedEntry, 0, len(entries))
	for _, entry := range entries {
		if shouldSkip(entry) {
			continue
		}
		ranked = append(ranked, rankedEntry{
			entry: entry,
			score: scoring.score(entry),
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

// scoringContext holds everything derived from the goal alone.
//
// A catalog can hold thousands of entries and is re-ranked on every turn, so the goal is tokenized
// and its keyword hints resolved once here rather than per entry.
type scoringContext struct {
	goalTokens    map[string]struct{}
	typeScores    map[session.ResourceType]int
	goal          string
	preferredType session.ResourceType
	groupCount    int
	metricHint    bool
	latencyHint   bool
	endpointHint  bool
	cpuHint       bool
}

func newScoringContext(goal string, preferredType session.ResourceType, groupCount int) scoringContext {
	normalizedGoal := strings.ToLower(goal)
	goalTokens := make(map[string]struct{})
	for _, token := range tokens(normalizedGoal) {
		goalTokens[token] = struct{}{}
	}
	return scoringContext{
		goal:          normalizedGoal,
		goalTokens:    goalTokens,
		preferredType: preferredType,
		groupCount:    groupCount,
		typeScores:    goalTypeScores(normalizedGoal),
		metricHint:    containsAny(normalizedGoal, "metric", "endpoint", "latency", "指标", "端点", "延迟"),
		latencyHint:   containsAny(normalizedGoal, "slow", "latency", "慢", "延迟"),
		endpointHint:  containsAny(normalizedGoal, "endpoint", "payment", "端点", "支付"),
		cpuHint:       containsAny(normalizedGoal, "cpu", "处理器"),
	}
}

func (scoring scoringContext) score(entry session.CatalogEntry) int {
	entryName := strings.ToLower(entry.Name)
	entryGroup := strings.ToLower(entry.Group)
	score := 0
	for _, nameToken := range tokens(entryName) {
		if _, matched := scoring.goalTokens[nameToken]; matched {
			score += 12
		}
	}
	for _, groupToken := range tokens(entryGroup) {
		if _, matched := scoring.goalTokens[groupToken]; matched {
			score += 6
		}
	}
	if strings.Contains(scoring.goal, entryName) {
		score += 20
	}
	if strings.Contains(scoring.goal, entryGroup) {
		score += 10
	}
	if entry.Type == scoring.preferredType {
		score += 8
	}
	if entry.Group == "default" && scoring.groupCount > 1 {
		score -= 8
	}
	if scoring.metricHint && strings.Contains(entryGroup, "metric") {
		score += 6
	}
	score += scoring.typeScores[entry.Type]
	if scoring.latencyHint && strings.Contains(entryName, "latency") {
		score += 8
	}
	if scoring.endpointHint && strings.Contains(entryName, "endpoint") {
		score += 8
	}
	if scoring.cpuHint && strings.Contains(entryName, "cpu") {
		score += 12
	}
	return score
}

// goalTypeScores resolves the resource-type keyword bonus for the goal once per ranking pass.
func goalTypeScores(goal string) map[session.ResourceType]int {
	scores := make(map[session.ResourceType]int, len(typeKeywordBonuses))
	for _, bonus := range typeKeywordBonuses {
		if containsAny(goal, bonus.keywords...) {
			scores[bonus.resourceType] = bonus.score
		}
	}
	return scores
}

var typeKeywordBonuses = []struct {
	resourceType session.ResourceType
	keywords     []string
	score        int
}{
	{resourceType: session.ResourceTypeStream, keywords: []string{"log", "stream", "日志"}, score: 6},
	{resourceType: session.ResourceTypeTrace, keywords: []string{"trace", "span", "链路"}, score: 6},
	{resourceType: session.ResourceTypeProperty, keywords: []string{"property", "属性"}, score: 6},
	{resourceType: session.ResourceTypeTopN, keywords: []string{"top", "最高", "最低"}, score: 6},
	{resourceType: session.ResourceTypeMeasure, keywords: []string{"measure", "metric", "latency", "指标", "延迟"}, score: 4},
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
		if len(preferredGroups) > 0 && !slices.Contains(preferredGroups, entry.Group) {
			continue
		}
		if preferredType != "" && entry.Type != preferredType {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

// tokens splits value into its distinct lowercase alphanumeric runs.
//
// Ranking calls this once per catalog entry, so it scans directly instead of running a regular
// expression: every other byte, `_` and CJK text included, is already a separator.
func tokens(value string) []string {
	lowered := strings.ToLower(value)
	var collected []string
	tokenStart := -1
	for idx := 0; idx < len(lowered); idx++ {
		if isTokenByte(lowered[idx]) {
			if tokenStart < 0 {
				tokenStart = idx
			}
			continue
		}
		if tokenStart >= 0 {
			collected = append(collected, lowered[tokenStart:idx])
			tokenStart = -1
		}
	}
	if tokenStart >= 0 {
		collected = append(collected, lowered[tokenStart:])
	}
	return tuitext.Compact(collected)
}

func isTokenByte(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9')
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
