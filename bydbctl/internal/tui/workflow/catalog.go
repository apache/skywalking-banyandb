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

package workflow

import (
	"strings"

	tuicatalog "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/catalog"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// ResolvedSlots contains workflow-owned slot values after catalog resolution.
type ResolvedSlots struct {
	ResourceType session.ResourceType
	ResourceName string
	Groups       []string
	Goal         string
	TimeRange    session.TimeRange
	SlotsPinned  bool
	AutoMatched  bool
}

// ResolveSessionSlots applies user slots, catalog matching, and safe defaults.
func ResolveSessionSlots(options StartOptions, catalog session.SchemaCatalog) ResolvedSlots {
	goal := strings.TrimSpace(options.Goal)
	resourceType := options.ResourceType
	if resourceType == "" {
		resourceType = inferResourceType(goal)
	}
	resourceName := strings.TrimSpace(options.ResourceName)
	groups := normalizeGroupsIfProvided(options.Groups)
	slotsPinned := options.NameProvided && options.GroupsProvided

	resolved := ResolvedSlots{
		ResourceType: resourceType,
		ResourceName: resourceName,
		Groups:       groups,
		Goal:         goal,
		TimeRange:    applyTimeDefaults(options.TimeRange),
		SlotsPinned:  slotsPinned,
	}
	if slotsPinned {
		return finalizeResolvedSlots(resolved, catalog)
	}
	if len(catalog.Entries) == 0 {
		return finalizeResolvedSlots(resolved, catalog)
	}
	matchType := resourceType
	if !options.TypeProvided && resourceName != "" {
		matchType = ""
	}
	match := matchResourceFromGoal(goal, catalog, matchType, resourceName, groups)
	if !match.Matched {
		return finalizeResolvedSlots(resolved, catalog)
	}
	if resourceName == "" {
		resolved.ResourceName = match.Name
		resolved.AutoMatched = true
	}
	if len(resolved.Groups) == 0 {
		resolved.Groups = []string{match.Group}
		resolved.AutoMatched = true
	}
	if !options.TypeProvided {
		resolved.ResourceType = match.Type
		resolved.AutoMatched = true
	}
	return finalizeResolvedSlots(resolved, catalog)
}

type catalogMatch struct {
	Matched   bool
	Ambiguous bool
	Group     string
	Name      string
	Type      session.ResourceType
	Score     int
}

func matchResourceFromGoal(
	goal string,
	catalog session.SchemaCatalog,
	preferredType session.ResourceType,
	preferredName string,
	preferredGroups []string,
) catalogMatch {
	matchedEntry := tuicatalog.MatchGoal(goal, catalog, preferredType, preferredName, preferredGroups)
	return catalogMatch{
		Matched:   matchedEntry.Matched,
		Ambiguous: matchedEntry.Ambiguous,
		Group:     matchedEntry.Group,
		Name:      matchedEntry.Name,
		Type:      matchedEntry.Type,
		Score:     matchedEntry.Score,
	}
}

const maxPromptCatalogCandidates = 10

// CatalogRankingGoal returns the text used to rank catalog entries for the current turn.
func CatalogRankingGoal(userGoal, turnHint string) string {
	turnHint = strings.TrimSpace(turnHint)
	userGoal = strings.TrimSpace(userGoal)
	if turnHint == "" {
		return userGoal
	}
	if userGoal == "" {
		return turnHint
	}
	return turnHint + " " + userGoal
}

// FindExplicitResourceMention matches a catalog entry named directly in the user text.
func FindExplicitResourceMention(goal string, entries []session.CatalogEntry) *session.CatalogEntry {
	return tuicatalog.FindExplicit(RepairFragmentedQuery(goal), entries)
}

// RankCatalogCandidates returns the highest-scoring catalog entries for a goal.
func RankCatalogCandidates(goal string, entries []session.CatalogEntry, limit int) []session.CatalogEntry {
	return tuicatalog.Rank(goal, entries, limit)
}

// EnsureCatalogEntry includes entry in candidates when missing, keeping the shortest list possible.
func EnsureCatalogEntry(candidates []session.CatalogEntry, entry session.CatalogEntry, limit int) []session.CatalogEntry {
	return tuicatalog.Ensure(candidates, entry, limit)
}

func normalizeGroupsIfProvided(groups []string) []string {
	var normalizedGroups []string
	for _, group := range groups {
		parts := strings.Split(group, ",")
		for _, part := range parts {
			trimmedPart := strings.TrimSpace(part)
			if trimmedPart != "" {
				normalizedGroups = append(normalizedGroups, trimmedPart)
			}
		}
	}
	return normalizedGroups
}

func applyTimeDefaults(timeRange session.TimeRange) session.TimeRange {
	if strings.TrimSpace(timeRange.Start) != "" {
		return timeRange
	}
	return session.TimeRange{
		Start: defaultTimeStart,
		End:   strings.TrimSpace(timeRange.End),
	}
}

func finalizeResolvedSlots(resolved ResolvedSlots, catalog session.SchemaCatalog) ResolvedSlots {
	if len(catalog.Entries) > 0 {
		return resolved
	}
	if strings.TrimSpace(resolved.ResourceName) == "" {
		resolved.ResourceName = defaultResourceName
	}
	if len(resolved.Groups) == 0 {
		resolved.Groups = []string{defaultGroupName}
	}
	return resolved
}
