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
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

var (
	topNPattern     = regexp.MustCompile(`(?i)\b(top|highest|lowest|best|worst)\b`)
	topLimitPattern = regexp.MustCompile(`(?i)\b(?:top|highest|lowest|best|worst)\s+(\d+)\b`)
	limitPattern    = regexp.MustCompile(`(?i)\blast\s+(\d+)\s+`)
	timeUnitPattern = regexp.MustCompile(`(?i)\b(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)\b`)
)

// ClassifyIntent derives structured hints from the user goal and TUI slots.
func ClassifyIntent(querySession *session.QuerySession) agent.QueryHints {
	if querySession == nil {
		return agent.QueryHints{}
	}
	hints := agent.QueryHints{
		SlotsPinned: querySession.SlotsPinned,
		UseSlots:    querySession.SlotsPinned,
		AutoMatched: querySession.AutoMatched,
	}
	goal := strings.TrimSpace(querySession.UserGoal)
	if discoveryGoal := strings.TrimSpace(querySession.DiscoveryGoal); discoveryGoal != "" {
		goal = discoveryGoal
	}
	if topNPattern.MatchString(goal) {
		hints.PreferShowTop = true
	}
	if topLimitMatches := topLimitPattern.FindStringSubmatch(goal); len(topLimitMatches) == 2 {
		if limitValue, parseErr := strconv.Atoi(topLimitMatches[1]); parseErr == nil && limitValue > 0 {
			hints.LimitHint = limitValue
		}
	}
	if timeMatches := timeUnitPattern.FindStringSubmatch(goal); len(timeMatches) >= 3 {
		hints.TimeRangeHint = formatRelativeTime(timeMatches[1], timeMatches[2])
	} else if start := strings.TrimSpace(querySession.TimeRange.Start); start != "" {
		hints.TimeRangeHint = start
	}
	if hints.LimitHint == 0 {
		if limitMatches := limitPattern.FindStringSubmatch(goal); len(limitMatches) == 2 {
			if limitValue, parseErr := strconv.Atoi(limitMatches[1]); parseErr == nil && limitValue > 0 {
				hints.LimitHint = limitValue
			}
		}
	}
	return hints
}

func formatRelativeTime(value, unit string) string {
	normalizedUnit := strings.ToLower(strings.TrimSpace(unit))
	switch normalizedUnit {
	case "m", "min", "mins", "minute", "minutes":
		return "-" + value + "m"
	case "h", "hr", "hrs", "hour", "hours":
		return "-" + value + "h"
	case "d", "day", "days":
		return "-" + value + "d"
	case "w", "week", "weeks":
		return "-" + value + "w"
	default:
		return "-" + value + "m"
	}
}
