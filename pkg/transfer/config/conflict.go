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

package config

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Schema conflict policies of an import.
const (
	// ConflictPolicyError rejects the import on a schema conflict.
	ConflictPolicyError = "error"
	// ConflictPolicyIgnore keeps the target schema and imports the data anyway.
	ConflictPolicyIgnore = "ignore"
	// ConflictPolicyOverwrite replaces the target schema with the artifact's.
	ConflictPolicyOverwrite = "overwrite"
)

// ConflictMatchKinds are the schema kinds a conflict rule can match.
var ConflictMatchKinds = []string{"group", "stream", "measure", "trace", "property", "indexRule", "indexRuleBinding", "topNAggregation"}

// ConflictRule overrides the schema conflict policy of the resources its Match names:
// `<kind>/<group>` (every resource of that kind in the group, or the group itself for the
// group kind) or `<kind>/<group>/<name>`.
type ConflictRule struct {
	Match  string `json:"match"`
	Policy string `json:"policy"`
}

// ParseConflictPolicy accepts exactly error, ignore or overwrite.
func ParseConflictPolicy(raw string) (string, error) {
	switch p := strings.TrimSpace(raw); p {
	case ConflictPolicyError, ConflictPolicyIgnore, ConflictPolicyOverwrite:
		return p, nil
	default:
		return "", fmt.Errorf("unsupported schema conflict policy %q (error|ignore|overwrite)", raw)
	}
}

// ValidateConflictMatch checks `<kind>/<group>[/<name>]`: the kind is one of
// ConflictMatchKinds (case-sensitive), every segment is non-empty, and the group kind only
// takes the two-segment form. Wildcards are not supported.
func ValidateConflictMatch(match string) error {
	parts := strings.Split(match, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return fmt.Errorf("match %q must be <kind>/<group>[/<name>]", match)
	}
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("match %q has an empty segment", match)
		}
	}
	if !slices.Contains(ConflictMatchKinds, parts[0]) {
		return fmt.Errorf("match %q: unknown kind %q (%s)", match, parts[0], strings.Join(ConflictMatchKinds, "|"))
	}
	if parts[0] == "group" && len(parts) == 3 {
		return fmt.Errorf("match %q: the group kind only takes group/<group>", match)
	}
	return nil
}

// decodeConflictRules decodes the plan.yaml list of {match, policy} objects, rejecting
// unknown fields and naming the index of a bad rule.
func decodeConflictRules(raw any) ([]ConflictRule, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("must be a list of {match, policy}, got %s", describe(raw))
	}
	rules := make([]ConflictRule, len(list))
	for i, item := range list {
		if err := decodeConflictRule(item, &rules[i]); err != nil {
			return nil, fmt.Errorf("[%d]: %w", i, err)
		}
	}
	return rules, nil
}

func decodeConflictRule(item any, rule *ConflictRule) error {
	m, ok := item.(map[string]any)
	if !ok {
		return fmt.Errorf("must be a {match, policy} mapping, got %s", describe(item))
	}
	// Keys are matched exactly, like the rest of plan.yaml: encoding/json would accept
	// "Match" or "POLICY" and silently fold a "match"/"Match" pair into one value.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		var field *string
		switch k {
		case "match":
			field = &rule.Match
		case "policy":
			field = &rule.Policy
		default:
			return fmt.Errorf("must be a {match, policy} mapping: unknown field %q", k)
		}
		s, isString := m[k].(string)
		if !isString {
			return fmt.Errorf("%s must be a string, got %s", k, describe(m[k]))
		}
		*field = s
	}
	err := ValidateConflictMatch(rule.Match)
	if err != nil {
		return err
	}
	rule.Policy, err = ParseConflictPolicy(rule.Policy)
	return err
}

// ParseCount parses a plain decimal integer >= 1.
func ParseCount(raw string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 1 {
		return 0, fmt.Errorf("count must be an integer >= 1, got %q", raw)
	}
	return n, nil
}
