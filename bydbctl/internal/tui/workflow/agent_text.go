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
	"strings"
	"unicode"
)

// Some providers stream text with spaces inserted inside identifiers and keywords ("ME ASURE",
// "service _name"). The helpers below repair that for display and for candidate extraction, leaving
// text outside backticks conservatively alone.

var fragmentedTimeRangePattern = regexp.MustCompile(`'-\s*(\d+)\s*m\s*'`)

var fragmentedTokenReplacements = []struct {
	old string
	new string
}{
	{old: "by db ql", new: "bydbql"},
	{old: "b yd b ql", new: "bydbql"},
	{old: "SH OW", new: "SHOW"},
	{old: "A GG REG ATE", new: "AGGREGATE"},
	{old: "AGGREGATE BY AV G", new: "AGGREGATE BY AVG"},
	{old: "AGGREGATE BY MA X", new: "AGGREGATE BY MAX"},
	{old: "AGGREGATE BY MI N", new: "AGGREGATE BY MIN"},
	{old: "AV G", new: "AVG"},
	{old: "MA X", new: "MAX"},
	{old: "MI N", new: "MIN"},
	{old: "TOP text 10", new: "TOP 10"},
	{old: "TOP text ", new: "TOP "},
	{old: "ME ASURE", new: "MEASURE"},
	{old: "ME AS URE", new: "MEASURE"},
	{old: "ST REAM", new: "STREAM"},
	{old: "TR ACE", new: "TRACE"},
	{old: "PROP ERTY", new: "PROPERTY"},
	{old: "SER VICE", new: "SERVICE"},
	{old: "LI MIT", new: "LIMIT"},
	{old: "GRO UP", new: "GROUP"},
	{old: "OR DER", new: "ORDER"},
	{old: "WHE RE", new: "WHERE"},
	{old: "service _", new: "service_"},
	{old: "service_end point_l atency", new: "service_endpoint_latency"},
	{old: "endpoint _", new: "endpoint_"},
	{old: "_ ", new: "_"},
	{old: " - ", new: "-"},
	{old: "text 10 text", new: "10"},
	{old: "text 100 text", new: "100"},
	{old: "text SELECT", new: "SELECT"},
	{old: "sche mas", new: "schemas"},
	{old: "sche ma", new: "schema"},
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

// NormalizeAgentDisplayText repairs fragmented natural-language output for UI display.
func NormalizeAgentDisplayText(text string) string {
	normalizedText := singleLine(text)
	if normalizedText == "" {
		return strings.TrimSpace(text)
	}
	if !strings.Contains(normalizedText, "`") {
		return normalizePlainAgentText(normalizedText)
	}
	var builder strings.Builder
	segmentStart := 0
	for segmentStart < len(normalizedText) {
		backtickStart := strings.Index(normalizedText[segmentStart:], "`")
		if backtickStart < 0 {
			builder.WriteString(normalizePlainAgentText(normalizedText[segmentStart:]))
			break
		}
		backtickStart += segmentStart
		builder.WriteString(normalizePlainAgentText(normalizedText[segmentStart:backtickStart]))
		backtickEnd := strings.Index(normalizedText[backtickStart+1:], "`")
		if backtickEnd < 0 {
			builder.WriteString(normalizeFragmentedAgentText(normalizedText[backtickStart:]))
			break
		}
		backtickEnd += backtickStart + 1
		innerText := strings.TrimSpace(normalizedText[backtickStart+1 : backtickEnd])
		builder.WriteString("`")
		builder.WriteString(normalizeFragmentedAgentText(innerText))
		builder.WriteString("`")
		segmentStart = backtickEnd + 1
	}
	return strings.TrimSpace(builder.String())
}

func normalizePlainAgentText(text string) string {
	if text == "" {
		return text
	}
	plainText := collapseCJKSpacing(text)
	plainText = collapseContractionSpacing(plainText)
	plainText = strings.ReplaceAll(plainText, " ,", ",")
	plainText = strings.ReplaceAll(plainText, " .", ".")
	plainText = strings.ReplaceAll(plainText, "( ", "(")
	plainText = strings.ReplaceAll(plainText, " )", ")")
	plainText = strings.ReplaceAll(plainText, " - ", "-")
	plainText = strings.ReplaceAll(plainText, " -", "-")
	plainText = strings.ReplaceAll(plainText, "- ", "-")
	if strings.Contains(plainText, "_") {
		return normalizeFragmentedAgentText(plainText)
	}
	plainText = collapseIdentifierFragments(plainText)
	for _, replacement := range fragmentedTokenReplacements {
		if strings.Contains(plainText, replacement.old) {
			plainText = strings.ReplaceAll(plainText, replacement.old, replacement.new)
		}
	}
	return plainText
}

func collapseContractionSpacing(text string) string {
	replacements := []string{
		" n't", "n't",
		" 't ", "'t ",
		" 's ", "'s ",
		" 're ", "'re ",
		" 've ", "'ve ",
		" 'd ", "'d ",
		" 'll ", "'ll ",
	}
	for idx := 0; idx < len(replacements); idx += 2 {
		text = strings.ReplaceAll(text, replacements[idx], replacements[idx+1])
	}
	return text
}

func collapseCJKSpacing(text string) string {
	runes := []rune(text)
	if len(runes) == 0 {
		return text
	}
	var builder strings.Builder
	builder.Grow(len(runes))
	for runeIdx := 0; runeIdx < len(runes); runeIdx++ {
		currentRune := runes[runeIdx]
		if currentRune == ' ' && runeIdx > 0 && runeIdx+1 < len(runes) && shouldCollapseProviderSpacing(runes[runeIdx-1], runes[runeIdx+1]) {
			continue
		}
		builder.WriteRune(currentRune)
	}
	return builder.String()
}

func shouldCollapseProviderSpacing(left, right rune) bool {
	return isProviderCompactRune(left) && isProviderCompactRune(right)
}

func isProviderCompactRune(value rune) bool {
	if unicode.Is(unicode.Han, value) {
		return true
	}
	switch value {
	case '，', '。', '、', '；', '：', '？', '！', '）', '（', '》', '《', '」', '「', '’', '‘', '”', '“':
		return true
	default:
		return false
	}
}

// RepairFragmentedQuery normalizes fragmented BYDBQL text into a single statement.
func RepairFragmentedQuery(query string) string {
	normalizedQuery := normalizeFragmentedAgentText(query)
	if normalizedQuery == "" {
		return strings.TrimSpace(query)
	}
	return normalizedQuery
}

func normalizeFragmentedAgentText(text string) string {
	normalizedText := singleLine(text)
	normalizedText = strings.ReplaceAll(normalizedText, "` ` `", "```")
	normalizedText = strings.ReplaceAll(normalizedText, "`` `", "```")
	normalizedText = strings.ReplaceAll(normalizedText, "` ``", "```")
	normalizedText = collapseIdentifierFragments(normalizedText)
	for _, replacement := range fragmentedTokenReplacements {
		normalizedText = strings.ReplaceAll(normalizedText, replacement.old, replacement.new)
	}
	normalizedText = strings.ReplaceAll(normalizedText, " ,", ",")
	normalizedText = strings.ReplaceAll(normalizedText, " .", ".")
	normalizedText = strings.ReplaceAll(normalizedText, "( ", "(")
	normalizedText = strings.ReplaceAll(normalizedText, " )", ")")
	normalizedText = strings.ReplaceAll(normalizedText, " text ", " ")
	normalizedText = fragmentedTimeRangePattern.ReplaceAllString(normalizedText, "'-${1}m'")
	normalizedText = strings.ReplaceAll(normalizedText, ">'", "> '")
	normalizedText = strings.ReplaceAll(normalizedText, "<'", "< '")
	return strings.TrimSpace(normalizedText)
}

func collapseIdentifierFragments(text string) string {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return ""
	}
	collapsedFields := make([]string, 0, len(fields))
	for fieldIdx := 0; fieldIdx < len(fields); fieldIdx++ {
		currentField := fields[fieldIdx]
		if fieldIdx+1 < len(fields) && shouldJoinIdentifierFragment(currentField, fields[fieldIdx+1]) {
			collapsedFields = append(collapsedFields, currentField+fields[fieldIdx+1])
			fieldIdx++
			continue
		}
		collapsedFields = append(collapsedFields, currentField)
	}
	return strings.Join(collapsedFields, " ")
}

func shouldJoinIdentifierFragment(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	if isFragmentJoinStopword(left) || isFragmentJoinStopword(right) {
		return false
	}
	if strings.HasSuffix(left, "_") || strings.HasPrefix(right, "_") {
		return true
	}
	if strings.Contains(left, "_") && len(right) <= 4 && isIdentifierFragment(right) {
		return true
	}
	if len(right) == 1 && isUpperAlpha(right) {
		switch left + right {
		case "AVG", "MAX", "MIN":
			return true
		}
	}
	return isLowerAlpha(left) && isLowerAlpha(right) && len(left) <= 4 && len(right) <= 12 && len(left)+len(right) <= 8
}

func isFragmentJoinStopword(token string) bool {
	_, found := fragmentJoinStopwords[token]
	return found
}

var fragmentJoinStopwords = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "you": {}, "your": {}, "need": {}, "more": {}, "let": {}, "but": {}, "not": {},
	"see": {}, "ask": {}, "use": {}, "with": {}, "from": {}, "that": {}, "this": {}, "what": {}, "when": {}, "have": {},
	"has": {}, "are": {}, "was": {}, "were": {}, "been": {}, "into": {}, "also": {}, "all": {}, "can": {}, "could": {},
	"would": {}, "should": {}, "will": {}, "after": {}, "before": {}, "about": {}, "than": {}, "then": {}, "them": {},
	"they": {}, "most": {}, "like": {}, "just": {}, "only": {}, "very": {}, "here": {}, "there": {}, "how": {}, "who": {},
	"why": {}, "its": {}, "our": {}, "out": {}, "any": {}, "may": {}, "did": {}, "don": {}, "does": {}, "didn": {},
	"doesn": {}, "isn": {}, "aren": {}, "won": {}, "cant": {}, "couldn": {}, "wouldn": {}, "shouldn": {}, "must": {},
	"still": {}, "even": {}, "over": {}, "such": {}, "once": {}, "each": {}, "both": {}, "me": {}, "by": {}, "to": {},
	"in": {}, "on": {}, "at": {}, "or": {}, "if": {}, "as": {}, "an": {}, "be": {}, "we": {}, "he": {}, "it": {},
	"my": {}, "up": {}, "so": {}, "no": {}, "do": {}, "go": {}, "is": {}, "am": {},
}

func isUpperAlpha(value string) bool {
	for _, valueRune := range value {
		if valueRune < 'A' || valueRune > 'Z' {
			return false
		}
	}
	return value != ""
}

func isLowerAlpha(value string) bool {
	for _, valueRune := range value {
		if valueRune < 'a' || valueRune > 'z' {
			return false
		}
	}
	return true
}

func isIdentifierFragment(value string) bool {
	if value == "" {
		return false
	}
	for _, valueRune := range value {
		if (valueRune < 'a' || valueRune > 'z') && valueRune != '_' {
			return false
		}
	}
	return true
}

func cleanBydbqlCandidate(text string) string {
	candidate := strings.TrimSpace(text)
	if candidate == "" {
		return ""
	}
	if strings.Contains(candidate, ";") {
		return ""
	}
	if !looksLikeBydbql(candidate) {
		return ""
	}
	upperCandidate := strings.ToUpper(candidate)
	if strings.HasPrefix(upperCandidate, "SELECT ") && !strings.Contains(upperCandidate, " FROM ") && !strings.Contains(upperCandidate, "\nFROM ") {
		return ""
	}
	return candidate
}

func looksLikeBydbql(text string) bool {
	upperText := strings.ToUpper(strings.TrimSpace(text))
	return strings.HasPrefix(upperText, "SELECT ") || strings.HasPrefix(upperText, "SHOW TOP ")
}
