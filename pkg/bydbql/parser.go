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

package bydbql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// Keywords list - single source of truth for all BydbQL keywords.
var bydbqlKeywords = []string{
	"SELECT", "SHOW", "TOP", "FROM", "STREAM", "MEASURE", "TRACE", "PROPERTY",
	"IN", "ON", "STAGES", "TIME", "BETWEEN", "AND", "OR", "WHERE", "GROUP", "BY", "ORDER",
	"ASC", "DESC", "LIMIT", "OFFSET", "WITH", "QUERY_TRACE", "SUM", "MEAN",
	"AVG", "COUNT", "MAX", "MIN", "TAG", "FIELD", "NOT", "HAVING", "MATCH",
	"AGGREGATE", "NULL",
}

// Function keywords that can be directly followed by '('.
var functionKeywords = map[string]bool{
	"SUM": true, "MEAN": true, "AVG": true, "COUNT": true, "MAX": true, "MIN": true,
	"MATCH": true, // MATCH can also be followed by '('
}

// Lexer and parser are initialized in init().
var (
	bydbqlLexer     lexer.Definition
	particpleParser *participle.Parser[Grammar]
	keywordPattern  *regexp.Regexp
)

func init() {
	// Build lexer dynamically from keyword list to avoid duplication
	keywordStr := strings.Join(bydbqlKeywords, "|")
	bydbqlLexer = lexer.MustSimple([]lexer.SimpleRule{
		{
			Name:    "Keyword",
			Pattern: fmt.Sprintf(`(?i)(%s)\b`, keywordStr),
		},
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_-]*`},
		{Name: "Int", Pattern: `[-+]?\d+`},
		{Name: "String", Pattern: `'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"`},
		{Name: "QuotedIdent", Pattern: `"[a-zA-Z_][a-zA-Z0-9_.]*"|'[a-zA-Z_][a-zA-Z0-9_.]*'`},
		{Name: "Operators", Pattern: `!=|>=|<=|::|[=><,.()*]`},
		{Name: "whitespace", Pattern: `\s+`},
	})

	// Build parser with the dynamically created lexer
	var err error
	particpleParser, err = participle.Build[Grammar](
		participle.Lexer(bydbqlLexer),
		participle.Unquote("String"),
		participle.Unquote("QuotedIdent"),
		participle.CaseInsensitive("Keyword"),
		participle.UseLookahead(2),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to build BydbQL parser: %v", err))
	}

	// Build keyword pattern for validation (reuse keywordStr to avoid duplication)
	keywordPattern = regexp.MustCompile(fmt.Sprintf(`(?i)\b(%s)\b`, keywordStr))
}

// validateKeywordSpacing checks that all keywords in the query are properly separated by whitespace.
// Returns an error if any keyword is not preceded or followed by valid separators.
func validateKeywordSpacing(query string) error {
	// Find all string literals (single and double quoted) to exclude them from validation
	stringPattern := regexp.MustCompile(`'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"`)
	stringRanges := stringPattern.FindAllStringIndex(query, -1)

	// Find all quoted identifiers to exclude them from validation
	quotedIdentPattern := regexp.MustCompile(`"[a-zA-Z_][a-zA-Z0-9_.-]*"|'[a-zA-Z_][a-zA-Z0-9_.-]*'`)
	quotedIdentRanges := quotedIdentPattern.FindAllStringIndex(query, -1)

	// Helper function to check if a position is inside a string literal or quoted identifier
	isInExcludedRange := func(pos int) bool {
		for _, r := range stringRanges {
			if pos >= r[0] && pos < r[1] {
				return true
			}
		}
		for _, r := range quotedIdentRanges {
			if pos >= r[0] && pos < r[1] {
				return true
			}
		}
		return false
	}

	// Find all keyword matches
	matches := keywordPattern.FindAllStringIndex(query, -1)
	if matches == nil {
		return nil
	}

	for _, match := range matches {
		start := match[0]
		end := match[1]
		keyword := query[start:end]

		// Skip if keyword is inside a string literal or quoted identifier
		if isInExcludedRange(start) {
			continue
		}

		// Skip if the keyword is part of an identifier (e.g., test-trace-group)
		if (start > 0 && isIdentifierChar(query[start-1])) ||
			(end < len(query) && isIdentifierChar(query[end])) {
			continue
		}

		// Check character before keyword
		if start > 0 {
			prevChar := query[start-1]
			// Check for :: (type specifier) - if the keyword is TAG or FIELD, :: is allowed before it
			if start >= 2 && query[start-2:start] == "::" && (strings.EqualFold(keyword, "TAG") || strings.EqualFold(keyword, "FIELD")) {
				// This is a type specifier like ::tag or ::field, which is valid
				continue
			}
			// Check for . (identifier path) - if previous char is ., this keyword is part of an identifier path
			if prevChar == '.' {
				// This is part of an identifier path like trace.span.id, which is valid
				continue
			}
			// Valid preceding characters: whitespace or comma
			if !isWhitespace(prevChar) && prevChar != ',' {
				return fmt.Errorf(
					"missing space before keyword '%s' at position %d\n"+
						"Query: %s\n"+
						"       %s^ here\n"+
						"Hint: Add a space before '%s'",
					keyword, start, query, strings.Repeat(" ", start), keyword,
				)
			}
		}

		// Check character after keyword
		if end < len(query) {
			nextChar := query[end]
			// Check for . (identifier path) - if next char is ., this keyword is part of an identifier path
			if nextChar == '.' {
				// This is part of an identifier path like trace.span.id, which is valid
				continue
			}
			// Check for :: (type specifier) - if the keyword is followed by ::, it might be part of a path
			if end+2 <= len(query) && query[end:end+2] == "::" {
				// This is followed by a type specifier like name::tag, which is valid
				continue
			}
			// For function keywords, allow opening parenthesis
			if functionKeywords[strings.ToUpper(keyword)] && nextChar == '(' {
				// This is a function call like SUM(field), which is valid
				continue
			}
			// Valid following characters: whitespace or comma
			if !isWhitespace(nextChar) && nextChar != ',' {
				return fmt.Errorf(
					"missing space after keyword '%s' at position %d\n"+
						"Query: %s\n"+
						"       %s^ here\n"+
						"Hint: Add a space after '%s'",
					keyword, end, query, strings.Repeat(" ", end), keyword,
				)
			}
		}
	}

	return nil
}

// isWhitespace checks if a character is whitespace (space, tab, newline, carriage return).
func isWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// isIdentifierChar checks if a character can be part of an identifier.
func isIdentifierChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_' ||
		c == '-'
}

// ParseQuery parses a BydbQL query string into a ParsedQuery AST.
func ParseQuery(query string) (*ParsedQuery, error) {
	// Validate keyword spacing before parsing
	if err := validateKeywordSpacing(query); err != nil {
		return nil, fmt.Errorf("spacing error: %w", err)
	}

	// Parse using Participle
	grammar, err := particpleParser.ParseString("", query)
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}

	// Convert grammar to AST
	stmt, err := grammar.toAST()
	if err != nil {
		return nil, fmt.Errorf("failed to build AST: %w", err)
	}

	if stmt == nil {
		return nil, fmt.Errorf("failed to build query statement")
	}

	// Create ParsedQuery
	parsed := &ParsedQuery{
		Statement: stmt,
		Context: &QueryContext{
			CurrentTime: time.Now(),
		},
	}

	// Extract resource information
	switch s := stmt.(type) {
	case *SelectStatement:
		if s.From != nil {
			parsed.ResourceType = s.From.ResourceType
			parsed.ResourceName = s.From.ResourceName
			parsed.Groups = append(parsed.Groups, s.From.Groups...)
		}
	case *TopNStatement:
		if s.From != nil {
			parsed.ResourceType = s.From.ResourceType
			parsed.ResourceName = s.From.ResourceName
			parsed.Groups = append(parsed.Groups, s.From.Groups...)
		}
	}

	return parsed, nil
}
