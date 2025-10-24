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
	"strings"

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

// Lexer and parser are initialized in init().
var (
	bydbqlLexer     lexer.Definition
	particpleParser *participle.Parser[Grammar]
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
}

// ParseQuery parses a BydbQL query string into a Grammar struct.
func ParseQuery(query string) (*Grammar, error) {
	// Parse using Participle
	grammar, err := particpleParser.ParseString("", query)
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}

	return grammar, nil
}
