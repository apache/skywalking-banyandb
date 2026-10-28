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
	"errors"
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
	"AGGREGATE", "NULL", "DISTINCT", "TIME_BUCKET",
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
		// `-` and `*` are accepted as continuation characters, never as the first one, so that
		// resource names the schema layer already accepts at creation time — e.g. a TopN rule
		// named `endpoint_avg-cluster-excludes-*` — stay referenceable from a query. Confining
		// `*` to non-initial positions is what keeps the `SELECT *` projection unambiguous: a
		// bare `*` cannot begin an identifier, so it still lexes as an operator.
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_*-]*`},
		{Name: "Int", Pattern: `[-+]?\d+`},
		{Name: "String", Pattern: `'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"`},
		{Name: "QuotedIdent", Pattern: `"[a-zA-Z_][a-zA-Z0-9_.]*"|'[a-zA-Z_][a-zA-Z0-9_.]*'`},
		{Name: "Param", Pattern: `\?`},
		{Name: "Operators", Pattern: `!=|>=|<=|::|[=><,.()*+\-]`},
		{Name: "whitespace", Pattern: `\s+`},
	})

	// Build parser with the dynamically created lexer
	var err error
	particpleParser, err = participle.Build[Grammar](
		participle.Lexer(bydbqlLexer),
		participle.Unquote("String"),
		participle.Unquote("QuotedIdent"),
		participle.CaseInsensitive("Keyword", "Ident"),
		participle.UseLookahead(2),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to build BydbQL parser: %v", err))
	}
}

// maxParenDepth bounds the parenthesis nesting ParseQuery accepts. Participle's
// recursive-descent parser calls back into GrammarOrExpr once per nesting level
// with no built-in recursion limit, so a query with enough nested parentheses
// can exhaust the goroutine stack and crash the process via runtime.throw — a
// fatal error recover() cannot catch. Rejecting excess nesting with a flat,
// non-recursive scan before the query ever reaches the parser keeps the fix
// itself immune to the same failure mode.
const maxParenDepth = 64

// ParseQuery parses a BydbQL query string into a Grammar struct.
func ParseQuery(query string) (*Grammar, error) {
	if err := checkParenDepth(query); err != nil {
		return nil, err
	}
	grammar, err := parseQuerySyntax(query)
	if err != nil {
		ordered, orderErr := parseExpressionOrder(query)
		if !errors.Is(orderErr, errNoExpressionOrder) {
			grammar, err = ordered, orderErr
		}
	}
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}
	if grammar.Select != nil && grammar.Select.Distinct && grammar.Select.From.Shorthand == nil && !strings.EqualFold(grammar.Select.From.ResourceType, "MEASURE") {
		return nil, fmt.Errorf("SELECT DISTINCT is supported only for measures")
	}
	markExpressionLiterals(grammar, query)
	if grammar.Select != nil && grammar.Select.From.Shorthand != nil {
		from := grammar.Select.From
		from.ResourceType = "MEASURE"
		from.ResourceName = from.Shorthand.Name
		from.In = &GrammarInClause{Groups: []string{from.Shorthand.Group}}
	}
	if grammar.TopN != nil && grammar.TopN.From.Shorthand != nil {
		return nil, fmt.Errorf("syntax error: source shorthand is valid only for measure SELECT")
	}
	if grammar.Select != nil && grammar.Select.OrderBy != nil && len(grammar.Select.OrderBy.Keys) > 0 &&
		!strings.EqualFold(grammar.Select.From.ResourceType, "MEASURE") {
		return nil, fmt.Errorf("syntax error: typed ORDER BY is supported only for measures")
	}
	if err := checkExpressionSource(grammar); err != nil {
		return nil, err
	}
	if err := checkAggregateAndGroupByShape(grammar); err != nil {
		return nil, err
	}
	if err := checkTimeValuePositions(grammar); err != nil {
		return nil, err
	}

	return grammar, nil
}

func parseQuerySyntax(query string) (*Grammar, error) {
	grammar, err := parseSelectSyntax(query)
	if err != nil {
		havingGrammar, havingErr := parseExpressionHaving(query)
		if !errors.Is(havingErr, errNoExpressionHaving) {
			return havingGrammar, havingErr
		}
	}
	return grammar, err
}

func parseSelectSyntax(query string) (*Grammar, error) {
	grammar, err := parseSelectWithoutComputedGroups(query)
	if err != nil {
		groupGrammar, groupErr := parseExpressionGroupBy(query)
		if !errors.Is(groupErr, errNoExpressionGroupBy) {
			return groupGrammar, groupErr
		}
	}
	return grammar, err
}

func parseSelectWithoutComputedGroups(query string) (*Grammar, error) {
	grammar, err := particpleParser.ParseString("", query)
	if err != nil {
		grammar, err = parseExpressionProjection(query)
		if err != nil {
			whereGrammar, whereErr := parseExpressionWhere(query)
			if !errors.Is(whereErr, errNoExpressionWhere) {
				grammar, err = whereGrammar, whereErr
			}
		}
	}
	return grammar, err
}

// checkAggregateAndGroupByShape enforces two constraints participle's
// struct-tag grammar cannot express as per-alternative predicates:
//   - DISTINCT is only valid inside COUNT — SUM(DISTINCT x) etc. is rejected.
//   - At most one TIME_BUCKET(...) pseudo-column is allowed per GROUP BY.
//
// Both are semantic rules, but are enforced here (rather than at transform
// time) so a malformed query fails exactly like any other syntax error, one
// step after the raw participle parse — the same precedent checkParenDepth
// already set for a pre-parse check.
func checkAggregateAndGroupByShape(g *Grammar) error {
	if g.Select == nil {
		return nil
	}
	if g.Select.Where != nil && grammarExpressionHasBucket(g.Select.Where.Expression) {
		return fmt.Errorf("syntax error: TIME_BUCKET is supported only in grouped SELECT and HAVING expressions")
	}
	if proj := g.Select.Projection; proj != nil {
		var columns []*GrammarColumn
		columns = append(columns, proj.Columns...)
		if proj.TopN != nil {
			columns = append(columns, proj.TopN.OtherColumns...)
		}
		for _, col := range columns {
			if g.Select.GroupBy == nil && grammarExpressionHasBucket(col.Expression) {
				return fmt.Errorf("syntax error: TIME_BUCKET projection requires GROUP BY")
			}
			if col.Aggregate == nil {
				continue
			}
			if col.Aggregate.TypeSpec != nil {
				if col.TypeSpec != nil {
					return fmt.Errorf("syntax error: aggregate qualifier appears inside and outside the argument")
				}
				if !strings.EqualFold(g.Select.From.ResourceType, "MEASURE") {
					return fmt.Errorf("syntax error: qualified aggregate arguments are supported only for measures")
				}
			}
			if err := checkAggregateShape(col.Aggregate); err != nil {
				return err
			}
		}
	}
	if having := g.Select.Having; having != nil {
		if err := checkHavingAggregateShape(having.Expression); err != nil {
			return err
		}
	}
	if gb := g.Select.GroupBy; gb != nil {
		bucketCount := 0
		for _, col := range gb.Columns {
			if col.TimeBucket != nil || grammarExpressionHasBucket(col.Expression) {
				bucketCount++
			}
		}
		if bucketCount > 1 {
			return fmt.Errorf("syntax error: at most one TIME_BUCKET(...) is allowed in GROUP BY, got %d", bucketCount)
		}
	}
	return nil
}

func grammarExpressionHasBucket(expression *GrammarExpression) bool {
	if expression == nil {
		return false
	}
	if expression.Op == "time_bucket" {
		return true
	}
	for _, child := range expression.Args {
		if grammarExpressionHasBucket(child) {
			return true
		}
	}
	return false
}

func checkAggregateShape(aggregate *GrammarAggregateFunction) error {
	if aggregate != nil && aggregate.Distinct && !strings.EqualFold(aggregate.Function, "COUNT") {
		return fmt.Errorf("syntax error: DISTINCT is only valid inside COUNT, got %s(DISTINCT ...)", aggregate.Function)
	}
	return nil
}

func checkHavingAggregateShape(expression *GrammarExpression) error {
	if err := checkAggregateShape(expression.Aggregate); err != nil {
		return err
	}
	for _, child := range expression.Args {
		if err := checkHavingAggregateShape(child); err != nil {
			return err
		}
	}
	return nil
}

// checkParenDepth rejects queries whose parenthesis nesting exceeds maxParenDepth.
// It skips over quoted string content — which may contain unbalanced or
// backslash-escaped quote characters, per the String lexer rule — so literal
// values never distort the depth count.
func checkParenDepth(query string) error {
	depth := 0
	var quote byte
	for i := 0; i < len(query); i++ {
		c := query[i]
		if quote != 0 {
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '(':
			depth++
			if depth > maxParenDepth {
				return fmt.Errorf("syntax error: parenthesis nesting exceeds maximum depth of %d", maxParenDepth)
			}
		case ')':
			if depth > 0 {
				depth--
			}
		}
	}
	return nil
}
