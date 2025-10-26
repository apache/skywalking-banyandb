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

// Package bydbql provides BanyanDB Query Language (BydbQL) parsing and translation capabilities.
//
//nolint:govet // ignore fieldalignment in this file; layout is the bydbQL grammar
package bydbql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2/lexer"
)

// Grammar represents the root of a BydbQL statement parsed by Participle.
type Grammar struct {
	Select *GrammarSelectStatement `parser:"  @@"`
	TopN   *GrammarTopNStatement   `parser:"| @@"`
}

// GrammarSelectStatement represents a SELECT statement in Participle grammar.
type GrammarSelectStatement struct {
	Pos            lexer.Position
	Select         string                      `parser:"@'SELECT'"`
	Projection     *GrammarProjection          `parser:"@@"`
	From           *GrammarFromClause          `parser:"@@"`
	Time           *GrammarTimeClause          `parser:"@@?"`
	Where          *GrammarSelectWhereClause   `parser:"@@?"`
	GroupBy        *GrammarGroupByClause       `parser:"@@?"`
	OrderBy        *GrammarSelectOrderByClause `parser:"@@?"`
	WithQueryTrace *GrammarWithTraceClause     `parser:"@@?"`
	Limit          *GrammarLimitClause         `parser:"@@?"`
	Offset         *GrammarOffsetClause        `parser:"@@?"`
}

// GrammarTopNStatement represents a SHOW TOP N statement.
type GrammarTopNStatement struct {
	Pos            lexer.Position
	Show           string                        `parser:"@'SHOW'"`
	Top            string                        `parser:"@'TOP'"`
	N              int                           `parser:"@Int"`
	From           *GrammarFromClause            `parser:"@@"`
	Time           *GrammarTimeClause            `parser:"@@?"`
	Where          *GrammarTopNWhereClause       `parser:"@@?"`
	AggregateBy    *GrammarTopNAggregateByClause `parser:"@@?"`
	OrderBy        *GrammarTopNOrderByClause     `parser:"@@?"`
	WithQueryTrace *GrammarWithTraceClause       `parser:"@@?"`
}

// GrammarProjection represents projection in SELECT.
type GrammarProjection struct {
	All     bool                   `parser:"  @'*'"`
	Empty   bool                   `parser:"| @'(' ')'"`
	TopN    *GrammarTopNProjection `parser:"| 'TOP' @@"`
	Columns []*GrammarColumn       `parser:"| @@ ( ',' @@ )*"`
}

// GrammarTopNProjection represents TOP N projection.
type GrammarTopNProjection struct {
	N            int                    `parser:"@Int"`
	OrderField   *GrammarIdentifierPath `parser:"@@"`
	Direction    *string                `parser:"@('ASC'|'DESC')?"`
	OtherColumns []*GrammarColumn       `parser:"  ( ',' @@ ( ',' @@ )* )?"`
}

// GrammarColumn represents a column in projection.
type GrammarColumn struct {
	Aggregate  *GrammarAggregateFunction `parser:"  @@"`
	Identifier *GrammarIdentifierPath    `parser:"| @@"`
	TypeSpec   *string                   `parser:"( '::' @('TAG'|'FIELD') )?"`
}

// GrammarAggregateFunction represents aggregate functions.
type GrammarAggregateFunction struct {
	Function string                 `parser:"@('SUM'|'MEAN'|'AVG'|'COUNT'|'MAX'|'MIN')"`
	Column   *GrammarIdentifierPath `parser:"'(' @@ ')'"`
}

// GrammarTopNAggregateFunction represents aggregate functions without column (for TOP N).
type GrammarTopNAggregateFunction struct {
	Function string `parser:"@('SUM'|'MEAN'|'AVG'|'COUNT'|'MAX'|'MIN')"`
}

// GrammarFromClause represents FROM clause.
type GrammarFromClause struct {
	From         string              `parser:"@'FROM'"`
	ResourceType string              `parser:"@('STREAM'|'MEASURE'|'TRACE'|'PROPERTY')"`
	ResourceName string              `parser:"@Ident"`
	In           *GrammarInClause    `parser:"@@"`
	Stage        *GrammarStageClause `parser:"@@?"`
}

// GrammarInClause represents IN clause.
type GrammarInClause struct {
	In     string   `parser:"@'IN'"`
	LParen bool     `parser:"@'('?"`
	Groups []string `parser:"@Ident ( ',' @Ident )*"`
	RParen bool     `parser:"@')'?"`
}

// GrammarStageClause represents STAGES clause.
type GrammarStageClause struct {
	On      string   `parser:"@'ON'"`
	LParen  bool     `parser:"@'('?"`
	Stages  []string `parser:"@Ident ( ',' @Ident )*"`
	RParen  bool     `parser:"@')'?"`
	Stages2 string   `parser:"@'STAGES'"`
}

// GrammarTimeClause represents TIME clause.
type GrammarTimeClause struct {
	Time       string              `parser:"@'TIME'"`
	Comparator *string             `parser:"(  @( '=' | '>' | '<' | '>=' | '<=' )"`
	Value      *GrammarTimeValue   `parser:"   @@"`
	Between    *GrammarTimeBetween `parser:"| @@ )"`
}

// GrammarTimeBetween represents TIME BETWEEN.
type GrammarTimeBetween struct {
	Between string            `parser:"@'BETWEEN'"`
	Begin   *GrammarTimeValue `parser:"@@"`
	And     string            `parser:"@'AND'"`
	End     *GrammarTimeValue `parser:"@@"`
}

// GrammarTimeValue represents a time value (string or integer).
type GrammarTimeValue struct {
	String  *string `parser:"  @String"`
	Integer *int64  `parser:"| @Int"`
}

// GrammarSelectWhereClause represents WHERE clause.
type GrammarSelectWhereClause struct {
	Where string         `parser:"@'WHERE'"`
	Expr  *GrammarOrExpr `parser:"@@"`
}

// GrammarTopNWhereClause represents WHERE clause in TOP N.
type GrammarTopNWhereClause struct {
	Where string          `parser:"@'WHERE'"`
	Expr  *GrammarAndExpr `parser:"@@"`
}

// GrammarOrExpr represents OR expression.
type GrammarOrExpr struct {
	Left  *GrammarAndExpr   `parser:"@@"`
	Right []*GrammarOrRight `parser:"@@*"`
}

// GrammarOrRight represents right side of OR.
type GrammarOrRight struct {
	Or    string          `parser:"@'OR'"`
	Right *GrammarAndExpr `parser:"@@"`
}

// GrammarAndExpr represents AND expression.
type GrammarAndExpr struct {
	Left  *GrammarPredicate  `parser:"@@"`
	Right []*GrammarAndRight `parser:"@@*"`
}

// GrammarAndRight represents right side of AND.
type GrammarAndRight struct {
	And   string            `parser:"@'AND'"`
	Right *GrammarPredicate `parser:"@@"`
}

// GrammarPredicate represents a predicate.
type GrammarPredicate struct {
	Paren  *GrammarOrExpr          `parser:"  '(' @@ ')'"`
	Binary *GrammarBinaryPredicate `parser:"| @@"`
	In     *GrammarInPredicate     `parser:"| @@"`
	Having *GrammarHavingPredicate `parser:"| @@"`
}

// GrammarBinaryPredicate captures comparison or MATCH predicates sharing an identifier left-hand side.
type GrammarBinaryPredicate struct {
	Identifier *GrammarIdentifierPath      `parser:"@@"`
	Tail       *GrammarBinaryPredicateTail `parser:"@@"`
}

// GrammarBinaryPredicateTail distinguishes between a MATCH suffix and a standard comparison operator.
type GrammarBinaryPredicateTail struct {
	Match   *GrammarMatchTail   `parser:"  @@"`
	Compare *GrammarCompareTail `parser:"| @@"`
}

// GrammarCompareTail represents traditional binary comparison operators.
type GrammarCompareTail struct {
	Operator string        `parser:"@( '=' | '!=' | '>=' | '<=' | '>' | '<' )"`
	Value    *GrammarValue `parser:"@@"`
}

// GrammarMatchTail represents the RHS of a MATCH predicate.
type GrammarMatchTail struct {
	MatchToken string              `parser:"@'MATCH'"`
	LParen     string              `parser:"@'('"`
	Values     *GrammarMatchValues `parser:"@@"`
	Analyzer   *string             `parser:"( ',' @String"`
	Operator   *string             `parser:"  ( ',' @String )? )?"`
	RParen     string              `parser:"@')'"`
}

// GrammarInPredicate represents IN/NOT IN predicate.
type GrammarInPredicate struct {
	Identifier *GrammarIdentifierPath `parser:"@@"`
	Not        *string                `parser:"@'NOT'?"`
	In         string                 `parser:"@'IN'"`
	Values     []*GrammarValue        `parser:"'(' @@? ( ',' @@ )* ')'"`
}

// GrammarHavingPredicate represents HAVING/NOT HAVING predicate.
type GrammarHavingPredicate struct {
	Identifier *GrammarIdentifierPath `parser:"@@"`
	Not        *string                `parser:"@'NOT'?"`
	Having     string                 `parser:"@'HAVING'"`
	Values     *GrammarHavingValues   `parser:"@@"`
}

// GrammarHavingValues represents values in HAVING.
type GrammarHavingValues struct {
	Single *GrammarValue   `parser:"  @@"`
	Array  []*GrammarValue `parser:"| '(' @@ ( ',' @@ )* ')'"`
}

// GrammarMatchValues represents values in MATCH.
type GrammarMatchValues struct {
	Single *GrammarValue   `parser:"  @@"`
	Array  []*GrammarValue `parser:"| '(' @@ ( ',' @@ )* ')'"`
}

// GrammarValue represents a value.
type GrammarValue struct {
	String  *string `parser:"  @String"`
	Integer *int64  `parser:"| @Int"`
	Null    bool    `parser:"| @'NULL'"`
}

// GrammarIdentifierPart Can be either an Ident or a Keyword (keywords are allowed in paths, but not as standalone identifiers).
type GrammarIdentifierPart struct {
	Ident   *string `parser:"  @Ident"`
	Keyword *string `parser:"| @Keyword"`
}

// Value returns the string value of the identifier part.
func (p *GrammarIdentifierPart) Value() string {
	if p.Ident != nil {
		return *p.Ident
	}
	if p.Keyword != nil {
		return *p.Keyword
	}
	return ""
}

// GrammarIdentifierPath Examples: column_name, _column, response.time, metadata.service.id, "count", 'trace.span.id'.
type GrammarIdentifierPath struct {
	QuotedIdent *string                  `parser:"  ( @QuotedIdent | @String )"`
	First       *GrammarIdentifierPart   `parser:"| @@"`
	Rest        []*GrammarIdentifierPart `parser:"  ( '.' @@ )*"`
}

// GrammarGroupByClause represents GROUP BY clause.
type GrammarGroupByClause struct {
	Group   string                  `parser:"@'GROUP'"`
	By      string                  `parser:"@'BY'"`
	Columns []*GrammarGroupByColumn `parser:"@@ ( ',' @@ )*"`
}

// GrammarGroupByColumn represents a column in GROUP BY.
type GrammarGroupByColumn struct {
	Identifier *GrammarIdentifierPath `parser:"@@"`
	TypeSpec   *string                `parser:"( '::' @('TAG'|'FIELD') )?"`
}

// GrammarSelectOrderByClause represents ORDER BY clause in SELECT statement.
type GrammarSelectOrderByClause struct {
	Order string             `parser:"@'ORDER'"`
	By    string             `parser:"@'BY'"`
	Tail  GrammarOrderByTail `parser:"@@"`
}

// GrammarTopNOrderByClause represents ORDER BY clause in TOP N statement.
type GrammarTopNOrderByClause struct {
	Order string  `parser:"@'ORDER'"`
	By    string  `parser:"@'BY'"`
	Dir   *string `parser:"@('ASC'|'DESC')?"`
}

// GrammarOrderByTail represents the tail of ORDER BY clause, Identifier with optional direction or direction only.
type GrammarOrderByTail struct {
	DirOnly   *string                  `parser:"  @('ASC'|'DESC')"`
	WithIdent *GrammarOrderByWithIdent `parser:"| @@ "`
}

// GrammarOrderByWithIdent represents ORDER BY with identifier and optional direction.
type GrammarOrderByWithIdent struct {
	Identifier *GrammarIdentifierPath `parser:"@@"`
	Direction  *string                `parser:"@( 'ASC' | 'DESC' )?"`
}

// GrammarLimitClause represents LIMIT clause.
type GrammarLimitClause struct {
	Limit string `parser:"@'LIMIT'"`
	Value int    `parser:"@Int"`
}

// GrammarOffsetClause represents OFFSET clause.
type GrammarOffsetClause struct {
	Offset string `parser:"@'OFFSET'"`
	Value  int    `parser:"@Int"`
}

// GrammarWithTraceClause represents WITH QUERY_TRACE clause.
type GrammarWithTraceClause struct {
	With       string `parser:"@'WITH'"`
	QueryTrace string `parser:"@'QUERY_TRACE'"`
}

// GrammarTopNAggregateByClause represents AGGREGATE BY clause.
type GrammarTopNAggregateByClause struct {
	Aggregate string                        `parser:"@'AGGREGATE'"`
	By        string                        `parser:"@'BY'"`
	Function  *GrammarTopNAggregateFunction `parser:"@@"`
}

// ToString converts time value to string representation.
func (g *GrammarTimeValue) ToString() string {
	if g.String != nil {
		return *g.String
	}
	if g.Integer != nil {
		return strconv.FormatInt(*g.Integer, 10)
	}
	return ""
}

// ToString converts identifier path to string representation.
func (g *GrammarIdentifierPath) ToString(hasTypeSpec bool) (string, error) {
	// If the identifier is quoted (using String or QuotedIdent), return it directly without keyword checking
	if g.QuotedIdent != nil {
		// Validate that the quoted string contains a valid identifier pattern
		identPattern := `^[a-zA-Z_][a-zA-Z0-9_.]*$`
		matched, err := regexp.MatchString(identPattern, *g.QuotedIdent)
		if err != nil {
			return "", fmt.Errorf("failed to validate identifier: %w", err)
		}
		if !matched {
			return "", fmt.Errorf("quoted identifier %q contains invalid characters (only letters, digits, underscore, and dots allowed)", *g.QuotedIdent)
		}
		return *g.QuotedIdent, nil
	}

	if g.First == nil {
		return "", fmt.Errorf("identifier path is empty")
	}

	first := g.First.Value()
	if first == "" {
		return "", fmt.Errorf("identifier path is empty")
	}

	// if the identifier is a keyword, it must have a sub path or must have type spec(such as ::field or ::tag)
	if g.First.Keyword != nil && len(g.Rest) == 0 && !hasTypeSpec {
		return "", fmt.Errorf("identifier %q cannot be a keyword without a sub path", first)
	}

	if len(g.Rest) == 0 {
		return first, nil
	}

	parts := make([]string, 0, 1+len(g.Rest))
	parts = append(parts, first)
	for _, part := range g.Rest {
		parts = append(parts, part.Value())
	}
	return strings.Join(parts, "."), nil
}
