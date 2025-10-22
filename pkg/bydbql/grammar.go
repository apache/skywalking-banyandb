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
	From         string           `parser:"@'FROM'"`
	ResourceType string           `parser:"@('STREAM'|'MEASURE'|'TRACE'|'PROPERTY')"`
	ResourceName string           `parser:"@Ident"`
	In           *GrammarInClause `parser:"@@"`
}

// GrammarInClause represents IN clause.
type GrammarInClause struct {
	In     string   `parser:"@'IN'"`
	LParen bool     `parser:"@'('?"`
	Groups []string `parser:"@Ident ( ',' @Ident )*"`
	RParen bool     `parser:"@')'?"`
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

// Helper methods to convert grammar to AST.

func (g *Grammar) toAST() (Statement, error) {
	if g.Select != nil {
		return g.Select.toAST()
	}
	if g.TopN != nil {
		return g.TopN.toAST()
	}
	return nil, fmt.Errorf("invalid grammar: no statement found")
}

func (g *GrammarSelectStatement) toAST() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// Projection
	if g.Projection != nil {
		proj, err := g.Projection.toAST()
		if err != nil {
			return nil, err
		}
		stmt.Projection = proj
	}

	// From clause
	if g.From != nil {
		from, err := g.From.toAST()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	// Time clause
	if g.Time != nil {
		stmt.Time = g.Time.toAST()
	}

	// Where clause
	if g.Where != nil {
		where, err := g.Where.toAST()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// GroupBy clause
	if g.GroupBy != nil {
		groupBy, err := g.GroupBy.toAST()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// OrderBy clause
	if g.OrderBy != nil {
		orderBy, err := g.OrderBy.toAST()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Limit
	if g.Limit != nil {
		limit := g.Limit.Value
		stmt.Limit = &limit
	}

	// Offset
	if g.Offset != nil {
		offset := g.Offset.Value
		stmt.Offset = &offset
	}

	// Query trace
	if g.WithQueryTrace != nil {
		stmt.QueryTrace = true
	}

	return stmt, nil
}

func (g *GrammarTopNStatement) toAST() (*TopNStatement, error) {
	if g.N <= 0 {
		return nil, fmt.Errorf("TOP N requires a positive integer, got %d", g.N)
	}

	stmt := &TopNStatement{
		TopN: g.N,
	}

	// From clause
	if g.From != nil {
		from, err := g.From.toAST()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	// Time clause
	if g.Time != nil {
		stmt.Time = g.Time.toAST()
	}

	// Where clause
	if g.Where != nil {
		where, err := g.Where.toAST()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// AggregateBy
	if g.AggregateBy != nil && g.AggregateBy.Function != nil {
		stmt.AggregateBy = g.AggregateBy.Function.toAST()
	}

	// OrderBy clause
	if g.OrderBy != nil {
		stmt.OrderBy = g.OrderBy.toAST()
	}

	// Query trace
	if g.WithQueryTrace != nil {
		stmt.QueryTrace = true
	}

	return stmt, nil
}

func (g *GrammarProjection) toAST() (*Projection, error) {
	proj := &Projection{
		All:   g.All,
		Empty: g.Empty,
	}

	if g.TopN != nil {
		topN, err := g.TopN.toAST()
		if err != nil {
			return nil, err
		}
		proj.TopN = topN

		if g.TopN.OtherColumns != nil {
			g.Columns = append(g.Columns, g.TopN.OtherColumns...)
		}
	}

	if g.Columns != nil {
		cols, err := parseColumnsToAST(g.Columns)
		if err != nil {
			return nil, err
		}

		proj.Columns = cols
	}

	return proj, nil
}

func parseColumnsToAST(columns []*GrammarColumn) ([]*Column, error) {
	cols := make([]*Column, 0, len(columns))
	aggregateCount := 0
	for _, col := range columns {
		c, err := col.toAST()
		if err != nil {
			return nil, err
		}
		// Count aggregate functions
		if c.Function != nil {
			aggregateCount++
		}
		cols = append(cols, c)
	}

	// Validate that there is at most one aggregate function
	if aggregateCount > 1 {
		return nil, fmt.Errorf("only one aggregate function is allowed per SELECT query, found %d", aggregateCount)
	}

	return cols, nil
}

func (g *GrammarTopNProjection) toAST() (*TopNProjection, error) {
	if g.OrderField == nil {
		return nil, fmt.Errorf("TOP N requires an order field")
	}

	orderFieldName, err := g.OrderField.toString(true) // the field can be a keywork name(such as some some field named count)
	if err != nil {
		return nil, err
	}
	if g.N < 0 {
		return nil, fmt.Errorf("TOP N requires a non-negative integer, got %d", g.N)
	}

	topN := &TopNProjection{
		N:          g.N,
		OrderField: orderFieldName,
		Desc:       false,
	}

	if g.Direction != nil {
		topN.Desc = strings.EqualFold(*g.Direction, "DESC")
	}

	return topN, nil
}

func (g *GrammarColumn) toAST() (*Column, error) {
	col := &Column{Type: ColumnTypeAuto}

	if g.Aggregate != nil {
		fn, err := g.Aggregate.toAST()
		if err != nil {
			return nil, err
		}
		col.Function = fn
	} else if g.Identifier != nil {
		name, err := g.Identifier.toString(g.TypeSpec != nil)
		if err != nil {
			return nil, err
		}
		col.Name = name
	}

	if g.TypeSpec != nil {
		switch strings.ToUpper(*g.TypeSpec) {
		case "TAG":
			col.Type = ColumnTypeTag
		case "FIELD":
			col.Type = ColumnTypeField
		}
	}

	return col, nil
}

func (g *GrammarGroupByColumn) toAST() (*GroupedColumn, error) {
	col := &GroupedColumn{Type: ColumnTypeAuto}

	if g.Identifier == nil {
		return nil, fmt.Errorf("group by column must have an identifier")
	}
	name, err := g.Identifier.toString(g.TypeSpec != nil)
	if err != nil {
		return nil, err
	}
	col.Name = name

	if g.TypeSpec != nil {
		switch strings.ToUpper(*g.TypeSpec) {
		case "TAG":
			col.Type = ColumnTypeTag
		case "FIELD":
			col.Type = ColumnTypeField
		}
	}

	return col, nil
}

func (g *GrammarAggregateFunction) toAST() (*AggregateFunction, error) {
	column, err := g.Column.toString(true) // the aggregate function column can be a keyword name(such as some some field named count)
	if err != nil {
		return nil, err
	}

	return &AggregateFunction{
		Function: strings.ToUpper(g.Function),
		Column:   column,
	}, nil
}

func (g *GrammarTopNAggregateFunction) toAST() *AggregateFunction {
	return &AggregateFunction{
		Function: strings.ToUpper(g.Function),
	}
}

func (g *GrammarFromClause) toAST() (*FromClause, error) {
	from := &FromClause{
		ResourceName: g.ResourceName,
	}

	// Parse resource type
	switch strings.ToUpper(g.ResourceType) {
	case "STREAM":
		from.ResourceType = ResourceTypeStream
	case "MEASURE":
		from.ResourceType = ResourceTypeMeasure
	case "TRACE":
		from.ResourceType = ResourceTypeTrace
	case "PROPERTY":
		from.ResourceType = ResourceTypeProperty
	default:
		return nil, fmt.Errorf("unknown resource type: %s", g.ResourceType)
	}

	// Parse groups
	if g.In != nil {
		if g.In.LParen && !g.In.RParen {
			return nil, fmt.Errorf("missing closing parenthesis in IN clause")
		}
		from.Groups = g.In.Groups
	}

	return from, nil
}

func (g *GrammarTimeClause) toAST() *TimeCondition {
	time := &TimeCondition{}

	if g.Between != nil {
		time.Operator = TimeOpBetween
		if g.Between.Begin != nil {
			time.Begin = g.Between.Begin.toString()
		}
		if g.Between.End != nil {
			time.End = g.Between.End.toString()
		}
	} else if g.Comparator != nil && g.Value != nil {
		switch *g.Comparator {
		case "=":
			time.Operator = TimeOpEqual
		case ">":
			time.Operator = TimeOpGreater
		case "<":
			time.Operator = TimeOpLess
		case ">=":
			time.Operator = TimeOpGreaterEqual
		case "<=":
			time.Operator = TimeOpLessEqual
		}
		time.Timestamp = g.Value.toString()
	}

	return time
}

func (g *GrammarTimeValue) toString() string {
	if g.String != nil {
		return *g.String
	}
	if g.Integer != nil {
		return strconv.FormatInt(*g.Integer, 10)
	}
	return ""
}

func (g *GrammarSelectWhereClause) toAST() (*WhereClause, error) {
	if g.Expr == nil {
		return nil, nil
	}

	expr, err := g.Expr.toAST()
	if err != nil {
		return nil, err
	}

	return &WhereClause{Expr: expr}, nil
}

func (g *GrammarTopNWhereClause) toAST() (*WhereClause, error) {
	if g.Expr == nil {
		return nil, nil
	}

	expr, err := g.Expr.toAST()
	if err != nil {
		return nil, err
	}

	return &WhereClause{Expr: expr}, nil
}

func (g *GrammarOrExpr) toAST() (ConditionExpr, error) {
	left, err := g.Left.toAST()
	if err != nil {
		return nil, err
	}

	// Build left-associative tree for OR operations
	result := left
	for _, right := range g.Right {
		rightExpr, err := right.Right.toAST()
		if err != nil {
			return nil, err
		}
		result = &BinaryLogicExpr{
			Left:     result,
			Operator: LogicOr,
			Right:    rightExpr,
		}
	}

	return result, nil
}

func (g *GrammarAndExpr) toAST() (ConditionExpr, error) {
	left, err := g.Left.toAST()
	if err != nil {
		return nil, err
	}

	// Build left-associative tree for AND operations
	result := left
	for _, right := range g.Right {
		rightExpr, err := right.Right.toAST()
		if err != nil {
			return nil, err
		}
		result = &BinaryLogicExpr{
			Left:     result,
			Operator: LogicAnd,
			Right:    rightExpr,
		}
	}

	return result, nil
}

func (g *GrammarPredicate) toAST() (ConditionExpr, error) {
	if g.Paren != nil {
		return g.Paren.toAST()
	}
	if g.Binary != nil {
		return g.Binary.toAST()
	}
	if g.In != nil {
		return g.In.toAST()
	}
	if g.Having != nil {
		return g.Having.toAST()
	}
	return nil, fmt.Errorf("invalid predicate")
}

func (g *GrammarBinaryPredicate) toAST() (*Condition, error) {
	if g.Identifier == nil || g.Tail == nil {
		return nil, fmt.Errorf("invalid binary predicate")
	}

	left, err := g.Identifier.toString(false)
	if err != nil {
		return nil, err
	}

	cond := &Condition{Left: left}

	switch {
	case g.Tail.Match != nil:
		matchOpt, err := g.Tail.Match.toAST()
		if err != nil {
			return nil, err
		}
		cond.Operator = OpMatch
		cond.MatchOption = matchOpt
	case g.Tail.Compare != nil:
		switch g.Tail.Compare.Operator {
		case "=":
			cond.Operator = OpEqual
		case "!=":
			cond.Operator = OpNotEqual
		case ">":
			cond.Operator = OpGreater
		case "<":
			cond.Operator = OpLess
		case ">=":
			cond.Operator = OpGreaterEqual
		case "<=":
			cond.Operator = OpLessEqual
		default:
			return nil, fmt.Errorf("unknown operator: %s", g.Tail.Compare.Operator)
		}

		val, err := g.Tail.Compare.Value.toAST()
		if err != nil {
			return nil, err
		}
		cond.Right = val
	default:
		return nil, fmt.Errorf("invalid binary predicate tail")
	}

	return cond, nil
}

func (g *GrammarInPredicate) toAST() (*Condition, error) {
	left, err := g.Identifier.toString(false)
	if err != nil {
		return nil, err
	}

	cond := &Condition{Left: left}

	if g.Not != nil {
		cond.Operator = OpNotIn
	} else {
		cond.Operator = OpIn
	}

	// Parse values
	values := make([]*Value, 0, len(g.Values))
	for _, v := range g.Values {
		val, err := v.toAST()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	cond.Values = values

	return cond, nil
}

func (g *GrammarHavingPredicate) toAST() (*Condition, error) {
	left, err := g.Identifier.toString(false)
	if err != nil {
		return nil, err
	}

	cond := &Condition{Left: left}

	if g.Not != nil {
		cond.Operator = OpNotHaving
	} else {
		cond.Operator = OpHaving
	}

	// Parse values
	if g.Values == nil {
		return nil, fmt.Errorf("HAVING requires values")
	}

	switch {
	case g.Values.Single != nil:
		val, err := g.Values.Single.toAST()
		if err != nil {
			return nil, err
		}
		cond.Right = val
	case len(g.Values.Array) > 0:
		values := make([]*Value, 0, len(g.Values.Array))
		for _, v := range g.Values.Array {
			val, err := v.toAST()
			if err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		cond.Values = values
	default:
		return nil, fmt.Errorf("HAVING requires at least one value")
	}

	return cond, nil
}

func (g *GrammarValue) toAST() (*Value, error) {
	if g.Null {
		return &Value{
			Type:   ValueTypeNull,
			IsNull: true,
		}, nil
	}

	if g.String != nil {
		return &Value{
			Type:      ValueTypeString,
			StringVal: *g.String,
		}, nil
	}

	if g.Integer != nil {
		return &Value{
			Type:    ValueTypeInteger,
			Integer: *g.Integer,
		}, nil
	}

	return nil, fmt.Errorf("invalid value")
}

func (g *GrammarMatchTail) toAST() (*MatchOption, error) {
	matchOpt := &MatchOption{}

	if g.Values != nil {
		if g.Values.Single != nil {
			val, err := g.Values.Single.toAST()
			if err != nil {
				return nil, err
			}
			matchOpt.Values = []*Value{val}
		} else if g.Values.Array != nil {
			values := make([]*Value, 0, len(g.Values.Array))
			for _, v := range g.Values.Array {
				val, err := v.toAST()
				if err != nil {
					return nil, err
				}
				values = append(values, val)
			}
			matchOpt.Values = values
		}
	}

	if g.Analyzer != nil {
		matchOpt.Analyzer = *g.Analyzer
	}

	if g.Operator != nil {
		matchOpt.Operator = strings.ToUpper(*g.Operator)
	}

	return matchOpt, nil
}

func (g *GrammarIdentifierPath) toString(hasTypeSpec bool) (string, error) {
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

func (g *GrammarGroupByClause) toAST() (*GroupByClause, error) {
	columns := make([]*GroupedColumn, 0, len(g.Columns))
	for _, col := range g.Columns {
		name, err := col.toAST()
		if err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return &GroupByClause{Columns: columns}, nil
}

func (g *GrammarSelectOrderByClause) toAST() (*OrderByClause, error) {
	order := &OrderByClause{}

	switch {
	case g.Tail.DirOnly != nil:
		dir := strings.ToUpper(*g.Tail.DirOnly)
		order.Desc = dir == "DESC"
	case g.Tail.WithIdent != nil:
		name, err := g.Tail.WithIdent.Identifier.toString(true)
		if err != nil {
			return nil, err
		}
		order.Column = name
		if strings.EqualFold(order.Column, "value") {
			order.Column = "value"
		}
		if g.Tail.WithIdent.Direction != nil {
			dir := strings.ToUpper(*g.Tail.WithIdent.Direction)
			order.Desc = dir == "DESC"
		}
	default:
		return nil, fmt.Errorf("ORDER BY clause requires a column or direction")
	}

	return order, nil
}

func (g *GrammarTopNOrderByClause) toAST() *OrderByClause {
	order := &OrderByClause{}

	if g.Dir != nil {
		dir := strings.ToUpper(*g.Dir)
		order.Desc = dir == "DESC"
	}

	return order
}
