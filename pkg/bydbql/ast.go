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
package bydbql

import "time"

// Node represents a node in the Abstract Syntax Tree.
type Node interface{}

// Statement represents a BydbQL statement.
type Statement interface {
	Node
	statementNode()
}

// Expression represents a BydbQL expression.
type Expression interface {
	Node
	expressionNode()
}

// SelectStatement represents a SELECT statement.
type SelectStatement struct {
	Projection *Projection
	From       *FromClause
	Time       *TimeCondition
	Where      *WhereClause
	GroupBy    *GroupByClause
	OrderBy    *OrderByClause
	Limit      *int
	Offset     *int
	QueryTrace bool
}

func (s *SelectStatement) statementNode() {}

// TopNStatement represents a SHOW TOP N statement.
type TopNStatement struct {
	From        *FromClause
	Time        *TimeCondition
	Where       *WhereClause
	AggregateBy *AggregateFunction
	OrderBy     *OrderByClause
	TopN        int
	QueryTrace  bool
}

func (s *TopNStatement) statementNode() {}

// Projection represents the SELECT projection clause.
type Projection struct {
	TopN    *TopNProjection
	Columns []*Column
	All     bool
	Empty   bool
}

func (p *Projection) expressionNode() {}

// TopNProjection represents TOP N projection.
type TopNProjection struct {
	OrderField string
	N          int
	Desc       bool
}

func (p *TopNProjection) expressionNode() {}

// Column represents a column in projection.
type Column struct {
	Function *AggregateFunction
	Name     string
	Type     ColumnType
}

func (c *Column) expressionNode() {}

// GroupedColumn represents a column in GROUP BY clause.
type GroupedColumn struct {
	Name string
	Type ColumnType
}

// ColumnType represents the type disambiguator for columns.
type ColumnType int

// Possible values are AUTO, TAG, FIELD.
const (
	ColumnTypeAuto ColumnType = iota
	ColumnTypeTag
	ColumnTypeField
)

// AggregateFunction represents aggregate functions (SUM, MEAN, COUNT, MAX, MIN).
type AggregateFunction struct {
	Function string // SUM, MEAN, COUNT, MAX, MIN
	Column   string
}

func (f *AggregateFunction) expressionNode() {}

// FromClause represents the FROM clause.
type FromClause struct {
	ResourceName string
	Groups       []string
	ResourceType ResourceType
}

func (f *FromClause) expressionNode() {}

// ResourceType represents the type of resource being queried.
type ResourceType int

// Possible values are AUTO, STREAM, MEASURE, TRACE, PROPERTY.
const (
	ResourceTypeAuto ResourceType = iota
	ResourceTypeStream
	ResourceTypeMeasure
	ResourceTypeTrace
	ResourceTypeProperty
)

func (r ResourceType) String() string {
	switch r {
	case ResourceTypeStream:
		return "STREAM"
	case ResourceTypeMeasure:
		return "MEASURE"
	case ResourceTypeTrace:
		return "TRACE"
	case ResourceTypeProperty:
		return "PROPERTY"
	default:
		return "AUTO"
	}
}

// TimeCondition represents TIME conditions.
type TimeCondition struct {
	Timestamp string
	Begin     string
	End       string
	Operator  TimeOperator
}

func (t *TimeCondition) expressionNode() {}

// TimeOperator represents time comparison operators.
type TimeOperator int

// Possible values are EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, BETWEEN.
const (
	TimeOpEqual TimeOperator = iota
	TimeOpGreater
	TimeOpLess
	TimeOpGreaterEqual
	TimeOpLessEqual
	TimeOpBetween
)

// WhereClause represents the WHERE clause with an expression tree.
type WhereClause struct {
	Expr ConditionExpr
}

func (w *WhereClause) expressionNode() {}

// ConditionExpr represents a condition expression node in the expression tree.
type ConditionExpr interface {
	Expression
	conditionExprNode()
}

// BinaryLogicExpr represents a binary logical expression (AND/OR) in the condition tree.
type BinaryLogicExpr struct {
	Left     ConditionExpr
	Right    ConditionExpr
	Operator LogicOperator
}

func (b *BinaryLogicExpr) expressionNode()    {}
func (b *BinaryLogicExpr) conditionExprNode() {}

// Condition represents a single condition (leaf node in the condition tree).
type Condition struct {
	Right       *Value
	MatchOption *MatchOption
	Left        string
	Values      []*Value
	Operator    BinaryOperator
}

func (c *Condition) expressionNode()    {}
func (c *Condition) conditionExprNode() {}

// MatchOption represents options for MATCH operator.
type MatchOption struct {
	Analyzer string
	Operator string
	Values   []*Value
}

func (m *MatchOption) expressionNode() {}

// BinaryOperator represents binary operators.
type BinaryOperator int

// Possible values are EQUAL, NOT_EQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL, IN, NOT_IN, HAVING, NOT_HAVING, MATCH.
const (
	OpEqual BinaryOperator = iota
	OpNotEqual
	OpGreater
	OpLess
	OpGreaterEqual
	OpLessEqual
	OpIn
	OpNotIn
	OpHaving
	OpNotHaving
	OpMatch
)

// LogicOperator represents logical operators (AND, OR).
type LogicOperator int

// Possible values are AND, OR.
const (
	LogicAnd LogicOperator = iota
	LogicOr
)

// Value represents a value in conditions.
type Value struct {
	StringVal string
	Type      ValueType
	Integer   int64
	IsNull    bool
}

func (v *Value) expressionNode() {}

// ValueType represents the type of a value.
type ValueType int

// Possible values are STRING, INTEGER, NULL.
const (
	ValueTypeString ValueType = iota
	ValueTypeInteger
	ValueTypeNull
)

// GroupByClause represents GROUP BY clause.
type GroupByClause struct {
	Columns []*GroupedColumn
}

func (g *GroupByClause) expressionNode() {}

// OrderByClause represents ORDER BY clause.
type OrderByClause struct {
	Column string
	Desc   bool
}

func (o *OrderByClause) expressionNode() {}

// ParsedQuery represents a parsed BydbQL query with metadata.
type ParsedQuery struct {
	Statement    Statement
	Context      *QueryContext
	ResourceName string
	Groups       []string
	ResourceType ResourceType
}

// QueryContext provides execution context for queries.
type QueryContext struct {
	CurrentTime time.Time
}
