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

// Node represents a node in the Abstract Syntax Tree
type Node interface {
	String() string
}

// Statement represents a BydbQL statement
type Statement interface {
	Node
	statementNode()
}

// Expression represents a BydbQL expression
type Expression interface {
	Node
	expressionNode()
}

// SelectStatement represents a SELECT statement
type SelectStatement struct {
	Projection  *Projection
	From        *FromClause
	Time        *TimeCondition
	Where       *WhereClause
	GroupBy     *GroupByClause
	OrderBy     *OrderByClause
	Limit       *int
	Offset      *int
	QueryTrace  bool
}

func (s *SelectStatement) statementNode() {}
func (s *SelectStatement) String() string { return "SELECT" }

// TopNStatement represents a SHOW TOP N statement
type TopNStatement struct {
	TopN        int
	From        *FromClause
	Time        *TimeCondition
	Where       *WhereClause
	AggregateBy *AggregateFunction
	OrderBy     *OrderByClause
	QueryTrace  bool
}

func (s *TopNStatement) statementNode() {}
func (s *TopNStatement) String() string { return "SHOW TOP" }

// Projection represents the SELECT projection clause
type Projection struct {
	All     bool
	Empty   bool // SELECT () for traces
	Columns []*Column
	TopN    *TopNProjection
}

func (p *Projection) expressionNode() {}
func (p *Projection) String() string { return "projection" }

// TopNProjection represents TOP N projection
type TopNProjection struct {
	N           int
	Projection  *Projection
}

func (p *TopNProjection) expressionNode() {}
func (p *TopNProjection) String() string { return "TOP N" }

// Column represents a column in projection
type Column struct {
	Name      string
	Type      ColumnType // ::tag or ::field disambiguator
	Function  *AggregateFunction
}

func (c *Column) expressionNode() {}
func (c *Column) String() string { return c.Name }

// ColumnType represents the type disambiguator for columns
type ColumnType int

const (
	ColumnTypeAuto ColumnType = iota
	ColumnTypeTag
	ColumnTypeField
)

// AggregateFunction represents aggregate functions (SUM, MEAN, COUNT, MAX, MIN)
type AggregateFunction struct {
	Function string // SUM, MEAN, COUNT, MAX, MIN
	Column   string
}

func (f *AggregateFunction) expressionNode() {}
func (f *AggregateFunction) String() string { return f.Function }

// FromClause represents the FROM clause
type FromClause struct {
	ResourceType ResourceType
	ResourceName string
	Groups       []string
}

func (f *FromClause) expressionNode() {}
func (f *FromClause) String() string { return "FROM" }

// ResourceType represents the type of resource being queried
type ResourceType int

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

// TimeCondition represents TIME conditions
type TimeCondition struct {
	Operator  TimeOperator
	Timestamp string
	Begin     string // for BETWEEN
	End       string // for BETWEEN
}

func (t *TimeCondition) expressionNode() {}
func (t *TimeCondition) String() string { return "TIME" }

// TimeOperator represents time comparison operators
type TimeOperator int

const (
	TimeOpEqual TimeOperator = iota
	TimeOpGreater
	TimeOpLess
	TimeOpGreaterEqual
	TimeOpLessEqual
	TimeOpBetween
)

// WhereClause represents the WHERE clause
type WhereClause struct {
	Conditions []*Condition
	Logic      LogicOperator
}

func (w *WhereClause) expressionNode() {}
func (w *WhereClause) String() string { return "WHERE" }

// Condition represents a single condition in WHERE clause
type Condition struct {
	Left     string
	Operator BinaryOperator
	Right    *Value
	Values   []*Value // for IN, NOT IN
	Logic    LogicOperator // for combining conditions
}

func (c *Condition) expressionNode() {}
func (c *Condition) String() string { return c.Left }

// BinaryOperator represents binary operators
type BinaryOperator int

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

func (op BinaryOperator) String() string {
	switch op {
	case OpEqual:
		return "="
	case OpNotEqual:
		return "!="
	case OpGreater:
		return ">"
	case OpLess:
		return "<"
	case OpGreaterEqual:
		return ">="
	case OpLessEqual:
		return "<="
	case OpIn:
		return "IN"
	case OpNotIn:
		return "NOT IN"
	case OpHaving:
		return "HAVING"
	case OpNotHaving:
		return "NOT HAVING"
	case OpMatch:
		return "MATCH"
	default:
		return "UNKNOWN"
	}
}

// LogicOperator represents logical operators (AND, OR)
type LogicOperator int

const (
	LogicAnd LogicOperator = iota
	LogicOr
)

func (op LogicOperator) String() string {
	switch op {
	case LogicAnd:
		return "AND"
	case LogicOr:
		return "OR"
	default:
		return "UNKNOWN"
	}
}

// Value represents a value in conditions
type Value struct {
	Type     ValueType
	StringVal string
	Integer   int64
	IsNull    bool
}

func (v *Value) expressionNode() {}
func (v *Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case ValueTypeString:
		return v.StringVal
	case ValueTypeInteger:
		return string(rune(v.Integer))
	default:
		return ""
	}
}

// ValueType represents the type of a value
type ValueType int

const (
	ValueTypeString ValueType = iota
	ValueTypeInteger
	ValueTypeNull
)

// GroupByClause represents GROUP BY clause
type GroupByClause struct {
	Columns []string
}

func (g *GroupByClause) expressionNode() {}
func (g *GroupByClause) String() string { return "GROUP BY" }

// OrderByClause represents ORDER BY clause
type OrderByClause struct {
	Column string
	Desc   bool
}

func (o *OrderByClause) expressionNode() {}
func (o *OrderByClause) String() string { return "ORDER BY" }

// ParsedQuery represents a parsed BydbQL query with metadata
type ParsedQuery struct {
	Statement    Statement
	ResourceType ResourceType
	ResourceName string
	Groups       []string
	Context      *QueryContext
}

// QueryContext provides execution context for queries
type QueryContext struct {
	DefaultGroup        string
	DefaultResourceName string
	DefaultResourceType ResourceType
	CurrentTime         time.Time
}