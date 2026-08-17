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

package planner

import (
	"errors"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// The plan vocabulary below is the contract the agent submits through propose_query_plan: a closed
// set of enums and structs, so an unknown value is rejected before anything is compiled.

// AggregateFunction is a supported measure aggregation function.
type AggregateFunction string

// Aggregate functions.
const (
	AggregateMean  AggregateFunction = "MEAN"
	AggregateCount AggregateFunction = "COUNT"
	AggregateMax   AggregateFunction = "MAX"
	AggregateMin   AggregateFunction = "MIN"
	AggregateSum   AggregateFunction = "SUM"
)

// Operator is a supported deterministic filter operator.
type Operator string

// Filter operators.
const (
	OperatorEqual        Operator = "="
	OperatorNotEqual     Operator = "!="
	OperatorGreaterThan  Operator = ">"
	OperatorGreaterEqual Operator = ">="
	OperatorLessThan     Operator = "<"
	OperatorLessEqual    Operator = "<="
	OperatorIn           Operator = "IN"
	OperatorNotIn        Operator = "NOT IN"
	OperatorAnd          Operator = "AND"
	OperatorOr           Operator = "OR"
)

// OrderDirection is a supported order direction.
type OrderDirection string

// Order directions.
const (
	OrderAscending  OrderDirection = "ASC"
	OrderDescending OrderDirection = "DESC"
)

// Resource selects one BanyanDB queryable resource.
type Resource struct {
	Type   session.ResourceType `json:"type"`
	Name   string               `json:"name"`
	Groups []string             `json:"groups"`
}

// Aggregate selects a single numeric aggregation.
type Aggregate struct {
	Function AggregateFunction `json:"function"`
	Column   string            `json:"column"`
}

// Projection selects a column or aggregate.
type Projection struct {
	Aggregate *Aggregate `json:"aggregate,omitempty"`
	Column    string     `json:"column,omitempty"`
}

// ProjectionMode selects an implicit all-column or empty trace projection.
type ProjectionMode string

// Projection modes.
const (
	ProjectionModeAll  ProjectionMode = "ALL"
	ProjectionModeNone ProjectionMode = "NONE"
)

// Predicate is a typed comparison leaf or AND/OR expression tree.
type Predicate struct {
	Value    any         `json:"value,omitempty"`
	Column   string      `json:"column,omitempty"`
	Operator Operator    `json:"operator"`
	Children []Predicate `json:"children,omitempty"`
}

// Order specifies an index rule and direction. An empty index rule orders by time.
type Order struct {
	IndexRule string         `json:"index_rule,omitempty"`
	Direction OrderDirection `json:"direction"`
}

// TimeRange supplies BYDBQL-compatible bounds for a time-series query.
type TimeRange struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

// QueryPlan describes one query without embedding any BYDBQL text.
type QueryPlan struct {
	Filter         *Predicate     `json:"filter,omitempty"`
	Aggregate      *Aggregate     `json:"aggregate,omitempty"`
	OrderBy        *Order         `json:"order_by,omitempty"`
	TimeRange      TimeRange      `json:"time_range,omitempty"`
	ProjectionMode ProjectionMode `json:"projection_mode,omitempty"`
	ID             string         `json:"id,omitempty"`
	Resource       Resource       `json:"resource"`
	Projection     []Projection   `json:"projection,omitempty"`
	GroupBy        []string       `json:"group_by,omitempty"`
	Limit          int            `json:"limit,omitempty"`
	TopN           int            `json:"top_n,omitempty"`
}

// WorkflowPlan describes a sequence of independently approved query plans.
type WorkflowPlan struct {
	Steps []QueryPlan `json:"steps"`
}

// Diagnostic is a stable, machine-readable query-plan failure.
type Diagnostic struct {
	Code    string   `json:"code"`
	Path    string   `json:"path,omitempty"`
	Message string   `json:"message"`
	Allowed []string `json:"allowed,omitempty"`
}

// PlanError wraps a query-plan diagnostic as an error.
type PlanError struct {
	Diagnostic Diagnostic
}

// Error returns the human-readable diagnostic message.
func (planErr *PlanError) Error() string {
	return planErr.Diagnostic.Message
}

// DescribeError returns a stable diagnostic for any planner failure.
func DescribeError(planErr error) Diagnostic {
	var typedError *PlanError
	if errors.As(planErr, &typedError) {
		return typedError.Diagnostic
	}
	return Diagnostic{Code: "PLAN_SEMANTIC_ERROR", Message: planErr.Error()}
}

func diagnosticError(code, path, message string, allowed ...string) error {
	return &PlanError{Diagnostic: Diagnostic{
		Code:    code,
		Path:    path,
		Message: message,
		Allowed: append([]string(nil), allowed...),
	}}
}

func isAggregateFunction(function AggregateFunction) bool {
	switch function {
	case AggregateMean, AggregateCount, AggregateMax, AggregateMin, AggregateSum:
		return true
	default:
		return false
	}
}

func isComparisonOperator(operator Operator) bool {
	switch operator {
	case OperatorEqual, OperatorNotEqual, OperatorGreaterThan, OperatorGreaterEqual, OperatorLessThan, OperatorLessEqual, OperatorIn, OperatorNotIn:
		return true
	default:
		return false
	}
}

func isOrderDirection(direction OrderDirection) bool {
	return direction == OrderAscending || direction == OrderDescending
}

func isResourceType(resourceType session.ResourceType) bool {
	switch resourceType {
	case session.ResourceTypeMeasure, session.ResourceTypeStream, session.ResourceTypeTrace, session.ResourceTypeProperty, session.ResourceTypeTopN:
		return true
	default:
		return false
	}
}
