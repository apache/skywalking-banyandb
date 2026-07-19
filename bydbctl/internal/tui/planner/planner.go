// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package planner validates structured query plans and deterministically renders BYDBQL.
package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

const (
	defaultLimit     = 10
	defaultTimeStart = "-30m"
	maximumLimit     = 1000
)

var (
	identifierPattern   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)
	relativeTimePattern = regexp.MustCompile(`^-[1-9][0-9]*[mhdw]$`)
)

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
	Column    string     `json:"column,omitempty"`
	Aggregate *Aggregate `json:"aggregate,omitempty"`
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
	Children []Predicate `json:"children,omitempty"`
	Column   string      `json:"column,omitempty"`
	Operator Operator    `json:"operator"`
	Value    any         `json:"value,omitempty"`
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
	Resource       Resource       `json:"resource"`
	Projection     []Projection   `json:"projection,omitempty"`
	ProjectionMode ProjectionMode `json:"projection_mode,omitempty"`
	Filter         *Predicate     `json:"filter,omitempty"`
	Aggregate      *Aggregate     `json:"aggregate,omitempty"`
	OrderBy        *Order         `json:"order_by,omitempty"`
	TimeRange      TimeRange      `json:"time_range,omitempty"`
	GroupBy        []string       `json:"group_by,omitempty"`
	ID             string         `json:"id,omitempty"`
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

// CompiledQuery is a validated deterministic BYDBQL query ready for local validation.
type CompiledQuery struct {
	Resource Resource
	ID       string
	Query    string
}

// Compile validates one structured query plan against a schema and renders BYDBQL.
func Compile(plan QueryPlan, schema session.SchemaSnapshot) (CompiledQuery, error) {
	if resourceErr := validateResource(plan.Resource, schema); resourceErr != nil {
		return CompiledQuery{}, resourceErr
	}
	if plan.Resource.Type == session.ResourceTypeTopN {
		query, topNErr := compileTopN(plan, schema)
		if topNErr != nil {
			return CompiledQuery{}, topNErr
		}
		return CompiledQuery{ID: plan.ID, Resource: plan.Resource, Query: query}, nil
	}
	if shapeErr := validateSelectShape(plan); shapeErr != nil {
		return CompiledQuery{}, shapeErr
	}
	query, selectErr := compileSelect(plan, schema)
	if selectErr != nil {
		return CompiledQuery{}, selectErr
	}
	return CompiledQuery{ID: plan.ID, Resource: plan.Resource, Query: query}, nil
}

func validateSelectShape(plan QueryPlan) error {
	if plan.TopN != 0 {
		return fmt.Errorf("SELECT plans cannot set top_n")
	}
	if plan.Resource.Type == session.ResourceTypeProperty && (plan.TimeRange.Start != "" || plan.TimeRange.End != "") {
		return fmt.Errorf("PROPERTY plans cannot set time_range")
	}
	if plan.TimeRange.Start == "" && plan.TimeRange.End != "" {
		return fmt.Errorf("time_range.end requires time_range.start")
	}
	if plan.ProjectionMode != "" && plan.ProjectionMode != ProjectionModeAll && plan.ProjectionMode != ProjectionModeNone {
		return fmt.Errorf("unsupported projection_mode %q", plan.ProjectionMode)
	}
	if plan.ProjectionMode != "" && len(plan.Projection) != 0 {
		return fmt.Errorf("projection_mode cannot be combined with explicit projections")
	}
	if plan.ProjectionMode == ProjectionModeNone && plan.Resource.Type != session.ResourceTypeTrace {
		return fmt.Errorf("projection_mode NONE is supported only for TRACE queries")
	}
	aggregateCount := 0
	if plan.Aggregate != nil {
		aggregateCount++
	}
	for projectionIndex, projection := range plan.Projection {
		hasColumn := strings.TrimSpace(projection.Column) != ""
		hasAggregate := projection.Aggregate != nil
		if hasColumn == hasAggregate {
			return fmt.Errorf("projection %d requires exactly one of column or aggregate", projectionIndex+1)
		}
		if projection.Aggregate != nil {
			aggregateCount++
		}
	}
	if aggregateCount > 1 {
		return fmt.Errorf("a deterministic query plan supports at most one aggregate")
	}
	if plan.Resource.Type != session.ResourceTypeMeasure && aggregateCount != 0 {
		return fmt.Errorf("aggregations are supported only for MEASURE queries")
	}
	if plan.Resource.Type != session.ResourceTypeMeasure && len(plan.GroupBy) != 0 {
		return fmt.Errorf("GROUP BY is supported only for MEASURE queries")
	}
	if len(plan.GroupBy) != 0 && aggregateCount != 1 {
		return fmt.Errorf("GROUP BY requires exactly one aggregate")
	}
	if plan.ProjectionMode == ProjectionModeNone && aggregateCount != 0 {
		return fmt.Errorf("projection_mode NONE cannot be combined with an aggregate")
	}
	return nil
}

func compileSelect(plan QueryPlan, schema session.SchemaSnapshot) (string, error) {
	projections, projectionErr := compileProjections(plan.Projection, plan.ProjectionMode, plan.Aggregate, plan.Resource, schema)
	if projectionErr != nil {
		return "", projectionErr
	}
	groups, groupsErr := compileGroups(plan.GroupBy, plan.Projection, plan.ProjectionMode, schema)
	if groupsErr != nil {
		return "", groupsErr
	}
	filter, filterErr := compileFilter(plan.Filter, schema)
	if filterErr != nil {
		return "", filterErr
	}
	order, orderErr := compileOrder(plan.OrderBy, schema)
	if orderErr != nil {
		return "", orderErr
	}
	limit, limitErr := compileLimit(plan.Limit)
	if limitErr != nil {
		return "", limitErr
	}
	parts := []string{"SELECT " + projections, "FROM " + string(plan.Resource.Type), plan.Resource.Name, "IN", groupExpression(plan.Resource.Groups)}
	if plan.Resource.Type != session.ResourceTypeProperty {
		timeExpression, timeErr := compileTimeRange(plan.TimeRange)
		if timeErr != nil {
			return "", timeErr
		}
		parts = append(parts, timeExpression)
	}
	if filter != "" {
		parts = append(parts, "WHERE "+filter)
	}
	if groups != "" {
		parts = append(parts, "GROUP BY "+groups)
	}
	if order != "" {
		parts = append(parts, "ORDER BY "+order)
	}
	parts = append(parts, fmt.Sprintf("LIMIT %d", limit))
	return strings.Join(parts, " "), nil
}

func compileTopN(plan QueryPlan, schema session.SchemaSnapshot) (string, error) {
	if len(plan.Projection) != 0 || plan.ProjectionMode != "" || len(plan.GroupBy) != 0 || plan.Limit != 0 {
		return "", fmt.Errorf("TOPN plans do not support projection, projection_mode, group_by, or limit")
	}
	if plan.Aggregate != nil && strings.TrimSpace(plan.Aggregate.Column) != "" {
		return "", fmt.Errorf("TOPN aggregation cannot select a column")
	}
	if plan.OrderBy != nil && strings.TrimSpace(plan.OrderBy.IndexRule) != "" {
		return "", fmt.Errorf("TOPN order cannot select an index rule")
	}
	topN := plan.TopN
	if topN < 1 {
		return "", fmt.Errorf("top_n must be greater than zero")
	}
	if topN > maximumLimit {
		return "", fmt.Errorf("top_n cannot exceed %d", maximumLimit)
	}
	function := AggregateSum
	if plan.Aggregate != nil {
		function = plan.Aggregate.Function
	}
	if !isAggregateFunction(function) {
		return "", fmt.Errorf("unsupported TOPN aggregation %q", function)
	}
	direction := OrderDescending
	if plan.OrderBy != nil {
		direction = plan.OrderBy.Direction
	}
	if !isOrderDirection(direction) {
		return "", fmt.Errorf("unsupported TOPN order direction %q", direction)
	}
	if directionErr := validateTopNDirection(direction, schema.FieldValueSort); directionErr != nil {
		return "", directionErr
	}
	timeExpression, timeErr := compileTimeRange(plan.TimeRange)
	if timeErr != nil {
		return "", timeErr
	}
	filter, filterErr := compileTopNFilter(plan.Filter, schema)
	if filterErr != nil {
		return "", filterErr
	}
	parts := []string{
		fmt.Sprintf("SHOW TOP %d", topN),
		"FROM MEASURE " + plan.Resource.Name,
		"IN " + groupExpression(plan.Resource.Groups),
		timeExpression,
	}
	if filter != "" {
		parts = append(parts, "WHERE "+filter)
	}
	parts = append(parts, "AGGREGATE BY "+string(function), "ORDER BY "+string(direction))
	return strings.Join(parts, " "), nil
}

func compileProjections(
	projections []Projection,
	projectionMode ProjectionMode,
	aggregate *Aggregate,
	resource Resource,
	schema session.SchemaSnapshot,
) (string, error) {
	if projectionMode == ProjectionModeNone {
		return "()", nil
	}
	if projectionMode == ProjectionModeAll {
		return "*", nil
	}
	if aggregate != nil {
		projections = append(append([]Projection(nil), projections...), Projection{Aggregate: aggregate})
	}
	if len(projections) == 0 {
		return "*", nil
	}
	compiled := make([]string, 0, len(projections))
	for _, projection := range projections {
		if projection.Aggregate != nil {
			value, aggregateErr := compileAggregate(*projection.Aggregate, resource, schema)
			if aggregateErr != nil {
				return "", aggregateErr
			}
			compiled = append(compiled, value)
			continue
		}
		columnName := strings.TrimSpace(projection.Column)
		if columnName == "*" {
			if len(projections) != 1 {
				return "", fmt.Errorf("wildcard projection cannot be combined with other projections")
			}
			return "*", nil
		}
		column, columnErr := typedColumn(columnName, schema)
		if columnErr != nil {
			return "", columnErr
		}
		compiled = append(compiled, column.Name)
	}
	return strings.Join(compiled, ", "), nil
}

func compileAggregate(aggregate Aggregate, resource Resource, schema session.SchemaSnapshot) (string, error) {
	if resource.Type != session.ResourceTypeMeasure {
		return "", fmt.Errorf("aggregations are supported only for MEASURE queries")
	}
	if !isAggregateFunction(aggregate.Function) {
		return "", fmt.Errorf("unsupported aggregation %q", aggregate.Function)
	}
	column, columnErr := typedColumn(aggregate.Column, schema)
	if columnErr != nil {
		return "", columnErr
	}
	if column.Kind != session.SchemaColumnField {
		return "", diagnosticError("AGGREGATE_COLUMN_NOT_FIELD", "/aggregate/column", fmt.Sprintf("aggregation column %q must be a field", aggregate.Column))
	}
	if column.Type != session.SchemaValueTypeInt && column.Type != session.SchemaValueTypeFloat {
		return "", diagnosticError("AGGREGATE_FIELD_NOT_NUMERIC", "/aggregate/column", fmt.Sprintf("aggregation field %q must be numeric", aggregate.Column))
	}
	return fmt.Sprintf("%s(%s)", aggregate.Function, column.Name), nil
}

func compileGroups(groups []string, projections []Projection, projectionMode ProjectionMode, schema session.SchemaSnapshot) (string, error) {
	if len(groups) == 0 {
		return "", nil
	}
	compiled := make([]string, 0, len(groups))
	fieldCount := 0
	for _, group := range groups {
		column, columnErr := typedColumn(group, schema)
		if columnErr != nil {
			return "", columnErr
		}
		if projectionMode != ProjectionModeAll && !projectionContainsColumn(projections, column.Name) {
			return "", fmt.Errorf("GROUP BY column %q must also be projected", column.Name)
		}
		if column.Kind == session.SchemaColumnField {
			fieldCount++
			if fieldCount > 1 {
				return "", fmt.Errorf("GROUP BY supports at most one field")
			}
			compiled = append(compiled, column.Name+"::FIELD")
			continue
		}
		if column.Kind != session.SchemaColumnTag && column.Kind != session.SchemaColumnEntityTag {
			return "", fmt.Errorf("GROUP BY column %q must be a tag or field", column.Name)
		}
		compiled = append(compiled, column.Name+"::TAG")
	}
	return strings.Join(compiled, ", "), nil
}

func projectionContainsColumn(projections []Projection, columnName string) bool {
	for _, projection := range projections {
		if strings.TrimSpace(projection.Column) == columnName {
			return true
		}
	}
	return false
}

func compileFilter(predicate *Predicate, schema session.SchemaSnapshot) (string, error) {
	if predicate == nil {
		return "", nil
	}
	if predicate.Operator == OperatorAnd || predicate.Operator == OperatorOr {
		return compilePredicateSet(*predicate, schema)
	}
	if len(predicate.Children) != 0 {
		return "", fmt.Errorf("comparison predicate cannot contain children")
	}
	return compileComparison(*predicate, schema)
}

func compilePredicateSet(predicate Predicate, schema session.SchemaSnapshot) (string, error) {
	if len(predicate.Children) < 2 {
		return "", fmt.Errorf("%s predicate requires at least two children", predicate.Operator)
	}
	parts := make([]string, 0, len(predicate.Children))
	for _, child := range predicate.Children {
		part, childErr := compileFilter(&child, schema)
		if childErr != nil {
			return "", childErr
		}
		parts = append(parts, "("+part+")")
	}
	return strings.Join(parts, " "+string(predicate.Operator)+" "), nil
}

func compileComparison(predicate Predicate, schema session.SchemaSnapshot) (string, error) {
	if !isComparisonOperator(predicate.Operator) {
		return "", fmt.Errorf("unsupported filter operator %q", predicate.Operator)
	}
	column, columnErr := filterColumn(predicate.Column, schema)
	if columnErr != nil {
		return "", columnErr
	}
	if predicate.Operator == OperatorIn || predicate.Operator == OperatorNotIn {
		values, valuesOK := predicate.Value.([]any)
		if !valuesOK || len(values) == 0 {
			return "", fmt.Errorf("%s requires a non-empty value array", predicate.Operator)
		}
		compiledValues := make([]string, 0, len(values))
		for _, value := range values {
			compiledValue, valueErr := compileValue(value, column.Type)
			if valueErr != nil {
				return "", valueErr
			}
			compiledValues = append(compiledValues, compiledValue)
		}
		return fmt.Sprintf("%s %s (%s)", column.Name, predicate.Operator, strings.Join(compiledValues, ", ")), nil
	}
	compiledValue, valueErr := compileValue(predicate.Value, column.Type)
	if valueErr != nil {
		return "", valueErr
	}
	return fmt.Sprintf("%s %s %s", column.Name, predicate.Operator, compiledValue), nil
}

func filterColumn(columnName string, schema session.SchemaSnapshot) (session.SchemaColumn, error) {
	if schema.Type == session.ResourceTypeProperty && strings.TrimSpace(columnName) == "ID" {
		return session.SchemaColumn{Name: "ID", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString}, nil
	}
	column, columnErr := typedColumn(columnName, schema)
	if columnErr != nil {
		return session.SchemaColumn{}, columnErr
	}
	if column.Kind != session.SchemaColumnTag && column.Kind != session.SchemaColumnEntityTag {
		return session.SchemaColumn{}, diagnosticError("FILTER_COLUMN_NOT_TAG", "/filter/column", fmt.Sprintf("filter column %q must be a tag", columnName))
	}
	return column, nil
}

func compileTopNFilter(predicate *Predicate, schema session.SchemaSnapshot) (string, error) {
	if predicate == nil {
		return "", nil
	}
	if predicate.Operator == OperatorOr {
		return "", fmt.Errorf("TOPN filters do not support OR")
	}
	if predicate.Operator == OperatorAnd {
		if len(predicate.Children) < 2 {
			return "", fmt.Errorf("TOPN AND predicate requires at least two children")
		}
		parts := make([]string, 0, len(predicate.Children))
		for _, child := range predicate.Children {
			part, childErr := compileTopNFilter(&child, schema)
			if childErr != nil {
				return "", childErr
			}
			parts = append(parts, part)
		}
		return strings.Join(parts, " AND "), nil
	}
	if len(predicate.Children) != 0 {
		return "", fmt.Errorf("TOPN comparison predicate cannot contain children")
	}
	if predicate.Operator != OperatorEqual {
		return "", diagnosticError(
			"TOPN_FILTER_OPERATOR_UNSUPPORTED",
			"/filter/operator",
			fmt.Sprintf("TOPN filter operator %q is unsupported; use =", predicate.Operator),
			string(OperatorEqual),
		)
	}
	column, columnErr := filterColumn(predicate.Column, schema)
	if columnErr != nil {
		return "", columnErr
	}
	if !containsExact(schema.EntityTags, column.Name) {
		return "", diagnosticError(
			"TOPN_FILTER_NOT_ENTITY_TAG",
			"/filter/column",
			fmt.Sprintf("TOPN filter column %q must be an entity tag", column.Name),
			schema.EntityTags...,
		)
	}
	return compileComparison(*predicate, schema)
}

func validateTopNDirection(direction OrderDirection, schemaSort string) error {
	normalizedSort := strings.ToUpper(strings.TrimSpace(schemaSort))
	if normalizedSort == "" || normalizedSort == "SORT_UNSPECIFIED" {
		return nil
	}
	if normalizedSort == "SORT_ASC" && direction != OrderAscending {
		return fmt.Errorf("TOPN resource supports only ASC order")
	}
	if normalizedSort == "SORT_DESC" && direction != OrderDescending {
		return fmt.Errorf("TOPN resource supports only DESC order")
	}
	return nil
}

func containsExact(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func compileOrder(order *Order, schema session.SchemaSnapshot) (string, error) {
	if order == nil {
		return "", nil
	}
	indexRuleName := strings.TrimSpace(order.IndexRule)
	if !isOrderDirection(order.Direction) {
		return "", fmt.Errorf("unsupported order direction %q", order.Direction)
	}
	if indexRuleName == "" {
		return string(order.Direction), nil
	}
	if indexRuleName == "TIME" {
		return "TIME " + string(order.Direction), nil
	}
	for _, sortableIndex := range schema.SortableIndexes {
		if sortableIndex.RuleName == indexRuleName {
			return sortableIndex.RuleName + " " + string(order.Direction), nil
		}
	}
	if len(schema.SortableIndexes) == 0 {
		column, columnErr := typedColumn(indexRuleName, schema)
		if columnErr == nil && column.Indexed {
			return column.Name + " " + string(order.Direction), nil
		}
	}
	allowedRules := []string{"TIME"}
	for _, sortableIndex := range schema.SortableIndexes {
		allowedRules = append(allowedRules, sortableIndex.RuleName)
	}
	return "", diagnosticError(
		"ORDER_INDEX_NOT_SORTABLE",
		"/order_by/index_rule",
		fmt.Sprintf("ORDER BY index rule %q is not sortable", indexRuleName),
		allowedRules...,
	)
}

func compileLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultLimit, nil
	}
	if limit < 0 {
		return 0, fmt.Errorf("limit must be greater than zero")
	}
	if limit > maximumLimit {
		return 0, diagnosticError("LIMIT_EXCEEDS_MAXIMUM", "/limit", fmt.Sprintf("limit cannot exceed %d", maximumLimit), strconv.Itoa(maximumLimit))
	}
	return limit, nil
}

func compileTimeRange(timeRange TimeRange) (string, error) {
	start := strings.TrimSpace(timeRange.Start)
	if start == "" {
		start = defaultTimeStart
	}
	if startErr := validateTimeValue(start); startErr != nil {
		return "", diagnosticError("INVALID_TIME_START", "/time_range/start", fmt.Sprintf("invalid time_range.start: %v", startErr))
	}
	end := strings.TrimSpace(timeRange.End)
	if end == "" {
		return "TIME > '" + quoteLiteral(start) + "'", nil
	}
	if endErr := validateTimeValue(end); endErr != nil {
		return "", diagnosticError("INVALID_TIME_END", "/time_range/end", fmt.Sprintf("invalid time_range.end: %v", endErr))
	}
	return "TIME BETWEEN '" + quoteLiteral(start) + "' AND '" + quoteLiteral(end) + "'", nil
}

func validateTimeValue(value string) error {
	if value == "now" || relativeTimePattern.MatchString(value) {
		return nil
	}
	if _, parseErr := time.Parse(time.RFC3339, value); parseErr != nil {
		return fmt.Errorf("%q must be RFC3339, now, or a relative value such as -30m", value)
	}
	return nil
}

func typedColumn(columnName string, schema session.SchemaSnapshot) (session.SchemaColumn, error) {
	trimmedName := strings.TrimSpace(columnName)
	if !identifierPattern.MatchString(trimmedName) {
		return session.SchemaColumn{}, fmt.Errorf("invalid column name %q", columnName)
	}
	column, found := schema.ExactColumn(trimmedName)
	if !found || column.Type == session.SchemaValueTypeUnknown {
		return session.SchemaColumn{}, fmt.Errorf("typed schema metadata is required to use column %q", trimmedName)
	}
	return column, nil
}

func validateResource(resource Resource, schema session.SchemaSnapshot) error {
	if !isResourceType(resource.Type) {
		return fmt.Errorf("unsupported resource type %q", resource.Type)
	}
	if !identifierPattern.MatchString(strings.TrimSpace(resource.Name)) {
		return fmt.Errorf("invalid resource name %q", resource.Name)
	}
	if len(resource.Groups) == 0 {
		return fmt.Errorf("resource %s requires at least one group", resource.Name)
	}
	for _, group := range resource.Groups {
		if !identifierPattern.MatchString(strings.TrimSpace(group)) {
			return fmt.Errorf("invalid group name %q", group)
		}
	}
	if schema.Type != "" && schema.Type != resource.Type {
		return diagnosticError(
			"RESOURCE_TYPE_MISMATCH",
			"/resource/type",
			fmt.Sprintf("plan resource type %s does not match discovered schema type %s", resource.Type, schema.Type),
			schema.Type.String(),
		)
	}
	if schema.Name != "" && schema.Name != resource.Name {
		return diagnosticError(
			"RESOURCE_NAME_MISMATCH",
			"/resource/name",
			fmt.Sprintf("plan resource name %q does not match discovered schema %q", resource.Name, schema.Name),
			schema.Name,
		)
	}
	if len(schema.Groups) > 0 && session.SchemaKey(resource.Type, resource.Name, resource.Groups) != session.SchemaKey(schema.Type, schema.Name, schema.Groups) {
		return diagnosticError("RESOURCE_GROUP_MISMATCH", "/resource/groups", "plan resource groups do not match the discovered schema groups", schema.Groups...)
	}
	return nil
}

func compileValue(value any, valueType session.SchemaValueType) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	switch valueType {
	case session.SchemaValueTypeString, session.SchemaValueTypeTimestamp:
		stringValue, isString := value.(string)
		if !isString {
			return "", fmt.Errorf("value %v must be a string", value)
		}
		return "'" + quoteLiteral(stringValue) + "'", nil
	case session.SchemaValueTypeInt:
		return integerValue(value)
	case session.SchemaValueTypeFloat:
		return floatValue(value)
	default:
		return "", fmt.Errorf("column type %q is not supported for deterministic filters", valueType)
	}
}

func integerValue(value any) (string, error) {
	switch number := value.(type) {
	case int:
		return strconv.Itoa(number), nil
	case int8:
		return strconv.FormatInt(int64(number), 10), nil
	case int16:
		return strconv.FormatInt(int64(number), 10), nil
	case int32:
		return strconv.FormatInt(int64(number), 10), nil
	case int64:
		return strconv.FormatInt(number, 10), nil
	case uint:
		return strconv.FormatUint(uint64(number), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(number), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(number), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(number), 10), nil
	case uint64:
		return strconv.FormatUint(number, 10), nil
	case float64:
		if number == float64(int64(number)) {
			return strconv.FormatInt(int64(number), 10), nil
		}
	case json.Number:
		integer, parseErr := strconv.ParseInt(string(number), 10, 64)
		if parseErr == nil {
			return strconv.FormatInt(integer, 10), nil
		}
	}
	return "", fmt.Errorf("value %v must be an integer", value)
}

func floatValue(value any) (string, error) {
	switch number := value.(type) {
	case int:
		return strconv.Itoa(number), nil
	case int64:
		return strconv.FormatInt(number, 10), nil
	case float32:
		return strconv.FormatFloat(float64(number), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(number, 'f', -1, 64), nil
	case json.Number:
		if _, parseErr := strconv.ParseFloat(string(number), 64); parseErr == nil {
			return string(number), nil
		}
	}
	return "", fmt.Errorf("value %v must be numeric", value)
}

// CompileDisplayDraft renders a best-effort BYDBQL sketch for UI preview when full compilation fails.
func CompileDisplayDraft(plan QueryPlan) string {
	resourceName := strings.TrimSpace(plan.Resource.Name)
	if resourceName == "" || len(plan.Resource.Groups) == 0 {
		return ""
	}
	groups := groupExpression(plan.Resource.Groups)
	timeClause, timeErr := compileTimeRange(plan.TimeRange)
	if timeErr != nil {
		timeClause = "TIME > '" + defaultTimeStart + "'"
	}
	if plan.Resource.Type == session.ResourceTypeTopN {
		topN := plan.TopN
		if topN == 0 {
			topN = defaultLimit
		}
		function := AggregateSum
		if plan.Aggregate != nil && plan.Aggregate.Function != "" {
			function = plan.Aggregate.Function
		}
		direction := OrderDescending
		if plan.OrderBy != nil && plan.OrderBy.Direction != "" {
			direction = plan.OrderBy.Direction
		}
		return fmt.Sprintf(
			"SHOW TOP %d FROM MEASURE %s IN %s %s AGGREGATE BY %s ORDER BY %s",
			topN,
			resourceName,
			groups,
			timeClause,
			function,
			direction,
		)
	}
	resourceType := plan.Resource.Type
	if resourceType == "" {
		resourceType = session.ResourceTypeMeasure
	}
	limit, limitErr := compileLimit(plan.Limit)
	if limitErr != nil {
		limit = defaultLimit
	}
	if resourceType == session.ResourceTypeProperty {
		return fmt.Sprintf("SELECT * FROM PROPERTY %s IN %s LIMIT %d", resourceName, groups, limit)
	}
	return fmt.Sprintf("SELECT * FROM %s %s IN %s %s LIMIT %d", resourceType, resourceName, groups, timeClause, limit)
}

func groupExpression(groups []string) string {
	if len(groups) == 1 {
		return strings.TrimSpace(groups[0])
	}
	return "(" + strings.Join(groups, ", ") + ")"
}

func quoteLiteral(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
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
