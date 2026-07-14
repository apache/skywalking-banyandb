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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

const (
	defaultLimit     = 10
	defaultTimeStart = "-30m"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)

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

// Predicate is a typed comparison leaf or AND/OR expression tree.
type Predicate struct {
	Children []Predicate `json:"children,omitempty"`
	Column   string      `json:"column,omitempty"`
	Operator Operator    `json:"operator"`
	Value    any         `json:"value,omitempty"`
}

// Order specifies an indexed column and direction.
type Order struct {
	Column    string         `json:"column"`
	Direction OrderDirection `json:"direction"`
}

// TimeRange supplies BYDBQL-compatible bounds for a time-series query.
type TimeRange struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

// QueryPlan describes one query without embedding any BYDBQL text.
type QueryPlan struct {
	Resource   Resource     `json:"resource"`
	Projection []Projection `json:"projection,omitempty"`
	Filter     *Predicate   `json:"filter,omitempty"`
	Aggregate  *Aggregate   `json:"aggregate,omitempty"`
	OrderBy    *Order       `json:"order_by,omitempty"`
	TimeRange  TimeRange    `json:"time_range,omitempty"`
	GroupBy    []string     `json:"group_by,omitempty"`
	ID         string       `json:"id,omitempty"`
	Limit      int          `json:"limit,omitempty"`
	TopN       int          `json:"top_n,omitempty"`
}

// WorkflowPlan describes a sequence of independently approved query plans.
type WorkflowPlan struct {
	Steps []QueryPlan `json:"steps"`
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
	aggregateCount := 0
	if plan.Aggregate != nil {
		aggregateCount++
	}
	for _, projection := range plan.Projection {
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
	return nil
}

func compileSelect(plan QueryPlan, schema session.SchemaSnapshot) (string, error) {
	projections, projectionErr := compileProjections(plan.Projection, plan.Aggregate, plan.Resource, schema)
	if projectionErr != nil {
		return "", projectionErr
	}
	groups, groupsErr := compileGroups(plan.GroupBy, schema)
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
		parts = append(parts, compileTimeRange(plan.TimeRange))
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
	if len(plan.Projection) != 0 || plan.Filter != nil || len(plan.GroupBy) != 0 || plan.Limit != 0 {
		return "", fmt.Errorf("TOPN plans support only resource, aggregate function, order direction, time range, and top_n")
	}
	if plan.Aggregate != nil && strings.TrimSpace(plan.Aggregate.Column) != "" {
		return "", fmt.Errorf("TOPN aggregation cannot select a column")
	}
	if plan.OrderBy != nil && strings.TrimSpace(plan.OrderBy.Column) != "" {
		return "", fmt.Errorf("TOPN order cannot select a column")
	}
	topN := plan.TopN
	if topN == 0 {
		topN = defaultLimit
	}
	if topN < 1 {
		return "", fmt.Errorf("top_n must be greater than zero")
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
	return fmt.Sprintf(
		"SHOW TOP %d FROM MEASURE %s IN %s %s AGGREGATE BY %s ORDER BY %s",
		topN,
		plan.Resource.Name,
		groupExpression(plan.Resource.Groups),
		compileTimeRange(plan.TimeRange),
		function,
		direction,
	), nil
}

func compileProjections(projections []Projection, aggregate *Aggregate, resource Resource, schema session.SchemaSnapshot) (string, error) {
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
	if column.Type != session.SchemaValueTypeInt && column.Type != session.SchemaValueTypeFloat {
		return "", fmt.Errorf("aggregation column %q must be numeric", aggregate.Column)
	}
	return fmt.Sprintf("%s(%s)", aggregate.Function, column.Name), nil
}

func compileGroups(groups []string, schema session.SchemaSnapshot) (string, error) {
	if len(groups) == 0 {
		return "", nil
	}
	compiled := make([]string, 0, len(groups))
	for _, group := range groups {
		column, columnErr := typedColumn(group, schema)
		if columnErr != nil {
			return "", columnErr
		}
		compiled = append(compiled, column.Name)
	}
	return strings.Join(compiled, ", "), nil
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
	column, columnErr := typedColumn(predicate.Column, schema)
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

func compileOrder(order *Order, schema session.SchemaSnapshot) (string, error) {
	if order == nil {
		return "", nil
	}
	columnName := strings.TrimSpace(order.Column)
	if !isOrderDirection(order.Direction) {
		return "", fmt.Errorf("unsupported order direction %q", order.Direction)
	}
	if strings.EqualFold(columnName, "TIME") {
		return "TIME " + string(order.Direction), nil
	}
	column, columnErr := typedColumn(columnName, schema)
	if columnErr != nil {
		return "", columnErr
	}
	if !column.Indexed {
		return "", fmt.Errorf("ORDER BY column %q is not indexed", columnName)
	}
	return column.Name + " " + string(order.Direction), nil
}

func compileLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultLimit, nil
	}
	if limit < 0 {
		return 0, fmt.Errorf("limit must be greater than zero")
	}
	return limit, nil
}

func compileTimeRange(timeRange TimeRange) string {
	start := strings.TrimSpace(timeRange.Start)
	if start == "" {
		start = defaultTimeStart
	}
	end := strings.TrimSpace(timeRange.End)
	if end == "" {
		return "TIME > '" + quoteLiteral(start) + "'"
	}
	return "TIME BETWEEN '" + quoteLiteral(start) + "' AND '" + quoteLiteral(end) + "'"
}

func typedColumn(columnName string, schema session.SchemaSnapshot) (session.SchemaColumn, error) {
	trimmedName := strings.TrimSpace(columnName)
	if !identifierPattern.MatchString(trimmedName) {
		return session.SchemaColumn{}, fmt.Errorf("invalid column name %q", columnName)
	}
	column, found := schema.Column(trimmedName)
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
		if resource.Type == session.ResourceTypeTopN && schema.Type == session.ResourceTypeMeasure {
			return nil
		}
		return fmt.Errorf("plan resource type %s does not match discovered schema type %s", resource.Type, schema.Type)
	}
	if schema.Name != "" && !strings.EqualFold(schema.Name, resource.Name) {
		return fmt.Errorf("plan resource name %q does not match discovered schema %q", resource.Name, schema.Name)
	}
	return nil
}

func compileValue(value any, valueType session.SchemaValueType) (string, error) {
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
	timeClause := compileTimeRange(plan.TimeRange)
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
