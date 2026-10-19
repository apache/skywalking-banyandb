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
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// ErrInvalidMeasureQuery identifies invalid typed measure syntax or bound values.
var ErrInvalidMeasureQuery = errors.New("invalid measure query")

// MeasureTemplate contains immutable, schema-resolved measure planning metadata.
// Each Bind creates independent requests and operators; no parameter or clock value is retained.
type MeasureTemplate struct {
	statement      *PreparedStatement
	request        *measurev1.QueryRequest
	predicate      *measurePredicate
	legacyTags     map[string]*tagSpecWithFamily
	aggregate      *ql.AggregatePlan
	filter         *measureExpression
	having         *measureExpression
	havingOutputs  []ql.Slot
	filterTags     map[measureColumnKey]bool
	nativeEntities []vectorized.ColumnDef
	slots          []ql.Slot
	expressions    []measureExpressionOutput
	groups         []measureExpressionOutput
	ordering       []measureSortKey
	sortVisible    int
	extended       bool
}

// MeasureExecution is one fresh binding of a measure template.
// A nil Plan preserves the legacy native execution and output contract.
type MeasureExecution struct {
	Request *measurev1.QueryRequest
	Plan    *ql.RawPlan
}

func invalidMeasure(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidMeasureQuery, err)
}

// CompileMeasure resolves a prepared measure query against a read-only schema snapshot.
// Non-measure queries return a nil template and retain their existing dispatch.
func (t *Transformer) CompileMeasure(ctx context.Context, statement *PreparedStatement) (*MeasureTemplate, error) {
	return t.compileMeasure(ctx, statement, nil)
}

// CompileMeasureWithSchemas resolves a query using only the supplied schema snapshots.
// Missing snapshots are errors; this entry point never falls back to a registry.
func (t *Transformer) CompileMeasureWithSchemas(
	ctx context.Context, statement *PreparedStatement, schemas map[MeasureSource]*databasev1.Measure,
) (*MeasureTemplate, error) {
	if schemas == nil {
		schemas = make(map[MeasureSource]*databasev1.Measure)
	}
	return t.compileMeasure(ctx, statement, schemas)
}

func (t *Transformer) compileMeasure(
	ctx context.Context, statement *PreparedStatement, captured map[MeasureSource]*databasev1.Measure,
) (*MeasureTemplate, error) {
	if statement == nil || statement.template == nil {
		return nil, invalidMeasure(errors.New("nil prepared statement"))
	}
	grammar := statement.template
	if grammar.Select == nil || !strings.EqualFold(grammar.Select.From.ResourceType, "MEASURE") {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selectStmt := grammar.Select
	extended := requiresMeasurePlan(selectStmt)
	if err := validateGrammarCounts(grammar); err != nil {
		if extended {
			return nil, invalidMeasure(err)
		}
		return nil, err
	}
	run := &transformRun{Transformer: t}
	groups, name := selectStmt.From.In.Groups, selectStmt.From.ResourceName
	if err := run.validateGroupOrResourceName(groups, name); err != nil {
		if extended {
			return nil, invalidMeasure(err)
		}
		return nil, err
	}
	snapshots := make(map[string]*databasev1.Measure, len(groups))
	for _, group := range groups {
		// Repeated requested groups retain their output semantics but share one
		// privately owned schema clone during compilation.
		if _, cloned := snapshots[group]; cloned {
			continue
		}
		var schema *databasev1.Measure
		var err error
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if captured != nil {
			schema = captured[MeasureSource{Group: group, Name: name}]
		} else {
			schema, err = t.getMeasure(ctx, &commonv1.Metadata{Name: name, Group: group})
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get measure %s/%s: %w", group, name, err)
		}
		if schema == nil {
			return nil, fmt.Errorf("measure %s/%s has no schema", group, name)
		}
		snapshots[group] = proto.Clone(schema).(*databasev1.Measure)
	}
	if !extended && selectStmt.Where != nil {
		extended = legacyWhereNeedsSchemaFilter(selectStmt.Where.Expr, snapshots)
	}
	if !extended {
		extended = measureOrderNeedsTyped(selectStmt, snapshots)
	}

	template := &MeasureTemplate{statement: statement, extended: extended}
	if template.extended {
		if compileErr := template.compileTyped(snapshots); compileErr != nil {
			return nil, invalidMeasure(compileErr)
		}
	} else {
		if err := template.compileLegacy(run, snapshots); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return template, nil
}

func (m *MeasureTemplate) compileTyped(schemas map[string]*databasev1.Measure) error {
	stmt := m.statement.template.Select
	columns, err := resolveMeasureColumns(stmt.From.In.Groups, schemas)
	if err != nil {
		return err
	}
	for _, group := range stmt.From.In.Groups {
		columns.intervals = append(columns.intervals, schemas[group].GetInterval())
	}
	switch {
	case stmt.Having != nil || stmt.GroupBy != nil:
		err = m.compileAggregate(columns)
	case hasMeasureExpressions(stmt.Projection):
		err = m.compileExpressions(columns)
	case measureAggregateCount(stmt) > 0:
		err = m.compileAggregate(columns)
	default:
		err = m.compileRaw(columns)
	}
	if err != nil {
		return err
	}
	if err = m.compileSort(columns); err != nil {
		return err
	}
	if stmt.Distinct {
		if err = m.validateDistinct(); err != nil {
			return err
		}
	}
	if m.aggregate != nil || m.expressions != nil || m.filter != nil || len(m.ordering) > 0 {
		m.compileNativeEntities(schemas, columns)
	}
	return nil
}

func (m *MeasureTemplate) compileLegacy(run *transformRun, schemas map[string]*databasev1.Measure) error {
	stmt := m.statement.template.Select
	projection, fields, tags, allFields, err := run.convertTagAndField(stmt.From.In.Groups, stmt.From.ResourceName,
		func(group, _ string) ([]*databasev1.TagFamilySpec, []*databasev1.FieldSpec, error) {
			schema := schemas[group]
			return schema.TagFamilies, schema.Fields, nil
		}, stmt.Projection)
	if err != nil {
		return err
	}
	agg, err := run.convertAggregation(stmt.Projection, tags, allFields)
	if err != nil {
		return err
	}
	groupBy, err := run.convertGroupBy(stmt.GroupBy, projection, fields)
	if err != nil {
		return err
	}
	top, err := run.convertTOP(stmt.Projection, allFields, agg)
	if err != nil {
		return err
	}
	m.request = baseMeasureRequest(stmt)
	m.request.TagProjection = projection
	if len(fields) > 0 {
		m.request.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: fields}
	}
	m.request.Agg, m.request.GroupBy, m.request.Top = agg, groupBy, top
	m.request.OrderBy = run.convertSelectOrderBy(stmt.OrderBy)
	m.legacyTags = tags
	return nil
}

func baseMeasureRequest(stmt *GrammarSelectStatement) *measurev1.QueryRequest {
	request := &measurev1.QueryRequest{Name: stmt.From.ResourceName, Groups: slices.Clone(stmt.From.In.Groups), Trace: stmt.WithQueryTrace != nil}
	if stmt.From.Stage != nil {
		request.Stages = slices.Clone(stmt.From.Stage.Stages)
	}
	return request
}

// Bind instantiates a template with fresh parameter values and a single execution clock.
func (m *MeasureTemplate) Bind(bound *BoundQuery, now time.Time) (*MeasureExecution, error) {
	if m == nil || bound == nil || bound.stmt == nil || len(bound.values) != len(m.statement.specs) {
		return nil, invalidMeasure(errors.New("bound query does not belong to this measure template"))
	}
	if bound.stmt != m.statement && (bound.stmt.query != m.statement.query || !slices.Equal(bound.stmt.specs, m.statement.specs)) {
		return nil, invalidMeasure(errors.New("bound query does not belong to this measure template"))
	}
	run := &transformRun{bound: bound.values}
	request := proto.Clone(m.request).(*measurev1.QueryRequest)
	stmt := m.statement.template.Select
	legacyRange, err := run.convertTimeRange(now, stmt.Time)
	if err != nil {
		if m.extended {
			return nil, invalidMeasure(err)
		}
		return nil, err
	}
	request.TimeRange = legacyRange
	if !m.extended {
		request.Criteria, err = run.convertSelectCriteria(stmt.Where, m.legacyTags)
		if err != nil {
			return nil, err
		}
		if stmt.Limit != nil {
			request.Limit = uint32(run.resolveCount(stmt.Limit.Value, stmt.Limit.Param, stmt.Limit.ParamIndex))
		}
		if stmt.Offset != nil {
			request.Offset = uint32(run.resolveCount(stmt.Offset.Value, stmt.Offset.Param, stmt.Offset.ParamIndex))
		}
		if stmt.Projection.TopN != nil {
			top := stmt.Projection.TopN
			request.Top.Number = int32(run.resolveCount(top.N, top.NParam, top.NParamIndex))
		}
		// Scalar pagination applies to the completed count, never to shard input.
		if bindLegacyScalarCountPage(stmt, request) {
			return &MeasureExecution{Request: request, Plan: &ql.RawPlan{Empty: true}}, nil
		}
		return &MeasureExecution{Request: request}, nil
	}
	plan, err := m.bindLocalOperators(bound)
	if err != nil {
		return nil, invalidMeasure(err)
	}
	if stmt.Limit != nil {
		limit := uint32(run.resolveCount(stmt.Limit.Value, stmt.Limit.Param, stmt.Limit.ParamIndex))
		plan.Limit = &limit
		plan.Empty = limit == 0
	}
	if stmt.Offset != nil {
		plan.Offset = uint32(run.resolveCount(stmt.Offset.Value, stmt.Offset.Param, stmt.Offset.ParamIndex))
	}
	ranges, err := m.bindWhere(bound, now, request, plan)
	if err != nil {
		return nil, invalidMeasure(err)
	}
	if legacyRange != nil {
		legacyIntervals, rangeErr := nativeTimeIntervals(legacyRange)
		if rangeErr != nil {
			return nil, invalidMeasure(rangeErr)
		}
		ranges = intersectTimeRanges(ranges, legacyIntervals)
	}
	plan.Ranges = ranges
	plan.Residual = len(ranges) > 1
	plan.Empty = plan.Empty || len(ranges) == 0
	// Native measure scan ranges are inclusive. Keep the SQL lattice half-open
	// internally and adapt only at this boundary; legacy native semantics stay intact.
	if len(ranges) == 0 {
		request.TimeRange = &modelv1.TimeRange{Begin: timestamppb.New(time.UnixMilli(0)), End: timestamppb.New(time.UnixMilli(0))}
	} else {
		request.TimeRange = &modelv1.TimeRange{Begin: timestamppb.New(time.UnixMilli(ranges[0].Begin)), End: timestamppb.New(time.UnixMilli(ranges[len(ranges)-1].End - 1))}
	}
	// Final SQL pagination belongs after the complete native scan and residual.
	request.Offset = 0
	request.Limit = math.MaxUint32
	plan.NativeAggregate = m.bindNativeAggregate(request, plan)
	if err = m.bindMeasureEntities(request, plan); err != nil {
		return nil, invalidMeasure(err)
	}
	return &MeasureExecution{Request: request, Plan: plan}, nil
}

func requiresMeasurePlan(stmt *GrammarSelectStatement) bool {
	if hasComputedMeasureGroups(stmt.GroupBy) || stmt.OrderBy != nil && len(stmt.OrderBy.Keys) > 0 {
		return true
	}
	if stmt.Distinct || stmt.Having != nil || stmt.From.Shorthand != nil || measureAggregateCount(stmt) > 1 || hasMeasureExpressions(stmt.Projection) ||
		stmt.Where != nil && stmt.Where.Expression != nil {
		return true
	}
	if stmt.Projection != nil {
		for _, column := range stmt.Projection.Columns {
			if column.Alias != nil || column.Aggregate != nil && column.Aggregate.TypeSpec != nil ||
				(column.TypeSpec == nil && isBuiltinTime(column.Identifier)) {
				return true
			}
		}
		if stmt.Projection.TopN != nil {
			for _, column := range stmt.Projection.TopN.OtherColumns {
				if column.Alias != nil {
					return true
				}
			}
		}
	}
	return whereHasTime(stmt.Where)
}

func hasComputedMeasureGroups(groupBy *GrammarGroupByClause) bool {
	if groupBy == nil {
		return false
	}
	for _, group := range groupBy.Columns {
		if group.Expression != nil || group.Ordinal != nil {
			return true
		}
	}
	return false
}

func measureAggregateCount(stmt *GrammarSelectStatement) int {
	if stmt.Projection == nil {
		return 0
	}
	count := 0
	for _, column := range stmt.Projection.Columns {
		if column.Aggregate != nil {
			count++
		}
	}
	return count
}

func whereHasTime(where *GrammarSelectWhereClause) bool {
	if where == nil || where.Expr == nil {
		return false
	}
	var orTime func(*GrammarOrExpr) bool
	var andTime func(*GrammarAndExpr) bool
	predTime := func(pred *GrammarPredicate) bool {
		if pred == nil {
			return false
		}
		if pred.Paren != nil {
			return orTime(pred.Paren)
		}
		if pred.Binary != nil {
			return isBuiltinTime(pred.Binary.Identifier)
		}
		if pred.In != nil {
			return isBuiltinTime(pred.In.Identifier)
		}
		if pred.Having != nil {
			return isBuiltinTime(pred.Having.Identifier)
		}
		return false
	}
	andTime = func(expr *GrammarAndExpr) bool {
		if expr == nil {
			return false
		}
		if predTime(expr.Left) {
			return true
		}
		for _, right := range expr.Right {
			if predTime(right.Right) {
				return true
			}
		}
		return false
	}
	orTime = func(expr *GrammarOrExpr) bool {
		if expr == nil {
			return false
		}
		if andTime(expr.Left) {
			return true
		}
		for _, right := range expr.Right {
			if andTime(right.Right) {
				return true
			}
		}
		return false
	}
	return orTime(where.Expr)
}

func isBuiltinTime(identifier *GrammarIdentifierPath) bool {
	return identifier != nil && identifier.QuotedIdent == nil && identifier.First != nil && len(identifier.Rest) == 0 && strings.EqualFold(identifier.First.Value(), "TIME")
}

func bindLegacyScalarCountPage(stmt *GrammarSelectStatement, request *measurev1.QueryRequest) bool {
	if request.GroupBy != nil || request.Top != nil ||
		request.GetAgg().GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT {
		return false
	}
	if stmt.Limit != nil && request.Limit == 0 || request.Offset > 0 {
		return true
	}
	// Native distributed limits prune node-local partials. A scalar count must
	// consume them all, including when QL omits LIMIT and native defaults apply.
	request.Limit = math.MaxUint32
	return false
}

func (m *MeasureTemplate) bindLocalOperators(bound *BoundQuery) (*ql.RawPlan, error) {
	var err error
	plan := &ql.RawPlan{
		Slots: slices.Clone(m.slots), Distinct: m.statement.template.Select.Distinct, Ordered: m.statement.template.Select.OrderBy != nil,
	}

	plan.Expressions, err = m.bindExpressions(bound)
	if err != nil {
		return nil, err
	}
	if m.aggregate != nil {
		plan.Aggregate, err = m.bindAggregate(bound)
		if err != nil {
			return nil, err
		}
	}
	if plan.Distinct {
		if err = ql.ValidateDistinct(plan); err != nil {
			return nil, err
		}
	}
	plan.Having, err = m.bindHaving(bound, plan.Aggregate)
	if err != nil {
		return nil, err
	}
	plan.Sort, err = m.bindSort(bound, plan)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

