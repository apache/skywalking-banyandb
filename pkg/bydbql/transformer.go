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
	"strconv"
	"strings"
	"time"

	"github.com/xhit/go-str2duration/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
)

var defaultBeginTime = time.Unix(0, 0)

// QueryType represents the type of query.
type QueryType int

// Supported query types.
const (
	QueryTypeMeasure QueryType = iota
	QueryTypeStream
	QueryTypeTrace
	QueryTypeProperty
	QueryTypeTopN
)

func (t QueryType) String() string {
	switch t {
	case QueryTypeMeasure:
		return "measure"
	case QueryTypeStream:
		return "stream"
	case QueryTypeTrace:
		return "trace"
	case QueryTypeProperty:
		return "property"
	case QueryTypeTopN:
		return "topn"
	default:
		return "unknown"
	}
}

// TransformResult is the result of transforming a ParsedQuery into a query request.
type TransformResult struct {
	QueryRequest proto.Message
	Original     *ParsedQuery
	Type         QueryType
}

// Transformer transforms a ParsedQuery into a native query request.
type Transformer struct {
	schemaRegistry metadata.Repo
}

// NewTransformer creates a new Transformer with the given schema registry.
func NewTransformer(registry metadata.Repo) *Transformer {
	return &Transformer{
		schemaRegistry: registry,
	}
}

// Transform transforms a ParsedQuery into a native query request.
func (t *Transformer) Transform(ctx context.Context, query *ParsedQuery) (*TransformResult, error) {
	switch query.Statement.(type) {
	case *SelectStatement:
		switch query.ResourceType {
		case ResourceTypeStream:
			return t.transformStreamQuery(ctx, query)
		case ResourceTypeMeasure:
			return t.transformMeasureQuery(ctx, query)
		case ResourceTypeTrace:
			return t.transformTraceQuery(ctx, query)
		case ResourceTypeProperty:
			return t.transformPropertyQuery(ctx, query)
		default:
			return nil, fmt.Errorf("unsupported resource type in select statement: %s", query.ResourceType)
		}
	case *TopNStatement:
		if query.ResourceType == ResourceTypeMeasure {
			return t.transformTopNMeasureQuery(ctx, query)
		}
		return nil, fmt.Errorf("unsupported resource type in topn statement: %s", query.ResourceType)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", query.Statement)
	}
}

func (t *Transformer) transformStreamQuery(ctx context.Context, query *ParsedQuery) (*TransformResult, error) {
	statement, ok := query.Statement.(*SelectStatement)
	if !ok {
		return nil, errors.New("stream query must be a select statement")
	}

	// validate query
	err := t.validateGroupOrResourceName(query)
	if err != nil {
		return nil, err
	}

	// query schema for getting tags
	projection, _, allTags, _, err := t.convertTagAndField(
		query.Groups, query.ResourceName,
		func(group, name string) ([]*databasev1.TagFamilySpec, []*databasev1.FieldSpec, error) {
			stream, getErr := t.schemaRegistry.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  name,
				Group: group,
			})
			if getErr != nil {
				return nil, nil, fmt.Errorf("failed to get stream %s/%s: %w", group, name, getErr)
			}
			return stream.TagFamilies, nil, nil
		}, statement.Projection)
	if err != nil {
		return nil, fmt.Errorf("failed to convert tags: %w", err)
	}

	// convert time range
	timeRange, err := t.convertTimeRange(query.Context, statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// convert order by
	orderBy := t.convertOrderBy(statement.OrderBy)

	// convert criteria
	criteria, err := t.convertCriteria(statement.Where, allTags)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	var offset, limit uint32 = 0, 0
	if statement.Offset != nil {
		offset = uint32(*statement.Offset)
	}
	if statement.Limit != nil {
		limit = uint32(*statement.Limit)
	}
	return &TransformResult{
		Type:     QueryTypeStream,
		Original: query,
		QueryRequest: &streamv1.QueryRequest{
			Groups:     query.Groups,
			Name:       query.ResourceName,
			TimeRange:  timeRange,
			Offset:     offset,
			Limit:      limit,
			OrderBy:    orderBy,
			Criteria:   criteria,
			Projection: projection,
			Trace:      statement.QueryTrace,
			Stages:     statement.From.Stages,
		},
	}, nil
}

func (t *Transformer) transformMeasureQuery(ctx context.Context, query *ParsedQuery) (*TransformResult, error) {
	statement, ok := query.Statement.(*SelectStatement)
	if !ok {
		return nil, errors.New("measure query must be a select statement")
	}

	// validate query
	err := t.validateGroupOrResourceName(query)
	if err != nil {
		return nil, err
	}

	// query schema for getting tags and fields
	projection, fields, allTags, allFields, err := t.convertTagAndField(
		query.Groups, query.ResourceName,
		func(group, name string) ([]*databasev1.TagFamilySpec, []*databasev1.FieldSpec, error) {
			measure, getErr := t.schemaRegistry.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
				Name:  name,
				Group: group,
			})
			if getErr != nil {
				return nil, nil, fmt.Errorf("failed to get measure %s/%s: %w", group, name, getErr)
			}
			return measure.TagFamilies, measure.Fields, nil
		}, statement.Projection)
	if err != nil {
		return nil, fmt.Errorf("failed to convert tags and fields: %w", err)
	}
	var fieldProjection *measurev1.QueryRequest_FieldProjection
	if len(fields) > 0 {
		fieldProjection = &measurev1.QueryRequest_FieldProjection{
			Names: fields,
		}
	}

	// convert time range
	timeRange, err := t.convertTimeRange(query.Context, statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// convert order by
	orderBy := t.convertOrderBy(statement.OrderBy)

	// convert criteria
	criteria, err := t.convertCriteria(statement.Where, allTags)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	agg, err := t.convertAggregation(statement.Projection.Columns, allFields)
	if err != nil {
		return nil, fmt.Errorf("failed to convert aggregation: %w", err)
	}

	// convert group by
	groupBy, err := t.convertGroupBy(statement.GroupBy, projection, fields)
	if err != nil {
		return nil, fmt.Errorf("failed to convert group by: %w", err)
	}
	if agg != nil && groupBy != nil && groupBy.FieldName == "" {
		return nil, errors.New("when aggregation and group by are both present, group by must include a field")
	}

	top, err := t.convertTOP(statement.Projection.TopN, allFields)
	if err != nil {
		return nil, fmt.Errorf("failed to convert top: %w", err)
	}

	var offset, limit uint32 = 0, 0
	if statement.Offset != nil {
		offset = uint32(*statement.Offset)
	}
	if statement.Limit != nil {
		limit = uint32(*statement.Limit)
	}

	return &TransformResult{
		Type:     QueryTypeMeasure,
		Original: query,
		QueryRequest: &measurev1.QueryRequest{
			Groups:          query.Groups,
			Name:            query.ResourceName,
			TimeRange:       timeRange,
			Criteria:        criteria,
			TagProjection:   projection,
			FieldProjection: fieldProjection,
			GroupBy:         groupBy,
			Agg:             agg,
			Top:             top,
			Offset:          offset,
			Limit:           limit,
			OrderBy:         orderBy,
			Trace:           statement.QueryTrace,
			Stages:          statement.From.Stages,
		},
	}, nil
}

func (t *Transformer) transformTraceQuery(ctx context.Context, query *ParsedQuery) (*TransformResult, error) {
	statement, ok := query.Statement.(*SelectStatement)
	if !ok {
		return nil, errors.New("trace query must be a select statement")
	}

	// validate query
	err := t.validateGroupOrResourceName(query)
	if err != nil {
		return nil, err
	}

	timeRange, err := t.convertTimeRange(query.Context, statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// query the stream schema for getting tags
	allTags := make(map[string]*tagSpecWithFamily)
	for _, g := range query.Groups {
		trace, getErr := t.schemaRegistry.TraceRegistry().GetTrace(ctx, &commonv1.Metadata{
			Name:  query.ResourceName,
			Group: g,
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get trace %s/%s: %w", g, query.ResourceName, getErr)
		}
		for _, tag := range trace.Tags {
			allTags[tag.Name] = &tagSpecWithFamily{
				family: "",
				tag: &databasev1.TagSpec{
					Name: tag.Name,
					Type: tag.Type,
				},
			}
		}
	}

	// convert all query tags
	var tagProjection []string
	if statement.Projection != nil && len(statement.Projection.Columns) > 0 {
		for _, c := range statement.Projection.Columns {
			if c.Type == ColumnTypeField {
				return nil, fmt.Errorf("field %s not supported in trace query", c.Name)
			}
			tagProjection = append(tagProjection, c.Name)
		}
	}

	// convert criteria
	criteria, err := t.convertCriteria(statement.Where, allTags)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	// convert order by
	orderBy := t.convertOrderBy(statement.OrderBy)

	var offset, limit uint32 = 0, 0
	if statement.Offset != nil {
		offset = uint32(*statement.Offset)
	}
	if statement.Limit != nil {
		limit = uint32(*statement.Limit)
	}

	return &TransformResult{
		Type:     QueryTypeTrace,
		Original: query,
		QueryRequest: &tracev1.QueryRequest{
			Groups:        query.Groups,
			Name:          query.ResourceName,
			TimeRange:     timeRange,
			Offset:        offset,
			Limit:         limit,
			OrderBy:       orderBy,
			Criteria:      criteria,
			TagProjection: tagProjection,
			Trace:         statement.QueryTrace,
			Stages:        statement.From.Stages,
		},
	}, nil
}

func (t *Transformer) transformPropertyQuery(ctx context.Context, query *ParsedQuery) (*TransformResult, error) {
	statement, ok := query.Statement.(*SelectStatement)
	if !ok {
		return nil, errors.New("property query must be a select statement")
	}

	// validate query
	err := t.validateGroupOrResourceName(query)
	if err != nil {
		return nil, err
	}

	// get property schema to extract all tags
	allTags := make(map[string]*tagSpecWithFamily)
	for _, g := range query.Groups {
		property, getErr := t.schemaRegistry.PropertyRegistry().GetProperty(ctx, &commonv1.Metadata{
			Name:  query.ResourceName,
			Group: g,
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get property %s/%s: %w", g, query.ResourceName, getErr)
		}
		for _, tag := range property.Tags {
			allTags[tag.Name] = &tagSpecWithFamily{
				tag:    tag,
				family: "",
			}
		}
	}

	// convert projection
	var tagProjection []string
	if statement.Projection != nil {
		if statement.Projection.All {
			// Select all tags
			for tagName := range allTags {
				tagProjection = append(tagProjection, tagName)
			}
		} else if len(statement.Projection.Columns) > 0 {
			// Select specific columns
			for _, col := range statement.Projection.Columns {
				if col.Type == ColumnTypeField {
					return nil, fmt.Errorf("field %s not supported in property query", col.Name)
				}
				if col.Function != nil {
					return nil, fmt.Errorf("aggregation function not supported in property query")
				}
				tagProjection = append(tagProjection, col.Name)
			}
		}
	}

	// extract IDs and filter criteria from WHERE clause
	var ids []string
	var criteria *modelv1.Criteria
	if statement.Where != nil && statement.Where.Expr != nil {
		ids, criteria, err = t.extractIDsAndCriteria(statement.Where.Expr, allTags)
		if err != nil {
			return nil, fmt.Errorf("failed to convert criteria: %w", err)
		}
	}

	// handle limit
	var limit uint32
	if statement.Limit != nil {
		limit = uint32(*statement.Limit)
	}

	return &TransformResult{
		Type:     QueryTypeProperty,
		Original: query,
		QueryRequest: &propertyv1.QueryRequest{
			Groups:        query.Groups,
			Name:          query.ResourceName,
			Ids:           ids,
			Criteria:      criteria,
			TagProjection: tagProjection,
			Limit:         limit,
			Trace:         statement.QueryTrace,
		},
	}, nil
}

func (t *Transformer) transformTopNMeasureQuery(ctx context.Context, query *ParsedQuery) (*TransformResult, error) {
	statement, ok := query.Statement.(*TopNStatement)
	if !ok {
		return nil, errors.New("topn measure query must be a topn statement")
	}

	// validate query
	err := t.validateGroupOrResourceName(query)
	if err != nil {
		return nil, err
	}

	// convert time range
	timeRange, err := t.convertTimeRange(query.Context, statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// convert agg
	var aggFunc modelv1.AggregationFunction
	if statement.AggregateBy != nil {
		aggFunc, err = t.convertAggregationFunc(statement.AggregateBy.Function)
		if err != nil {
			return nil, err
		}
	}

	// convert conditions
	conditions, err := t.convertAndConditions(ctx, statement.Where, query)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	// convert order by
	orderBy := t.convertOrderBy(statement.OrderBy)
	sort := modelv1.Sort_SORT_UNSPECIFIED
	if orderBy != nil {
		sort = orderBy.Sort
	}

	return &TransformResult{
		Type:     QueryTypeTopN,
		Original: query,
		QueryRequest: &measurev1.TopNRequest{
			Groups:         query.Groups,
			Name:           query.ResourceName,
			TimeRange:      timeRange,
			TopN:           int32(statement.TopN),
			Agg:            aggFunc,
			Conditions:     conditions,
			FieldValueSort: sort,
			Trace:          statement.QueryTrace,
			Stages:         statement.From.Stages,
		},
	}, nil
}

func (t *Transformer) convertCriteria(where *WhereClause, allTags map[string]*tagSpecWithFamily) (*modelv1.Criteria, error) {
	if where == nil || where.Expr == nil {
		return nil, nil
	}

	return t.convertConditionExpr(where.Expr, allTags, nil)
}

func (t *Transformer) convertAndConditions(ctx context.Context, where *WhereClause, query *ParsedQuery) ([]*modelv1.Condition, error) {
	if where == nil || where.Expr == nil {
		return nil, nil
	}

	allTags := make(map[string]*tagSpecWithFamily)
	for _, g := range query.Groups {
		aggregation, getErr := t.schemaRegistry.TopNAggregationRegistry().GetTopNAggregation(ctx, &commonv1.Metadata{
			Name:  query.ResourceName,
			Group: g,
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get topn aggregation %s/%s: %w", g, query.ResourceName, getErr)
		}
		sourceMeasure := aggregation.SourceMeasure
		measure, getErr := t.schemaRegistry.MeasureRegistry().GetMeasure(ctx, sourceMeasure)
		if getErr != nil {
			return nil, fmt.Errorf("failed to get measure %s/%s: %w", sourceMeasure.Group, sourceMeasure.Name, getErr)
		}

		for _, tag := range measure.TagFamilies {
			for _, tagSpec := range tag.Tags {
				allTags[tagSpec.Name] = &tagSpecWithFamily{
					tag:    tagSpec,
					family: tag.Name,
				}
			}
		}
	}

	var conditions []*modelv1.Condition
	_, err := t.convertConditionExpr(where.Expr, allTags, func(c *modelv1.Condition) {
		conditions = append(conditions, c)
	})
	if err != nil {
		return nil, err
	}

	return conditions, nil
}

func (t *Transformer) convertGroupBy(g *GroupByClause, queryTags *modelv1.TagProjection, queryFields []string) (*measurev1.QueryRequest_GroupBy, error) {
	if g == nil {
		return nil, nil
	}

	groupBy := &measurev1.QueryRequest_GroupBy{}
	tagFamilyWithNames := make(map[string]map[string]bool)

	foundInTags := func(name string) string {
		for _, family := range queryTags.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if tag == name {
					return family.Name
				}
			}
		}
		return ""
	}

	foundInField := func(name string) bool {
		for _, field := range queryFields {
			if field == name {
				return true
			}
		}
		return false
	}

	for _, c := range g.Columns {
		if c.Type == ColumnTypeAuto {
			familyExist := foundInTags(c.Name) != ""
			fieldExist := foundInField(c.Name)
			if familyExist && fieldExist {
				return nil, fmt.Errorf("column %s found in both tags and fields, please specify the type explicitly in group by", c.Name)
			}
			switch {
			case familyExist:
				c.Type = ColumnTypeTag
			case fieldExist:
				c.Type = ColumnTypeField
			default:
				return nil, fmt.Errorf("column %s not found in projection", c.Name)
			}
		}

		if c.Type == ColumnTypeField {
			if groupBy.FieldName != "" {
				return nil, errors.New("only one field is allowed in GROUP BY")
			}
			// check if the field exists in the projection
			if !foundInField(c.Name) {
				return nil, fmt.Errorf("field %s not found in projection", c.Name)
			}
			groupBy.FieldName = c.Name
			continue
		}

		// check if the tag exists in the projection
		tagExists := false
		if familyName := foundInTags(c.Name); familyName != "" {
			tagExists = true
			if _, ok := tagFamilyWithNames[familyName]; !ok {
				tagFamilyWithNames[familyName] = make(map[string]bool)
			}
			tagFamilyWithNames[familyName][c.Name] = true
		}
		if !tagExists {
			return nil, fmt.Errorf("tag %s not found in projection", c.Name)
		}
	}

	// convert tagFamilyWithNames to TagFamilies
	var tagFamilies []*modelv1.TagProjection_TagFamily
	for familyName, namesMap := range tagFamilyWithNames {
		names := make([]string, 0, len(namesMap))
		for name := range namesMap {
			names = append(names, name)
		}
		tagFamilies = append(tagFamilies, &modelv1.TagProjection_TagFamily{
			Name: familyName,
			Tags: names,
		})
	}
	groupBy.TagProjection = &modelv1.TagProjection{
		TagFamilies: tagFamilies,
	}

	return groupBy, nil
}

func (t *Transformer) convertAggregation(queryColumns []*Column, allFields map[string]*databasev1.FieldSpec) (*measurev1.QueryRequest_Aggregation, error) {
	if len(queryColumns) == 0 {
		return nil, nil
	}

	// found the aggregation column
	var aggCol *Column
	for _, col := range queryColumns {
		if col.Function != nil {
			if aggCol != nil {
				return nil, errors.New("only one aggregation function is allowed in SELECT")
			}
			aggCol = col
		}
	}

	if aggCol == nil {
		return nil, nil
	}

	// check the aggregation column in the field list
	_, exist := allFields[aggCol.Function.Column]
	if !exist {
		return nil, fmt.Errorf("field %s not found in schema", aggCol.Function.Column)
	}

	aggFunc, err := t.convertAggregationFunc(aggCol.Function.Function)
	if err != nil {
		return nil, err
	}

	return &measurev1.QueryRequest_Aggregation{
		Function:  aggFunc,
		FieldName: aggCol.Function.Column,
	}, nil
}

func (t *Transformer) convertAggregationFunc(f string) (modelv1.AggregationFunction, error) {
	switch strings.ToUpper(f) {
	case "MEAN":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN, nil
	case "MAX":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX, nil
	case "MIN":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN, nil
	case "COUNT":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, nil
	case "SUM":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, nil
	default:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED, fmt.Errorf("unsupported aggregation function: %s", f)
	}
}

// convertConditionExpr recursively converts a ConditionExpr tree to Criteria.
func (t *Transformer) convertConditionExpr(expr ConditionExpr, allTags map[string]*tagSpecWithFamily, accept func(c *modelv1.Condition)) (*modelv1.Criteria, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *BinaryLogicExpr:
		// Convert binary logical expression (AND/OR)
		leftCriteria, err := t.convertConditionExpr(e.Left, allTags, accept)
		if err != nil {
			return nil, err
		}

		rightCriteria, err := t.convertConditionExpr(e.Right, allTags, accept)
		if err != nil {
			return nil, err
		}

		logicalOp := modelv1.LogicalExpression_LOGICAL_OP_AND
		if e.Operator == LogicOr {
			logicalOp = modelv1.LogicalExpression_LOGICAL_OP_OR
		}

		return &modelv1.Criteria{
			Exp: &modelv1.Criteria_Le{
				Le: &modelv1.LogicalExpression{
					Op:    logicalOp,
					Left:  leftCriteria,
					Right: rightCriteria,
				},
			},
		}, nil

	case *Condition:
		// Convert leaf condition
		condition, err := t.convertCondition(e, allTags)
		if condition != nil && accept != nil {
			accept(condition.Exp.(*modelv1.Criteria_Condition).Condition)
		}
		return condition, err

	default:
		return nil, fmt.Errorf("unsupported condition expression type: %T", expr)
	}
}

func (t *Transformer) convertTimeRange(ctx *QueryContext, condition *TimeCondition) (*modelv1.TimeRange, error) {
	if condition == nil {
		return nil, nil
	}
	now := ctx.CurrentTime
	var begin, end time.Time
	switch condition.Operator {
	case TimeOpEqual:
		timestamp, err := t.parseTimestamp(now, condition.Timestamp)
		if err != nil {
			return nil, err
		}
		begin = *timestamp
		end = *timestamp
	case TimeOpGreater:
		timestamp, err := t.parseTimestamp(now, condition.Timestamp)
		if err != nil {
			return nil, err
		}
		begin = *timestamp
		end = now
	case TimeOpGreaterEqual:
		timestamp, err := t.parseTimestamp(now, condition.Timestamp)
		if err != nil {
			return nil, err
		}
		begin = *timestamp
		end = now
	case TimeOpLess:
		timestamp, err := t.parseTimestamp(now, condition.Timestamp)
		if err != nil {
			return nil, err
		}
		begin = defaultBeginTime
		end = *timestamp
	case TimeOpLessEqual:
		timestamp, err := t.parseTimestamp(now, condition.Timestamp)
		if err != nil {
			return nil, err
		}
		begin = defaultBeginTime
		end = *timestamp
	case TimeOpBetween:
		beginTS, err := t.parseTimestamp(now, condition.Begin)
		if err != nil {
			return nil, err
		}
		endTS, err := t.parseTimestamp(now, condition.End)
		if err != nil {
			return nil, err
		}
		begin = *beginTS
		end = *endTS
	}

	return &modelv1.TimeRange{
		Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
		End:   timestamppb.New(end.Truncate(time.Millisecond)),
	}, nil
}

func (t *Transformer) parseTimestamp(now time.Time, timestamp string) (*time.Time, error) {
	// Try parsing as absolute time first (RFC3339)
	if parsedTime, err := time.Parse(time.RFC3339, timestamp); err == nil {
		return &parsedTime, nil
	}

	// Try parsing as relative time (duration string)
	if strings.EqualFold(timestamp, "now") {
		return &now, nil
	}

	duration, err := str2duration.ParseDuration(timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format: %s", timestamp)
	}

	resultTime := now.Add(duration)
	return &resultTime, nil
}

func (t *Transformer) convertTagAndField(
	groups []string,
	name string,
	query func(group, name string) ([]*databasev1.TagFamilySpec, []*databasev1.FieldSpec, error),
	projection *Projection,
) (tags *modelv1.TagProjection, fields []string, allTags map[string]*tagSpecWithFamily, allFields map[string]*databasev1.FieldSpec, err error) {
	if projection == nil {
		return nil, nil, nil, nil, errors.New("projection must not be empty")
	}
	includeAll := projection.All
	combinedColumns := make([]*Column, 0, len(projection.Columns))
	if len(projection.Columns) > 0 {
		combinedColumns = append(combinedColumns, projection.Columns...)
	}
	if !includeAll && len(combinedColumns) == 0 {
		return nil, nil, nil, nil, errors.New("projection must not be empty")
	}
	allTags = make(map[string]*tagSpecWithFamily)
	allFields = make(map[string]*databasev1.FieldSpec)
	targetTagFamilies := make([]*modelv1.TagProjection_TagFamily, 0)
	targetFields := make([]string, 0)
	columnExists := make(map[Column]bool)

	// query all groups to collect tag and field specs
	for _, g := range groups {
		tagSpecs, fieldSpecs, err := query(g, name)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to get metadata from group %s, name: %s, %w", g, name, err)
		}
		for _, spec := range tagSpecs {
			for _, tag := range spec.Tags {
				allTags[tag.Name] = &tagSpecWithFamily{
					family: spec.Name,
					tag:    tag,
				}
			}
		}
		for _, spec := range fieldSpecs {
			allFields[spec.Name] = spec
		}
	}

	if includeAll {
		for _, spec := range allTags {
			if addErr := t.checkOrAddTagOrField(allTags, allFields, &targetTagFamilies, &targetFields, &columnExists, Column{
				Name: spec.tag.Name,
				Type: ColumnTypeTag,
			}); addErr != nil {
				return nil, nil, nil, nil, addErr
			}
		}
		for _, f := range allFields {
			if addErr := t.checkOrAddTagOrField(allTags, allFields, &targetTagFamilies, &targetFields, &columnExists, Column{
				Name: f.Name,
				Type: ColumnTypeField,
			}); addErr != nil {
				return nil, nil, nil, nil, addErr
			}
		}
	}

	for _, col := range combinedColumns {
		if addErr := t.checkOrAddTagOrField(allTags, allFields, &targetTagFamilies, &targetFields, &columnExists, *col); addErr != nil {
			return nil, nil, nil, nil, addErr
		}
	}

	var tagProjection *modelv1.TagProjection
	if len(targetTagFamilies) > 0 {
		tagProjection = &modelv1.TagProjection{TagFamilies: targetTagFamilies}
	}
	return tagProjection, targetFields, allTags, allFields, nil
}

func (t *Transformer) findTagOrField(
	sourceTags map[string]*tagSpecWithFamily,
	sourceFields map[string]*databasev1.FieldSpec,
	target Column,
) (tag *tagSpecWithFamily, field *databasev1.FieldSpec) {
	if target.Type == ColumnTypeTag || target.Type == ColumnTypeAuto {
		tag = sourceTags[target.Name]
	}
	if target.Type == ColumnTypeField || target.Type == ColumnTypeAuto {
		field = sourceFields[target.Name]
	}
	return
}

type tagSpecWithFamily struct {
	tag    *databasev1.TagSpec
	family string
}

func (t *Transformer) checkOrAddTagOrField(
	sourceTags map[string]*tagSpecWithFamily,
	sourceFields map[string]*databasev1.FieldSpec,
	targetTags *[]*modelv1.TagProjection_TagFamily,
	targetFields *[]string,
	columnExists *map[Column]bool,
	col Column,
) error {
	// skip the function column, that should be handled in aggregation
	if col.Function != nil {
		return nil
	}
	tagTypeIsAuto := col.Type == ColumnTypeAuto

	checkColumnExist := func(name string, tag ColumnType) bool {
		_, exist := (*columnExists)[Column{Name: name, Type: tag}]
		return exist
	}
	addColumnExist := func(name string, tag ColumnType) {
		(*columnExists)[Column{Name: name, Type: tag}] = true
	}

	// if the column already exists, skip it
	if checkColumnExist(col.Name, col.Type) {
		return nil
	}

	tag, field := t.findTagOrField(sourceTags, sourceFields, col)
	if tag == nil && field == nil {
		return fmt.Errorf("column %s not found in schema", col.Name)
	}

	// if the column type is automatic, then detected it from source tags and fields
	if col.Type == ColumnTypeAuto {
		switch {
		case tag != nil && field == nil:
			col.Type = ColumnTypeTag
		case tag == nil && field != nil:
			col.Type = ColumnTypeField
		case tag == nil && field == nil:
			return fmt.Errorf("column %s not found in projection", col.Name)
		case tag != nil && field != nil:
			return fmt.Errorf("ambiguous column %s found in both tags and fields", col.Name)
		}
	}

	if col.Type == ColumnTypeTag {
		if tag == nil {
			return fmt.Errorf("tag %s not found in schema", col.Name)
		}
		if checkColumnExist(col.Name, col.Type) {
			return nil
		}

		addedToExistFamily := false
		for _, tagFamily := range *targetTags {
			if tagFamily.Name == tag.family {
				tagFamily.Tags = append(tagFamily.Tags, col.Name)
				addedToExistFamily = true
				break
			}
		}

		if !addedToExistFamily {
			*targetTags = append(*targetTags, &modelv1.TagProjection_TagFamily{
				Name: tag.family,
				Tags: []string{tag.tag.Name},
			})
		}
	} else if col.Type == ColumnTypeField {
		if field == nil {
			return fmt.Errorf("field %s not found in schema", col.Name)
		}
		if checkColumnExist(col.Name, col.Type) {
			return nil
		}

		*targetFields = append(*targetFields, field.Name)
	}

	addColumnExist(col.Name, col.Type)
	if tagTypeIsAuto {
		addColumnExist(col.Name, ColumnTypeAuto)
	}
	return nil
}

func (t *Transformer) validateGroupOrResourceName(query *ParsedQuery) error {
	if len(query.Groups) == 0 {
		return errors.New("at least one group must be specified")
	}
	if query.ResourceName == "" {
		return errors.New("resource name must be specified")
	}

	return nil
}

func (t *Transformer) convertOrderBy(by *OrderByClause) *modelv1.QueryOrder {
	if by == nil {
		return nil
	}

	sort := modelv1.Sort_SORT_ASC
	if by.Desc {
		sort = modelv1.Sort_SORT_DESC
	}
	var indexRuleName string
	if !strings.EqualFold(by.Column, "TIME") { // if the column is TIME, we don't set index rule name
		indexRuleName = by.Column
	}
	return &modelv1.QueryOrder{
		IndexRuleName: indexRuleName,
		Sort:          sort,
	}
}

func (t *Transformer) convertCondition(condition *Condition, allTags map[string]*tagSpecWithFamily) (*modelv1.Criteria, error) {
	tagSpec, exist := allTags[condition.Left]
	if !exist {
		return nil, fmt.Errorf("tag %s not found in schema", condition.Left)
	}
	criteria := &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: condition.Left,
				Op:   t.convertConditionBinary(condition.Operator),
			},
		},
	}

	if err := t.settingConditionValue(criteria, condition, tagSpec); err != nil {
		return nil, fmt.Errorf("failed to set condition for tag %s: %w", condition.Left, err)
	}
	return criteria, nil
}

func (t *Transformer) settingConditionValue(criteria *modelv1.Criteria, condition *Condition, tagSpec *tagSpecWithFamily) error {
	cond := criteria.GetCondition()

	// Special handling for MATCH operator
	if condition.Operator == OpMatch {
		return t.setMatchConditionValue(cond, condition)
	}

	// Handle multi-value operators: IN, NOT_IN, HAVING, NOT_HAVING
	// for the HAVING/NOT_HAVING, we only check len(Values) > 0 to distinguish it from single value operators
	if condition.Operator == OpIn || condition.Operator == OpNotIn ||
		((condition.Operator == OpHaving || condition.Operator == OpNotHaving) && len(condition.Values) > 0) {
		return t.setMultiValueTagValue(cond, condition, tagSpec)
	}

	// Handle single value operators
	return t.setSingleValueTagValue(cond, condition, tagSpec)
}

// setMatchConditionValue handles MATCH operator condition value setting.
func (t *Transformer) setMatchConditionValue(cond *modelv1.Condition, condition *Condition) error {
	if condition.MatchOption == nil {
		return fmt.Errorf("MATCH operator requires MatchOption")
	}

	matchOpt := condition.MatchOption

	// Check values list is not empty
	if len(matchOpt.Values) == 0 {
		return fmt.Errorf("MATCH requires at least one value")
	}

	// Check for NULL values
	for _, val := range matchOpt.Values {
		if val.IsNull {
			return fmt.Errorf("MATCH operator does not support NULL value")
		}
	}

	// Set TagValue based on number of values
	if len(matchOpt.Values) == 1 {
		// Single value: set as Str
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{Value: t.convertToString(matchOpt.Values[0])},
			},
		}
	} else {
		// Multiple values: set as StrArray
		strArr := make([]string, len(matchOpt.Values))
		for i, val := range matchOpt.Values {
			strArr[i] = t.convertToString(val)
		}
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{Value: strArr},
			},
		}
	}

	// Set MatchOption if analyzer or operator is specified
	if matchOpt.Analyzer != "" || matchOpt.Operator != "" {
		pbMatchOpt := &modelv1.Condition_MatchOption{}

		if matchOpt.Analyzer != "" {
			pbMatchOpt.Analyzer = matchOpt.Analyzer
		}

		if matchOpt.Operator != "" {
			if matchOpt.Operator == "AND" {
				pbMatchOpt.Operator = modelv1.Condition_MatchOption_OPERATOR_AND
			} else if matchOpt.Operator == "OR" {
				pbMatchOpt.Operator = modelv1.Condition_MatchOption_OPERATOR_OR
			}
		}

		cond.MatchOption = pbMatchOpt
	}

	return nil
}

// setMultiValueTagValue handles multi-value operators (IN, NOT_IN, HAVING, NOT_HAVING).
func (t *Transformer) setMultiValueTagValue(cond *modelv1.Condition, condition *Condition, tagSpec *tagSpecWithFamily) error {
	switch tagSpec.tag.Type {
	case databasev1.TagType_TAG_TYPE_STRING, databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		// Convert to StrArray
		strArr := make([]string, len(condition.Values))
		for i, val := range condition.Values {
			if val.IsNull {
				return fmt.Errorf("NULL is not allowed in array values")
			}
			strArr[i] = t.convertToString(val)
		}
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{Value: strArr},
			},
		}

	case databasev1.TagType_TAG_TYPE_INT, databasev1.TagType_TAG_TYPE_INT_ARRAY:
		// Convert to IntArray
		intArr := make([]int64, len(condition.Values))
		for i, val := range condition.Values {
			if val.IsNull {
				return fmt.Errorf("NULL is not allowed in array values")
			}
			intVal, err := t.convertToInt64(val)
			if err != nil {
				return err
			}
			intArr[i] = intVal
		}
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{
				IntArray: &modelv1.IntArray{Value: intArr},
			},
		}

	default:
		return fmt.Errorf("unsupported tag type for array operation: %v", tagSpec.tag.Type)
	}

	return nil
}

// setSingleValueTagValue handles single value operators (=, !=, <, >, <=, >=).
func (t *Transformer) setSingleValueTagValue(cond *modelv1.Condition, condition *Condition, tagSpec *tagSpecWithFamily) error {
	// Handle NULL
	if condition.Right.IsNull {
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Null{
				Null: 0, // google.protobuf.NullValue.NULL_VALUE
			},
		}
		return nil
	}

	// Convert based on TagSpec.Type
	switch tagSpec.tag.Type {
	case databasev1.TagType_TAG_TYPE_STRING, databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		// Convert to Str
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{Value: t.convertToString(condition.Right)},
			},
		}

	case databasev1.TagType_TAG_TYPE_INT, databasev1.TagType_TAG_TYPE_INT_ARRAY:
		// Convert to Int
		intVal, err := t.convertToInt64(condition.Right)
		if err != nil {
			return err
		}
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{Value: intVal},
			},
		}

	case databasev1.TagType_TAG_TYPE_DATA_BINARY, databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return fmt.Errorf("tag type %v (binary/timestamp) is not supported in condition values", tagSpec.tag.Type)

	default:
		return fmt.Errorf("unsupported tag type for single value operation: %v", tagSpec.tag.Type)
	}

	return nil
}

// convertToString converts a Value to string.
func (t *Transformer) convertToString(val *Value) string {
	if val.Type == ValueTypeString {
		return val.StringVal
	}
	return fmt.Sprintf("%d", val.Integer)
}

// convertToInt64 converts a Value to int64.
func (t *Transformer) convertToInt64(val *Value) (int64, error) {
	if val.Type == ValueTypeInteger {
		return val.Integer, nil
	}
	intVal, err := strconv.ParseInt(val.StringVal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse string '%s' as integer: %w", val.StringVal, err)
	}
	return intVal, nil
}

func (t *Transformer) convertConditionBinary(op BinaryOperator) modelv1.Condition_BinaryOp {
	switch op {
	case OpEqual:
		return modelv1.Condition_BINARY_OP_EQ
	case OpNotEqual:
		return modelv1.Condition_BINARY_OP_NE
	case OpLess:
		return modelv1.Condition_BINARY_OP_LT
	case OpGreater:
		return modelv1.Condition_BINARY_OP_GT
	case OpLessEqual:
		return modelv1.Condition_BINARY_OP_LE
	case OpGreaterEqual:
		return modelv1.Condition_BINARY_OP_GE
	case OpHaving:
		return modelv1.Condition_BINARY_OP_HAVING
	case OpNotHaving:
		return modelv1.Condition_BINARY_OP_NOT_HAVING
	case OpIn:
		return modelv1.Condition_BINARY_OP_IN
	case OpNotIn:
		return modelv1.Condition_BINARY_OP_NOT_IN
	case OpMatch:
		return modelv1.Condition_BINARY_OP_MATCH
	default:
		return modelv1.Condition_BINARY_OP_EQ
	}
}

func (t *Transformer) convertTOP(topn *TopNProjection, fields map[string]*databasev1.FieldSpec) (*measurev1.QueryRequest_Top, error) {
	if topn == nil {
		return nil, nil
	}

	// check the field exists in the field list
	_, exist := fields[topn.OrderField]
	if !exist {
		return nil, fmt.Errorf("field %s not found in schema", topn.OrderField)
	}

	sort := modelv1.Sort_SORT_ASC
	if topn.Desc {
		sort = modelv1.Sort_SORT_DESC
	}

	return &measurev1.QueryRequest_Top{
		Number:         int32(topn.N),
		FieldName:      topn.OrderField,
		FieldValueSort: sort,
	}, nil
}

// extractIDsAndCriteria separates ID conditions from other conditions in property queries.
// ID conditions are extracted into a string array, while other conditions are converted to criteria.
func (t *Transformer) extractIDsAndCriteria(expr ConditionExpr, allTags map[string]*tagSpecWithFamily) ([]string, *modelv1.Criteria, error) {
	if expr == nil {
		return nil, nil, nil
	}

	switch e := expr.(type) {
	case *Condition:
		// Check if this is an ID condition
		if strings.EqualFold(e.Left, "ID") {
			return t.extractIDValues(e)
		}
		// Not an ID condition, convert to criteria
		criteria, err := t.convertCondition(e, allTags)
		return nil, criteria, err

	case *BinaryLogicExpr:
		// Recursively process left and right
		leftIDs, leftCriteria, err := t.extractIDsAndCriteria(e.Left, allTags)
		if err != nil {
			return nil, nil, err
		}

		rightIDs, rightCriteria, err := t.extractIDsAndCriteria(e.Right, allTags)
		if err != nil {
			return nil, nil, err
		}

		// Merge IDs from both sides
		ids := make([]string, 0, len(leftIDs)+len(rightIDs))
		ids = append(ids, leftIDs...)
		ids = append(ids, rightIDs...)

		// Build criteria tree from non-ID conditions
		var criteria *modelv1.Criteria
		switch {
		case leftCriteria != nil && rightCriteria != nil:
			// Both sides have criteria, combine with logical operator
			logicalOp := modelv1.LogicalExpression_LOGICAL_OP_AND
			if e.Operator == LogicOr {
				logicalOp = modelv1.LogicalExpression_LOGICAL_OP_OR
			}
			criteria = &modelv1.Criteria{
				Exp: &modelv1.Criteria_Le{
					Le: &modelv1.LogicalExpression{
						Op:    logicalOp,
						Left:  leftCriteria,
						Right: rightCriteria,
					},
				},
			}
		case leftCriteria != nil:
			// Only left has criteria
			criteria = leftCriteria
		case rightCriteria != nil:
			// Only right has criteria
		}

		return ids, criteria, nil

	default:
		return nil, nil, fmt.Errorf("unsupported condition expression type: %T", expr)
	}
}

// extractIDValues extracts ID values from a condition.
// Supports: ID = 'value', ID IN ('value1', 'value2', ...)
func (t *Transformer) extractIDValues(condition *Condition) ([]string, *modelv1.Criteria, error) {
	switch condition.Operator {
	case OpEqual:
		// ID = 'value'
		if condition.Right == nil {
			return nil, nil, fmt.Errorf("ID condition requires a value")
		}
		if condition.Right.IsNull {
			return nil, nil, fmt.Errorf("ID cannot be NULL")
		}
		idValue := t.convertToString(condition.Right)
		return []string{idValue}, nil, nil

	case OpIn:
		// ID IN ('value1', 'value2', ...)
		if len(condition.Values) == 0 {
			return nil, nil, fmt.Errorf("ID IN requires at least one value")
		}
		ids := make([]string, 0, len(condition.Values))
		for _, val := range condition.Values {
			if val.IsNull {
				return nil, nil, fmt.Errorf("ID cannot be NULL in IN clause")
			}
			ids = append(ids, t.convertToString(val))
		}
		return ids, nil, nil

	default:
		return nil, nil, fmt.Errorf("unsupported operator for ID condition: %v (only = and IN are supported)", condition.Operator)
	}
}
