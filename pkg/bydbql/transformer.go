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

const (
	columnTypeAuto  = "AUTO"
	columnTypeTag   = "TAG"
	columnTypeField = "FIELD"
)

const (
	orderDESC = "DESC"
)

// TransformResult is the result of transforming a Grammar into a query request.
type TransformResult struct {
	QueryRequest proto.Message
	Original     *Grammar
	Type         QueryType
}

// Transformer transforms a Grammar into a native query request.
type Transformer struct {
	schemaRegistry metadata.Repo
}

// NewTransformer creates a new Transformer with the given schema registry.
func NewTransformer(registry metadata.Repo) *Transformer {
	return &Transformer{
		schemaRegistry: registry,
	}
}

// Transform transforms a Grammar into a native query request.
func (t *Transformer) Transform(ctx context.Context, grammar *Grammar) (*TransformResult, error) {
	if grammar.Select != nil {
		// Extract resource type from SELECT statement
		resourceType := grammar.Select.From.ResourceType
		switch strings.ToUpper(resourceType) {
		case "STREAM":
			return t.transformStreamQuery(ctx, grammar)
		case "MEASURE":
			return t.transformMeasureQuery(ctx, grammar)
		case "TRACE":
			return t.transformTraceQuery(ctx, grammar)
		case "PROPERTY":
			return t.transformPropertyQuery(ctx, grammar)
		default:
			return nil, fmt.Errorf("unsupported resource type in select statement: %s", resourceType)
		}
	}
	if grammar.TopN != nil {
		resourceType := grammar.TopN.From.ResourceType
		if strings.ToUpper(resourceType) == "MEASURE" {
			return t.transformTopNMeasureQuery(ctx, grammar)
		}
		return nil, fmt.Errorf("unsupported resource type in topn statement: %s", resourceType)
	}
	return nil, errors.New("grammar must contain either Select or TopN statement")
}

func (t *Transformer) transformStreamQuery(ctx context.Context, grammar *Grammar) (*TransformResult, error) {
	statement := grammar.Select
	if statement == nil {
		return nil, errors.New("stream query must be a select statement")
	}

	// extract groups and resource name
	groups := statement.From.In.Groups
	resourceName := statement.From.ResourceName

	// validate query
	if err := t.validateGroupOrResourceName(groups, resourceName); err != nil {
		return nil, err
	}

	// query schema for getting tags
	projection, _, allTags, _, err := t.convertTagAndField(
		groups, resourceName,
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
	timeRange, err := t.convertTimeRange(time.Now(), statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// convert order by
	orderBy := t.convertSelectOrderBy(statement.OrderBy)

	// convert criteria
	criteria, err := t.convertSelectCriteria(statement.Where, allTags)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	var offset, limit uint32
	if statement.Offset != nil {
		offset = uint32(statement.Offset.Value)
	}
	if statement.Limit != nil {
		limit = uint32(statement.Limit.Value)
	}

	// extract stages
	var stages []string
	if statement.From.Stage != nil {
		stages = statement.From.Stage.Stages
	}

	return &TransformResult{
		Type:     QueryTypeStream,
		Original: grammar,
		QueryRequest: &streamv1.QueryRequest{
			Groups:     groups,
			Name:       resourceName,
			TimeRange:  timeRange,
			Offset:     offset,
			Limit:      limit,
			OrderBy:    orderBy,
			Criteria:   criteria,
			Projection: projection,
			Trace:      statement.WithQueryTrace != nil,
			Stages:     stages,
		},
	}, nil
}

func (t *Transformer) transformMeasureQuery(ctx context.Context, grammar *Grammar) (*TransformResult, error) {
	statement := grammar.Select
	if statement == nil {
		return nil, errors.New("measure query must be a select statement")
	}

	// extract groups and resource name
	groups := statement.From.In.Groups
	resourceName := statement.From.ResourceName

	// validate query
	if err := t.validateGroupOrResourceName(groups, resourceName); err != nil {
		return nil, err
	}

	// query schema for getting tags and fields
	projection, fields, allTags, allFields, err := t.convertTagAndField(
		groups, resourceName,
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
	timeRange, err := t.convertTimeRange(time.Now(), statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// convert order by
	orderBy := t.convertSelectOrderBy(statement.OrderBy)

	// convert criteria
	criteria, err := t.convertSelectCriteria(statement.Where, allTags)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	// convert aggregation
	agg, err := t.convertAggregation(statement.Projection, allFields)
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

	top, err := t.convertTOP(statement.Projection, allFields)
	if err != nil {
		return nil, fmt.Errorf("failed to convert top: %w", err)
	}

	var offset, limit uint32
	if statement.Offset != nil {
		offset = uint32(statement.Offset.Value)
	}
	if statement.Limit != nil {
		limit = uint32(statement.Limit.Value)
	}

	// extract stages
	var stages []string
	if statement.From.Stage != nil {
		stages = statement.From.Stage.Stages
	}

	return &TransformResult{
		Type:     QueryTypeMeasure,
		Original: grammar,
		QueryRequest: &measurev1.QueryRequest{
			Groups:          groups,
			Name:            resourceName,
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
			Trace:           statement.WithQueryTrace != nil,
			Stages:          stages,
		},
	}, nil
}

func (t *Transformer) transformTraceQuery(ctx context.Context, grammar *Grammar) (*TransformResult, error) {
	statement := grammar.Select
	if statement == nil {
		return nil, errors.New("trace query must be a select statement")
	}

	// extract groups and resource name
	groups := statement.From.In.Groups
	resourceName := statement.From.ResourceName

	// validate query
	if err := t.validateGroupOrResourceName(groups, resourceName); err != nil {
		return nil, err
	}

	timeRange, err := t.convertTimeRange(time.Now(), statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// query the trace schema for getting tags
	allTags := make(map[string]*tagSpecWithFamily)
	for _, g := range groups {
		trace, getErr := t.schemaRegistry.TraceRegistry().GetTrace(ctx, &commonv1.Metadata{
			Name:  resourceName,
			Group: g,
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get trace %s/%s: %w", g, resourceName, getErr)
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
			// check if column is a field type
			if c.TypeSpec != nil && strings.ToUpper(*c.TypeSpec) == columnTypeField {
				colName, nameErr := c.Identifier.ToString(c.TypeSpec != nil)
				if nameErr != nil {
					return nil, fmt.Errorf("failed to parse column identifier: %w", nameErr)
				}
				return nil, fmt.Errorf("field %s not supported in trace query", colName)
			}
			colName, nameErr := c.Identifier.ToString(c.TypeSpec != nil)
			if nameErr != nil {
				return nil, fmt.Errorf("failed to parse column identifier: %w", nameErr)
			}
			tagProjection = append(tagProjection, colName)
		}
	}

	// convert criteria
	criteria, err := t.convertSelectCriteria(statement.Where, allTags)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	// convert order by
	orderBy := t.convertSelectOrderBy(statement.OrderBy)

	var offset, limit uint32
	if statement.Offset != nil {
		offset = uint32(statement.Offset.Value)
	}
	if statement.Limit != nil {
		limit = uint32(statement.Limit.Value)
	}

	// extract stages
	var stages []string
	if statement.From.Stage != nil {
		stages = statement.From.Stage.Stages
	}

	return &TransformResult{
		Type:     QueryTypeTrace,
		Original: grammar,
		QueryRequest: &tracev1.QueryRequest{
			Groups:        groups,
			Name:          resourceName,
			TimeRange:     timeRange,
			Offset:        offset,
			Limit:         limit,
			OrderBy:       orderBy,
			Criteria:      criteria,
			TagProjection: tagProjection,
			Trace:         statement.WithQueryTrace != nil,
			Stages:        stages,
		},
	}, nil
}

func (t *Transformer) transformPropertyQuery(ctx context.Context, grammar *Grammar) (*TransformResult, error) {
	statement := grammar.Select
	if statement == nil {
		return nil, errors.New("property query must be a select statement")
	}

	// extract groups and resource name
	groups := statement.From.In.Groups
	resourceName := statement.From.ResourceName

	// validate query
	if err := t.validateGroupOrResourceName(groups, resourceName); err != nil {
		return nil, err
	}

	// get property schema to extract all tags
	allTags := make(map[string]*tagSpecWithFamily)
	for _, g := range groups {
		property, getErr := t.schemaRegistry.PropertyRegistry().GetProperty(ctx, &commonv1.Metadata{
			Name:  resourceName,
			Group: g,
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get property %s/%s: %w", g, resourceName, getErr)
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
			// select specific columns
			for _, col := range statement.Projection.Columns {
				if col.TypeSpec != nil && strings.ToUpper(*col.TypeSpec) == columnTypeField {
					colName, nameErr := col.Identifier.ToString(col.TypeSpec != nil)
					if nameErr != nil {
						return nil, fmt.Errorf("failed to parse column identifier: %w", nameErr)
					}
					return nil, fmt.Errorf("field %s not supported in property query", colName)
				}
				if col.Aggregate != nil {
					return nil, fmt.Errorf("aggregation function not supported in property query")
				}
				colName, nameErr := col.Identifier.ToString(col.TypeSpec != nil)
				if nameErr != nil {
					return nil, fmt.Errorf("failed to parse column identifier: %w", nameErr)
				}
				tagProjection = append(tagProjection, colName)
			}
		}
	}

	// extract ID and filter criteria from WHERE clause
	var ids []string
	var criteria *modelv1.Criteria
	if statement.Where != nil && statement.Where.Expr != nil {
		var extractErr error
		ids, criteria, extractErr = t.extractIDsAndCriteria(statement.Where.Expr, allTags)
		if extractErr != nil {
			return nil, fmt.Errorf("failed to convert criteria: %w", extractErr)
		}
	}

	// handle limit
	var limit uint32
	if statement.Limit != nil {
		limit = uint32(statement.Limit.Value)
	}

	return &TransformResult{
		Type:     QueryTypeProperty,
		Original: grammar,
		QueryRequest: &propertyv1.QueryRequest{
			Groups:        groups,
			Name:          resourceName,
			Ids:           ids,
			Criteria:      criteria,
			TagProjection: tagProjection,
			Limit:         limit,
			Trace:         statement.WithQueryTrace != nil,
		},
	}, nil
}

func (t *Transformer) transformTopNMeasureQuery(ctx context.Context, grammar *Grammar) (*TransformResult, error) {
	statement := grammar.TopN
	if statement == nil {
		return nil, errors.New("topn measure query must be a topn statement")
	}

	// extract groups and resource name
	groups := statement.From.In.Groups
	resourceName := statement.From.ResourceName

	// validate query
	if err := t.validateGroupOrResourceName(groups, resourceName); err != nil {
		return nil, err
	}

	// convert time range
	timeRange, err := t.convertTimeRange(time.Now(), statement.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to convert time range: %w", err)
	}

	// convert agg
	var aggFunc modelv1.AggregationFunction
	if statement.AggregateBy != nil {
		aggFunc, err = t.convertAggregationFunc(statement.AggregateBy.Function.Function)
		if err != nil {
			return nil, err
		}
	}

	// convert conditions
	conditions, err := t.convertTopNAndConditions(ctx, statement.Where, groups, resourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to convert criteria: %w", err)
	}

	// convert order by
	orderBy := t.convertTopNOrderBy(statement.OrderBy)
	sort := modelv1.Sort_SORT_UNSPECIFIED
	if orderBy != nil {
		sort = orderBy.Sort
	}

	// extract stages
	var stages []string
	if statement.From.Stage != nil {
		stages = statement.From.Stage.Stages
	}

	return &TransformResult{
		Type:     QueryTypeTopN,
		Original: grammar,
		QueryRequest: &measurev1.TopNRequest{
			Groups:         groups,
			Name:           resourceName,
			TimeRange:      timeRange,
			TopN:           int32(statement.N),
			Agg:            aggFunc,
			Conditions:     conditions,
			FieldValueSort: sort,
			Trace:          statement.WithQueryTrace != nil,
			Stages:         stages,
		},
	}, nil
}

func (t *Transformer) convertSelectCriteria(where *GrammarSelectWhereClause, allTags map[string]*tagSpecWithFamily) (*modelv1.Criteria, error) {
	if where == nil || where.Expr == nil {
		return nil, nil
	}

	return t.convertOrExpr(where.Expr, allTags, nil)
}

func (t *Transformer) convertTopNAndConditions(ctx context.Context, where *GrammarTopNWhereClause, groups []string, resourceName string) ([]*modelv1.Condition, error) {
	if where == nil || where.Expr == nil {
		return nil, nil
	}

	allTags := make(map[string]*tagSpecWithFamily)
	for _, g := range groups {
		aggregation, getErr := t.schemaRegistry.TopNAggregationRegistry().GetTopNAggregation(ctx, &commonv1.Metadata{
			Name:  resourceName,
			Group: g,
		})
		if getErr != nil {
			return nil, fmt.Errorf("failed to get topn aggregation %s/%s: %w", g, resourceName, getErr)
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
	_, err := t.convertAndExpr(where.Expr, allTags, func(c *modelv1.Condition) {
		conditions = append(conditions, c)
	})
	if err != nil {
		return nil, err
	}

	return conditions, nil
}

func (t *Transformer) convertGroupBy(g *GrammarGroupByClause, queryTags *modelv1.TagProjection, queryFields []string) (*measurev1.QueryRequest_GroupBy, error) {
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
		colName, nameErr := c.Identifier.ToString(c.TypeSpec != nil)
		if nameErr != nil {
			return nil, fmt.Errorf("failed to parse column identifier: %w", nameErr)
		}

		colType := columnTypeAuto
		if c.TypeSpec != nil {
			colType = strings.ToUpper(*c.TypeSpec)
		}

		if colType == columnTypeAuto {
			familyExist := foundInTags(colName) != ""
			fieldExist := foundInField(colName)
			if familyExist && fieldExist {
				return nil, fmt.Errorf("column %s found in both tags and fields, please specify the type explicitly in group by", colName)
			}
			switch {
			case familyExist:
				colType = columnTypeTag
			case fieldExist:
				colType = columnTypeField
			default:
				return nil, fmt.Errorf("column %s not found in projection", colName)
			}
		}

		if colType == columnTypeField {
			if groupBy.FieldName != "" {
				return nil, errors.New("only one field is allowed in GROUP BY")
			}
			// Check if the field exists in the projection
			if !foundInField(colName) {
				return nil, fmt.Errorf("field %s not found in projection", colName)
			}
			groupBy.FieldName = colName
			continue
		}

		// check if the tag exists in the projection
		tagExists := false
		if familyName := foundInTags(colName); familyName != "" {
			tagExists = true
			if _, ok := tagFamilyWithNames[familyName]; !ok {
				tagFamilyWithNames[familyName] = make(map[string]bool)
			}
			tagFamilyWithNames[familyName][colName] = true
		}
		if !tagExists {
			return nil, fmt.Errorf("tag %s not found in projection", colName)
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

func (t *Transformer) convertAggregation(projection *GrammarProjection, allFields map[string]*databasev1.FieldSpec) (*measurev1.QueryRequest_Aggregation, error) {
	var columns []*GrammarColumn
	if projection != nil && len(projection.Columns) > 0 {
		columns = append(columns, projection.Columns...)
	}
	if projection.TopN != nil && len(projection.TopN.OtherColumns) > 0 {
		columns = append(columns, projection.TopN.OtherColumns...)
	}

	// find the aggregation column
	var aggCol *GrammarColumn
	for _, col := range columns {
		if col.Aggregate != nil {
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
	aggColName, nameErr := aggCol.Aggregate.Column.ToString(true)
	if nameErr != nil {
		return nil, fmt.Errorf("failed to parse aggregate column identifier: %w", nameErr)
	}

	_, exist := allFields[aggColName]
	if !exist {
		return nil, fmt.Errorf("field %s not found in schema", aggColName)
	}

	aggFunc, err := t.convertAggregationFunc(aggCol.Aggregate.Function)
	if err != nil {
		return nil, err
	}

	return &measurev1.QueryRequest_Aggregation{
		Function:  aggFunc,
		FieldName: aggColName,
	}, nil
}

func (t *Transformer) convertAggregationFunc(f string) (modelv1.AggregationFunction, error) {
	switch strings.ToUpper(f) {
	case "MEAN", "AVG":
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

func (t *Transformer) convertOrExpr(expr *GrammarOrExpr, allTags map[string]*tagSpecWithFamily, accept func(c *modelv1.Condition)) (*modelv1.Criteria, error) {
	if expr == nil {
		return nil, nil
	}

	leftCriteria, err := t.convertAndExpr(expr.Left, allTags, accept)
	if err != nil {
		return nil, err
	}

	if len(expr.Right) == 0 {
		return leftCriteria, nil
	}

	// process all OR operations
	for _, orRight := range expr.Right {
		rightCriteria, err := t.convertAndExpr(orRight.Right, allTags, accept)
		if err != nil {
			return nil, err
		}

		leftCriteria = &modelv1.Criteria{
			Exp: &modelv1.Criteria_Le{
				Le: &modelv1.LogicalExpression{
					Op:    modelv1.LogicalExpression_LOGICAL_OP_OR,
					Left:  leftCriteria,
					Right: rightCriteria,
				},
			},
		}
	}

	return leftCriteria, nil
}

func (t *Transformer) convertAndExpr(expr *GrammarAndExpr, allTags map[string]*tagSpecWithFamily, accept func(c *modelv1.Condition)) (*modelv1.Criteria, error) {
	if expr == nil {
		return nil, nil
	}

	leftCriteria, err := t.convertPredicate(expr.Left, allTags, accept)
	if err != nil {
		return nil, err
	}

	if len(expr.Right) == 0 {
		return leftCriteria, nil
	}

	// process all AND operations
	for _, andRight := range expr.Right {
		rightCriteria, err := t.convertPredicate(andRight.Right, allTags, accept)
		if err != nil {
			return nil, err
		}

		leftCriteria = &modelv1.Criteria{
			Exp: &modelv1.Criteria_Le{
				Le: &modelv1.LogicalExpression{
					Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
					Left:  leftCriteria,
					Right: rightCriteria,
				},
			},
		}
	}

	return leftCriteria, nil
}

func (t *Transformer) convertPredicate(pred *GrammarPredicate, allTags map[string]*tagSpecWithFamily, accept func(c *modelv1.Condition)) (*modelv1.Criteria, error) {
	if pred == nil {
		return nil, nil
	}

	if pred.Paren != nil {
		return t.convertOrExpr(pred.Paren, allTags, accept)
	}

	if pred.Binary != nil {
		return t.convertBinaryPredicate(pred.Binary, allTags, accept)
	}

	if pred.In != nil {
		return t.convertInPredicate(pred.In, allTags, accept)
	}

	if pred.Having != nil {
		return t.convertHavingPredicate(pred.Having, allTags, accept)
	}

	return nil, errors.New("empty predicate")
}

func (t *Transformer) convertBinaryPredicate(
	pred *GrammarBinaryPredicate,
	allTags map[string]*tagSpecWithFamily,
	accept func(c *modelv1.Condition),
) (*modelv1.Criteria, error) {
	identifierName, nameErr := pred.Identifier.ToString(false)
	if nameErr != nil {
		return nil, fmt.Errorf("failed to parse identifier: %w", nameErr)
	}

	tagSpec, exist := allTags[identifierName]
	if !exist {
		return nil, fmt.Errorf("tag %s not found in schema", identifierName)
	}

	if pred.Tail.Match != nil {
		return t.convertMatchPredicate(identifierName, pred.Tail.Match, accept)
	}

	if pred.Tail.Compare != nil {
		return t.convertComparePredicate(identifierName, pred.Tail.Compare, tagSpec, accept)
	}

	return nil, errors.New("empty binary predicate tail")
}

func (t *Transformer) convertMatchPredicate(identifierName string, match *GrammarMatchTail, accept func(c *modelv1.Condition)) (*modelv1.Criteria, error) {
	if match.Values == nil {
		return nil, fmt.Errorf("MATCH operator requires values")
	}

	var values []*GrammarValue
	if match.Values.Single != nil {
		values = []*GrammarValue{match.Values.Single}
	} else if match.Values.Array != nil {
		values = match.Values.Array
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("MATCH requires at least one value")
	}

	// check for NULL values
	for _, val := range values {
		if val.Null {
			return nil, fmt.Errorf("MATCH operator does not support NULL value")
		}
	}

	cond := &modelv1.Condition{
		Name: identifierName,
		Op:   modelv1.Condition_BINARY_OP_MATCH,
	}

	// set tag value based on number of values
	if len(values) == 1 {
		// single value: set as Str
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{Value: t.grammarValueToString(values[0])},
			},
		}
	} else {
		// multiple values: set as string array
		strArr := make([]string, len(values))
		for i, val := range values {
			strArr[i] = t.grammarValueToString(val)
		}
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{Value: strArr},
			},
		}
	}

	// set MatchOption if analyzer or operator is specified
	if match.Analyzer != nil || match.Operator != nil {
		pbMatchOpt := &modelv1.Condition_MatchOption{}

		if match.Analyzer != nil {
			pbMatchOpt.Analyzer = *match.Analyzer
		}

		if match.Operator != nil {
			if *match.Operator == "AND" {
				pbMatchOpt.Operator = modelv1.Condition_MatchOption_OPERATOR_AND
			} else if *match.Operator == "OR" {
				pbMatchOpt.Operator = modelv1.Condition_MatchOption_OPERATOR_OR
			}
		}

		cond.MatchOption = pbMatchOpt
	}

	if accept != nil {
		accept(cond)
	}

	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: cond,
		},
	}, nil
}

func (t *Transformer) convertComparePredicate(
	identifierName string,
	compare *GrammarCompareTail,
	tagSpec *tagSpecWithFamily,
	accept func(c *modelv1.Condition),
) (*modelv1.Criteria, error) {
	cond := &modelv1.Condition{
		Name: identifierName,
		Op:   t.convertCompareOp(compare.Operator),
	}

	if err := t.setGrammarConditionValue(cond, compare.Value, tagSpec); err != nil {
		return nil, fmt.Errorf("failed to set condition for tag %s: %w", identifierName, err)
	}

	if accept != nil {
		accept(cond)
	}

	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: cond,
		},
	}, nil
}

func (t *Transformer) convertInPredicate(pred *GrammarInPredicate, allTags map[string]*tagSpecWithFamily, accept func(c *modelv1.Condition)) (*modelv1.Criteria, error) {
	identifierName, nameErr := pred.Identifier.ToString(false)
	if nameErr != nil {
		return nil, fmt.Errorf("failed to parse identifier: %w", nameErr)
	}

	tagSpec, exist := allTags[identifierName]
	if !exist {
		return nil, fmt.Errorf("tag %s not found in schema", identifierName)
	}

	op := modelv1.Condition_BINARY_OP_IN
	if pred.Not != nil {
		op = modelv1.Condition_BINARY_OP_NOT_IN
	}

	cond := &modelv1.Condition{
		Name: identifierName,
		Op:   op,
	}

	if err := t.setGrammarMultiValueCondition(cond, pred.Values, tagSpec); err != nil {
		return nil, fmt.Errorf("failed to set condition for tag %s: %w", identifierName, err)
	}

	if accept != nil {
		accept(cond)
	}

	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: cond,
		},
	}, nil
}

func (t *Transformer) convertHavingPredicate(
	pred *GrammarHavingPredicate,
	allTags map[string]*tagSpecWithFamily,
	accept func(c *modelv1.Condition),
) (*modelv1.Criteria, error) {
	identifierName, nameErr := pred.Identifier.ToString(false)
	if nameErr != nil {
		return nil, fmt.Errorf("failed to parse identifier: %w", nameErr)
	}

	tagSpec, exist := allTags[identifierName]
	if !exist {
		return nil, fmt.Errorf("tag %s not found in schema", identifierName)
	}

	op := modelv1.Condition_BINARY_OP_HAVING
	if pred.Not != nil {
		op = modelv1.Condition_BINARY_OP_NOT_HAVING
	}

	cond := &modelv1.Condition{
		Name: identifierName,
		Op:   op,
	}

	var err error
	if pred.Values.Single != nil {
		err = t.setGrammarConditionValue(cond, pred.Values.Single, tagSpec)
	} else if pred.Values.Array != nil {
		err = t.setGrammarMultiValueCondition(cond, pred.Values.Array, tagSpec)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to set condition for tag %s: %w", identifierName, err)
	}

	if accept != nil {
		accept(cond)
	}

	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: cond,
		},
	}, nil
}

func (t *Transformer) setGrammarConditionValue(cond *modelv1.Condition, val *GrammarValue, tagSpec *tagSpecWithFamily) error {
	if val.Null {
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Null{
				Null: 0,
			},
		}
		return nil
	}

	switch tagSpec.tag.Type {
	case databasev1.TagType_TAG_TYPE_STRING, databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		// Convert to Str
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{Value: t.grammarValueToString(val)},
			},
		}

	case databasev1.TagType_TAG_TYPE_INT, databasev1.TagType_TAG_TYPE_INT_ARRAY:
		intVal, err := t.grammarValueToInt64(val)
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

func (t *Transformer) setGrammarMultiValueCondition(cond *modelv1.Condition, values []*GrammarValue, tagSpec *tagSpecWithFamily) error {
	switch tagSpec.tag.Type {
	case databasev1.TagType_TAG_TYPE_STRING, databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		strArr := make([]string, len(values))
		for i, val := range values {
			if val.Null {
				return fmt.Errorf("NULL is not allowed in array values")
			}
			strArr[i] = t.grammarValueToString(val)
		}
		cond.Value = &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{Value: strArr},
			},
		}

	case databasev1.TagType_TAG_TYPE_INT, databasev1.TagType_TAG_TYPE_INT_ARRAY:
		intArr := make([]int64, len(values))
		for i, val := range values {
			if val.Null {
				return fmt.Errorf("NULL is not allowed in array values")
			}
			intVal, err := t.grammarValueToInt64(val)
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

func (t *Transformer) convertTimeRange(now time.Time, timeClause *GrammarTimeClause) (*modelv1.TimeRange, error) {
	if timeClause == nil {
		return nil, nil
	}
	var begin, end time.Time

	if timeClause.Between != nil {
		beginTS, err := t.parseTimestamp(now, timeClause.Between.Begin.ToString())
		if err != nil {
			return nil, err
		}
		endTS, err := t.parseTimestamp(now, timeClause.Between.End.ToString())
		if err != nil {
			return nil, err
		}
		begin = *beginTS
		end = *endTS
	} else if timeClause.Comparator != nil && timeClause.Value != nil {
		timestamp, err := t.parseTimestamp(now, timeClause.Value.ToString())
		if err != nil {
			return nil, err
		}
		switch *timeClause.Comparator {
		case "=":
			begin = *timestamp
			end = *timestamp
		case ">":
			begin = *timestamp
			end = now
		case ">=":
			begin = *timestamp
			end = now
		case "<":
			begin = defaultBeginTime
			end = *timestamp
		case "<=":
			begin = defaultBeginTime
			end = *timestamp
		default:
			return nil, fmt.Errorf("unsupported time comparator: %s", *timeClause.Comparator)
		}
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
	projection *GrammarProjection,
) (tags *modelv1.TagProjection, fields []string, allTags map[string]*tagSpecWithFamily, allFields map[string]*databasev1.FieldSpec, err error) {
	if projection == nil {
		return nil, nil, nil, nil, errors.New("projection must not be empty")
	}
	includeAll := projection.All
	var combinedColumns []*GrammarColumn
	if len(projection.Columns) > 0 {
		combinedColumns = append(combinedColumns, projection.Columns...)
	}
	// add TOP N other columns if present
	if projection.TopN != nil && len(projection.TopN.OtherColumns) > 0 {
		combinedColumns = append(combinedColumns, projection.TopN.OtherColumns...)
	}
	if !includeAll && len(combinedColumns) == 0 {
		return nil, nil, nil, nil, errors.New("projection must not be empty")
	}
	allTags = make(map[string]*tagSpecWithFamily)
	allFields = make(map[string]*databasev1.FieldSpec)
	targetTagFamilies := make([]*modelv1.TagProjection_TagFamily, 0)
	targetFields := make([]string, 0)
	columnExists := make(map[string]map[string]bool) // map[columnName]map[columnType]bool

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
			if addErr := t.checkOrAddGrammarTagOrField(allTags, allFields, &targetTagFamilies, &targetFields, columnExists, spec.tag.Name, columnTypeTag, nil); addErr != nil {
				return nil, nil, nil, nil, addErr
			}
		}
		for _, f := range allFields {
			if addErr := t.checkOrAddGrammarTagOrField(allTags, allFields, &targetTagFamilies, &targetFields, columnExists, f.Name, columnTypeField, nil); addErr != nil {
				return nil, nil, nil, nil, addErr
			}
		}
	}

	for _, col := range combinedColumns {
		if col.Aggregate != nil {
			continue
		}
		colName, nameErr := col.Identifier.ToString(col.TypeSpec != nil)
		if nameErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to parse column identifier: %w", nameErr)
		}
		colType := columnTypeAuto
		if col.TypeSpec != nil {
			colType = strings.ToUpper(*col.TypeSpec)
		}
		if addErr := t.checkOrAddGrammarTagOrField(allTags, allFields, &targetTagFamilies, &targetFields, columnExists, colName, colType, col.Aggregate); addErr != nil {
			return nil, nil, nil, nil, addErr
		}
	}

	var tagProjection *modelv1.TagProjection
	if len(targetTagFamilies) > 0 {
		tagProjection = &modelv1.TagProjection{TagFamilies: targetTagFamilies}
	}
	return tagProjection, targetFields, allTags, allFields, nil
}

func (t *Transformer) findGrammarTagOrField(
	sourceTags map[string]*tagSpecWithFamily,
	sourceFields map[string]*databasev1.FieldSpec,
	targetName string,
	targetType string,
) (tag *tagSpecWithFamily, field *databasev1.FieldSpec) {
	if targetType == columnTypeTag || targetType == columnTypeAuto {
		tag = sourceTags[targetName]
	}
	if targetType == columnTypeField || targetType == columnTypeAuto {
		field = sourceFields[targetName]
	}
	return
}

type tagSpecWithFamily struct {
	tag    *databasev1.TagSpec
	family string
}

func (t *Transformer) checkOrAddGrammarTagOrField(
	sourceTags map[string]*tagSpecWithFamily,
	sourceFields map[string]*databasev1.FieldSpec,
	targetTags *[]*modelv1.TagProjection_TagFamily,
	targetFields *[]string,
	columnExists map[string]map[string]bool,
	colName string,
	colType string,
	aggregate *GrammarAggregateFunction,
) error {
	// skip the function column, that should be handled in aggregation
	if aggregate != nil {
		return nil
	}
	tagTypeIsAuto := colType == columnTypeAuto

	checkColumnExist := func(name string, colType string) bool {
		if columnExists[name] == nil {
			return false
		}
		return columnExists[name][colType]
	}
	addColumnExist := func(name string, colType string) {
		if columnExists[name] == nil {
			columnExists[name] = make(map[string]bool)
		}
		columnExists[name][colType] = true
	}

	// if the column already exists, skip it
	if checkColumnExist(colName, colType) {
		return nil
	}

	tag, field := t.findGrammarTagOrField(sourceTags, sourceFields, colName, colType)
	if tag == nil && field == nil {
		return fmt.Errorf("column %s not found in schema", colName)
	}

	// if the column type is automatic, then detected it from source tags and fields
	if colType == columnTypeAuto {
		switch {
		case tag != nil && field == nil:
			colType = columnTypeTag
		case tag == nil && field != nil:
			colType = columnTypeField
		case tag == nil && field == nil:
			return fmt.Errorf("column %s not found in projection", colName)
		case tag != nil && field != nil:
			return fmt.Errorf("ambiguous column %s found in both tags and fields", colName)
		}
	}

	if colType == columnTypeTag {
		if tag == nil {
			return fmt.Errorf("tag %s not found in schema", colName)
		}
		if checkColumnExist(colName, colType) {
			return nil
		}

		addedToExistFamily := false
		for _, tagFamily := range *targetTags {
			if tagFamily.Name == tag.family {
				tagFamily.Tags = append(tagFamily.Tags, colName)
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
	} else if colType == columnTypeField {
		if field == nil {
			return fmt.Errorf("field %s not found in schema", colName)
		}
		if checkColumnExist(colName, colType) {
			return nil
		}

		*targetFields = append(*targetFields, field.Name)
	}

	addColumnExist(colName, colType)
	if tagTypeIsAuto {
		addColumnExist(colName, columnTypeAuto)
	}
	return nil
}

func (t *Transformer) validateGroupOrResourceName(groups []string, resourceName string) error {
	if len(groups) == 0 {
		return errors.New("at least one group must be specified")
	}
	if resourceName == "" {
		return errors.New("resource name must be specified")
	}

	return nil
}

func (t *Transformer) convertSelectOrderBy(orderBy *GrammarSelectOrderByClause) *modelv1.QueryOrder {
	if orderBy == nil {
		return nil
	}

	sort := modelv1.Sort_SORT_ASC
	var indexRuleName string

	if orderBy.Tail.DirOnly != nil {
		// direction only (defaults to TIME)
		if strings.EqualFold(*orderBy.Tail.DirOnly, orderDESC) {
			sort = modelv1.Sort_SORT_DESC
		}
	} else if orderBy.Tail.WithIdent != nil {
		// with identifier
		colName, nameErr := orderBy.Tail.WithIdent.Identifier.ToString(false)
		if nameErr == nil && !strings.EqualFold(colName, "TIME") {
			indexRuleName = colName
		}
		if orderBy.Tail.WithIdent.Direction != nil && strings.EqualFold(*orderBy.Tail.WithIdent.Direction, orderDESC) {
			sort = modelv1.Sort_SORT_DESC
		}
	}

	return &modelv1.QueryOrder{
		IndexRuleName: indexRuleName,
		Sort:          sort,
	}
}

func (t *Transformer) convertTopNOrderBy(orderBy *GrammarTopNOrderByClause) *modelv1.QueryOrder {
	if orderBy == nil {
		return nil
	}

	sort := modelv1.Sort_SORT_ASC
	if orderBy.Dir != nil && strings.EqualFold(*orderBy.Dir, orderDESC) {
		sort = modelv1.Sort_SORT_DESC
	}

	return &modelv1.QueryOrder{
		Sort: sort,
	}
}

func (t *Transformer) convertCompareOp(op string) modelv1.Condition_BinaryOp {
	switch op {
	case "=":
		return modelv1.Condition_BINARY_OP_EQ
	case "!=":
		return modelv1.Condition_BINARY_OP_NE
	case "<":
		return modelv1.Condition_BINARY_OP_LT
	case ">":
		return modelv1.Condition_BINARY_OP_GT
	case "<=":
		return modelv1.Condition_BINARY_OP_LE
	case ">=":
		return modelv1.Condition_BINARY_OP_GE
	default:
		return modelv1.Condition_BINARY_OP_EQ
	}
}

func (t *Transformer) convertTOP(projection *GrammarProjection, fields map[string]*databasev1.FieldSpec) (*measurev1.QueryRequest_Top, error) {
	if projection == nil || projection.TopN == nil {
		return nil, nil
	}

	topn := projection.TopN
	orderFieldName, nameErr := topn.OrderField.ToString(false)
	if nameErr != nil {
		return nil, fmt.Errorf("failed to parse order field identifier: %w", nameErr)
	}

	// check the field exists in the field list
	_, exist := fields[orderFieldName]
	if !exist {
		return nil, fmt.Errorf("field %s not found in schema", orderFieldName)
	}

	sort := modelv1.Sort_SORT_ASC
	if topn.Direction != nil && strings.EqualFold(*topn.Direction, orderDESC) {
		sort = modelv1.Sort_SORT_DESC
	}

	return &measurev1.QueryRequest_Top{
		Number:         int32(topn.N),
		FieldName:      orderFieldName,
		FieldValueSort: sort,
	}, nil
}

func (t *Transformer) grammarValueToString(val *GrammarValue) string {
	if val.String != nil {
		return *val.String
	}
	if val.Integer != nil {
		return fmt.Sprintf("%d", *val.Integer)
	}
	return ""
}

func (t *Transformer) grammarValueToInt64(val *GrammarValue) (int64, error) {
	if val.Integer != nil {
		return *val.Integer, nil
	}
	if val.String != nil {
		intVal, err := strconv.ParseInt(*val.String, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse string '%s' as integer: %w", *val.String, err)
		}
		return intVal, nil
	}
	return 0, fmt.Errorf("cannot convert value to int64")
}

// extractIDsAndCriteria separates ID conditions from other conditions in property queries.
// ID conditions are extracted into a string array, while other conditions are converted to criteria.
func (t *Transformer) extractIDsAndCriteria(expr *GrammarOrExpr, allTags map[string]*tagSpecWithFamily) ([]string, *modelv1.Criteria, error) {
	if expr == nil {
		return nil, nil, nil
	}

	// Process the expression tree and collect IDs and criteria
	ids, criteria, err := t.extractIDsFromOrExpr(expr, allTags)
	if err != nil {
		return nil, nil, err
	}

	return ids, criteria, nil
}

// extractIDsFromOrExpr processes OR expressions for ID extraction.
func (t *Transformer) extractIDsFromOrExpr(expr *GrammarOrExpr, allTags map[string]*tagSpecWithFamily) ([]string, *modelv1.Criteria, error) {
	leftIDs, leftCriteria, err := t.extractIDsFromAndExpr(expr.Left, allTags)
	if err != nil {
		return nil, nil, err
	}

	if len(expr.Right) == 0 {
		return leftIDs, leftCriteria, nil
	}

	// Process OR branches
	var allIDs []string
	allIDs = append(allIDs, leftIDs...)
	currentCriteria := leftCriteria

	for _, orRight := range expr.Right {
		rightIDs, rightCriteria, err := t.extractIDsFromAndExpr(orRight.Right, allTags)
		if err != nil {
			return nil, nil, err
		}
		allIDs = append(allIDs, rightIDs...)

		if currentCriteria != nil && rightCriteria != nil {
			currentCriteria = &modelv1.Criteria{
				Exp: &modelv1.Criteria_Le{
					Le: &modelv1.LogicalExpression{
						Op:    modelv1.LogicalExpression_LOGICAL_OP_OR,
						Left:  currentCriteria,
						Right: rightCriteria,
					},
				},
			}
		} else if rightCriteria != nil {
			currentCriteria = rightCriteria
		}
	}

	return allIDs, currentCriteria, nil
}

// extractIDsFromAndExpr processes AND expressions for ID extraction.
func (t *Transformer) extractIDsFromAndExpr(expr *GrammarAndExpr, allTags map[string]*tagSpecWithFamily) ([]string, *modelv1.Criteria, error) {
	leftIDs, leftCriteria, err := t.extractIDsFromPredicate(expr.Left, allTags)
	if err != nil {
		return nil, nil, err
	}

	if len(expr.Right) == 0 {
		return leftIDs, leftCriteria, nil
	}

	// Process AND branches
	var allIDs []string
	allIDs = append(allIDs, leftIDs...)
	currentCriteria := leftCriteria

	for _, andRight := range expr.Right {
		rightIDs, rightCriteria, err := t.extractIDsFromPredicate(andRight.Right, allTags)
		if err != nil {
			return nil, nil, err
		}
		allIDs = append(allIDs, rightIDs...)

		if currentCriteria != nil && rightCriteria != nil {
			currentCriteria = &modelv1.Criteria{
				Exp: &modelv1.Criteria_Le{
					Le: &modelv1.LogicalExpression{
						Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
						Left:  currentCriteria,
						Right: rightCriteria,
					},
				},
			}
		} else if rightCriteria != nil {
			currentCriteria = rightCriteria
		}
	}

	return allIDs, currentCriteria, nil
}

// extractIDsFromPredicate processes predicates for ID extraction.
func (t *Transformer) extractIDsFromPredicate(pred *GrammarPredicate, allTags map[string]*tagSpecWithFamily) ([]string, *modelv1.Criteria, error) {
	if pred.Paren != nil {
		return t.extractIDsFromOrExpr(pred.Paren, allTags)
	}

	if pred.Binary != nil {
		identifierName, nameErr := pred.Binary.Identifier.ToString(false)
		if nameErr != nil {
			return nil, nil, fmt.Errorf("failed to parse identifier: %w", nameErr)
		}

		// Check if this is an ID condition
		if strings.EqualFold(identifierName, "ID") {
			if pred.Binary.Tail.Compare != nil && pred.Binary.Tail.Compare.Operator == "=" {
				// ID = 'value'
				if pred.Binary.Tail.Compare.Value.Null {
					return nil, nil, fmt.Errorf("ID cannot be NULL")
				}
				idValue := t.grammarValueToString(pred.Binary.Tail.Compare.Value)
				return []string{idValue}, nil, nil
			}
			return nil, nil, fmt.Errorf("unsupported operator for ID condition (only = is supported)")
		}

		// Not an ID condition, convert to criteria
		criteria, err := t.convertBinaryPredicate(pred.Binary, allTags, nil)
		return nil, criteria, err
	}

	if pred.In != nil {
		identifierName, nameErr := pred.In.Identifier.ToString(false)
		if nameErr != nil {
			return nil, nil, fmt.Errorf("failed to parse identifier: %w", nameErr)
		}

		// Check if this is an ID IN condition
		if strings.EqualFold(identifierName, "ID") {
			if pred.In.Not != nil {
				return nil, nil, fmt.Errorf("NOT IN is not supported for ID condition")
			}
			// ID IN ('value1', 'value2', ...)
			if len(pred.In.Values) == 0 {
				return nil, nil, fmt.Errorf("ID IN requires at least one value")
			}
			ids := make([]string, 0, len(pred.In.Values))
			for _, val := range pred.In.Values {
				if val.Null {
					return nil, nil, fmt.Errorf("ID cannot be NULL in IN clause")
				}
				ids = append(ids, t.grammarValueToString(val))
			}
			return ids, nil, nil
		}

		// not an ID condition, convert to criteria
		criteria, err := t.convertInPredicate(pred.In, allTags, nil)
		return nil, criteria, err
	}

	if pred.Having != nil {
		// HAVING is not an ID condition
		criteria, err := t.convertHavingPredicate(pred.Having, allTags, nil)
		return nil, criteria, err
	}

	return nil, nil, errors.New("empty predicate")
}
