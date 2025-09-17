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
	"fmt"
	"strconv"
	"strings"
	"time"

	str2duration "github.com/xhit/go-str2duration/v2"
	"sigs.k8s.io/yaml"
)

// QueryYAML represents the YAML structure for BanyanDB queries
type QueryYAML struct {
	// Common fields
	Name       string                 `yaml:"name,omitempty"`
	Groups     []string               `yaml:"groups,omitempty"`
	TimeRange  *TimeRangeYAML         `yaml:"timeRange,omitempty"`
	Criteria   []map[string]interface{} `yaml:"criteria,omitempty"`
	Limit      *int                   `yaml:"limit,omitempty"`
	Offset     *int                   `yaml:"offset,omitempty"`
	Trace      bool                   `yaml:"trace,omitempty"`

	// Stream/Trace specific
	Projection []string               `yaml:"projection,omitempty"`
	OrderBy    *OrderByYAML           `yaml:"orderBy,omitempty"`

	// Measure specific
	TagProjection   []string              `yaml:"tagProjection,omitempty"`
	FieldProjection []string              `yaml:"fieldProjection,omitempty"`
	GroupBy         *GroupByYAML          `yaml:"groupBy,omitempty"`
	Agg             *AggregationYAML      `yaml:"agg,omitempty"`
	Top             *TopYAML              `yaml:"top,omitempty"`

	// Property specific
	IDs []string `yaml:"ids,omitempty"`

	// Top-N specific (for separate endpoint)
	TopN              int                `yaml:"topN,omitempty"`
	FieldValueSort    string             `yaml:"fieldValueSort,omitempty"`
	Conditions        []map[string]interface{} `yaml:"conditions,omitempty"`
}

// TimeRangeYAML represents time range in YAML
type TimeRangeYAML struct {
	Begin string `yaml:"begin"`
	End   string `yaml:"end"`
}

// OrderByYAML represents ORDER BY clause in YAML
type OrderByYAML struct {
	IndexRuleName string `yaml:"indexRuleName"`
	Sort          string `yaml:"sort"` // ASC or DESC
}

// GroupByYAML represents GROUP BY clause in YAML
type GroupByYAML struct {
	TagProjection []string `yaml:"tagProjection"`
}

// AggregationYAML represents aggregation function in YAML
type AggregationYAML struct {
	Function   string `yaml:"function"`
	FieldName  string `yaml:"fieldName"`
}

// TopYAML represents TOP N clause in YAML
type TopYAML struct {
	Number        int      `yaml:"number"`
	FieldName     string   `yaml:"fieldName"`
	FieldValueSort string  `yaml:"fieldValueSort"`
}

// Translator converts parsed BydbQL to YAML format
type Translator struct {
	context *QueryContext
}

// NewTranslator creates a new translator
func NewTranslator(context *QueryContext) *Translator {
	if context == nil {
		context = &QueryContext{CurrentTime: time.Now()}
	}
	return &Translator{context: context}
}

// TranslateToYAML translates a parsed query to YAML format
func (t *Translator) TranslateToYAML(query *ParsedQuery) ([]byte, error) {
	yamlQuery, err := t.translateQuery(query)
	if err != nil {
		return nil, err
	}

	return yaml.Marshal(yamlQuery)
}

// TranslateToMap translates a parsed query to a map suitable for JSON/YAML conversion
func (t *Translator) TranslateToMap(query *ParsedQuery) (map[string]interface{}, error) {
	yamlQuery, err := t.translateQuery(query)
	if err != nil {
		return nil, err
	}

	yamlBytes, err := yaml.Marshal(yamlQuery)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = yaml.Unmarshal(yamlBytes, &result)
	return result, err
}

// translateQuery translates the main query structure
func (t *Translator) translateQuery(query *ParsedQuery) (*QueryYAML, error) {
	yamlQuery := &QueryYAML{}

	// Set common fields from context or FROM clause
	if query.ResourceName != "" {
		yamlQuery.Name = query.ResourceName
	} else if t.context.DefaultResourceName != "" {
		yamlQuery.Name = t.context.DefaultResourceName
	}

	if len(query.Groups) > 0 {
		yamlQuery.Groups = query.Groups
	} else if t.context.DefaultGroup != "" {
		yamlQuery.Groups = []string{t.context.DefaultGroup}
	}

	switch stmt := query.Statement.(type) {
	case *SelectStatement:
		return t.translateSelectStatement(stmt, yamlQuery, query)
	case *TopNStatement:
		return t.translateTopNStatement(stmt, yamlQuery, query)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
}

// translateSelectStatement translates SELECT statements
func (t *Translator) translateSelectStatement(stmt *SelectStatement, yamlQuery *QueryYAML, query *ParsedQuery) (*QueryYAML, error) {
	// Set query trace flag
	yamlQuery.Trace = stmt.QueryTrace

	// Translate TIME clause
	if stmt.Time != nil {
		timeRange, err := t.translateTimeCondition(stmt.Time)
		if err != nil {
			return nil, err
		}
		yamlQuery.TimeRange = timeRange
	}

	// Translate WHERE clause
	if stmt.Where != nil {
		criteria, err := t.translateWhereClause(stmt.Where)
		if err != nil {
			return nil, err
		}
		yamlQuery.Criteria = criteria
	}

	// Set LIMIT and OFFSET
	yamlQuery.Limit = stmt.Limit
	yamlQuery.Offset = stmt.Offset

	// Translate based on resource type
	resourceType := query.ResourceType
	if resourceType == ResourceTypeAuto {
		resourceType = t.context.DefaultResourceType
	}

	switch resourceType {
	case ResourceTypeStream, ResourceTypeTrace:
		return t.translateStreamOrTraceQuery(stmt, yamlQuery)
	case ResourceTypeMeasure:
		return t.translateMeasureQuery(stmt, yamlQuery)
	case ResourceTypeProperty:
		return t.translatePropertyQuery(stmt, yamlQuery)
	default:
		// Auto-detect or use default
		return t.translateStreamOrTraceQuery(stmt, yamlQuery)
	}
}

// translateStreamOrTraceQuery translates stream/trace specific fields
func (t *Translator) translateStreamOrTraceQuery(stmt *SelectStatement, yamlQuery *QueryYAML) (*QueryYAML, error) {
	// Translate projection
	if stmt.Projection != nil {
		if stmt.Projection.All {
			// SELECT * - no specific projection needed
		} else if stmt.Projection.Empty {
			// SELECT () - empty projection for traces
			yamlQuery.Projection = []string{}
		} else if len(stmt.Projection.Columns) > 0 {
			for _, col := range stmt.Projection.Columns {
				yamlQuery.Projection = append(yamlQuery.Projection, col.Name)
			}
		}
	}

	// Translate ORDER BY
	if stmt.OrderBy != nil {
		yamlQuery.OrderBy = &OrderByYAML{
			IndexRuleName: stmt.OrderBy.Column,
		}
		if stmt.OrderBy.Desc {
			yamlQuery.OrderBy.Sort = "DESC"
		} else {
			yamlQuery.OrderBy.Sort = "ASC"
		}
	}

	return yamlQuery, nil
}

// translateMeasureQuery translates measure specific fields
func (t *Translator) translateMeasureQuery(stmt *SelectStatement, yamlQuery *QueryYAML) (*QueryYAML, error) {
	// Translate projection
	if stmt.Projection != nil {
		if stmt.Projection.All {
			// SELECT * - no specific projection needed
		} else if stmt.Projection.TopN != nil {
			// Handle TOP N projection
			yamlQuery.Top = &TopYAML{
				Number: stmt.Projection.TopN.N,
			}
			if stmt.OrderBy != nil {
				if stmt.OrderBy.Desc {
					yamlQuery.Top.FieldValueSort = "DESC"
				} else {
					yamlQuery.Top.FieldValueSort = "ASC"
				}
			}
		} else if len(stmt.Projection.Columns) > 0 {
			// Separate tags and fields based on column type or function
			for _, col := range stmt.Projection.Columns {
				if col.Function != nil {
					// Aggregate function
					yamlQuery.Agg = &AggregationYAML{
						Function:  col.Function.Function,
						FieldName: col.Function.Column,
					}
				} else {
					// Regular column - determine if tag or field
					switch col.Type {
					case ColumnTypeTag:
						yamlQuery.TagProjection = append(yamlQuery.TagProjection, col.Name)
					case ColumnTypeField:
						yamlQuery.FieldProjection = append(yamlQuery.FieldProjection, col.Name)
					default:
						// Auto-detect: assume tag for now (schema lookup would be needed)
						yamlQuery.TagProjection = append(yamlQuery.TagProjection, col.Name)
					}
				}
			}
		}
	}

	// Translate GROUP BY
	if stmt.GroupBy != nil {
		yamlQuery.GroupBy = &GroupByYAML{
			TagProjection: stmt.GroupBy.Columns,
		}
	}

	// Translate ORDER BY
	if stmt.OrderBy != nil {
		yamlQuery.OrderBy = &OrderByYAML{
			IndexRuleName: stmt.OrderBy.Column,
		}
		if stmt.OrderBy.Desc {
			yamlQuery.OrderBy.Sort = "DESC"
		} else {
			yamlQuery.OrderBy.Sort = "ASC"
		}
	}

	return yamlQuery, nil
}

// translatePropertyQuery translates property specific fields
func (t *Translator) translatePropertyQuery(stmt *SelectStatement, yamlQuery *QueryYAML) (*QueryYAML, error) {
	// Translate projection
	if stmt.Projection != nil && !stmt.Projection.All && len(stmt.Projection.Columns) > 0 {
		for _, col := range stmt.Projection.Columns {
			yamlQuery.Projection = append(yamlQuery.Projection, col.Name)
		}
	}

	// Extract IDs from WHERE clause if present
	if stmt.Where != nil {
		for _, condition := range stmt.Where.Conditions {
			if strings.ToUpper(condition.Left) == "ID" {
				if condition.Operator == OpEqual && condition.Right != nil {
					yamlQuery.IDs = []string{condition.Right.StringVal}
				} else if condition.Operator == OpIn && len(condition.Values) > 0 {
					for _, val := range condition.Values {
						yamlQuery.IDs = append(yamlQuery.IDs, val.StringVal)
					}
				}
			}
		}
	}

	return yamlQuery, nil
}

// translateTopNStatement translates SHOW TOP N statements
func (t *Translator) translateTopNStatement(stmt *TopNStatement, yamlQuery *QueryYAML, query *ParsedQuery) (*QueryYAML, error) {
	// Set Top-N specific fields
	yamlQuery.TopN = stmt.TopN
	yamlQuery.Trace = stmt.QueryTrace

	// Translate TIME clause
	if stmt.Time != nil {
		timeRange, err := t.translateTimeCondition(stmt.Time)
		if err != nil {
			return nil, err
		}
		yamlQuery.TimeRange = timeRange
	}

	// Translate WHERE clause to conditions (Top-N uses different field name)
	if stmt.Where != nil {
		conditions, err := t.translateWhereClause(stmt.Where)
		if err != nil {
			return nil, err
		}
		yamlQuery.Conditions = conditions
	}

	// Translate AGGREGATE BY
	if stmt.AggregateBy != nil {
		yamlQuery.Agg = &AggregationYAML{
			Function:  stmt.AggregateBy.Function,
			FieldName: stmt.AggregateBy.Column,
		}
	}

	// Translate ORDER BY
	if stmt.OrderBy != nil {
		if stmt.OrderBy.Desc {
			yamlQuery.FieldValueSort = "DESC"
		} else {
			yamlQuery.FieldValueSort = "ASC"
		}
	}

	return yamlQuery, nil
}

// translateTimeCondition translates TIME conditions to time range
func (t *Translator) translateTimeCondition(timeCondition *TimeCondition) (*TimeRangeYAML, error) {
	timeRange := &TimeRangeYAML{}

	switch timeCondition.Operator {
	case TimeOpEqual:
		timestamp, err := t.parseTimestamp(timeCondition.Timestamp)
		if err != nil {
			return nil, err
		}
		timeRange.Begin = timestamp
		timeRange.End = timestamp
	case TimeOpGreater:
		timestamp, err := t.parseTimestamp(timeCondition.Timestamp)
		if err != nil {
			return nil, err
		}
		timeRange.Begin = timestamp
		// End time not specified, use current time
		timeRange.End = t.context.CurrentTime.Format(time.RFC3339)
	case TimeOpLess:
		timestamp, err := t.parseTimestamp(timeCondition.Timestamp)
		if err != nil {
			return nil, err
		}
		// Begin time not specified, use a time far in the past
		timeRange.Begin = "1970-01-01T00:00:00Z"
		timeRange.End = timestamp
	case TimeOpGreaterEqual:
		timestamp, err := t.parseTimestamp(timeCondition.Timestamp)
		if err != nil {
			return nil, err
		}
		timeRange.Begin = timestamp
		timeRange.End = t.context.CurrentTime.Format(time.RFC3339)
	case TimeOpLessEqual:
		timestamp, err := t.parseTimestamp(timeCondition.Timestamp)
		if err != nil {
			return nil, err
		}
		timeRange.Begin = "1970-01-01T00:00:00Z"
		timeRange.End = timestamp
	case TimeOpBetween:
		begin, err := t.parseTimestamp(timeCondition.Begin)
		if err != nil {
			return nil, err
		}
		end, err := t.parseTimestamp(timeCondition.End)
		if err != nil {
			return nil, err
		}
		timeRange.Begin = begin
		timeRange.End = end
	}

	return timeRange, nil
}

// parseTimestamp parses timestamp string (absolute or relative)
func (t *Translator) parseTimestamp(timestamp string) (string, error) {
	// Try parsing as absolute time first (RFC3339)
	if parsedTime, err := time.Parse(time.RFC3339, timestamp); err == nil {
		return parsedTime.Format(time.RFC3339), nil
	}

	// Try parsing as relative time (duration string)
	if strings.ToLower(timestamp) == "now" {
		return t.context.CurrentTime.Format(time.RFC3339), nil
	}

	duration, err := str2duration.ParseDuration(timestamp)
	if err != nil {
		return "", fmt.Errorf("invalid timestamp format: %s", timestamp)
	}

	resultTime := t.context.CurrentTime.Add(duration)
	return resultTime.Format(time.RFC3339), nil
}

// translateWhereClause translates WHERE conditions to criteria format
func (t *Translator) translateWhereClause(where *WhereClause) ([]map[string]interface{}, error) {
	var criteria []map[string]interface{}

	for _, condition := range where.Conditions {
		criterion := make(map[string]interface{})

		// Set tag name
		criterion["tagName"] = condition.Left

		// Set operator and values
		switch condition.Operator {
		case OpEqual:
			criterion["op"] = "BINARY_OP_EQ"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		case OpNotEqual:
			criterion["op"] = "BINARY_OP_NE"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		case OpGreater:
			criterion["op"] = "BINARY_OP_GT"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		case OpLess:
			criterion["op"] = "BINARY_OP_LT"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		case OpGreaterEqual:
			criterion["op"] = "BINARY_OP_GE"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		case OpLessEqual:
			criterion["op"] = "BINARY_OP_LE"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		case OpIn:
			criterion["op"] = "BINARY_OP_IN"
			var values []interface{}
			for _, val := range condition.Values {
				values = append(values, t.translateValue(val))
			}
			criterion["value"] = map[string]interface{}{
				"strArray": map[string]interface{}{
					"value": values,
				},
			}
		case OpNotIn:
			criterion["op"] = "BINARY_OP_NOT_IN"
			var values []interface{}
			for _, val := range condition.Values {
				values = append(values, t.translateValue(val))
			}
			criterion["value"] = map[string]interface{}{
				"strArray": map[string]interface{}{
					"value": values,
				},
			}
		case OpHaving:
			criterion["op"] = "BINARY_OP_HAVING"
			var values []interface{}
			for _, val := range condition.Values {
				values = append(values, t.translateValue(val))
			}
			criterion["value"] = map[string]interface{}{
				"strArray": map[string]interface{}{
					"value": values,
				},
			}
		case OpNotHaving:
			criterion["op"] = "BINARY_OP_NOT_HAVING"
			var values []interface{}
			for _, val := range condition.Values {
				values = append(values, t.translateValue(val))
			}
			criterion["value"] = map[string]interface{}{
				"strArray": map[string]interface{}{
					"value": values,
				},
			}
		case OpMatch:
			criterion["op"] = "BINARY_OP_MATCH"
			if condition.Right != nil {
				criterion["value"] = t.translateValue(condition.Right)
			}
		}

		criteria = append(criteria, criterion)
	}

	return criteria, nil
}

// translateValue translates a value to the appropriate YAML format
func (t *Translator) translateValue(value *Value) interface{} {
	if value.IsNull {
		return nil
	}

	switch value.Type {
	case ValueTypeString:
		return map[string]interface{}{
			"str": map[string]interface{}{
				"value": value.StringVal,
			},
		}
	case ValueTypeInteger:
		return map[string]interface{}{
			"int": map[string]interface{}{
				"value": strconv.FormatInt(value.Integer, 10),
			},
		}
	default:
		return nil
	}
}

// TranslateQuery is a convenience function to translate a BydbQL query string to YAML
func TranslateQuery(query string, context *QueryContext) ([]byte, []string, error) {
	parsed, errors := ParseQuery(query)
	if len(errors) > 0 {
		return nil, errors, fmt.Errorf("parsing errors occurred")
	}

	if parsed == nil {
		return nil, []string{"failed to parse query"}, fmt.Errorf("parsing failed")
	}

	translator := NewTranslator(context)
	yamlBytes, err := translator.TranslateToYAML(parsed)
	if err != nil {
		return nil, []string{err.Error()}, err
	}

	return yamlBytes, nil, nil
}