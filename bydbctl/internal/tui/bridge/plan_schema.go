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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bridge

import "github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"

func proposeQueryPlanInputSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"oneOf": []map[string]any{
			{"required": []string{"plan"}},
			{"required": []string{"workflow"}},
		},
		"properties": map[string]any{
			"plan":     queryPlanObjectSchema(),
			"workflow": workflowPlanObjectSchema(),
		},
		"$defs": map[string]any{
			"predicate": predicateSchema(),
		},
	}
}

func workflowPlanObjectSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"steps"},
		"additionalProperties": false,
		"properties": map[string]any{
			"steps": map[string]any{
				"type":     "array",
				"minItems": 1,
				"maxItems": 10,
				"items":    queryPlanObjectSchema(),
			},
		},
	}
}

func queryPlanObjectSchema() map[string]any {
	return map[string]any{
		"oneOf": []map[string]any{
			selectPlanSchema(),
			topNPlanSchema(),
		},
	}
}

func selectPlanSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"resource"},
		"additionalProperties": false,
		"properties": map[string]any{
			"resource":        resourceSchema([]string{"MEASURE", "STREAM", "TRACE", "PROPERTY"}),
			"projection":      projectionSchema(),
			"projection_mode": map[string]any{"type": "string", "enum": []string{"ALL", "NONE"}},
			"filter":          map[string]any{"$ref": "#/$defs/predicate"},
			"aggregate":       aggregateSchema(true),
			"order_by":        selectOrderSchema(),
			"time_range":      timeRangeSchema(),
			"group_by":        stringArraySchema(1),
			"limit":           boundedIntegerSchema(1, 1000),
			"id":              map[string]any{"type": "string", "minLength": 1},
		},
	}
}

func topNPlanSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"resource", "top_n"},
		"additionalProperties": false,
		"properties": map[string]any{
			"resource":   resourceSchema([]string{"TOPN"}),
			"filter":     map[string]any{"$ref": "#/$defs/predicate"},
			"aggregate":  aggregateSchema(false),
			"order_by":   topNOrderSchema(),
			"time_range": timeRangeSchema(),
			"top_n":      boundedIntegerSchema(1, 1000),
			"id":         map[string]any{"type": "string", "minLength": 1},
		},
	}
}

func resourceSchema(resourceTypes []string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"type", "name", "groups"},
		"additionalProperties": false,
		"properties": map[string]any{
			"type":   map[string]any{"type": "string", "enum": resourceTypes},
			"name":   map[string]any{"type": "string", "minLength": 1},
			"groups": stringArraySchema(1),
		},
	}
}

func projectionSchema() map[string]any {
	return map[string]any{
		"type":     "array",
		"minItems": 1,
		"items": map[string]any{
			"oneOf": []map[string]any{
				{
					"type":                 "object",
					"required":             []string{"column"},
					"additionalProperties": false,
					"properties": map[string]any{
						"column": map[string]any{"type": "string", "minLength": 1},
					},
				},
				{
					"type":                 "object",
					"required":             []string{"aggregate"},
					"additionalProperties": false,
					"properties": map[string]any{
						"aggregate": aggregateSchema(true),
					},
				},
			},
		},
	}
}

func aggregateSchema(requireColumn bool) map[string]any {
	required := []string{"function"}
	properties := map[string]any{
		"function": map[string]any{"type": "string", "enum": []string{"MEAN", "COUNT", "MAX", "MIN", "SUM"}},
	}
	if requireColumn {
		required = append(required, "column")
		properties["column"] = map[string]any{"type": "string", "minLength": 1}
	}
	return map[string]any{
		"type":                 "object",
		"required":             required,
		"additionalProperties": false,
		"properties":           properties,
	}
}

func predicateSchema() map[string]any {
	return map[string]any{
		"oneOf": []map[string]any{
			{
				"type":                 "object",
				"required":             []string{"column", "operator", "value"},
				"additionalProperties": false,
				"properties": map[string]any{
					"column":   map[string]any{"type": "string", "minLength": 1},
					"operator": map[string]any{"type": "string", "enum": []string{"=", "!=", ">", ">=", "<", "<=", "IN", "NOT IN"}},
					"value":    predicateValueSchema(),
				},
			},
			{
				"type":                 "object",
				"required":             []string{"operator", "children"},
				"additionalProperties": false,
				"properties": map[string]any{
					"operator": map[string]any{"type": "string", "enum": []string{"AND", "OR"}},
					"children": map[string]any{
						"type":     "array",
						"minItems": 2,
						"items":    map[string]any{"$ref": "#/$defs/predicate"},
					},
				},
			},
		},
	}
}

func predicateValueSchema() map[string]any {
	scalar := map[string]any{
		"type": []string{"string", "integer", "number", "null"},
	}
	return map[string]any{
		"oneOf": []map[string]any{
			scalar,
			{
				"type":     "array",
				"minItems": 1,
				"items":    scalar,
			},
		},
	}
}

func selectOrderSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"direction"},
		"additionalProperties": false,
		"properties": map[string]any{
			"index_rule": map[string]any{"type": "string", "minLength": 1},
			"direction":  map[string]any{"type": "string", "enum": []string{"ASC", "DESC"}},
		},
	}
}

func topNOrderSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"direction"},
		"additionalProperties": false,
		"properties": map[string]any{
			"direction": map[string]any{"type": "string", "enum": []string{"ASC", "DESC"}},
		},
	}
}

func timeRangeSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"required":             []string{"start"},
		"additionalProperties": false,
		"properties": map[string]any{
			"start": map[string]any{"type": "string", "minLength": 1},
			"end":   map[string]any{"type": "string", "minLength": 1},
		},
	}
}

func stringArraySchema(minItems int) map[string]any {
	return map[string]any{
		"type":        "array",
		"minItems":    minItems,
		"uniqueItems": true,
		"items":       map[string]any{"type": "string", "minLength": 1},
	}
}

func boundedIntegerSchema(minimum, maximum int) map[string]any {
	return map[string]any{
		"type":    "integer",
		"minimum": minimum,
		"maximum": maximum,
	}
}

func queryPlanSchemaHint() string {
	return "Submit exactly one strict SelectPlan or TopNPlan. " +
		"SelectPlan uses MEASURE|STREAM|TRACE|PROPERTY and order_by.index_rule. " +
		"TopNPlan uses a real TOPN aggregation resource, top_n, aggregate.function, and direction-only order_by. " +
		"Unknown fields and structural coercions are rejected."
}

func planConstraintsForSnapshot(snapshot session.SchemaSnapshot) map[string]any {
	projectionColumns := make([]string, 0, len(snapshot.Columns))
	filterColumns := make([]string, 0, len(snapshot.Columns)+1)
	numericFields := make([]string, 0, len(snapshot.Fields))
	groupByColumns := make([]string, 0, len(snapshot.Columns))
	for _, column := range snapshot.Columns {
		projectionColumns = append(projectionColumns, column.Name)
		if column.Kind == session.SchemaColumnTag || column.Kind == session.SchemaColumnEntityTag {
			if snapshot.Type != session.ResourceTypeTopN || containsConstraintValue(snapshot.EntityTags, column.Name) {
				filterColumns = append(filterColumns, column.Name)
			}
			groupByColumns = append(groupByColumns, column.Name)
		}
		if column.Kind == session.SchemaColumnField {
			groupByColumns = append(groupByColumns, column.Name)
			if column.Type == session.SchemaValueTypeInt || column.Type == session.SchemaValueTypeFloat {
				numericFields = append(numericFields, column.Name)
			}
		}
	}
	if snapshot.Type == session.ResourceTypeProperty {
		filterColumns = append([]string{"ID"}, filterColumns...)
	}
	sortableIndexes := make([]map[string]any, 0, len(snapshot.SortableIndexes))
	for _, sortableIndex := range snapshot.SortableIndexes {
		sortableIndexes = append(sortableIndexes, map[string]any{
			"rule_name": sortableIndex.RuleName,
			"tags":      append([]string(nil), sortableIndex.Tags...),
		})
	}
	return map[string]any{
		"resource": map[string]any{
			"type":   snapshot.Type,
			"name":   snapshot.Name,
			"groups": append([]string(nil), snapshot.Groups...),
		},
		"projection_columns": projectionColumns,
		"filter_columns":     filterColumns,
		"numeric_fields":     numericFields,
		"group_by_columns":   groupByColumns,
		"sortable_indexes":   sortableIndexes,
		"source_measure":     snapshot.SourceMeasure,
		"source_group":       snapshot.SourceMeasureGroup,
		"topn_field_sort":    snapshot.FieldValueSort,
		"schema_fingerprint": snapshot.Fingerprint,
		"limit_maximum":      1000,
	}
}

func containsConstraintValue(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
