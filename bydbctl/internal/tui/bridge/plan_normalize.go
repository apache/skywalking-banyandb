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

import "strings"

// normalizePlanArgument performs lexical normalization only. Structural or semantic
// mistakes must be rejected by the strict decoder and planner instead of being silently rewritten.
func normalizePlanArgument(value any) any {
	planMap, ok := value.(map[string]any)
	if !ok {
		return value
	}
	normalized := copyPlanMap(planMap)
	if workflowSteps, stepsOK := normalized["steps"].([]any); stepsOK {
		normalizedSteps := make([]any, 0, len(workflowSteps))
		for _, step := range workflowSteps {
			normalizedSteps = append(normalizedSteps, normalizePlanArgument(step))
		}
		normalized["steps"] = normalizedSteps
		return normalized
	}
	if resource, resourceOK := normalized["resource"].(map[string]any); resourceOK {
		normalizedResource := copyPlanMap(resource)
		if resourceType, typeOK := normalizedResource["type"].(string); typeOK {
			normalizedResource["type"] = strings.ToUpper(strings.TrimSpace(resourceType))
		}
		normalized["resource"] = normalizedResource
	}
	if aggregate, aggregateOK := normalized["aggregate"].(map[string]any); aggregateOK {
		normalized["aggregate"] = normalizeAggregateMap(aggregate)
	}
	if orderBy, orderOK := normalized["order_by"].(map[string]any); orderOK {
		normalizedOrder := copyPlanMap(orderBy)
		if direction, directionOK := normalizedOrder["direction"].(string); directionOK {
			normalizedOrder["direction"] = strings.ToUpper(strings.TrimSpace(direction))
		}
		normalized["order_by"] = normalizedOrder
	}
	if projection, projectionOK := normalized["projection"].([]any); projectionOK {
		normalizedProjection := make([]any, 0, len(projection))
		for _, item := range projection {
			projectionMap, mapOK := item.(map[string]any)
			if !mapOK {
				normalizedProjection = append(normalizedProjection, item)
				continue
			}
			normalizedItem := copyPlanMap(projectionMap)
			if aggregate, aggregateOK := normalizedItem["aggregate"].(map[string]any); aggregateOK {
				normalizedItem["aggregate"] = normalizeAggregateMap(aggregate)
			}
			normalizedProjection = append(normalizedProjection, normalizedItem)
		}
		normalized["projection"] = normalizedProjection
	}
	if filter, filterOK := normalized["filter"].(map[string]any); filterOK {
		normalized["filter"] = normalizeFilterMap(filter)
	}
	return normalized
}

func normalizeAggregateMap(aggregate map[string]any) map[string]any {
	normalized := copyPlanMap(aggregate)
	if function, functionOK := normalized["function"].(string); functionOK {
		normalized["function"] = strings.ToUpper(strings.TrimSpace(function))
	}
	return normalized
}

func normalizeFilterMap(filter map[string]any) map[string]any {
	normalized := copyPlanMap(filter)
	if operator, operatorOK := normalized["operator"].(string); operatorOK {
		normalized["operator"] = normalizeFilterOperator(operator)
	}
	if children, childrenOK := normalized["children"].([]any); childrenOK {
		normalizedChildren := make([]any, 0, len(children))
		for _, child := range children {
			childMap, childOK := child.(map[string]any)
			if !childOK {
				normalizedChildren = append(normalizedChildren, child)
				continue
			}
			normalizedChildren = append(normalizedChildren, normalizeFilterMap(childMap))
		}
		normalized["children"] = normalizedChildren
	}
	return normalized
}

func normalizeFilterOperator(operator string) string {
	switch strings.ToUpper(strings.TrimSpace(operator)) {
	case "=", "==", "EQ", "EQUAL", "EQUALS":
		return "="
	case "!=", "<>", "NE", "NOT_EQUAL", "NOT EQUAL":
		return "!="
	case "GT", ">":
		return ">"
	case "GTE", ">=", "GE":
		return ">="
	case "LT", "<":
		return "<"
	case "LTE", "<=", "LE":
		return "<="
	case "IN":
		return "IN"
	case "NOT IN", "NOTIN", "NOT_IN":
		return "NOT IN"
	case "AND":
		return "AND"
	case "OR":
		return "OR"
	default:
		return strings.TrimSpace(operator)
	}
}

func copyPlanMap(planMap map[string]any) map[string]any {
	copied := make(map[string]any, len(planMap))
	for key, value := range planMap {
		copied[key] = value
	}
	return copied
}
