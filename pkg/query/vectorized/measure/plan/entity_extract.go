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

package plan

import (
	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// extractSeriesMatchers partitions criteria into the series-matcher entity
// tuples and the remaining criteria the series index evaluates, replicating
// the split pkg/index/inverted.BuildQuery performed before the native
// cutover (NIDX-03 §5) -- unchanged logic, just emitting a *modelv1.Criteria
// subtree instead of a bluge query. It reuses logical.ParseExprOrEntity and
// logical.ParseEntities exactly as BuildQuery did; the only thing this
// function does differently is what a non-entity leaf condition becomes:
// BuildQuery compiled it to a bluge query node, this returns the condition
// itself (after confirming it has an index rule, matching BuildQuery's own
// "mandatory index rule conf" validation) for criteria.Filter to evaluate
// later via a FieldResolver.
//
// isMatchAll mirrors BuildQuery's internal bookkeeping for AND/OR
// combination (true when this subtree is a tautology, as when every leaf
// resolved to a bare entity condition); the caller does not need it beyond
// that recursion, since remaining == nil already means "no filter" either
// way criteria.Filter treats it identically to isMatchAll in both cases.
func extractSeriesMatchers(criteria *modelv1.Criteria, schema logical.Schema, entityDict map[string]int,
	entity []*modelv1.TagValue,
) (remaining *modelv1.Criteria, entities [][]*modelv1.TagValue, isMatchAll bool, err error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, false, nil
	}
	switch expression := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := expression.Condition
		_, parsedEntity, parseErr := logical.ParseExprOrEntity(entityDict, entity, cond)
		if parseErr != nil {
			return nil, nil, false, parseErr
		}
		if parsedEntity != nil {
			return nil, parsedEntity, false, nil
		}
		if ok, _ := schema.IndexDefined(cond.Name); !ok {
			return nil, nil, false, errors.Wrapf(logical.ErrUnsupportedConditionOp, "mandatory index rule conf:%s", cond)
		}
		return criteria, [][]*modelv1.TagValue{entity}, false, nil
	case *modelv1.Criteria_Le:
		le := expression.Le
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, nil, false, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return extractSeriesMatchers(le.Right, schema, entityDict, entity)
		}
		if le.GetRight() == nil {
			return extractSeriesMatchers(le.Left, schema, entityDict, entity)
		}
		left, leftEntities, leftAll, leftErr := extractSeriesMatchers(le.Left, schema, entityDict, entity)
		if leftErr != nil {
			return nil, nil, false, leftErr
		}
		right, rightEntities, rightAll, rightErr := extractSeriesMatchers(le.Right, schema, entityDict, entity)
		if rightErr != nil {
			return nil, nil, false, rightErr
		}
		mergedEntities := logical.ParseEntities(le.Op, entity, leftEntities, rightEntities)
		if mergedEntities == nil {
			return nil, nil, false, nil
		}
		if left == nil && right == nil {
			// Both sides reduced to a pure entity condition (no remaining
			// filter): the subtree is still a tautology whenever either side
			// already was one, matching BuildQuery's matchAll propagation.
			// Hardcoding false here (as this branch previously did) loses
			// that propagation up through nested AND/OR, producing a
			// non-nil "remaining" criteria the legacy evaluator would have
			// treated as always-true.
			return nil, mergedEntities, leftAll || rightAll, nil
		}
		if leftAll && rightAll {
			return nil, mergedEntities, true, nil
		}
		switch le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			return combineCriteriaAnd(left, right), mergedEntities, false, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			if leftAll || rightAll {
				return nil, mergedEntities, true, nil
			}
			// When one OR branch is entity-only (nil remaining), entity-level
			// series lookup already constrains the scope. If all entities are
			// specific (no AnyTagValue wildcards), the remaining filter is
			// unneeded -- matches BuildQuery's allEntitiesSpecific special case.
			if (left == nil || right == nil) && allEntitiesSpecific(mergedEntities) {
				return nil, mergedEntities, true, nil
			}
			return combineCriteriaOr(left, right), mergedEntities, false, nil
		}
		return nil, nil, false, logical.ErrInvalidCriteriaType
	}
	return nil, nil, false, logical.ErrInvalidCriteriaType
}

// validateIndexModeCriteria replicates pkg/index/inverted.buildIndexModeCriteria's
// two leaf-condition validations that BuildIndexModeQuery raised before the
// native cutover, which the native path's resolver (measureFieldResolver,
// via pkg/index/native/criteria.Filter) does not raise on its own: a
// resolver miss is a "soft" ok=false there (no hits for a positive
// condition), not an error. Index mode passes criteria straight through
// (dispatch.go does not call extractSeriesMatchers for it), so this walks
// the same criteria tree shape buildIndexModeCriteria did and errors on
// exactly the same two cases:
//
//   - a tag that is neither backed by an index rule nor an entity tag:
//     "mandatory index rule conf" (ErrUnsupportedConditionOp).
//   - a MATCH condition on an entity tag with no index rule (an index-mode
//     entity tag resolves to its synthetic _im_entity_tag_<tag> field, which
//     MATCH cannot analyze): "index rule is mandatory for match operation"
//     (ErrUnsupportedConditionOp).
//
// A tag backed by an index rule needs no check: any op, including MATCH,
// is valid against it.
func validateIndexModeCriteria(criteria *modelv1.Criteria, schema logical.Schema, entityDict map[string]int) error {
	if criteria == nil {
		return nil
	}
	switch expression := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := expression.Condition
		if ok, _ := schema.IndexDefined(cond.Name); ok {
			return nil
		}
		if _, ok := entityDict[cond.Name]; ok {
			if cond.Op == modelv1.Condition_BINARY_OP_MATCH {
				return errors.WithMessagef(logical.ErrUnsupportedConditionOp, "index rule is mandatory for match operation: %s", cond)
			}
			return nil
		}
		return errors.Wrapf(logical.ErrUnsupportedConditionOp, "mandatory index rule conf:%s", cond)
	case *modelv1.Criteria_Le:
		le := expression.Le
		if le.GetLeft() == nil && le.GetRight() == nil {
			return errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if err := validateIndexModeCriteria(le.GetLeft(), schema, entityDict); err != nil {
			return err
		}
		return validateIndexModeCriteria(le.GetRight(), schema, entityDict)
	default:
		return logical.ErrInvalidCriteriaType
	}
}

// combineCriteriaAnd mirrors BuildQuery's AddMust-only-non-nil behavior: a
// nil operand contributes no constraint, so the combination degenerates to
// whichever operand is non-nil (or nil, if both are).
func combineCriteriaAnd(left, right *modelv1.Criteria) *modelv1.Criteria {
	return combineCriteria(modelv1.LogicalExpression_LOGICAL_OP_AND, left, right)
}

// combineCriteriaOr mirrors BuildQuery's AddShould-only-non-nil behavior
// over a should query whose MinShould is 1: with exactly one non-nil
// operand, a one-clause should query is equivalent to evaluating that clause
// alone, so this also degenerates to the non-nil operand.
func combineCriteriaOr(left, right *modelv1.Criteria) *modelv1.Criteria {
	return combineCriteria(modelv1.LogicalExpression_LOGICAL_OP_OR, left, right)
}

func combineCriteria(op modelv1.LogicalExpression_LogicalOp, left, right *modelv1.Criteria) *modelv1.Criteria {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: op, Left: left, Right: right}}}
}

// allEntitiesSpecific reports whether every entry in every entity tuple is a
// concrete value (no AnyTagValue wildcard), ported unchanged from
// pkg/index/inverted.BuildQuery.
func allEntitiesSpecific(entities [][]*modelv1.TagValue) bool {
	for _, entity := range entities {
		for _, entry := range entity {
			if entry == pbv1.AnyTagValue {
				return false
			}
		}
	}
	return true
}

// measureFieldResolver implements model.FieldResolver (and so, structurally,
// pkg/index/native/criteria.FieldResolver) for Measure, per NIDX-03 §5: a
// tag backed by an index rule resolves to the rule's 4-byte field key and
// analyzer; an index-mode entity tag with no index rule (an entity tag can
// still appear in index-mode criteria, since index mode does not extract
// entity conditions into series matchers) resolves to its synthetic
// "_im_entity_tag_<tag>" field. Any other tag is unresolved (ok=false),
// which criteria.Filter treats as "no hits" for a positive condition and
// "unchanged universe" for a negative one.
type measureFieldResolver struct {
	schema logical.Schema
}

func (r measureFieldResolver) Field(tagName string) (string, string, bool) {
	if ok, rule := r.schema.IndexDefined(tagName); ok {
		fk := index.FieldKey{IndexRuleID: rule.Metadata.Id}
		return fk.Marshal(), rule.Analyzer, true
	}
	for _, name := range r.schema.EntityList() {
		if name == tagName {
			return index.IndexModeEntityTagPrefix + tagName, "", true
		}
	}
	return "", "", false
}
