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

package logical

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var errInvalidLogicalExpression = errors.New("invalid logical expression")

// GlobalIndexError represents a index rule is "global".
// The local filter can't handle it.
type GlobalIndexError struct {
	IndexRule *databasev1.IndexRule
	Expr      LiteralExpr
}

func (g GlobalIndexError) Error() string { return g.IndexRule.String() }

// BuildLocalQuery returns bluge.Query for local indices.
func BuildLocalQuery(criteria *modelv1.Criteria, schema Schema, entityDict map[string]int,
	entity []*modelv1.TagValue,
) (bluge.Query, [][]*modelv1.TagValue, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, nil
	}
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, parsedEntity, err := parseExprOrEntity(entityDict, entity, cond)
		if err != nil {
			return nil, nil, err
		}
		if parsedEntity != nil {
			return nil, parsedEntity, nil
		}
		if ok, indexRule := schema.IndexDefined(cond.Name); ok {
			return parseConditionToQuery(cond, indexRule, expr, entity)
		}
		return nil, nil, errors.Wrapf(errUnsupportedConditionOp, "mandatory index rule conf:%s", cond)
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, nil, errors.WithMessagef(errInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return BuildLocalQuery(le.Right, schema, entityDict, entity)
		}
		if le.GetRight() == nil {
			return BuildLocalQuery(le.Left, schema, entityDict, entity)
		}
		left, leftEntities, err := BuildLocalQuery(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		right, rightEntities, err := BuildLocalQuery(le.Right, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		entities := parseEntities(le.Op, entity, leftEntities, rightEntities)
		if entities == nil {
			return nil, nil, nil
		}
		if left == nil && right == nil {
			return nil, entities, nil
		}
		switch le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			query := bluge.NewBooleanQuery()
			if left != nil {
				query.AddMust(left)
			}
			if right != nil {
				query.AddMust(right)
			}
			return query, entities, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			query := bluge.NewBooleanQuery()
			query.SetMinShould(1)
			if left != nil {
				query.AddShould(left)
			}
			if right != nil {
				query.AddShould(right)
			}
			return query, entities, nil
		}
	}
	return nil, nil, errInvalidCriteriaType
}

// BuildLocalFilter returns a new index.Filter for local indices.
func BuildLocalFilter(criteria *modelv1.Criteria, schema Schema, entityDict map[string]int, entity []*modelv1.TagValue) (index.Filter, [][]*modelv1.TagValue, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, nil
	}
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, parsedEntity, err := parseExprOrEntity(entityDict, entity, cond)
		if err != nil {
			return nil, nil, err
		}
		if parsedEntity != nil {
			return nil, parsedEntity, nil
		}
		if ok, indexRule := schema.IndexDefined(cond.Name); ok {
			return parseConditionToFilter(cond, indexRule, expr, entity)
		}
		return ENode, [][]*modelv1.TagValue{entity}, nil
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, nil, errors.WithMessagef(errInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return BuildLocalFilter(le.Right, schema, entityDict, entity)
		}
		if le.GetRight() == nil {
			return BuildLocalFilter(le.Left, schema, entityDict, entity)
		}
		left, leftEntities, err := BuildLocalFilter(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		right, rightEntities, err := BuildLocalFilter(le.Right, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		entities := parseEntities(le.Op, entity, leftEntities, rightEntities)
		if entities == nil {
			return nil, nil, nil
		}
		if left == nil && right == nil {
			return nil, entities, nil
		}
		if left == ENode && right == ENode {
			return ENode, entities, nil
		}
		switch le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			and := newAnd(2)
			and.append(left).append(right)
			return and, entities, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			if left == ENode || right == ENode {
				return ENode, entities, nil
			}
			or := newOr(2)
			or.append(left).append(right)
			return or, entities, nil
		}
	}
	return nil, nil, errInvalidCriteriaType
}

func parseConditionToQuery(cond *modelv1.Condition, indexRule *databasev1.IndexRule,
	expr LiteralExpr, entity []*modelv1.TagValue,
) (bluge.Query, [][]*modelv1.TagValue, error) {
	field := string(convert.Uint32ToBytes(indexRule.Metadata.Id))
	b := expr.Bytes()
	if len(b) < 1 {
		return bluge.NewMatchAllQuery(), [][]*modelv1.TagValue{entity}, nil
	}
	term := string(b[0])
	minTerm, maxTerm := string([][]byte{convert.Int64ToBytes(math.MinInt64)}[0]), string([][]byte{convert.Int64ToBytes(math.MaxInt64)}[0])
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		return bluge.NewTermRangeInclusiveQuery(term, maxTerm, false, false).SetField(field), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_GE:
		return bluge.NewTermRangeQuery(term, maxTerm).SetField(field), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LT:
		return bluge.NewTermRangeInclusiveQuery(minTerm, term, false, false).SetField(field), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LE:
		return bluge.NewTermRangeInclusiveQuery(minTerm, term, false, true).SetField(field), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_EQ:
		return bluge.NewTermQuery(term).SetField(field), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_MATCH:
		return bluge.NewMatchQuery(term).SetField(field).SetAnalyzer(inverted.Analyzers[indexRule.Analyzer]), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NE:
		query := bluge.NewBooleanQuery()
		query.AddMustNot(bluge.NewTermQuery(term).SetField(field))
		return query, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_HAVING:
		bb := expr.Bytes()
		query := bluge.NewBooleanQuery()
		for _, b := range bb {
			query.AddMust(bluge.NewTermQuery(string(b)).SetField(field))
		}
		return query, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		bb := expr.Bytes()
		subQuery := bluge.NewBooleanQuery()
		for _, b := range bb {
			subQuery.AddMust(bluge.NewTermQuery(string(b)).SetField(field))
		}
		query := bluge.NewBooleanQuery()
		return query.AddMustNot(subQuery), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_IN:
		bb := expr.Bytes()
		query := bluge.NewBooleanQuery()
		query.SetMinShould(1)
		for _, b := range bb {
			query.AddShould(bluge.NewTermQuery(string(b)).SetField(field))
		}
		return query, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
		bb := expr.Bytes()
		subQuery := bluge.NewBooleanQuery()
		subQuery.SetMinShould(1)
		for _, b := range bb {
			subQuery.AddShould(bluge.NewTermQuery(string(b)).SetField(field))
		}
		query := bluge.NewBooleanQuery()
		return query.AddMustNot(subQuery), [][]*modelv1.TagValue{entity}, nil
	}
	return nil, nil, errors.WithMessagef(errUnsupportedConditionOp, "index filter parses %v", cond)
}

func parseConditionToFilter(cond *modelv1.Condition, indexRule *databasev1.IndexRule,
	expr LiteralExpr, entity []*modelv1.TagValue,
) (index.Filter, [][]*modelv1.TagValue, error) {
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		return newRange(indexRule, index.RangeOpts{
			Lower: bytes.Join(expr.Bytes(), nil),
		}), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_GE:
		return newRange(indexRule, index.RangeOpts{
			IncludesLower: true,
			Lower:         bytes.Join(expr.Bytes(), nil),
		}), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LT:
		return newRange(indexRule, index.RangeOpts{
			Upper: bytes.Join(expr.Bytes(), nil),
		}), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LE:
		return newRange(indexRule, index.RangeOpts{
			IncludesUpper: true,
			Upper:         bytes.Join(expr.Bytes(), nil),
		}), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_EQ:
		return newEq(indexRule, expr), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_MATCH:
		return newMatch(indexRule, expr), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NE:
		return newNot(indexRule, newEq(indexRule, expr)), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_HAVING:
		bb := expr.Bytes()
		l := len(bb)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		and := newAnd(l)
		for _, b := range bb {
			and.append(newEq(indexRule, newBytesLiteral(b)))
		}
		return and, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		bb := expr.Bytes()
		l := len(bb)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		and := newAnd(l)
		for _, b := range bb {
			and.append(newEq(indexRule, newBytesLiteral(b)))
		}
		return newNot(indexRule, and), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_IN:
		bb := expr.Bytes()
		l := len(bb)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		or := newOr(l)
		for _, b := range bb {
			or.append(newEq(indexRule, newBytesLiteral(b)))
		}
		return or, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
		bb := expr.Bytes()
		l := len(bb)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		or := newOr(l)
		for _, b := range bb {
			or.append(newEq(indexRule, newBytesLiteral(b)))
		}
		return newNot(indexRule, or), [][]*modelv1.TagValue{entity}, nil
	}
	return nil, nil, errors.WithMessagef(errUnsupportedConditionOp, "index filter parses %v", cond)
}

func parseExprOrEntity(entityDict map[string]int, entity []*modelv1.TagValue, cond *modelv1.Condition) (LiteralExpr, [][]*modelv1.TagValue, error) {
	entityIdx, ok := entityDict[cond.Name]
	if ok && cond.Op != modelv1.Condition_BINARY_OP_EQ && cond.Op != modelv1.Condition_BINARY_OP_IN {
		return nil, nil, errors.WithMessagef(errUnsupportedConditionOp, "tag belongs to the entity only supports EQ or IN operation in condition(%v)", cond)
	}
	switch v := cond.Value.Value.(type) {
	case *modelv1.TagValue_Str:
		if ok {
			parsedEntity := make([]*modelv1.TagValue, len(entity))
			copy(parsedEntity, entity)
			parsedEntity[entityIdx] = cond.Value
			return nil, [][]*modelv1.TagValue{parsedEntity}, nil
		}
		return str(v.Str.GetValue()), nil, nil
	case *modelv1.TagValue_StrArray:
		if ok && cond.Op == modelv1.Condition_BINARY_OP_IN {
			entities := make([][]*modelv1.TagValue, len(v.StrArray.Value))
			for i, va := range v.StrArray.Value {
				parsedEntity := make([]*modelv1.TagValue, len(entity))
				copy(parsedEntity, entity)
				parsedEntity[entityIdx] = &modelv1.TagValue{
					Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{
							Value: va,
						},
					},
				}
				entities[i] = parsedEntity
			}
			return nil, entities, nil
		}
		return &strArrLiteral{
			arr: v.StrArray.GetValue(),
		}, nil, nil
	case *modelv1.TagValue_Int:
		if ok {
			parsedEntity := make([]*modelv1.TagValue, len(entity))
			copy(parsedEntity, entity)
			parsedEntity[entityIdx] = cond.Value
			return nil, [][]*modelv1.TagValue{parsedEntity}, nil
		}
		return &int64Literal{
			int64: v.Int.GetValue(),
		}, nil, nil
	case *modelv1.TagValue_IntArray:
		if ok && cond.Op == modelv1.Condition_BINARY_OP_IN {
			entities := make([][]*modelv1.TagValue, len(v.IntArray.Value))
			for i, va := range v.IntArray.Value {
				parsedEntity := make([]*modelv1.TagValue, len(entity))
				copy(parsedEntity, entity)
				parsedEntity[entityIdx] = &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{
						Int: &modelv1.Int{
							Value: va,
						},
					},
				}
				entities[i] = parsedEntity
			}
			return nil, entities, nil
		}
		return &int64ArrLiteral{
			arr: v.IntArray.GetValue(),
		}, nil, nil
	case *modelv1.TagValue_Null:
		return nullLiteralExpr, nil, nil
	}
	return nil, nil, errors.WithMessagef(errUnsupportedConditionValue, "index filter parses %v", cond)
}

func parseEntities(op modelv1.LogicalExpression_LogicalOp, input []*modelv1.TagValue, left, right [][]*modelv1.TagValue) [][]*modelv1.TagValue {
	count := len(input)
	result := make([]*modelv1.TagValue, count)
	anyEntity := func(entities [][]*modelv1.TagValue) bool {
		for _, entity := range entities {
			for _, entry := range entity {
				if entry != pbv1.AnyTagValue {
					return false
				}
			}
		}
		return true
	}
	leftAny := anyEntity(left)
	rightAny := anyEntity(right)

	mergedEntities := make([][]*modelv1.TagValue, 0, len(left)+len(right))

	switch op {
	case modelv1.LogicalExpression_LOGICAL_OP_AND:
		if leftAny && !rightAny {
			return right
		}
		if !leftAny && rightAny {
			return left
		}
		mergedEntities = append(mergedEntities, left...)
		mergedEntities = append(mergedEntities, right...)
		for i := 0; i < count; i++ {
			entry := pbv1.AnyTagValue
			for j := 0; j < len(mergedEntities); j++ {
				e := mergedEntities[j][i]
				if e == pbv1.AnyTagValue {
					continue
				}
				if entry == pbv1.AnyTagValue {
					entry = e
				} else if pbv1.MustCompareTagValue(entry, e) != 0 {
					return nil
				}
			}
			result[i] = entry
		}
	case modelv1.LogicalExpression_LOGICAL_OP_OR:
		if leftAny {
			return left
		}
		if rightAny {
			return right
		}
		mergedEntities = append(mergedEntities, left...)
		mergedEntities = append(mergedEntities, right...)
		for i := 0; i < count; i++ {
			entry := pbv1.AnyTagValue
			for j := 0; j < len(mergedEntities); j++ {
				e := mergedEntities[j][i]
				if entry == pbv1.AnyTagValue {
					entry = e
				} else if pbv1.MustCompareTagValue(entry, e) != 0 {
					return mergedEntities
				}
			}
			result[i] = entry
		}
	}
	return [][]*modelv1.TagValue{result}
}

type fieldKey struct {
	*databasev1.IndexRule
}

func newFieldKey(indexRule *databasev1.IndexRule) fieldKey {
	return fieldKey{indexRule}
}

func (fk fieldKey) toIndex(seriesID common.SeriesID) index.FieldKey {
	return index.FieldKey{
		IndexRuleID: fk.Metadata.Id,
		Analyzer:    fk.Analyzer,
		SeriesID:    seriesID,
	}
}

type logicalOP interface {
	index.Filter
	merge(...posting.List) (posting.List, error)
}

type node struct {
	SubNodes []index.Filter `json:"sub_nodes,omitempty"`
}

func (n *node) append(sub index.Filter) *node {
	if sub == nil {
		return n
	}
	n.SubNodes = append(n.SubNodes, sub)
	return n
}

func execute(searcher index.GetSearcher, seriesID common.SeriesID, n *node, lp logicalOP) (posting.List, error) {
	if len(n.SubNodes) < 1 {
		return bList, nil
	}
	var result posting.List
	for _, sn := range n.SubNodes {
		r, err := sn.Execute(searcher, seriesID)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = r
			continue
		}
		result, err = lp.merge(result, r)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

type andNode struct {
	*node
}

func newAnd(size int) *andNode {
	return &andNode{
		node: &node{
			SubNodes: make([]index.Filter, 0, size),
		},
	}
}

func (an *andNode) merge(list ...posting.List) (posting.List, error) {
	var result posting.List
	for _, l := range list {
		if _, ok := l.(*bypassList); ok {
			continue
		}
		if result == nil {
			result = l
			continue
		}
		if err := result.Intersect(l); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (an *andNode) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	return execute(searcher, seriesID, an.node, an)
}

func (an *andNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["and"] = an.node.SubNodes
	return json.Marshal(data)
}

func (an *andNode) String() string {
	return jsonToString(an)
}

type orNode struct {
	*node
}

func newOr(size int) *orNode {
	return &orNode{
		node: &node{
			SubNodes: make([]index.Filter, 0, size),
		},
	}
}

func (on *orNode) merge(list ...posting.List) (posting.List, error) {
	var result posting.List
	for _, l := range list {
		// If a predicator is not indexed, all predicator are ignored.
		// The tagFilter will take up this job to filter this items.
		if _, ok := l.(*bypassList); ok {
			return bList, nil
		}
		if result == nil {
			result = l
			continue
		}
		if err := result.Union(l); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (on *orNode) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	return execute(searcher, seriesID, on.node, on)
}

func (on *orNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["or"] = on.node.SubNodes
	return json.Marshal(data)
}

func (on *orNode) String() string {
	return jsonToString(on)
}

type leaf struct {
	index.Filter
	Key  fieldKey
	Expr LiteralExpr
}

func (l *leaf) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["index"] = l.Key.IndexRule.Metadata.Name + ":" + l.Key.IndexRule.Metadata.Group
	data["expr"] = l.Expr.String()
	return json.Marshal(data)
}

type not struct {
	index.Filter
	Key   fieldKey
	Inner index.Filter
}

func newNot(indexRule *databasev1.IndexRule, inner index.Filter) *not {
	return &not{
		Key:   newFieldKey(indexRule),
		Inner: inner,
	}
}

func (n *not) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	s, err := searcher(n.Key.Type)
	if err != nil {
		return nil, err
	}
	all, err := s.MatchField(n.Key.toIndex(seriesID))
	if err != nil {
		return nil, err
	}
	list, err := n.Inner.Execute(searcher, seriesID)
	if err != nil {
		return nil, err
	}
	err = all.Difference(list)
	return all, err
}

func (n *not) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["not"] = n.Inner
	return json.Marshal(data)
}

func (n *not) String() string {
	return jsonToString(n)
}

type eq struct {
	*leaf
}

func newEq(indexRule *databasev1.IndexRule, values LiteralExpr) *eq {
	return &eq{
		leaf: &leaf{
			Key:  newFieldKey(indexRule),
			Expr: values,
		},
	}
}

func (eq *eq) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	s, err := searcher(eq.Key.Type)
	if err != nil {
		return nil, err
	}
	return s.MatchTerms(index.Field{
		Key:  eq.Key.toIndex(seriesID),
		Term: bytes.Join(eq.Expr.Bytes(), nil),
	})
}

func (eq *eq) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["eq"] = eq.leaf
	return json.Marshal(data)
}

func (eq *eq) String() string {
	return jsonToString(eq)
}

type match struct {
	*leaf
}

func newMatch(indexRule *databasev1.IndexRule, values LiteralExpr) *match {
	return &match{
		leaf: &leaf{
			Key:  newFieldKey(indexRule),
			Expr: values,
		},
	}
}

func (match *match) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	s, err := searcher(match.Key.Type)
	if err != nil {
		return nil, err
	}
	bb := match.Expr.Bytes()
	matches := make([]string, len(bb))
	for i, v := range bb {
		matches[i] = string(v)
	}
	return s.Match(
		match.Key.toIndex(seriesID),
		matches,
	)
}

func (match *match) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["match"] = match.leaf
	return json.Marshal(data)
}

func (match *match) String() string {
	return jsonToString(match)
}

type rangeOp struct {
	*leaf
	Opts index.RangeOpts
}

func newRange(indexRule *databasev1.IndexRule, opts index.RangeOpts) *rangeOp {
	return &rangeOp{
		leaf: &leaf{
			Key: newFieldKey(indexRule),
		},
		Opts: opts,
	}
}

func (r *rangeOp) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	s, err := searcher(r.Key.Type)
	if err != nil {
		return nil, err
	}
	return s.Range(r.Key.toIndex(seriesID), r.Opts)
}

func (r *rangeOp) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	var builder strings.Builder
	if r.Opts.Lower != nil {
		if r.Opts.IncludesLower {
			builder.WriteString("[")
		} else {
			builder.WriteString("(")
		}
	}
	builder.WriteString(base64.StdEncoding.EncodeToString(r.Opts.Lower))
	builder.WriteString(",")
	builder.WriteString(base64.StdEncoding.EncodeToString(r.Opts.Upper))
	if r.Opts.Upper != nil {
		if r.Opts.IncludesUpper {
			builder.WriteString("]")
		} else {
			builder.WriteString(")")
		}
	}
	data["key"] = r.Key.IndexRule.Metadata.Name + ":" + r.Key.Metadata.Group
	data["range"] = builder.String()
	return json.Marshal(data)
}

func (r *rangeOp) String() string {
	return jsonToString(r)
}

func jsonToString(marshaler json.Marshaler) string {
	bb, err := marshaler.MarshalJSON()
	if err != nil {
		return err.Error()
	}
	return string(bb)
}

var (
	// ENode is an empty node.
	ENode = new(emptyNode)
	bList = new(bypassList)
)

type emptyNode struct{}

func (an emptyNode) Execute(_ index.GetSearcher, _ common.SeriesID) (posting.List, error) {
	return bList, nil
}

func (an emptyNode) String() string {
	return "empty"
}

type bypassList struct{}

func (bl bypassList) Contains(_ uint64) bool {
	// all items should be fetched
	return true
}

func (bl bypassList) IsEmpty() bool {
	return false
}

func (bl bypassList) Max() (uint64, error) {
	panic("not invoked")
}

func (bl bypassList) Len() int {
	return 0
}

func (bl bypassList) Iterator() posting.Iterator {
	panic("not invoked")
}

func (bl bypassList) Clone() posting.List {
	panic("not invoked")
}

func (bl bypassList) Equal(_ posting.List) bool {
	panic("not invoked")
}

func (bl bypassList) Insert(_ uint64) {
	panic("not invoked")
}

func (bl bypassList) Intersect(_ posting.List) error {
	panic("not invoked")
}

func (bl bypassList) Difference(_ posting.List) error {
	panic("not invoked")
}

func (bl bypassList) Union(_ posting.List) error {
	panic("not invoked")
}

func (bl bypassList) UnionMany(_ []posting.List) error {
	panic("not invoked")
}

func (bl bypassList) AddIterator(_ posting.Iterator) error {
	panic("not invoked")
}

func (bl bypassList) AddRange(_ uint64, _ uint64) error {
	panic("not invoked")
}

func (bl bypassList) RemoveRange(_ uint64, _ uint64) error {
	panic("not invoked")
}

func (bl bypassList) Reset() {
	panic("not invoked")
}

func (bl bypassList) ToSlice() []uint64 {
	panic("not invoked")
}

func (bl bypassList) Marshall() ([]byte, error) {
	panic("not invoked")
}

func (bl bypassList) Unmarshall(_ []byte) error {
	panic("not invoked")
}

func (bl bypassList) SizeInBytes() int64 {
	panic("not invoked")
}
