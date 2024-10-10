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

package inverted

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	minTerm = string([][]byte{convert.Int64ToBytes(math.MinInt64)}[0])
	maxTerm = string([][]byte{convert.Int64ToBytes(math.MaxInt64)}[0])
	minInf  = "-inf"
	maxInf  = "+inf"
)

// GlobalIndexError represents a index rule is "global".
// The local filter can't handle it.
type GlobalIndexError struct {
	IndexRule *databasev1.IndexRule
	Expr      logical.LiteralExpr
}

func (g GlobalIndexError) Error() string { return g.IndexRule.String() }

var _ index.Query = (*queryNode)(nil)

// queryNode is a wrapper for bluge.Query.
type queryNode struct {
	query bluge.Query
	node
}

func (q *queryNode) String() string {
	return q.node.String()
}

// BuildLocalQuery returns blugeQuery for local indices.
func BuildLocalQuery(criteria *modelv1.Criteria, schema logical.Schema, entityDict map[string]int,
	entity []*modelv1.TagValue,
) (index.Query, [][]*modelv1.TagValue, bool, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, false, nil
	}
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, parsedEntity, err := logical.ParseExprOrEntity(entityDict, entity, cond)
		if err != nil {
			return nil, nil, false, err
		}
		if parsedEntity != nil {
			return nil, parsedEntity, false, nil
		}
		if ok, indexRule := schema.IndexDefined(cond.Name); ok {
			return parseConditionToQuery(cond, indexRule, expr, entity)
		}
		return nil, nil, false, errors.Wrapf(logical.ErrUnsupportedConditionOp, "mandatory index rule conf:%s", cond)
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, nil, false, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return BuildLocalQuery(le.Right, schema, entityDict, entity)
		}
		if le.GetRight() == nil {
			return BuildLocalQuery(le.Left, schema, entityDict, entity)
		}
		left, leftEntities, leftIsMatchAllQuery, err := BuildLocalQuery(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, false, err
		}
		right, rightEntities, rightIsMatchAllQuery, err := BuildLocalQuery(le.Right, schema, entityDict, entity)
		if err != nil {
			return nil, nil, false, err
		}
		entities := logical.ParseEntities(le.Op, entity, leftEntities, rightEntities)
		if entities == nil {
			return nil, nil, false, nil
		}
		if left == nil && right == nil {
			return nil, entities, false, nil
		}
		if leftIsMatchAllQuery && rightIsMatchAllQuery {
			return &queryNode{
				query: bluge.NewMatchAllQuery(),
				node:  newMatchAllNode(),
			}, entities, true, nil
		}
		switch le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			query, node := bluge.NewBooleanQuery(), newMustNode()
			if left != nil {
				query.AddMust(left.(*queryNode).query)
				node.Append(left.(*queryNode).node)
			}
			if right != nil {
				query.AddMust(right.(*queryNode).query)
				node.Append(right.(*queryNode).node)
			}
			return &queryNode{query, node}, entities, false, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			if leftIsMatchAllQuery || rightIsMatchAllQuery {
				return &queryNode{
					query: bluge.NewMatchAllQuery(),
					node:  newMatchAllNode(),
				}, entities, true, nil
			}
			query, node := bluge.NewBooleanQuery(), newShouldNode()
			query.SetMinShould(1)
			if left != nil {
				query.AddShould(left.(*queryNode).query)
				node.Append(left.(*queryNode).node)
			}
			if right != nil {
				query.AddShould(right.(*queryNode).query)
				node.Append(right.(*queryNode).node)
			}
			return &queryNode{query, node}, entities, false, nil
		}
	}
	return nil, nil, false, logical.ErrInvalidCriteriaType
}

func parseConditionToQuery(cond *modelv1.Condition, indexRule *databasev1.IndexRule,
	expr logical.LiteralExpr, entity []*modelv1.TagValue,
) (*queryNode, [][]*modelv1.TagValue, bool, error) {
	field := string(convert.Uint32ToBytes(indexRule.Metadata.Id))
	b := expr.Bytes()
	if len(b) < 1 {
		return &queryNode{
			query: bluge.NewMatchAllQuery(),
			node:  newMatchAllNode(),
		}, [][]*modelv1.TagValue{entity}, true, nil
	}
	term, str := string(b[0]), expr.String()
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		query := bluge.NewTermRangeInclusiveQuery(term, maxTerm, false, false).SetField(field)
		node := newTermRangeInclusiveNode(str, maxInf, false, false, indexRule)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_GE:
		query := bluge.NewTermRangeInclusiveQuery(term, maxTerm, true, false).SetField(field)
		node := newTermRangeInclusiveNode(str, maxInf, true, false, indexRule)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_LT:
		query := bluge.NewTermRangeInclusiveQuery(minTerm, term, false, false).SetField(field)
		node := newTermRangeInclusiveNode(minInf, str, false, false, indexRule)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_LE:
		query := bluge.NewTermRangeInclusiveQuery(minTerm, term, false, true).SetField(field)
		node := newTermRangeInclusiveNode(minInf, str, false, true, indexRule)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_EQ:
		query := bluge.NewTermQuery(term).SetField(field)
		node := newTermNode(str, indexRule)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_MATCH:
		analyzer, operator := getMatchOptions(indexRule.Analyzer, cond.MatchOption)
		query := bluge.NewMatchQuery(term).SetField(field).SetAnalyzer(analyzer).SetOperator(operator)
		node := newMatchNode(str, indexRule)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_NE:
		query, node := bluge.NewBooleanQuery(), newMustNotNode()
		query.AddMustNot(bluge.NewTermQuery(term).SetField(field))
		node.SetSubNode(newTermNode(str, indexRule))
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_HAVING:
		bb, elements := expr.Bytes(), expr.Elements()
		query, node := bluge.NewBooleanQuery(), newMustNode()
		for _, b := range bb {
			query.AddMust(bluge.NewTermQuery(string(b)).SetField(field))
		}
		for _, e := range elements {
			node.Append(newTermNode(e, indexRule))
		}
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		bb, elements := expr.Bytes(), expr.Elements()
		subQuery, subNode := bluge.NewBooleanQuery(), newMustNode()
		for _, b := range bb {
			subQuery.AddMust(bluge.NewTermQuery(string(b)).SetField(field))
		}
		for _, e := range elements {
			subNode.Append(newTermNode(e, indexRule))
		}
		query, node := bluge.NewBooleanQuery(), newMustNotNode()
		query.AddMustNot(subQuery)
		node.SetSubNode(node)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_IN:
		bb, elements := expr.Bytes(), expr.Elements()
		query, node := bluge.NewBooleanQuery(), newShouldNode()
		query.SetMinShould(1)
		for _, b := range bb {
			query.AddShould(bluge.NewTermQuery(string(b)).SetField(field))
		}
		for _, e := range elements {
			node.Append(newTermNode(e, indexRule))
		}
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
		bb, elements := expr.Bytes(), expr.Elements()
		subQuery, subNode := bluge.NewBooleanQuery(), newShouldNode()
		subQuery.SetMinShould(1)
		for _, b := range bb {
			subQuery.AddShould(bluge.NewTermQuery(string(b)).SetField(field))
		}
		for _, e := range elements {
			subNode.Append(newTermNode(e, indexRule))
		}
		query, node := bluge.NewBooleanQuery(), newMustNotNode()
		query.AddMustNot(subQuery)
		node.SetSubNode(subNode)
		return &queryNode{query, node}, [][]*modelv1.TagValue{entity}, false, nil
	}
	return nil, nil, false, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "index filter parses %v", cond)
}

type node interface {
	fmt.Stringer
}

type mustNode struct {
	subNodes []node
}

func newMustNode() *mustNode {
	return &mustNode{
		subNodes: make([]node, 0),
	}
}

func (m *mustNode) Append(subNode node) {
	m.subNodes = append(m.subNodes, subNode)
}

func (m *mustNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["must"] = m.subNodes
	return json.Marshal(data)
}

func (m *mustNode) String() string {
	return convert.JSONToString(m)
}

type shouldNode struct {
	subNodes []node
}

func newShouldNode() *shouldNode {
	return &shouldNode{
		subNodes: make([]node, 0),
	}
}

func (s *shouldNode) Append(subNode node) {
	s.subNodes = append(s.subNodes, subNode)
}

func (s *shouldNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["should"] = s.subNodes
	return json.Marshal(data)
}

func (s *shouldNode) String() string {
	return convert.JSONToString(s)
}

type mustNotNode struct {
	subNode node
}

func newMustNotNode() *mustNotNode {
	return &mustNotNode{}
}

func (m *mustNotNode) SetSubNode(subNode node) {
	m.subNode = subNode
}

func (m *mustNotNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["mustNot"] = m.subNode
	return json.Marshal(data)
}

func (m *mustNotNode) String() string {
	return convert.JSONToString(m)
}

type matchAllNode struct{}

func newMatchAllNode() *matchAllNode {
	return &matchAllNode{}
}

func (m *matchAllNode) String() string {
	return "matchAll"
}

type termRangeInclusiveNode struct {
	indexRule    *databasev1.IndexRule
	min          string
	max          string
	minInclusive bool
	maxInclusive bool
}

func newTermRangeInclusiveNode(minVal, maxVal string, minInclusive, maxInclusive bool, indexRule *databasev1.IndexRule) *termRangeInclusiveNode {
	return &termRangeInclusiveNode{
		indexRule:    indexRule,
		min:          minVal,
		max:          maxVal,
		minInclusive: minInclusive,
		maxInclusive: maxInclusive,
	}
}

func (t *termRangeInclusiveNode) MarshalJSON() ([]byte, error) {
	inner := make(map[string]interface{}, 1)
	var builder strings.Builder
	if t.minInclusive {
		builder.WriteString("[")
	} else {
		builder.WriteString("(")
	}
	builder.WriteString(t.min + " ")
	builder.WriteString(t.max)
	if t.maxInclusive {
		builder.WriteString("]")
	} else {
		builder.WriteString(")")
	}
	inner["range"] = builder.String()
	if t.indexRule != nil {
		inner["index"] = t.indexRule.Metadata.Name + ":" + t.indexRule.Metadata.Group
	}
	data := make(map[string]interface{}, 1)
	data["termRangeInclusive"] = inner
	return json.Marshal(data)
}

func (t *termRangeInclusiveNode) String() string {
	return convert.JSONToString(t)
}

type termNode struct {
	indexRule *databasev1.IndexRule
	term      string
}

func newTermNode(term string, indexRule *databasev1.IndexRule) *termNode {
	return &termNode{
		indexRule: indexRule,
		term:      term,
	}
}

func (t *termNode) MarshalJSON() ([]byte, error) {
	inner := make(map[string]interface{}, 1)
	if t.indexRule != nil {
		inner["index"] = t.indexRule.Metadata.Name + ":" + t.indexRule.Metadata.Group
	}
	inner["value"] = t.term
	data := make(map[string]interface{}, 1)
	data["term"] = inner
	return json.Marshal(data)
}

func (t *termNode) String() string {
	return convert.JSONToString(t)
}

type matchNode struct {
	indexRule *databasev1.IndexRule
	match     string
}

func newMatchNode(match string, indexRule *databasev1.IndexRule) *matchNode {
	return &matchNode{
		indexRule: indexRule,
		match:     match,
	}
}

func (m *matchNode) MarshalJSON() ([]byte, error) {
	inner := make(map[string]interface{}, 1)
	inner["index"] = m.indexRule.Metadata.Name + ":" + m.indexRule.Metadata.Group
	inner["value"] = m.match
	inner["analyzer"] = m.indexRule.Analyzer
	data := make(map[string]interface{}, 1)
	data["match"] = inner
	return json.Marshal(data)
}

func (m *matchNode) String() string {
	return convert.JSONToString(m)
}

type prefixNode struct {
	prefix string
}

func newPrefixNode(prefix string) *prefixNode {
	return &prefixNode{
		prefix: prefix,
	}
}

func (m *prefixNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["prefix"] = m.prefix
	return json.Marshal(data)
}

func (m *prefixNode) String() string {
	return convert.JSONToString(m)
}

type wildcardNode struct {
	wildcard string
}

func newWildcardNode(wildcard string) *wildcardNode {
	return &wildcardNode{
		wildcard: wildcard,
	}
}

func (m *wildcardNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["wildcard"] = m.wildcard
	return json.Marshal(data)
}

func (m *wildcardNode) String() string {
	return convert.JSONToString(m)
}
