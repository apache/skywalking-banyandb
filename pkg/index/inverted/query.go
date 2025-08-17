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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	termRangeQuery = "termRangeQuery"
	timeRangeQuery = "timeRangeQuery"
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

// BuildQuery returns blugeQuery for local indices.
func BuildQuery(criteria *modelv1.Criteria, schema logical.Schema, entityDict map[string]int,
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
			fk := index.FieldKey{IndexRuleID: indexRule.Metadata.Id}
			q, err := parseConditionToQuery(cond, indexRule, expr, fk.Marshal())
			if err != nil {
				return nil, nil, false, err
			}
			return q, [][]*modelv1.TagValue{entity}, false, nil
		}
		return nil, nil, false, errors.Wrapf(logical.ErrUnsupportedConditionOp, "mandatory index rule conf:%s", cond)
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, nil, false, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return BuildQuery(le.Right, schema, entityDict, entity)
		}
		if le.GetRight() == nil {
			return BuildQuery(le.Left, schema, entityDict, entity)
		}
		left, leftEntities, leftIsMatchAllQuery, err := BuildQuery(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, false, err
		}
		right, rightEntities, rightIsMatchAllQuery, err := BuildQuery(le.Right, schema, entityDict, entity)
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
		return nil, nil, false, logical.ErrInvalidCriteriaType
	}
	return nil, nil, false, logical.ErrInvalidCriteriaType
}

// BuildIndexModeQuery returns blugeQuery for index mode.
func BuildIndexModeQuery(measureName string, criteria *modelv1.Criteria, schema logical.Schema) (index.Query, error) {
	var subjectQuery bluge.Query
	var subjectNode node
	if measureName != "" {
		subjectQuery = bluge.NewTermQuery(measureName).SetField(index.IndexModeName)
		subjectNode = newTermNode(measureName, nil)
	}
	if criteria == nil {
		if subjectQuery == nil {
			return nil, nil
		}
		return &queryNode{
			query: subjectQuery,
			node:  subjectNode,
		}, nil
	}
	entityList := schema.EntityList()
	entityDict := make(map[string]int)
	for idx, e := range entityList {
		entityDict[e] = idx
	}
	criteriaQuery, err := buildIndexModeCriteria(criteria, schema, entityDict)
	if err != nil {
		return nil, err
	}
	if subjectQuery == nil {
		return criteriaQuery, nil
	}
	query, node := bluge.NewBooleanQuery(), newMustNode()
	query.AddMust(subjectQuery)
	query.AddMust(criteriaQuery.(*queryNode).query)
	node.Append(subjectNode)
	node.Append(criteriaQuery.(*queryNode).node)
	return &queryNode{query, node}, nil
}

func buildIndexModeCriteria(criteria *modelv1.Criteria, schema logical.Schema, entityDict map[string]int) (index.Query, error) {
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, err := logical.ParseExpr(cond)
		if err != nil {
			return nil, err
		}
		if ok, indexRule := schema.IndexDefined(cond.Name); ok {
			fk := index.FieldKey{IndexRuleID: indexRule.Metadata.Id}
			return parseConditionToQuery(cond, indexRule, expr, fk.Marshal())
		}
		if _, ok := entityDict[cond.Name]; ok {
			fk := index.FieldKey{TagName: index.IndexModeEntityTagPrefix + cond.Name}
			return parseConditionToQuery(cond, nil, expr, fk.Marshal())
		}
		return nil, errors.Wrapf(logical.ErrUnsupportedConditionOp, "mandatory index rule conf:%s", cond)
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		if le.GetLeft() == nil && le.GetRight() == nil {
			return nil, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return buildIndexModeCriteria(le.Right, schema, entityDict)
		}
		if le.GetRight() == nil {
			return buildIndexModeCriteria(le.Left, schema, entityDict)
		}
		left, err := buildIndexModeCriteria(le.Left, schema, entityDict)
		if err != nil {
			return nil, err
		}
		right, err := buildIndexModeCriteria(le.Right, schema, entityDict)
		if err != nil {
			return nil, err
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
			return &queryNode{query, node}, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
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
			return &queryNode{query, node}, nil
		}
		return nil, logical.ErrInvalidCriteriaType
	}
	return nil, logical.ErrInvalidCriteriaType
}

func parseConditionToQuery(cond *modelv1.Condition, indexRule *databasev1.IndexRule,
	expr logical.LiteralExpr, fieldKey string,
) (*queryNode, error) {
	str := expr.String()
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		query := bluge.NewTermRangeInclusiveQuery(convert.BytesToString(bb[0]), maxTerm, false, false).SetField(fieldKey)
		node := newTermRangeInclusiveNode(str, maxInf, false, false, indexRule, false)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_GE:
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		query := bluge.NewTermRangeInclusiveQuery(convert.BytesToString(bb[0]), maxTerm, true, false).SetField(fieldKey)
		node := newTermRangeInclusiveNode(str, maxInf, true, false, indexRule, false)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_LT:
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		query := bluge.NewTermRangeInclusiveQuery(minTerm, convert.BytesToString(bb[0]), false, false).SetField(fieldKey)
		node := newTermRangeInclusiveNode(minInf, str, false, false, indexRule, false)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_LE:
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		query := bluge.NewTermRangeInclusiveQuery(minTerm, convert.BytesToString(bb[0]), false, true).SetField(fieldKey)
		node := newTermRangeInclusiveNode(minInf, str, false, true, indexRule, false)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_EQ:
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		query := bluge.NewTermQuery(convert.BytesToString(bb[0])).SetField(fieldKey)
		node := newTermNode(str, indexRule)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_MATCH:
		if indexRule == nil {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "index rule is mandatory for match operation: %s", cond)
		}
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		analyzer, operator := getMatchOptions(indexRule.Analyzer, cond.MatchOption)
		query := bluge.NewMatchQuery(convert.BytesToString(bb[0])).SetField(fieldKey).SetAnalyzer(analyzer).SetOperator(operator)
		node := newMatchNode(str, indexRule)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_NE:
		bb := expr.Bytes()
		if len(bb) != 1 {
			return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "don't support multiple or null value: %s", cond)
		}
		query, node := bluge.NewBooleanQuery(), newMustNotNode()
		query.AddMustNot(bluge.NewTermQuery(convert.BytesToString(bb[0])).SetField(fieldKey))
		node.SetSubNode(newTermNode(str, indexRule))
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_HAVING:
		bb, elements := expr.Bytes(), expr.Elements()
		query, node := bluge.NewBooleanQuery(), newMustNode()
		for _, b := range bb {
			query.AddMust(bluge.NewTermQuery(string(b)).SetField(fieldKey))
		}
		for _, e := range elements {
			node.Append(newTermNode(e, indexRule))
		}
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		bb, elements := expr.Bytes(), expr.Elements()
		subQuery, subNode := bluge.NewBooleanQuery(), newMustNode()
		for _, b := range bb {
			subQuery.AddMust(bluge.NewTermQuery(string(b)).SetField(fieldKey))
		}
		for _, e := range elements {
			subNode.Append(newTermNode(e, indexRule))
		}
		query, node := bluge.NewBooleanQuery(), newMustNotNode()
		query.AddMustNot(subQuery)
		node.SetSubNode(node)
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_IN:
		bb, elements := expr.Bytes(), expr.Elements()
		query, node := bluge.NewBooleanQuery(), newShouldNode()
		query.SetMinShould(1)
		for _, b := range bb {
			query.AddShould(bluge.NewTermQuery(string(b)).SetField(fieldKey))
		}
		for _, e := range elements {
			node.Append(newTermNode(e, indexRule))
		}
		return &queryNode{query, node}, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
		bb, elements := expr.Bytes(), expr.Elements()
		subQuery, subNode := bluge.NewBooleanQuery(), newShouldNode()
		subQuery.SetMinShould(1)
		for _, b := range bb {
			subQuery.AddShould(bluge.NewTermQuery(string(b)).SetField(fieldKey))
		}
		for _, e := range elements {
			subNode.Append(newTermNode(e, indexRule))
		}
		query, node := bluge.NewBooleanQuery(), newMustNotNode()
		query.AddMustNot(subQuery)
		node.SetSubNode(subNode)
		return &queryNode{query, node}, nil
	}
	return nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "index filter parses %v", cond)
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
	indexRule        *databasev1.IndexRule
	min              string
	max              string
	minInclusive     bool
	maxInclusive     bool
	isTimeRangeQuery bool
}

func newTermRangeInclusiveNode(minVal, maxVal string, minInclusive, maxInclusive bool, indexRule *databasev1.IndexRule, isTimeRangeQuery bool) *termRangeInclusiveNode {
	return &termRangeInclusiveNode{
		indexRule:        indexRule,
		min:              minVal,
		max:              maxVal,
		minInclusive:     minInclusive,
		maxInclusive:     maxInclusive,
		isTimeRangeQuery: isTimeRangeQuery,
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
	if t.isTimeRangeQuery {
		inner["queryType"] = timeRangeQuery
	} else {
		inner["queryType"] = termRangeQuery
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

type timeRangeNode struct {
	timeRange *timestamp.TimeRange
}

func newTimeRangeNode(timeRange *timestamp.TimeRange) *timeRangeNode {
	return &timeRangeNode{
		timeRange: timeRange,
	}
}

func (t *timeRangeNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["time_range"] = t.timeRange.String()
	return json.Marshal(data)
}

func (t *timeRangeNode) String() string {
	return convert.JSONToString(t)
}

// BuildPropertyQuery returns blugeQuery for property query.
func BuildPropertyQuery(req *propertyv1.QueryRequest, groupField, idField string) (index.Query, error) {
	iq, err := BuildIndexModeQuery(req.Name, req.Criteria, schemaInstance)
	if err != nil {
		return nil, err
	}
	bq := bluge.NewBooleanQuery()
	bn := newMustNode()
	if iq != nil {
		iqn := iq.(*queryNode)
		bq.AddMust(iqn.query)
		bn.Append(iqn.node)
	}
	if len(req.Groups) > 1 {
		gq := bluge.NewBooleanQuery()
		gn := newShouldNode()
		for _, g := range req.Groups {
			gq.AddShould(bluge.NewTermQuery(g).SetField(groupField))
			gn.Append(newTermNode(g, nil))
		}
		gq.SetMinShould(1)
		bq.AddMust(gq)
		bn.Append(gn)
	} else {
		bq.AddMust(bluge.NewTermQuery(req.Groups[0]).SetField(groupField))
		bn.Append(newTermNode(req.Groups[0], nil))
	}
	switch len(req.Ids) {
	case 0:
	case 1:
		bq.AddMust(bluge.NewTermQuery(req.Ids[0]).SetField(idField))
		bn.Append(newTermNode(req.Ids[0], nil))
	default:
		iq := bluge.NewBooleanQuery()
		in := newShouldNode()
		for _, id := range req.Ids {
			iq.AddShould(bluge.NewTermQuery(id).SetField(idField))
			in.Append(newTermNode(id, nil))
		}
		iq.SetMinShould(1)
		bq.AddMust(iq)
		bn.Append(in)
	}
	return &queryNode{
		query: bq,
		node:  bn,
	}, nil
}

// BuildPropertyQueryFromEntity builds a property query from entity information.
func BuildPropertyQueryFromEntity(groupField, group, name, entityIDField, entityID string) (index.Query, error) {
	if group == "" || name == "" || entityID == "" {
		return nil, errors.New("group, name and entityID are mandatory for property query")
	}
	bq := bluge.NewBooleanQuery()
	bn := newMustNode()
	bq.AddMust(bluge.NewTermQuery(group).SetField(groupField))
	bn.Append(newTermNode(group, nil))
	bq.AddMust(bluge.NewTermQuery(name).SetField(index.IndexModeName))
	bn.Append(newTermNode(name, nil))
	bq.AddMust(bluge.NewTermQuery(entityID).SetField(entityIDField))
	bn.Append(newTermNode(entityID, nil))
	return &queryNode{
		query: bq,
		node:  bn,
	}, nil
}

var (
	_              logical.Schema = (*schema)(nil)
	schemaInstance                = &schema{}
)

type schema struct{}

func (p *schema) CreateFieldRef(...*logical.Field) ([]*logical.FieldRef, error) {
	panic("unimplemented")
}

func (p *schema) CreateTagRef(...[]*logical.Tag) ([][]*logical.TagRef, error) {
	panic("unimplemented")
}

func (p *schema) EntityList() []string {
	return nil
}

func (p *schema) Equal(logical.Schema) bool {
	panic("unimplemented")
}

func (p *schema) FindTagSpecByName(string) *logical.TagSpec {
	panic("unimplemented")
}

func (p *schema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return true, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Id: uint32(convert.HashStr(tagName)),
		},
	}
}

func (p *schema) IndexRuleDefined(string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (p *schema) ProjFields(...*logical.FieldRef) logical.Schema {
	panic("unimplemented")
}

func (p *schema) ProjTags(...[]*logical.TagRef) logical.Schema {
	panic("unimplemented")
}

func (p *schema) Children() []logical.Schema {
	return nil
}
