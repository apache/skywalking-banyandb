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

package stream

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// buildLocalFilter builds a new index.Filter for local indices.
func buildLocalFilter(criteria *modelv1.Criteria, schema logical.Schema, entityDict map[string]int, entity []*modelv1.TagValue,
	startTime int64, endTime int64,
) (index.Filter, [][]*modelv1.TagValue, error) {
	criteriaFilter, entities, err := buildLocalFilterFromCriteria(criteria, schema, entityDict, entity)
	opts := index.NewTimeRangeOpts(startTime, endTime, true, true)
	timeRangeFilter := newTimeRange(inverted.TimestampField, opts)
	filter := newAnd(2)
	filter.append(criteriaFilter).append(timeRangeFilter)
	return filter, entities, err
}

// buildLocalFilterFromCriteria builds a new index.Filter from criteria for local indices.
func buildLocalFilterFromCriteria(criteria *modelv1.Criteria, schema logical.Schema,
	entityDict map[string]int, entity []*modelv1.TagValue,
) (index.Filter, [][]*modelv1.TagValue, error) {
	if criteria == nil {
		return nil, [][]*modelv1.TagValue{entity}, nil
	}
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, parsedEntity, err := logical.ParseExprOrEntity(entityDict, entity, cond)
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
			return nil, nil, errors.WithMessagef(logical.ErrInvalidLogicalExpression, "both sides(left and right) of [%v] are empty", criteria)
		}
		if le.GetLeft() == nil {
			return buildLocalFilterFromCriteria(le.Right, schema, entityDict, entity)
		}
		if le.GetRight() == nil {
			return buildLocalFilterFromCriteria(le.Left, schema, entityDict, entity)
		}
		left, leftEntities, err := buildLocalFilterFromCriteria(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		right, rightEntities, err := buildLocalFilterFromCriteria(le.Right, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		entities := logical.ParseEntities(le.Op, entity, leftEntities, rightEntities)
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
	return nil, nil, logical.ErrInvalidCriteriaType
}

func parseConditionToFilter(cond *modelv1.Condition, indexRule *databasev1.IndexRule,
	expr logical.LiteralExpr, entity []*modelv1.TagValue,
) (index.Filter, [][]*modelv1.TagValue, error) {
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		return newRange(indexRule, expr.RangeOpts(false, false, false)), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_GE:
		return newRange(indexRule, expr.RangeOpts(false, true, false)), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LT:
		return newRange(indexRule, expr.RangeOpts(true, false, false)), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_LE:
		return newRange(indexRule, expr.RangeOpts(true, false, true)), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_EQ:
		return newEq(indexRule, expr), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_MATCH:
		return newMatch(indexRule, expr, cond.MatchOption), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NE:
		return newNot(indexRule, newEq(indexRule, expr)), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_HAVING:
		ee := expr.SubExprs()
		l := len(ee)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		and := newAnd(l)
		for i := range ee {
			and.append(newEq(indexRule, ee[i]))
		}
		return and, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		ee := expr.SubExprs()
		l := len(ee)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		and := newAnd(l)
		for i := range ee {
			and.append(newEq(indexRule, ee[i]))
		}
		return newNot(indexRule, and), [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_IN:
		ee := expr.SubExprs()
		l := len(ee)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		or := newOr(l)
		for i := range ee {
			or.append(newEq(indexRule, ee[i]))
		}
		return or, [][]*modelv1.TagValue{entity}, nil
	case modelv1.Condition_BINARY_OP_NOT_IN:
		ee := expr.SubExprs()
		l := len(ee)
		if l < 1 {
			return ENode, [][]*modelv1.TagValue{entity}, nil
		}
		or := newOr(l)
		for i := range ee {
			or.append(newEq(indexRule, ee[i]))
		}
		return newNot(indexRule, or), [][]*modelv1.TagValue{entity}, nil
	}
	return nil, nil, errors.WithMessagef(logical.ErrUnsupportedConditionOp, "index filter parses %v", cond)
}

type fieldKey struct {
	*databasev1.IndexRule
	TagName string
}

func newFieldKeyWithIndexRule(indexRule *databasev1.IndexRule) fieldKey {
	return fieldKey{indexRule, ""}
}

func newFieldKeyWithTagName(tagName string) fieldKey {
	fk := fieldKey{&databasev1.IndexRule{}, tagName}
	fk.Type = databasev1.IndexRule_TYPE_INVERTED
	return fk
}

func (fk fieldKey) toIndex(seriesID common.SeriesID) index.FieldKey {
	ifk := index.FieldKey{
		SeriesID: seriesID,
	}
	if fk.Metadata != nil {
		ifk.IndexRuleID = fk.Metadata.Id
		ifk.Analyzer = fk.Analyzer
	}
	if fk.TagName != "" {
		ifk.TagName = fk.TagName
	}
	return ifk
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
	return convert.JSONToString(an)
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
	return convert.JSONToString(on)
}

type leaf struct {
	index.Filter
	Expr logical.LiteralExpr
	Key  fieldKey
}

func (l *leaf) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["index"] = l.Key.IndexRule.Metadata.Name + ":" + l.Key.IndexRule.Metadata.Group
	data["expr"] = l.Expr.String()
	return json.Marshal(data)
}

type not struct {
	index.Filter
	Inner index.Filter
	Key   fieldKey
}

func newNot(indexRule *databasev1.IndexRule, inner index.Filter) *not {
	return &not{
		Key:   newFieldKeyWithIndexRule(indexRule),
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
	return convert.JSONToString(n)
}

type eq struct {
	*leaf
}

func newEq(indexRule *databasev1.IndexRule, values logical.LiteralExpr) *eq {
	return &eq{
		leaf: &leaf{
			Key:  newFieldKeyWithIndexRule(indexRule),
			Expr: values,
		},
	}
}

func (eq *eq) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	s, err := searcher(eq.Key.Type)
	if err != nil {
		return nil, err
	}
	return s.MatchTerms(eq.Expr.Field(eq.Key.toIndex(seriesID)))
}

func (eq *eq) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["eq"] = eq.leaf
	return json.Marshal(data)
}

func (eq *eq) String() string {
	return convert.JSONToString(eq)
}

type match struct {
	*leaf
	opts *modelv1.Condition_MatchOption
}

func newMatch(indexRule *databasev1.IndexRule, values logical.LiteralExpr, opts *modelv1.Condition_MatchOption) *match {
	return &match{
		leaf: &leaf{
			Key:  newFieldKeyWithIndexRule(indexRule),
			Expr: values,
		},
		opts: opts,
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
		match.opts,
	)
}

func (match *match) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["match"] = match.leaf
	return json.Marshal(data)
}

func (match *match) String() string {
	return convert.JSONToString(match)
}

type rangeOp struct {
	*leaf
	Opts index.RangeOpts
}

func newRange(indexRule *databasev1.IndexRule, opts index.RangeOpts) *rangeOp {
	return &rangeOp{
		leaf: &leaf{
			Key: newFieldKeyWithIndexRule(indexRule),
		},
		Opts: opts,
	}
}

func newTimeRange(tagName string, opts index.RangeOpts) *rangeOp {
	return &rangeOp{
		leaf: &leaf{
			Key: newFieldKeyWithTagName(tagName),
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
	builder.WriteString(r.Opts.Lower.String())
	builder.WriteString(",")
	builder.WriteString(r.Opts.Upper.String())
	if r.Opts.Upper != nil {
		if r.Opts.IncludesUpper {
			builder.WriteString("]")
		} else {
			builder.WriteString(")")
		}
	}
	if r.Key.IndexRule != nil && r.Key.IndexRule.Metadata != nil && r.Key.Metadata != nil {
		data["key"] = r.Key.IndexRule.Metadata.Name + ":" + r.Key.Metadata.Group
	}
	data["range"] = builder.String()
	return json.Marshal(data)
}

func (r *rangeOp) String() string {
	return convert.JSONToString(r)
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
