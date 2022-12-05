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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var errUnsupportedLogicalOperation = errors.New("unsupported logical operation")

// TagFilter allows matching a tag based on a predicate.
type TagFilter interface {
	fmt.Stringer
	Match(tagFamilies []*modelv1.TagFamily) (bool, error)
}

// BuildTagFilter returns a TagFilter if predicates doesn't match any indices.
func BuildTagFilter(criteria *modelv1.Criteria, entityDict map[string]int, schema Schema, hasGlobalIndex bool) (TagFilter, error) {
	if criteria == nil {
		return DummyFilter, nil
	}
	switch criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, err := parseExpr(cond.Value)
		if err != nil {
			return nil, err
		}
		if ok, _ := schema.IndexDefined(cond.Name); ok {
			return DummyFilter, nil
		}
		if _, ok := entityDict[cond.Name]; ok {
			return DummyFilter, nil
		}
		return parseFilter(cond, expr)
	case *modelv1.Criteria_Le:
		le := criteria.GetLe()
		left, err := BuildTagFilter(le.Left, entityDict, schema, hasGlobalIndex)
		if err != nil {
			return nil, err
		}
		right, err := BuildTagFilter(le.Right, entityDict, schema, hasGlobalIndex)
		if err != nil {
			return nil, err
		}
		if left == DummyFilter && right == DummyFilter {
			return DummyFilter, nil
		}
		switch le.Op {
		case modelv1.LogicalExpression_LOGICAL_OP_AND:
			and := newAndLogicalNode(2)
			and.append(left).append(right)
			return and, nil
		case modelv1.LogicalExpression_LOGICAL_OP_OR:
			if hasGlobalIndex {
				return nil, errors.WithMessage(errUnsupportedLogicalOperation, "global index doesn't support OR")
			}
			or := newOrLogicalNode(2)
			or.append(left).append(right)
			return or, nil
		}
	}
	return nil, errInvalidCriteriaType
}

func parseFilter(cond *modelv1.Condition, expr ComparableExpr) (TagFilter, error) {
	switch cond.Op {
	case modelv1.Condition_BINARY_OP_GT:
		return newRangeTag(cond.Name, rangeOpts{
			Lower: expr,
		}), nil
	case modelv1.Condition_BINARY_OP_GE:
		return newRangeTag(cond.Name, rangeOpts{
			IncludesLower: true,
			Lower:         expr,
		}), nil
	case modelv1.Condition_BINARY_OP_LT:
		return newRangeTag(cond.Name, rangeOpts{
			Upper: expr,
		}), nil
	case modelv1.Condition_BINARY_OP_LE:
		return newRangeTag(cond.Name, rangeOpts{
			IncludesUpper: true,
			Upper:         expr,
		}), nil
	case modelv1.Condition_BINARY_OP_EQ:
		return newEqTag(cond.Name, expr), nil
	case modelv1.Condition_BINARY_OP_NE:
		return newNotTag(newEqTag(cond.Name, expr)), nil
	case modelv1.Condition_BINARY_OP_HAVING:
		return newHavingTag(cond.Name, expr), nil
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return newNotTag(newHavingTag(cond.Name, expr)), nil
	case modelv1.Condition_BINARY_OP_IN:
		panic("unimplemented")
	case modelv1.Condition_BINARY_OP_NOT_IN:
		panic("unimplemented")
	default:
		return nil, errors.WithMessagef(errUnsupportedConditionOp, "tag filter parses %v", cond)
	}
}

func parseExpr(value *modelv1.TagValue) (ComparableExpr, error) {
	switch v := value.Value.(type) {
	case *modelv1.TagValue_Str:
		return &strLiteral{v.Str.GetValue()}, nil
	case *modelv1.TagValue_Id:
		return &idLiteral{v.Id.GetValue()}, nil
	case *modelv1.TagValue_StrArray:
		return &strArrLiteral{
			arr: v.StrArray.GetValue(),
		}, nil
	case *modelv1.TagValue_Int:
		return &int64Literal{
			int64: v.Int.GetValue(),
		}, nil
	case *modelv1.TagValue_IntArray:
		return &int64ArrLiteral{
			arr: v.IntArray.GetValue(),
		}, nil
	case *modelv1.TagValue_Null:
		return nullLiteralExpr, nil
	}
	return nil, errors.WithMessagef(errUnsupportedConditionValue, "tag filter parses %v", value)
}

// DummyFilter matches any predicate.
var DummyFilter = new(dummyTagFilter)

type dummyTagFilter struct{}

func (dummyTagFilter) Match(_ []*modelv1.TagFamily) (bool, error) { return true, nil }

func (dummyTagFilter) String() string { return "dummy" }

type logicalNodeOP interface {
	TagFilter
	merge(...bool) bool
}

type logicalNode struct {
	SubNodes []TagFilter `json:"sub_nodes,omitempty"`
}

func (n *logicalNode) append(sub TagFilter) *logicalNode {
	if sub == DummyFilter {
		return n
	}
	n.SubNodes = append(n.SubNodes, sub)
	return n
}

func matchTag(tagFamilies []*modelv1.TagFamily, n *logicalNode, lp logicalNodeOP) (bool, error) {
	var result *bool
	for _, sn := range n.SubNodes {
		r, err := sn.Match(tagFamilies)
		if err != nil {
			return false, err
		}
		if result == nil {
			result = &r
			continue
		}
		mr := lp.merge(*result, r)
		result = &mr
	}
	return *result, nil
}

type andLogicalNode struct {
	*logicalNode
}

func newAndLogicalNode(size int) *andLogicalNode {
	return &andLogicalNode{
		logicalNode: &logicalNode{
			SubNodes: make([]TagFilter, 0, size),
		},
	}
}

func (an *andLogicalNode) merge(bb ...bool) bool {
	for _, b := range bb {
		if !b {
			return false
		}
	}
	return true
}

func (an *andLogicalNode) Match(tagFamilies []*modelv1.TagFamily) (bool, error) {
	return matchTag(tagFamilies, an.logicalNode, an)
}

func (an *andLogicalNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["and"] = an.logicalNode.SubNodes
	return json.Marshal(data)
}

func (an *andLogicalNode) String() string {
	return jsonToString(an)
}

type orLogicalNode struct {
	*logicalNode
}

func newOrLogicalNode(size int) *orLogicalNode {
	return &orLogicalNode{
		logicalNode: &logicalNode{
			SubNodes: make([]TagFilter, 0, size),
		},
	}
}

func (on *orLogicalNode) merge(bb ...bool) bool {
	for _, b := range bb {
		if b {
			return true
		}
	}
	return false
}

func (on *orLogicalNode) Match(tagFamilies []*modelv1.TagFamily) (bool, error) {
	return matchTag(tagFamilies, on.logicalNode, on)
}

func (on *orLogicalNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["or"] = on.logicalNode.SubNodes
	return json.Marshal(data)
}

func (on *orLogicalNode) String() string {
	return jsonToString(on)
}

type tagLeaf struct {
	TagFilter
	Expr LiteralExpr
	Name string
}

func (l *tagLeaf) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["name"] = l.Name
	data["expr"] = l.Expr.String()
	return json.Marshal(data)
}

type notTag struct {
	TagFilter
	Inner TagFilter
}

func newNotTag(inner TagFilter) *notTag {
	return &notTag{
		Inner: inner,
	}
}

func (n *notTag) Match(tagFamilies []*modelv1.TagFamily) (bool, error) {
	b, err := n.Inner.Match(tagFamilies)
	if err != nil {
		return false, err
	}
	return !b, nil
}

func (n *notTag) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["not"] = n.Inner
	return json.Marshal(data)
}

func (n *notTag) String() string {
	return jsonToString(n)
}

type eqTag struct {
	*tagLeaf
}

func newEqTag(tagName string, values LiteralExpr) *eqTag {
	return &eqTag{
		tagLeaf: &tagLeaf{
			Name: tagName,
			Expr: values,
		},
	}
}

func (eq *eqTag) Match(tagFamilies []*modelv1.TagFamily) (bool, error) {
	expr, err := tagExpr(tagFamilies, eq.Name)
	if err != nil {
		return false, err
	}
	return eq.Expr.Equal(expr), nil
}

func (eq *eqTag) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["eq"] = eq.tagLeaf
	return json.Marshal(data)
}

func (eq *eqTag) String() string {
	return jsonToString(eq)
}

type rangeOpts struct {
	Upper         ComparableExpr
	Lower         ComparableExpr
	IncludesUpper bool
	IncludesLower bool
}

type rangeTag struct {
	*tagLeaf
	Opts rangeOpts
}

func newRangeTag(tagName string, opts rangeOpts) *rangeTag {
	return &rangeTag{
		tagLeaf: &tagLeaf{
			Name: tagName,
		},
		Opts: opts,
	}
}

func (r *rangeTag) Match(tagFamilies []*modelv1.TagFamily) (bool, error) {
	expr, err := tagExpr(tagFamilies, r.Name)
	if err != nil {
		return false, err
	}
	if r.Opts.Lower != nil {
		lower := r.Opts.Lower
		c, b := lower.Compare(expr)
		if !b {
			return false, nil
		}
		if r.Opts.IncludesLower {
			if c > 0 {
				return false, nil
			}
		} else {
			if c >= 0 {
				return false, nil
			}
		}
	}
	if r.Opts.Upper != nil {
		upper := r.Opts.Upper
		c, b := upper.Compare(expr)
		if !b {
			return false, nil
		}
		if r.Opts.IncludesUpper {
			if c < 0 {
				return false, nil
			}
		} else {
			if c <= 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func (r *rangeTag) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	var builder strings.Builder
	if r.Opts.Lower != nil {
		if r.Opts.IncludesLower {
			builder.WriteString("[")
		} else {
			builder.WriteString("(")
		}
		builder.WriteString(r.Opts.Lower.String())
	}
	if r.Opts.Upper != nil {
		builder.WriteString(",")
		builder.WriteString(r.Opts.Upper.String())
		if r.Opts.IncludesUpper {
			builder.WriteString("]")
		} else {
			builder.WriteString(")")
		}
	}
	data["key"] = r.tagLeaf
	data["range"] = builder.String()
	return json.Marshal(data)
}

func (r *rangeTag) String() string {
	return jsonToString(r)
}

func tagExpr(tagFamilies []*modelv1.TagFamily, tagName string) (ComparableExpr, error) {
	for _, tf := range tagFamilies {
		for _, t := range tf.Tags {
			if t.Key == tagName {
				return parseExpr(t.Value)
			}
		}
	}
	return nil, errTagNotDefined
}

type havingTag struct {
	*tagLeaf
}

func newHavingTag(tagName string, values LiteralExpr) *havingTag {
	return &havingTag{
		tagLeaf: &tagLeaf{
			Name: tagName,
			Expr: values,
		},
	}
}

func (h *havingTag) Match(tagFamilies []*modelv1.TagFamily) (bool, error) {
	expr, err := tagExpr(tagFamilies, h.Name)
	if err != nil {
		return false, err
	}
	return expr.Contains(h.Expr), nil
}

func (h *havingTag) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["having"] = h.tagLeaf
	return json.Marshal(data)
}

func (h *havingTag) String() string {
	return jsonToString(h)
}
