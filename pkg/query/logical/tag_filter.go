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

	model_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var ErrUnsupportedLogicalOperation = errors.New("unsupported logical operation")

type TagFilter interface {
	fmt.Stringer
	Match(tagFamilies []*model_v1.TagFamily) (bool, error)
}

func BuildTagFilter(criteria *model_v1.Criteria, entityDict map[string]int, schema Schema, hasGlobalIndex bool) (TagFilter, error) {
	if criteria == nil {
		return BypassFilter, nil
	}
	switch criteria.GetExp().(type) {
	case *model_v1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, err := parseExpr(cond.Value)
		if err != nil {
			return nil, err
		}
		if ok, _ := schema.IndexDefined(cond.Name); ok {
			return BypassFilter, nil
		}
		if _, ok := entityDict[cond.Name]; ok {
			return BypassFilter, nil
		}
		return parseFilter(cond, expr)
	case *model_v1.Criteria_Le:
		le := criteria.GetLe()
		left, err := BuildTagFilter(le.Left, entityDict, schema, hasGlobalIndex)
		if err != nil {
			return nil, err
		}
		right, err := BuildTagFilter(le.Right, entityDict, schema, hasGlobalIndex)
		if err != nil {
			return nil, err
		}
		if left == BypassFilter && right == BypassFilter {
			return BypassFilter, nil
		}
		switch le.Op {
		case model_v1.LogicalExpression_LOGICAL_OP_AND:
			and := newAndLogicalNode(2)
			and.append(left).append(right)
			return and, nil
		case model_v1.LogicalExpression_LOGICAL_OP_OR:
			if hasGlobalIndex {
				return nil, errors.WithMessage(ErrUnsupportedLogicalOperation, "global index doesn't support OR")
			}
			or := newOrLogicalNode(2)
			or.append(left).append(right)
			return or, nil
		}

	}
	return nil, ErrInvalidCriteriaType
}

func parseFilter(cond *model_v1.Condition, expr ComparableExpr) (TagFilter, error) {
	switch cond.Op {
	case model_v1.Condition_BINARY_OP_GT:
		return newRangeTag(cond.Name, RangeOpts{
			Lower: expr,
		}), nil
	case model_v1.Condition_BINARY_OP_GE:
		return newRangeTag(cond.Name, RangeOpts{
			IncludesLower: true,
			Lower:         expr,
		}), nil
	case model_v1.Condition_BINARY_OP_LT:
		return newRangeTag(cond.Name, RangeOpts{
			Upper: expr,
		}), nil
	case model_v1.Condition_BINARY_OP_LE:
		return newRangeTag(cond.Name, RangeOpts{
			IncludesUpper: true,
			Upper:         expr,
		}), nil
	case model_v1.Condition_BINARY_OP_EQ:
		return newEqTag(cond.Name, expr), nil
	case model_v1.Condition_BINARY_OP_NE:
		return newNotTag(newEqTag(cond.Name, expr)), nil
	case model_v1.Condition_BINARY_OP_HAVING:
		return newHavingTag(cond.Name, expr), nil
	case model_v1.Condition_BINARY_OP_NOT_HAVING:
		return newNotTag(newHavingTag(cond.Name, expr)), nil
	}
	return nil, errors.WithMessagef(ErrUnsupportedConditionOp, "tag filter parses %v", cond)
}

func parseExpr(value *model_v1.TagValue) (ComparableExpr, error) {
	switch v := value.Value.(type) {
	case *model_v1.TagValue_Str:
		return &strLiteral{v.Str.GetValue()}, nil
	case *model_v1.TagValue_Id:
		return &idLiteral{v.Id.GetValue()}, nil
	case *model_v1.TagValue_StrArray:
		return &strArrLiteral{
			arr: v.StrArray.GetValue(),
		}, nil
	case *model_v1.TagValue_Int:
		return &int64Literal{
			int64: v.Int.GetValue(),
		}, nil
	case *model_v1.TagValue_IntArray:
		return &int64ArrLiteral{
			arr: v.IntArray.GetValue(),
		}, nil
	case *model_v1.TagValue_Null:
		return nullLiteralExpr, nil
	}
	return nil, errors.WithMessagef(ErrUnsupportedConditionValue, "tag filter parses %v", value)
}

var BypassFilter = new(emptyFilter)

type emptyFilter struct{}

func (emptyFilter) Match(_ []*model_v1.TagFamily) (bool, error) { return true, nil }

func (emptyFilter) String() string { return "bypass" }

type logicalNodeOP interface {
	TagFilter
	merge(...bool) bool
}

type logicalNode struct {
	SubNodes []TagFilter `json:"sub_nodes,omitempty"`
}

func (n *logicalNode) append(sub TagFilter) *logicalNode {
	if sub == BypassFilter {
		return n
	}
	n.SubNodes = append(n.SubNodes, sub)
	return n
}

func matchTag(tagFamilies []*model_v1.TagFamily, n *logicalNode, lp logicalNodeOP) (bool, error) {
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

func (an *andLogicalNode) Match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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

func (on *orLogicalNode) Match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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
	Name string
	Expr LiteralExpr
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

func (n *notTag) Match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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

func (eq *eqTag) Match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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

type RangeOpts struct {
	Upper         ComparableExpr
	Lower         ComparableExpr
	IncludesUpper bool
	IncludesLower bool
}

type rangeTag struct {
	*tagLeaf
	Opts RangeOpts
}

func newRangeTag(tagName string, opts RangeOpts) *rangeTag {
	return &rangeTag{
		tagLeaf: &tagLeaf{
			Name: tagName,
		},
		Opts: opts,
	}
}

func (r *rangeTag) Match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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

func tagExpr(tagFamilies []*model_v1.TagFamily, tagName string) (ComparableExpr, error) {
	for _, tf := range tagFamilies {
		for _, t := range tf.Tags {
			if t.Key == tagName {
				return parseExpr(t.Value)
			}
		}
	}
	return nil, ErrTagNotDefined
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

func (h *havingTag) Match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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
