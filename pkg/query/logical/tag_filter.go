package logical

import (
	"encoding/json"
	"fmt"
	"strings"

	model_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type tagFilter interface {
	fmt.Stringer
	match(tagFamilies []*model_v1.TagFamily) (bool, error)
}

func buildTagFilter(criteria *model_v1.Criteria, schema Schema) (tagFilter, error) {
	switch criteria.GetExp().(type) {
	case *model_v1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, err := parseExpr(cond.Value)
		if err != nil {
			return nil, err
		}
		if ok, _ := schema.IndexDefined(cond.Name); ok {
			return bypassFilter, nil
		}
		return parseFilter(cond, expr)
	case *model_v1.Criteria_Le:
		le := criteria.GetLe()
		left, err := buildTagFilter(le.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := buildTagFilter(le.Left, schema)
		if err != nil {
			return nil, err
		}
		switch le.Op {
		case model_v1.LogicalExpression_LOGICAL_OP_AND:
			and := newAndLogicalNode(2)
			and.append(left).append(right)
			return and, nil
		case model_v1.LogicalExpression_LOGICAL_OP_OR:
			or := newOrLogicalNode(2)
			or.append(left).append(right)
			return or, nil
		}

	}
	return nil, ErrInvalidConditionType
}

func parseFilter(cond *model_v1.Condition, expr ComparableExpr) (tagFilter, error) {
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
	return nil, ErrInvalidConditionType
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
	}
	return nil, ErrInvalidConditionType
}

var bypassFilter = new(emptyFilter)

type emptyFilter struct{}

func (emptyFilter) match(_ []*model_v1.TagFamily) (bool, error) { return true, nil }

func (emptyFilter) String() string { return "true" }

type logicalNodeOP interface {
	tagFilter
	merge(bool)
}

type logicalNode struct {
	result   *bool
	SubNodes []tagFilter `json:"sub_nodes,omitempty"`
}

func (n *logicalNode) append(sub tagFilter) *logicalNode {
	n.SubNodes = append(n.SubNodes, sub)
	return n
}

func (n *logicalNode) pop() (tagFilter, bool) {
	if len(n.SubNodes) < 1 {
		return nil, false
	}
	sn := n.SubNodes[0]
	n.SubNodes = n.SubNodes[1:]
	return sn, true
}

func matchTag(tagFamilies []*model_v1.TagFamily, n *logicalNode, lp logicalNodeOP) (bool, error) {
	ex, hasNext := n.pop()
	if !hasNext {
		if n.result == nil {
			return true, nil
		}
		return *n.result, nil
	}
	r, err := ex.match(tagFamilies)
	if err != nil {
		return false, err
	}
	if n.result == nil {
		n.result = &r
		return lp.match(tagFamilies)
	}
	lp.merge(r)
	return lp.match(tagFamilies)
}

type andLogicalNode struct {
	*logicalNode
}

func newAndLogicalNode(size int) *andLogicalNode {
	return &andLogicalNode{
		logicalNode: &logicalNode{
			SubNodes: make([]tagFilter, 0, size),
		},
	}
}

func (an *andLogicalNode) merge(b bool) {
	r := *an.result && b
	an.result = &r
}

func (an *andLogicalNode) match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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
			SubNodes: make([]tagFilter, 0, size),
		},
	}
}

func (on *orLogicalNode) merge(b bool) {
	r := *on.result || b
	on.result = &r
}

func (on *orLogicalNode) match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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
	tagFilter
	Name string
	Expr LiteralExpr
}

type notTag struct {
	tagFilter
	Inner tagFilter
}

func newNotTag(inner tagFilter) *notTag {
	return &notTag{
		Inner: inner,
	}
}

func (n *notTag) match(tagFamilies []*model_v1.TagFamily) (bool, error) {
	b, err := n.Inner.match(tagFamilies)
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

func (eq *eqTag) match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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

func (r *rangeTag) match(tagFamilies []*model_v1.TagFamily) (bool, error) {
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

func (h *havingTag) match(tagFamilies []*model_v1.TagFamily) (bool, error) {
	expr, err := tagExpr(tagFamilies, h.Name)
	if err != nil {
		return false, err
	}
	return expr.BelongTo(h.Expr), nil
}

func (h *havingTag) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["having"] = h.tagLeaf
	return json.Marshal(data)
}

func (h *havingTag) String() string {
	return jsonToString(h)
}
