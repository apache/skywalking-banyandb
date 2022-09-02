package logical

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	model_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/pkg/errors"
)

var (
	ErrNotRangeOperation = errors.New("this is not an range operation")
	ErrEmptyTree         = errors.New("tree is empty")
)

type globalIndexError struct {
	indexRule *database_v1.IndexRule
	expr      LiteralExpr
}

func (g *globalIndexError) Error() string { return g.indexRule.String() }

func buildLocalFilter(criteria *model_v1.Criteria, schema Schema, entityDict map[string]int, entity tsdb.Entity) (index.Filter, []tsdb.Entity, error) {
	switch criteria.GetExp().(type) {
	case *model_v1.Criteria_Condition:
		cond := criteria.GetCondition()
		expr, entity, err := parseExprOrEntity(entityDict, entity, cond)
		if err != nil {
			return nil, nil, err
		}
		if entity != nil {
			return nil, []tsdb.Entity{entity}, nil
		}
		if ok, indexRule := schema.IndexDefined(cond.Name); ok {
			if indexRule.Location == database_v1.IndexRule_LOCATION_GLOBAL {
				return nil, nil, &globalIndexError{
					indexRule: indexRule,
					expr:      expr,
				}
			}
			return parseCondition(cond, indexRule, expr, entity)
		} else {
			return eNode, []tsdb.Entity{entity}, nil
		}
	case *model_v1.Criteria_Le:
		le := criteria.GetLe()
		left, leftEntities, err := buildLocalFilter(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		right, rightEntities, err := buildLocalFilter(le.Left, schema, entityDict, entity)
		if err != nil {
			return nil, nil, err
		}
		entities := parseEntities(le.Op, entity, leftEntities, rightEntities)
		if entities == nil {
			return nil, nil, nil
		}
		switch le.Op {
		case model_v1.LogicalExpression_LOGICAL_OP_AND:
			and := newAnd(2)
			and.append(left).append(right)
			return and, entities, nil
		case model_v1.LogicalExpression_LOGICAL_OP_OR:
			or := newOr(2)
			or.append(left).append(right)
			return or, entities, nil
		}

	}
	return nil, nil, ErrInvalidConditionType
}

func parseCondition(cond *model_v1.Condition, indexRule *database_v1.IndexRule, expr LiteralExpr, entity tsdb.Entity) (index.Filter, []tsdb.Entity, error) {
	switch cond.Op {
	case model_v1.Condition_BINARY_OP_GT:
		return newRange(indexRule, index.RangeOpts{
			Lower: bytes.Join(expr.Bytes(), nil),
		}), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_GE:
		return newRange(indexRule, index.RangeOpts{
			IncludesLower: true,
			Lower:         bytes.Join(expr.Bytes(), nil),
		}), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_LT:
		return newRange(indexRule, index.RangeOpts{
			Upper: bytes.Join(expr.Bytes(), nil),
		}), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_LE:
		return newRange(indexRule, index.RangeOpts{
			IncludesUpper: true,
			Upper:         bytes.Join(expr.Bytes(), nil),
		}), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_EQ:
		return newEq(indexRule, expr), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_MATCH:
		return newMatch(indexRule, expr), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_NE:
		return newNot(indexRule, newEq(indexRule, expr)), []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_HAVING:
		bb := expr.Bytes()
		and := newAnd(len(bb))
		for _, b := range bb {
			and.append(newEq(indexRule, newBytesLiteral(b)))
		}
		return and, []tsdb.Entity{entity}, nil
	case model_v1.Condition_BINARY_OP_NOT_HAVING:
		bb := expr.Bytes()
		and := newAnd(len(bb))
		for _, b := range bb {
			and.append(newEq(indexRule, newBytesLiteral(b)))
		}
		return newNot(indexRule, and), []tsdb.Entity{entity}, nil
	}
	return nil, nil, ErrInvalidConditionType
}

func parseExprOrEntity(entityDict map[string]int, entity tsdb.Entity, cond *model_v1.Condition) (LiteralExpr, tsdb.Entity, error) {
	entityIdx, ok := entityDict[cond.Name]
	if ok && cond.Op != model_v1.Condition_BINARY_OP_EQ {
		return nil, nil, errors.WithMessagef(ErrInvalidConditionType, "tag belongs to the entity only supports EQ operation in condition(%v)", cond)
	}
	switch v := cond.Value.Value.(type) {
	case *model_v1.TagValue_Str:
		if ok {
			entity[entityIdx] = []byte(v.Str.GetValue())
			return nil, entity, nil
		}
		return Str(v.Str.GetValue()), nil, nil
	case *model_v1.TagValue_Id:
		if ok {
			entity[entityIdx] = []byte(v.Id.GetValue())
			return nil, entity, nil
		}
		return ID(v.Id.GetValue()), nil, nil

	case *model_v1.TagValue_StrArray:
		return &strArrLiteral{
			arr: v.StrArray.GetValue(),
		}, nil, nil
	case *model_v1.TagValue_Int:
		if ok {
			entity[entityIdx] = convert.Int64ToBytes(v.Int.GetValue())
			return nil, entity, nil
		}
		return &int64Literal{
			int64: v.Int.GetValue(),
		}, nil, nil
	case *model_v1.TagValue_IntArray:
		return &int64ArrLiteral{
			arr: v.IntArray.GetValue(),
		}, nil, nil
	}
	return nil, nil, ErrInvalidConditionType
}

func parseEntities(op model_v1.LogicalExpression_LogicalOp, input tsdb.Entity, left, right []tsdb.Entity) []tsdb.Entity {
	count := len(input)
	result := make(tsdb.Entity, count)
	mergedEntities := make([]tsdb.Entity, 0, len(left)+len(right))
	mergedEntities = append(mergedEntities, left...)
	mergedEntities = append(mergedEntities, right...)
	switch op {
	case model_v1.LogicalExpression_LOGICAL_OP_AND:
		for i := 0; i < count; i++ {
			entry := tsdb.AnyEntry
			for j := 0; j < len(mergedEntities); j++ {
				e := mergedEntities[j][i]
				if bytes.Equal(e, tsdb.AnyEntry) {
					entry = e
				} else if !bytes.Equal(entry, e) {
					return nil
				}
			}
			result[i] = entry
		}
	case model_v1.LogicalExpression_LOGICAL_OP_OR:
		return mergedEntities
	}
	return nil
}

type FieldKey struct {
	*database_v1.IndexRule
}

func newFieldKey(indexRule *database_v1.IndexRule) FieldKey {
	return FieldKey{indexRule}
}

func (fk FieldKey) ToIndex(seriesID common.SeriesID) index.FieldKey {
	return index.FieldKey{
		IndexRuleID: fk.Metadata.Id,
		Analyzer:    fk.Analyzer,
		SeriesID:    seriesID,
	}
}

type logicalOP interface {
	index.Filter
	merge(posting.List) error
}

type node struct {
	value    posting.List
	SubNodes []index.Filter `json:"sub_nodes,omitempty"`
}

func (n *node) append(sub index.Filter) *node {
	n.SubNodes = append(n.SubNodes, sub)
	return n
}

func (n *node) pop() (index.Filter, bool) {
	if len(n.SubNodes) < 1 {
		return nil, false
	}
	sn := n.SubNodes[0]
	n.SubNodes = n.SubNodes[1:]
	return sn, true
}

func execute(searcher index.GetSearcher, seriesID common.SeriesID, n *node, lp logicalOP) (posting.List, error) {
	ex, hasNext := n.pop()
	if !hasNext {
		if n.value == nil {
			return nil, ErrEmptyTree
		}
		return n.value, nil
	}
	r, err := ex.Execute(searcher, seriesID)
	if err != nil {
		return nil, err
	}
	err = lp.merge(r)
	if err != nil {
		return nil, err
	}
	return lp.Execute(searcher, seriesID)
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

func (an *andNode) merge(list posting.List) error {
	if _, ok := list.(bypassList); ok {
		return nil
	}
	if an.value == nil {
		an.value = list
		return nil
	}
	return an.value.Intersect(list)
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

func (on *orNode) merge(list posting.List) error {
	// If a predicator is not indexed, all predicator are ignored.
	// The tagFilter will take up this job to filter this items.
	if _, ok := on.value.(bypassList); ok {
		return nil
	}
	if _, ok := list.(bypassList); ok {
		on.value = list
		return nil
	}
	if on.value == nil {
		on.value = list
	}
	return on.value.Union(list)
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
	Key  FieldKey
	Expr LiteralExpr
}

type not struct {
	index.Filter
	Key   FieldKey
	Inner index.Filter
}

func newNot(indexRule *database_v1.IndexRule, inner index.Filter) *not {
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
	all, err := s.MatchField(n.Key.ToIndex(seriesID))
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

func newEq(indexRule *database_v1.IndexRule, values LiteralExpr) *eq {
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
		Key:  eq.Key.ToIndex(seriesID),
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

func newMatch(indexRule *database_v1.IndexRule, values LiteralExpr) *match {
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
		match.Key.ToIndex(seriesID),
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

func newRange(indexRule *database_v1.IndexRule, opts index.RangeOpts) *rangeOp {
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
	return s.Range(r.Key.ToIndex(seriesID), r.Opts)
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
	data["key"] = r.Key
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
	eNode = new(emptyNode)
	bList = new(bypassList)
)

type emptyNode struct{}

func (an emptyNode) Execute(searcher index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	return bList, nil
}

func (an emptyNode) String() string {
	return "empty"
}

type bypassList struct{}

func (bl bypassList) Contains(id common.ItemID) bool {
	// all items should be fetched
	return true
}

func (bl bypassList) IsEmpty() bool {
	return false
}

func (bl bypassList) Max() (common.ItemID, error) {
	panic("not invoked")
}

func (bl bypassList) Len() int {
	panic("not invoked")
}

func (bl bypassList) Iterator() posting.Iterator {
	panic("not invoked")
}

func (bl bypassList) Clone() posting.List {
	panic("not invoked")
}

func (bl bypassList) Equal(other posting.List) bool {
	panic("not invoked")
}

func (bl bypassList) Insert(i common.ItemID) {
	panic("not invoked")
}

func (bl bypassList) Intersect(other posting.List) error {
	panic("not invoked")
}

func (bl bypassList) Difference(other posting.List) error {
	panic("not invoked")
}

func (bl bypassList) Union(other posting.List) error {
	panic("not invoked")
}

func (bl bypassList) UnionMany(others []posting.List) error {
	panic("not invoked")
}

func (bl bypassList) AddIterator(iter posting.Iterator) error {
	panic("not invoked")
}

func (bl bypassList) AddRange(min common.ItemID, max common.ItemID) error {
	panic("not invoked")
}

func (bl bypassList) RemoveRange(min common.ItemID, max common.ItemID) error {
	panic("not invoked")
}

func (bl bypassList) Reset() {
	panic("not invoked")
}

func (bl bypassList) ToSlice() []common.ItemID {
	panic("not invoked")
}

func (bl bypassList) Marshall() ([]byte, error) {
	panic("not invoked")
}

func (bl bypassList) Unmarshall(data []byte) error {
	panic("not invoked")
}

func (bl bypassList) SizeInBytes() int64 {
	panic("not invoked")
}
