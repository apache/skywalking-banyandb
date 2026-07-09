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

package bydbql

import (
	"fmt"
	"math"
	"reflect"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// placeholderKind classifies a `?` placeholder by the position it occupies,
// which determines the parameter types Bind accepts and how Transform reads it.
type placeholderKind uint8

const (
	// phScalar is a WHERE/HAVING comparison value: str, int, or null.
	phScalar placeholderKind = iota
	// phList is an element of an IN/MATCH/HAVING value list: scalar or an array
	// that expands in place.
	phList
	// phTime is a TIME value: str or timestamp.
	phTime
	// phCount is a LIMIT/OFFSET/TOP N count: an int within maxCount.
	phCount
)

// placeholderSpec describes one placeholder, recorded once by Prepare and read
// per request by Bind (to validate the parameter) and Transform (to read it).
type placeholderSpec struct {
	maxCount int64 // upper bound for phCount positions
	kind     placeholderKind
}

// PreparedStatement is an immutable, reusable parsed query. It is safe to cache
// and to Bind concurrently: Prepare numbers the placeholders once, and Bind
// never mutates the template — the bound values live in a per-request overlay.
type PreparedStatement struct {
	template *Grammar
	specs    []placeholderSpec
}

// Prepare parses a query and numbers its `?` placeholders so it can be bound
// repeatedly with different parameters without re-parsing.
func Prepare(query string) (*PreparedStatement, error) {
	g, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}
	p := &preparer{}
	p.walkGrammar(g)
	return &PreparedStatement{template: g, specs: p.specs}, nil
}

// NumPlaceholders reports how many `?` placeholders the statement carries. A zero
// count means a literal query with no reusable parameters.
func (ps *PreparedStatement) NumPlaceholders() int {
	return len(ps.specs)
}

// EstimatedSize returns an approximate in-memory byte footprint of the prepared
// statement — its parsed grammar template and placeholder specs. It is a
// heuristic for cache byte-accounting (a reflection walk of the object graph),
// not an exact measurement, and is meant to be called off the hot path.
func (ps *PreparedStatement) EstimatedSize() int {
	return deepSize(reflect.ValueOf(ps))
}

// deepSize sums the bytes reachable from v: the inline size of each pointed-to
// value plus the out-of-line bytes of strings and slice backing arrays. Inline
// struct/array fields are already covered by their container's size, so only
// their referenced memory is added. It is shaped for the grammar, an acyclic tree
// of concrete structs, pointers, slices, strings, and scalars.
func deepSize(v reflect.Value) int {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return 0
		}
		elem := v.Elem()
		return int(elem.Type().Size()) + deepSize(elem)
	case reflect.Struct:
		total := 0
		for i := 0; i < v.NumField(); i++ {
			total += deepSize(v.Field(i))
		}
		return total
	case reflect.Slice:
		if v.IsNil() {
			return 0
		}
		total := v.Cap() * int(v.Type().Elem().Size())
		for i := 0; i < v.Len(); i++ {
			total += deepSize(v.Index(i))
		}
		return total
	case reflect.String:
		return v.Len()
	default:
		return 0
	}
}

// resolvedParam holds one placeholder's bound value in the per-request overlay.
// Which field is populated follows the placeholder's kind: a scalar/list
// position fills values (one node, or several for an expanded array), a TIME
// position fills timeStr, and a count position fills count.
type resolvedParam struct {
	timeStr string
	values  []*GrammarValue
	count   int
}

// BoundQuery pairs an immutable prepared statement with the values bound for one
// request. Its values are the per-request overlay indexed by placeholder
// position, allocated by Bind and never shared or mutated after. Pass it to
// Transformer.TransformBound.
type BoundQuery struct {
	stmt   *PreparedStatement
	values []resolvedParam
}

// Bind resolves params against the statement's placeholders and returns a
// per-request BoundQuery, leaving the template untouched, so a statement may be
// bound repeatedly and concurrently. Parameter positions in errors are 1-based,
// matching BindParams.
func (ps *PreparedStatement) Bind(params []*modelv1.TagValue) (*BoundQuery, error) {
	if len(params) != len(ps.specs) {
		return nil, fmt.Errorf("parameter count mismatch: query contains %d placeholder(s) but %d parameter(s) provided", len(ps.specs), len(params))
	}
	values := make([]resolvedParam, len(ps.specs))
	for i, spec := range ps.specs {
		p := params[i]
		if p == nil || p.GetValue() == nil {
			return nil, fmt.Errorf("parameter #%d has no value", i+1)
		}
		var err error
		switch spec.kind {
		case phScalar:
			var v *GrammarValue
			v, err = resolveScalarParam(p)
			if err == nil {
				values[i].values = []*GrammarValue{v}
			}
		case phList:
			values[i].values, err = resolveListParam(p)
		case phTime:
			values[i].timeStr, err = resolveTimeParam(p)
		case phCount:
			values[i].count, err = resolveCountParam(p, spec.maxCount)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to bind parameter #%d: %w", i+1, err)
		}
	}
	return &BoundQuery{stmt: ps, values: values}, nil
}

// preparer walks a parsed grammar in textual order, assigning each placeholder
// its positional index and recording its spec. The traversal mirrors
// binder.collect; the reflection tripwire test asserts both cover every
// placeholder-bearing grammar field.
type preparer struct {
	specs []placeholderSpec
}

func (p *preparer) add(kind placeholderKind, maxCount int64) int {
	idx := len(p.specs)
	p.specs = append(p.specs, placeholderSpec{kind: kind, maxCount: maxCount})
	return idx
}

func (p *preparer) walkGrammar(g *Grammar) {
	if g.Select != nil {
		if g.Select.Projection != nil && g.Select.Projection.TopN != nil {
			if topN := g.Select.Projection.TopN; topN.NParam {
				topN.NParamIndex = p.add(phCount, math.MaxInt32)
			}
		}
		p.walkTime(g.Select.Time)
		if g.Select.Where != nil {
			p.walkOrExpr(g.Select.Where.Expr)
		}
		if g.Select.Limit != nil && g.Select.Limit.Param {
			g.Select.Limit.ParamIndex = p.add(phCount, math.MaxUint32)
		}
		if g.Select.Offset != nil && g.Select.Offset.Param {
			g.Select.Offset.ParamIndex = p.add(phCount, math.MaxUint32)
		}
	}
	if g.TopN != nil {
		if g.TopN.NParam {
			g.TopN.NParamIndex = p.add(phCount, math.MaxInt32)
		}
		p.walkTime(g.TopN.Time)
		if g.TopN.Where != nil {
			p.walkAndExpr(g.TopN.Where.Expr)
		}
	}
}

func (p *preparer) walkTime(clause *GrammarTimeClause) {
	if clause == nil {
		return
	}
	p.walkTimeValue(clause.Value)
	if clause.Between != nil {
		p.walkTimeValue(clause.Between.Begin)
		p.walkTimeValue(clause.Between.End)
	}
}

func (p *preparer) walkTimeValue(v *GrammarTimeValue) {
	if v != nil && v.Param {
		v.ParamIndex = p.add(phTime, 0)
	}
}

func (p *preparer) walkOrExpr(expr *GrammarOrExpr) {
	if expr == nil {
		return
	}
	p.walkAndExpr(expr.Left)
	for _, right := range expr.Right {
		p.walkAndExpr(right.Right)
	}
}

func (p *preparer) walkAndExpr(expr *GrammarAndExpr) {
	if expr == nil {
		return
	}
	p.walkPredicate(expr.Left)
	for _, right := range expr.Right {
		p.walkPredicate(right.Right)
	}
}

func (p *preparer) walkPredicate(pred *GrammarPredicate) {
	if pred == nil {
		return
	}
	switch {
	case pred.Paren != nil:
		p.walkOrExpr(pred.Paren)
	case pred.Binary != nil && pred.Binary.Tail != nil:
		if compare := pred.Binary.Tail.Compare; compare != nil && compare.Value != nil && compare.Value.Param {
			compare.Value.ParamIndex = p.add(phScalar, 0)
		}
		if match := pred.Binary.Tail.Match; match != nil && match.Values != nil {
			p.walkValueList(match.Values.Single, match.Values.Array)
		}
	case pred.In != nil:
		p.walkValueList(nil, pred.In.Values)
	case pred.Having != nil && pred.Having.Values != nil:
		p.walkValueList(pred.Having.Values.Single, pred.Having.Values.Array)
	}
}

// walkValueList numbers the placeholders in a MATCH/HAVING single-or-array or an
// IN list. A container is parsed as either a single value or an array; both are
// walked the same way here.
func (p *preparer) walkValueList(single *GrammarValue, array []*GrammarValue) {
	values := array
	if single != nil {
		values = []*GrammarValue{single}
	}
	for _, v := range values {
		if v != nil && v.Param {
			v.ParamIndex = p.add(phList, 0)
		}
	}
}
