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
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// BindParams binds positional parameters to the `?` placeholders in the parsed grammar,
// in their order of appearance in the query text. After a successful call, the grammar is
// indistinguishable from one parsed with literal values, so the transformer needs no
// placeholder awareness. Callers must invoke it before Transform even when params is empty,
// so a query holding unbound placeholders is rejected instead of silently misinterpreted.
// Parameter positions in error messages are 1-based.
//
// Binding mutates the grammar in place, so a grammar binds exactly once: rebinding is
// rejected and a failed bind leaves the grammar partially bound and unusable — parse the
// query again in both cases. Parse-once/bind-many reuse requires an immutable binding
// design and is tracked as a follow-up.
func BindParams(g *Grammar, params []*modelv1.TagValue) error {
	if g.paramsBound {
		return fmt.Errorf("grammar is already bound; parse the query again to bind new parameters")
	}
	b := &binder{}
	b.collect(g)
	if len(b.slots) != len(params) {
		return fmt.Errorf("parameter count mismatch: query contains %d placeholder(s) but %d parameter(s) provided", len(b.slots), len(params))
	}
	for idx, slot := range b.slots {
		if params[idx].GetValue() == nil {
			return fmt.Errorf("parameter #%d has no value", idx+1)
		}
		if bindErr := slot(params[idx]); bindErr != nil {
			return fmt.Errorf("failed to bind parameter #%d: %w", idx+1, bindErr)
		}
	}
	b.expandLists()
	g.paramsBound = true
	return nil
}

// countUnboundParams reports how many `?` placeholders in the grammar are still
// unbound, using the same traversal as binding but without allocating slots.
func countUnboundParams(g *Grammar) int {
	b := &binder{countOnly: true}
	b.collect(g)
	return b.count
}

type listContainer struct {
	assign func([]*GrammarValue)
	values []*GrammarValue
}

type binder struct {
	expansions map[*GrammarValue][]*GrammarValue
	slots      []func(*modelv1.TagValue) error
	lists      []listContainer
	// countOnly makes collect count placeholders instead of building slots.
	countOnly bool
	count     int
}

// collect gathers placeholder slots in their order of appearance in the query
// text: the grammar fixes the clause order and expression trees are visited
// depth-first left-to-right, so the slot order needs no re-sorting.
func (b *binder) collect(g *Grammar) {
	if g.Select != nil {
		if g.Select.Projection != nil && g.Select.Projection.TopN != nil {
			topN := g.Select.Projection.TopN
			b.collectIntSlot(&topN.N, &topN.NParam)
		}
		b.collectTime(g.Select.Time)
		if g.Select.Where != nil {
			b.collectOrExpr(g.Select.Where.Expr)
		}
		if g.Select.Limit != nil {
			b.collectIntSlot(&g.Select.Limit.Value, &g.Select.Limit.Param)
		}
		if g.Select.Offset != nil {
			b.collectIntSlot(&g.Select.Offset.Value, &g.Select.Offset.Param)
		}
	}
	if g.TopN != nil {
		b.collectIntSlot(&g.TopN.N, &g.TopN.NParam)
		b.collectTime(g.TopN.Time)
		if g.TopN.Where != nil {
			b.collectAndExpr(g.TopN.Where.Expr)
		}
	}
}

// validateCountValue rejects count values that would silently wrap when the
// transformer narrows them to int32/uint32. It guards both the bound path and
// the literal path so the two cannot diverge.
func validateCountValue(label string, value int64) error {
	if value < 0 || value > math.MaxInt32 {
		return fmt.Errorf("%s value %d is out of range [0, %d]", label, value, math.MaxInt32)
	}
	return nil
}

// validateGrammarCounts applies the wrap guard to every literal count position
// of a grammar: LIMIT, OFFSET, and the two TOP N counts.
func validateGrammarCounts(g *Grammar) error {
	if g.Select != nil {
		if g.Select.Projection != nil && g.Select.Projection.TopN != nil {
			if err := validateCountValue("TOP", int64(g.Select.Projection.TopN.N)); err != nil {
				return err
			}
		}
		if g.Select.Limit != nil {
			if err := validateCountValue("LIMIT", int64(g.Select.Limit.Value)); err != nil {
				return err
			}
		}
		if g.Select.Offset != nil {
			if err := validateCountValue("OFFSET", int64(g.Select.Offset.Value)); err != nil {
				return err
			}
		}
	}
	if g.TopN != nil {
		if err := validateCountValue("SHOW TOP", int64(g.TopN.N)); err != nil {
			return err
		}
	}
	return nil
}

// collectIntSlot registers a placeholder in an integer-only position
// (LIMIT, OFFSET, or a TOP N count).
func (b *binder) collectIntSlot(value *int, param *bool) {
	if !*param {
		return
	}
	if b.countOnly {
		b.count++
		return
	}
	b.slots = append(b.slots, func(p *modelv1.TagValue) error {
		intVal, ok := p.Value.(*modelv1.TagValue_Int)
		if !ok {
			return fmt.Errorf("this position only accepts int parameters, got %T", p.Value)
		}
		bound := intVal.Int.GetValue()
		if err := validateCountValue("int parameter", bound); err != nil {
			return err
		}
		*value = int(bound)
		*param = false
		return nil
	})
}

func (b *binder) collectTime(clause *GrammarTimeClause) {
	if clause == nil {
		return
	}
	b.collectTimeValue(clause.Value)
	if clause.Between != nil {
		b.collectTimeValue(clause.Between.Begin)
		b.collectTimeValue(clause.Between.End)
	}
}

func (b *binder) collectTimeValue(v *GrammarTimeValue) {
	if v == nil || !v.Param {
		return
	}
	if b.countOnly {
		b.count++
		return
	}
	b.slots = append(b.slots, func(p *modelv1.TagValue) error { return bindTimeValue(v, p) })
}

func (b *binder) collectOrExpr(expr *GrammarOrExpr) {
	if expr == nil {
		return
	}
	b.collectAndExpr(expr.Left)
	for _, right := range expr.Right {
		b.collectAndExpr(right.Right)
	}
}

func (b *binder) collectAndExpr(expr *GrammarAndExpr) {
	if expr == nil {
		return
	}
	b.collectPredicate(expr.Left)
	for _, right := range expr.Right {
		b.collectPredicate(right.Right)
	}
}

func (b *binder) collectPredicate(pred *GrammarPredicate) {
	if pred == nil {
		return
	}
	switch {
	case pred.Paren != nil:
		b.collectOrExpr(pred.Paren)
	case pred.Binary != nil && pred.Binary.Tail != nil:
		if compare := pred.Binary.Tail.Compare; compare != nil {
			b.collectScalarValue(compare.Value)
		}
		if match := pred.Binary.Tail.Match; match != nil && match.Values != nil {
			b.collectSingleOrArray(&match.Values.Single, &match.Values.Array)
		}
	case pred.In != nil:
		inPred := pred.In
		b.collectValueList(inPred.Values, func(vs []*GrammarValue) { inPred.Values = vs })
	case pred.Having != nil && pred.Having.Values != nil:
		b.collectSingleOrArray(&pred.Having.Values.Single, &pred.Having.Values.Array)
	}
}

// collectSingleOrArray collects placeholders from a MATCH/HAVING container and
// splices bound values back preserving its original form: a single-value
// container stays single unless an array expansion produced multiple values,
// while a parenthesized list stays a list regardless of the value count,
// exactly as if written literally.
func (b *binder) collectSingleOrArray(single **GrammarValue, array *[]*GrammarValue) {
	wasSingle := *single != nil
	values := *array
	if wasSingle {
		values = []*GrammarValue{*single}
	}
	b.collectValueList(values, func(vs []*GrammarValue) {
		if wasSingle && len(vs) == 1 {
			*single, *array = vs[0], nil
			return
		}
		*single, *array = nil, vs
	})
}

func (b *binder) collectScalarValue(v *GrammarValue) {
	if v == nil || !v.Param {
		return
	}
	if b.countOnly {
		b.count++
		return
	}
	b.slots = append(b.slots, func(p *modelv1.TagValue) error { return bindScalarValue(v, p) })
}

func (b *binder) collectValueList(values []*GrammarValue, assign func([]*GrammarValue)) {
	hasParam := false
	for _, v := range values {
		if v == nil || !v.Param {
			continue
		}
		hasParam = true
		if b.countOnly {
			b.count++
			continue
		}
		b.slots = append(b.slots, func(p *modelv1.TagValue) error { return b.bindListValue(v, p) })
	}
	if hasParam && !b.countOnly {
		b.lists = append(b.lists, listContainer{values: values, assign: assign})
	}
}

func (b *binder) bindListValue(v *GrammarValue, p *modelv1.TagValue) error {
	switch val := p.Value.(type) {
	case *modelv1.TagValue_Str, *modelv1.TagValue_Int, *modelv1.TagValue_Null:
		return bindScalarValue(v, p)
	case *modelv1.TagValue_StrArray:
		if len(val.StrArray.GetValue()) == 0 {
			return fmt.Errorf("array parameter must not be empty")
		}
		expanded := make([]*GrammarValue, 0, len(val.StrArray.GetValue()))
		for _, s := range val.StrArray.GetValue() {
			expanded = append(expanded, &GrammarValue{String: &s})
		}
		b.recordExpansion(v, expanded)
	case *modelv1.TagValue_IntArray:
		if len(val.IntArray.GetValue()) == 0 {
			return fmt.Errorf("array parameter must not be empty")
		}
		expanded := make([]*GrammarValue, 0, len(val.IntArray.GetValue()))
		for _, i := range val.IntArray.GetValue() {
			expanded = append(expanded, &GrammarValue{Integer: &i})
		}
		b.recordExpansion(v, expanded)
	default:
		return fmt.Errorf("value list only accepts str, int, null, str_array, or int_array parameters, got %T", p.Value)
	}
	return nil
}

func (b *binder) recordExpansion(v *GrammarValue, expanded []*GrammarValue) {
	if b.expansions == nil {
		b.expansions = make(map[*GrammarValue][]*GrammarValue)
	}
	b.expansions[v] = expanded
}

func (b *binder) expandLists() {
	for _, list := range b.lists {
		rebuilt := make([]*GrammarValue, 0, len(list.values))
		for _, v := range list.values {
			if expanded, ok := b.expansions[v]; ok {
				rebuilt = append(rebuilt, expanded...)
				continue
			}
			rebuilt = append(rebuilt, v)
		}
		list.assign(rebuilt)
	}
}

func bindTimeValue(v *GrammarTimeValue, p *modelv1.TagValue) error {
	switch val := p.Value.(type) {
	case *modelv1.TagValue_Str:
		strVal := val.Str.GetValue()
		v.String = &strVal
	case *modelv1.TagValue_Timestamp:
		// A nil inner timestamp would silently decode as the Unix epoch and an
		// out-of-range one would format as a nonsense year; reject both here.
		if err := val.Timestamp.CheckValid(); err != nil {
			return fmt.Errorf("invalid timestamp parameter: %w", err)
		}
		strVal := val.Timestamp.AsTime().Format(time.RFC3339Nano)
		v.String = &strVal
	default:
		// int is rejected as well: the transformer cannot parse a bare integer
		// as a timestamp, so accepting it would only defer a certain failure.
		return fmt.Errorf("time clause only accepts str or timestamp parameters, got %T", p.Value)
	}
	v.Param = false
	return nil
}

func bindScalarValue(v *GrammarValue, p *modelv1.TagValue) error {
	switch val := p.Value.(type) {
	case *modelv1.TagValue_Str:
		strVal := val.Str.GetValue()
		v.String = &strVal
	case *modelv1.TagValue_Int:
		intVal := val.Int.GetValue()
		v.Integer = &intVal
	case *modelv1.TagValue_Null:
		v.Null = true
	default:
		return fmt.Errorf("this position only accepts str, int, or null parameters, got %T", p.Value)
	}
	v.Param = false
	return nil
}
