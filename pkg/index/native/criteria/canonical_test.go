// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package criteria

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// The canonical suite is the deterministic half of the criteria tests: it enumerates the classical cases instead of sampling
// them, and it knows what "all of them" means, so that a case that goes missing fails the suite.
//
// The cases are the cells of a coverage matrix:
//
//   - a condition: every operator on every kind of field (a field that holds one term, one that holds several or none, an
//     analyzed text, a tag that is not indexed), against values below, at, between and above the data;
//   - a shape: every way up to three conditions can be joined by AND and OR (11 shapes, left-deep and right-deep included);
//   - a result: the criteria selects no document, some, or all of them, for each shape;
//   - a fold: a conjunction holds two ranges of the single-term field (they fold), two ranges of the multi-term field (they
//     must not), or neither.
//
// The documents are chosen to put each classical document in the data: with and without each field, with one, several and
// every term, at the minimum, the middle and the maximum of the values, spread over three segments.
//
// The random test (differential_test.go) explores around this suite, and TestRandomCriteriaReachEveryCanonicalCell keeps it
// honest: the random generator must be able to reach every cell the suite lists. What the random test finds goes into
// canonicalRegressions, below, so that it stays covered.

// canonicalDocs builds the documents: s is absent or one of a, d, h; m is absent or holds {a}, {a,h}, {d}, {b,g} or {c,d,e};
// t is one of two texts. Every combination is one document, 48 in all, dealt over three segments.
func canonicalDocs(t *testing.T) (batches [][]native.Document, terms map[string]map[string][]string, total int) {
	t.Helper()
	terms = make(map[string]map[string][]string)
	batches = make([][]native.Document, 3)
	singles := []string{"", "a", "d", "h"}
	multis := [][]string{nil, {"a"}, {"a", "h"}, {"d"}, {"b", "g"}, {"c", "d", "e"}}
	texts := []string{"red fox", "blue dog"}
	for _, single := range singles {
		for _, multi := range multis {
			for _, text := range texts {
				id := fmt.Sprintf("doc-%03d", total)
				terms[id] = make(map[string][]string)
				fields := []native.Field{{Name: "kind", Value: []byte("doc"), Index: true}}
				if single != "" {
					terms[id]["s"] = []string{single}
					fields = append(fields, native.Field{Name: "s", Value: []byte(single), Index: true})
				}
				if len(multi) > 0 {
					terms[id]["m"] = multi
					nativeTerms := make([]native.Term, len(multi))
					for i, value := range multi {
						nativeTerms[i] = native.Term{Value: []byte(value)}
					}
					fields = append(fields, native.Field{Name: "m", Terms: nativeTerms, Index: true})
				}
				var textTerms []native.Term
				for _, word := range strings.Fields(text) {
					textTerms = append(textTerms, native.Term{Value: []byte(word)})
					terms[id]["t"] = append(terms[id]["t"], word)
				}
				fields = append(fields, native.Field{Name: "t", Terms: textTerms, Index: true})
				batches[total%len(batches)] = append(batches[total%len(batches)], native.Document{Identifier: []byte(id), Fields: fields})
				total++
			}
		}
	}
	return batches, terms, total
}

func newCanonicalView(t *testing.T) (view *native.ReadView, terms map[string]map[string][]string, total int) {
	t.Helper()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	batches, terms, total := canonicalDocs(t)
	for _, batch := range batches {
		require.NoError(t, owner.Batch(context.Background(), native.Batch{Documents: batch}))
	}
	view, err = owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, view.Close())
		require.NoError(t, owner.Close())
	})
	return view, terms, total
}

// canonicalLeaf is one condition of the suite.
type canonicalLeaf struct {
	c    *modelv1.Criteria
	name string
}

// canonicalLeaves returns every condition of the suite and the smaller set of representatives the joined shapes use. The
// values sit below the data ("A", uppercase sorts first), at its minimum ("a"), between ("d"), at its maximum ("h") and above
// it ("z"); the arrays hold the two ends, the middle and a value that is not there.
func canonicalLeaves() (all, representatives []canonicalLeaf) {
	scalars := []string{"A", "a", "d", "h", "z"}
	arrays := [][]string{{"a", "h"}, {"d"}, {"z"}}
	scalarOps := []modelv1.Condition_BinaryOp{
		modelv1.Condition_BINARY_OP_EQ, modelv1.Condition_BINARY_OP_NE, modelv1.Condition_BINARY_OP_GT,
		modelv1.Condition_BINARY_OP_GE, modelv1.Condition_BINARY_OP_LT, modelv1.Condition_BINARY_OP_LE,
	}
	arrayOps := []modelv1.Condition_BinaryOp{
		modelv1.Condition_BINARY_OP_IN, modelv1.Condition_BINARY_OP_NOT_IN,
		modelv1.Condition_BINARY_OP_HAVING, modelv1.Condition_BINARY_OP_NOT_HAVING,
	}
	label := func(field string, op modelv1.Condition_BinaryOp, arg string) string {
		return fmt.Sprintf("%s %s %s", field, strings.TrimPrefix(op.String(), "BINARY_OP_"), arg)
	}
	for _, field := range []string{"s", "m"} {
		for _, op := range scalarOps {
			for _, value := range scalars {
				all = append(all, canonicalLeaf{name: label(field, op, value), c: condition(field, op, strValue(value))})
			}
		}
		for _, op := range arrayOps {
			for _, values := range arrays {
				all = append(all, canonicalLeaf{name: label(field, op, "["+strings.Join(values, ",")+"]"), c: condition(field, op, strArrayValue(values...))})
			}
		}
		for _, rep := range []struct {
			arg any
			op  modelv1.Condition_BinaryOp
		}{
			{"d", modelv1.Condition_BINARY_OP_EQ},
			{"d", modelv1.Condition_BINARY_OP_NE},
			{[]string{"a", "h"}, modelv1.Condition_BINARY_OP_IN},
			{[]string{"a", "h"}, modelv1.Condition_BINARY_OP_NOT_IN},
			{[]string{"a", "h"}, modelv1.Condition_BINARY_OP_HAVING},
			{[]string{"a", "h"}, modelv1.Condition_BINARY_OP_NOT_HAVING},
			{"d", modelv1.Condition_BINARY_OP_GT},
			{"d", modelv1.Condition_BINARY_OP_GE},
			{"d", modelv1.Condition_BINARY_OP_LT},
			{"d", modelv1.Condition_BINARY_OP_LE},
			{"z", modelv1.Condition_BINARY_OP_EQ},
			{"z", modelv1.Condition_BINARY_OP_NE},
		} {
			if text, scalar := rep.arg.(string); scalar {
				representatives = append(representatives, canonicalLeaf{name: label(field, rep.op, text), c: condition(field, rep.op, strValue(text))})
				continue
			}
			values := rep.arg.([]string)
			representatives = append(representatives, canonicalLeaf{
				name: label(field, rep.op, "["+strings.Join(values, ",")+"]"), c: condition(field, rep.op, strArrayValue(values...)),
			})
		}
	}
	for _, operator := range []modelv1.Condition_MatchOption_Operator{
		modelv1.Condition_MatchOption_OPERATOR_OR, modelv1.Condition_MatchOption_OPERATOR_AND,
	} {
		for _, text := range []string{"red", "red fox", "blue dog", "fox dog"} {
			leaf := canonicalLeaf{
				name: fmt.Sprintf("t MATCH %s %q", strings.TrimPrefix(operator.String(), "OPERATOR_"), text),
				c:    buildMatchCondition("t", &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: operator}, strValue(text)),
			}
			all = append(all, leaf)
			if text == "red fox" {
				representatives = append(representatives, leaf)
			}
		}
	}
	for _, op := range []modelv1.Condition_BinaryOp{modelv1.Condition_BINARY_OP_EQ, modelv1.Condition_BINARY_OP_NE} {
		leaf := canonicalLeaf{name: label("unresolvable", op, "x"), c: condition("unresolvable", op, strValue("x"))}
		all = append(all, leaf)
		representatives = append(representatives, leaf)
	}
	return all, representatives
}

// The coverage matrix: what the suite promises to have run, and the counters that say what it did.

// leafCells lists the (field, operator) cells of the criteria: one per condition it holds.
func leafCells(c *modelv1.Criteria, cells map[string]struct{}) {
	switch expression := c.GetExp().(type) {
	case *modelv1.Criteria_Le:
		leafCells(expression.Le.GetLeft(), cells)
		leafCells(expression.Le.GetRight(), cells)
	case *modelv1.Criteria_Condition:
		cells[expression.Condition.GetName()+" "+strings.TrimPrefix(expression.Condition.GetOp().String(), "BINARY_OP_")] = struct{}{}
	}
}

// shapeOf is how a criteria joins its conditions, with the conditions themselves left out: "x", "(x AND x)", "((x OR x) AND x)".
func shapeOf(c *modelv1.Criteria) string {
	expression, joined := c.GetExp().(*modelv1.Criteria_Le)
	if !joined {
		return "x"
	}
	op := strings.TrimPrefix(expression.Le.GetOp().String(), "LOGICAL_OP_")
	return "(" + shapeOf(expression.Le.GetLeft()) + " " + op + " " + shapeOf(expression.Le.GetRight()) + ")"
}

// rangeOperators are the comparisons that select a range of terms.
var rangeOperators = []modelv1.Condition_BinaryOp{
	modelv1.Condition_BINARY_OP_GT, modelv1.Condition_BINARY_OP_GE, modelv1.Condition_BINARY_OP_LT, modelv1.Condition_BINARY_OP_LE,
}

// foldCells lists the folds the criteria offers: "fold:s" when a conjunction (however nested) holds two ranges of s, "fold:m"
// for two of m, and "fold:none" when no conjunction holds two ranges of one field.
func foldCells(c *modelv1.Criteria) map[string]struct{} {
	cells := make(map[string]struct{})
	collectFolds(c, cells)
	if len(cells) == 0 {
		cells["fold:none"] = struct{}{}
	}
	return cells
}

func collectFolds(c *modelv1.Criteria, cells map[string]struct{}) {
	expression, joined := c.GetExp().(*modelv1.Criteria_Le)
	if !joined {
		return
	}
	if expression.Le.GetOp() == modelv1.LogicalExpression_LOGICAL_OP_OR {
		collectFolds(expression.Le.GetLeft(), cells)
		collectFolds(expression.Le.GetRight(), cells)
		return
	}
	ranges := make(map[string]int)
	for _, conjunct := range conjuncts(c) {
		if condition := conjunct.GetCondition(); condition != nil {
			if slices.Contains(rangeOperators, condition.GetOp()) {
				ranges[condition.GetName()]++
			}
			continue
		}
		collectFolds(conjunct, cells)
	}
	for field, count := range ranges {
		if count >= 2 {
			cells["fold:"+field] = struct{}{}
		}
	}
}

// conjuncts flattens the AND chain a criteria starts with into its operands; any other criteria is its own only operand.
func conjuncts(c *modelv1.Criteria) []*modelv1.Criteria {
	expression, joined := c.GetExp().(*modelv1.Criteria_Le)
	if !joined || expression.Le.GetOp() != modelv1.LogicalExpression_LOGICAL_OP_AND {
		return []*modelv1.Criteria{c}
	}
	return append(conjuncts(expression.Le.GetLeft()), conjuncts(expression.Le.GetRight())...)
}

func resultClass(matched, total int) string {
	switch matched {
	case 0:
		return "empty"
	case total:
		return "all"
	default:
		return "some"
	}
}

// canonicalCoverage counts the cells the suite has run.
type canonicalCoverage struct {
	leaves      map[string]int
	shapes      map[string]int
	shapeResult map[string]int
	folds       map[string]int
	mu          sync.Mutex
}

func newCanonicalCoverage() *canonicalCoverage {
	return &canonicalCoverage{leaves: map[string]int{}, shapes: map[string]int{}, shapeResult: map[string]int{}, folds: map[string]int{}}
}

func (cov *canonicalCoverage) record(c *modelv1.Criteria, matched, total int) {
	cov.mu.Lock()
	defer cov.mu.Unlock()
	cells := make(map[string]struct{})
	leafCells(c, cells)
	for cell := range cells {
		cov.leaves[cell]++
	}
	shape := shapeOf(c)
	cov.shapes[shape]++
	cov.shapeResult[shape+" -> "+resultClass(matched, total)]++
	for cell := range foldCells(c) {
		cov.folds[cell]++
	}
}

// expectedLeafCells is every (field, operator) the suite promises: the ten operators on the single-term and multi-term
// fields, MATCH on the text, and both comparisons of a tag that is not indexed.
func expectedLeafCells() []string {
	var cells []string
	for _, field := range []string{"s", "m"} {
		for _, op := range []string{"EQ", "NE", "IN", "NOT_IN", "HAVING", "NOT_HAVING", "GT", "GE", "LT", "LE"} {
			cells = append(cells, field+" "+op)
		}
	}
	return append(cells, "t MATCH", "unresolvable EQ", "unresolvable NE")
}

// expectedShapes is every way up to three conditions can be joined: a condition alone, two conditions, and three in the
// left-deep and the right-deep shape, for each choice of AND and OR.
func expectedShapes() []string {
	shapes := []string{"x"}
	for _, op := range []string{"AND", "OR"} {
		shapes = append(shapes, "(x "+op+" x)")
	}
	for _, outer := range []string{"AND", "OR"} {
		for _, inner := range []string{"AND", "OR"} {
			shapes = append(shapes, "((x "+inner+" x) "+outer+" x)", "(x "+outer+" (x "+inner+" x))")
		}
	}
	return shapes
}

// missing lists the cells of the matrix the suite did not run, empty when the suite is complete.
func (cov *canonicalCoverage) missing() []string {
	cov.mu.Lock()
	defer cov.mu.Unlock()
	var missing []string
	for _, cell := range expectedLeafCells() {
		if cov.leaves[cell] == 0 {
			missing = append(missing, "condition "+cell)
		}
	}
	for _, shape := range expectedShapes() {
		if cov.shapes[shape] == 0 {
			missing = append(missing, "shape "+shape)
		}
		for _, result := range []string{"empty", "some", "all"} {
			if cov.shapeResult[shape+" -> "+result] == 0 {
				missing = append(missing, "shape "+shape+" selecting "+result)
			}
		}
	}
	for _, fold := range []string{"fold:s", "fold:m", "fold:none"} {
		if cov.folds[fold] == 0 {
			missing = append(missing, fold)
		}
	}
	sort.Strings(missing)
	return missing
}

// The suite itself.

// canonicalResolvers are the two ways a caller can resolve the fields: vouching for none of them, so no range folds, and for
// s alone, so the ranges of s fold and the ranges of the multi-term field m and the text t do not.
func canonicalResolvers() map[string]FieldResolver {
	return map[string]FieldResolver{
		"no fold":   identityResolver{},
		"fold on s": vouchingResolver{single: map[string]bool{"s": true}},
	}
}

func TestCanonicalCriteriaSuite(t *testing.T) {
	view, docTerms, total := newCanonicalView(t)
	all, err := view.MatchTermsSet(context.Background(), differentialUniverse)
	require.NoError(t, err)
	require.Len(t, all, total)
	cov := newCanonicalCoverage()
	check := func(t *testing.T, c *modelv1.Criteria, name string) {
		t.Helper()
		for resolverName, resolver := range canonicalResolvers() {
			violation, matched := checkCriteria(t, view, docTerms, all, resolver, c, name+" ["+resolverName+"]")
			require.Empty(t, violation)
			cov.record(c, matched, total)
		}
	}
	all94, representatives := canonicalLeaves()

	t.Run("every operator on every field against every classical value", func(t *testing.T) {
		for _, leaf := range all94 {
			check(t, leaf.c, leaf.name)
		}
	})

	t.Run("every pair of representative conditions, joined by AND and by OR", func(t *testing.T) {
		for _, left := range representatives {
			for _, right := range representatives {
				check(t, andOf(left.c, right.c), "("+left.name+") AND ("+right.name+")")
				check(t, orOf(left.c, right.c), "("+left.name+") OR ("+right.name+")")
			}
		}
	})

	t.Run("every three conditions, in every shape", func(t *testing.T) {
		// The three that matter most are the two bounds of a field and a condition between them, in every order, so the
		// representatives of the triples are the range pair of each field, an exact value that matches and one that matches
		// nothing, a negation, MATCH and an unindexed tag.
		byName := make(map[string]canonicalLeaf, len(representatives))
		for _, leaf := range representatives {
			byName[leaf.name] = leaf
		}
		triples := []canonicalLeaf{
			byName["s GE d"], byName["s LE d"], byName["m GE d"], byName["m LE d"], byName["s EQ d"], byName["s EQ z"],
			byName["m NOT_HAVING [a,h]"], byName["unresolvable NE x"], byName[`t MATCH OR "red fox"`],
		}
		for _, leaf := range triples {
			require.NotNil(t, leaf.c, "a representative of the triples is missing from the leaves")
		}
		joins := []struct {
			combine func(left, right *modelv1.Criteria) *modelv1.Criteria
			name    string
		}{{andOf, "AND"}, {orOf, "OR"}}
		for _, first := range triples {
			for _, second := range triples {
				for _, third := range triples {
					for _, inner := range joins {
						for _, outer := range joins {
							name := fmt.Sprintf("%s | %s | %s", first.name, second.name, third.name)
							check(t, outer.combine(inner.combine(first.c, second.c), third.c), fmt.Sprintf("((%s) left-deep %s/%s)", name, inner.name, outer.name))
							check(t, outer.combine(first.c, inner.combine(second.c, third.c)), fmt.Sprintf("((%s) right-deep %s/%s)", name, outer.name, inner.name))
						}
					}
				}
			}
		}
	})

	t.Run("the regressions", func(t *testing.T) {
		for _, regression := range canonicalRegressions() {
			check(t, regression.c, regression.name)
		}
	})

	require.Empty(t, strings.Join(cov.missing(), "\n"), "the canonical suite no longer covers every cell of its matrix")
}

// canonicalRegression is a criteria that once broke, or nearly broke, the posting filter.
type canonicalRegression struct {
	c    *modelv1.Criteria
	name string
}

// canonicalRegressions are the criteria the review and the random test found, so that each stays covered whatever the
// generator does next. A criteria the random test reports belongs here.
func canonicalRegressions() []canonicalRegression {
	has := func(field, op string, value string) *modelv1.Criteria {
		ops := map[string]modelv1.Condition_BinaryOp{
			"GE": modelv1.Condition_BINARY_OP_GE, "LE": modelv1.Condition_BINARY_OP_LE, "EQ": modelv1.Condition_BINARY_OP_EQ,
			"NE": modelv1.Condition_BINARY_OP_NE,
		}
		return condition(field, ops[op], strValue(value))
	}
	matchAnd := buildMatchCondition("t", &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: modelv1.Condition_MatchOption_OPERATOR_AND}, strValue("red fox"))
	return []canonicalRegression{
		{
			name: "a document matches both bounds of a multi-term field through different terms (a fold would drop it)",
			c:    andOf(has("m", "GE", "g"), has("m", "LE", "b")),
		},
		{
			name: "three conditions with the bounds of one field apart, left-deep (the bounds must still fold)",
			c:    andOf(andOf(has("s", "GE", "d"), has("m", "EQ", "a")), has("s", "LE", "h")),
		},
		{
			name: "three conditions with the bounds of one field apart, right-deep",
			c:    andOf(has("s", "GE", "d"), andOf(has("m", "EQ", "a"), has("s", "LE", "h"))),
		},
		{name: "a negation of an unindexed tag inside an OR", c: orOf(has("unresolvable", "NE", "x"), has("s", "EQ", "a"))},
		{name: "an OR with one side that selects nothing", c: orOf(has("s", "EQ", "z"), condition("m", modelv1.Condition_BINARY_OP_HAVING, strArrayValue("a", "h")))},
		{name: "the bounds of the single-term field, folded", c: andOf(has("s", "GE", "a"), has("s", "LE", "d"))},
		{name: "a lower bound above the upper one, folded to nothing", c: andOf(has("s", "GE", "h"), has("s", "LE", "a"))},
		{name: "MATCH inside a conjunction that holds two ranges", c: andOf(andOf(matchAnd, has("s", "GE", "d")), has("s", "LE", "h"))},
		{
			name: "a negation and an exact value under an OR, inside a conjunction with a range",
			c: andOf(orOf(condition("m", modelv1.Condition_BINARY_OP_NOT_HAVING, strArrayValue("a", "h")), has("s", "EQ", "a")),
				has("m", "GE", "d")),
		},
		{
			name: "two ranges of the multi-term field under an OR that sits in a conjunction with two ranges of the single-term field",
			c: andOf(andOf(has("s", "GE", "a"), orOf(andOf(has("m", "GE", "g"), has("m", "LE", "b")), has("s", "EQ", "h"))),
				has("s", "LE", "d")),
		},
	}
}

// The random generator is only worth running if it can reach what the canonical suite promises: every condition, every shape
// and every kind of fold. If a change to the generator makes it blind to a cell, this test says which.
func TestRandomCriteriaReachEveryCanonicalCell(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test data
	cov := newCanonicalCoverage()
	for round := 0; round < 4000; round++ {
		c := randomCriteria(rng, 3)
		cells := make(map[string]struct{})
		leafCells(c, cells)
		for cell := range cells {
			cov.leaves[cell]++
		}
		cov.shapes[shapeOf(c)]++
		for cell := range foldCells(c) {
			cov.folds[cell]++
		}
	}
	var missing []string
	for _, cell := range expectedLeafCells() {
		if cov.leaves[cell] == 0 {
			missing = append(missing, "condition "+cell)
		}
	}
	for _, shape := range expectedShapes() {
		if cov.shapes[shape] == 0 {
			missing = append(missing, "shape "+shape)
		}
	}
	for _, fold := range []string{"fold:s", "fold:m", "fold:none"} {
		if cov.folds[fold] == 0 {
			missing = append(missing, fold)
		}
	}
	require.Empty(t, missing, "the random generator cannot reach every cell of the canonical matrix")
}

// The canonical documents are the classical ones, and the random generator builds criteria of shapes and values the suite does
// not list. Run on the canonical documents, it explores around the suite: what it finds there is a case to add to
// canonicalRegressions, with the seed that reproduced it.
func TestRandomCriteriaOnTheCanonicalDocuments(t *testing.T) {
	view, docTerms, total := newCanonicalView(t)
	all, err := view.MatchTermsSet(context.Background(), differentialUniverse)
	require.NoError(t, err)
	require.Len(t, all, total)
	for _, seed := range []int64{11, 12, 13} {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
		for round := 0; round < 400; round++ {
			c := randomCriteria(rng, 3)
			for resolverName, resolver := range canonicalResolvers() {
				label := fmt.Sprintf("seed %d round %d [%s]: %s", seed, round, resolverName, describeCriteria(c))
				violation, _ := checkCriteria(t, view, docTerms, all, resolver, c, label)
				require.Empty(t, violation, "add this criteria to canonicalRegressions so it stays covered")
			}
		}
	}
}
