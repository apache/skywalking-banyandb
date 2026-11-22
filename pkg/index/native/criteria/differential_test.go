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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
)

// The posting-level filter is an optimisation of Filter and promises two things the example tests only sample:
//
//   - it selects a superset of what Filter selects, so applying Filter to what it leaves gives Filter's answer; and
//   - when PostingFilterExact reports the filter exact, it selects exactly what Filter selects.
//
// This test attacks both with random criteria over documents that hold several terms per field, spread over several
// segments, because the bugs a posting filter can have live where an example does not look: a range over a field that holds
// several terms, a conjunction of three conditions, a negation inside an OR, an unindexed tag in the middle of a tree.

// differentialDocs builds the documents of the test: "s" holds one term (or none), "m" holds zero to three terms, "t" is an
// analyzed text. Every document carries kind=doc, which is the universe the criteria narrows.
func differentialDocs(t *testing.T, rng *rand.Rand, count int) (batches [][]native.Document, terms map[string]map[string][]string) {
	t.Helper()
	terms = make(map[string]map[string][]string, count)
	letters := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	words := []string{"red", "blue", "green", "fox", "dog", "quick", "lazy"}
	pick := func() string { return letters[rng.Intn(len(letters))] }
	batches = make([][]native.Document, 3)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		terms[id] = make(map[string][]string)
		fields := []native.Field{{Name: "kind", Value: []byte("doc"), Index: true}}
		if rng.Intn(10) != 0 { // one document in ten has no "s" at all
			value := pick()
			terms[id]["s"] = []string{value}
			fields = append(fields, native.Field{Name: "s", Value: []byte(value), Index: true})
		}
		if n := rng.Intn(4); n > 0 {
			seen := map[string]struct{}{}
			var multi []native.Term
			for len(multi) < n {
				value := pick()
				if _, dup := seen[value]; dup {
					continue
				}
				seen[value] = struct{}{}
				multi = append(multi, native.Term{Value: []byte(value)})
				terms[id]["m"] = append(terms[id]["m"], value)
			}
			fields = append(fields, native.Field{Name: "m", Terms: multi, Index: true})
		}
		text := words[rng.Intn(len(words))] + " " + words[rng.Intn(len(words))]
		analyzed, err := nativeanalysis.Analyze("standard", []byte(text))
		require.NoError(t, err)
		textTerms := make([]native.Term, 0, len(analyzed))
		for _, term := range analyzed {
			textTerms = append(textTerms, native.Term{Value: term.Value, Frequency: term.Frequency})
			terms[id]["t"] = append(terms[id]["t"], string(term.Value))
		}
		fields = append(fields, native.Field{Name: "t", Terms: textTerms, Index: true})
		batch := i % len(batches)
		batches[batch] = append(batches[batch], native.Document{Identifier: []byte(id), Fields: fields})
	}
	return batches, terms
}

// randomCriteria builds a criteria of at most depth levels: conditions on the single-term field, the multi-term field, the
// analyzed text and a tag that is not indexed, joined by AND and OR into trees of any shape, left-deep ones included.
func randomCriteria(rng *rand.Rand, depth int) *modelv1.Criteria {
	if depth > 0 && rng.Intn(10) < 6 {
		combine := andOf
		if rng.Intn(2) == 0 {
			combine = orOf
		}
		return combine(randomCriteria(rng, depth-1), randomCriteria(rng, depth-1))
	}
	letters := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	letter := func() string { return letters[rng.Intn(len(letters))] }
	several := func() *modelv1.TagValue {
		values := make([]string, 1+rng.Intn(3))
		for i := range values {
			values[i] = letter()
		}
		return strArrayValue(values...)
	}
	switch rng.Intn(14) {
	case 0:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_EQ, strValue(letter()))
	case 1:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_NE, strValue(letter()))
	case 2:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_IN, several())
	case 3:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_NOT_IN, several())
	case 4:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_HAVING, several())
	case 5:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_NOT_HAVING, several())
	case 6, 7:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_GT, strValue(letter()))
	case 8, 9:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_GE, strValue(letter()))
	case 10:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_LT, strValue(letter()))
	case 11:
		return condition(randomField(rng), modelv1.Condition_BINARY_OP_LE, strValue(letter()))
	default:
		words := []string{"red", "blue", "green", "fox", "dog", "quick", "lazy"}
		operator := modelv1.Condition_MatchOption_OPERATOR_OR
		if rng.Intn(2) == 0 {
			operator = modelv1.Condition_MatchOption_OPERATOR_AND
		}
		text := words[rng.Intn(len(words))] + " " + words[rng.Intn(len(words))]
		return buildMatchCondition("t", &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: operator}, strValue(text))
	}
}

// randomField picks the field a comparison runs on: mostly the single-term and the multi-term field, now and then a tag that
// is not indexed.
func randomField(rng *rand.Rand) string {
	switch rng.Intn(10) {
	case 0:
		return "unresolvable"
	case 1, 2, 3, 4:
		return "s"
	default:
		return "m"
	}
}

// docTermSet is the terms one document holds in one field.
type docTermSet []string

func (terms docTermSet) has(value string) bool {
	return terms.some(func(term string) bool { return term == value })
}

func (terms docTermSet) some(satisfies func(term string) bool) bool {
	for _, term := range terms {
		if satisfies(term) {
			return true
		}
	}
	return false
}

func (terms docTermSet) anyOf(values []string) bool {
	return terms.some(func(term string) bool {
		for _, value := range values {
			if term == value {
				return true
			}
		}
		return false
	})
}

func (terms docTermSet) allOf(values []string) bool {
	for _, value := range values {
		if !terms.has(value) {
			return false
		}
	}
	return true
}

// holds is the oracle: it evaluates a criteria directly on the terms a document holds, from what each operator means and
// without any of the code under test. A comparison holds when some term of the field satisfies it, a negation when no term
// does, and a tag that is not indexed matches nothing, or everything under a negation.
func holds(t *testing.T, c *modelv1.Criteria, docTerms map[string][]string) bool {
	t.Helper()
	switch expression := c.GetExp().(type) {
	case *modelv1.Criteria_Le:
		left, right := holds(t, expression.Le.GetLeft(), docTerms), holds(t, expression.Le.GetRight(), docTerms)
		if expression.Le.GetOp() == modelv1.LogicalExpression_LOGICAL_OP_AND {
			return left && right
		}
		return left || right
	case *modelv1.Criteria_Condition:
		return holdsCondition(t, expression.Condition, docTerms)
	}
	t.Fatalf("the oracle does not know %s", describeCriteria(c))
	return false
}

func holdsCondition(t *testing.T, condition *modelv1.Condition, docTerms map[string][]string) bool {
	t.Helper()
	op := condition.GetOp()
	negated := op == modelv1.Condition_BINARY_OP_NE || op == modelv1.Condition_BINARY_OP_NOT_IN || op == modelv1.Condition_BINARY_OP_NOT_HAVING
	if condition.GetName() == "unresolvable" {
		return negated
	}
	values := []string{condition.GetValue().GetStr().GetValue()}
	if array := condition.GetValue().GetStrArray(); array != nil {
		values = array.GetValue()
	}
	terms := docTermSet(docTerms[condition.GetName()])
	switch op {
	case modelv1.Condition_BINARY_OP_EQ:
		return terms.has(values[0])
	case modelv1.Condition_BINARY_OP_NE:
		return !terms.has(values[0])
	case modelv1.Condition_BINARY_OP_IN:
		return terms.anyOf(values)
	case modelv1.Condition_BINARY_OP_NOT_IN:
		return !terms.anyOf(values)
	case modelv1.Condition_BINARY_OP_HAVING:
		return terms.allOf(values)
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return !terms.allOf(values)
	case modelv1.Condition_BINARY_OP_GT:
		return terms.some(func(term string) bool { return term > values[0] })
	case modelv1.Condition_BINARY_OP_GE:
		return terms.some(func(term string) bool { return term >= values[0] })
	case modelv1.Condition_BINARY_OP_LT:
		return terms.some(func(term string) bool { return term < values[0] })
	case modelv1.Condition_BINARY_OP_LE:
		return terms.some(func(term string) bool { return term <= values[0] })
	case modelv1.Condition_BINARY_OP_MATCH:
		return holdsMatch(t, condition, terms, values[0])
	}
	t.Fatalf("the oracle does not know the operator %s", op)
	return false
}

// holdsMatch is a MATCH condition: the text, analyzed, matches a document that holds any (or, with the AND operator, all) of
// its terms.
func holdsMatch(t *testing.T, condition *modelv1.Condition, terms docTermSet, text string) bool {
	t.Helper()
	analyzed, err := nativeanalysis.Analyze("standard", []byte(text))
	require.NoError(t, err)
	matched := 0
	for _, term := range analyzed {
		if terms.has(string(term.Value)) {
			matched++
		}
	}
	if condition.GetMatchOption().GetOperator() == modelv1.Condition_MatchOption_OPERATOR_AND {
		return matched == len(analyzed)
	}
	return matched > 0
}

// describeCriteria renders a criteria for a failure message.
func describeCriteria(c *modelv1.Criteria) string {
	switch expression := c.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		condition := expression.Condition
		return fmt.Sprintf("%s %s %s", condition.GetName(), strings.TrimPrefix(condition.GetOp().String(), "BINARY_OP_"), tagValueString(condition.GetValue()))
	case *modelv1.Criteria_Le:
		op := strings.TrimPrefix(expression.Le.GetOp().String(), "LOGICAL_OP_")
		return "(" + describeCriteria(expression.Le.GetLeft()) + " " + op + " " + describeCriteria(expression.Le.GetRight()) + ")"
	}
	return "?"
}

func tagValueString(value *modelv1.TagValue) string {
	if array := value.GetStrArray(); array != nil {
		return "[" + strings.Join(array.GetValue(), ",") + "]"
	}
	return value.GetStr().GetValue()
}

// vouchingResolver is an identityResolver that vouches for the fields it lists as holding one term per document.
type vouchingResolver struct {
	identityResolver
	single map[string]bool
}

func (r vouchingResolver) SingleTerm(tagName string) bool { return r.single[tagName] }

// differentialUniverse is the request that selects every document of a differential view, and the hits it selects.
var differentialUniverse = native.TermSetRequest{Field: "kind", Terms: [][]byte{[]byte("doc")}, Mode: native.MatchAnyTerm}

// checkCriteria runs one criteria against the view with the resolver and returns what broke, or "" when nothing did, with the
// number of documents the criteria selects according to the oracle. It checks the promises of the posting filter: that
// Filter means what the criteria says, that the posting filter selects a superset of Filter, exactly Filter when it reports
// itself exact, and that Filter over what it leaves gives Filter's answer.
func checkCriteria(t *testing.T, view *native.ReadView, docTerms map[string]map[string][]string, all []native.QueryHit,
	resolver FieldResolver, c *modelv1.Criteria, label string,
) (violation string, matched int) {
	t.Helper()
	want, filterErr := Filter(context.Background(), view, all, c, resolver)
	require.NoError(t, filterErr, "the criteria must be one the hit-level filter accepts: %s", label)
	filter, exact := PostingFilterExact(c, resolver)
	narrowed, truncated, listErr := view.MatchTermsSetPrefixFiltered(context.Background(), differentialUniverse, filter, 0)
	require.NoError(t, listErr, label)
	require.False(t, truncated, label)
	keptByFilter, afterErr := Filter(context.Background(), view, narrowed, c, resolver)
	require.NoError(t, afterErr, label)
	var oracleIDs []string
	for _, hit := range all {
		if holds(t, c, docTerms[string(hit.Identifier)]) {
			oracleIDs = append(oracleIDs, string(hit.Identifier))
		}
	}
	if differs := symmetricDifference(oracleIDs, ids(want)); len(differs) > 0 {
		return fmt.Sprintf("%s\n  Filter and the oracle disagree on %v: the criteria does not mean what it says", label, differs), len(oracleIDs)
	}
	wantIDs, narrowedIDs := ids(want), ids(narrowed)
	if missing := missingFrom(wantIDs, narrowedIDs); len(missing) > 0 {
		return fmt.Sprintf("%s\n  the posting filter dropped %v, which Filter keeps (it must select a superset)", label, missing), len(oracleIDs)
	}
	if exact && len(narrowedIDs) != len(wantIDs) {
		return fmt.Sprintf("%s\n  the filter reports itself exact but selects %d documents and Filter selects %d", label, len(narrowedIDs), len(wantIDs)), len(oracleIDs)
	}
	if extra := missingFrom(ids(keptByFilter), wantIDs); len(extra) > 0 || len(keptByFilter) != len(want) {
		return fmt.Sprintf("%s\n  Filter over the narrowed hits keeps %d documents and Filter over all of them keeps %d", label, len(keptByFilter), len(want)), len(oracleIDs)
	}
	return "", len(oracleIDs)
}

// findDifferentialViolation runs rounds random criteria against the view with the resolver and returns the first criteria for
// which the posting filter breaks a promise, with what broke, or "" when none does. A failure names the seed and the round, so
// that the criteria it found can be added to canonicalRegressions.
func findDifferentialViolation(t *testing.T, view *native.ReadView, docTerms map[string]map[string][]string, resolver FieldResolver,
	seed int64, rounds int,
) string {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	all, err := view.MatchTermsSet(context.Background(), differentialUniverse)
	require.NoError(t, err)
	for round := 0; round < rounds; round++ {
		c := randomCriteria(rng, 3)
		label := fmt.Sprintf("seed %d round %d: %s", seed, round, describeCriteria(c))
		if violation, _ := checkCriteria(t, view, docTerms, all, resolver, c, label); violation != "" {
			return violation + "\n  (add this criteria to canonicalRegressions so it stays covered)"
		}
	}
	return ""
}

// symmetricDifference returns the identifiers that are in exactly one of the two lists.
func symmetricDifference(left, right []string) []string {
	return append(missingFrom(left, right), missingFrom(right, left)...)
}

// missingFrom returns the identifiers of want that are not in have.
func missingFrom(want, have []string) []string {
	present := make(map[string]struct{}, len(have))
	for _, id := range have {
		present[id] = struct{}{}
	}
	var missing []string
	for _, id := range want {
		if _, ok := present[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func newDifferentialView(t *testing.T, seed int64, docs int) (*native.ReadView, map[string]map[string][]string) {
	t.Helper()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	batches, docTerms := differentialDocs(t, rand.New(rand.NewSource(seed)), docs) //nolint:gosec // deterministic test data
	for _, batch := range batches {
		require.NoError(t, owner.Batch(context.Background(), native.Batch{Documents: batch}))
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, view.Close())
		require.NoError(t, owner.Close())
	})
	return view, docTerms
}

func TestPostingFilterOfAgreesWithFilterOnRandomCriteria(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		view, docTerms := newDifferentialView(t, seed, 240)
		for name, resolver := range map[string]FieldResolver{
			"no field is vouched for single-term": identityResolver{},
			// "s" holds one term per document, so its ranges fold; "m" and "t" can hold several, so theirs must not.
			"only s is vouched for single-term": vouchingResolver{single: map[string]bool{"s": true}},
		} {
			t.Run(fmt.Sprintf("seed=%d/%s", seed, name), func(t *testing.T) {
				require.Empty(t, findDifferentialViolation(t, view, docTerms, resolver, seed*100, 600))
			})
		}
	}
}

// The differential test is only worth having if it can fail. A resolver that wrongly vouches for the multi-term field makes
// its ranges fold, and a document that matches each bound through a different term is lost: the test must see that.
func TestDifferentialTestNoticesAWronglyVouchedMultiTermField(t *testing.T) {
	view, docTerms := newDifferentialView(t, 1, 240)
	wrong := vouchingResolver{single: map[string]bool{"s": true, "m": true}}
	violation := findDifferentialViolation(t, view, docTerms, wrong, 100, 600)
	require.NotEmpty(t, violation, "folding the ranges of a multi-term field must be caught")
	require.Contains(t, violation, "superset")
}
