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
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeanalysis"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type testLease struct{}

func (testLease) Validate() error { return nil }

// identityResolver resolves every tag name to itself as the engine field and
// never reports an analyzer, except "unresolvable" which always reports
// ok=false, exercising the "tag is not indexed for this caller" contract.
type identityResolver struct{}

func (identityResolver) Field(tagName string) (string, string, bool) {
	if tagName == "unresolvable" {
		return "", "", false
	}
	return tagName, "", true
}

// newCriteriaTestView builds three documents (a, b, c) carrying a scalar
// "name", a scalar byte-ordered "age", a multi-valued "colors", and an
// analyzed "bio" text field, and returns a pinned view over them plus its
// teardown.
func newCriteriaTestView(t *testing.T) (*native.ReadView, func()) {
	t.Helper()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	bio := func(text string) native.Field {
		terms, analyzeErr := nativeanalysis.Analyze("standard", []byte(text))
		require.NoError(t, analyzeErr)
		nativeTerms := make([]native.Term, 0, len(terms))
		for _, term := range terms {
			nativeTerms = append(nativeTerms, native.Term{Value: term.Value, Frequency: term.Frequency})
		}
		return native.Field{Name: "bio", Terms: nativeTerms, Index: true}
	}
	colors := func(values ...string) native.Field {
		terms := make([]native.Term, 0, len(values))
		for _, value := range values {
			terms = append(terms, native.Term{Value: []byte(value)})
		}
		return native.Field{Name: "colors", Terms: terms, Index: true}
	}
	age := func(value int64) native.Field {
		return native.Field{Name: "age", Value: convert.Int64ToBytes(value), Index: true}
	}
	name := func(value string) native.Field {
		return native.Field{Name: "name", Value: []byte(value), Index: true}
	}
	require.NoError(t, owner.Batch(context.Background(), native.Batch{Documents: []native.Document{
		{Identifier: []byte("a"), Fields: []native.Field{name("alice"), age(10), colors("red", "blue"), bio("the quick red fox")}},
		{Identifier: []byte("b"), Fields: []native.Field{name("bob"), age(20), colors("blue"), bio("the lazy dog")}},
		{Identifier: []byte("c"), Fields: []native.Field{name("carol"), age(30), colors("red", "green"), bio("red fox and blue dog")}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	return view, func() {
		require.NoError(t, view.Close())
		require.NoError(t, owner.Close())
	}
}

func universe(t *testing.T, view *native.ReadView) []native.QueryHit {
	t.Helper()
	hits, err := view.MatchTermsSet(context.Background(), native.TermSetRequest{
		Field: "name", Terms: [][]byte{[]byte("alice"), []byte("bob"), []byte("carol")},
	})
	require.NoError(t, err)
	require.Len(t, hits, 3)
	return hits
}

func ids(hits []native.QueryHit) []string {
	result := make([]string, len(hits))
	for i, hit := range hits {
		result[i] = string(hit.Identifier)
	}
	return result
}

func strValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

func strArrayValue(values ...string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
}

func intValue(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: value}}}
}

func condition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: name, Op: op, Value: value}}}
}

func buildMatchCondition(name string, option *modelv1.Condition_MatchOption, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: name, Op: modelv1.Condition_BINARY_OP_MATCH, MatchOption: option, Value: value,
	}}}
}

func TestFilterNilCriteriaReturnsUniverseUnchanged(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	base := universe(t, view)
	hits, err := Filter(context.Background(), view, base, nil, identityResolver{})
	require.NoError(t, err)
	require.Equal(t, base, hits)
}

func TestFilterEQ(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(context.Background(), view, universe(t, view), condition("name", modelv1.Condition_BINARY_OP_EQ, strValue("bob")), identityResolver{})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, ids(hits))
}

func TestFilterNE(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(context.Background(), view, universe(t, view), condition("name", modelv1.Condition_BINARY_OP_NE, strValue("bob")), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "c"}, ids(hits))
}

func TestFilterIN(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(context.Background(), view, universe(t, view), condition("name", modelv1.Condition_BINARY_OP_IN, strArrayValue("bob", "carol")), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"b", "c"}, ids(hits))
}

func TestFilterNotIN(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(
		context.Background(), view, universe(t, view), condition("name", modelv1.Condition_BINARY_OP_NOT_IN, strArrayValue("bob", "carol")), identityResolver{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids(hits))
}

func TestFilterHAVING(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(
		context.Background(), view, universe(t, view), condition("colors", modelv1.Condition_BINARY_OP_HAVING, strArrayValue("red", "blue")), identityResolver{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids(hits), "only a document carrying both red and blue qualifies")
}

func TestFilterNotHAVING(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(
		context.Background(), view, universe(t, view), condition("colors", modelv1.Condition_BINARY_OP_NOT_HAVING, strArrayValue("red", "blue")), identityResolver{},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"b", "c"}, ids(hits))
}

func TestFilterRanges(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	gt, err := Filter(context.Background(), view, universe(t, view), condition("age", modelv1.Condition_BINARY_OP_GT, intValue(10)), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"b", "c"}, ids(gt))
	ge, err := Filter(context.Background(), view, universe(t, view), condition("age", modelv1.Condition_BINARY_OP_GE, intValue(10)), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, ids(ge))
	lt, err := Filter(context.Background(), view, universe(t, view), condition("age", modelv1.Condition_BINARY_OP_LT, intValue(30)), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, ids(lt))
	le, err := Filter(context.Background(), view, universe(t, view), condition("age", modelv1.Condition_BINARY_OP_LE, intValue(20)), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, ids(le))
}

func TestFilterMATCH(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	anyMode := &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: modelv1.Condition_MatchOption_OPERATOR_OR}
	allMode := &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: modelv1.Condition_MatchOption_OPERATOR_AND}
	anyHits, err := Filter(context.Background(), view, universe(t, view), buildMatchCondition("bio", anyMode, strValue("red dog")), identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, ids(anyHits), "OR matches any document containing \"red\" or \"dog\"")
	allHits, err := Filter(context.Background(), view, universe(t, view), buildMatchCondition("bio", allMode, strValue("red dog")), identityResolver{})
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, ids(allHits), "AND matches only the document containing both \"red\" and \"dog\"")
}

func TestFilterAND(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	criteria := &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
		Left:  condition("colors", modelv1.Condition_BINARY_OP_EQ, strValue("red")),
		Right: condition("age", modelv1.Condition_BINARY_OP_GE, intValue(30)),
	}}}
	hits, err := Filter(context.Background(), view, universe(t, view), criteria, identityResolver{})
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, ids(hits))
}

func TestFilterOR(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	criteria := &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op:    modelv1.LogicalExpression_LOGICAL_OP_OR,
		Left:  condition("name", modelv1.Condition_BINARY_OP_EQ, strValue("alice")),
		Right: condition("name", modelv1.Condition_BINARY_OP_EQ, strValue("bob")),
	}}}
	hits, err := Filter(context.Background(), view, universe(t, view), criteria, identityResolver{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, ids(hits))
}

func TestFilterLogicalExpressionWithOneNilOperand(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	leftOnly := &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op: modelv1.LogicalExpression_LOGICAL_OP_AND, Left: condition("name", modelv1.Condition_BINARY_OP_EQ, strValue("alice")),
	}}}
	hits, err := Filter(context.Background(), view, universe(t, view), leftOnly, identityResolver{})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, ids(hits))

	rightOnly := &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op: modelv1.LogicalExpression_LOGICAL_OP_AND, Right: condition("name", modelv1.Condition_BINARY_OP_EQ, strValue("bob")),
	}}}
	hits, err = Filter(context.Background(), view, universe(t, view), rightOnly, identityResolver{})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, ids(hits))

	empty := &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: modelv1.LogicalExpression_LOGICAL_OP_AND}}}
	_, err = Filter(context.Background(), view, universe(t, view), empty, identityResolver{})
	require.ErrorIs(t, err, logical.ErrInvalidLogicalExpression)
}

func TestFilterUnresolvedFieldPositiveConditionYieldsNoHits(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	hits, err := Filter(context.Background(), view, universe(t, view), condition("unresolvable", modelv1.Condition_BINARY_OP_EQ, strValue("x")), identityResolver{})
	require.NoError(t, err)
	require.Empty(t, hits)
}

func TestFilterUnresolvedFieldNegativeConditionYieldsFullUniverse(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	base := universe(t, view)
	hits, err := Filter(context.Background(), view, base, condition("unresolvable", modelv1.Condition_BINARY_OP_NE, strValue("x")), identityResolver{})
	require.NoError(t, err)
	require.Equal(t, base, hits)
}

func TestFilterInvalidCriteriaType(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	_, err := Filter(context.Background(), view, universe(t, view), &modelv1.Criteria{}, identityResolver{})
	require.ErrorIs(t, err, logical.ErrInvalidCriteriaType)
}

func andOf(left, right *modelv1.Criteria) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: modelv1.LogicalExpression_LOGICAL_OP_AND, Left: left, Right: right}}}
}

func orOf(left, right *modelv1.Criteria) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: modelv1.LogicalExpression_LOGICAL_OP_OR, Left: left, Right: right}}}
}

// The posting-level filter of a criteria selects the same documents Filter selects, for every operator and combination:
// narrowing the universe by the filter and then applying Filter gives Filter's answer, and when every condition translates
// the filter alone already is that answer.
// narrowByPosting is the production entry point for a posting-level filter: it lists the candidates of the universe that the
// filter matches, without decoding any document it drops.
func narrowByPosting(t *testing.T, view *native.ReadView, filter *native.PostingFilter) []native.QueryHit {
	t.Helper()
	hits, truncated, err := view.MatchTermsSetPrefixFiltered(context.Background(), native.TermSetRequest{
		Field: "name", Terms: [][]byte{[]byte("alice"), []byte("bob"), []byte("carol")},
	}, filter, 0)
	require.NoError(t, err)
	require.False(t, truncated)
	return hits
}

// singleTermResolver is an identityResolver that vouches for the tags that hold one term per document.
type singleTermResolver struct {
	identityResolver
	single map[string]bool
}

func (r singleTermResolver) SingleTerm(tagName string) bool { return r.single[tagName] }

// The posting-level filter of a criteria selects the same documents Filter selects, for every operator and combination:
// narrowing the universe by the filter and then applying Filter gives Filter's answer, and when every condition translates
// the filter alone already is that answer.
func TestPostingFilterOfAgreesWithFilter(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	name := func(op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
		return condition("name", op, value)
	}
	eq := name(modelv1.Condition_BINARY_OP_EQ, strValue("bob"))
	in := name(modelv1.Condition_BINARY_OP_IN, strArrayValue("bob", "carol"))
	ne := name(modelv1.Condition_BINARY_OP_NE, strValue("bob"))
	notIn := name(modelv1.Condition_BINARY_OP_NOT_IN, strArrayValue("bob", "carol"))
	having := condition("colors", modelv1.Condition_BINARY_OP_HAVING, strArrayValue("red", "blue"))
	notHaving := condition("colors", modelv1.Condition_BINARY_OP_NOT_HAVING, strArrayValue("red", "blue"))
	gt := condition("age", modelv1.Condition_BINARY_OP_GT, intValue(10))
	ge := condition("age", modelv1.Condition_BINARY_OP_GE, intValue(10))
	lt := condition("age", modelv1.Condition_BINARY_OP_LT, intValue(30))
	le := condition("age", modelv1.Condition_BINARY_OP_LE, intValue(20))
	unindexed := condition("unresolvable", modelv1.Condition_BINARY_OP_EQ, strValue("x"))
	unindexedNot := condition("unresolvable", modelv1.Condition_BINARY_OP_NE, strValue("x"))
	invalid := name(modelv1.Condition_BINARY_OP_IN, strValue("bob")) // IN needs an array: Filter reports the error
	matchAny := buildMatchCondition("bio", &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: modelv1.Condition_MatchOption_OPERATOR_OR}, strValue("red dog"))
	matchAll := buildMatchCondition("bio", &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: modelv1.Condition_MatchOption_OPERATOR_AND}, strValue("red dog"))
	matchName := buildMatchCondition("name", &modelv1.Condition_MatchOption{Analyzer: "standard", Operator: modelv1.Condition_MatchOption_OPERATOR_OR}, strValue("bob"))
	type tc struct {
		criteria *modelv1.Criteria
		name     string
		// wantErr marks a criteria the hit-level filter rejects: nothing then checks the narrowed answer against Filter's, only
		// that narrowing kept every document the translatable part of the criteria keeps (superset), which is eq here.
		wantErr bool
	}
	for _, c := range []tc{
		{name: "eq", criteria: eq},
		{name: "in", criteria: in},
		{name: "ne", criteria: ne},
		{name: "not in", criteria: notIn},
		{name: "having", criteria: having},
		{name: "not having", criteria: notHaving},
		{name: "gt", criteria: gt},
		{name: "ge", criteria: ge},
		{name: "lt", criteria: lt},
		{name: "le", criteria: le},
		{name: "match any", criteria: matchAny},
		{name: "match all", criteria: matchAll},
		{name: "match and range", criteria: andOf(matchAll, ge)},
		{name: "match on a keyword field", criteria: matchName},
		{name: "unindexed", criteria: unindexed},
		{name: "unindexed negation", criteria: unindexedNot},
		{name: "and", criteria: andOf(in, ne)},
		{name: "and range", criteria: andOf(in, gt)},
		{name: "or", criteria: orOf(eq, ne)},
		{name: "or of ranges", criteria: orOf(lt, gt)},
		{name: "not in and range", criteria: andOf(notIn, le)},
		{name: "nested", criteria: andOf(orOf(eq, in), andOf(in, gt))},
		{name: "deep", criteria: orOf(andOf(eq, ge), andOf(notHaving, lt))},
		{name: "unindexed under or", criteria: orOf(eq, unindexed)},
		{name: "unindexed negation under and", criteria: andOf(eq, unindexedNot)},
		{name: "and with an untranslatable side", criteria: andOf(eq, invalid), wantErr: true},
		{name: "or with an untranslatable side", criteria: orOf(eq, invalid), wantErr: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := universe(t, view)
			narrowed := narrowByPosting(t, view, PostingFilterOf(c.criteria, identityResolver{}))
			want, err := Filter(context.Background(), view, base, c.criteria, identityResolver{})
			if c.wantErr {
				require.Error(t, err)
				eqHits, eqErr := Filter(context.Background(), view, base, eq, identityResolver{})
				require.NoError(t, eqErr)
				require.Subset(t, ids(narrowed), ids(eqHits))
				return
			}
			require.NoError(t, err)
			// Every condition translates, so the filter alone is Filter's answer.
			require.ElementsMatch(t, ids(want), ids(narrowed))
			got, err := Filter(context.Background(), view, narrowed, c.criteria, identityResolver{})
			require.NoError(t, err)
			require.ElementsMatch(t, ids(want), ids(got))
		})
	}
}

// A range on a field that can hold several terms must not fold, or a document that matches each bound through a different
// term is lost; a range on a field the resolver vouches for as single-term does fold, and still agrees with Filter.
func TestPostingFilterOfFoldsOnlyRangesOfSingleTermFields(t *testing.T) {
	view, closeFn := newCriteriaTestView(t)
	defer closeFn()
	// Document a holds the colors red and blue, c holds red and green: "colors >= red AND colors <= blue" matches a through
	// red (>= red) and blue (<= blue), and matches no single term of the folded range [red, blue], which is empty.
	between := andOf(
		condition("colors", modelv1.Condition_BINARY_OP_GE, strValue("red")),
		condition("colors", modelv1.Condition_BINARY_OP_LE, strValue("blue")))
	want, err := Filter(context.Background(), view, universe(t, view), between, identityResolver{})
	require.NoError(t, err)
	require.Contains(t, ids(want), "a", "the hit-level filter keeps the document with a term on each side")

	for name, resolver := range map[string]FieldResolver{
		"no resolver vouches":   identityResolver{},
		"colors is not vouched": singleTermResolver{single: map[string]bool{"age": true}},
	} {
		filter, exact := PostingFilterExact(between, resolver)
		require.True(t, exact, name)
		require.ElementsMatch(t, ids(want), ids(narrowByPosting(t, view, filter)), "%s: the unfolded ranges keep what the hit-level filter keeps", name)
	}

	// age is one integer per document: its two bounds fold, and the folded range is what Filter selects.
	ages := andOf(condition("age", modelv1.Condition_BINARY_OP_GE, intValue(10)), condition("age", modelv1.Condition_BINARY_OP_LE, intValue(20)))
	vouched := singleTermResolver{single: map[string]bool{"age": true}}
	wantAges, err := Filter(context.Background(), view, universe(t, view), ages, vouched)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, ids(wantAges))
	require.ElementsMatch(t, ids(wantAges), ids(narrowByPosting(t, view, PostingFilterOf(ages, vouched))))
}

func TestPostingFilterOfPushesNothingForNoCriteria(t *testing.T) {
	require.Nil(t, PostingFilterOf(nil, identityResolver{}))
	require.Nil(t, PostingFilterOf(condition("name", modelv1.Condition_BINARY_OP_EQ, strValue("bob")), nil))
}
