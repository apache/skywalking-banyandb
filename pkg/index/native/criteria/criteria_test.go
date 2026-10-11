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
