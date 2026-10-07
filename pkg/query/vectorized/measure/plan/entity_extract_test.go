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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logicalmeasure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
)

// entityExtractTestSchema builds a fixture with two entity tags (service,
// instance), one regular indexed tag (region, rule ID 7, "keyword"
// analyzer) and one tag with neither (unindexed) -- enough to exercise
// every branch of extractSeriesMatchers and measureFieldResolver.
func entityExtractTestSchema(t *testing.T) logical.Schema {
	t.Helper()
	md := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "default"},
		Entity:   &databasev1.Entity{TagNames: []string{"service", "instance"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "instance", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "unindexed", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Id: 7, Name: "region_rule"}, Tags: []string{"region"}, Analyzer: "keyword"},
	}
	s, err := logicalmeasure.BuildSchema(md, rules)
	require.NoError(t, err)
	return s
}

// strCond builds an EQ condition; every caller below wants EQ, so the op is
// not a parameter (unparam).
func strCond(name, value string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: name, Op: modelv1.Condition_BINARY_OP_EQ,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	}}}
}

// matchCond builds a MATCH condition.
func matchCond(name, value string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: name, Op: modelv1.Condition_BINARY_OP_MATCH,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	}}}
}

func strArrayCond(name string, op modelv1.Condition_BinaryOp, values ...string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: name, Op: op,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}},
	}}}
}

func logicalExpr(op modelv1.LogicalExpression_LogicalOp, left, right *modelv1.Criteria) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: op, Left: left, Right: right}}}
}

// entityTemplate builds the (entityDict, entity) pair extractSeriesMatchers
// expects, the same way dispatch.go does from a schema's EntityList.
func entityTemplate(entityList []string) (map[string]int, []*modelv1.TagValue) {
	entityDict := make(map[string]int, len(entityList))
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityDict[e] = idx
		entity[idx] = pbv1.AnyTagValue
	}
	return entityDict, entity
}

func TestExtractSeriesMatchers_EntityConditionOnly(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	remaining, entities, _, err := extractSeriesMatchers(strCond("service", "frontend"), schema, entityDict, entity)
	require.NoError(t, err)
	require.Nil(t, remaining, "a pure entity condition leaves no filter for the series index")
	require.Len(t, entities, 1)
	require.Equal(t, "frontend", entities[0][0].GetStr().GetValue())
	require.Same(t, pbv1.AnyTagValue, entities[0][1], "the untouched entity slot stays wildcard")
}

func TestExtractSeriesMatchers_IndexedConditionOnly(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	cond := strCond("region", "us-west")
	remaining, entities, _, err := extractSeriesMatchers(cond, schema, entityDict, entity)
	require.NoError(t, err)
	require.Same(t, cond, remaining, "a non-entity indexed condition passes through unchanged for criteria.Filter")
	require.Len(t, entities, 1)
	require.Same(t, pbv1.AnyTagValue, entities[0][0])
	require.Same(t, pbv1.AnyTagValue, entities[0][1])
}

func TestExtractSeriesMatchers_UnindexedConditionErrors(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	remaining, entities, isMatchAll, err := extractSeriesMatchers(strCond("unindexed", "x"), schema, entityDict, entity)
	require.Error(t, err, "a tag with neither an index rule nor entity membership must be rejected, matching BuildQuery's mandatory-index-rule check")
	require.Nil(t, remaining)
	require.Nil(t, entities)
	require.False(t, isMatchAll)
}

func TestExtractSeriesMatchers_AndSplitsEntityFromFilter(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	entityCond := strCond("service", "frontend")
	indexedCond := strCond("region", "us-west")
	remaining, entities, _, err := extractSeriesMatchers(logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, entityCond, indexedCond), schema, entityDict, entity)
	require.NoError(t, err)
	require.Same(t, indexedCond, remaining, "AND keeps only the non-entity branch as remaining criteria")
	require.Len(t, entities, 1)
	require.Equal(t, "frontend", entities[0][0].GetStr().GetValue())
}

func TestExtractSeriesMatchers_InOnEntityTagProducesMultipleTuples(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	remaining, entities, _, err := extractSeriesMatchers(
		strArrayCond("service", modelv1.Condition_BINARY_OP_IN, "frontend", "backend"), schema, entityDict, entity)
	require.NoError(t, err)
	require.Nil(t, remaining)
	require.Len(t, entities, 2)
	require.Equal(t, "frontend", entities[0][0].GetStr().GetValue())
	require.Equal(t, "backend", entities[1][0].GetStr().GetValue())
}

func TestExtractSeriesMatchers_OrOfTwoIndexedConditions(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	left := strCond("region", "us-west")
	right := strCond("region", "us-east")
	remaining, _, _, err := extractSeriesMatchers(logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR, left, right), schema, entityDict, entity)
	require.NoError(t, err)
	require.NotNil(t, remaining, "two indexed conditions OR'd together still need a remaining filter")
	le := remaining.GetLe()
	require.NotNil(t, le)
	require.Equal(t, modelv1.LogicalExpression_LOGICAL_OP_OR, le.Op)
}

func TestExtractSeriesMatchers_NilCriteriaMatchesAllWithTemplateEntity(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	remaining, entities, isMatchAll, err := extractSeriesMatchers(nil, schema, entityDict, entity)
	require.NoError(t, err)
	require.Nil(t, remaining)
	require.False(t, isMatchAll)
	require.Equal(t, [][]*modelv1.TagValue{entity}, entities)
}

// TestExtractSeriesMatchers_MatchAllPropagatesThroughNestedOr is the S4
// regression: extractSeriesMatchers' AND/OR combiner, when both recursive
// results reduce to a nil "remaining" criteria, used to hardcode
// isMatchAll=false instead of propagating leftAll||rightAll. A nil
// "remaining" is overloaded in this function -- it means both "this subtree
// is a pure entity condition, not evaluated as a filter" (isMatchAll=false)
// and "this subtree is an explicit tautology" (isMatchAll=true, produced by
// the allEntitiesSpecific/both-match-all shortcuts a few lines up) -- so
// losing the flag at a "both nil" node silently un-flags an ancestor that
// really is match-all, which a later OR combine then wrongly turns into a
// real (and wrong) filter instead of recognizing the whole subtree always
// matches.
//
// The fixture below is the exact nested shape that exposed it:
//
//	x = OR(AND(service=a, instance=i1), AND(service=b, AND(instance=i2, region=r)))
//	y = OR(x, AND(service=c, instance=i3))
//	z = OR(y, region=r2)
//
// x's OR already collapses via the allEntitiesSpecific shortcut (both its
// entity tuples are fully specific), so x.isMatchAll=true with x's own
// "remaining" nil. y combines x (remaining=nil, isMatchAll=true) with a
// second pure-entity AND (remaining=nil, isMatchAll=false): both operands'
// remaining is nil, hitting the fixed branch. Before the fix y silently
// dropped x's matchAll=true, and z (OR'd with a genuine indexed condition,
// region=r2) then surfaced "region==r2" as if it were a real required
// filter instead of recognizing the whole expression always matches.
//
// Each step was also asserted, before Phase 3 deleted it, against the
// in-tree pkg/index/inverted.BuildQuery, which never lost the flag because
// it represented "match all" with an explicit non-nil bluge.NewMatchAllQuery
// sentinel rather than nil. The facts that oracle contributed -- the query
// was always non-nil and specifically a MatchAllQuery (query.String() ==
// "matchAll") for x, y and z, legacyIsMatchAll is true for all three, and
// the entity matchers BuildQuery produced alongside that query -- are
// captured below as literals.
//
// The capture itself (F3, 2026-10-07): `git archive 195de135 | tar -x -C
// /mnt/d/tmp-gao-build/head-195de135` exports HEAD, which still has
// BuildQuery and the Phase 2 differential test that called it
// (`git show 195de135:pkg/query/vectorized/measure/plan/entity_extract_test.go`).
// Running that test there with a temporary fmt.Printf of
// legacyQuery/legacyEntities/legacyIsMatchAll for x, y and z produced:
//
//	case=x isMatchAll=true queryNil=false queryString="matchAll" entities=[[Str(a) Str(i1)] [Str(b) Str(i2)]]
//	case=y isMatchAll=true queryNil=false queryString="matchAll" entities=[[Str(a) Str(i1)] [Str(b) Str(i2)] [Str(c) Str(i3)]]
//	case=z isMatchAll=true queryNil=false queryString="matchAll" entities=[[AnyTagValue AnyTagValue]]
//
// A second, identical temporary print of the CURRENT, live
// extractSeriesMatchers(tc.criteria, schema, entityDict, entity) in this
// package (no legacy evaluator involved) reproduced the exact same
// isMatchAll and entities values for x, y and z, confirming
// extractSeriesMatchers preserves BuildQuery's entity-matcher extraction
// even though it no longer builds a bluge query at all; remaining was nil
// in every case on both sides. Both temporary prints were removed after
// capture; nothing here was fabricated.
func TestExtractSeriesMatchers_MatchAllPropagatesThroughNestedOr(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, entity := entityTemplate(schema.EntityList())

	a1 := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, strCond("service", "a"), strCond("instance", "i1"))
	a2 := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, strCond("service", "b"),
		logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, strCond("instance", "i2"), strCond("region", "r")))
	x := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR, a1, a2)
	y := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR, x,
		logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, strCond("service", "c"), strCond("instance", "i3")))
	z := logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR, y, strCond("region", "r2"))

	cases := []struct { //nolint:govet // table-test fields grouped by meaning, not padding.
		name         string
		criteria     *modelv1.Criteria
		wantEntities [][]*modelv1.TagValue
	}{
		{
			name:     "x",
			criteria: x,
			wantEntities: [][]*modelv1.TagValue{
				{strTagValue("a"), strTagValue("i1")},
				{strTagValue("b"), strTagValue("i2")},
			},
		},
		{
			name:     "y",
			criteria: y,
			wantEntities: [][]*modelv1.TagValue{
				{strTagValue("a"), strTagValue("i1")},
				{strTagValue("b"), strTagValue("i2")},
				{strTagValue("c"), strTagValue("i3")},
			},
		},
		{
			name:     "z",
			criteria: z,
			// Neither leaf on this path touches an entity tag (region only),
			// so the single surviving tuple is the untouched wildcard
			// template -- the same pbv1.AnyTagValue sentinel entityTemplate
			// seeded entity with, not a copy.
			wantEntities: [][]*modelv1.TagValue{
				{pbv1.AnyTagValue, pbv1.AnyTagValue},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			remaining, entities, isMatchAll, err := extractSeriesMatchers(tc.criteria, schema, entityDict, entity)
			require.NoError(t, err)

			// legacyIsMatchAll and legacyQueryWasNonNil are literals captured
			// from a live inverted.BuildQuery(tc.criteria, schema, entityDict,
			// entity) call before NIDX-03 deleted that function; see the
			// capture procedure in this test's doc comment above and in
			// docs/design/0.12.0/native-inverted-index/verification/nidx-03-series-cutover/README.md.
			const (
				legacyIsMatchAll     = true
				legacyQueryWasNonNil = true
			)

			require.Equal(t, legacyIsMatchAll, isMatchAll, "isMatchAll must match the legacy evaluator's captured result")
			require.True(t, isMatchAll, "the whole expression always matches once an OR branch is a tautology")
			require.Nil(t, remaining, "a match-all subtree needs no remaining filter for criteria.Filter")
			require.True(t, legacyQueryWasNonNil, "legacy represented match-all with an explicit non-nil MatchAllQuery node, captured before deletion")
			require.Equal(t, tc.wantEntities, entities, "entity matchers must match the legacy evaluator's captured result")
			for i, tuple := range tc.wantEntities {
				for j, want := range tuple {
					if want == pbv1.AnyTagValue {
						require.Same(t, pbv1.AnyTagValue, entities[i][j], "an untouched entity slot must stay the shared wildcard sentinel, not a copy")
					}
				}
			}
		})
	}
}

// strTagValue builds a string *modelv1.TagValue the same way strCond's
// underlying condition value does, for comparing against extractSeriesMatchers'
// entity-tuple output.
func strTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

// TestValidateIndexModeCriteria is the S3 regression: index mode passes
// criteria straight through to criteria.Filter via measureFieldResolver,
// whose resolver-miss (ok=false) is deliberately permissive -- it must not
// be the only gate for a condition the previous release's
// BuildIndexModeQuery/buildIndexModeCriteria rejected outright.
func TestValidateIndexModeCriteria(t *testing.T) {
	schema := entityExtractTestSchema(t)
	entityDict, _ := entityTemplate(schema.EntityList())

	tests := []struct { //nolint:govet // table-test fields grouped by meaning, not padding.
		name      string
		criteria  *modelv1.Criteria
		wantErr   bool
		wantedMsg string
	}{
		{name: "nil criteria is fine", criteria: nil},
		{name: "EQ on an index-rule-backed tag is fine", criteria: strCond("region", "us-west")},
		{name: "MATCH on an index-rule-backed tag is fine", criteria: matchCond("region", "us-west")},
		{name: "EQ on an entity tag with no rule is fine", criteria: strCond("service", "frontend")},
		{
			name: "MATCH on an entity tag with no rule errors", criteria: matchCond("service", "frontend"),
			wantErr: true, wantedMsg: "index rule is mandatory for match operation",
		},
		{
			name: "a tag with neither an index rule nor entity membership errors", criteria: strCond("unindexed", "x"),
			wantErr: true, wantedMsg: "mandatory index rule conf",
		},
		{
			name: "the invalid leaf is still rejected nested inside AND/OR",
			criteria: logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR,
				strCond("region", "us-west"),
				logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, strCond("service", "frontend"), matchCond("instance", "x"))),
			wantErr: true, wantedMsg: "index rule is mandatory for match operation",
		},
		{
			name: "a fully valid nested AND/OR tree passes",
			criteria: logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_OR,
				strCond("region", "us-west"),
				logicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND, strCond("service", "frontend"), matchCond("region", "x"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIndexModeCriteria(tt.criteria, schema, entityDict)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, tt.wantedMsg)
		})
	}
}

// TestMeasureFieldResolver_Field table-tests every resolution outcome NIDX-03
// §5 specifies: an index-rule-backed tag resolves to its 4-byte field key and
// analyzer; an index-mode entity tag with no rule resolves to its synthetic
// _im_entity_tag_<tag> field; anything else is unresolved.
func TestMeasureFieldResolver_Field(t *testing.T) {
	schema := entityExtractTestSchema(t)
	resolver := measureFieldResolver{schema: schema}

	tests := []struct {
		name         string
		tag          string
		wantField    string
		wantAnalyzer string
		wantOK       bool
	}{
		{name: "indexed tag resolves to rule field key + analyzer", tag: "region", wantField: index.FieldKey{IndexRuleID: 7}.Marshal(), wantAnalyzer: "keyword", wantOK: true},
		{name: "entity tag without a rule resolves to synthetic field", tag: "service", wantField: index.IndexModeEntityTagPrefix + "service", wantOK: true},
		{name: "second entity tag without a rule also resolves", tag: "instance", wantField: index.IndexModeEntityTagPrefix + "instance", wantOK: true},
		{name: "unindexed, non-entity tag is unresolved", tag: "unindexed", wantOK: false},
		{name: "unknown tag is unresolved", tag: "does-not-exist", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, analyzer, ok := resolver.Field(tt.tag)
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.Equal(t, tt.wantField, field)
				require.Equal(t, tt.wantAnalyzer, analyzer)
			}
		})
	}
}
