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

package bydbql_test

import (
	"context"
	"time"

	"github.com/alecthomas/participle/v2/lexer"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// equivalenceCase describes a parameterized query that must be indistinguishable,
// both at the AST level and at the native query request level, from the same
// query written with literal values.
type equivalenceCase struct {
	name          string
	parameterized string
	literal       string
	params        []*modelv1.TagValue
	// ignoreTimeRange skips the time_range comparison for queries whose range
	// depends on the wall clock at transform time (relative times, open-ended comparators).
	ignoreTimeRange bool
	// expectError marks cases whose transform must fail on BOTH sides with the
	// same message; without it a case reaching the error branch fails the test,
	// so no case can pass vacuously.
	expectError bool
}

func equivalenceStream() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "sw", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{
				{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "message", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "tags", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "codes", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
				{Name: "created_at", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			},
		}},
	}
}

func equivalenceCases() []equivalenceCase {
	base := "SELECT * FROM STREAM sw IN default "
	return []equivalenceCase{
		// TIME clause: every accepted parameter type
		{
			name:            "str relative time in TIME >",
			parameterized:   base + "TIME > ?",
			literal:         base + "TIME > '-30m'",
			params:          []*modelv1.TagValue{strParam("-30m")},
			ignoreTimeRange: true,
		},
		{
			name:          "str absolute times in TIME BETWEEN",
			parameterized: base + "TIME BETWEEN ? AND ?",
			literal:       base + "TIME BETWEEN '2026-07-06T10:00:00Z' AND '2026-07-06T11:00:00Z'",
			params:        []*modelv1.TagValue{strParam("2026-07-06T10:00:00Z"), strParam("2026-07-06T11:00:00Z")},
		},
		{
			name:          "timestamp params in TIME BETWEEN",
			parameterized: base + "TIME BETWEEN ? AND ?",
			literal:       base + "TIME BETWEEN '2026-07-06T10:00:00Z' AND '2026-07-06T11:00:00Z'",
			params: []*modelv1.TagValue{
				timestampParam(time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)),
				timestampParam(time.Date(2026, 7, 6, 11, 0, 0, 0, time.UTC)),
			},
		},
		// Scalar comparisons: every operator, every accepted parameter type
		{
			name:          "str param in =",
			parameterized: base + "WHERE service_id = ?",
			literal:       base + "WHERE service_id = 'webapp'",
			params:        []*modelv1.TagValue{strParam("webapp")},
		},
		{
			name:          "str param in !=",
			parameterized: base + "WHERE service_id != ?",
			literal:       base + "WHERE service_id != 'webapp'",
			params:        []*modelv1.TagValue{strParam("webapp")},
		},
		{
			name:          "int param in >",
			parameterized: base + "WHERE duration > ?",
			literal:       base + "WHERE duration > 100",
			params:        []*modelv1.TagValue{intParam(100)},
		},
		{
			name:          "int param in >=",
			parameterized: base + "WHERE duration >= ?",
			literal:       base + "WHERE duration >= 100",
			params:        []*modelv1.TagValue{intParam(100)},
		},
		{
			name:          "int param in <",
			parameterized: base + "WHERE duration < ?",
			literal:       base + "WHERE duration < 100",
			params:        []*modelv1.TagValue{intParam(100)},
		},
		{
			name:          "int param in <=",
			parameterized: base + "WHERE duration <= ?",
			literal:       base + "WHERE duration <= 100",
			params:        []*modelv1.TagValue{intParam(100)},
		},
		{
			name:          "null param in =",
			parameterized: base + "WHERE service_id = ?",
			literal:       base + "WHERE service_id = NULL",
			params:        []*modelv1.TagValue{nullParam()},
		},
		// IN predicate
		{
			name:          "two str params in IN",
			parameterized: base + "WHERE service_id IN (?, ?)",
			literal:       base + "WHERE service_id IN ('a', 'b')",
			params:        []*modelv1.TagValue{strParam("a"), strParam("b")},
		},
		{
			name:          "str_array param expands in IN",
			parameterized: base + "WHERE service_id IN (?)",
			literal:       base + "WHERE service_id IN ('a', 'b')",
			params:        []*modelv1.TagValue{strArrayParam("a", "b")},
		},
		{
			name:          "int_array param expands in IN",
			parameterized: base + "WHERE duration IN (?)",
			literal:       base + "WHERE duration IN (100, 200)",
			params:        []*modelv1.TagValue{intArrayParam(100, 200)},
		},
		{
			name:          "str_array param expands between literals in IN",
			parameterized: base + "WHERE service_id IN ('a', ?, 'd')",
			literal:       base + "WHERE service_id IN ('a', 'b', 'c', 'd')",
			params:        []*modelv1.TagValue{strArrayParam("b", "c")},
		},
		{
			name:          "two array params and a scalar param expand in one IN list",
			parameterized: base + "WHERE service_id IN (?, ?, ?)",
			literal:       base + "WHERE service_id IN ('a', 'b', 'c', 'd')",
			params:        []*modelv1.TagValue{strArrayParam("a", "b"), strParam("c"), strArrayParam("d")},
		},
		{
			name:          "array params expand in two lists of one query",
			parameterized: base + "WHERE service_id IN (?) AND codes HAVING (?)",
			literal:       base + "WHERE service_id IN ('a', 'b') AND codes HAVING (200, 500)",
			params:        []*modelv1.TagValue{strArrayParam("a", "b"), intArrayParam(200, 500)},
		},
		{
			name:          "str param in NOT IN",
			parameterized: base + "WHERE service_id NOT IN (?)",
			literal:       base + "WHERE service_id NOT IN ('a')",
			params:        []*modelv1.TagValue{strParam("a")},
		},
		// MATCH predicate
		{
			name:          "str param in MATCH",
			parameterized: base + "WHERE message MATCH(?)",
			literal:       base + "WHERE message MATCH('error')",
			params:        []*modelv1.TagValue{strParam("error")},
		},
		{
			name:          "str_array param expands in MATCH",
			parameterized: base + "WHERE message MATCH(?)",
			literal:       base + "WHERE message MATCH(('error', 'warn'))",
			params:        []*modelv1.TagValue{strArrayParam("error", "warn")},
		},
		{
			name:          "str param in MATCH keeps literal analyzer",
			parameterized: base + "WHERE message MATCH(?, 'simple')",
			literal:       base + "WHERE message MATCH('error', 'simple')",
			params:        []*modelv1.TagValue{strParam("error")},
		},
		// HAVING predicate
		{
			name:          "str param in HAVING",
			parameterized: base + "WHERE tags HAVING ?",
			literal:       base + "WHERE tags HAVING 'blue'",
			params:        []*modelv1.TagValue{strParam("blue")},
		},
		{
			name:          "int_array param expands in HAVING",
			parameterized: base + "WHERE codes HAVING (?)",
			literal:       base + "WHERE codes HAVING (200, 500)",
			params:        []*modelv1.TagValue{intArrayParam(200, 500)},
		},
		{
			name:          "str_array param expands in NOT HAVING",
			parameterized: base + "WHERE tags NOT HAVING (?)",
			literal:       base + "WHERE tags NOT HAVING ('red', 'green')",
			params:        []*modelv1.TagValue{strArrayParam("red", "green")},
		},
		// Single value vs list form: the bound container must keep the exact
		// form a literal query would have, because the transformer emits a
		// scalar condition for the single form and an array condition for the
		// list form.
		{
			name:          "one-element array in a parenthesized HAVING list stays a list",
			parameterized: base + "WHERE codes HAVING (?)",
			literal:       base + "WHERE codes HAVING (200)",
			params:        []*modelv1.TagValue{intArrayParam(200)},
		},
		{
			name:          "one-element array in a single HAVING value stays a single value",
			parameterized: base + "WHERE tags HAVING ?",
			literal:       base + "WHERE tags HAVING 'blue'",
			params:        []*modelv1.TagValue{strArrayParam("blue")},
		},
		{
			name:          "multi-element array in a single HAVING value becomes a list",
			parameterized: base + "WHERE tags HAVING ?",
			literal:       base + "WHERE tags HAVING ('a', 'b')",
			params:        []*modelv1.TagValue{strArrayParam("a", "b")},
		},
		{
			name:          "scalar in a parenthesized MATCH list stays a list",
			parameterized: base + "WHERE message MATCH((?))",
			literal:       base + "WHERE message MATCH(('error'))",
			params:        []*modelv1.TagValue{strParam("error")},
		},
		// Parameter type vs tag schema type: mismatches must behave exactly like
		// the same mismatch written as a literal — lenient conversion where the
		// transformer converts, identical errors where it rejects.
		{
			name:          "int param on a STRING tag converts like an int literal",
			parameterized: base + "WHERE service_id = ?",
			literal:       base + "WHERE service_id = 123",
			params:        []*modelv1.TagValue{intParam(123)},
		},
		{
			name:          "numeric str param on an INT tag parses like a str literal",
			parameterized: base + "WHERE duration > ?",
			literal:       base + "WHERE duration > '100'",
			params:        []*modelv1.TagValue{strParam("100")},
		},
		{
			name:          "non-numeric str param on an INT tag fails like a str literal",
			parameterized: base + "WHERE duration > ?",
			literal:       base + "WHERE duration > 'abc'",
			params:        []*modelv1.TagValue{strParam("abc")},
			expectError:   true,
		},
		{
			name:          "non-numeric str_array param on an INT_ARRAY tag fails like literals",
			parameterized: base + "WHERE codes HAVING (?)",
			literal:       base + "WHERE codes HAVING ('a', 'b')",
			params:        []*modelv1.TagValue{strArrayParam("a", "b")},
			expectError:   true,
		},
		{
			name:          "int_array param on a STRING_ARRAY tag converts like int literals",
			parameterized: base + "WHERE tags HAVING (?)",
			literal:       base + "WHERE tags HAVING (1, 2)",
			params:        []*modelv1.TagValue{intArrayParam(1, 2)},
		},
		{
			name:          "param on a TIMESTAMP tag is rejected like a literal",
			parameterized: base + "WHERE created_at = ?",
			literal:       base + "WHERE created_at = '2026-07-06T10:00:00Z'",
			params:        []*modelv1.TagValue{strParam("2026-07-06T10:00:00Z")},
			expectError:   true,
		},
		{
			name:          "param on an unknown tag fails like a literal",
			parameterized: base + "WHERE nonexistent = ?",
			literal:       base + "WHERE nonexistent = 'x'",
			params:        []*modelv1.TagValue{strParam("x")},
			expectError:   true,
		},
		// Combined clauses and nested expressions
		{
			name:            "params across TIME and nested OR/AND expressions",
			parameterized:   base + "TIME > ? WHERE (service_id = ? OR duration > ?) AND message MATCH(?)",
			literal:         base + "TIME > '-15m' WHERE (service_id = 'svc' OR duration > 42) AND message MATCH('boom')",
			params:          []*modelv1.TagValue{strParam("-15m"), strParam("svc"), intParam(42), strParam("boom")},
			ignoreTimeRange: true,
		},
		// Integer-only positions: LIMIT / OFFSET / TOP N counts
		{
			name:          "int params in LIMIT and OFFSET",
			parameterized: base + "WHERE service_id = 'svc' LIMIT ? OFFSET ?",
			literal:       base + "WHERE service_id = 'svc' LIMIT 10 OFFSET 5",
			params:        []*modelv1.TagValue{intParam(10), intParam(5)},
		},
		{
			name:          "int param in SELECT TOP N projection count",
			parameterized: "SELECT TOP ? value, service FROM MEASURE svc_metrics IN default",
			literal:       "SELECT TOP 3 value, service FROM MEASURE svc_metrics IN default",
			params:        []*modelv1.TagValue{intParam(3)},
		},
		// SHOW TOP N statement
		{
			name:            "params in SHOW TOP N count, TIME, and WHERE",
			parameterized:   "SHOW TOP ? FROM MEASURE svc_topn IN default TIME > ? WHERE service = ?",
			literal:         "SHOW TOP 5 FROM MEASURE svc_topn IN default TIME > '-1h' WHERE service = 'svc'",
			params:          []*modelv1.TagValue{intParam(5), strParam("-1h"), strParam("svc")},
			ignoreTimeRange: true,
		},
	}
}

var _ = Describe("BindParams equivalence with literal queries", func() {
	var transformer *Transformer

	BeforeEach(func() {
		ctrl := gomock.NewController(GinkgoT())
		streamRegistry := schema.NewMockStream(ctrl)
		streamRegistry.EXPECT().GetStream(gomock.Any(), gomock.Any()).Return(equivalenceStream(), nil).AnyTimes()
		topNRegistry := schema.NewMockTopNAggregation(ctrl)
		topNRegistry.EXPECT().GetTopNAggregation(gomock.Any(), gomock.Any()).Return(&databasev1.TopNAggregation{
			Metadata:      &commonv1.Metadata{Name: "svc_topn", Group: "default"},
			SourceMeasure: &commonv1.Metadata{Name: "svc_metrics", Group: "default"},
		}, nil).AnyTimes()
		measureRegistry := schema.NewMockMeasure(ctrl)
		measureRegistry.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).Return(&databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "svc_metrics", Group: "default"},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: "default",
				Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}},
			}},
			Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		}, nil).AnyTimes()
		mockRepo := metadata.NewMockRepo(ctrl)
		mockRepo.EXPECT().StreamRegistry().AnyTimes().Return(streamRegistry)
		mockRepo.EXPECT().TopNAggregationRegistry().AnyTimes().Return(topNRegistry)
		mockRepo.EXPECT().MeasureRegistry().AnyTimes().Return(measureRegistry)
		transformer = NewTransformer(mockRepo)
	})

	Describe("literal count wrap guard", func() {
		// The bound path rejects out-of-range counts at bind time; the literal
		// path must be equally strict instead of wrapping in the uint32 cast.
		It("accepts the uint32 maximum as a literal LIMIT", func() {
			// Regression for the pre-existing max-limit integration case:
			// LIMIT is uint32 on the wire, so 4294967295 is a legal literal.
			grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default LIMIT 4294967295")
			Expect(err).To(BeNil())
			Expect(BindParams(grammar, nil)).To(Succeed())
			result, err := transformer.Transform(context.Background(), grammar)
			Expect(err).To(BeNil())
			Expect(result).NotTo(BeNil())
		})

		It("rejects a negative literal LIMIT", func() {
			grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default LIMIT -5")
			Expect(err).To(BeNil())
			Expect(BindParams(grammar, nil)).To(Succeed())
			_, err = transformer.Transform(context.Background(), grammar)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})

		It("rejects an int32-overflowing literal SHOW TOP count", func() {
			grammar, err := ParseQuery("SHOW TOP 3000000000 FROM MEASURE svc_topn IN default")
			Expect(err).To(BeNil())
			Expect(BindParams(grammar, nil)).To(Succeed())
			_, err = transformer.Transform(context.Background(), grammar)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})
	})

	for _, testCase := range equivalenceCases() {
		It(testCase.name, func() {
			boundGrammar, err := ParseQuery(testCase.parameterized)
			Expect(err).To(BeNil())
			Expect(BindParams(boundGrammar, testCase.params)).To(Succeed())
			literalGrammar, err := ParseQuery(testCase.literal)
			Expect(err).To(BeNil())
			Expect(BindParams(literalGrammar, nil)).To(Succeed())

			// The bound AST must be indistinguishable from the literal AST
			astDiff := cmp.Diff(literalGrammar, boundGrammar, cmpopts.IgnoreTypes(lexer.Position{}), cmpopts.IgnoreUnexported(Grammar{}))
			Expect(astDiff).To(BeEmpty(), "AST mismatch:\n%s", astDiff)

			// Both must transform to the same native query request
			ctx := context.Background()
			literalResult, literalErr := transformer.Transform(ctx, literalGrammar)
			boundResult, boundErr := transformer.Transform(ctx, boundGrammar)
			if literalErr != nil {
				Expect(testCase.expectError).To(BeTrue(), "unexpected transform error: %v", literalErr)
				Expect(boundErr).To(HaveOccurred())
				Expect(boundErr.Error()).To(Equal(literalErr.Error()))
				return
			}
			Expect(testCase.expectError).To(BeFalse(), "expected a transform error but both sides succeeded")
			Expect(boundErr).To(BeNil())
			Expect(boundResult.Type).To(Equal(literalResult.Type))
			// SELECT * projections are built from a map, so their tag order is
			// nondeterministic per Transform call; sort them before comparing.
			opts := []cmp.Option{
				protocmp.Transform(),
				protocmp.SortRepeatedFields(&modelv1.TagProjection_TagFamily{}, "tags"),
			}
			if testCase.ignoreTimeRange {
				opts = append(opts,
					protocmp.IgnoreFields(&streamv1.QueryRequest{}, "time_range"),
					protocmp.IgnoreFields(&measurev1.TopNRequest{}, "time_range"))
			}
			requestDiff := cmp.Diff(literalResult.QueryRequest, boundResult.QueryRequest, opts...)
			Expect(requestDiff).To(BeEmpty(), "query request mismatch:\n%s", requestDiff)
		})
	}
})
