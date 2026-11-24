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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func bucketCondition(op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: "bucket", Op: op, Value: value}}}
}

// randomBucketCriteria builds a criteria of at most depth levels over the "bucket" tag (t0..t99), with every comparison the
// index understands joined by AND and OR into trees of any shape.
func randomBucketCriteria(rng *rand.Rand, depth int) *modelv1.Criteria {
	if depth > 0 && rng.Intn(10) < 6 {
		op := modelv1.LogicalExpression_LOGICAL_OP_AND
		if rng.Intn(2) == 0 {
			op = modelv1.LogicalExpression_LOGICAL_OP_OR
		}
		return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
			Op: op, Left: randomBucketCriteria(rng, depth-1), Right: randomBucketCriteria(rng, depth-1),
		}}}
	}
	bucket := func() string { return fmt.Sprintf("t%d", rng.Intn(100)) }
	buckets := func() *modelv1.TagValue {
		values := make([]string, 1+rng.Intn(4))
		for i := range values {
			values[i] = bucket()
		}
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
	}
	one := func() *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: bucket()}}}
	}
	switch rng.Intn(8) {
	case 0:
		return bucketCondition(modelv1.Condition_BINARY_OP_EQ, one())
	case 1:
		return bucketCondition(modelv1.Condition_BINARY_OP_NE, one())
	case 2:
		return bucketCondition(modelv1.Condition_BINARY_OP_IN, buckets())
	case 3:
		return bucketCondition(modelv1.Condition_BINARY_OP_NOT_IN, buckets())
	case 4:
		return bucketCondition(modelv1.Condition_BINARY_OP_GT, one())
	case 5:
		return bucketCondition(modelv1.Condition_BINARY_OP_GE, one())
	case 6:
		return bucketCondition(modelv1.Condition_BINARY_OP_LT, one())
	default:
		return bucketCondition(modelv1.Condition_BINARY_OP_LE, one())
	}
}

// A limited search must give what the unlimited search gives, cut to the limit: the limited path narrows its candidates on
// postings and lists them a prefix at a time, and the unlimited path never uses either. With an order the rows are the first
// ones of the unlimited order, with none they are any rows of the unlimited result, as many as the limit allows.
func TestSeriesIndex_LimitedSearchMatchesFullSearchOnRandomCriteria(t *testing.T) {
	const count = 1200
	si := newLadderIndexModeSegment(t, count)
	notShadowed := func(context.Context, [][]byte) (map[string]struct{}, error) { return nil, nil }
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: orderRuleID}}
	for name, order := range map[string]*index.OrderBy{
		"unordered":        nil,
		"index ascending":  {Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_ASC, Index: rule},
		"index descending": {Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_DESC, Index: rule},
	} {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test data
			for round := 0; round < 60; round++ {
				criteria := randomBucketCriteria(rng, 3)
				limit := []int{1, 5, 50, 400, 2000}[rng.Intn(5)]
				base := IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: limitTestAllTime(), Fields: tagResolver{}, Order: order, Criteria: criteria}
				full, _ := searchReserved(t, si, base)
				limited := base
				limited.Limit, limited.IsShadowed = limit, notShadowed
				got, _ := searchReserved(t, si, limited)
				label := fmt.Sprintf("round %d, limit %d", round, limit)
				require.Len(t, got, min(limit, len(full)), label)
				if order == nil {
					require.Subset(t, full, got, label)
					continue
				}
				require.Equal(t, full[:len(got)], got, "%s: the first rows of the unlimited order", label)
			}
		})
	}
}

// The classical limit cases, enumerated: a result of no document, of some and of all of them, cut at a limit of one, one less
// than the result, exactly the result, one more and far more, for each order. The boundaries are where a prefix listing, a
// growing chunk and the truncation flag go wrong, and a random limit seldom lands on them.
func TestSeriesIndex_LimitBoundariesOnCanonicalCriteria(t *testing.T) {
	const count = 400 // 100 buckets of 4 documents
	si := newLadderIndexModeSegment(t, count)
	notShadowed := func(context.Context, [][]byte) (map[string]struct{}, error) { return nil, nil }
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: orderRuleID}}
	str := func(value string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
	}
	list := func(values ...string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
	}
	join := func(op modelv1.LogicalExpression_LogicalOp, left, right *modelv1.Criteria) *modelv1.Criteria {
		return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: op, Left: left, Right: right}}}
	}
	for _, criteria := range []struct {
		c    *modelv1.Criteria
		name string
		rows int
	}{
		{name: "no criteria: every document", c: nil, rows: count},
		{name: "a value no document has: none", c: bucketCondition(modelv1.Condition_BINARY_OP_EQ, str("zz")), rows: 0},
		{name: "a negation of that value: every document", c: bucketCondition(modelv1.Condition_BINARY_OP_NE, str("zz")), rows: count},
		{name: "one bucket", c: bucketCondition(modelv1.Condition_BINARY_OP_EQ, str("t7")), rows: 4},
		{name: "three buckets", c: bucketCondition(modelv1.Condition_BINARY_OP_IN, list("t1", "t2", "t3")), rows: 12},
		{name: "everything but three buckets", c: bucketCondition(modelv1.Condition_BINARY_OP_NOT_IN, list("t1", "t2", "t3")), rows: count - 12},
		{
			name: "two buckets joined by OR",
			c: join(modelv1.LogicalExpression_LOGICAL_OP_OR,
				bucketCondition(modelv1.Condition_BINARY_OP_EQ, str("t7")), bucketCondition(modelv1.Condition_BINARY_OP_EQ, str("t8"))),
			rows: 8,
		},
		{
			name: "two bounds that meet at one bucket",
			c: join(modelv1.LogicalExpression_LOGICAL_OP_AND,
				bucketCondition(modelv1.Condition_BINARY_OP_GE, str("t5")), bucketCondition(modelv1.Condition_BINARY_OP_LE, str("t5"))),
			rows: 4,
		},
		{
			name: "two bounds with nothing between them",
			c: join(modelv1.LogicalExpression_LOGICAL_OP_AND,
				bucketCondition(modelv1.Condition_BINARY_OP_GE, str("t9")), bucketCondition(modelv1.Condition_BINARY_OP_LE, str("t1"))),
			rows: 0,
		},
	} {
		for orderName, order := range map[string]*index.OrderBy{
			"unordered":        nil,
			"index ascending":  {Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_ASC, Index: rule},
			"index descending": {Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_DESC, Index: rule},
		} {
			t.Run(criteria.name+"/"+orderName, func(t *testing.T) {
				base := IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: limitTestAllTime(), Fields: tagResolver{}, Order: order, Criteria: criteria.c}
				full, _ := searchReserved(t, si, base)
				require.Len(t, full, criteria.rows, "the criteria must select the documents the case says it does")
				// A newer segment that holds every even document shadows them: the rows the search may return are the odd ones,
				// and a chunk that held shadowed rows falls short and has to grow, which a search without shadowing never does.
				var unshadowed []int
				for _, service := range full {
					if service%2 == 1 {
						unshadowed = append(unshadowed, service)
					}
				}
				for _, shadowing := range []struct {
					shadow func(context.Context, [][]byte) (map[string]struct{}, error)
					name   string
					want   []int
				}{
					{name: "no shadowing", shadow: notShadowed, want: full},
					{name: "even documents shadowed", shadow: shadowEvenServices, want: unshadowed},
				} {
					for _, limit := range []int{1, len(shadowing.want) - 1, len(shadowing.want), len(shadowing.want) + 1, 10 * count} {
						if limit < 1 {
							continue
						}
						limited := base
						limited.Limit, limited.IsShadowed = limit, shadowing.shadow
						got, _ := searchReserved(t, si, limited)
						require.Len(t, got, min(limit, len(shadowing.want)), "%s, limit %d", shadowing.name, limit)
						if order == nil {
							require.Subset(t, shadowing.want, got, "%s, limit %d", shadowing.name, limit)
							continue
						}
						require.Equal(t, shadowing.want[:len(got)], got, "%s, limit %d: the first rows of the unlimited order", shadowing.name, limit)
					}
				}
			})
		}
	}
}

// shadowEvenServices is the check a newer segment answers for the documents whose service number is even: the identifier of a
// document holds the service as "svc-" and five digits.
func shadowEvenServices(_ context.Context, identifiers [][]byte) (map[string]struct{}, error) {
	shadowed := make(map[string]struct{})
	for _, identifier := range identifiers {
		at := bytes.Index(identifier, []byte("svc-"))
		if at < 0 || at+9 > len(identifier) {
			continue
		}
		if (identifier[at+8]-'0')%2 == 0 {
			shadowed[string(identifier)] = struct{}{}
		}
	}
	return shadowed, nil
}
