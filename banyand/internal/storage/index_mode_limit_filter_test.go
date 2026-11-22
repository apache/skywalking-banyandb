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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

type tagResolver struct{}

func (tagResolver) Field(tagName string) (string, string, bool) {
	return index.FieldKey{TagName: tagName}.Marshal(), "", true
}

func newTaggedIndexModeSegment(t *testing.T, count int) *seriesIndex {
	t.Helper()
	dir, fn := setUp(require.New(t))
	t.Cleanup(fn)
	si, err := newSeriesIndex(context.Background(), dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, si.Close()) })
	var docs index.Documents
	for i := 0; i < count; i++ {
		var series pbv1.Series
		series.Subject = "idx_measure"
		series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc-%05d", i)}}}}
		require.NoError(t, series.Marshal())
		subjectField := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, "idx_measure")
		subjectField.Index, subjectField.NoSort = true, true
		groupField := index.NewStringField(index.FieldKey{TagName: "bucket"}, fmt.Sprintf("t%d", i%100))
		groupField.Index, groupField.NoSort = true, true
		uniqueField := index.NewStringField(index.FieldKey{TagName: "unique"}, fmt.Sprintf("u%d", i))
		uniqueField.Index, uniqueField.NoSort = true, true
		docs = append(docs, index.Document{
			EntityValues: append([]byte(nil), series.Buffer...),
			Fields:       []index.Field{subjectField, groupField, uniqueField},
			Timestamp:    1000,
			Version:      1,
		})
	}
	require.NoError(t, si.Insert(docs))
	return si
}

func strCondition(name string, op modelv1.Condition_BinaryOp, value string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: name, Op: op, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	}}}
}

// searchReserved runs the search and returns the rows' service numbers and the allocation bytes it reserved.
func searchReserved(t *testing.T, si *seriesIndex, opts IndexSearchOpts) (services []int, reserved uint64) {
	t.Helper()
	var total atomic.Uint64
	ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error {
		total.Add(uint64(bytes))
		return nil
	}, func(int64) {})
	defer scope.Close()
	sd, _, err := si.SearchWithoutSeries(ctx, opts)
	require.NoError(t, err)
	for _, series := range sd.SeriesList {
		number, convErr := strconv.Atoi(strings.TrimPrefix(series.EntityValues[0].GetStr().GetValue(), "svc-"))
		require.NoError(t, convErr)
		services = append(services, number)
	}
	return services, total.Load()
}

// A selective criteria with a small limit must narrow the candidates at posting level, before any document is decoded:
// it returns the same rows as the unlimited search, and reserves a small share of what listing every candidate does.
func TestSeriesIndex_IndexModeLimitWithSelectiveCriteria(t *testing.T) {
	const count = 6000
	si := newTaggedIndexModeSegment(t, count)
	notShadowed := func(context.Context, [][]byte) (map[string]struct{}, error) { return nil, nil }
	base := IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: limitTestAllTime(), Fields: tagResolver{}}

	everything := base
	_, reservedEverything := searchReserved(t, si, everything)

	for name, tc := range map[string]struct {
		criteria *modelv1.Criteria
		matches  func(number int) bool
		want     int
	}{
		"unique value": {
			criteria: strCondition("unique", modelv1.Condition_BINARY_OP_EQ, "u5999"),
			matches:  func(number int) bool { return number == 5999 }, want: 1,
		},
		"bucket value": {
			criteria: strCondition("bucket", modelv1.Condition_BINARY_OP_EQ, "t7"),
			matches:  func(number int) bool { return number%100 == 7 }, want: 5,
		},
		"or of exact values": {
			criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
				Op:    modelv1.LogicalExpression_LOGICAL_OP_OR,
				Left:  strCondition("unique", modelv1.Condition_BINARY_OP_EQ, "u5999"),
				Right: strCondition("unique", modelv1.Condition_BINARY_OP_EQ, "u17"),
			}}},
			matches: func(number int) bool { return number == 5999 || number == 17 }, want: 2,
		},
		"not in a narrow set": {
			criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
				Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
				Left:  strCondition("bucket", modelv1.Condition_BINARY_OP_EQ, "t7"),
				Right: strCondition("bucket", modelv1.Condition_BINARY_OP_NE, "t8"),
			}}},
			matches: func(number int) bool { return number%100 == 7 }, want: 5,
		},
		"and of exact and negation": {
			criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
				Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
				Left:  strCondition("bucket", modelv1.Condition_BINARY_OP_EQ, "t7"),
				Right: strCondition("unique", modelv1.Condition_BINARY_OP_NE, "u7"),
			}}},
			matches: func(number int) bool { return number%100 == 7 && number != 7 }, want: 5,
		},
	} {
		opts := base
		opts.Criteria, opts.Limit, opts.IsShadowed = tc.criteria, 5, notShadowed
		services, reserved := searchReserved(t, si, opts)
		require.Len(t, services, tc.want, name)
		for _, number := range services {
			require.True(t, tc.matches(number), "%s returned svc-%05d, which the criteria does not match", name, number)
		}
		require.Less(t, reserved*10, reservedEverything, "%s reserved %d bytes against %d for listing every candidate", name, reserved, reservedEverything)
	}

	// A range expands the dictionary terms it spans, so it is not as cheap as an exact value, but it still decodes only the
	// documents it keeps: it returns the right rows and reserves less than listing every candidate.
	rangeOpts := base
	rangeOpts.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
		Left:  strCondition("unique", modelv1.Condition_BINARY_OP_GE, "u1"),
		Right: strCondition("unique", modelv1.Condition_BINARY_OP_LE, "u1"),
	}}}
	rangeOpts.Limit, rangeOpts.IsShadowed = 5, notShadowed
	rangeServices, rangeReserved := searchReserved(t, si, rangeOpts)
	require.Equal(t, []int{1}, rangeServices)
	require.Less(t, rangeReserved, reservedEverything)

	// An OR narrows at posting level too, and still returns every match.
	opts := base
	opts.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op:    modelv1.LogicalExpression_LOGICAL_OP_OR,
		Left:  strCondition("bucket", modelv1.Condition_BINARY_OP_EQ, "t7"),
		Right: strCondition("bucket", modelv1.Condition_BINARY_OP_EQ, "t8"),
	}}}
	opts.Limit, opts.IsShadowed = 1000, notShadowed
	services, reserved := searchReserved(t, si, opts)
	require.Len(t, services, 120)
	for _, number := range services {
		require.Contains(t, []int{7, 8}, number%100)
	}
	require.Less(t, reserved*10, reservedEverything, "an OR reserved %d bytes against %d for listing every candidate", reserved, reservedEverything)
}

// ladderValue is the sort value of the i-th document of newLadderIndexModeSegment.
func ladderValue(i int) int64 { return int64((i * 7919) % 100003) }

// newLadderIndexModeSegment writes an index-mode segment whose documents carry an indexed "bucket" (i%100) and an int sort
// value with the sort ladder, the way the measure writer writes a scalar int index-rule tag.
func newLadderIndexModeSegment(t *testing.T, count int) *seriesIndex {
	t.Helper()
	dir, fn := setUp(require.New(t))
	t.Cleanup(fn)
	si, err := newSeriesIndex(context.Background(), dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, si.Close()) })
	var docs index.Documents
	for i := 0; i < count; i++ {
		var series pbv1.Series
		series.Subject = "idx_measure"
		series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc-%05d", i)}}}}
		require.NoError(t, series.Marshal())
		subjectField := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, "idx_measure")
		subjectField.Index, subjectField.NoSort = true, true
		bucket := index.NewStringField(index.FieldKey{TagName: "bucket"}, fmt.Sprintf("t%d", i%100))
		bucket.Index, bucket.NoSort = true, true
		sortField := index.NewBytesField(index.FieldKey{IndexRuleID: orderRuleID}, convert.Int64ToBytes(ladderValue(i)))
		sortField.Store, sortField.Index = true, true
		sortField.Numeric, sortField.NumericKey = true, ladderValue(i)
		docs = append(docs, index.Document{
			EntityValues: append([]byte(nil), series.Buffer...), Fields: []index.Field{subjectField, bucket, sortField},
			Timestamp: 1000, Version: 1,
		})
	}
	require.NoError(t, si.Insert(docs))
	return si
}

// An index-ordered limit with a selective criteria must also narrow the candidates at posting level: it returns the
// first rows of the order among the rows the criteria keeps, and reserves a small share of what ranking everything does.
func TestSeriesIndex_IndexModeOrderedLimitWithSelectiveCriteria(t *testing.T) {
	const count = 6000
	si := newLadderIndexModeSegment(t, count)
	valueOf := ladderValue
	notShadowed := func(context.Context, [][]byte) (map[string]struct{}, error) { return nil, nil }
	base := IndexSearchOpts{
		IndexModeSubject: "idx_measure", TimeRange: limitTestAllTime(), Fields: tagResolver{},
		Order: &index.OrderBy{Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_ASC, Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: orderRuleID}}},
	}
	everything := base
	_, reservedEverything := searchReserved(t, si, everything)

	for _, desc := range []bool{false, true} {
		var want []int
		for i := 0; i < count; i++ {
			if i%100 == 7 {
				want = append(want, i)
			}
		}
		sort.SliceStable(want, func(a, b int) bool {
			if valueOf(want[a]) != valueOf(want[b]) {
				return (valueOf(want[a]) < valueOf(want[b])) != desc
			}
			return want[a] < want[b]
		})
		opts := base
		opts.Order = &index.OrderBy{Type: base.Order.Type, Sort: modelv1.Sort_SORT_ASC, Index: base.Order.Index}
		if desc {
			opts.Order.Sort = modelv1.Sort_SORT_DESC
		}
		opts.Criteria = strCondition("bucket", modelv1.Condition_BINARY_OP_EQ, "t7")
		opts.Limit, opts.IsShadowed = 5, notShadowed
		services, reserved := searchReserved(t, si, opts)
		require.Len(t, services, 5, "desc=%v", desc)
		for rank, number := range services {
			require.Equal(t, valueOf(want[rank]), valueOf(number), "desc=%v rank %d", desc, rank)
			require.Equal(t, 7, number%100)
		}
		require.Less(t, reserved*10, reservedEverything, "desc=%v reserved %d bytes against %d for ranking every candidate", desc, reserved, reservedEverything)
	}
}

// A limit beyond the largest prefix the descent is asked for must not make a search list or rank every candidate: the
// unordered search grows its prefix without a cap, and an ordered search whose first chunk would pass the cap clamps it
// instead of skipping the descent. The cap is shrunk so that a small segment shows the cliff.
func TestSeriesIndex_IndexModeLargeLimitStaysProportionalToTheLimit(t *testing.T) {
	const count, fetchCap = 40000, 2048
	defer func(previous int) { topFetchCap = previous }(topFetchCap)
	topFetchCap = fetchCap
	si := newLadderIndexModeSegment(t, count)
	notShadowed := func(context.Context, [][]byte) (map[string]struct{}, error) { return nil, nil }
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: orderRuleID}}
	for _, tc := range []struct {
		order *index.OrderBy
		name  string
		limit int
	}{
		{name: "unordered, limit past the cap", order: nil, limit: 4 * fetchCap},
		{
			name:  "ordered, first chunk would pass the cap",
			order: &index.OrderBy{Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_ASC, Index: rule}, limit: fetchCap / 2,
		},
		{
			name:  "ordered desc, first chunk would pass the cap",
			order: &index.OrderBy{Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_DESC, Index: rule}, limit: fetchCap / 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := IndexSearchOpts{IndexModeSubject: "idx_measure", TimeRange: limitTestAllTime(), Fields: tagResolver{}, Order: tc.order}
			_, everything := searchReserved(t, si, base)
			limited := base
			limited.Limit, limited.IsShadowed = tc.limit, notShadowed
			services, reserved := searchReserved(t, si, limited)
			require.Len(t, services, tc.limit)
			t.Logf("limit %d of %d reserved %d vs %d (%.0f%%)", tc.limit, count, reserved, everything, 100*float64(reserved)/float64(everything))
			require.Less(t, reserved*10, everything*3, "a limit of %d of %d documents reserved %d bytes against %d for all of them", tc.limit, count, reserved, everything)
			if tc.order != nil {
				desc := tc.order.Sort == modelv1.Sort_SORT_DESC
				for i := 1; i < len(services); i++ {
					if desc {
						require.GreaterOrEqual(t, ladderValue(services[i-1]), ladderValue(services[i]), "row %d is out of order", i)
					} else {
						require.LessOrEqual(t, ladderValue(services[i-1]), ladderValue(services[i]), "row %d is out of order", i)
					}
				}
			}
		})
	}
}
