// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

// The criteria band and the per-node cap the pushdown case turns on.
// buildIndexOrderStream gives element j of a series the sort term
// value(indexOrderTSCount-j), so the band below sits in the MIDDLE of the index
// order and rejects its head in BOTH sort directions. topnMergeCap is the
// maxElementSize a limit+offset request produces, and it is smaller than the
// number of rows the band rejects ahead of its first survivor — which is what
// makes the filter's position relative to the merge observable.
const (
	topnBandLow  = "value004"
	topnBandHigh = "value009"
	topnLimit    = 3
	topnOffset   = 1
	topnMergeCap = topnLimit + topnOffset
)

// topnBandFilter builds `filter-tag >= value004 AND filter-tag <= value009`, the
// criteria the scan hands the merge as a pre-merge fusible.
func topnBandFilter(t *testing.T) logical.TagFilter {
	t.Helper()
	bound := func(op modelv1.Condition_BinaryOp, value string) *modelv1.Criteria {
		return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name:  "filter-tag",
			Op:    op,
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
		}}}
	}
	filter, err := logical.BuildSimpleTagFilter(&modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
		Left:  bound(modelv1.Condition_BINARY_OP_GE, topnBandLow),
		Right: bound(modelv1.Condition_BINARY_OP_LE, topnBandHigh),
	}}})
	require.NoError(t, err)
	return filter
}

// topnFilterRegistry numbers the criteria tag onto the (family, tag) coordinates
// fullProjection defines, as the projected schema a *tagFilterPlan carries does.
func topnFilterRegistry() logical.TagSpecMap {
	registry := logical.TagSpecMap{}
	registry.RegisterTagFamilies([]*databasev1.TagFamilySpec{{
		Name: "benchmark-family",
		Tags: []*databasev1.TagSpec{
			{Name: "entity-tag", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "filter-tag", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}})
	return registry
}

// runTopnFilteredScan drives the vec scan through the pipeline
// localIndexScan.ExecuteVectorized composes for a CRITERIA index-order query: the
// tag filter as a pre-merge fusible, a merge capped at maxElementSize, then
// distinct and a defensive limit of maxElementSize at offset 0. A nil filter gives
// the pre-pushdown shape — the same cap with the criteria left at the egress —
// which the control arm uses.
func runTopnFilteredScan(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions,
	filter logical.TagFilter, desc bool,
) []*streamv1.Element {
	t.Helper()
	src, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	require.NotNil(t, src)
	schema := src.Schema()
	var preMerge []vectorized.FusibleOperator
	if filter != nil {
		preMerge = append(preMerge, vstream.NewTagFilter(schema, sqo.TagProjection, filter, topnFilterRegistry()))
	}
	pipeline, buildErr := vstream.BuildStreamMergePipeline(
		&testVecSource{src: src, schema: schema}, schema, desc, 0, uint32(sqo.MaxElementSize),
		vstream.DefaultConfig().BatchSize, sqo.MaxElementSize, preMerge...)
	require.NoError(t, buildErr)
	require.NoError(t, pipeline.Init(ctx))
	var batches []*vectorized.RecordBatch
	for {
		batch, nextErr := pipeline.Next(ctx)
		require.NoError(t, nextErr)
		if batch == nil {
			break
		}
		batches = append(batches, batch)
	}
	elems, egressErr := BuildElementsFromBatches(batches, sqo.TagProjection)
	require.NoError(t, egressErr)
	require.NoError(t, pipeline.Close())
	return elems
}

// topnWantIDs maps a fixture j sequence to the element ids the egress emits for
// series 1, computed the way buildIndexOrderStream stamps them.
func topnWantIDs(js ...int) []string {
	ids := make([]string, 0, len(js))
	for _, j := range js {
		ids = append(ids, hex.EncodeToString(convert.Uint64ToBytes(convert.HashStr("1-"+strconv.Itoa(j)))))
	}
	return ids
}

// topnGotIDs reads back the ordered element ids of a result.
func topnGotIDs(elems []*streamv1.Element) []string {
	ids := make([]string, 0, len(elems))
	for _, elem := range elems {
		ids = append(ids, elem.ElementId)
	}
	return ids
}

// topnBandValue reads the criteria tag out of a materialized element.
// fullProjection puts filter-tag second in the single family.
func topnBandValue(t *testing.T, elem *streamv1.Element) string {
	t.Helper()
	require.Len(t, elem.TagFamilies, 1)
	require.Len(t, elem.TagFamilies[0].Tags, 2)
	return elem.TagFamilies[0].Tags[1].Value.GetStr().GetValue()
}

// TestQueryVectorized_FilteredIndexOrder_TopN is the #14056 evidence: a criteria
// index-order query whose per-node cap is smaller than the corpus must return the
// top-N of the FILTERED set, in index order.
//
// The expectation is analytic, not captured. buildIndexOrderStream gives element j
// of series 1 the sort term value(indexOrderTSCount-j) and the timestamp base+j
// seconds, so over ONE series the index order is total and is the exact REVERSE of
// timestamp order. The criteria keeps the band value004..value009 — six of the
// twelve elements, positioned so that the four rows nearest the head of the index
// order are rejected ascending, and two of the four are rejected descending. With
// the cap at limit+offset=4, a cap taken BEFORE the filter therefore cannot
// produce the asserted sequence in either direction: the control arm runs that
// shape and shows what it returns instead.
func TestQueryVectorized_FilteredIndexOrder_TopN(t *testing.T) {
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "filter-idx", Id: indexOrderRuleID},
		Tags:     []string{"filter-tag"},
	}
	s, tr := buildIndexOrderStream(t)
	ctx := context.Background()
	// One series, so the order key carries no cross-series ties and the fixture
	// fixes the total order.
	entities := parityEntities(1)

	for _, tc := range []struct {
		name        string
		wantCapped  []int
		wantSliced  []int
		wantControl []int
		sort        modelv1.Sort
	}{
		{
			// Index-asc walks j down from indexOrderTSCount. The band's first four
			// survivors are j=8,7,6,5, carrying value004..value007.
			name:        "index-order-asc",
			sort:        modelv1.Sort_SORT_ASC,
			wantCapped:  []int{8, 7, 6, 5},
			wantSliced:  []int{7, 6, 5},
			wantControl: []int{12, 11, 10, 9},
		},
		{
			// Index-desc walks j up from 1. The band's first four survivors are
			// j=3,4,5,6, carrying value009..value006.
			name:        "index-order-desc",
			sort:        modelv1.Sort_SORT_DESC,
			wantCapped:  []int{3, 4, 5, 6},
			wantSliced:  []int{4, 5, 6},
			wantControl: []int{1, 2, 3, 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desc := tc.sort == modelv1.Sort_SORT_DESC
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  fullProjection(),
				Order:          &index.OrderBy{Index: indexRule, Sort: tc.sort},
				MaxElementSize: topnMergeCap,
			}

			got := runTopnFilteredScan(ctx, t, s, sqo, topnBandFilter(t), desc)
			require.Equal(t, topnWantIDs(tc.wantCapped...), topnGotIDs(got),
				"result is not the first %d of the FILTERED index order", topnMergeCap)
			// Index order is the reverse of ts order in this fixture, so a merge that
			// silently keyed on the timestamp would fail here.
			assertMonotonicTS(t, got, !desc)
			for idx, elem := range got {
				value := topnBandValue(t, elem)
				require.GreaterOrEqual(t, value, topnBandLow, "row %d escaped the criteria band", idx)
				require.LessOrEqual(t, value, topnBandHigh, "row %d escaped the criteria band", idx)
			}
			// The enclosing *limit node applies the client offset:offset+limit slice.
			require.Equal(t, topnWantIDs(tc.wantSliced...), topnGotIDs(got[topnOffset:topnOffset+topnLimit]))

			// Control: the same cap with the criteria left at the egress. The merge
			// then bounds the UNFILTERED order, so the window the criteria is applied
			// to is chosen before selectivity is known.
			control := runTopnFilteredScan(ctx, t, s, sqo, nil, desc)
			require.Equal(t, topnWantIDs(tc.wantControl...), topnGotIDs(control))
			survivors := 0
			for _, elem := range control {
				if value := topnBandValue(t, elem); value >= topnBandLow && value <= topnBandHigh {
					survivors++
				}
			}
			require.Less(t, survivors, len(tc.wantCapped),
				"the control arm must under-deliver, else the fixture does not exercise the pushdown")
		})
	}
}
