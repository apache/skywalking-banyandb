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

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

const (
	pushdownFamily     = "searchable"
	pushdownOrderTag   = "duration"
	pushdownFilterTag  = "state"
	pushdownFilterWant = "open"
	// pushdownCorpus is far larger than pushdownMaxElements so an unbounded merge is
	// distinguishable from a bounded one by the returned element count alone.
	pushdownCorpus      = 200
	pushdownMaxElements = 25
)

// pushdownVecSource replays pre-built batches as an executor.StreamVecScanSource.
type pushdownVecSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	pos     int
}

func (s *pushdownVecSource) Schema() *vectorized.BatchSchema { return s.schema }

func (s *pushdownVecSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.pos]
	s.pos++
	return batch, nil
}

func (s *pushdownVecSource) Release() {}

// pushdownExecContext is a StreamExecutionContext whose vec source replays a
// fixed in-order corpus, so ExecuteVectorized can be driven without a data node.
type pushdownExecContext struct {
	src *pushdownVecSource
}

func (e *pushdownExecContext) Query(context.Context, model.StreamQueryOptions) (model.StreamQueryResult, error) {
	return nil, nil
}

func (e *pushdownExecContext) QueryVectorized(context.Context, model.StreamQueryOptions) (executor.StreamVecScanSource, error) {
	return e.src, nil
}

func (e *pushdownExecContext) VectorizedConfig() vstream.VectorizedConfig {
	return vstream.DefaultConfig()
}

// pushdownProjection is the two-tag projection the corpus populates: the ordered
// tag (so the plan is vec-eligible) plus the criteria tag.
func pushdownProjection() []model.TagProjection {
	return []model.TagProjection{{Family: pushdownFamily, Names: []string{pushdownOrderTag, pushdownFilterTag}}}
}

// pushdownStreamSchema is the stream definition both tags are declared in.
func pushdownStreamSchema() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "pushdown", Group: "test"},
		Entity:   &databasev1.Entity{TagNames: []string{pushdownOrderTag}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: pushdownFamily,
			Tags: []*databasev1.TagSpec{
				{Name: pushdownOrderTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: pushdownFilterTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}
}

// pushdownCorpusBatch builds one batch of pushdownCorpus rows, each a distinct
// ElementID, in ascending order-key order and all matching the criteria — so the
// element count ExecuteVectorized returns is decided by the merge cap alone, not
// by predicate selectivity.
func pushdownCorpusBatch(schema *vectorized.BatchSchema) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(schema, pushdownCorpus)
	tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	seriesCol := batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	orderIdx, _ := schema.TagIndex(pushdownFamily, pushdownOrderTag)
	orderTagCol := batch.Columns[orderIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
	stateIdx, _ := schema.TagIndex(pushdownFamily, pushdownFilterTag)
	stateCol := batch.Columns[stateIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
	keyCol := batch.Columns[schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte])
	for rowIdx := 0; rowIdx < pushdownCorpus; rowIdx++ {
		// Zero-padded so lexicographic key order equals numeric order.
		key := []byte(pushdownOrderTag + "-" + pad3(rowIdx))
		tsCol.Append(int64(rowIdx + 1))
		elemCol.Append(vstream.ElementIDToColumn(uint64(rowIdx + 1)))
		seriesCol.Append(vstream.SeriesIDToColumn(1))
		orderTagCol.Append(pushdownStr(string(key)))
		stateCol.Append(pushdownStr(pushdownFilterWant))
		keyCol.Append(key)
		batch.Len++
	}
	return batch
}

func pad3(n int) string {
	digits := []byte{byte('0' + n/100%10), byte('0' + n/10%10), byte('0' + n%10)}
	return string(digits)
}

func pushdownStr(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

// newPushdownFilteredPlan builds the criteria index-order shape
// *limit → *tagFilterPlan → *localIndexScan over a replayable vec corpus, and
// returns the scan the dispatch resolves plus that scan.
func newPushdownFilteredPlan(t *testing.T) *localIndexScan {
	t.Helper()
	sch, err := BuildSchema(pushdownStreamSchema(), nil)
	require.NoError(t, err)

	criteria := &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  pushdownFilterTag,
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: pushdownStr(pushdownFilterWant),
	}}}
	tagFilter, err := logical.BuildSimpleTagFilter(criteria)
	require.NoError(t, err)

	order := &logical.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "by-duration"},
			Tags:     []string{pushdownOrderTag},
		},
		Sort: modelv1.Sort_SORT_ASC,
	}
	projection := pushdownProjection()
	batchSchema := vstream.BuildStreamBatchSchema(projection, pushdownFamily, pushdownOrderTag)
	scan := &localIndexScan{
		schema:         sch,
		order:          order,
		projectionTags: projection,
		maxElementSize: pushdownMaxElements,
		metadata:       pushdownStreamSchema().GetMetadata(),
		ec: &pushdownExecContext{src: &pushdownVecSource{
			schema:  batchSchema,
			batches: []*vectorized.RecordBatch{pushdownCorpusBatch(batchSchema)},
		}},
	}
	filter := &tagFilterPlan{s: sch, parent: scan, tagFilter: tagFilter, hiddenTags: logical.NewHiddenTagSet()}
	require.NotNil(t, scanFromInput(filter), "the filtered index-order shape must resolve to a vec scan")
	return scan
}

// TestPushdown_FilteredIndexOrder_BoundsMerge is the #14056 acceptance assertion:
// a filtered index-order stream query must bound its vec merge at
// limit+offset. Today the filter runs at egress, after the merge, so the merge is
// deliberately left uncapped and ExecuteVectorized returns the whole ordered set;
// once the tag filter is pushed ahead of the merge the cap becomes sound and the
// element count drops to maxElementSize.
func TestPushdown_FilteredIndexOrder_BoundsMerge(t *testing.T) {
	scan := newPushdownFilteredPlan(t)
	batches, _, err := scan.ExecuteVectorized(context.Background())
	require.NoError(t, err)

	rows := 0
	for _, batch := range batches {
		rows += batch.ActiveLen()
	}
	require.Positive(t, rows, "corpus is degenerate: the vec pipeline returned nothing")
	require.LessOrEqual(t, rows, pushdownMaxElements,
		"filtered index-order merge is unbounded: returned %d elements for a limit+offset of %d", rows, pushdownMaxElements)
}
