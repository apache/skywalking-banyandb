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
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// vecTestSchema is the stream schema the plan-level vec tests resolve the ordered
// tag's family against.
func vecTestSchema(t *testing.T) logical.Schema {
	t.Helper()
	s, err := BuildSchema(&databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "vec-test"},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "status", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "state", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}, nil)
	require.NoError(t, err)
	return s
}

// newVecEligiblePlan builds a minimal *limit → *localIndexScan plan (the only
// vec-eligible shape) with the given order and tag projection so VecExecutable's
// eligibility decision can be exercised in isolation.
func newVecEligiblePlan(t *testing.T, order *logical.OrderBy, projection []model.TagProjection) (logical.Plan, *localIndexScan) {
	scan := &localIndexScan{order: order, projectionTags: projection, schema: vecTestSchema(t)}
	return &limit{Parent: &Parent{Input: scan}}, scan
}

// TestVecExecutable_IndexOrder_TagNotProjected_ProjectsItInternally covers the
// R-1 gap: an index-order query whose sort tag is absent from the projection used
// to fall back to the row path, because vec derives its OrderKey from the
// projected cell. Vec now asks the scan for the tag anyway and keeps it out of
// ProjectionTags(), so the query stays on the vec path and the extra tag never
// reaches the egress.
func TestVecExecutable_IndexOrder_TagNotProjected_ProjectsItInternally(t *testing.T) {
	order := &logical.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "by-status"},
			Tags:     []string{"status"},
		},
		Sort: modelv1.Sort_SORT_ASC,
	}
	// The ordered tag "status" is absent from the projection.
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service", "endpoint"}}}
	plan, scan := newVecEligiblePlan(t, order, projection)

	require.NotNil(t, VecExecutable(plan), "vec must accept an index-order query whose sort tag is not projected")
	scanProjection, hidden, resolved := scan.vecTagProjection()
	require.True(t, resolved)
	require.True(t, hidden, "the ordered tag must be reported as hidden so the frame egress is skipped")
	require.Equal(t, []model.TagProjection{{Family: "searchable", Names: []string{"service", "endpoint", "status"}}}, scanProjection)
	require.Equal(t, projection, scan.ProjectionTags(), "the client projection must not gain the ordered tag")
	require.True(t, scan.HidesOrderTag())
}

// TestVecExecutable_IndexOrder_TagNotInSchema_DeclinesVec is the one remaining
// decline: a stale index rule naming a tag the schema no longer defines cannot be
// added to the projection, and running vec anyway would sort by timestamp without
// saying so. The row path sorts via the inverted index regardless, so decline.
func TestVecExecutable_IndexOrder_TagNotInSchema_DeclinesVec(t *testing.T) {
	order := &logical.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "by-dropped"},
			Tags:     []string{"dropped-tag"},
		},
		Sort: modelv1.Sort_SORT_ASC,
	}
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service"}}}
	plan, _ := newVecEligiblePlan(t, order, projection)

	require.Nil(t, VecExecutable(plan),
		"vec must decline when the ordered tag cannot be resolved against the schema")
	require.Equal(t, "the order-by tag does not resolve against the stream schema", VecDeclineReason(plan),
		"the decline must name the tag-resolution failure, not a generic shape mismatch")
}

// TestVecExecutable_IndexOrder_TagProjected_AcceptsVec is the positive control:
// when the ordered tag IS projected, the OrderKey column can be populated, so vec
// is eligible and VecExecutable returns the scan.
func TestVecExecutable_IndexOrder_TagProjected_AcceptsVec(t *testing.T) {
	order := &logical.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "by-status"},
			Tags:     []string{"status"},
		},
		Sort: modelv1.Sort_SORT_ASC,
	}
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service", "status"}}}
	plan, _ := newVecEligiblePlan(t, order, projection)

	require.NotNil(t, VecExecutable(plan),
		"vec must accept an index-order query whose sort tag is projected")
}

// TestVecExecutable_TimeOrder_AcceptsVec confirms non-index-order (time-order)
// queries are unaffected by the H2 projection check: they key on timestamp, need
// no ordered tag, and remain vec-eligible regardless of projection.
func TestVecExecutable_TimeOrder_AcceptsVec(t *testing.T) {
	order := &logical.OrderBy{Sort: modelv1.Sort_SORT_DESC}
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service"}}}
	plan, _ := newVecEligiblePlan(t, order, projection)

	require.NotNil(t, VecExecutable(plan),
		"vec must accept a time-order query regardless of projection")
}

// newVecFilteredPlan builds the criteria shape *limit → *tagFilterPlan →
// *localIndexScan so scanFromInput's pushdown decision can be exercised directly.
// The tag filter is a real one, not DummyFilter: scanFromInput only pushes a
// filter down when the criteria survived BuildTagFilter, so a dummy would make
// both order arms report "no pushdown" for the wrong reason.
func newVecFilteredPlan(t *testing.T, order *logical.OrderBy, projection []model.TagProjection) (logical.Plan, *localIndexScan) {
	t.Helper()
	tagFilter, err := logical.BuildSimpleTagFilter(&modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  "state",
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "open"}}},
	}}})
	require.NoError(t, err)
	scan := &localIndexScan{order: order, projectionTags: projection, schema: vecTestSchema(t)}
	filter := &tagFilterPlan{parent: scan, tagFilter: tagFilter, hiddenTags: logical.HiddenTagSet{}}
	return &limit{Parent: &Parent{Input: filter}}, scan
}

// TestScanCap_FilteredTimeOrder_CapsMerge locks the row-parity rule a stream soak
// exposed. For TIMESTAMP order, one row Pull consumes a segment and caps at
// maxElementSize, and the following Pull only advances to a further segment — so
// within a segment the row scan is exhausted and row returns just that first capped
// batch's matches (30 scanned, 2 rejected ⇒ 28, NOT a filled limit of 30). The vec
// merge must therefore cap and leave the filter AT THE EGRESS, behind that cap,
// feeding it the same element set. Pushing the filter ahead of the merge here would
// let the merge fill the limit from rows row never sees, and over-return.
func TestScanCap_FilteredTimeOrder_CapsMerge(t *testing.T) {
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service", "state"}}}
	plan, scan := newVecFilteredPlan(t, nil, projection)

	require.NotNil(t, VecExecutable(plan), "a filtered time-order query is vec-eligible")
	require.Nil(t, scan.preMergeFilter,
		"time-order scans do not resume within a segment, so the filter must stay at the egress behind the cap")
}

// TestScanCap_FilteredIndexOrder_PushesFilterDown is the other arm, and the shape
// #14056 is about. For INDEX order the sorted iterator persists across Pulls (each
// drains the next maxElementSize entries), so row keeps pulling and DOES fill the
// limit. A cap taken BEFORE the filter would starve it, so the filter moves ahead
// of the merge instead: the merge then orders only surviving rows, and its cap
// bounds the top-N of the filtered set. That set matches the one row fills its
// limit from, except for duplicate ElementIDs, whose contract is filter-first —
// see BuildStreamMergePipeline. scanFromInput must therefore stash the filter on
// the scan.
func TestScanCap_FilteredIndexOrder_PushesFilterDown(t *testing.T) {
	order := &logical.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "duration"},
			Tags:     []string{"duration"},
		},
		Sort: modelv1.Sort_SORT_DESC,
	}
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"duration", "span_id"}}}
	plan, scan := newVecFilteredPlan(t, order, projection)

	require.NotNil(t, VecExecutable(plan), "a filtered index-order query with the sort tag projected is vec-eligible")
	require.NotNil(t, scan.preMergeFilter,
		"index-order scans resume across Pulls, so the filter must run pre-merge to make the merge cap sound")
}
