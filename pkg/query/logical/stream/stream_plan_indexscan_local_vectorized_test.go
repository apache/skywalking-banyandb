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

// newVecEligiblePlan builds a minimal *limit → *localIndexScan plan (the only
// vec-eligible shape) with the given order and tag projection so VecExecutable's
// eligibility decision can be exercised in isolation.
func newVecEligiblePlan(order *logical.OrderBy, projection []model.TagProjection) logical.Plan {
	scan := &localIndexScan{order: order, projectionTags: projection}
	return &limit{Parent: &Parent{Input: scan}}
}

// TestVecExecutable_IndexOrder_TagNotProjected_DeclinesVec is the H2 regression:
// an index-order query whose sort tag is NOT in the projection cannot populate the
// vec OrderKey column, so the vec merge would silently fall back to timestamp
// order (wrong result). VecExecutable must DECLINE (return nil) so the engine runs
// the row path, which sorts via the inverted index regardless of projection.
func TestVecExecutable_IndexOrder_TagNotProjected_DeclinesVec(t *testing.T) {
	order := &logical.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "by-status"},
			Tags:     []string{"status"},
		},
		Sort: modelv1.Sort_SORT_ASC,
	}
	// The ordered tag "status" is absent from the projection.
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service", "endpoint"}}}
	plan := newVecEligiblePlan(order, projection)

	require.Nil(t, VecExecutable(plan),
		"vec must decline an index-order query whose sort tag is not projected")
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
	plan := newVecEligiblePlan(order, projection)

	require.NotNil(t, VecExecutable(plan),
		"vec must accept an index-order query whose sort tag is projected")
}

// TestVecExecutable_TimeOrder_AcceptsVec confirms non-index-order (time-order)
// queries are unaffected by the H2 projection check: they key on timestamp, need
// no ordered tag, and remain vec-eligible regardless of projection.
func TestVecExecutable_TimeOrder_AcceptsVec(t *testing.T) {
	order := &logical.OrderBy{Sort: modelv1.Sort_SORT_DESC}
	projection := []model.TagProjection{{Family: "searchable", Names: []string{"service"}}}
	plan := newVecEligiblePlan(order, projection)

	require.NotNil(t, VecExecutable(plan),
		"vec must accept a time-order query regardless of projection")
}
