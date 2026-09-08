// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package stream

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// The stream index-order path derived MaxElementSize from limit+offset and
// preallocated elementIDsSorted with that capacity. When the vectorized path
// declines (order tag omitted from projection), this row path must stay bounded.
func TestQueryCapacityCapsIndexOrderElementIDs(t *testing.T) {
	if got := queryCapacity(math.MaxInt32); got != queryCapacityHint {
		t.Fatalf("queryCapacity(MaxInt32)=%d, want %d", got, queryCapacityHint)
	}
	if got := queryCapacity(64); got != 64 {
		t.Fatalf("queryCapacity(64)=%d, want 64", got)
	}
	const oversized = 4096
	ids := make([]uint64, 0, queryCapacity(oversized))
	if cap(ids) != queryCapacityHint {
		t.Fatalf("elementIDsSorted capacity %d, want %d", cap(ids), queryCapacityHint)
	}
}

func TestIndexOrderFallbackElementIDsCapacityBounded(t *testing.T) {
	const oversizedMaxElementSize = 4096
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "filter-idx", Id: indexOrderRuleID},
		Tags:     []string{"filter-tag"},
	}
	s, tr := buildIndexOrderStream(t)
	sqo := model.StreamQueryOptions{
		Name:      "benchmark",
		TimeRange: &tr,
		Entities:  parityEntities(indexOrderSeriesCount),
		// Order by filter-tag but project only entity-tag so the vectorized path
		// declines and the row index-sort path allocates elementIDsSorted.
		TagProjection: []model.TagProjection{{
			Family: "benchmark-family",
			Names:  []string{"entity-tag"},
		}},
		Order:          &index.OrderBy{Index: indexRule, Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: oversizedMaxElementSize,
	}
	res, err := s.Query(context.Background(), sqo)
	require.NoError(t, err)
	require.NotNil(t, res)
	defer res.Release()

	qr, ok := res.(*idxResult)
	require.True(t, ok, "expected idxResult from indexed-order query")
	_ = qr.Pull(context.Background())
	require.Equal(t, queryCapacityHint, cap(qr.elementIDsSorted),
		"idxResult.elementIDsSorted must stay hint-capped for oversized MaxElementSize")
}
