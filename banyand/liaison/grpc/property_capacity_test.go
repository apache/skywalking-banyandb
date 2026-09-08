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

package grpc

import (
	"math"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

// Ordered property merge buffers previously used req.Limit as make capacity.
// Keep the hint capped for oversized limits.
func TestPropertyQueryCapacityCapsOversizedLimit(t *testing.T) {
	if got := propertyQueryCapacity(math.MaxUint32); got != propertyQueryCapacityHint {
		t.Fatalf("propertyQueryCapacity(MaxUint32)=%d, want %d", got, propertyQueryCapacityHint)
	}
	if got := propertyQueryCapacity(32); got != 32 {
		t.Fatalf("propertyQueryCapacity(32)=%d, want 32", got)
	}
	const oversized uint32 = 4096
	buf := make([]*propertyWithCount, 0, propertyQueryCapacity(oversized))
	if cap(buf) != propertyQueryCapacityHint {
		t.Fatalf("liaison buffer capacity %d, want %d", cap(buf), propertyQueryCapacityHint)
	}
}

func TestSortedQueryWithDedupCapacityBounded(t *testing.T) {
	const oversizedLimit uint32 = 4096
	ps := &propertyServer{}
	req := &propertyv1.QueryRequest{
		Limit: oversizedLimit,
		OrderBy: &propertyv1.QueryOrder{
			TagName: "tag1",
			Sort:    modelv1.Sort_SORT_ASC,
		},
	}
	nodeProperties := map[string][]*propertyWithMetadata{
		"node-a": {{
			Property: &propertyv1.Property{
				Metadata: &commonv1.Metadata{
					Group:       "g",
					Name:        "n",
					ModRevision: 1,
				},
				Id: "cap-id",
			},
			node:        "node-a",
			sortedValue: []byte("tag1"),
		}},
	}
	out := ps.sortedQueryWithDedup(nodeProperties, req)
	if len(out) == 0 {
		t.Fatal("expected sortedQueryWithDedup to retain the input property")
	}
	if cap(out) != propertyQueryCapacityHint {
		t.Fatalf("sortedQueryWithDedup capacity %d, want %d", cap(out), propertyQueryCapacityHint)
	}
}
