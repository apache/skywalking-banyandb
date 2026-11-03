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

package query

import (
	"context"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

// The multi-group stream merge and the liaison's distributed Stream merge share one
// implementation in pkg/query/vectorized/stream: both hold several staged batches, rebuild
// them onto one schema, k-way merge them, deduplicate, and materialize Elements only from the
// surviving rows. The helpers below keep this package's call sites and tests on the same
// entry points while the implementation lives in the lower package, which the liaison can
// import without a cycle.

// alignedStreamGroups holds the staged batch of every non-empty group, rebuilt onto one
// shared schema.
type alignedStreamGroups = vstream.AlignedStreamBatches

// alignStreamGroupBatches rebuilds each staged group batch onto one merged schema.
func alignStreamGroupBatches(ctx context.Context, staged []*vectorized.RecordBatch,
	sortByTime bool,
) (*alignedStreamGroups, error) {
	return vstream.AlignStreamBatches(ctx, staged, sortByTime)
}

// mergeAlignedStreamGroups runs the aligned batches of every group through one SortedMerge,
// Distinct and Limit(offset, limit) pipeline and returns the surviving batches.
func mergeAlignedStreamGroups(ctx context.Context, groups *alignedStreamGroups, desc bool, offset, limit uint32,
) ([]*vectorized.RecordBatch, error) {
	return groups.Merge(ctx, desc, offset, limit)
}

// buildAlignedStreamElements materializes the merged batches once, then drops from each
// element the tags its group did not define.
func buildAlignedStreamElements(groups *alignedStreamGroups, batches []*vectorized.RecordBatch,
) ([]*streamv1.Element, error) {
	return groups.BuildElements(batches, nil)
}
