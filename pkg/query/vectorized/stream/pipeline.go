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

import "github.com/apache/skywalking-banyandb/pkg/query/vectorized"

// BuildStreamMergePipeline composes the liaison-side merge → distinct → limit
// pipeline over a source of stream RecordBatches. SortedMerge is the breaker
// (global ordering), Distinct and Limit are fusibles applied on the ordered
// output, in that strict order. There is no pre-merge short-circuit.
//
// maxRows bounds the merge to the in-order top-N (0 = unbounded). This is the
// per-node scan cap (maxElementSize = limit+offset), applied AFTER the merge
// sorts — the correct top-N in sort order — matching the row path, which caps
// after its in-order heap merge (blockHeap.merge / MergeStreamResults). It is
// distinct from the client offset/limit slice the trailing Limit applies.
func BuildStreamMergePipeline(
	source vectorized.PullOperator,
	schema *vectorized.BatchSchema,
	desc bool,
	offset, limit uint32,
	batchSize, maxRows int,
) (*vectorized.Pipeline, error) {
	return vectorized.NewPipelineBuilder().
		From(source).
		Break(NewSortedMergeWithCap(schema, desc, batchSize, maxRows)).
		Apply(NewDistinct(schema)).
		Apply(NewLimit(schema, offset, limit)).
		Build()
}
