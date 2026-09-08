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
// output, in that strict order.
//
// preMerge fusibles run on the RAW source batches, before the merge consumes
// them. A row-level filter belongs here rather than at the egress: once the merge
// sees only surviving rows, maxRows bounds the top-N of the FILTERED set, which is
// what makes a cap sound for a criteria query at all. A pre-merge fusible must be
// row-level and must never signal ErrLimitExhausted, which would truncate the scan.
//
// Filtering first also settles duplicate ElementIDs. The criteria is evaluated
// before Distinct picks a winner, so an element is represented by its first
// MATCHING row in the requested sort order, and one matching version makes the
// element eligible even when another version fails. Two rows of one element
// commonly carry the same ordered-tag value and therefore the same sort key; among
// equal keys the first row the scan yields wins, in both directions.
//
// maxRows bounds the merge to the in-order top-N (0 = unbounded). This is the
// per-node scan cap (maxElementSize = limit+offset), applied AFTER the merge
// sorts — the correct top-N in sort order — matching the row path, which caps
// after its own in-order merge: blockHeap.merge / MergeStreamResults for
// timestamp order, idxResult.loadSortingData + mergeByTagValue for index order.
// It is distinct from the client offset/limit slice the trailing Limit applies.
func BuildStreamMergePipeline(
	source vectorized.PullOperator,
	schema *vectorized.BatchSchema,
	desc bool,
	offset, limit uint32,
	batchSize, maxRows int,
	preMerge ...vectorized.FusibleOperator,
) (*vectorized.Pipeline, error) {
	builder := vectorized.NewPipelineBuilder().From(source)
	for _, op := range preMerge {
		builder = builder.Apply(op)
	}
	return builder.
		Break(NewSortedMergeWithCap(schema, desc, batchSize, maxRows)).
		Apply(NewDistinct(schema)).
		Apply(NewLimit(schema, offset, limit)).
		Build()
}
