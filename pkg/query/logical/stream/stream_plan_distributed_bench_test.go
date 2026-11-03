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
	"fmt"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

// benchElements keeps the benchmark result reachable so the compiler cannot drop it.
var benchElements int

// BenchmarkDistributedStreamMergeFrameReplies compares the two merge paths that
// distributedPlan.Execute can take for the same set of decoded frame batches:
//
//   - "row": the pre-change behavior, kept as the fallback when any node replies with
//     proto. Every frame batch is materialized into Elements before the k-way merge.
//   - "batch": the new frame-only path. The batches are aligned onto one schema, merged
//     as columns, windowed, and only the surviving rows are materialized.
//
// Both run the same wire -> decode -> build pipeline for their input, so the numbers are
// the cost of the merge stage alone. Run with -benchmem; the allocation delta is the point.
func BenchmarkDistributedStreamMergeFrameReplies(b *testing.B) {
	const tagCount = 3
	tags := make([]string, tagCount)
	for idx := range tags {
		tags[idx] = fmt.Sprintf("tag-%d", idx)
	}
	projection := []model.TagProjection{{Family: "fam", Names: tags}}

	for _, tc := range []struct {
		name         string
		nodes        int
		frames       int
		rowsPerFrame int
		offset       uint32
		limit        uint32
	}{
		{name: "small-2nodes-2frames-16rows", nodes: 2, frames: 2, rowsPerFrame: 16, offset: 4, limit: 10},
		{name: "medium-4nodes-4frames-256rows", nodes: 4, frames: 4, rowsPerFrame: 256, offset: 20, limit: 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			nodes, staged := buildBenchFrameReplies(b, projection, tc.nodes, tc.frames, tc.rowsPerFrame)
			plan := &distributedPlan{
				queryTemplate: &streamv1.QueryRequest{Name: "bench"},
				sortByTime:    true,
				offset:        tc.offset,
				limit:         tc.limit,
			}
			ctx := context.Background()

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(staged)), "batches/op")
				b.ResetTimer()
				for range b.N {
					elements, err := plan.mergeBatchReplies(ctx, staged, len(nodes), nil)
					if err != nil {
						b.Fatalf("mergeBatchReplies: %v", err)
					}
					benchElements = len(elements)
				}
			})
			b.Run("row", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(staged)), "batches/op")
				b.ResetTimer()
				for range b.N {
					elements, err := plan.mergeRowReplies(nodes, len(nodes), nil)
					if err != nil {
						b.Fatalf("mergeRowReplies: %v", err)
					}
					benchElements = len(elements)
				}
			})

			// Sanity: both paths must return the same window, otherwise the comparison is
			// meaningless.
			batchWindow, err := plan.mergeBatchReplies(ctx, staged, len(nodes), nil)
			if err != nil {
				b.Fatalf("mergeBatchReplies sanity: %v", err)
			}
			rowWindow, err := plan.mergeRowReplies(nodes, len(nodes), nil)
			if err != nil {
				b.Fatalf("mergeRowReplies sanity: %v", err)
			}
			if len(batchWindow) != len(rowWindow) {
				b.Fatalf("window sizes differ: batch=%d row=%d", len(batchWindow), len(rowWindow))
			}
			for idx := range batchWindow {
				if batchWindow[idx].ElementId != rowWindow[idx].ElementId {
					b.Fatalf("windows differ at %d: batch=%s row=%s",
						idx, batchWindow[idx].ElementId, rowWindow[idx].ElementId)
				}
			}
			b.Logf("window=%d elements, input batches=%d", len(batchWindow), len(staged))
		})
	}
}

// buildBenchFrameReplies creates tc.nodes streams, each with tc.frames native frames of
// tc.rowsPerFrame rows. Timestamps interleave across nodes; ElementIDs repeat across nodes
// for the same (frame, row) so the merge exercises cross-node de-duplication.
func buildBenchFrameReplies(tb testing.TB, projection []model.TagProjection,
	nodes, frames, rowsPerFrame int,
) ([]nodeStreamResult, []*vectorized.RecordBatch) {
	tb.Helper()
	schema := vstream.BuildStreamBatchSchema(projection, "", "")
	results := make([]nodeStreamResult, nodes)
	staged := make([]*vectorized.RecordBatch, 0, nodes*frames)
	for nodeIdx := range nodes {
		results[nodeIdx].responded = true
		for frameIdx := range frames {
			batch := vectorized.NewRecordBatch(schema, rowsPerFrame)
			tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
			elemCol := batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
			seriesCol := batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
			tagCols := make([]*vectorized.TypedColumn[*modelv1.TagValue], 0, len(projection[0].Names))
			for _, name := range projection[0].Names {
				tagIdx, ok := schema.TagIndex(projection[0].Family, name)
				if !ok {
					tb.Fatalf("tag %s missing from bench schema", name)
				}
				tagCols = append(tagCols, batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue]))
			}
			for rowIdx := range rowsPerFrame {
				row := frameIdx*rowsPerFrame + rowIdx
				tsCol.Append(int64(row*nodes + nodeIdx))
				elemCol.Append(vstream.ElementIDToColumn(uint64(row)))
				seriesCol.Append(vstream.SeriesIDToColumn(0))
				for tagIdx := range tagCols {
					tagCols[tagIdx].Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("v-%d", row%8)}}})
				}
			}
			batch.Len = rowsPerFrame
			body, err := streamframe.Encode(batch)
			if err != nil {
				tb.Fatalf("encode bench frame: %v", err)
			}
			decoded, err := streamframe.Decode(body)
			if err != nil {
				tb.Fatalf("decode bench frame: %v", err)
			}
			results[nodeIdx].chunks = append(results[nodeIdx].chunks, nodeStreamChunk{batch: decoded})
			staged = append(staged, decoded)
		}
	}
	return results, staged
}
