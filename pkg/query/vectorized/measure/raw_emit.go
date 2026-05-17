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

package measure

import (
	"context"
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// DrainPipelineToFrame consumes a vec Pipeline end-to-end, concatenates
// every emitted batch into a single RecordBatch, and frame.Encode-s the
// result. It is the data-node side of the G9f throughout-vec wire: under
// flag-on, TopicInternalMeasureQuery responses are exactly this single
// raw frame body (one row per group for AggModeMap, one row per source
// row for non-agg queries).
//
// schema is the propagated terminal-operator output schema (typically
// the same as the egress pool's). It MUST match each emitted batch — a
// mismatch is a planner bug, not a data error, so this function will
// fail loud on a per-batch shape check instead of silently coercing.
//
// Multi-batch coalesce: pipeline.Next is called until it returns nil;
// every non-nil batch's active rows are appended to a single output
// batch via copyOneValue. This is intentionally NOT a streaming-write
// of multiple frames — the codec contract carries one body per response
// (api/data/codec.go), and frame.Encode expects one batch. For typical
// distributed agg responses (one row per group, group count ≤ batchSize)
// the pipeline emits a single batch and coalesce is a no-op fast path.
//
// Returns the encoded frame body and the close error from pipeline.Close
// joined into a single error (mirrors vectorizedMIterator.Close).
func DrainPipelineToFrame(ctx context.Context, p *vectorized.Pipeline, schema *vectorized.BatchSchema) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("DrainPipelineToFrame: nil pipeline")
	}
	if schema == nil {
		return nil, fmt.Errorf("DrainPipelineToFrame: nil schema")
	}
	out := vectorized.NewRecordBatch(schema, 0)
	for {
		b, pullErr := p.Next(ctx)
		if pullErr != nil {
			return nil, fmt.Errorf("DrainPipelineToFrame: pipeline next: %w", pullErr)
		}
		if b == nil {
			break
		}
		if !schemasEqual(schema, b.Schema) {
			return nil, fmt.Errorf("DrainPipelineToFrame: batch schema mismatch (planner bug)")
		}
		appendActive(out, b)
	}
	return frame.Encode(out)
}

// appendActive copies every active row of src into dst's columns,
// respecting Selection. Per-column dispatch reuses copyOneValue, which
// already covers every supported TypedColumn instantiation (int64,
// float64, string, []byte, []int64, []string, *TagValue, *FieldValue)
// so this loop stays type-agnostic.
func appendActive(dst, src *vectorized.RecordBatch) {
	active := activeIndices(src)
	for _, rowIdx := range active {
		for colIdx, srcCol := range src.Columns {
			copyOneValue(dst.Columns[colIdx], srcCol, int(rowIdx))
		}
	}
	dst.Len += len(active)
}

// ReduceFramesToInternalDataPoints is the liaison-side composition for
// distributed agg queries under flag-on: it decodes the per-data-node
// partial frames, runs the (shard, group)-deduped Reduce, optionally
// applies a final Top-N (when topSpec.N > 0), and serializes the
// resulting batches back to []*measurev1.InternalDataPoint so the
// row-side pushedDownAggregatedIterator + downstream MIterator surface
// stay unchanged.
//
// The conversion back to proto is the price of bridging vec output into
// the row-path liaison's existing iterator. Once the row-path
// distributedPlan is replaced by a fully vec-distributed plan (out of
// scope for G9f.5), this round trip drops out.
//
// Empty input (no non-empty frames) returns an empty slice with no
// error — matches the row path's behaviour when every data node returned
// an empty distributed result.
func ReduceFramesToInternalDataPoints(
	frames [][]byte,
	keyTagNames []string,
	aggSpecs []AggReduceSpec,
	topSpec *ReduceTopSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*measurev1.InternalDataPoint, error) {
	reduced, reduceErr := ReduceRawFrames(frames, keyTagNames, aggSpecs, batchSize, tracker)
	if reduceErr != nil {
		return nil, reduceErr
	}
	if topSpec != nil && topSpec.N > 0 && len(reduced) > 0 {
		topped, topErr := ApplyTopToReduce(reduced, *topSpec, batchSize)
		if topErr != nil {
			return nil, topErr
		}
		reduced = topped
	}
	var out []*measurev1.InternalDataPoint
	for _, b := range reduced {
		if b == nil || b.Len == 0 {
			continue
		}
		out = serializeBatchToProto(b, out)
	}
	return out, nil
}

// DecodeFramesToInternalDataPoints decodes a sequence of vec raw frame
// bodies and concatenates their active rows into a single
// []*measurev1.InternalDataPoint, ready to feed the row-side
// distributedPlan's sortableElements / sortedMIterator merger (non-agg
// path) or pushedDownAggregatedIterator (agg path's flatten step).
//
// nil/empty bodies are skipped — the codec layer's RawFrameCodec carve-out
// returns nil for an empty distributed result and the decoder is below
// that carve-out (ReducePartialBatches has the same semantics).
//
// Schema introspection: serializeBatchToProto consults the decoded
// schema's ShardIDIndex to populate InternalDataPoint.ShardId, so the
// frame's shard_id column survives the round-trip back to proto. The
// per-frame schemas MUST all agree — under flag-on this is guaranteed
// because every data node runs the same vec plan.
func DecodeFramesToInternalDataPoints(frames [][]byte) ([]*measurev1.InternalDataPoint, error) {
	var out []*measurev1.InternalDataPoint
	for i, body := range frames {
		if len(body) == 0 {
			continue
		}
		b, decodeErr := frame.Decode(body)
		if decodeErr != nil {
			return nil, fmt.Errorf("DecodeFramesToInternalDataPoints: decode frame %d: %w", i, decodeErr)
		}
		out = serializeBatchToProto(b, out)
	}
	return out, nil
}

// schemasEqual reports whether two BatchSchemas have the same column
// names, roles, and types in the same order. Used as a defensive
// per-batch shape check inside DrainPipelineToFrame — a planner-level
// shape change mid-pipeline is unrecoverable, so the function fails
// loud rather than silently coerce.
func schemasEqual(a, b *vectorized.BatchSchema) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		ac, bc := a.Columns[i], b.Columns[i]
		if ac.Name != bc.Name || ac.Role != bc.Role || ac.Type != bc.Type {
			return false
		}
	}
	return true
}
