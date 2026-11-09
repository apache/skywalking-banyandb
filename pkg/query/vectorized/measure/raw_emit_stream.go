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
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// FrameLimits caps one frame of a streamed result. A zero field means no cap.
type FrameLimits struct {
	// MaxBytes caps the estimated size of the rows of one frame; a row larger than the cap goes alone.
	MaxBytes uint64
	// MaxRows caps the rows of one frame.
	MaxRows int
}

// FrameStreamEmitter is the streaming counterpart of FrameEmitter: an MIterator that implements it hands its
// result to emit one frame at a time, as its pipeline produces it, instead of draining it into one body. Every
// frame of a stream has the same column layout, so a receiver can merge them without seeing the whole result.
// emit returns once the frame has been sent; the frame is not used afterwards.
type FrameStreamEmitter interface {
	EmitFrames(ctx context.Context, limits FrameLimits, emit func(frame []byte) error) error
}

// EmitFrames implements FrameStreamEmitter by encoding the pipeline's batches as they arrive.
func (i *vectorizedMIterator) EmitFrames(ctx context.Context, limits FrameLimits, emit func([]byte) error) error {
	return streamPipelineFrames(ctx, i.pipeline, i.schema, limits, emit)
}

// EmitFrames implements FrameStreamEmitter on the public adapter.
func (v *VectorizedMIterator) EmitFrames(ctx context.Context, limits FrameLimits, emit func([]byte) error) error {
	return v.inner.EmitFrames(ctx, limits, emit)
}

// streamPipelineFrames pulls the pipeline's batches and emits each as one or more frames, releasing a batch's
// borrowed storage and its reservation before the next pull. It never narrows passthrough columns, so the layout
// of every frame is the schema's.
func streamPipelineFrames(ctx context.Context, p *vectorized.Pipeline, schema *vectorized.BatchSchema, limits FrameLimits, emit func([]byte) error) (retErr error) {
	// A nil pipeline is the canonical empty result.
	if p == nil {
		return nil
	}
	if schema == nil {
		return fmt.Errorf("streamPipelineFrames: nil schema")
	}
	var encodeSpan *query.Span
	var rowsOut, framesOut, bytesOut int
	if tracer := query.GetTracer(ctx); tracer != nil {
		encodeSpan, _ = tracer.StartSpan(ctx, "frame-encode")
		defer func() {
			if retErr != nil {
				encodeSpan.Error(retErr)
			} else {
				encodeSpan.Tagf(tracelabels.TagRowsOut, "%d", rowsOut)
				encodeSpan.Tagf(tracelabels.TagBatchesOut, "%d", framesOut)
				encodeSpan.Tagf(tracelabels.TagBytesOut, "%d", bytesOut)
				encodeSpan.Tagf(tracelabels.TagSchemaCols, "%d", len(schema.Columns))
			}
			encodeSpan.Stop()
		}()
	}
	conversion := needsFrameConversion(schema)
	for {
		batch, err := p.Next(ctx)
		if err != nil {
			return fmt.Errorf("streamPipelineFrames: pipeline next: %w", err)
		}
		if batch == nil {
			return nil
		}
		frames, rows, emitErr := emitBatchFrames(ctx, p.Tracker(), batch, schema, conversion, limits, emit)
		batch.ReleaseOwners()
		if emitErr != nil {
			return emitErr
		}
		rowsOut += rows
		framesOut += frames.count
		bytesOut += frames.bytes
	}
}

type emittedFrames struct {
	count, bytes int
}

// emitBatchFrames encodes the active rows of batch in frames within limits and emits them.
func emitBatchFrames(ctx context.Context, tracker *vectorized.MemoryTracker, batch *vectorized.RecordBatch, schema *vectorized.BatchSchema,
	conversion bool, limits FrameLimits, emit func([]byte) error,
) (emittedFrames, int, error) {
	var out emittedFrames
	rows := batch.ActiveLen()
	if rows == 0 {
		return out, 0, nil
	}
	if !schemasEqual(schema, batch.Schema) {
		return out, 0, fmt.Errorf("streamPipelineFrames: batch schema mismatch (planner bug)")
	}
	active, activeErr := activeIndices(batch)
	if activeErr != nil {
		return out, 0, activeErr
	}
	chunkRows := frameChunkRows(limits, vectorized.EstimatedRetainedBytes(batch), rows)
	for start := 0; start < rows; start += chunkRows {
		end := min(start+chunkRows, rows)
		part := batch
		if start > 0 || end < rows {
			part = &vectorized.RecordBatch{Schema: batch.Schema, Columns: batch.Columns, Len: batch.Len, Selection: active[start:end:end]}
		}
		body, encodeErr := encodeFrameChunk(ctx, tracker, part, schema, conversion)
		if encodeErr != nil {
			return out, 0, encodeErr
		}
		out.count++
		out.bytes += len(body.data)
		emitErr := emit(body.data)
		body.release()
		if emitErr != nil {
			return out, 0, emitErr
		}
	}
	return out, rows, nil
}

// frameChunkRows is how many of rows go in one frame: as many as the caps allow, at least one.
func frameChunkRows(limits FrameLimits, batchBytes uint64, rows int) int {
	chunk := rows
	if limits.MaxRows > 0 {
		chunk = min(chunk, limits.MaxRows)
	}
	if limits.MaxBytes > 0 && batchBytes > limits.MaxBytes {
		perRow := max(batchBytes/uint64(rows), 1)
		chunk = min(chunk, int(min(max(limits.MaxBytes/perRow, 1), math.MaxInt32)))
	}
	return max(chunk, 1)
}

type frameBody struct {
	release func()
	data    []byte
}

// encodeFrameChunk encodes part into one frame, reserving the memory the encoding needs on the tracker before it
// allocates. The reservation is held until the returned release is called, once the frame has been sent.
func encodeFrameChunk(ctx context.Context, tracker *vectorized.MemoryTracker, part *vectorized.RecordBatch,
	schema *vectorized.BatchSchema, conversion bool,
) (frameBody, error) {
	var reserved int64
	release := func() {
		if tracker != nil && reserved > 0 {
			tracker.Release(reserved)
		}
		reserved = 0
	}
	reserve := func(amount int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tracker == nil {
			return nil
		}
		if err := tracker.Reserve(amount); err != nil {
			return err
		}
		reserved += amount
		return nil
	}
	if conversion {
		amount, estimateErr := frameEgressReservationBytes(part)
		if estimateErr != nil {
			return frameBody{}, estimateErr
		}
		if reserveErr := reserve(amount + int64(1024+len(schema.Columns)*512)); reserveErr != nil {
			return frameBody{}, reserveErr
		}
		dense := vectorized.NewRecordBatch(schema, part.ActiveLen())
		defer dense.ReleaseOwners()
		if appendErr := appendActive(dense, part); appendErr != nil {
			release()
			return frameBody{}, fmt.Errorf("streamPipelineFrames: %w", appendErr)
		}
		converted, convErr := convertForFrame(dense, false)
		if convErr != nil {
			release()
			return frameBody{}, fmt.Errorf("streamPipelineFrames: %w", convErr)
		}
		encoded, encodeErr := frame.Encode(converted)
		if encodeErr != nil {
			release()
			return frameBody{}, encodeErr
		}
		return frameBody{data: encoded, release: release}, nil
	}
	input := vectorized.EstimatedRetainedBytes(part)
	if input > math.MaxInt64/2 {
		return frameBody{}, vectorized.ErrMemoryBudgetExceeded
	}
	if reserveErr := reserve(int64(1024+len(schema.Columns)*512) + int64(input)); reserveErr != nil {
		return frameBody{}, reserveErr
	}
	encoded, encodeErr := frame.EncodeWithReservation(part, func(size int) error { return reserve(int64(size)) })
	if encodeErr != nil {
		release()
		return frameBody{}, encodeErr
	}
	return frameBody{data: encoded, release: release}, nil
}
