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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// FrameEmitter is the data-node wire-emit contract under flag-on:
// any MIterator that participates in a TopicInternalMeasureQuery
// response must encapsulate its own drain + encode strategy via this
// method, so the processor.go Rev can dispatch uniformly without
// case-by-case knowledge of each wrapper.
//
// Implementations:
//
//   - VectorizedMIterator: drains the underlying vec Pipeline directly
//     via DrainPipelineToFrame — the throughput-optimal path that
//     never materialises proto datapoints.
//   - emptyMIterator: returns a nil body (matches the codec layer's
//     RawFrameCodec carve-out for empty distributed results).
//   - hiddenTagsMIterator: drains via Next / Current (which already
//     strips hidden criteria tags from the egress datapoints), then
//     reverse-serialises the surviving rows into a passthrough
//     RecordBatch through SerializeDataPointsToFrame.
//   - sortedMIterator: drains via Next / Current (which already
//     applies cross-group merge + version dedup), then reverse-
//     serialises through SerializeDataPointsToFrame.
//
// The reverse-serialise path is less efficient than draining a vec
// Pipeline (one allocation per cell during passthrough rebuild) but
// keeps the wrapper's row-side semantics — hidden-tag strip, sort,
// dedup — as the single source of truth on the wire.
type FrameEmitter interface {
	EmitFrame(ctx context.Context) ([]byte, error)
}

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
	// A nil pipeline is the canonical empty-result signal — emptyMIterator
	// returns nil from Pipeline() so the data-node Rev can short-circuit
	// to an empty raw frame body. The codec layer's RawFrameCodec treats
	// a nil/empty body as a legitimate empty distributed result (the
	// magic-byte guard is skipped on decode), so returning nil here is
	// what the wire contract expects.
	if p == nil {
		return nil, nil
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
	// Decode any TagValue / FieldValue passthrough columns into their
	// typed equivalents before frame.Encode. Storage's vec adapter emits
	// passthrough columns (TypedColumn[*modelv1.TagValue / *FieldValue])
	// as the in-process fast path — egress consumers reuse the original
	// proto pointer with zero allocation. For the cluster wire, however,
	// the frame format only carries int64/float64/string/bytes; passing a
	// passthrough column directly to frame.Encode fails with
	// ErrUnsupportedColumnType. The conversion picks the smallest-typed
	// wire encoding by inspecting each oneof variant, which is also the
	// most efficient choice: typed wire ≪ proto bytes per cell, both in
	// size (no field-tag overhead per row) and CPU (no proto.Marshal per
	// cell). The receive side's serializeBatchToProto already handles
	// typed columns; the typed-cell → TagValue/FieldValue reconstruction
	// happens at the row-egress (one allocation per surviving row, vs the
	// scan-time decode the storage avoided via passthrough — but the
	// trade is favourable when the wire crossing in between would
	// otherwise dominate).
	converted, convErr := convertPassthroughForFrame(out)
	if convErr != nil {
		return nil, fmt.Errorf("DrainPipelineToFrame: %w", convErr)
	}
	return frame.Encode(converted)
}

// SerializeDataPointsToFrame is the fallback wire-emit path for iterator
// wrappers (hiddenTagsMIterator, sortedMIterator) whose internal sort /
// strip / cross-group merge logic operates on []*InternalDataPoint
// rather than on a vec Pipeline. The wrapper drains itself via the
// row-side Next / Current API (so its existing strip / merge / dedup
// logic still runs); the resulting rows are reverse-serialised into a
// passthrough RecordBatch, convertPassthroughForFrame decodes the
// passthrough columns to typed wire columns, and frame.Encode produces
// the body.
//
// This path is less efficient than the vec native pipeline drain — it
// allocates a *modelv1.TagValue / *FieldValue per cell during reverse-
// serialise — but it keeps the wrapper's egress semantics intact end-
// to-end on the wire (hidden tags stripped, cross-group order honoured,
// version dedup applied) without re-implementing each one in columnar
// form.
//
// idps in the empty/zero case yields a nil body, matching the codec
// layer's RawFrameCodec carve-out for empty distributed results.
func SerializeDataPointsToFrame(idps []*measurev1.InternalDataPoint) ([]byte, error) {
	if len(idps) == 0 {
		return nil, nil
	}
	b, batchErr := buildPassthroughBatchFromDataPoints(idps)
	if batchErr != nil {
		return nil, fmt.Errorf("SerializeDataPointsToFrame: %w", batchErr)
	}
	converted, convErr := convertPassthroughForFrame(b)
	if convErr != nil {
		return nil, fmt.Errorf("SerializeDataPointsToFrame: %w", convErr)
	}
	return frame.Encode(converted)
}

// buildPassthroughBatchFromDataPoints walks idps to derive a unified
// schema (the first non-empty row defines the column layout) and emits
// a RecordBatch with passthrough TagValue / FieldValue columns plus
// timestamp / version / sid / shard_id native columns. The passthrough
// columns are then handed to convertPassthroughForFrame to pick wire
// types from each cell's oneof variant — matching the storage adapter's
// passthrough emit shape so the rest of the wire pipeline is identical
// to a fresh vec scan.
func buildPassthroughBatchFromDataPoints(idps []*measurev1.InternalDataPoint) (*vectorized.RecordBatch, error) {
	var sample *measurev1.DataPoint
	for _, idp := range idps {
		if idp == nil {
			continue
		}
		dp := idp.GetDataPoint()
		if dp != nil {
			sample = dp
			break
		}
	}
	if sample == nil {
		return nil, fmt.Errorf("buildPassthroughBatchFromDataPoints: no non-nil datapoint to derive schema from")
	}
	defs := []vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Name: "_version", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Name: "_sid", Type: vectorized.ColumnTypeInt64},
	}
	type tagSlot struct{ family, name string }
	tagOrder := make([]tagSlot, 0)
	for _, fam := range sample.GetTagFamilies() {
		for _, tag := range fam.GetTags() {
			tagOrder = append(tagOrder, tagSlot{family: fam.GetName(), name: tag.GetKey()})
			defs = append(defs, vectorized.ColumnDef{
				Role:      vectorized.RoleTag,
				TagFamily: fam.GetName(),
				Name:      tag.GetKey(),
				Type:      vectorized.ColumnTypeTagValue,
			})
		}
	}
	fieldOrder := make([]string, 0)
	for _, f := range sample.GetFields() {
		fieldOrder = append(fieldOrder, f.GetName())
		defs = append(defs, vectorized.ColumnDef{
			Role: vectorized.RoleField,
			Name: f.GetName(),
			Type: vectorized.ColumnTypeFieldValue,
		})
	}
	schema := vectorized.NewBatchSchema(defs)
	b := vectorized.NewRecordBatch(schema, len(idps))
	shardCol := b.Columns[0].(*vectorized.TypedColumn[int64])
	tsCol := b.Columns[1].(*vectorized.TypedColumn[int64])
	verCol := b.Columns[2].(*vectorized.TypedColumn[int64])
	sidCol := b.Columns[3].(*vectorized.TypedColumn[int64])
	tagColStart := 4
	fieldColStart := tagColStart + len(tagOrder)
	for _, idp := range idps {
		dp := idp.GetDataPoint()
		if dp == nil {
			shardCol.AppendNull()
			tsCol.AppendNull()
			verCol.AppendNull()
			sidCol.AppendNull()
			for i := range tagOrder {
				b.Columns[tagColStart+i].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(nil)
			}
			for i := range fieldOrder {
				b.Columns[fieldColStart+i].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(nil)
			}
			continue
		}
		shardCol.Append(int64(idp.GetShardId()))
		if dp.GetTimestamp() != nil {
			tsCol.Append(dp.GetTimestamp().AsTime().UnixNano())
		} else {
			tsCol.AppendNull()
		}
		verCol.Append(dp.GetVersion())
		sidCol.Append(int64(dp.GetSid()))
		tagLookup := make(map[string]*modelv1.TagValue, len(tagOrder))
		for _, fam := range dp.GetTagFamilies() {
			for _, tag := range fam.GetTags() {
				tagLookup[fam.GetName()+"\x00"+tag.GetKey()] = tag.GetValue()
			}
		}
		for i, slot := range tagOrder {
			b.Columns[tagColStart+i].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
				tagLookup[slot.family+"\x00"+slot.name],
			)
		}
		fieldLookup := make(map[string]*modelv1.FieldValue, len(fieldOrder))
		for _, f := range dp.GetFields() {
			fieldLookup[f.GetName()] = f.GetValue()
		}
		for i, name := range fieldOrder {
			b.Columns[fieldColStart+i].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(
				fieldLookup[name],
			)
		}
	}
	b.Len = len(idps)
	return b, nil
}

// convertPassthroughForFrame returns a RecordBatch in which every
// TagValue / FieldValue passthrough column has been decoded to its typed
// equivalent. Other column types (RoleShardID/timestamp/version/seriesID
// or already-typed tag/field columns produced by GroupBy + Agg paths)
// pass through unchanged. The fast path returns the input unchanged when
// the schema has no passthrough columns at all — agg/GroupBy queries
// typically allocate native typed columns for the keys + reduce outputs
// so this is a no-op.
//
// Wire-type selection is variant-driven: the first non-null cell decides
// what typed column to materialise (a Str variant ⇒ string column, Int ⇒
// int64, BinaryData ⇒ bytes). All-null columns default to bytes — the
// receiver's reconstruction uses the validity bitmap, not the data, so
// the chosen wire type is irrelevant for purely-null columns and bytes
// has the smallest empty-cell encoding (uvarint(0)).
//
// IntArray / StrArray variants of TagValue are NOT yet representable by
// frame.Encode (the format lacks array column types); the helper returns
// a typed error so the caller can surface it as a hard failure rather
// than silently mis-encoding. Adding native array column types to the
// frame format is the natural follow-up; until then, scans that materialise
// array-typed tags on the cluster wire are unsupported.
func convertPassthroughForFrame(b *vectorized.RecordBatch) (*vectorized.RecordBatch, error) {
	if !hasPassthroughColumn(b.Schema) {
		return b, nil
	}
	newDefs := make([]vectorized.ColumnDef, 0, len(b.Schema.Columns))
	newCols := make([]vectorized.Column, 0, len(b.Schema.Columns))
	for i, def := range b.Schema.Columns {
		col := b.Columns[i]
		switch def.Type {
		case vectorized.ColumnTypeTagValue:
			newDef, newCol, err := convertTagValueColumn(def, col, b.Len)
			if err != nil {
				return nil, fmt.Errorf("column %d (%q): %w", i, def.Name, err)
			}
			newDefs = append(newDefs, newDef)
			newCols = append(newCols, newCol)
		case vectorized.ColumnTypeFieldValue:
			newDef, newCol, err := convertFieldValueColumn(def, col, b.Len)
			if err != nil {
				return nil, fmt.Errorf("column %d (%q): %w", i, def.Name, err)
			}
			newDefs = append(newDefs, newDef)
			newCols = append(newCols, newCol)
		default:
			newDefs = append(newDefs, def)
			newCols = append(newCols, col)
		}
	}
	return &vectorized.RecordBatch{
		Schema:  vectorized.NewBatchSchema(newDefs),
		Columns: newCols,
		Len:     b.Len,
	}, nil
}

// hasPassthroughColumn reports whether any column declares a proto-
// passthrough type that frame.Encode cannot consume directly.
func hasPassthroughColumn(s *vectorized.BatchSchema) bool {
	for _, def := range s.Columns {
		if def.Type == vectorized.ColumnTypeTagValue || def.Type == vectorized.ColumnTypeFieldValue {
			return true
		}
	}
	return false
}

// convertTagValueColumn picks a wire-typed column for a TagValue
// passthrough by inspecting the first non-null cell's oneof variant, then
// converts every cell (null cells retain their validity bit; non-null
// cells write the underlying typed value into the new column). Returns
// ErrUnsupportedColumnType when the variant cannot be carried by the
// current frame format (IntArray / StrArray today).
func convertTagValueColumn(def vectorized.ColumnDef, col vectorized.Column, n int) (vectorized.ColumnDef, vectorized.Column, error) {
	tc, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue])
	if !ok {
		return vectorized.ColumnDef{}, nil, fmt.Errorf("declared TagValue passthrough but column is %T", col)
	}
	wireType, err := inferTagValueWireType(tc, n)
	if err != nil {
		return vectorized.ColumnDef{}, nil, err
	}
	newCol := vectorized.NewColumnForType(wireType, n)
	for i := range n {
		if tc.IsNull(i) {
			newCol.(interface{ AppendNull() }).AppendNull()
			continue
		}
		v := tc.Data()[i]
		if v == nil {
			newCol.(interface{ AppendNull() }).AppendNull()
			continue
		}
		switch payload := v.GetValue().(type) {
		case *modelv1.TagValue_Str:
			newCol.(*vectorized.TypedColumn[string]).Append(payload.Str.GetValue())
		case *modelv1.TagValue_Int:
			newCol.(*vectorized.TypedColumn[int64]).Append(payload.Int.GetValue())
		case *modelv1.TagValue_BinaryData:
			newCol.(*vectorized.TypedColumn[[]byte]).Append(append([]byte(nil), payload.BinaryData...))
		case *modelv1.TagValue_Null:
			newCol.(interface{ AppendNull() }).AppendNull()
		default:
			return vectorized.ColumnDef{}, nil, fmt.Errorf("TagValue variant %T not yet supported on the cluster wire", payload)
		}
	}
	return vectorized.ColumnDef{
		Role:      def.Role,
		TagFamily: def.TagFamily,
		Name:      def.Name,
		Type:      wireType,
	}, newCol, nil
}

// inferTagValueWireType walks col looking for the first non-null cell
// whose payload identifies the column's wire type. Returns
// ColumnTypeBytes for all-null columns — a benign default since the
// validity bitmap (not the data section) drives null reconstruction at
// the receive side. IntArray / StrArray variants are rejected here.
func inferTagValueWireType(tc *vectorized.TypedColumn[*modelv1.TagValue], n int) (vectorized.ColumnType, error) {
	for i := range n {
		if tc.IsNull(i) {
			continue
		}
		v := tc.Data()[i]
		if v == nil {
			continue
		}
		switch v.GetValue().(type) {
		case *modelv1.TagValue_Str:
			return vectorized.ColumnTypeString, nil
		case *modelv1.TagValue_Int:
			return vectorized.ColumnTypeInt64, nil
		case *modelv1.TagValue_BinaryData:
			return vectorized.ColumnTypeBytes, nil
		case *modelv1.TagValue_IntArray, *modelv1.TagValue_StrArray:
			return 0, fmt.Errorf("TagValue array variants not yet supported on the cluster wire (array column types are a frame-format follow-up)")
		case *modelv1.TagValue_Null:
			continue
		}
	}
	return vectorized.ColumnTypeBytes, nil
}

// convertFieldValueColumn is the FieldValue counterpart of
// convertTagValueColumn. FieldValue's oneof maps cleanly to the four
// supported frame types (Int → int64, Float → float64, Str → string,
// BinaryData → bytes) so there is no unsupported-variant branch.
func convertFieldValueColumn(def vectorized.ColumnDef, col vectorized.Column, n int) (vectorized.ColumnDef, vectorized.Column, error) {
	fc, ok := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
	if !ok {
		return vectorized.ColumnDef{}, nil, fmt.Errorf("declared FieldValue passthrough but column is %T", col)
	}
	wireType := inferFieldValueWireType(fc, n)
	newCol := vectorized.NewColumnForType(wireType, n)
	for i := range n {
		if fc.IsNull(i) {
			newCol.(interface{ AppendNull() }).AppendNull()
			continue
		}
		v := fc.Data()[i]
		if v == nil {
			newCol.(interface{ AppendNull() }).AppendNull()
			continue
		}
		switch payload := v.GetValue().(type) {
		case *modelv1.FieldValue_Int:
			newCol.(*vectorized.TypedColumn[int64]).Append(payload.Int.GetValue())
		case *modelv1.FieldValue_Float:
			newCol.(*vectorized.TypedColumn[float64]).Append(payload.Float.GetValue())
		case *modelv1.FieldValue_Str:
			newCol.(*vectorized.TypedColumn[string]).Append(payload.Str.GetValue())
		case *modelv1.FieldValue_BinaryData:
			newCol.(*vectorized.TypedColumn[[]byte]).Append(append([]byte(nil), payload.BinaryData...))
		case *modelv1.FieldValue_Null:
			newCol.(interface{ AppendNull() }).AppendNull()
		default:
			return vectorized.ColumnDef{}, nil, fmt.Errorf("FieldValue variant %T not yet supported on the cluster wire", payload)
		}
	}
	return vectorized.ColumnDef{
		Role:      def.Role,
		TagFamily: def.TagFamily,
		Name:      def.Name,
		Type:      wireType,
	}, newCol, nil
}

// inferFieldValueWireType is the FieldValue counterpart of
// inferTagValueWireType. FieldValue's oneof has no array variant so the
// helper is total over the value space.
func inferFieldValueWireType(fc *vectorized.TypedColumn[*modelv1.FieldValue], n int) vectorized.ColumnType {
	for i := range n {
		if fc.IsNull(i) {
			continue
		}
		v := fc.Data()[i]
		if v == nil {
			continue
		}
		switch v.GetValue().(type) {
		case *modelv1.FieldValue_Int:
			return vectorized.ColumnTypeInt64
		case *modelv1.FieldValue_Float:
			return vectorized.ColumnTypeFloat64
		case *modelv1.FieldValue_Str:
			return vectorized.ColumnTypeString
		case *modelv1.FieldValue_BinaryData:
			return vectorized.ColumnTypeBytes
		case *modelv1.FieldValue_Null:
			continue
		}
	}
	return vectorized.ColumnTypeBytes
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
// pushedDownAggregatedIterator (agg path's flatten step).
//
// For the NON-agg distributed merge path use DecodeFramesPerSource
// instead — concatenation here destroys the per-source ordering the
// sortedMIterator's cross-iterator merge + (sid, timestamp) dedup
// depends on, which is what caused replica duplicates to slip past
// the dedup map under flag-on.
//
// nil/empty bodies are skipped — the codec layer's RawFrameCodec carve-out
// returns nil for an empty distributed result and the decoder is below
// that carve-out.
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

// DecodeFramesPerSource decodes a sequence of vec raw frame bodies and
// returns one []*measurev1.InternalDataPoint slice per non-empty frame —
// preserving the per-data-node grouping the row path's sortedMIterator
// + sort.NewItemIter merger needs to dedup replicas. Each data node's
// scan output is internally sort-field ordered; sort.NewItemIter merges
// the per-source streams and loadOneGroup's hashDataPoint map removes
// (sid, timestamp) duplicates within each equal-sortField group.
// Concatenating into a single slice (as DecodeFramesToInternalDataPoints
// does) defeats the dedup because duplicate rows from different sources
// end up in different sortField groups across the flat sequence.
//
// nil / empty frame bodies produce no slice in the output — the codec
// carve-out for empty bodies is honoured here too.
func DecodeFramesPerSource(frames [][]byte) ([][]*measurev1.InternalDataPoint, error) {
	out := make([][]*measurev1.InternalDataPoint, 0, len(frames))
	for i, body := range frames {
		if len(body) == 0 {
			continue
		}
		b, decodeErr := frame.Decode(body)
		if decodeErr != nil {
			return nil, fmt.Errorf("DecodeFramesPerSource: decode frame %d: %w", i, decodeErr)
		}
		idps := serializeBatchToProto(b, nil)
		if len(idps) > 0 {
			out = append(out, idps)
		}
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
