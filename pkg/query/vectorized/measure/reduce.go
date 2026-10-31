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
	"slices"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// AggReduceSpec configures one aggregation output for the liaison-side
// Reduce. OutputName MUST match the column name produced by the data-node
// AggModeMap for the same aggregation (i.e. the AggSpec.Output that drove
// buildAggOutputLayout). Func selects the reducer (must agree with the
// Map-side function — SUM-Map paired with SUM-Reduce, MEAN with MEAN, etc).
type AggReduceSpec struct {
	OutputName string
	Func       AggFunc
}

// ReduceTopSpec configures an optional post-Reduce Top-N step. FieldName
// names the agg output column to sort by; N is the number of rows to
// retain; Asc=true keeps the lowest N, Asc=false keeps the highest N
// (mirrors the row path's distributed Top-over-Agg pattern — see
// pkg/query/logical/measure/measure_plan_distributed.go's
// DistributedAnalyze, where Top is applied AFTER the distributedPlan on
// the liaison).
type ReduceTopSpec struct {
	FieldName string
	N         int
	Asc       bool
}

// ReduceRawFrames runs the liaison-side Reduce phase on a sequence of vec
// partial frame bodies. Each frame is decoded into a RecordBatch and fed
// to a single AggModeReduce BatchAggregation that dedupes (shard_id,
// group_key) replica duplicates and combines partials across shards via
// aggregation.Reduce[N].
//
// Empty/nil frame bodies are skipped — a data node emitting an empty
// distributed result sends a body-less SendResponse and the codec layer
// passes nil bytes through (see api/data/codec.go's RawFrameCodec carve-out
// for nil/empty). Treating those as zero-row partials matches the row
// path, which simply has no contributing rows to combine.
//
// The first non-empty frame defines the partial schema (column count,
// roles, types, and names). Subsequent frames MUST share that schema —
// any structural mismatch is a producer bug, not a recoverable data error,
// and is reported loudly.
//
// keyTagFamily and keyTagNames select which tags form the group key — the
// same family and names the request's GroupBy used. The family must travel
// with the names: tag-family validation does not reject the same tag name
// in two families (design doc §5.1), so resolveKeyIndices matches on
// (family, name), not name alone — matching the node-side GroupBy's own
// (plan.go's lookupGroupByKeyIndices). Tags present in the partial schema
// but NOT listed here are still carried forward in the output as the
// first-seen value per group (mirrors the data-node operator's first-seen
// non-key tag rule).
//
// bucketed is true for a time-bucketed GroupBy (design §7.2): the bucket
// column is resolved as an additional leading key, ahead of keyTagNames.
// A bucketed reduce whose reference schema carries no RoleTimestamp column
// is the mixed-version case (design §10) — an older data node ignored
// time_bucket and returned whole-range partials — and resolveKeyIndices
// hard-errors rather than silently collapsing the series to one row.
//
// Returns the final reduced batches in group-insertion order (paginated
// by batchSize) and the AggValuePath that describes how the value column
// was resolved. Callers walk the batches sequentially; one row per group.
func ReduceRawFrames(
	frames [][]byte,
	keyTagFamily string,
	keyTagNames []string,
	bucketed bool,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, AggValuePath, error) {
	return ReduceRawFramesToSchema(frames, nil, keyTagFamily, keyTagNames, bucketed, aggSpecs, batchSize, tracker)
}

// ReduceRawFramesToSchema is ReduceRawFrames for partials that may come
// from groups with different schemas. Every partial is reshaped onto
// target before reducing (see checkPartialLayouts), so target, not whichever
// partials happen to arrive, decides the reduced layout. A nil target
// requires every partial to share one layout, like ReduceRawFrames.
func ReduceRawFramesToSchema(
	frames [][]byte,
	target *vectorized.BatchSchema,
	keyTagFamily string,
	keyTagNames []string,
	bucketed bool,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, AggValuePath, error) {
	decoded := make([]*vectorized.RecordBatch, 0, len(frames))
	// A columnar partial borrows its frame; the reduce copies the keys and values it keeps
	// (the aggregation inserts by copy), so the partials can go once it returns.
	defer func() {
		for _, b := range decoded {
			b.ReleaseOwners()
		}
	}()
	for i, body := range frames {
		if len(body) == 0 {
			continue
		}
		b, decodeErr := frame.Decode(body)
		if decodeErr != nil {
			return nil, AggValuePathUnresolved, fmt.Errorf("ReduceRawFrames: decode partial %d: %w", i, decodeErr)
		}
		decoded = append(decoded, b)
	}
	return ReducePartialBatchesToSchema(decoded, target, keyTagFamily, keyTagNames, bucketed, aggSpecs, batchSize, tracker)
}

// ReducePartialBatches is the in-memory counterpart of ReduceRawFrames —
// useful for tests and for callers that have already decoded their
// partials. Empty batches (nil or Len==0) are skipped; the first non-empty
// batch defines the schema and structural compatibility is checked against
// subsequent ones.
//
// Returns the reduced batches and the AggValuePath that describes how the
// value column was resolved (typed, fieldvalue-fallback, or unresolved).
func ReducePartialBatches(
	partials []*vectorized.RecordBatch,
	keyTagFamily string,
	keyTagNames []string,
	bucketed bool,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, AggValuePath, error) {
	return ReducePartialBatchesToSchema(partials, nil, keyTagFamily, keyTagNames, bucketed, aggSpecs, batchSize, tracker)
}

// ReducePartialBatchesToSchema is ReducePartialBatches with every
// non-empty partial first reshaped onto target (see checkPartialLayouts). A nil
// target keeps ReducePartialBatches' behavior.
func ReducePartialBatchesToSchema(
	partials []*vectorized.RecordBatch,
	target *vectorized.BatchSchema,
	keyTagFamily string,
	keyTagNames []string,
	bucketed bool,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, AggValuePath, error) {
	reducer := NewPartialReducer(target, keyTagFamily, keyTagNames, bucketed, aggSpecs, batchSize, tracker)
	defer reducer.Close()
	for i, b := range partials {
		if consumeErr := reducer.consume(b, i); consumeErr != nil {
			return nil, reducer.path, consumeErr
		}
	}
	return reducer.Finish()
}

// PartialReducer is ReducePartialBatchesToSchema as an operator that takes its partials one at a time, so a
// caller that receives them as a stream holds the reduced groups instead of every partial. The first non-empty
// partial defines the partial layout unless a target is given. The reducer copies what it keeps, so a partial
// can be released as soon as Consume returns.
type PartialReducer struct {
	refSchema    *vectorized.BatchSchema
	op           *BatchAggregation
	scratch      *vectorized.RecordBatch
	wideScratch  *vectorized.RecordBatch
	tracker      *vectorized.MemoryTracker
	target       *vectorized.BatchSchema
	path         AggValuePath
	keyTagFamily string
	keyTagNames  []string
	aggSpecs     []AggReduceSpec
	reserved     int64
	partials     int
	batchSize    int
	bucketed     bool
}

// NewPartialReducer returns a reducer with the parameters of ReducePartialBatchesToSchema.
func NewPartialReducer(
	target *vectorized.BatchSchema,
	keyTagFamily string,
	keyTagNames []string,
	bucketed bool,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) *PartialReducer {
	return &PartialReducer{
		target: target, keyTagFamily: keyTagFamily, keyTagNames: keyTagNames, bucketed: bucketed,
		aggSpecs: aggSpecs, batchSize: batchSize, tracker: tracker, path: AggValuePathTyped,
	}
}

// Consume folds one partial into the reduction. Empty partials are skipped.
func (r *PartialReducer) Consume(b *vectorized.RecordBatch) error {
	return r.consume(b, r.partials)
}

func (r *PartialReducer) consume(b *vectorized.RecordBatch, idx int) error {
	r.partials++
	if b == nil || b.Len == 0 {
		return nil
	}
	if layoutErr := checkPartialLayouts([]*vectorized.RecordBatch{b}, r.target, r.keyTagFamily, r.keyTagNames); layoutErr != nil {
		r.path = AggValuePathUnresolved
		return fmt.Errorf("ReducePartialBatches: %w", layoutErr)
	}
	if r.op == nil {
		if initErr := r.open(b); initErr != nil {
			return initErr
		}
	}
	op, refSchema, batchSize, target := r.op, r.refSchema, r.batchSize, r.target
	if target != nil && !slices.Equal(b.Schema.Columns, target.Columns) {
		if r.wideScratch == nil {
			// Reserve the target-width chunk before allocating it; the
			// received frames were only charged at their own width.
			reserved, reserveErr := reserveReshapeChunk(r.tracker, target, batchSize)
			if reserveErr != nil {
				return fmt.Errorf("ReducePartialBatches: reshape partial %d: %w", idx, reserveErr)
			}
			r.reserved += reserved
			r.wideScratch = vectorized.NewRecordBatch(target, batchSize)
		}
		if consumeErr := consumeReshaped(op, r.wideScratch, b, batchSize); consumeErr != nil {
			return fmt.Errorf("ReducePartialBatches: consume partial %d: %w", idx, consumeErr)
		}
		return nil
	}
	if compatErr := schemaCompatible(refSchema, b.Schema); compatErr != nil {
		return fmt.Errorf("ReducePartialBatches: partial %d schema mismatch: %w", idx, compatErr)
	}
	if b.ActiveLen() <= batchSize {
		// A partial that already fits in one chunk is consumed directly, with no
		// copy and no scratch batch involved.
		if consumeErr := op.Consume(context.Background(), b); consumeErr != nil {
			return fmt.Errorf("ReducePartialBatches: consume partial %d: %w", idx, consumeErr)
		}
		return nil
	}
	if r.scratch == nil {
		r.scratch = vectorized.NewRecordBatch(refSchema, batchSize)
	}
	if consumeErr := vectorized.ConsumeChunked(op, r.scratch, b, batchSize); consumeErr != nil {
		return fmt.Errorf("ReducePartialBatches: consume partial %d: %w", idx, consumeErr)
	}
	return nil
}

// open binds the keys and the aggregates on the layout of the first non-empty partial.
func (r *PartialReducer) open(first *vectorized.RecordBatch) error {
	r.refSchema = first.Schema
	if r.target != nil {
		r.refSchema = r.target
	}
	keyIndices, indicesErr := resolveKeyIndices(r.refSchema, r.keyTagFamily, r.keyTagNames, r.bucketed)
	if indicesErr != nil {
		r.path = AggValuePathUnresolved
		return indicesErr
	}
	specs, path, specsErr := bindAggReduceSpecs(r.refSchema, r.aggSpecs)
	if specsErr != nil {
		r.path = AggValuePathUnresolved
		return specsErr
	}
	r.path = path
	r.op = NewBatchAggregation(r.refSchema, keyIndices, specs, AggModeReduce, r.batchSize, r.tracker, aggEntrySize)
	if initErr := r.op.Init(context.Background()); initErr != nil {
		return fmt.Errorf("ReducePartialBatches: init: %w", initErr)
	}
	return nil
}

// OutputSchema returns the schema of the reduced batches, or nil before the first non-empty partial.
func (r *PartialReducer) OutputSchema() *vectorized.BatchSchema {
	if r.op == nil {
		return nil
	}
	return r.op.OutputSchema()
}

// Groups returns how many groups the reducer holds.
func (r *PartialReducer) Groups() int {
	if r.op == nil {
		return 0
	}
	return len(r.op.insertion)
}

// Finish returns the reduced batches in group-insertion order, bucket-ascending for a bucketed reduction, and the
// AggValuePath that describes how the value column was resolved. The reducer is spent afterwards.
func (r *PartialReducer) Finish() ([]*vectorized.RecordBatch, AggValuePath, error) {
	if r.op == nil {
		return nil, r.path, nil
	}
	if finalErr := r.op.Finalize(context.Background()); finalErr != nil {
		return nil, r.path, fmt.Errorf("ReducePartialBatches: finalize: %w", finalErr)
	}
	// Each partial arrives already bucket-ascending (design §7.2 streams
	// per node), but the reducer consumes partials one by one, so insertion
	// order is only piecewise ascending — e.g. node A's [2000, 3000]
	// followed by node B's [1000, 2000] inserts as [2000, 3000, 1000].
	// Sort once before draining so the merged result is globally
	// bucket-ascending, which iteratorFromBatches's offset/limit pagination
	// (design §7.5) requires to page correctly. A no-op when unbucketed.
	r.op.SortInsertionByBucket()
	var out []*vectorized.RecordBatch
	for {
		nb, nextErr := r.op.NextBatch(context.Background())
		if nextErr != nil {
			return nil, r.path, fmt.Errorf("ReducePartialBatches: next: %w", nextErr)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out, r.path, nil
}

// Close releases the reducer's aggregation state and the memory it reserved. It is safe to call more than once.
func (r *PartialReducer) Close() {
	if r.op != nil {
		_ = r.op.Close()
		r.op = nil
	}
	if r.reserved > 0 {
		r.tracker.Release(r.reserved)
		r.reserved = 0
	}
}

// resolveKeyIndices binds the bucket column (when bucketed) and
// (keyTagFamily, keyTagNames) to column indices in the partial schema.
// Matching requires both family and name — tag-family validation does not
// reject the same tag name in two families (design doc §5.1), so matching
// by name alone could silently bind to the wrong family's column on a
// valid schema. Returns an error if a tag does not resolve, or if bucketed
// is true but the schema carries no RoleTimestamp column (design §10's
// mixed-version guard). Neither bucketed nor any keyTagNames is allowed
// (scalar reduce) and returns nil keyIndices; keyTagFamily is unused then.
func resolveKeyIndices(schema *vectorized.BatchSchema, keyTagFamily string, keyTagNames []string, bucketed bool) ([]int, error) {
	var out []int
	if bucketed {
		idx := schema.TimestampIndex()
		if idx < 0 {
			return nil, fmt.Errorf("bucketed query expected a RoleTimestamp column in the partial schema (an older data node may have ignored time_bucket)")
		}
		out = append(out, idx)
	}
	for _, name := range keyTagNames {
		idx := -1
		for i, def := range schema.Columns {
			if def.Role == vectorized.RoleTag && def.TagFamily == keyTagFamily && def.Name == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("key tag %s.%s not present in partial schema", keyTagFamily, name)
		}
		out = append(out, idx)
	}
	return out, nil
}

// AggValuePath records which column-type path bindAggReduceSpecs used to
// resolve the aggregation value column.
type AggValuePath string

// AggValuePath constants match the trace tag values required by US-VT-1.
const (
	// AggValuePathTyped means all agg specs resolved via a native int64 or
	// float64 column — the normal AggModeMap partial path.
	AggValuePathTyped AggValuePath = "typed"
	// AggValuePathFieldValueFallback means at least one agg spec fell back
	// to a ColumnTypeFieldValue passthrough column because no native typed
	// column was found — the DataPoint-egress passthrough path.
	AggValuePathFieldValueFallback AggValuePath = "fieldvalue-fallback"
	// AggValuePathUnresolved means bindAggReduceSpecs could not find any
	// matching column for at least one agg spec and returned an error.
	AggValuePathUnresolved AggValuePath = "unresolved"
)

// bindAggReduceSpecs maps each requested AggReduceSpec to the partial
// schema's value column. The value column is identified by its name
// (AggReduceSpec.OutputName); the count sidecar — if present — is found
// by buildAggInputCountIdx, so callers only need to supply the value
// column name. A missing value column is a producer/planner mismatch and
// is reported loudly.
//
// The returned AggValuePath reflects the resolution path taken:
//   - AggValuePathTyped if all specs resolved via a native int64/float64 column.
//   - AggValuePathFieldValueFallback if at least one spec fell back to a FieldValue column.
//   - AggValuePathUnresolved (alongside a non-nil error) if a column was not found.
func bindAggReduceSpecs(schema *vectorized.BatchSchema, aggSpecs []AggReduceSpec) ([]AggSpec, AggValuePath, error) {
	out := make([]AggSpec, 0, len(aggSpecs))
	usedFieldValueFallback := false
	for _, ars := range aggSpecs {
		valueIdx := -1
		fieldValueIdx := -1
		for i, def := range schema.Columns {
			if def.Role != vectorized.RoleField || def.Name != ars.OutputName {
				continue
			}
			if isAggReduceValueType(def.Type) {
				valueIdx = i
				break
			}
			if def.Type == vectorized.ColumnTypeFieldValue && fieldValueIdx < 0 {
				fieldValueIdx = i
			}
		}
		if valueIdx < 0 && fieldValueIdx >= 0 {
			valueIdx = fieldValueIdx
			usedFieldValueFallback = true
		}
		if valueIdx < 0 {
			return nil, AggValuePathUnresolved, fmt.Errorf("agg output column %q not present in partial schema", ars.OutputName)
		}
		out = append(out, AggSpec{
			Output:   ars.OutputName,
			Func:     ars.Func,
			InputCol: valueIdx,
		})
	}
	path := AggValuePathTyped
	if usedFieldValueFallback {
		path = AggValuePathFieldValueFallback
	}
	return out, path, nil
}

func isAggReduceValueType(columnType vectorized.ColumnType) bool {
	return columnType == vectorized.ColumnTypeInt64 || columnType == vectorized.ColumnTypeFloat64
}

// schemaCompatible asserts that two BatchSchemas describe the same column
// layout — every per-column Name, Role and Type matches. The hard-cutover
// model forbids schema drift across partials of the same query, so any
// mismatch is a botched producer rollout, not a recoverable data error.
func schemaCompatible(want, got *vectorized.BatchSchema) error {
	if len(want.Columns) != len(got.Columns) {
		return fmt.Errorf("column count mismatch: got %d, want %d", len(got.Columns), len(want.Columns))
	}
	for i, wantCol := range want.Columns {
		gotCol := got.Columns[i]
		if wantCol.Name != gotCol.Name || wantCol.Role != gotCol.Role || wantCol.Type != gotCol.Type {
			return fmt.Errorf("column %d mismatch: got %+v, want %+v", i, gotCol, wantCol)
		}
	}
	return nil
}

// checkPartialLayouts checks every non-empty partial against target before
// anything is allocated. The liaison drops projected tags a group doesn't
// define from that group's request, so a partial may lack non-key tags,
// which the reduce null-fills. A non-key tag may also travel as TagValue
// rather than its planned native type, since a data node only narrows a
// passthrough column whose cells share one type. Any other difference (a
// key tag, a field or aggregate-state column, a metadata column, an extra
// column, or another type) is a mismatch. A nil target checks nothing.
func checkPartialLayouts(partials []*vectorized.RecordBatch, target *vectorized.BatchSchema, keyTagFamily string, keyTagNames []string) error {
	if target == nil {
		return nil
	}
	nonKeyTag := func(def vectorized.ColumnDef) bool {
		return def.Role == vectorized.RoleTag && (def.TagFamily != keyTagFamily || !slices.Contains(keyTagNames, def.Name))
	}
	for i, b := range partials {
		if b == nil || b.Len == 0 {
			continue
		}
		for _, def := range b.Schema.Columns {
			if columnPosition(target.Columns, def) < 0 {
				return fmt.Errorf("partial %d: unexpected column %s", i, columnLabel(def))
			}
		}
		for _, def := range target.Columns {
			src := columnPosition(b.Schema.Columns, def)
			switch {
			case src < 0 && !nonKeyTag(def):
				return fmt.Errorf("partial %d: missing column %s", i, columnLabel(def))
			case src < 0:
			case b.Schema.Columns[src].Type == def.Type:
			case nonKeyTag(def) && (b.Schema.Columns[src].Type == vectorized.ColumnTypeTagValue || def.Type == vectorized.ColumnTypeTagValue):
			default:
				return fmt.Errorf("partial %d: column %s is %v, want %v", i, columnLabel(def), b.Schema.Columns[src].Type, def.Type)
			}
		}
	}
	return nil
}

// reshapeCellBytes and reshapeRowBytes match the per-cell and per-row
// charge the liaison reserves for received native frames.
const (
	reshapeCellBytes = 128
	reshapeRowBytes  = 128
)

// reserveReshapeChunk reserves one batchSize-row chunk at target's width.
func reserveReshapeChunk(tracker *vectorized.MemoryTracker, target *vectorized.BatchSchema, batchSize int) (int64, error) {
	cols := int64(len(target.Columns))
	if cols > (math.MaxInt64/int64(batchSize)-reshapeRowBytes)/reshapeCellBytes {
		return 0, fmt.Errorf("reshape chunk estimate overflows")
	}
	amount := int64(batchSize) * (cols*reshapeCellBytes + reshapeRowBytes)
	if reserveErr := tracker.Reserve(amount); reserveErr != nil {
		return 0, reserveErr
	}
	return amount, nil
}

// consumeReshaped feeds b to op in chunks of up to batchSize rows laid out
// as scratch's schema. Tags b lacks are null-filled, TagValue cells of a
// non-key tag are read as its planned type, and native cells are wrapped
// when the plan keeps the tag as TagValue.
func consumeReshaped(op *BatchAggregation, scratch, b *vectorized.RecordBatch, batchSize int) error {
	cols := scratch.Schema.Columns
	src := make([]int, len(cols))
	for i, def := range cols {
		src[i] = columnPosition(b.Schema.Columns, def)
	}
	n := b.ActiveLen()
	for start := 0; start < n; start += batchSize {
		end := min(start+batchSize, n)
		scratch.Reset()
		for i, def := range cols {
			if src[i] < 0 {
				for range end - start {
					scratch.Columns[i].AppendNull()
				}
				continue
			}
			if copyErr := appendActiveRows(scratch.Columns[i], b.Columns[src[i]], b.Selection, start, end); copyErr != nil {
				return fmt.Errorf("column %s: %w", columnLabel(def), copyErr)
			}
		}
		scratch.Len = end - start
		if consumeErr := op.Consume(context.Background(), scratch); consumeErr != nil {
			return consumeErr
		}
	}
	return nil
}

// appendActiveRows copies active rows [start, end) of src into dst,
// converting between a native column and TagValue cells as needed.
func appendActiveRows(dst, src vectorized.Column, selection []uint16, start, end int) error {
	appendRange := AppendColumnRangeCoerced
	if src.Type() == vectorized.ColumnTypeTagValue && dst.Type() != vectorized.ColumnTypeTagValue {
		appendRange = appendTagValuesAsNative
	}
	if selection == nil {
		return appendRange(dst, src, start, end-start)
	}
	for _, rowIdx := range selection[start:end] {
		if appendErr := appendRange(dst, src, int(rowIdx), 1); appendErr != nil {
			return appendErr
		}
	}
	return nil
}

func columnPosition(cols []vectorized.ColumnDef, def vectorized.ColumnDef) int {
	return slices.IndexFunc(cols, func(c vectorized.ColumnDef) bool {
		return c.Role == def.Role && c.TagFamily == def.TagFamily && c.Name == def.Name
	})
}

func columnLabel(def vectorized.ColumnDef) string {
	if def.Role == vectorized.RoleTag {
		return def.TagFamily + "." + def.Name
	}
	return def.Name
}

// ApplyTopToReduce composes the Reduce output through a BatchTop operator,
// returning the top-N rows by FieldName. This is the liaison-side
// completion of the distributed Top-over-Agg pattern: data nodes emit
// AggModeMap partials (optionally pre-topped via the per-node BatchTop),
// the liaison Reduces them, then a second BatchTop selects the global
// top-N. Mirrors the row path's two-pass Top approach — sorting by
// partial value on each node, then re-sorting the merged reductions on
// the coordinator.
//
// fieldName MUST match a RoleField column in the reduced output schema
// (typically the AggReduceSpec.OutputName). N <= 0 returns reduced input
// unchanged.
//
// reduced is normally ReduceRawFrames/ReducePartialBatches's own
// NextBatch-paginated output, so in practice no single batch here exceeds
// batchSize rows. But this function does not control its caller's batching
// discipline either, and BatchTop.Consume shares BatchAggregation's exposure
// to the design/0.12.0/limit-after-aggregation §5 uint16 wraparound (both
// drive their per-row loop from activeIndices), so an oversized batch is
// chunked via ConsumeChunked rather than assumed away.
func ApplyTopToReduce(reduced []*vectorized.RecordBatch, spec ReduceTopSpec, batchSize int) ([]*vectorized.RecordBatch, error) {
	if spec.N <= 0 || len(reduced) == 0 {
		return reduced, nil
	}
	schema := reduced[0].Schema
	fieldIdx := -1
	for i, def := range schema.Columns {
		if def.Role == vectorized.RoleField && def.Name == spec.FieldName {
			fieldIdx = i
			break
		}
	}
	if fieldIdx < 0 {
		return nil, fmt.Errorf("ApplyTopToReduce: field %q not present in reduced schema", spec.FieldName)
	}
	top := NewBatchTop(schema, fieldIdx, spec.N, spec.Asc, batchSize)
	defer top.Close()
	if initErr := top.Init(context.Background()); initErr != nil {
		return nil, fmt.Errorf("ApplyTopToReduce: init: %w", initErr)
	}
	var scratch *vectorized.RecordBatch
	for i, b := range reduced {
		if compatErr := schemaCompatible(schema, b.Schema); compatErr != nil {
			return nil, fmt.Errorf("ApplyTopToReduce: batch %d schema mismatch: %w", i, compatErr)
		}
		if b.ActiveLen() <= batchSize {
			if consumeErr := top.Consume(context.Background(), b); consumeErr != nil {
				return nil, fmt.Errorf("ApplyTopToReduce: consume batch %d: %w", i, consumeErr)
			}
			continue
		}
		if scratch == nil {
			scratch = vectorized.NewRecordBatch(schema, batchSize)
		}
		if consumeErr := vectorized.ConsumeChunked(top, scratch, b, batchSize); consumeErr != nil {
			return nil, fmt.Errorf("ApplyTopToReduce: consume batch %d: %w", i, consumeErr)
		}
	}
	if finalErr := top.Finalize(context.Background()); finalErr != nil {
		return nil, fmt.Errorf("ApplyTopToReduce: finalize: %w", finalErr)
	}
	var out []*vectorized.RecordBatch
	for {
		nb, nextErr := top.NextBatch(context.Background())
		if nextErr != nil {
			return nil, fmt.Errorf("ApplyTopToReduce: next: %w", nextErr)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out, nil
}

// FormatReduceOutput is a debug helper for the topology matrix harness:
// it renders the reduced batch as "key1=v1,key2=v2|<aggname>=<val>" lines
// for stable, sorted comparison against the AggModeAll oracle.
func FormatReduceOutput(b *vectorized.RecordBatch) string {
	if b == nil {
		return ""
	}
	var sb strings.Builder
	for r := 0; r < b.Len; r++ {
		if r > 0 {
			sb.WriteByte('\n')
		}
		first := true
		for ci, def := range b.Schema.Columns {
			if def.Role != vectorized.RoleTag {
				continue
			}
			if !first {
				sb.WriteByte(',')
			}
			first = false
			fmt.Fprintf(&sb, "%s=%s", def.Name, columnStringAt(b.Columns[ci], r))
		}
		sb.WriteByte('|')
		first = true
		for ci, def := range b.Schema.Columns {
			if def.Role != vectorized.RoleField {
				continue
			}
			if !first {
				sb.WriteByte(',')
			}
			first = false
			fmt.Fprintf(&sb, "%s=%s", def.Name, columnStringAt(b.Columns[ci], r))
		}
	}
	return sb.String()
}

// columnStringAt renders one cell as a stable string for the debug helper.
// Nullable cells render as "<null>". The integer / float / string / bytes
// types cover every column the reducer can produce.
func columnStringAt(col vectorized.Column, row int) string {
	if col.IsNull(row) {
		return "<null>"
	}
	switch c := col.(type) {
	case *vectorized.ConstColumn:
		return columnStringAt(c.Expand(), row)
	case *vectorized.TypedColumn[int64]:
		return fmt.Sprintf("%d", c.Data()[row])
	case *vectorized.TypedColumn[float64]:
		return fmt.Sprintf("%g", c.Data()[row])
	case *vectorized.TypedColumn[string]:
		return c.Data()[row]
	case *vectorized.TypedColumn[[]byte]:
		return fmt.Sprintf("%x", c.Data()[row])
	}
	return "<?>"
}
