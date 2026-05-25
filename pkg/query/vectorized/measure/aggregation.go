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
	"errors"
	"fmt"
	"slices"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// AggMode selects the per-node aggregation strategy.
//
//   - AggModeAll  — single-node full reduce. emits one final value per
//     (group, agg).
//   - AggModeMap  — distributed Map phase (G9f.2). Emits a typed-column
//     partial batch (one row per group) carrying the Partial state from
//     aggregation.Map: Value for SUM/COUNT/MIN/MAX, plus a sidecar Count
//     column (named "<output>__agg_count") for MEAN. The batch is prefixed
//     with a RoleShardID column populated from the input batch's shard-id
//     column at the first row that creates each group (matching the row
//     path's incidental "first-idp" rule, measure_plan_aggregation.go:285);
//     scalar reduce (len(keyIndices)==0) emits shard_id=0 (matching the row
//     path's aggAllIterator.Current() at :364). The partial batch is then
//     serialized by pkg/query/vectorized/measure/frame.Encode for cluster
//     transport.
//   - AggModeReduce — coordinator's Reduce phase (G9f.3). Consumes the
//     typed-column partial batches emitted by AggModeMap (one row per
//     (shard, group)), dedupes them on (shard_id, group_key) — replica
//     duplicates of the same shard collapse to one contribution, mirroring
//     the row path's deduplicateAggregatedDataPointsWithShard — and combines
//     them through aggregation.Reduce[N] into a final batch shaped like
//     AggModeAll (tags + final value column, no shard column, no count
//     sidecar). The aggregation.Reduce[N].Val() handles MEAN finalization
//     (sum÷count) so the emit path stays type-symmetric with AggModeAll.
type AggMode int

// AggMode values.
const (
	AggModeAll AggMode = iota
	AggModeMap
	AggModeReduce
)

// AggFunc selects the reduction function applied to a column.
type AggFunc int

// AggFunc values.
const (
	AggSum AggFunc = iota
	AggCount
	AggMin
	AggMax
	AggMean
)

// ErrAggModeNotImplemented is returned by Consume / Finalize / NextBatch when
// AggMode falls outside the implemented modes. AggModeAll and AggModeMap
// have been implemented since G9f.2; AggModeReduce ships in G9f.3. The
// error remains for any future mode addition that lands without operator
// support, so an out-of-range mode fails loud rather than silently
// degrading.
var ErrAggModeNotImplemented = errors.New("vectorized.measure: BatchAggregation mode not implemented")

// shardIDOutputName is the AggModeMap output column name for the leading
// RoleShardID column. The receiving end identifies it by role rather than
// name, but a stable name keeps debug dumps / tests readable.
const shardIDOutputName = "shard_id"

// meanCountSuffix is appended to a MEAN agg's output name to derive the
// per-agg count sidecar column emitted in AggModeMap. The suffix mirrors the
// row path's aggCountFieldName ("__agg_count" in
// pkg/query/logical/measure/measure_plan_aggregation.go:35) so vec and row
// share the same convention in any cross-path diagnostics.
const meanCountSuffix = "__agg_count"

// AggSpec configures one aggregation output column.
type AggSpec struct {
	Output   string
	Func     AggFunc
	InputCol int // index into the input schema; must be int64 or float64
}

// BatchAggregation is a BreakerOperator that groups input rows by the configured
// key columns and reduces value columns via the configured AggSpec list.
//
// Per-function arithmetic delegates to pkg/query/aggregation, the same package
// the row-based path uses. This keeps numeric semantics in lockstep across the
// two paths so a fix in one path is shared by the other.
//
// Output schema = all projected tag columns (in input schema order — keys
// AND non-key tags) followed by one column per AggSpec. Non-key tags are
// carried forward as the first-seen value per group, matching the row
// path's aggregator (pkg/query/logical/measure/measure_plan_aggregation.go),
// which preserves any tag the request projected even when it is not a
// GroupBy key.
//
// Output rows are emitted one per group, in group-insertion order,
// paginated by batchSize.
type BatchAggregation struct {
	dedupSeen        map[string]struct{}
	outputSchema     *vectorized.BatchSchema
	pool             *vectorized.BatchPool
	tracker          *vectorized.MemoryTracker
	span             *query.Span
	groups           map[string]*aggGroup
	inputSchema      *vectorized.BatchSchema
	insertion        []*aggGroup
	tagIndices       []int
	aggs             []AggSpec
	aggOutOffsets    []int
	aggHasCount      []bool
	keyIndices       []int
	aggInputCountIdx []int
	shardIDIdx       int
	tagOutOffset     int
	outputShardIdx   int
	mode             AggMode
	aggValuePath     AggValuePath
	entrySize        int64
	reserved         int64
	rowsIn           int64
	rowsOut          int64
	batchSize        int
	cursor           int
	closed           bool
}

// aggGroup carries one bucket's reduction state plus a copy of every
// projected tag column for this group. tagCols is indexed by position in
// BatchAggregation.tagIndices (NOT just the GroupBy keys), so non-key
// projected tags can be emitted as their first-seen value.
//
// shardID is captured by newGroup from the input batch's RoleShardID column
// at the first row that created the group, matching the row path's
// incidental "first-idp-of-group" rule (G9f.2.a; see
// pkg/query/logical/measure/measure_plan_aggregation.go:285). It is only
// read by AggModeMap emit; AggModeAll ignores it. Stays zero when
// keyIndices is empty (scalar reduce) so the emitted partial matches the
// row path's aggAllIterator.Current() hardcoding ShardId: 0.
type aggGroup struct {
	key     string
	tagCols []vectorized.Column
	slots   []aggSlot
	shardID int64
}

// aggSlot holds either:
//
//   - AggModeAll / AggModeMap: an aggregation.Map of int64 or float64
//     (Consume folds raw values; emit reads Val() / Partial()).
//   - AggModeReduce:           an aggregation.Reduce of int64 or float64
//     (Consume reads Partial state from the input columns and Combine()s;
//     emit reads Val() to produce the final reduced value).
//
// Whether int or float is decided at construction time by AggFunc + input
// column type:
//
//   - SUM/MIN/MAX: matches input type (preserve precision).
//   - COUNT:        always int64.
//   - MEAN:         always float64 (so int inputs yield fractional means).
//
// Exactly one of intMap/floatMap is non-nil in Map/All mode; exactly one of
// intReduce/floatReduce is non-nil in Reduce mode.
type aggSlot struct {
	intMap       aggregation.Map[int64]
	floatMap     aggregation.Map[float64]
	intReduce    aggregation.Reduce[int64]
	floatReduce  aggregation.Reduce[float64]
	fn           AggFunc
	inputIsFloat bool
}

// NewBatchAggregation constructs a BatchAggregation. It builds the output
// schema internally (keys + agg outputs) and owns its output BatchPool.
//
// tracker carries the per-pipeline memory budget; entrySize is the bytes
// reserved per new group bucket (key columns + slots + map entry overhead).
// Pass entrySize=0 to disable per-group bookkeeping. tracker must not be nil
// — use a large NewMemoryTracker for unit tests that don't care about budget.
func NewBatchAggregation(
	input *vectorized.BatchSchema, keyIndices []int,
	aggs []AggSpec, mode AggMode, batchSize int,
	tracker *vectorized.MemoryTracker, entrySize int64,
) *BatchAggregation {
	tagIndices := collectTagIndices(input, keyIndices)
	layout := buildAggOutputLayout(input, tagIndices, aggs, mode)
	return &BatchAggregation{
		inputSchema:      input,
		outputSchema:     layout.schema,
		pool:             vectorized.NewBatchPool(layout.schema, batchSize),
		tracker:          tracker,
		keyIndices:       slices.Clone(keyIndices),
		tagIndices:       tagIndices,
		aggs:             slices.Clone(aggs),
		aggOutOffsets:    layout.aggOutOffsets,
		aggHasCount:      layout.aggHasCount,
		outputShardIdx:   layout.outputShardIdx,
		tagOutOffset:     layout.tagOutOffset,
		shardIDIdx:       findShardIDIndex(input),
		aggInputCountIdx: buildAggInputCountIdx(input, aggs, mode),
		mode:             mode,
		aggValuePath:     deriveAggValuePath(input, aggs),
		batchSize:        batchSize,
		entrySize:        entrySize,
	}
}

// deriveAggValuePath inspects the pre-built AggSpec list to determine which
// column-type path was used. If any spec's InputCol points to a
// ColumnTypeFieldValue column the path is AggValuePathFieldValueFallback;
// otherwise it is AggValuePathTyped. AggValuePathUnresolved is never returned
// here — callers that build AggSpecs have already verified the columns exist.
func deriveAggValuePath(input *vectorized.BatchSchema, aggs []AggSpec) AggValuePath {
	for _, spec := range aggs {
		if spec.InputCol >= 0 && spec.InputCol < len(input.Columns) {
			if input.Columns[spec.InputCol].Type == vectorized.ColumnTypeFieldValue {
				return AggValuePathFieldValueFallback
			}
		}
	}
	return AggValuePathTyped
}

// collectTagIndices returns every tag column index in input, in input
// schema order. When the schema has no RoleTag columns at all (synthetic
// unit-test fixtures that pre-date the storage bridge), fall back to
// keyIndices so the operator still produces the keys-only output those
// tests expect. Production paths always have RoleTag columns because
// BuildBatchSchema emits one per projected tag.
func collectTagIndices(input *vectorized.BatchSchema, keyIndices []int) []int {
	out := make([]int, 0, len(input.Columns))
	for i, def := range input.Columns {
		if def.Role == vectorized.RoleTag {
			out = append(out, i)
		}
	}
	if len(out) == 0 {
		return slices.Clone(keyIndices)
	}
	return out
}

// Init prepares the group map. It does NOT validate the mode — mode rejection
// happens at the per-method level (Consume/Finalize/NextBatch) so the
// dispatcher matches the spec's distributed forward-compat language. For
// AggModeReduce, Init also resets the (shard, group_key) replica-dedup map
// so the operator can be reused across distributed reduce runs without a
// fresh allocation per call.
func (a *BatchAggregation) Init(ctx context.Context) error {
	a.groups = make(map[string]*aggGroup)
	if a.mode == AggModeReduce {
		a.dedupSeen = make(map[string]struct{})
	}
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		spanName := "groupby-agg-all"
		switch a.mode {
		case AggModeMap:
			spanName = "groupby-agg-map"
		case AggModeReduce:
			spanName = "groupby-agg-reduce"
		}
		a.span, _ = tracer.StartSpan(ctx, "%s", spanName)
	}
	return nil
}

// OutputSchema returns the schema of emitted batches: key columns followed by
// agg output columns.
func (a *BatchAggregation) OutputSchema() *vectorized.BatchSchema { return a.outputSchema }

// Consume folds every active row into its group's accumulator. Null values are
// excluded from aggregation (count not incremented; sum/min/max unchanged).
//
// Each new group reserves entrySize bytes from the shared MemoryTracker. If
// the budget is exhausted, Consume returns the wrapped tracker error and the
// row's group is not added — partial-batch state is consistent.
//
// AggModeAll and AggModeMap share the per-row fold path; they diverge only at
// emit time (NextBatch). AggModeReduce uses a parallel combinePartial path:
// each input row is one (shard, group) partial; replica duplicates (same
// shard + same group_key) are dropped via dedupSeen, then the Partial is
// Combine'd into the group's Reduce accumulator. Cross-shard rows that share
// a group_key are KEPT and combined, matching the row path's
// deduplicateAggregatedDataPointsWithShard semantics.
func (a *BatchAggregation) Consume(_ context.Context, b *vectorized.RecordBatch) error {
	if a.mode != AggModeAll && a.mode != AggModeMap && a.mode != AggModeReduce {
		return ErrAggModeNotImplemented
	}
	active := activeIndices(b)
	if a.span != nil {
		a.rowsIn += int64(len(active))
	}
	for _, rowIdx := range active {
		key := a.computeKey(b, int(rowIdx))
		if a.mode == AggModeReduce && a.markDedupSeen(b, int(rowIdx), key) {
			// markDedupSeen returns true ⇒ this (shard, group_key) pair
			// already arrived from a replica of the same shard; skip the
			// row so it does not double-count.
			continue
		}
		group, exists := a.groups[key]
		if !exists {
			if a.entrySize > 0 {
				if reserveErr := a.tracker.Reserve(a.entrySize); reserveErr != nil {
					return fmt.Errorf("aggregation memory budget exceeded: %w", reserveErr)
				}
				a.reserved += a.entrySize
			}
			newGroup, newErr := a.newGroup(b, int(rowIdx), key)
			if newErr != nil {
				return newErr
			}
			a.groups[key] = newGroup
			a.insertion = append(a.insertion, newGroup)
			group = newGroup
		}
		for slotIdx, spec := range a.aggs {
			if a.mode == AggModeReduce {
				a.combinePartial(b, int(rowIdx), &group.slots[slotIdx], spec, a.aggInputCountIdx[slotIdx])
				continue
			}
			a.fold(b, int(rowIdx), &group.slots[slotIdx], spec)
		}
	}
	return nil
}

// Finalize is a no-op for AggModeAll, AggModeMap, and AggModeReduce —
// Consume eagerly maintains every group's accumulator state (Map or
// Reduce), so there is no batched flush step.
func (a *BatchAggregation) Finalize(_ context.Context) error {
	if a.mode != AggModeAll && a.mode != AggModeMap && a.mode != AggModeReduce {
		return ErrAggModeNotImplemented
	}
	return nil
}

// NextBatch emits one row per group in group-insertion order, paginated by
// batchSize. AggModeAll emits final values; AggModeMap emits typed-column
// partial state plus a leading shard-id column (see AggMode docs).
// AggModeReduce emits the same shape as AggModeAll — tags + final reduced
// value — but draws the value from aggregation.Reduce[N].Val() so MEAN
// finalization (sum÷count) happens inside the reducer.
func (a *BatchAggregation) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if a.mode != AggModeAll && a.mode != AggModeMap && a.mode != AggModeReduce {
		return nil, ErrAggModeNotImplemented
	}
	if a.cursor >= len(a.insertion) {
		return nil, nil
	}
	out := a.pool.Get()
	for out.Len < a.batchSize && a.cursor < len(a.insertion) {
		group := a.insertion[a.cursor]
		a.emitGroupRow(out, group)
		out.Len++
		a.cursor++
		if a.span != nil {
			a.rowsOut++
		}
	}
	if out.Len == 0 {
		a.pool.Put(out)
		return nil, nil
	}
	return out, nil
}

// Close releases the group map and refunds the outstanding memory
// reservation. Idempotent.
func (a *BatchAggregation) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	memoryChargedBytes := a.reserved
	if a.reserved > 0 {
		a.tracker.Release(a.reserved)
		a.reserved = 0
	}
	if a.span != nil {
		mode := "all"
		switch a.mode {
		case AggModeMap:
			mode = "map"
		case AggModeReduce:
			mode = "reduce"
		}
		a.span.Tag(tracelabels.TagMode, mode)
		a.span.Tagf(tracelabels.TagRowsIn, "%d", a.rowsIn)
		a.span.Tagf(tracelabels.TagRowsOut, "%d", a.rowsOut)
		a.span.Tagf(tracelabels.TagGroupsOut, "%d", len(a.insertion))
		a.span.Tagf(tracelabels.TagMemoryChargedBytes, "%d", memoryChargedBytes)
		if a.mode == AggModeMap {
			a.span.Tag(tracelabels.TagAggValuePath, string(a.aggValuePath))
		}
		a.span.Stop()
	}
	a.groups = nil
	a.insertion = nil
	return nil
}

func (a *BatchAggregation) newGroup(b *vectorized.RecordBatch, rowIdx int, key string) (*aggGroup, error) {
	tagCols := make([]vectorized.Column, len(a.tagIndices))
	for i, tIdx := range a.tagIndices {
		tagCols[i] = vectorized.NewColumnForType(a.inputSchema.Columns[tIdx].Type, 1)
		copyOneValue(tagCols[i], b.Columns[tIdx], rowIdx)
	}
	slots := make([]aggSlot, len(a.aggs))
	for i, spec := range a.aggs {
		inputIsFloat := a.inputSchema.Columns[spec.InputCol].Type == vectorized.ColumnTypeFloat64
		slot, slotErr := newAggSlot(spec.Func, inputIsFloat, a.mode)
		if slotErr != nil {
			return nil, slotErr
		}
		slots[i] = slot
	}
	g := &aggGroup{key: key, tagCols: tagCols, slots: slots}
	// Capture the first-fed-idp's shard for AggModeMap on grouped agg, mirroring
	// the row path's incidental rule at measure_plan_aggregation.go:285 (the
	// G9f.2.a semantic-repro requirement). Scalar reduce (len(keyIndices)==0)
	// leaves shardID at zero — matching the row path's aggAllIterator.Current()
	// hardcoding ShardId: 0 at :364. AggModeAll never reads the field.
	if a.mode == AggModeMap && a.shardIDIdx >= 0 && len(a.keyIndices) > 0 {
		if shardCol, ok := b.Columns[a.shardIDIdx].(*vectorized.TypedColumn[int64]); ok {
			data := shardCol.Data()
			if rowIdx >= 0 && rowIdx < len(data) {
				g.shardID = data[rowIdx]
			}
		}
	}
	return g, nil
}

// fold delegates one row's value to the slot's underlying aggregation.Map.
// Nulls are skipped — neither the count nor the running min/max/sum is touched.
func (a *BatchAggregation) fold(b *vectorized.RecordBatch, rowIdx int, slot *aggSlot, spec AggSpec) {
	col := b.Columns[spec.InputCol]
	if col.IsNull(rowIdx) {
		return
	}
	if slot.intMap != nil {
		var v int64
		if slot.inputIsFloat {
			v = int64(col.(*vectorized.TypedColumn[float64]).Data()[rowIdx])
		} else {
			v = col.(*vectorized.TypedColumn[int64]).Data()[rowIdx]
		}
		slot.intMap.In(v)
		return
	}
	var v float64
	if slot.inputIsFloat {
		v = col.(*vectorized.TypedColumn[float64]).Data()[rowIdx]
	} else {
		v = float64(col.(*vectorized.TypedColumn[int64]).Data()[rowIdx])
	}
	slot.floatMap.In(v)
}

func (a *BatchAggregation) emitGroupRow(out *vectorized.RecordBatch, group *aggGroup) {
	// Optional leading RoleShardID column (AggModeMap only). outputShardIdx is
	// -1 in AggModeAll, 0 in AggModeMap (see buildAggOutputLayout).
	if a.outputShardIdx >= 0 {
		out.Columns[a.outputShardIdx].(*vectorized.TypedColumn[int64]).Append(group.shardID)
	}
	// Projected tag columns, in tagIndices order — including non-key tags
	// carried forward as the first-seen value. tagOutOffset is 0 in
	// AggModeAll and 1 in AggModeMap (after the shard-id column).
	for i := range a.tagIndices {
		copyOneValue(out.Columns[a.tagOutOffset+i], group.tagCols[i], 0)
	}
	// Agg output columns. AggModeAll emits one Val() per slot; AggModeMap
	// emits Partial().Value plus a sidecar Partial().Count for MEAN (per
	// aggHasCount); AggModeReduce emits one Reduce.Val() per slot — same
	// shape as AggModeAll. Offsets are precomputed in aggOutOffsets so the
	// emit loop does no per-row schema walking.
	for slotIdx := range a.aggs {
		slot := &group.slots[slotIdx]
		valueIdx := a.aggOutOffsets[slotIdx]
		switch a.mode {
		case AggModeMap:
			countIdx := -1
			if a.aggHasCount[slotIdx] {
				countIdx = valueIdx + 1
			}
			slot.writePartial(out, valueIdx, countIdx)
		case AggModeReduce:
			slot.writeReduce(out.Columns[valueIdx])
		default:
			slot.write(out.Columns[valueIdx])
		}
	}
}

func (a *BatchAggregation) computeKey(b *vectorized.RecordBatch, rowIdx int) string {
	// Shared encoding with BatchGroupBy (length-prefixed variable components,
	// canonicalised float zero) — see appendKeyComponent in groupby.go.
	var sb [64]byte
	buf := sb[:0]
	for _, kIdx := range a.keyIndices {
		buf = appendKeyComponent(buf, b.Columns[kIdx], rowIdx)
	}
	return string(buf)
}

// newAggSlot builds the accumulator for the (function, input type, mode)
// triple. AggModeAll / AggModeMap allocate an aggregation.Map (raw fold +
// optional Partial export); AggModeReduce allocates an aggregation.Reduce
// (Combine partials + final Val). The numeric type mirrors aggOutputType so
// the slot's value can be Append'd directly to the typed output column.
func newAggSlot(fn AggFunc, inputIsFloat bool, mode AggMode) (aggSlot, error) {
	af, modelErr := toModelAggFunc(fn)
	if modelErr != nil {
		return aggSlot{}, modelErr
	}
	slot := aggSlot{fn: fn, inputIsFloat: inputIsFloat}
	// All functions follow the input type to match the row path, whose
	// aggregation.NewMap[int64] / [float64] is dispatched on the field's
	// declared type in pkg/query/logical/measure/measure_plan_aggregation.go
	// (FIELD_TYPE_INT → int64; FIELD_TYPE_FLOAT → float64). COUNT is
	// included: the row path's countFunc[N] is parameterized by N and
	// ToFieldValue[N] emits FieldValue_Int / FieldValue_Float by N, so
	// COUNT on a float field must emit a float (e.g. float_top_count).
	useFloat := inputIsFloat
	if mode == AggModeReduce {
		if useFloat {
			r, reduceErr := aggregation.NewReduce[float64](af)
			if reduceErr != nil {
				return aggSlot{}, reduceErr
			}
			slot.floatReduce = r
		} else {
			r, reduceErr := aggregation.NewReduce[int64](af)
			if reduceErr != nil {
				return aggSlot{}, reduceErr
			}
			slot.intReduce = r
		}
		return slot, nil
	}
	if useFloat {
		m, mapErr := aggregation.NewMap[float64](af)
		if mapErr != nil {
			return aggSlot{}, mapErr
		}
		slot.floatMap = m
	} else {
		m, mapErr := aggregation.NewMap[int64](af)
		if mapErr != nil {
			return aggSlot{}, mapErr
		}
		slot.intMap = m
	}
	return slot, nil
}

// write emits the slot's reduced value to the typed output column.
func (s *aggSlot) write(col vectorized.Column) {
	if s.intMap != nil {
		col.(*vectorized.TypedColumn[int64]).Append(s.intMap.Val())
		return
	}
	col.(*vectorized.TypedColumn[float64]).Append(s.floatMap.Val())
}

// writePartial emits the slot's Partial() to the AggModeMap output. The
// Value lands at out.Columns[valueIdx]; when countIdx >= 0 (MEAN only)
// the Count sidecar lands at out.Columns[countIdx]. The count column has
// the same numeric type as the value column (aggregation.Partial[N] uses
// the same N for both), matching the row path's per-N FieldValue oneof
// (FIELD_TYPE_INT → int64, FIELD_TYPE_FLOAT → float64).
func (s *aggSlot) writePartial(out *vectorized.RecordBatch, valueIdx, countIdx int) {
	if s.intMap != nil {
		p := s.intMap.Partial()
		out.Columns[valueIdx].(*vectorized.TypedColumn[int64]).Append(p.Value)
		if countIdx >= 0 {
			out.Columns[countIdx].(*vectorized.TypedColumn[int64]).Append(p.Count)
		}
		return
	}
	p := s.floatMap.Partial()
	out.Columns[valueIdx].(*vectorized.TypedColumn[float64]).Append(p.Value)
	if countIdx >= 0 {
		out.Columns[countIdx].(*vectorized.TypedColumn[float64]).Append(p.Count)
	}
}

func toModelAggFunc(fn AggFunc) (modelv1.AggregationFunction, error) {
	switch fn {
	case AggSum:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, nil
	case AggCount:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, nil
	case AggMin:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN, nil
	case AggMax:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX, nil
	case AggMean:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN, nil
	}
	return modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED,
		fmt.Errorf("vectorized.measure: unknown AggFunc %d", fn)
}

// aggOutputLayout captures the AggModeAll / AggModeMap output-batch layout
// produced by buildAggOutputLayout. It is plumbed onto BatchAggregation so
// emitGroupRow does no per-row schema walking.
type aggOutputLayout struct {
	schema *vectorized.BatchSchema
	// aggOutOffsets[i] is the output-batch column index of the i-th agg's
	// VALUE column. When aggHasCount[i] is true (AggModeMap + AggMean only),
	// the agg's count sidecar lives at aggOutOffsets[i]+1.
	aggOutOffsets []int
	// aggHasCount[i] is true iff the i-th agg emits a Partial.Count sidecar
	// — i.e. AggModeMap + AggMean. Other modes/funcs are false.
	aggHasCount []bool
	// outputShardIdx is the output-batch column index of the leading
	// RoleShardID column. -1 in AggModeAll (no shard column emitted);
	// 0 in AggModeMap (the partial batch always carries shard id first).
	outputShardIdx int
	// tagOutOffset is the output-batch column index where the tag columns
	// begin (0 in AggModeAll; 1 in AggModeMap).
	tagOutOffset int
}

// buildAggOutputLayout derives the output-batch ColumnDef list AND the
// per-agg / shard-id index bookkeeping for a given (input schema, tag
// indices, agg specs, mode). It is the sole place that decides Map-mode's
// shard-id-first + MEAN-emits-two-columns layout, so emit-time code can
// just consult precomputed offsets.
func buildAggOutputLayout(
	input *vectorized.BatchSchema, tagIndices []int, aggs []AggSpec, mode AggMode,
) aggOutputLayout {
	// Worst-case capacity: shard-id (1) + tags + 2 per agg (MEAN value + count).
	defs := make([]vectorized.ColumnDef, 0, 1+len(tagIndices)+2*len(aggs))
	layout := aggOutputLayout{
		aggOutOffsets:  make([]int, len(aggs)),
		aggHasCount:    make([]bool, len(aggs)),
		outputShardIdx: -1,
	}
	if mode == AggModeMap {
		defs = append(defs, vectorized.ColumnDef{
			Role: vectorized.RoleShardID,
			Name: shardIDOutputName,
			Type: vectorized.ColumnTypeInt64,
		})
		layout.outputShardIdx = 0
	}
	layout.tagOutOffset = len(defs)
	for _, ti := range tagIndices {
		defs = append(defs, input.Columns[ti])
	}
	for i, agg := range aggs {
		layout.aggOutOffsets[i] = len(defs)
		valueType := aggOutputType(input.Columns[agg.InputCol].Type, agg.Func)
		defs = append(defs, vectorized.ColumnDef{
			Role: vectorized.RoleField,
			Name: agg.Output,
			Type: valueType,
		})
		if mode == AggModeMap && agg.Func == AggMean {
			layout.aggHasCount[i] = true
			defs = append(defs, vectorized.ColumnDef{
				Role: vectorized.RoleField,
				Name: agg.Output + meanCountSuffix,
				Type: valueType,
			})
		}
	}
	layout.schema = vectorized.NewBatchSchema(defs)
	return layout
}

// findShardIDIndex returns the input-schema column index of the RoleShardID
// column, or -1 when input has none (unit-test fixtures that pre-date the
// storage bridge). AggModeMap consults this in newGroup to capture the
// first-fed-idp shard per group (G9f.2.a).
func findShardIDIndex(schema *vectorized.BatchSchema) int {
	for i, def := range schema.Columns {
		if def.Role == vectorized.RoleShardID {
			return i
		}
	}
	return -1
}

// aggOutputType maps (input type, agg func) to the output column type.
// Every function (COUNT included) preserves the input type so vec egress
// emits the same FieldValue oneof variant the row path uses: the row
// path's accumulator and ToFieldValue[N] are dispatched on the field's
// declared type (FIELD_TYPE_INT → int64 → FieldValue_Int;
// FIELD_TYPE_FLOAT → float64 → FieldValue_Float; see
// measure_plan_aggregation.go and pkg/query/aggregation).
func aggOutputType(in vectorized.ColumnType, _ AggFunc) vectorized.ColumnType {
	if in == vectorized.ColumnTypeFieldValue {
		return vectorized.ColumnTypeInt64
	}
	return in
}
