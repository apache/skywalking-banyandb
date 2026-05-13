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
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// AggMode selects the per-node aggregation strategy. v1 implements AggModeAll
// (single-node, full reduce). Map and Reduce are dispatcher slots reserved for
// future distributed work; they return ErrAggModeNotImplemented today so the
// switch site does not need to change when distributed lands.
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
// AggMode is Map or Reduce. v1 implements only AggModeAll; the dispatcher
// exists at the per-method level so distributed mode fills in those branches
// without an interface change.
var ErrAggModeNotImplemented = errors.New("vectorized.measure: AggMode Map/Reduce not implemented in v1")

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
// Output schema = key columns (same definitions as input) + one column per AggSpec.
// Output rows are emitted one per group, in group-insertion order, paginated by batchSize.
type BatchAggregation struct {
	inputSchema  *vectorized.BatchSchema
	outputSchema *vectorized.BatchSchema
	pool         *vectorized.BatchPool
	tracker      *vectorized.MemoryTracker
	groups       map[string]*aggGroup
	insertion    []*aggGroup
	keyIndices   []int
	aggs         []AggSpec
	mode         AggMode
	entrySize    int64
	reserved     int64
	batchSize    int
	cursor       int
	closed       bool
}

// aggGroup carries one bucket's reduction state plus a copy of its key column values.
type aggGroup struct {
	key     string
	keyCols []vectorized.Column
	slots   []aggSlot
}

// aggSlot holds an aggregation.Map of either int64 or float64. Whether int or
// float is decided at construction time by AggFunc + input column type:
//
//   - SUM/MIN/MAX: matches input type (preserve precision).
//   - COUNT:        always int64.
//   - MEAN:         always float64 (so int inputs yield fractional means).
//
// Exactly one of intMap/floatMap is non-nil per slot.
type aggSlot struct {
	intMap       aggregation.Map[int64]
	floatMap     aggregation.Map[float64]
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
	outputSchema := buildAggOutputSchema(input, keyIndices, aggs)
	return &BatchAggregation{
		inputSchema:  input,
		outputSchema: outputSchema,
		pool:         vectorized.NewBatchPool(outputSchema, batchSize),
		tracker:      tracker,
		keyIndices:   slices.Clone(keyIndices),
		aggs:         slices.Clone(aggs),
		mode:         mode,
		batchSize:    batchSize,
		entrySize:    entrySize,
	}
}

// Init prepares the group map. It does NOT validate the mode — mode rejection
// happens at the per-method level (Consume/Finalize/NextBatch) so the
// dispatcher matches the spec's distributed forward-compat language.
func (a *BatchAggregation) Init(_ context.Context) error {
	a.groups = make(map[string]*aggGroup)
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
func (a *BatchAggregation) Consume(_ context.Context, b *vectorized.RecordBatch) error {
	if a.mode != AggModeAll {
		return ErrAggModeNotImplemented
	}
	active := activeIndices(b)
	for _, rowIdx := range active {
		key := a.computeKey(b, int(rowIdx))
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
			a.fold(b, int(rowIdx), &group.slots[slotIdx], spec)
		}
	}
	return nil
}

// Finalize rejects unsupported modes; AggModeAll is a no-op (accumulators
// are eagerly maintained in Consume).
func (a *BatchAggregation) Finalize(_ context.Context) error {
	if a.mode != AggModeAll {
		return ErrAggModeNotImplemented
	}
	return nil
}

// NextBatch emits aggregated rows in group-insertion order, paginated by batchSize.
func (a *BatchAggregation) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if a.mode != AggModeAll {
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
	if a.reserved > 0 {
		a.tracker.Release(a.reserved)
		a.reserved = 0
	}
	a.groups = nil
	a.insertion = nil
	return nil
}

func (a *BatchAggregation) newGroup(b *vectorized.RecordBatch, rowIdx int, key string) (*aggGroup, error) {
	keyCols := make([]vectorized.Column, len(a.keyIndices))
	for i, kIdx := range a.keyIndices {
		keyCols[i] = vectorized.NewColumnForType(a.inputSchema.Columns[kIdx].Type, 1)
		copyOneValue(keyCols[i], b.Columns[kIdx], rowIdx)
	}
	slots := make([]aggSlot, len(a.aggs))
	for i, spec := range a.aggs {
		inputIsFloat := a.inputSchema.Columns[spec.InputCol].Type == vectorized.ColumnTypeFloat64
		slot, slotErr := newAggSlot(spec.Func, inputIsFloat)
		if slotErr != nil {
			return nil, slotErr
		}
		slots[i] = slot
	}
	return &aggGroup{key: key, keyCols: keyCols, slots: slots}, nil
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
	// Key columns come first in the output schema, in keyIndices order.
	for i := range a.keyIndices {
		copyOneValue(out.Columns[i], group.keyCols[i], 0)
	}
	// Then agg output columns.
	for slotIdx := range a.aggs {
		colIdx := len(a.keyIndices) + slotIdx
		group.slots[slotIdx].write(out.Columns[colIdx])
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

// newAggSlot builds an aggregation.Map of the appropriate numeric type for the
// (function, input type) pair. The mapping rules mirror aggOutputType so the
// slot's value can be Append'd directly to the typed output column.
func newAggSlot(fn AggFunc, inputIsFloat bool) (aggSlot, error) {
	af, modelErr := toModelAggFunc(fn)
	if modelErr != nil {
		return aggSlot{}, modelErr
	}
	slot := aggSlot{fn: fn, inputIsFloat: inputIsFloat}
	useFloat := false
	switch fn {
	case AggCount:
		useFloat = false
	case AggMean:
		useFloat = true
	default:
		useFloat = inputIsFloat
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

func buildAggOutputSchema(
	input *vectorized.BatchSchema, keyIndices []int, aggs []AggSpec,
) *vectorized.BatchSchema {
	defs := make([]vectorized.ColumnDef, 0, len(keyIndices)+len(aggs))
	for _, ki := range keyIndices {
		defs = append(defs, input.Columns[ki])
	}
	for _, agg := range aggs {
		defs = append(defs, vectorized.ColumnDef{
			Role: vectorized.RoleField,
			Name: agg.Output,
			Type: aggOutputType(input.Columns[agg.InputCol].Type, agg.Func),
		})
	}
	return vectorized.NewBatchSchema(defs)
}

// aggOutputType maps (input type, agg func) to the output column type.
//   - COUNT is always int64.
//   - MEAN is always float64.
//   - SUM/MIN/MAX preserve the input type.
func aggOutputType(in vectorized.ColumnType, fn AggFunc) vectorized.ColumnType {
	switch fn {
	case AggCount:
		return vectorized.ColumnTypeInt64
	case AggMean:
		return vectorized.ColumnTypeFloat64
	case AggSum, AggMin, AggMax:
		return in
	}
	return in
}
