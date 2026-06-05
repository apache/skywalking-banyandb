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
	"encoding/binary"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// buildAggInputCountIdx returns, per agg in aggs, the input-schema column
// index of the agg's PartialCount sidecar. The sidecar is the next column
// after the agg's value column whose name equals "<value>__agg_count"
// (mirroring the AggModeMap output convention in
// buildAggOutputLayout / meanCountSuffix). The slot is -1 when no sidecar
// exists — i.e. non-MEAN aggs in any mode, or any agg under
// AggModeAll/AggModeMap (those modes never consume Partial state). The
// slot lookup is keyed on column NAME rather than positional offset so
// that a producer reordering the count sidecar (or omitting it for
// non-MEAN) does not silently mis-bind here.
func buildAggInputCountIdx(input *vectorized.BatchSchema, aggs []AggSpec, mode AggMode) []int {
	idx := make([]int, len(aggs))
	for i := range aggs {
		idx[i] = -1
	}
	if mode != AggModeReduce {
		return idx
	}
	for i, agg := range aggs {
		if agg.Func != AggMean {
			continue
		}
		valueDef := input.Columns[agg.InputCol]
		countName := valueDef.Name + meanCountSuffix
		// Walk forward from the value column — count sidecar must follow
		// its value column in the AggModeMap output, so a sequential scan
		// finds it in O(1) without rebuilding a name-to-index map.
		for j := agg.InputCol + 1; j < len(input.Columns); j++ {
			if input.Columns[j].Name == countName {
				idx[i] = j
				break
			}
		}
	}
	return idx
}

// markDedupSeen records (shardID, groupKey) in the replica-dedup map and
// reports whether the pair was already present. Returns true when the row
// is a replica duplicate that the caller must skip; false when the row is
// new and has just been recorded. AggModeReduce only — the caller guards
// the invocation on mode so this helper does not re-check.
//
// dedupSeen keys on a length-prefixed (shardID || groupKey) byte string,
// matching the row path's hashWithShard semantics in
// measure_plan_distributed.go: same shard + same group ⇒ duplicate;
// different shards with the same group are preserved (their keys differ
// in the shardID prefix and Combine into the same group bucket without
// being dedup'd).
//
// When the input schema has no RoleShardID column (shardIDIdx == -1 — a
// degenerate fixture that pre-dates the storage bridge), dedup falls back
// to the group key alone. This is the safest behavior: we cannot
// distinguish replicas without a shard id, so we treat every (group_key)
// pair as the single contribution.
func (a *BatchAggregation) markDedupSeen(b *vectorized.RecordBatch, rowIdx int, groupKey string) bool {
	var shardID int64
	if a.shardIDIdx >= 0 {
		if shardCol, ok := b.Columns[a.shardIDIdx].(*vectorized.TypedColumn[int64]); ok {
			data := shardCol.Data()
			if rowIdx >= 0 && rowIdx < len(data) {
				shardID = data[rowIdx]
			}
		}
	}
	var sb [8 + 64]byte
	dedupKey := binary.LittleEndian.AppendUint64(sb[:0], uint64(shardID))
	dedupKey = append(dedupKey, groupKey...)
	keyStr := string(dedupKey)
	if _, seen := a.dedupSeen[keyStr]; seen {
		return true
	}
	a.dedupSeen[keyStr] = struct{}{}
	return false
}

// combinePartial reads one input row's Partial state into the slot's
// aggregation.Reduce accumulator. The value column is spec.InputCol; the
// count sidecar (MEAN only) lives at countIdx (-1 if absent). Null value
// rows are skipped (consistent with Map-side fold, which also skips nulls
// when accumulating — see fold above).
//
// For non-MEAN aggregations Partial.Count is left zero; aggregation.Reduce's
// Combine ignores it and only inspects Value. For MEAN, Partial.Count is
// the per-shard rowcount and Combine.Val divides total Sum by total Count.
//
// The Reduce accumulator's numeric type matches the slot (intReduce or
// floatReduce). The input column type must agree with the slot's
// inputIsFloat — a mismatch is a producer/planner bug, not a data error,
// so this code asserts the type via the cast and panics on a mismatch
// (the planner builds Map and Reduce in matched pairs, so a divergence
// is unrecoverable).
func (a *BatchAggregation) combinePartial(b *vectorized.RecordBatch, rowIdx int, slot *aggSlot, spec AggSpec, countIdx int) {
	col := b.Columns[spec.InputCol]
	if col.IsNull(rowIdx) {
		return
	}
	if slot.inputIsFloat {
		p := aggregation.Partial[float64]{Value: partialFloatValue(col, rowIdx)}
		if countIdx >= 0 {
			p.Count = partialFloatValue(b.Columns[countIdx], rowIdx)
		}
		slot.floatReduce.Combine(p)
		return
	}
	p := aggregation.Partial[int64]{Value: partialIntValue(col, rowIdx)}
	if countIdx >= 0 {
		p.Count = partialIntValue(b.Columns[countIdx], rowIdx)
	}
	slot.intReduce.Combine(p)
}

func partialIntValue(col vectorized.Column, rowIdx int) int64 {
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		return c.Data()[rowIdx]
	case *vectorized.TypedColumn[float64]:
		return int64(c.Data()[rowIdx])
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		fv := c.Data()[rowIdx]
		if fv == nil {
			return 0
		}
		switch value := fv.GetValue().(type) {
		case *modelv1.FieldValue_Int:
			return value.Int.GetValue()
		case *modelv1.FieldValue_Float:
			return int64(value.Float.GetValue())
		}
		return 0
	default:
		valueCol := col.(*vectorized.TypedColumn[int64])
		return valueCol.Data()[rowIdx]
	}
}

func partialFloatValue(col vectorized.Column, rowIdx int) float64 {
	switch c := col.(type) {
	case *vectorized.TypedColumn[float64]:
		return c.Data()[rowIdx]
	case *vectorized.TypedColumn[int64]:
		return float64(c.Data()[rowIdx])
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		fv := c.Data()[rowIdx]
		if fv == nil {
			return 0
		}
		switch value := fv.GetValue().(type) {
		case *modelv1.FieldValue_Float:
			return value.Float.GetValue()
		case *modelv1.FieldValue_Int:
			return float64(value.Int.GetValue())
		}
		return 0
	default:
		valueCol := col.(*vectorized.TypedColumn[float64])
		return valueCol.Data()[rowIdx]
	}
}

// writeReduce emits the slot's reduced final value to the typed output
// column. Mirrors aggSlot.write but reads from the Reduce accumulator
// (whose Val() handles MEAN finalization by dividing Sum by Count).
func (s *aggSlot) writeReduce(col vectorized.Column) {
	if s.intReduce != nil {
		col.(*vectorized.TypedColumn[int64]).Append(s.intReduce.Val())
		return
	}
	col.(*vectorized.TypedColumn[float64]).Append(s.floatReduce.Val())
}
