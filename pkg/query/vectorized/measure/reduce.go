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
// keyTagNames selects which tags form the group key — the same names the
// request's GroupBy used. Tags present in the partial schema but NOT
// listed here are still carried forward in the output as the first-seen
// value per group (mirrors the data-node operator's first-seen non-key
// tag rule).
//
// Returns the final reduced batches in group-insertion order (paginated
// by batchSize) and the AggValuePath that describes how the value column
// was resolved. Callers walk the batches sequentially; one row per group.
func ReduceRawFrames(
	frames [][]byte,
	keyTagNames []string,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, AggValuePath, error) {
	decoded := make([]*vectorized.RecordBatch, 0, len(frames))
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
	return ReducePartialBatches(decoded, keyTagNames, aggSpecs, batchSize, tracker)
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
	keyTagNames []string,
	aggSpecs []AggReduceSpec,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, AggValuePath, error) {
	var refSchema *vectorized.BatchSchema
	for _, b := range partials {
		if b == nil || b.Len == 0 {
			continue
		}
		refSchema = b.Schema
		break
	}
	if refSchema == nil {
		return nil, AggValuePathTyped, nil
	}
	keyIndices, indicesErr := resolveKeyIndices(refSchema, keyTagNames)
	if indicesErr != nil {
		return nil, AggValuePathUnresolved, indicesErr
	}
	specs, path, specsErr := bindAggReduceSpecs(refSchema, aggSpecs)
	if specsErr != nil {
		return nil, AggValuePathUnresolved, specsErr
	}
	op := NewBatchAggregation(refSchema, keyIndices, specs, AggModeReduce, batchSize, tracker, 0)
	defer op.Close()
	if initErr := op.Init(context.Background()); initErr != nil {
		return nil, path, fmt.Errorf("ReducePartialBatches: init: %w", initErr)
	}
	for i, b := range partials {
		if b == nil || b.Len == 0 {
			continue
		}
		if compatErr := schemaCompatible(refSchema, b.Schema); compatErr != nil {
			return nil, path, fmt.Errorf("ReducePartialBatches: partial %d schema mismatch: %w", i, compatErr)
		}
		if consumeErr := op.Consume(context.Background(), b); consumeErr != nil {
			return nil, path, fmt.Errorf("ReducePartialBatches: consume partial %d: %w", i, consumeErr)
		}
	}
	if finalErr := op.Finalize(context.Background()); finalErr != nil {
		return nil, path, fmt.Errorf("ReducePartialBatches: finalize: %w", finalErr)
	}
	var out []*vectorized.RecordBatch
	for {
		nb, nextErr := op.NextBatch(context.Background())
		if nextErr != nil {
			return nil, path, fmt.Errorf("ReducePartialBatches: next: %w", nextErr)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out, path, nil
}

// resolveKeyIndices binds keyTagNames to column indices in the partial
// schema. Returns an error if a name does not resolve. Empty list is
// allowed (scalar reduce) and returns nil keyIndices.
func resolveKeyIndices(schema *vectorized.BatchSchema, keyTagNames []string) ([]int, error) {
	if len(keyTagNames) == 0 {
		return nil, nil
	}
	out := make([]int, 0, len(keyTagNames))
	for _, name := range keyTagNames {
		idx := -1
		for i, def := range schema.Columns {
			if def.Role == vectorized.RoleTag && def.Name == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("key tag %q not present in partial schema", name)
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
	for i, b := range reduced {
		if compatErr := schemaCompatible(schema, b.Schema); compatErr != nil {
			return nil, fmt.Errorf("ApplyTopToReduce: batch %d schema mismatch: %w", i, compatErr)
		}
		if consumeErr := top.Consume(context.Background(), b); consumeErr != nil {
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
