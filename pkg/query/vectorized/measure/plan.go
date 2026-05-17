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
	"fmt"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Conservative per-new-group memory estimate. Covers map entry + bucket
// struct + key/keyCol allocations; calibrated against G7g bench (TBD).
const aggEntrySize int64 = 512

// BuildOperators translates the GroupBy + Agg slice of MeasureQueryOptions
// into a list of BreakerOperators that, chained after the scan source, form
// the vectorized aggregation pipeline.
//
// Routing rules:
//   - GroupBy + Agg both set → emit a single BatchAggregation. The operator's
//     own keyIndex map produces per-group buckets and folds the agg slot;
//     a separate BatchGroupBy would double the row materialization.
//   - Agg set without GroupBy → scalar reduce: a BatchAggregation with no
//     key columns. Every row maps to one group, so a single output row is
//     emitted carrying the first-seen projected tags plus the agg result,
//     matching the row path's aggAllIterator.
//   - GroupBy set without Agg → raw GroupBy: a first-seen-row-per-group
//     BatchGroupBy. The output preserves the input schema; one row per
//     group is emitted in group-insertion order, matching the row path's
//     groupIterator + processor.go's current[0] read.
//   - Neither set → empty operator list; caller emits raw rows.
//
// tracker is the per-pipeline MemoryTracker (G7a); it must be non-nil when
// any operator is emitted so per-group reservations route to the shared
// budget. batchSize controls the operator's output pagination.
//
// mode selects the BatchAggregation strategy when an agg operator is built
// (Agg set): AggModeAll for single-node final reduce; AggModeMap for the
// distributed Map phase (G9f.2) that emits typed-column partials. mode is
// ignored for the raw-GroupBy branch (BatchGroupBy doesn't carry partial
// state — first-seen-row per group is already the final shape).
// AggModeReduce is rejected here loudly; its operator is built by the
// liaison-side reduce plan (G9f.3), not via BuildOperators.
func BuildOperators(
	opts model.MeasureQueryOptions, schema *vectorized.BatchSchema,
	tracker *vectorized.MemoryTracker, batchSize int, mode AggMode,
) ([]vectorized.BreakerOperator, error) {
	hasGroupBy := opts.GroupBy != nil && opts.GroupBy.TagFamily != "" && len(opts.GroupBy.TagNames) > 0
	hasAgg := opts.Agg != nil

	if !hasGroupBy && !hasAgg {
		return nil, nil
	}
	if tracker == nil {
		return nil, fmt.Errorf("vectorized.measure: BuildOperators requires a non-nil shared MemoryTracker")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("vectorized.measure: batchSize must be > 0, got %d", batchSize)
	}
	if mode == AggModeReduce {
		return nil, fmt.Errorf("vectorized.measure: BuildOperators does not build AggModeReduce — that operator is built by the liaison reduce plan (G9f.3)")
	}

	var keyIndices []int
	if hasGroupBy {
		var keyErr error
		keyIndices, keyErr = lookupGroupByKeyIndices(schema, opts.GroupBy)
		if keyErr != nil {
			return nil, keyErr
		}
	}

	if !hasAgg {
		// Raw GroupBy: emit the first-seen row of each group with the
		// input schema unchanged. entrySize accounts for the per-group
		// bucket; rowSize is zero because at most one row is retained.
		pool := vectorized.NewBatchPool(schema, batchSize)
		gb := NewBatchGroupByFirst(schema, keyIndices, pool, batchSize, tracker, aggEntrySize)
		return []vectorized.BreakerOperator{gb}, nil
	}

	fieldIdx, fieldErr := lookupFieldColumnIndex(schema, opts.Agg.FieldName)
	if fieldErr != nil {
		return nil, fieldErr
	}

	aggFn, fnErr := protoAggFuncToInternal(opts.Agg.Func)
	if fnErr != nil {
		return nil, fnErr
	}

	// The agg result column inherits the input field's name to match the
	// row-path aggregator (aggGroupIterator.Current() in
	// pkg/query/logical/measure/measure_plan_aggregation.go). Row-path
	// fixtures expect a single output field named after the original
	// input (e.g. "value"), not an auto-derived "<field>_<func>" suffix
	// like "value_sum" — the suffix would break proto.Equal parity in the
	// integration suite. The aggregation function lives on the operator
	// spec, not in the column name.
	//
	// keyIndices is empty for scalar reduce (Agg without GroupBy):
	// BatchAggregation.computeKey then returns the same key for every
	// row, so all rows collapse into a single output row carrying the
	// first-seen projected tags plus the agg result — the columnar
	// equivalent of the row path's aggAllIterator.
	spec := AggSpec{
		Func:     aggFn,
		InputCol: fieldIdx,
		Output:   opts.Agg.FieldName,
	}
	agg := NewBatchAggregation(schema, keyIndices, []AggSpec{spec},
		mode, batchSize, tracker, aggEntrySize)
	return []vectorized.BreakerOperator{agg}, nil
}

// lookupGroupByKeyIndices resolves each GroupBy tag name to its column index
// in schema. All names must exist within the configured tag family.
func lookupGroupByKeyIndices(schema *vectorized.BatchSchema, gb *model.MeasureGroupBy) ([]int, error) {
	indices := make([]int, 0, len(gb.TagNames))
	for _, name := range gb.TagNames {
		idx := -1
		for i, def := range schema.Columns {
			if def.Role == vectorized.RoleTag && def.TagFamily == gb.TagFamily && def.Name == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("vectorized.measure: GroupBy tag %s.%s not present in schema", gb.TagFamily, name)
		}
		indices = append(indices, idx)
	}
	return indices, nil
}

// lookupFieldColumnIndex resolves the agg input field name to its column
// index. Only field columns (Role == RoleField) are eligible.
func lookupFieldColumnIndex(schema *vectorized.BatchSchema, name string) (int, error) {
	for i, def := range schema.Columns {
		if def.Role == vectorized.RoleField && def.Name == name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("vectorized.measure: Agg field %q not present in schema", name)
}

// protoAggFuncToInternal maps the proto AggregationFunction enum to the
// internal AggFunc constant. UNSPECIFIED is rejected — Aggregation must
// name a concrete function.
func protoAggFuncToInternal(f modelv1.AggregationFunction) (AggFunc, error) {
	switch f {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return AggSum, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return AggCount, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return AggMin, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return AggMax, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return AggMean, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED:
		return 0, fmt.Errorf("vectorized.measure: Agg.Function is UNSPECIFIED")
	}
	return 0, fmt.Errorf("vectorized.measure: unknown AggregationFunction %v", f)
}
