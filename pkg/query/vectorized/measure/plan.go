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
	"strings"

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
// Routing rules (v1):
//   - GroupBy + Agg both set → emit a single BatchAggregation. The operator's
//     own keyIndex map produces per-group buckets and folds the agg slot;
//     a separate BatchGroupBy would double the row materialization.
//   - GroupBy set without Agg → unsupported. The proto field is paired with
//     `agg` for the existing TopN path; without Agg the result semantic is
//     ambiguous (selection? deduplication?). Return an error.
//   - Agg set without GroupBy → unsupported. Scalar reduce → single output
//     row is deferred to a future increment.
//   - Neither set → empty operator list; caller emits raw rows.
//
// tracker is the per-pipeline MemoryTracker (G7a); it must be non-nil when
// any operator is emitted so per-group reservations route to the shared
// budget. batchSize controls the operator's output pagination.
func BuildOperators(
	opts model.MeasureQueryOptions, schema *vectorized.BatchSchema,
	tracker *vectorized.MemoryTracker, batchSize int,
) ([]vectorized.BreakerOperator, error) {
	hasGroupBy := opts.GroupBy != nil && opts.GroupBy.TagFamily != "" && len(opts.GroupBy.TagNames) > 0
	hasAgg := opts.Agg != nil

	if !hasGroupBy && !hasAgg {
		return nil, nil
	}
	if hasGroupBy && !hasAgg {
		return nil, fmt.Errorf("vectorized.measure: GroupBy without Agg is not supported in v1")
	}
	if hasAgg && !hasGroupBy {
		return nil, fmt.Errorf("vectorized.measure: Agg without GroupBy (scalar reduce) is not supported in v1")
	}
	if tracker == nil {
		return nil, fmt.Errorf("vectorized.measure: BuildOperators requires a non-nil shared MemoryTracker")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("vectorized.measure: batchSize must be > 0, got %d", batchSize)
	}

	keyIndices, keyErr := lookupGroupByKeyIndices(schema, opts.GroupBy)
	if keyErr != nil {
		return nil, keyErr
	}

	fieldIdx, fieldErr := lookupFieldColumnIndex(schema, opts.Agg.FieldName)
	if fieldErr != nil {
		return nil, fieldErr
	}

	aggFn, fnErr := protoAggFuncToInternal(opts.Agg.Func)
	if fnErr != nil {
		return nil, fnErr
	}

	spec := AggSpec{
		Func:     aggFn,
		InputCol: fieldIdx,
		Output:   aggOutputName(opts.Agg.FieldName, aggFn),
	}
	agg := NewBatchAggregation(schema, keyIndices, []AggSpec{spec},
		AggModeAll, batchSize, tracker, aggEntrySize)
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

// aggOutputName derives the agg result column name: <field>_<func> (lowercase).
func aggOutputName(fieldName string, fn AggFunc) string {
	suffix := ""
	switch fn {
	case AggSum:
		suffix = "sum"
	case AggCount:
		suffix = "count"
	case AggMin:
		suffix = "min"
	case AggMax:
		suffix = "max"
	case AggMean:
		suffix = "mean"
	}
	return strings.Join([]string{fieldName, suffix}, "_")
}
