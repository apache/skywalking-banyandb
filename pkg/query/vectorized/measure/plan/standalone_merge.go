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

package plan

import (
	"context"
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// MergeStandaloneBatches transfers ownership of group results to a native batch merger.
func MergeStandaloneBatches(ctx context.Context, groups []*BatchResult, schema *vectorized.BatchSchema,
	req *measurev1.QueryRequest, schemas []logical.Schema, cfg vmeasure.VectorizedConfig,
) (*BatchResult, error) {
	defer func() {
		for _, group := range groups {
			_ = group.Close()
		}
	}()
	var schemaErr error
	schema, schemaErr = standaloneMergedSchema(groups, schema)
	if schemaErr != nil {
		return nil, schemaErr
	}
	tracker := vectorized.QueryMemoryTracker(ctx, int64(cfg.QueryMemoryMiB)*1024*1024)
	var inputReserved int64
	defer func() { tracker.Release(inputReserved) }()
	var outputReserved int64
	transferred := false
	defer func() {
		if !transferred {
			tracker.Release(outputReserved)
		}
	}()
	var sources []decodedBatchSource
	defer func() { releaseSources(sources) }()
	for groupIdx, group := range groups {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			raw, err := group.NextBatch(ctx)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				break
			}
			// Reserve copied columns and eventual merge output before allocating either.
			retained := int64(vectorized.EstimatedRetainedBytes(raw)) + estimateRowWidth(schema)*int64(raw.Len)
			if err = tracker.Reserve(retained * 2); err != nil {
				return nil, err
			}
			inputReserved += retained
			outputReserved += retained
			mapped := vectorized.NewRecordBatch(schema, raw.Len)
			mapped.Len = raw.Len
			mapped.Selection = append([]uint16(nil), raw.Selection...)
			if raw.Selection != nil && len(raw.Selection) == 0 {
				mapped.Selection = []uint16{}
			}
			indices := make(map[string]int)
			for idx, def := range raw.Schema.Columns {
				indices[schemaColKey(def)] = idx
			}
			for idx, def := range schema.Columns {
				src, ok := indices[schemaColKey(def)]
				if !ok {
					for range raw.Len {
						mapped.Columns[idx].AppendNull()
					}
					continue
				}
				if err = appendColumnRangeCoerced(mapped.Columns[idx], raw.Columns[src], 0, raw.Len); err != nil {
					return nil, err
				}
			}
			mapped.Owners.PinAll(&raw.Owners)
			sources = append(sources, decodedBatchSource{batch: mapped, source: len(sources), group: groupIdx})
		}
	}
	spec := distributedRowsSpec{
		BatchSize: cfg.BatchSize, OrderByColIdx: -1, Desc: req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC,
		Tracker: vectorized.QueryMemoryTracker(ctx, int64(cfg.QueryMemoryMiB)*1024*1024),
	}
	if ruleName := req.GetOrderBy().GetIndexRuleName(); ruleName != "" {
		for _, ls := range schemas {
			if found, rule := ls.IndexRuleDefined(ruleName); found && len(rule.Tags) == 1 {
				for _, def := range schema.Columns {
					if def.Role == vectorized.RoleTag && def.Name == rule.Tags[0] {
						spec.OrderByFamily = def.TagFamily
						spec.OrderByTagName = def.Name
						break
					}
				}
				break
			}
		}
	}
	sortIdx, err := resolveDistributedSortColumn(schema, spec)
	if err != nil {
		return nil, err
	}
	iters, err := buildDistributedRowSourceIters(sources, schema, spec.Desc, sortIdx)
	if err != nil {
		return nil, err
	}
	batches, err := runDistributedRowMerge(iters, schema, spec, cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("merge standalone batches: %w", err)
	}
	owned := &reservedBatchSource{batchSliceSource: batchSliceSource{schema: schema, batches: batches}, tracker: tracker, reserved: outputReserved}
	result, err := NewBatchResult(ctx, owned, schema, cfg.BatchSize)
	transferred = true
	return result, err
}

type reservedBatchSource struct {
	tracker *vectorized.MemoryTracker
	batchSliceSource
	reserved int64
}

func (s *reservedBatchSource) Close() error {
	err := s.batchSliceSource.Close()
	s.tracker.Release(s.reserved)
	s.reserved = 0
	return err
}

// standaloneMergedSchema accounts for passthrough nulls in per-group projections.
func standaloneMergedSchema(groups []*BatchResult, schema *vectorized.BatchSchema) (*vectorized.BatchSchema, error) {
	defs := append([]vectorized.ColumnDef(nil), schema.Columns...)
	for _, group := range groups {
		for _, sourceDef := range group.OutputSchema().Columns {
			for idx, destDef := range defs {
				if schemaColKey(sourceDef) != schemaColKey(destDef) || sourceDef.Type == destDef.Type {
					continue
				}
				switch destDef.Role {
				case vectorized.RoleTag:
					defs[idx].Type = vectorized.ColumnTypeTagValue
				case vectorized.RoleField:
					defs[idx].Type = vectorized.ColumnTypeFieldValue
				default:
					return nil, fmt.Errorf("incompatible metadata column %s", destDef.Name)
				}
			}
		}
	}
	return vectorized.NewBatchSchema(defs), nil
}
