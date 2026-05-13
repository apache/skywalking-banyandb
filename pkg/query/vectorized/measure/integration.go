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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BuildBatchSchema derives a BatchSchema from a Measure schema and a query's
// projection list. The output column order is fixed:
//
//	timestamp, version, sid, shardID, then projected tags (in TagProjection
//	order, family-by-family, name-by-name), then projected fields (in
//	FieldProjection order).
//
// Tag families and tag names that are not present in the Measure schema are
// dropped — the row path silently skips unknown tags as well, so this matches
// existing semantics. Fields not present in the schema yield a Null-typed
// column so projection still produces a slot in the output.
func BuildBatchSchema(measureSchema *databasev1.Measure, opts model.MeasureQueryOptions) (*vectorized.BatchSchema, error) {
	if measureSchema == nil {
		return nil, fmt.Errorf("vectorized.measure: nil Measure schema")
	}
	cols := []vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Type: vectorized.ColumnTypeInt64},
	}

	tagSpecs := make(map[string]map[string]*databasev1.TagSpec)
	for _, tf := range measureSchema.GetTagFamilies() {
		byName := make(map[string]*databasev1.TagSpec, len(tf.GetTags()))
		for _, ts := range tf.GetTags() {
			byName[ts.GetName()] = ts
		}
		tagSpecs[tf.GetName()] = byName
	}

	// Tag and field projections become passthrough columns: the column cell
	// type is *modelv1.TagValue / *modelv1.FieldValue, holding the original
	// protobuf pointer from the scan source unchanged. The egress serializer
	// returns those pointers directly, matching the row path's zero-alloc
	// per-cell behavior.
	//
	// Why passthrough beats native here: with the gRPC wire format frozen
	// (`*measurev1.InternalDataPoint` is row-shaped), egress must produce
	// a *modelv1.TagValue per cell. Passthrough lets the cell flow through
	// the pipeline pre-built; native columns force the egress to
	// reconstruct the protobuf wrapper (3 allocs/cell), regressing the
	// G5a bench gates by ~1.5–2× ns/op. Native column types only pay off
	// when downstream operators (BatchGroupBy / BatchAggregation /
	// BatchTop, planned for G6) consume the typed primitives — at which
	// point the operator output reduces row count enough to amortize the
	// reconstruction. Until then, passthrough is the production-correct
	// choice.
	//
	// We still validate that the schema declares each projected name with
	// a supported variant so the row-path null fill (for projection
	// entries absent from a multi-group result) carries known semantics.
	for _, tp := range opts.TagProjection {
		family := tagSpecs[tp.Family]
		for _, name := range tp.Names {
			if family != nil {
				if spec, found := family[name]; found {
					if _, mapErr := tagTypeToColumnType(spec.GetType()); mapErr != nil {
						return nil, fmt.Errorf("vectorized.measure: tag %s.%s: %w", tp.Family, name, mapErr)
					}
				}
			}
			cols = append(cols, vectorized.ColumnDef{
				Role:      vectorized.RoleTag,
				TagFamily: tp.Family,
				Name:      name,
				Type:      vectorized.ColumnTypeTagValue,
			})
		}
	}

	fieldSpecs := make(map[string]*databasev1.FieldSpec, len(measureSchema.GetFields()))
	for _, fs := range measureSchema.GetFields() {
		fieldSpecs[fs.GetName()] = fs
	}
	for _, name := range opts.FieldProjection {
		if spec, found := fieldSpecs[name]; found {
			if _, mapErr := fieldTypeToColumnType(spec.GetFieldType()); mapErr != nil {
				return nil, fmt.Errorf("vectorized.measure: field %s: %w", name, mapErr)
			}
		}
		cols = append(cols, vectorized.ColumnDef{
			Role: vectorized.RoleField,
			Name: name,
			Type: vectorized.ColumnTypeFieldValue,
		})
	}

	return vectorized.NewBatchSchema(cols), nil
}

func tagTypeToColumnType(t databasev1.TagType) (vectorized.ColumnType, error) {
	switch t {
	case databasev1.TagType_TAG_TYPE_INT:
		return vectorized.ColumnTypeInt64, nil
	case databasev1.TagType_TAG_TYPE_STRING:
		return vectorized.ColumnTypeString, nil
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return vectorized.ColumnTypeBytes, nil
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return vectorized.ColumnTypeInt64Array, nil
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return vectorized.ColumnTypeStrArray, nil
	case databasev1.TagType_TAG_TYPE_UNSPECIFIED, databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return 0, fmt.Errorf("unsupported tag type %v", t)
	}
	return 0, fmt.Errorf("unsupported tag type %v", t)
}

func fieldTypeToColumnType(t databasev1.FieldType) (vectorized.ColumnType, error) {
	switch t {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return vectorized.ColumnTypeInt64, nil
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return vectorized.ColumnTypeFloat64, nil
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return vectorized.ColumnTypeString, nil
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return vectorized.ColumnTypeBytes, nil
	case databasev1.FieldType_FIELD_TYPE_UNSPECIFIED:
		return 0, fmt.Errorf("unsupported field type %v", t)
	}
	return 0, fmt.Errorf("unsupported field type %v", t)
}

// NewMIterator builds a vectorized adapter that drives a Pipeline over qr and
// satisfies executor.MIterator. The returned VectorizedMIterator owns the qr
// lifetime through the pipeline: Close → pipeline.Close → BatchScan.Close →
// SeriesCursor.Close → qr.Release. Callers must NOT release qr themselves on
// the success path.
//
// On error, ownership of qr stays with the caller; the caller is responsible
// for releasing it. Construction is split so qr.Release-on-failure is decided
// at the call site (where build inputs other than qr are also tracked).
func NewMIterator(ctx context.Context, qr model.MeasureQueryResult,
	measureSchema *databasev1.Measure, opts model.MeasureQueryOptions, cfg VectorizedConfig,
) (*VectorizedMIterator, error) {
	if validateErr := cfg.Validate(); validateErr != nil {
		return nil, validateErr
	}
	schema, schemaErr := BuildBatchSchema(measureSchema, opts)
	if schemaErr != nil {
		return nil, schemaErr
	}
	pool := vectorized.NewBatchPool(schema, cfg.BatchSize)

	// G5c (US-007): when the storage result also satisfies
	// MeasureBatchResult, drive the pipeline from PullBatch directly via
	// BatchSourceFromBatchResult. This bypasses extract.go's per-cell
	// decode pass — the source columns are already typed (passthrough or
	// native, depending on BuildBatchSchema's column type choice).
	//
	// The fallback is the original BatchScan path that consumes
	// MeasureQueryResult.Pull and runs extract* on each cell. It stays
	// alive for any future caller that produces a query result not
	// satisfying MeasureBatchResult; v1's storage layer satisfies both.
	var source vectorized.PullOperator
	if br, ok := qr.(model.MeasureBatchResult); ok {
		source = NewBatchSourceFromBatchResult(br, schema, pool, cfg.BatchSize)
	} else {
		source = NewBatchScan(qr, schema, pool, cfg.BatchSize)
	}

	pipeline, buildErr := vectorized.NewPipelineBuilder().From(source).Build()
	if buildErr != nil {
		// source was constructed but never wired into a Pipeline; close
		// it directly to release qr through the source.
		_ = source.Close()
		return nil, buildErr
	}
	if initErr := source.Init(ctx); initErr != nil {
		_ = pipeline.Close()
		return nil, initErr
	}
	return &VectorizedMIterator{inner: newVectorizedMIterator(ctx, pipeline, pool)}, nil
}

// NewIteratorFromPipeline wraps an already-built *vectorized.Pipeline (with
// its source/operators already attached and Pipeline.Init called) as a
// VectorizedMIterator. Used by the G8 vec executor at
// pkg/query/vectorized/measure/plan to compose plan trees into the public
// MIterator contract without going through NewMIterator's leaf-substitution
// path. pool is the egress BatchPool the adapter recycles consumed batches
// into; it must match the pipeline's terminal output schema.
func NewIteratorFromPipeline(ctx context.Context, pipeline *vectorized.Pipeline, pool *vectorized.BatchPool) *VectorizedMIterator {
	return &VectorizedMIterator{inner: newVectorizedMIterator(ctx, pipeline, pool)}
}

// VectorizedMIterator is the public adapter exposed to other packages. It is
// a thin facade over the unexported vectorizedMIterator so the executor
// interface is satisfied without leaking the package-private type.
type VectorizedMIterator struct {
	inner *vectorizedMIterator
}

// Next advances one DataPoint.
func (v *VectorizedMIterator) Next() bool { return v.inner.Next() }

// Current returns the current row as a single-element slice (matches row-path
// contract).
func (v *VectorizedMIterator) Current() []*measurev1.InternalDataPoint { return v.inner.Current() }

// Err returns any sticky storage error that terminated iteration.
func (v *VectorizedMIterator) Err() error { return v.inner.Err() }

// Close releases the pipeline (and through it the BatchScan, cursor, and
// underlying MeasureQueryResult). Returns the join of the sticky iteration
// error and the pipeline-close error, matching resultMIterator.Close.
func (v *VectorizedMIterator) Close() error {
	return v.inner.Close()
}
