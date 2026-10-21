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
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// buildAdapterPipeline wires a BatchScan into a minimal Pipeline against the
// supplied fake MeasureQueryResult and returns the pipeline, its terminal
// schema, and its pool so the adapter can recycle batches.
func buildAdapterPipeline(t *testing.T, qr model.MeasureQueryResult) (*vectorized.Pipeline, *vectorized.BatchSchema, *vectorized.BatchPool) {
	t.Helper()
	schema := minimalSchema()
	pool := vectorized.NewBatchPool(schema, 4)
	scan := NewBatchScan(qr, schema, pool, 4)
	p, err := vectorized.NewPipelineBuilder().From(scan).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := scan.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p, schema, pool
}

func TestVectorizedMIterator_Next_PullsAndSerializes(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100, 200)}}
	p, schema, pool := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p, schema, pool)
	defer it.Close()

	count := 0
	for it.Next() {
		dps := it.Current()
		if len(dps) != 1 {
			t.Fatalf("Current length: want 1, got %d", len(dps))
		}
		count++
	}
	if count != 2 {
		t.Fatalf("iterations: want 2, got %d", count)
	}
}

func TestVectorizedMIterator_Next_ReturnsFalseOnEOF(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: nil}
	p, schema, pool := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p, schema, pool)
	defer it.Close()

	if it.Next() {
		t.Fatal("Next on empty source must return false")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("EOF must not surface as error; got %v", err)
	}
}

func TestVectorizedMIterator_Next_ReturnsFalseOnError_ErrExposedViaErr(t *testing.T) {
	boom := errors.New("storage boom")
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResultErr(boom)}}
	p, schema, pool := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p, schema, pool)
	defer it.Close()

	if it.Next() {
		t.Fatal("Next must return false on storage error")
	}
	if !errors.Is(it.Err(), boom) {
		t.Fatalf("Err must surface the storage error; got %v", it.Err())
	}
}

func TestVectorizedMIterator_Current_ReturnsCurrentRow(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	p, schema, pool := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p, schema, pool)
	defer it.Close()

	_ = it.Next()
	first := it.Current()
	if len(first) != 1 {
		t.Fatalf("first Current len: want 1, got %d", len(first))
	}
	// Current called repeatedly without Next must keep returning the same row.
	if got := it.Current(); len(got) != 1 {
		t.Fatalf("repeat Current must return same row; got len %d", len(got))
	}
}

func TestVectorizedMIterator_Close_DelegatesToPipelineClose_Idempotent(t *testing.T) {
	qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100)}}
	p, schema, pool := buildAdapterPipeline(t, qr)
	it := newVectorizedMIterator(context.Background(), p, schema, pool)
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	// Pipeline.Close is idempotent at the pipeline level; calling adapter.Close
	// twice must not error.
	if err := it.Close(); err != nil {
		t.Fatalf("second Close must be no-op, got %v", err)
	}
}

func TestFinalSerializationReservationFollowsTransportLifetime(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		ctx := context.Background()
		var scope *query.TransportScope
		if scoped {
			ctx, scope = query.NewTransportScope(ctx)
			defer scope.Close()
		}
		tracker := vectorized.NewMemoryTracker(1 << 20)
		schema := minimalSchema()
		pool := vectorized.NewBatchPool(schema, 4)
		qr := &fakeMeasureQueryResult{seq: []*model.MeasureResult{mkResult(1, 100, 200)}}
		scan := NewBatchScan(qr, schema, pool, 4)
		pipeline, err := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(scan).Build()
		if err != nil {
			t.Fatal(err)
		}
		if err = pipeline.Init(ctx); err != nil {
			t.Fatal(err)
		}
		iter := newVectorizedMIterator(ctx, pipeline, schema, pool)
		for iter.Next() {
			_ = iter.Current()
		}
		if err = iter.Close(); err != nil {
			t.Fatal(err)
		}
		if scoped {
			if tracker.Used() == 0 {
				t.Fatal("serialized response reservation released before transport completion")
			}
			scope.Close()
		}
		if tracker.Used() != 0 {
			t.Fatalf("post-transport reservation leak: %d", tracker.Used())
		}
	}
}

func encodingBoundBatch(t *testing.T) *vectorized.RecordBatch {
	t.Helper()
	family := strings.Repeat("family", 80)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: family, Name: "integer", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: family, Name: "text", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: family, Name: "binary", Type: vectorized.ColumnTypeBytes},
		{Role: vectorized.RoleTag, TagFamily: family, Name: "integers", Type: vectorized.ColumnTypeInt64Array},
		{Role: vectorized.RoleTag, TagFamily: family, Name: "strings", Type: vectorized.ColumnTypeStrArray},
		{Role: vectorized.RoleField, Name: strings.Repeat("name", 120), Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "float", Type: vectorized.ColumnTypeFloat64},
		{Role: vectorized.RoleField, Name: "text", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "binary", Type: vectorized.ColumnTypeBytes},
	})
	batch := vectorized.NewRecordBatch(schema, 4)
	for row := range 4 {
		for _, idx := range []int{0, 1, 2, 3, 8} {
			batch.Columns[idx].(*vectorized.TypedColumn[int64]).Append(math.MinInt64 + int64(row))
		}
		for _, idx := range []int{4, 10} {
			batch.Columns[idx].(*vectorized.TypedColumn[string]).Append(strings.Repeat("payload", 300))
		}
		for _, idx := range []int{5, 11} {
			batch.Columns[idx].(*vectorized.TypedColumn[[]byte]).Append([]byte(strings.Repeat("binary", 200)))
		}
		batch.Columns[6].(*vectorized.TypedColumn[[]int64]).Append([]int64{math.MinInt64, -1, 0, math.MaxInt64})
		batch.Columns[7].(*vectorized.TypedColumn[[]string]).Append([]string{"", strings.Repeat("array", 400), "unicode字"})
		batch.Columns[9].(*vectorized.TypedColumn[float64]).Append(math.NaN())
	}
	batch.Len = 4
	return batch
}

func TestSerializationEncodingBoundCoversWireShapes(t *testing.T) {
	for _, variant := range []string{"typed", "nulls", "constants", "selection", "empty"} {
		t.Run(variant, func(t *testing.T) {
			batch := encodingBoundBatch(t)
			switch variant {
			case "nulls":
				for _, column := range batch.Columns[3:] {
					column.MarkNullAt(2)
				}
			case "constants":
				batch.Columns[4] = vectorized.NewConstString(strings.Repeat("constant", 10000), batch.Len)
				batch.Columns[11] = vectorized.NewConstBytes([]byte(strings.Repeat("blob", 15000)), batch.Len)
			case "selection":
				batch.Selection = []uint16{0, 3}
			case "empty":
				batch.Selection = []uint16{}
			}
			_, bound, covered, err := serializationPreflight(batch)
			require.NoError(t, err)
			require.True(t, covered)
			points := serializeBatchToProto(batch, nil)
			response := &measurev1.QueryResponse{}
			for _, point := range points {
				response.DataPoints = append(response.DataPoints, point.DataPoint)
			}
			require.GreaterOrEqual(t, bound, uint64(proto.Size(response)))
		})
	}
}

func TestSerializationEncodingFallsBackForDuplicateAndOpaqueColumns(t *testing.T) {
	batch := encodingBoundBatch(t)
	batch.Selection = []uint16{3, 3, 0}
	_, _, covered, err := serializationPreflight(batch)
	require.NoError(t, err)
	require.False(t, covered, "non-monotone selections must not assume payload uniqueness")
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, Name: "opaque", Type: vectorized.ColumnTypeTagValue}})
	opaque := vectorized.NewRecordBatch(schema, 1)
	opaque.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(&modelv1.TagValue{})
	opaque.Len = 1
	_, _, covered, err = serializationPreflight(opaque)
	require.NoError(t, err)
	require.False(t, covered)
}

func TestSerializationEncodingPreflightRejectsConstantExpansionOverflow(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleField, Name: "constant", Type: vectorized.ColumnTypeString}})
	batch := &vectorized.RecordBatch{Schema: schema, Columns: []vectorized.Column{vectorized.NewConstString("payload", math.MaxInt)}, Len: math.MaxInt}
	memory, bound, covered, err := serializationPreflight(batch)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Zero(t, memory)
	require.Zero(t, bound)
	require.False(t, covered)
}

func TestFinalEncodingCoverageSurvivesBatchesAndClose(t *testing.T) {
	caller := vectorized.WithQueryMemoryScope(t.Context())
	ctx := vectorized.WithQueryMemoryTracker(caller, 64<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	first, second := encodingBoundBatch(t), encodingBoundBatch(t)
	source := &frameBorrowedSource{schema: first.Schema, batches: []*vectorized.RecordBatch{first, second}}
	pipeline, err := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source).Build()
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	iterator := newVectorizedMIterator(WithFinalBatchSerialization(ctx), pipeline, first.Schema, nil)
	response := &measurev1.QueryResponse{}
	for iterator.Next() {
		response.DataPoints = append(response.DataPoints, iterator.Current()[0].DataPoint)
	}
	require.NoError(t, iterator.Err())
	require.Len(t, response.DataPoints, 8)
	_, firstBound, _, err := serializationPreflight(first)
	require.NoError(t, err)
	require.NoError(t, iterator.Close())
	require.Zero(t, tracker.Used())
	release, err := vectorized.ReserveResponseEncoding(caller, response)
	require.NoError(t, err)
	require.Equal(t, int64(2*(2*firstBound+11)), tracker.Used(), "both pointer slabs must retain their original identities")
	release()
	require.Zero(t, tracker.Used())
}

func TestFinalEncodingDuplicateSelectionUsesExactFallback(t *testing.T) {
	caller := vectorized.WithQueryMemoryScope(t.Context())
	ctx := vectorized.WithQueryMemoryTracker(caller, 64<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	batch := encodingBoundBatch(t)
	batch.Selection = []uint16{3, 3, 0}
	source := &frameBorrowedSource{schema: batch.Schema, batches: []*vectorized.RecordBatch{batch}}
	pipeline, err := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source).Build()
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	iterator := newVectorizedMIterator(WithFinalBatchSerialization(ctx), pipeline, batch.Schema, nil)
	response := &measurev1.QueryResponse{}
	for iterator.Next() {
		response.DataPoints = append(response.DataPoints, iterator.Current()[0].DataPoint)
	}
	require.NoError(t, iterator.Err())
	require.Len(t, response.DataPoints, 3)
	require.True(t, proto.Equal(response.DataPoints[0], response.DataPoints[1]))
	require.NoError(t, iterator.Close())
	release, err := vectorized.ReserveResponseEncoding(caller, response)
	require.NoError(t, err)
	require.Equal(t, int64(2*(proto.Size(response)+11)), tracker.Used())
	release()
	require.Zero(t, tracker.Used())
}

func TestFinalEncodingRejectsConstantCopiesBeforeMaterialization(t *testing.T) {
	caller := vectorized.WithQueryMemoryScope(t.Context())
	ctx := vectorized.WithQueryMemoryTracker(caller, 16<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeString}})
	batch := &vectorized.RecordBatch{Schema: schema, Columns: []vectorized.Column{vectorized.NewConstString(strings.Repeat("large", 2048), 1024)}, Len: 1024}
	source := &frameBorrowedSource{schema: schema, batches: []*vectorized.RecordBatch{batch}}
	pipeline, err := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source).Build()
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	iterator := newVectorizedMIterator(WithFinalBatchSerialization(ctx), pipeline, schema, nil)
	require.False(t, iterator.Next())
	require.ErrorIs(t, iterator.Err(), vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, iterator.batch, "budget rejection must precede DataPoint copies")
	require.ErrorIs(t, iterator.Close(), vectorized.ErrMemoryBudgetExceeded)
	require.Zero(t, tracker.Used())
}

func TestSerializationUnsortedUniqueKeepsExactCopyResidency(t *testing.T) {
	batch := encodingBoundBatch(t)
	batch.Selection = []uint16{0, 1, 2, 3}
	ascending, err := serializationReservationBytes(batch)
	require.NoError(t, err)
	for _, selection := range [][]uint16{{3, 2, 1, 0}, {2, 0, 3, 1}} {
		batch.Selection = selection
		memory, bound, covered, preflightErr := serializationPreflight(batch)
		require.NoError(t, preflightErr)
		require.Equal(t, ascending, memory)
		require.Zero(t, bound)
		require.False(t, covered)
		tracker := vectorized.NewMemoryTracker(ascending)
		source := &frameBorrowedSource{schema: batch.Schema, batches: []*vectorized.RecordBatch{batch}}
		pipeline, buildErr := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source).Build()
		require.NoError(t, buildErr)
		require.NoError(t, pipeline.Init(t.Context()))
		iterator := newVectorizedMIterator(t.Context(), pipeline, batch.Schema, nil)
		var rows int
		for iterator.Next() {
			rows++
		}
		require.NoError(t, iterator.Err(), "a tight sufficient budget also admits descending unique rows")
		require.Equal(t, 4, rows)
		require.NoError(t, iterator.Close())
		require.Zero(t, tracker.Used())
	}
	batch.Selection = []uint16{3, 3, 3, 0}
	duplicated, err := serializationReservationBytes(batch)
	require.NoError(t, err)
	require.Greater(t, duplicated, ascending, "repeated payload copies must increase the preallocation guard")
}
