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

package ql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type rawSource struct {
	initErr              error
	schema               *vectorized.BatchSchema
	batches              []*vectorized.RecordBatch
	poison               []byte
	pulls, closes, inits int
}

func (s *rawSource) Init(context.Context) error            { s.inits++; return s.initErr }
func (s *rawSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *rawSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	s.pulls++
	if len(s.batches) == 0 {
		copy(s.poison, "xxxxx")
		return nil, nil
	}
	batch := s.batches[0]
	s.batches = s.batches[1:]
	return batch, nil
}
func (s *rawSource) Close() error { s.closes++; copy(s.poison, "xxxxx"); return nil }
func rawFixture() (*rawSource, []Slot) {
	defs := []vectorized.ColumnDef{
		{Name: "timestamp", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: "sid", Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Name: "version", Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
		{Name: "host", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeBytes},
		{Name: "v", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
		{Name: "secret", Role: vectorized.RoleField, Type: vectorized.ColumnTypeInt64},
	}
	schema := vectorized.NewBatchSchema(defs)
	batch := vectorized.NewRecordBatch(schema, 4)
	payload := []byte("first")
	for row := 0; row < 4; row++ {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(row) * int64(time.Second))
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(7)
		batch.Columns[2].(*vectorized.TypedColumn[int64]).Append(9)
		batch.Columns[3].(*vectorized.TypedColumn[[]byte]).Append(payload)
		if row == 2 {
			batch.Columns[4].AppendNull()
		} else {
			batch.Columns[4].(*vectorized.TypedColumn[int64]).Append(int64(row))
		}
		batch.Columns[5].(*vectorized.TypedColumn[int64]).Append(99)
	}
	batch.Len = 4
	host := defs[3]
	host.Name = "node"
	field := defs[4]
	field.Name = "value"
	again := field
	again.Name = "again"
	timestamp := vectorized.ColumnDef{Name: "observed", TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeTagValue}
	slots := []Slot{
		{ID: 1, Source: defs[3], Output: host},
		{ID: 2, Source: defs[4], Output: field},
		{ID: 3, Source: defs[4], Output: again},
		{ID: 4, Source: defs[0], Output: timestamp},
	}
	return &rawSource{schema: schema, batches: []*vectorized.RecordBatch{batch}, poison: payload}, slots
}

func TestRawProjectionLendsColumnsAndKeepsIndependentDescriptors(t *testing.T) {
	source, slots := rawFixture()
	original := source.batches[0]
	op, err := WrapRaw(t.Context(), source, &RawPlan{Slots: slots})
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	batch, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Same(t, original.Columns[4], batch.Columns[1])
	require.Same(t, batch.Columns[1], batch.Columns[2])
	require.Equal(t, "value", batch.Schema.Columns[1].Name)
	require.Equal(t, "again", batch.Schema.Columns[2].Name)
	require.Len(t, batch.Schema.FieldColumns, 2)
	require.True(t, batch.Columns[1].IsNull(2))
	require.GreaterOrEqual(t, batch.Schema.TimestampIndex(), 0)
	require.GreaterOrEqual(t, batch.Schema.SeriesIDIndex(), 0)
	require.NoError(t, op.Close())
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
}

func TestRawFinalEgressPreservesTypedAliasesAndOwnsFinalValues(t *testing.T) {
	source, slots := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots})
	require.NoError(t, err)
	iter, err := RawIterator(ctx, op)
	require.NoError(t, err)
	var rows []*measurev1.DataPoint
	for iter.Next() {
		rows = append(rows, iter.Current()[0].DataPoint)
	}
	require.NoError(t, iter.Close())
	require.NoError(t, iter.Close())
	require.Equal(t, 1, source.closes)
	require.Len(t, rows, 4)
	require.Equal(t, []byte("first"), rows[0].TagFamilies[0].Tags[0].Value.GetBinaryData())
	require.EqualValues(t, 7, rows[0].Sid)
	require.EqualValues(t, 9, rows[0].Version)
	require.Len(t, rows[2].Fields, 2)
	require.NotNil(t, rows[2].Fields[0].Value.GetNull())
	for row, point := range rows {
		require.Equal(t, "value", point.Fields[0].Name)
		require.Equal(t, "again", point.Fields[1].Name)
		require.True(t, proto.Equal(point.Timestamp, point.TagFamilies[1].Tags[0].Value.GetTimestamp()), "row %d", row)
	}
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestRawResidualUsesPhysicalSelectionBeforeOffsetAndLimit(t *testing.T) {
	source, slots := rawFixture()
	source.batches[0].Selection = []uint16{3, 2, 1, 0}
	limit := uint32(1)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots, Residual: true, Ranges: []MillisRange{{Begin: 0, End: 1}, {Begin: 2000, End: 4000}}, Offset: 1, Limit: &limit})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint16{2}, batch.Selection)
	eof, err := op.NextBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, eof)
	require.Equal(t, 1, source.pulls)
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestRawEmptyAndZeroLimitDoNotInitializeOrPullSource(t *testing.T) {
	for name, empty := range map[string]bool{"zero-limit": false, "empty": true} {
		t.Run(name, func(t *testing.T) {
			source, slots := rawFixture()
			zero := uint32(0)
			plan := &RawPlan{Slots: slots, Empty: empty}
			if !empty {
				plan.Limit = &zero
			}
			op, err := WrapRaw(t.Context(), source, plan)
			require.NoError(t, err)
			require.NoError(t, op.Init(t.Context()))
			batch, err := op.NextBatch(t.Context())
			require.NoError(t, err)
			require.Nil(t, batch)
			require.Zero(t, source.inits)
			require.Zero(t, source.pulls)
			require.NoError(t, op.Close())
			require.Equal(t, 1, source.closes)
		})
	}
}

func TestRawCancellationClosesExactlyOnce(t *testing.T) {
	source, slots := rawFixture()
	op, err := WrapRaw(t.Context(), source, &RawPlan{Slots: slots})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	batch, err := op.NextBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, batch)
	require.NoError(t, op.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, source.pulls)
}

func TestRawMillisResidualFloorsNegativeNanoseconds(t *testing.T) {
	require.True(t, inMillisRanges(-1, []MillisRange{{Begin: -1, End: 0}}))
	require.False(t, inMillisRanges(-1, []MillisRange{{Begin: 0, End: 1}}))
}

func TestRawUnlimitedAndPaginationAcrossBatches(t *testing.T) {
	for _, limited := range []bool{false, true} {
		source, slots := rawFixture()
		batch := source.batches[0]
		source.batches = make([]*vectorized.RecordBatch, 129)
		for index := range source.batches {
			source.batches[index] = batch
		}
		plan := &RawPlan{Slots: slots}
		want := 516
		if limited {
			count := uint32(5)
			plan.Limit = &count
			plan.Offset = 3
			want = 5
		}
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := WrapRaw(ctx, source, plan)
		require.NoError(t, err)
		require.NoError(t, op.Init(ctx))
		count := 0
		for {
			result, pullErr := op.NextBatch(ctx)
			require.NoError(t, pullErr)
			if result == nil {
				break
			}
			count += result.ActiveLen()
		}
		require.Equal(t, want, count)
		require.NoError(t, op.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestRawSelectionBudgetFailureReleasesSource(t *testing.T) {
	source, slots := rawFixture()
	descriptorBytes, err := rawDescriptorBound(len(slots), 3, 1)
	require.NoError(t, err)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), descriptorBytes+7)
	op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots, Residual: true, Ranges: []MillisRange{{Begin: 0, End: 1}}})
	require.NoError(t, err)
	require.NoError(t, op.Init(ctx))
	batch, err := op.NextBatch(ctx)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, batch)
	require.Equal(t, 1, source.closes)
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestRawRejectsUnaddressableSelectionBatchAndClosesSource(t *testing.T) {
	source, slots := rawFixture()
	source.batches[0].Len = 65537
	op, err := WrapRaw(t.Context(), source, &RawPlan{Slots: slots})
	require.NoError(t, err)
	batch, err := op.NextBatch(t.Context())
	require.ErrorContains(t, err, "maximum")
	require.Nil(t, batch)
	require.Equal(t, 1, source.closes)
}

func TestRawDescriptorBudgetRejectsBeforeSourceInitialization(t *testing.T) {
	source, slots := rawFixture()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	require.NoError(t, tracker.Reserve(1))
	op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots})
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, op)
	require.Zero(t, source.inits)
	require.Zero(t, source.pulls)
	require.Equal(t, 1, source.closes)
	require.EqualValues(t, 1, tracker.Used())
	tracker.Release(1)
	require.Zero(t, tracker.Used())
}

func TestRawWideDescriptorsRespectBudgetAndKeepOriginalTracker(t *testing.T) {
	source, fixtureSlots := rawFixture()
	slots := make([]Slot, 64)
	for i := range slots {
		slots[i] = fixtureSlots[1]
		slots[i].ID = SlotID(i)
		slots[i].Output.Name = fmt.Sprintf("alias_%d", i)
	}
	denied := vectorized.WithQueryMemoryTracker(t.Context(), 16<<10)
	op, err := WrapRaw(denied, source, &RawPlan{Slots: slots})
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, op)
	require.Equal(t, 1, source.closes)
	require.Zero(t, source.pulls)
	require.Zero(t, vectorized.QueryMemoryTracker(denied, 0).Used())

	source, _ = rawFixture()
	allowed := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err = WrapRaw(allowed, source, &RawPlan{Slots: slots})
	require.NoError(t, err)
	require.Greater(t, vectorized.QueryMemoryTracker(allowed, 0).Used(), int64(len(slots)*80))
	other := vectorized.WithQueryMemoryTracker(t.Context(), 1)
	require.NoError(t, op.Init(other))
	_, err = op.NextBatch(other)
	require.NoError(t, err)
	require.NoError(t, op.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(allowed, 0).Used())
	require.Zero(t, vectorized.QueryMemoryTracker(other, 0).Used())
}

func TestRawConstructorAndIteratorInitializationFailuresReleaseMetadata(t *testing.T) {
	t.Run("invalid-source-column", func(t *testing.T) {
		source, slots := rawFixture()
		slots[0].Source.Name = "missing"
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		_, err := WrapRaw(ctx, source, &RawPlan{Slots: slots})
		require.ErrorContains(t, err, "absent")
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	})
	t.Run("native-init", func(t *testing.T) {
		source, slots := rawFixture()
		failure := errors.New("source init failed")
		source.initErr = failure
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots})
		require.NoError(t, err)
		_, err = RawIterator(ctx, op)
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, source.closes)
		require.Zero(t, source.pulls)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	})
	t.Run("timestamp-descriptor-budget", func(t *testing.T) {
		source, slots := rawFixture()
		bytes, err := rawDescriptorBound(len(slots), 3, 0)
		require.NoError(t, err)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), bytes+1)
		op, err := WrapRaw(ctx, source, &RawPlan{Slots: slots})
		require.NoError(t, err)
		_, err = RawIterator(ctx, op)
		require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
		require.Equal(t, 1, source.closes)
		require.Zero(t, source.inits)
		require.Zero(t, source.pulls)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	})
}
