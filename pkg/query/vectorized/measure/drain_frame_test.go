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
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// pinOwner is an Owner that counts its references, standing in for storage memory a
// drained batch borrows.
type pinOwner struct{ refs int }

func (o *pinOwner) Retain()  { o.refs++ }
func (o *pinOwner) Release() { o.refs-- }

type batchesSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	next    int
}

func (s *batchesSource) Init(context.Context) error            { return nil }
func (s *batchesSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *batchesSource) Close() error                          { return nil }
func (s *batchesSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.next >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.next]
	s.next++
	return b, nil
}

func drainSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "entity", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeFloat64},
	})
}

// drainBatch builds rows of drainSchema; svc borrows from owner-backed memory in the sense of
// the pin only, and entity is a constant column.
func drainBatch(schema *vectorized.BatchSchema, firstTS int64, rows int, owner vectorized.Owner) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, rows)
	for i := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
		b.Columns[1].(*vectorized.TypedColumn[int64]).Append(firstTS + int64(i))
		svc := b.Columns[2].(*vectorized.TypedColumn[string])
		if i%5 == 3 {
			svc.AppendNull()
		} else {
			svc.Append(fmt.Sprintf("svc-%d", (int(firstTS)+i)%4))
		}
		v := b.Columns[4].(*vectorized.TypedColumn[float64])
		if i%7 == 6 {
			v.AppendNull()
		} else {
			v.Append(float64(i) * 1.5)
		}
	}
	b.Columns[3] = vectorized.NewConstString("the-entity", rows)
	b.Len = rows
	if owner != nil {
		b.Owners.Pin(owner)
	}
	return b
}

func drainFrame(t *testing.T, batches ...*vectorized.RecordBatch) []byte {
	t.Helper()
	schema := drainSchema()
	pipeline, buildErr := vectorized.NewPipelineBuilder().From(&batchesSource{schema: schema, batches: batches}).Build()
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	ctx := context.Background()
	if initErr := pipeline.Init(ctx); initErr != nil {
		t.Fatalf("Init: %v", initErr)
	}
	body, drainErr := DrainPipelineToFrame(ctx, pipeline, schema)
	if drainErr != nil {
		t.Fatalf("DrainPipelineToFrame: %v", drainErr)
	}
	if closeErr := pipeline.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	return body
}

func decodedProtos(t *testing.T, body []byte) []*measurev1.InternalDataPoint {
	t.Helper()
	b, err := frame.Decode(body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	defer b.ReleaseOwners()
	return serializeBatchToProto(b, nil)
}

func requireSameFrameRows(t *testing.T, want, got []*measurev1.InternalDataPoint) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("rows: want %d, got %d", len(want), len(got))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Fatalf("row %d differs:\n want %v\n got  %v", i, want[i], got[i])
		}
	}
}

// TestDrainPipelineToFrame_MatchesCoalescedRows drains batches into a columnar frame: the decoded
// rows must equal the active rows of the batches coalesced into one, for one batch (encoded
// directly, constant column kept) and for several (coalesced), with and without a selection, and
// every owner a drained batch pins must be released.
func TestDrainPipelineToFrame_MatchesCoalescedRows(t *testing.T) {
	schema := drainSchema()
	cases := map[string]func(owner vectorized.Owner) []*vectorized.RecordBatch{
		"one batch": func(o vectorized.Owner) []*vectorized.RecordBatch {
			return []*vectorized.RecordBatch{drainBatch(schema, 100, 40, o)}
		},
		"one batch with selection": func(o vectorized.Owner) []*vectorized.RecordBatch {
			b := drainBatch(schema, 100, 40, o)
			for i := 0; i < 40; i += 3 {
				b.Selection = append(b.Selection, uint16(i))
			}
			return []*vectorized.RecordBatch{b}
		},
		"one batch with empty selection": func(o vectorized.Owner) []*vectorized.RecordBatch {
			b := drainBatch(schema, 100, 40, o)
			b.Selection = []uint16{}
			return []*vectorized.RecordBatch{b}
		},
		"three batches": func(o vectorized.Owner) []*vectorized.RecordBatch {
			return []*vectorized.RecordBatch{drainBatch(schema, 0, 10, o), drainBatch(schema, 50, 20, o), drainBatch(schema, 90, 5, nil)}
		},
		"no batches": func(vectorized.Owner) []*vectorized.RecordBatch { return nil },
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			want := vectorized.NewRecordBatch(schema, 0)
			for _, b := range build(nil) {
				if err := appendActive(want, b); err != nil {
					t.Fatal(err)
				}
			}
			owner := &pinOwner{}
			body := drainFrame(t, build(owner)...)
			if owner.refs != 0 {
				t.Fatalf("%d references on the drained batches' owner were not released", owner.refs)
			}
			if got := body[frame.MagicLen]; got != frame.WireVersion {
				t.Fatalf("frame carries version %d, want %d", got, frame.WireVersion)
			}
			requireSameFrameRows(t, serializeBatchToProto(want, nil), decodedProtos(t, body))
		})
	}
}

// recyclingDrainSource lends storage that must become reusable before the next pull.
type recyclingDrainSource struct {
	schema *vectorized.BatchSchema
	owner  pinOwner
	next   int
}

func (s *recyclingDrainSource) Init(context.Context) error            { return nil }
func (s *recyclingDrainSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *recyclingDrainSource) Close() error                          { return nil }
func (s *recyclingDrainSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.owner.refs != 0 {
		return nil, fmt.Errorf("previous scan storage still pinned during next pull: %d", s.owner.refs)
	}
	if s.next == 3 {
		return nil, nil
	}
	b := drainBatch(s.schema, int64(s.next*10), 10, &s.owner)
	s.next++
	return b, nil
}

func TestDrainPipelineToFrameReleasesStorageBeforeNextPull(t *testing.T) {
	schema := drainSchema()
	source := &recyclingDrainSource{schema: schema}
	pipeline, err := vectorized.NewPipelineBuilder().From(source).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := pipeline.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	ctx := context.Background()
	if err = pipeline.Init(ctx); err != nil {
		t.Fatal(err)
	}
	body, err := DrainPipelineToFrame(ctx, pipeline, schema)
	if err != nil {
		t.Fatal(err)
	}
	if rows := len(decodedProtos(t, body)); rows != 30 {
		t.Fatalf("rows = %d, want 30", rows)
	}
	if source.owner.refs != 0 {
		t.Fatalf("owner refs = %d", source.owner.refs)
	}
}

func TestDrainPipelineToFrameWideBudgetAndCleanup(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{{Role: vectorized.RoleTag, TagFamily: "default", Name: "payload", Type: vectorized.ColumnTypeString}})
	for _, budget := range []int64{8 << 20, 512} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			owner := &pinOwner{}
			var batches []*vectorized.RecordBatch
			for part := range 3 {
				batch := vectorized.NewRecordBatch(schema, 256)
				for row := range 256 {
					batch.Columns[0].(*vectorized.TypedColumn[string]).Append(fmt.Sprintf("%d-%d-%s", part, row, strings.Repeat("w", 2048)))
				}
				batch.Len = 256
				batch.Owners.Pin(owner)
				batches = append(batches, batch)
			}
			tracker := vectorized.NewMemoryTracker(budget)
			pipeline, err := vectorized.NewPipelineBuilder().From(&batchesSource{schema: schema, batches: batches}).WithMemoryTracker(tracker).Build()
			if err != nil {
				t.Fatal(err)
			}
			ctx, scope := query.NewTransportScope(context.Background())
			defer scope.Close()
			if err = pipeline.Init(ctx); err != nil {
				t.Fatal(err)
			}
			body, err := DrainPipelineToFrame(ctx, pipeline, schema)
			if closeErr := pipeline.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			// This simple source does not own batches that were never pulled.
			for _, batch := range batches {
				batch.ReleaseOwners()
			}
			if budget == 512 {
				if !errors.Is(err, vectorized.ErrMemoryBudgetExceeded) || body != nil {
					t.Fatalf("body %v, error %v", body, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				decoded, decodeErr := frame.Decode(body)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if decoded.Len != 768 {
					t.Fatalf("rows %d, want 768", decoded.Len)
				}
				decoded.ReleaseOwners()
			}
			if err == nil && tracker.Used() < 2*int64(cap(body)) {
				t.Fatalf("transport reservation %d does not cover frame + transport copy %d", tracker.Used(), 2*cap(body))
			}
			scope.Close()
			if tracker.Used() != 0 || owner.refs != 0 {
				t.Fatalf("tracker %d, owner refs %d", tracker.Used(), owner.refs)
			}
		})
	}
}

type cancelDrainSource struct {
	cancel context.CancelFunc
	recyclingDrainSource
}

func (s *cancelDrainSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if s.next == 1 {
		s.cancel()
		return nil, ctx.Err()
	}
	return s.recyclingDrainSource.NextBatch(ctx)
}

func TestDrainPipelineToFrameCancellationReleasesBufferedFrames(t *testing.T) {
	schema := drainSchema()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &cancelDrainSource{recyclingDrainSource: recyclingDrainSource{schema: schema}, cancel: cancel}
	tracker := vectorized.NewMemoryTracker(1 << 20)
	pipeline, err := vectorized.NewPipelineBuilder().From(source).WithMemoryTracker(tracker).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = pipeline.Init(ctx); err != nil {
		t.Fatal(err)
	}
	body, err := DrainPipelineToFrame(ctx, pipeline, schema)
	if closeErr := pipeline.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, context.Canceled) || body != nil {
		t.Fatalf("body %v, error %v", body, err)
	}
	if tracker.Used() != 0 || source.owner.refs != 0 {
		t.Fatalf("tracker %d, owner refs %d", tracker.Used(), source.owner.refs)
	}
}
