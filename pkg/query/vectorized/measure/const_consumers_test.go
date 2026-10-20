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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// constPair builds the same rows twice: one batch lent from a single series, whose
// entity tags svc and inst are ConstColumns, and one whose columns are all typed.
// Every consumer of a batch must give the same result for both.
func constPair(nullEntity bool) (constBatch, typedBatch *vectorized.RecordBatch) {
	schema := runsSchema()
	rows := make([]runsRow, 0, 12)
	for i := range 12 {
		r := runsRow{
			ts: int64(1000 + 100*i), svc: "svc-a", inst: 5,
			note: fmt.Sprintf("n%d", i%3), vi: int64(i*7 - 20), vf: 0.1 * float64(i+1),
			viNull: i == 4, vfNull: i == 9,
		}
		if i%4 != 0 {
			r.name = fmt.Sprintf("name-%d", i%2)
		}
		rows = append(rows, r)
	}
	b := runsBatch{rows: rows, order: vectorized.TimeOrderAsc}
	constBatch, typedBatch = b.build(schema, true), b.build(schema, false)
	if nullEntity {
		constBatch.Columns[runsColSvc] = vectorized.NewConstNull(vectorized.ColumnTypeString, len(rows))
		constBatch.Columns[runsColInst] = vectorized.NewConstNull(vectorized.ColumnTypeInt64, len(rows))
		svc := vectorized.NewStringColumn(len(rows))
		inst := vectorized.NewInt64Column(len(rows))
		for range rows {
			svc.AppendNull()
			inst.AppendNull()
		}
		typedBatch.Columns[runsColSvc], typedBatch.Columns[runsColInst] = svc, inst
	}
	return constBatch, typedBatch
}

func drainBreaker(t *testing.T, op interface {
	Init(ctx context.Context) error
	Consume(ctx context.Context, b *vectorized.RecordBatch) error
	Finalize(ctx context.Context) error
	NextBatch(ctx context.Context) (*vectorized.RecordBatch, error)
	Close() error
}, batches ...*vectorized.RecordBatch,
) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, op.Init(ctx))
	defer op.Close()
	for _, b := range batches {
		require.NoError(t, op.Consume(ctx, b))
	}
	require.NoError(t, op.Finalize(ctx))
	var sb strings.Builder
	for {
		out, err := op.NextBatch(ctx)
		require.NoError(t, err)
		if out == nil {
			return sb.String()
		}
		sb.WriteString(dumpBatch(out))
	}
}

// TestConsumersHandleConstColumns runs every consumer of batch columns over a batch
// with ConstColumns and the same batch typed, and requires identical results.
func TestConsumersHandleConstColumns(t *testing.T) {
	schema := runsSchema()
	tracker := func() *vectorized.MemoryTracker { return vectorized.NewMemoryTracker(1 << 30) }
	consumers := map[string]func(t *testing.T, b *vectorized.RecordBatch) string{
		"BatchGroupBy keyed by constants": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchGroupBy(schema, []int{runsColSvc, runsColInst}, vectorized.NewBatchPool(schema, 64), 64, tracker(), 8, 8), b)
		},
		"BatchGroupBy keyed by a typed column": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchGroupBy(schema, []int{runsColNote}, vectorized.NewBatchPool(schema, 64), 64, tracker(), 8, 8), b)
		},
		"BatchGroupBy first-only": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchGroupByFirst(schema, []int{runsColSvc}, vectorized.NewBatchPool(schema, 64), 64, tracker(), 8), b)
		},
		"BatchAggregation keyed by constants": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchAggregation(schema, []int{runsColSvc, runsColInst}, runsAggSpecs(), AggModeAll, 64, tracker(), 8), b)
		},
		"BatchAggregation map mode": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchAggregation(schema, []int{runsColInst}, runsAggSpecs(), AggModeMap, 64, tracker(), 8), b)
		},
		"BatchAggregation scalar": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchAggregation(schema, nil, runsAggSpecs(), AggModeAll, 64, tracker(), 8), b)
		},
		"BatchAggregation with runs off": func(t *testing.T, b *vectorized.RecordBatch) string {
			defer SetRunsDisabled(SetRunsDisabled(true))
			return drainBreaker(t, NewBatchAggregation(schema, []int{runsColSvc}, runsAggSpecs(), AggModeAll, 64, tracker(), 8), b)
		},
		"BatchTimeBucket with runs off": func(t *testing.T, b *vectorized.RecordBatch) string {
			defer SetRunsDisabled(SetRunsDisabled(true))
			bt := NewBatchTimeBucket(&runsSource{schema: schema, batches: []*vectorized.RecordBatch{b}}, schema, []int{runsColSvc}, runsColTS, 500,
				runsAggSpecs(), AggModeAll, 64, tracker(), 8, true)
			require.NoError(t, bt.Init(context.Background()))
			defer bt.Close()
			var sb strings.Builder
			for {
				out, err := bt.NextBatch(context.Background())
				require.NoError(t, err)
				if out == nil {
					return sb.String()
				}
				sb.WriteString(dumpBatch(out))
			}
		},
		"BatchTop": func(t *testing.T, b *vectorized.RecordBatch) string {
			return drainBreaker(t, NewBatchTop(schema, runsColVI, 4, false, 64), b)
		},
		"BatchLimit": func(t *testing.T, b *vectorized.RecordBatch) string {
			limit := NewBatchLimit(schema, 2, 5)
			require.ErrorIs(t, limit.Process(context.Background(), b), vectorized.ErrLimitExhausted)
			return fmt.Sprint(b.Selection)
		},
		"BatchColumnFilter": func(t *testing.T, b *vectorized.RecordBatch) string {
			keep := func(col vectorized.Column, row int) bool {
				if c, ok := vectorized.AsConst(col); ok {
					return c.Str() == "svc-a"
				}
				return col.(*vectorized.TypedColumn[string]).Data()[row] == "svc-a"
			}
			require.NoError(t, NewBatchColumnFilter(schema, runsColSvc, keep).Process(context.Background(), b))
			return fmt.Sprint(b.ActiveLen())
		},
		"serializeBatchToProto": func(_ *testing.T, b *vectorized.RecordBatch) string {
			var sb strings.Builder
			for _, idp := range serializeBatchToProto(b, nil) {
				sb.WriteString(prototext.MarshalOptions{Multiline: false}.Format(idp))
				sb.WriteByte('\n')
			}
			return sb.String()
		},
		"appendActive": func(t *testing.T, b *vectorized.RecordBatch) string {
			dst := vectorized.NewRecordBatch(schema, 16)
			require.NoError(t, appendActive(dst, b))
			return dumpBatch(dst)
		},
		"appendActive with a selection": func(t *testing.T, b *vectorized.RecordBatch) string {
			b.Selection = []uint16{7, 3, 3, 0}
			dst := vectorized.NewRecordBatch(schema, 16)
			require.NoError(t, appendActive(dst, b))
			return dumpBatch(dst)
		},
		"vectorized.AppendActiveRows": func(t *testing.T, b *vectorized.RecordBatch) string {
			b.Selection = []uint16{9, 1, 2}
			dst := vectorized.NewRecordBatch(schema, 16)
			require.NoError(t, vectorized.AppendActiveRows(dst, b))
			return dumpBatch(dst)
		},
		"vectorized.ConsumeChunked": func(t *testing.T, b *vectorized.RecordBatch) string {
			op := NewBatchGroupBy(schema, []int{runsColSvc}, vectorized.NewBatchPool(schema, 64), 64, tracker(), 8, 8)
			scratch := vectorized.NewRecordBatch(schema, 5)
			require.NoError(t, op.Init(context.Background()))
			defer op.Close()
			require.NoError(t, vectorized.ConsumeChunked(op, scratch, b, 5))
			require.NoError(t, op.Finalize(context.Background()))
			var sb strings.Builder
			for {
				out, err := op.NextBatch(context.Background())
				require.NoError(t, err)
				if out == nil {
					return sb.String()
				}
				sb.WriteString(dumpBatch(out))
			}
		},
		"frame.Encode": func(t *testing.T, b *vectorized.RecordBatch) string {
			b.Selection = []uint16{0, 4, 5, 11}
			body, err := frame.Encode(b)
			require.NoError(t, err)
			return dumpFrame(t, body)
		},
		"DrainPipelineToFrame": func(t *testing.T, b *vectorized.RecordBatch) string {
			pipeline, err := vectorized.NewPipelineBuilder().From(&runsSource{schema: schema, batches: []*vectorized.RecordBatch{b}}).Build()
			require.NoError(t, err)
			require.NoError(t, pipeline.Init(context.Background()))
			defer pipeline.Close()
			body, err := DrainPipelineToFrame(context.Background(), pipeline, schema)
			require.NoError(t, err)
			return dumpFrame(t, body)
		},
		"columnStringAt": func(_ *testing.T, b *vectorized.RecordBatch) string {
			var sb strings.Builder
			for row := range b.Len {
				sb.WriteString(columnStringAt(b.Columns[runsColSvc], row))
				sb.WriteString(columnStringAt(b.Columns[runsColInst], row))
			}
			return sb.String()
		},
	}
	for _, nullEntity := range []bool{false, true} {
		for name, consumer := range consumers {
			t.Run(fmt.Sprintf("%s/nullEntity=%v", name, nullEntity), func(t *testing.T) {
				constBatch, typedBatch := constPair(nullEntity)
				_, isConst := vectorized.AsConst(constBatch.Columns[runsColSvc])
				require.True(t, isConst)
				want := consumer(t, typedBatch)
				got := consumer(t, constBatch)
				require.NotEmpty(t, want)
				require.Equal(t, want, got)
			})
		}
	}
}

// dumpFrame decodes a frame body and dumps the batch: a constant column is sent as one value, so the
// bytes of a frame differ from the bytes of the same rows in a flat column while the rows are equal.
func dumpFrame(t *testing.T, body []byte) string {
	t.Helper()
	decoded, err := frame.Decode(body)
	require.NoError(t, err)
	defer decoded.ReleaseOwners()
	return dumpBatch(decoded)
}

func TestReduceExpandsConstInputs(t *testing.T) {
	s := aggReduceSchema()
	build := func(constant bool) *vectorized.RecordBatch {
		b := vectorized.NewRecordBatch(s, 3)
		for _, v := range []int64{10, 20, 30} {
			b.Columns[0].(*vectorized.TypedColumn[int64]).Append(v / 10)
			b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
			b.Columns[2].(*vectorized.TypedColumn[int64]).Append(v)
		}
		b.Len = 3
		if constant {
			b.Columns[1] = vectorized.NewConstString("a", 3)
			b.Columns[2] = vectorized.NewConstInt64(10, 3)
			b.Columns[0] = vectorized.NewConstInt64(1, 3)
		} else {
			b.Columns[2] = vectorized.NewInt64Column(3)
			b.Columns[0] = vectorized.NewInt64Column(3)
			for range 3 {
				b.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
				b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
			}
		}
		return b
	}
	run := func(constant bool) string {
		op := NewBatchAggregation(s, []int{1}, []AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}}, AggModeReduce, 8, vectorized.NewMemoryTracker(1<<30), 0)
		return drainBreaker(t, op, build(constant))
	}
	require.Equal(t, run(false), run(true))
}
