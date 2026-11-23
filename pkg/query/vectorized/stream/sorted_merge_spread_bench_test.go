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

package stream

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// countingLease is a lease that records what is charged now and the most it ever was.
type countingLease struct {
	used atomic.Uint64
	peak atomic.Uint64
}

func (l *countingLease) Charge(bytes uint64) error {
	now := l.used.Add(bytes)
	for {
		peak := l.peak.Load()
		if now <= peak || l.peak.CompareAndSwap(peak, now) {
			return nil
		}
	}
}

func (l *countingLease) Refund(bytes uint64) { l.used.Add(^(bytes - 1)) }
func (*countingLease) Limit() uint64         { return 96 << 20 }
func (l *countingLease) Used() uint64        { return l.used.Load() }
func (*countingLease) Owner() any            { return "compaction-test" }

// spreadBatches returns one batch per series. Row r of series s has timestamp r*series+s, so the
// newest rows of the whole corpus come from as many different series, and so batches, as possible.
func spreadBatches(schema *vectorized.BatchSchema, series, rowsPerBatch int) []*vectorized.RecordBatch {
	batches := make([]*vectorized.RecordBatch, 0, series)
	for s := 0; s < series; s++ {
		rows := make([]testRow, rowsPerBatch)
		for r := range rows {
			rows[r] = testRow{
				tag:      fmt.Sprintf("svc-%04d-%s", s, strings.Repeat("x", 40)),
				ts:       int64(r*series + s),
				elemID:   uint64(s*rowsPerBatch + r + 1),
				orderKey: []byte(fmt.Sprintf("%012d", r*series+s)),
			}
		}
		batches = append(batches, buildBatch(schema, rows))
	}
	return batches
}

// BenchmarkSortedMergeSpreadAcrossSeries merges one batch per series where the newest rows of the corpus
// come from as many different batches as possible, which is what a top-N over a wide window and many
// series looks like. It reports the most the merge ever had charged to the query lease.
func BenchmarkSortedMergeSpreadAcrossSeries(b *testing.B) {
	const series, rowsPerBatch = 400, 2048
	schema := tsSchema()
	corpus := spreadBatches(schema, series, rowsPerBatch)
	for _, limit := range []int{20, 100, 1000} {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			var peak, held uint64
			for b.Loop() {
				lease := &countingLease{}
				ctx := query.WithBudgetLease(context.Background(), lease)
				m := NewSortedMergeWithCap(schema, true, rowsPerBatch, limit)
				require.NoError(b, m.Init(ctx))
				for _, batch := range corpus {
					require.NoError(b, m.Consume(ctx, batch))
				}
				require.NoError(b, m.Finalize(ctx))
				peak, held = lease.peak.Load(), lease.Used()
				require.NoError(b, m.Close())
			}
			b.ReportMetric(float64(peak), "peak-charge-bytes")
			b.ReportMetric(float64(held), "held-after-finalize-bytes")
		})
	}
}
