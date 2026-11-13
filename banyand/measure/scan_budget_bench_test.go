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
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BenchmarkBudgetedTimeOrderedScan runs a time-ordered scan with the shared query budget
// attached, as a query on a data node does, over series that overlap in time. The limit
// is large enough for every cursor to be admitted under any envelope, so only the cost of
// the accounting and the scan itself differs.
func BenchmarkBudgetedTimeOrderedScan(b *testing.B) {
	// The package tests poison released cursors; production does not, so measure without it.
	defer setPoisonReleasedCursor(setPoisonReleasedCursor(false))
	const series, rows = 100, 360
	var part fxPart
	for sid := common.SeriesID(1); sid <= series; sid++ {
		part = append(part, fxRange{sid, 1, rows, 1})
	}
	fx := newFixture(b, part)
	for _, w := range []struct {
		name   string
		maxTS  int64
		wantTS int64
	}{{"window15of360", 15, 15}, {"window360of360", rows, rows}} {
		b.Run(w.name, func(b *testing.B) {
			q := prefetchQuery(1, w.maxTS, true, true)
			schema := q.schema(b)
			b.ReportAllocs()
			b.ResetTimer()
			total := 0
			for b.Loop() {
				lease := &scanReuseLease{limit: 1 << 30}
				ctx := vectorized.WithQueryMemoryTracker(query.WithBudgetLease(context.Background(), lease), 1<<30)
				result := fx.newResult(b, q, schema)
				result.ctx = ctx
				result.budget = newScanBudget(1<<30, result.dp)
				if err := result.budget.attachShared(ctx, len(result.batchSchema.Columns), 1024, len(result.data)); err != nil {
					b.Fatal(err)
				}
				for {
					mb, err := result.PullBatch(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if mb == nil {
						break
					}
					total += mb.ActiveRows()
					mb.Release()
				}
				result.Release()
			}
			if total == 0 {
				b.Fatal(fmt.Sprintf("no rows scanned for %s", w.name))
			}
		})
	}
}
