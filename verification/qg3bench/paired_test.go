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

package qg3bench

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type pairedQuery struct {
	name, selection, suffix, want string
	native                        bool
}

func expectedGroups(rows int) string {
	n := rows / 8
	values := make([]string, 8)
	for i := range values {
		values[i] = fmt.Sprintf("r%d:%d", i, n*(2*i*n+n+1)/2)
	}
	return strings.Join(values, ";")
}

func runPaired(b *testing.B, ctx context.Context, client bydbqlv1.BydbQLServiceClient, base time.Time, rows int, expected, grouped string) {
	queries := []pairedQuery{
		{"scalar_int", "SUM(value)", "", expected, false},
		{"scalar_decimal", "SUM(DECIMAL_FROM_SCALED(value, 10))", "", decimalExpected(expected), false},
		{"group_int", "rid, SUM(value)", " GROUP BY rid", grouped, false},
		{"group_decimal", "rid, SUM(DECIMAL_FROM_SCALED(value, 10))", " GROUP BY rid", decimalExpected(grouped), false},
	}

	for _, q := range queries {
		actual, err := runQuery(ctx, client, base, q.selection, q.suffix, true)
		if err != nil || actual != q.want {
			b.Fatalf("warmup %s: %q want %q: %v", q.name, actual, q.want, err)
		}
		before := liaisongrpc.BydbQLPlanCacheStats()
		nativeBefore := ql.NativeAggregateExecutionStats()
		stateBefore := ql.AggregateExecutionStats()
		actual, err = runQuery(ctx, client, base, q.selection, q.suffix, true)
		after := liaisongrpc.BydbQLPlanCacheStats()
		nativeAfter := ql.NativeAggregateExecutionStats()
		if err != nil || actual != q.want || after.Hits <= before.Hits || after.Compiles != before.Compiles {
			b.Fatalf("verification %s: result=%q err=%v cache before=%+v after=%+v", q.name, actual, err, before, after)
		}
		if q.native && nativeAfter.Executions <= nativeBefore.Executions {
			b.Fatalf("%s did not execute native merge: before=%+v after=%+v", q.name, nativeBefore, nativeAfter)
		}
		stateAfter := ql.AggregateExecutionStats()
		b.Logf("verified %s cache_hit=true native_merge=%t state_bytes=%d state_capacity=%d tracked_process_peak=%d live_bytes=%d", q.name,
			nativeAfter.Executions > nativeBefore.Executions, stateAfter.StateBytes-stateBefore.StateBytes,
			stateAfter.StateCapacity-stateBefore.StateCapacity, vectorized.QueryMemoryStats().PeakReservedBytes, vectorized.QueryMemoryStats().LiveReservedBytes)
	}
	iterations := 1000
	if rows > 64 {
		iterations = 300
	}
	round, err := strconv.Atoi(os.Getenv("QG3_ROUND"))
	if err != nil || round < 0 || round >= 5 {
		b.Fatal("QG3_ROUND must be between 0 and 4")
	}
	for i := range queries {
		index := (i + round) % len(queries)
		if round%2 == 1 {
			index = (len(queries) - 1 - i + round) % len(queries)
		}
		q := queries[index]
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		startCPU := cpuTime()
		start := time.Now()
		for j := 0; j < iterations; j++ {
			actual, err := runQuery(ctx, client, base, q.selection, q.suffix, true)
			if err != nil || actual != q.want {
				b.Fatalf("sample %s: %q want %q: %v", q.name, actual, q.want, err)
			}
		}
		elapsed, cpu := time.Since(start), cpuTime()-startCPU
		runtime.ReadMemStats(&after)
		b.Logf("SAMPLE rows=%d round=%d case=%s n=%d ns/op=%d cpu-ns/op=%d bytes/op=%d allocs/op=%d", rows, round, q.name, iterations,
			elapsed.Nanoseconds()/int64(iterations), cpu.Nanoseconds()/int64(iterations), (after.TotalAlloc-before.TotalAlloc)/uint64(iterations),
			(after.Mallocs-before.Mallocs)/uint64(iterations))
	}
}

func decimalExpected(integer string) string {
	values := strings.Split(integer, ";")
	for i, value := range values {
		parts := strings.SplitN(value, ":", 2)
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			panic(err)
		}
		values[i] = fmt.Sprintf("%s:%d.%010d", parts[0], n/10000000000, n%10000000000)
	}
	return strings.Join(values, ";")
}
