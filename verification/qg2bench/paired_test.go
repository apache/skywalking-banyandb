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

package qg2bench

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
	n := rows / 8
	floatWant := fmt.Sprintf(":%d", n*(n+1)/2)
	queries := []pairedQuery{
		{"float_sum", "SUM(fv)", " AND rid = 'r0'", floatWant, true},
		{"scalar_sum", "SUM(value)", "", expected, false},
		{"group_sum", "rid, SUM(value)", " GROUP BY rid", grouped, false},
	}
	if os.Getenv("QG2_BASELINE") != "1" {
		queries = append(queries,
			pairedQuery{"float_coalesce", "COALESCE(SUM(fv), 0)", " AND rid = 'r0'", floatWant, true},
			pairedQuery{"scalar_coalesce", "COALESCE(SUM(value), 0)", "", expected, false},
			pairedQuery{"scalar_computed", "SUM(prior + 1)", "", expected, false},
			pairedQuery{"group_coalesce", "rid, COALESCE(SUM(value), 0)", " GROUP BY rid", grouped, false},
			pairedQuery{"group_computed", "rid, SUM(prior + 1)", " GROUP BY rid", grouped, false})
	}
	for _, q := range queries {
		actual, err := runQuery(ctx, client, base, q.selection, q.suffix, true)
		if err != nil || actual != q.want {
			b.Fatalf("warmup %s: %q want %q: %v", q.name, actual, q.want, err)
		}
		before := liaisongrpc.BydbQLPlanCacheStats()
		nativeBefore := ql.NativeAggregateExecutionStats()
		actual, err = runQuery(ctx, client, base, q.selection, q.suffix, true)
		after := liaisongrpc.BydbQLPlanCacheStats()
		nativeAfter := ql.NativeAggregateExecutionStats()
		if err != nil || actual != q.want || after.Hits <= before.Hits || after.Compiles != before.Compiles {
			b.Fatalf("verification %s: result=%q err=%v cache before=%+v after=%+v", q.name, actual, err, before, after)
		}
		if q.native && nativeAfter.Executions <= nativeBefore.Executions {
			b.Fatalf("%s did not execute native merge: before=%+v after=%+v", q.name, nativeBefore, nativeAfter)
		}
		b.Logf("verified %s cache_hit=true native_merge=%t", q.name, nativeAfter.Executions > nativeBefore.Executions)
	}
	iterations := 1800
	if rows > 64 {
		iterations = 500
	}
	round, err := strconv.Atoi(os.Getenv("QG2_ROUND"))
	if err != nil || round < 0 || round >= 5 {
		b.Fatal("QG2_ROUND must be between 0 and 4")
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

func runPairedIfRequested(b *testing.B, ctx context.Context, client bydbqlv1.BydbQLServiceClient,
	base time.Time, rows int, expected, grouped string,
) bool {
	if os.Getenv("QG2_PAIRED") != "1" {
		return false
	}
	runPaired(b, ctx, client, base, rows, expected, grouped)
	return true
}
