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

package trace

import (
	"math"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/series"
)

// Commit: 01c28bc0b8cccc927780a2e1cad7293cf71f9276
// go test -bench=. -benchtime=60s -benchmem ./banyand/series/trace/...
// goos: darwin
// goarch: amd64
// pkg: github.com/apache/skywalking-banyandb/banyand/series/trace
// cpu: Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
// Benchmark_Query_ScanEntity-8   	 1982455	     35238 ns/op	   14166 B/op	     333 allocs/op
// PASS
// Current commit:
// Benchmark_Query_ScanEntity-8   	 2146119	     32553 ns/op	   12923 B/op	     288 allocs/op
func Benchmark_Query_ScanEntity(b *testing.B) {
	baseTS := uint64((time.Now().UnixNano() / 1e6) * 1e6)
	ts, stopFunc := setup(b)
	defer stopFunc()
	setUpTestData(b, ts, testData(baseTS))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ts.ScanEntity(0, math.MaxUint64, series.ScanOptions{
			Projection: []string{"data_binary", "trace_id"},
			State:      series.TraceStateDefault,
		})
	}
}

// Commit: 01c28bc0b8cccc927780a2e1cad7293cf71f9276
// go test -bench=Benchmark_Query_FetchTrace -run=^a -benchtime=60s -benchmem ./banyand/series/trace/...
// goos: darwin
// goarch: amd64
// pkg: github.com/apache/skywalking-banyandb/banyand/series/trace
// cpu: Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
// Benchmark_Query_FetchTrace-8   	 4519570	     16007 ns/op	    6279 B/op	     149 allocs/op
// PASS
// ok  	github.com/apache/skywalking-banyandb/banyand/series/trace	153.538s
func Benchmark_Query_FetchTrace(b *testing.B) {
	baseTS := uint64((time.Now().UnixNano() / 1e6) * 1e6)
	ts, stopFunc := setup(b)
	defer stopFunc()
	setUpTestData(b, ts, testData(baseTS))

	traceID := "trace_id-xxfff.111323"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ts.FetchTrace(traceID, series.ScanOptions{Projection: []string{"data_binary", "trace_id"}})
	}
}
