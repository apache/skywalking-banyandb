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
