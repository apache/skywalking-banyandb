// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. The Apache Software Foundation (ASF) licenses this file to you under
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

// Layer 1 of Phase 9 — engine microbenchmarks for the shared sampler. The file
// lives next to sampler_bench_test.go in the same package, so the existing
// benchPlugins / benchEntries / benchBatch / mustSampler / runDecide / benchSink
// helpers are reused rather than re-declared.
//
// Layer 1 only covers synthetic-data engine microbenchmarks. No production-ratio
// claim, no calibrated fixture, no .so loading. See
// docs/design/phase-9-sw-trace-sampler-bench.md §Layer 1.
package tracesampler

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// realisticMixBucket returns the deterministic per-trace classification used by
// the cumulative-config benchmark. The buckets are calibrated against the
// production trace distribution documented in the Phase 9 design:
//
//	0..59  -> error     (60% — the calibrated error rate)
//	60..68 -> tagMatch  (9%  — the schema's matching tag rule)
//	69     -> slow      (1%  — duration envelope exceeds the threshold)
//	70..99 -> healthy   (30% — no rule matches; falls through to the sample)
//
// The per-trace id is the single source of truth: every cumulative config
// sees the SAME trace stream, so the per-rule cost deltas are attributable
// to the added rule rather than to input drift. FNV-1a is the same hash
// sampleFraction uses, so healthy-bucket sampling stays deterministic
// across benchmark runs.
func realisticMixBucket(traceID string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(traceID))
	return int(h.Sum64() % 100)
}

// matchEntryForPlugin returns the entry that the calibrated production rule
// expects to find. SkyWalking ships db.type=PostgreSQL; Zipkin's production
// rule is a regex on http.status_code=5xx, so we encode a 503 entry that
// the regex matches when one is configured.
func matchEntryForPlugin(p benchPlugin) string {
	if p.name == "zipkin" {
		return "http.status_code=503"
	}
	return "db.type=PostgreSQL"
}

// tagRuleForPlugin returns the JSON object form of one keeps-tag rule
// for the given plugin: equals for SkyWalking, regex for Zipkin.
func tagRuleForPlugin(p benchPlugin) string {
	if p.name == "zipkin" {
		return `{"tagKey":"http.status_code","regex":"5\\d\\d"}`
	}
	return `{"tagKey":"db.type","equals":"PostgreSQL"}`
}

// benchRealisticMixBatch builds a batch whose trace ids flow the calibrated
// mix past every cumulative config. rows is fixed at 3 and entries at 8
// to match the production calibration; one row would underweight the
// duration envelope's per-row cost and shift the per-trace ns reading.
func benchRealisticMixBatch(b *testing.B, p benchPlugin, traces int) *sdk.TraceBatch {
	b.Helper()
	const (
		rows    = 3
		entries = 8
		// slowDurationMs is well above the schema's threshold (500ms for
		// segment, 1000ms for zipkin) so the duration rule fires.
		slowDurationMs int64 = 5000
	)
	hitEntry := matchEntryForPlugin(p)
	blocks := make([]sdk.TraceBlock, 0, traces)
	for t := 0; t < traces; t++ {
		traceID := fmt.Sprintf("mix-trace-%d", t)
		bucket := realisticMixBucket(traceID)
		tb := sdktest.NewTrace(traceID)
		for r := 0; r < rows; r++ {
			tb.TagAs(p.startCol, valuetype.ValueTypeTimestamp, int64(r)*int64(1_000_000))
			rowDur := int64(3)
			if bucket == 69 {
				rowDur = slowDurationMs
			}
			tb.Tag(p.durCol, rowDur)
			if p.errCol != "" {
				if bucket <= 59 {
					tb.Tag(p.errCol, int64(1))
				} else {
					tb.Tag(p.errCol, int64(0))
				}
			}
			tagEntries := benchEntries(entries, false)
			if bucket >= 60 && bucket <= 68 {
				tagEntries[0] = hitEntry
			}
			tb.Tag(p.arrayCol, tagEntries)
		}
		blk, buildErr := tb.Build()
		if buildErr != nil {
			b.Fatalf("fixture: %v", buildErr)
		}
		blocks = append(blocks, blk)
	}
	return sdktest.Batch(blocks...)
}

// benchTraceWithTagMatchAt builds a single trace whose tags array has the
// rule-matching entry at position matchAt (0-indexed). The remaining
// entries are non-matching filler. Used by the tag-match-position sweep
// to verify the early-exit scan: a hit at index 0 should short-circuit,
// a hit at the last index exercises the full scan.
func benchTraceWithTagMatchAt(p benchPlugin, traceID string, rows, entries, matchAt int) (sdk.TraceBlock, error) {
	hitEntry := matchEntryForPlugin(p)
	tb := sdktest.NewTrace(traceID)
	for r := 0; r < rows; r++ {
		tb.TagAs(p.startCol, valuetype.ValueTypeTimestamp, int64(r)*int64(1_000_000))
		tb.Tag(p.durCol, int64(3))
		if p.errCol != "" {
			tb.Tag(p.errCol, int64(0))
		}
		tagEntries := benchEntries(entries, false)
		if matchAt >= 0 && matchAt < len(tagEntries) {
			tagEntries[matchAt] = hitEntry
		}
		tb.Tag(p.arrayCol, tagEntries)
	}
	return tb.Build()
}

// BenchmarkDecide_CumulativeConfig sweeps the production rule set in
// evaluation order, on a realistic-mix input. Each cumulative config
// adds ONE rule, so the per-rule cost is the delta from the previous
// row. The same trace ids flow through every config — the bucket
// classifier is deterministic — so deltas are attributable to the
// added rule, not to input drift.
//
// Configurations:
//
//	duration          -> {durationThresholdMs}
//	duration+err      -> adds keepErrors
//	duration+err+tag  -> adds the schema's matching tag rule
//	full              -> adds healthySampleRate (the production scenario)
//
// Why no exact-count gate: the calibrated ratio is 35.0025% — a 3% noise
// floor on a 64-trace batch is wider than the ratio itself. The Phase 9
// design explicitly defers the ratio claim to Layer 3.
func BenchmarkDecide_CumulativeConfig(b *testing.B) {
	const traces = 64
	for _, p := range benchPlugins() {
		cumulative := []struct {
			name string
			cfg  string
		}{
			{
				name: "duration",
				cfg:  fmt.Sprintf(`{"durationThresholdMs":%d}`, p.durationMs),
			},
			{
				name: "duration+err",
				cfg:  fmt.Sprintf(`{"durationThresholdMs":%d,"keepErrors":true}`, p.durationMs),
			},
			{
				name: "duration+err+tag",
				cfg: fmt.Sprintf(
					`{"durationThresholdMs":%d,"keepErrors":true,"keepTagRules":[%s]}`,
					p.durationMs, tagRuleForPlugin(p),
				),
			},
			{
				name: "full",
				cfg:  p.scenario,
			},
		}
		for _, c := range cumulative {
			b.Run(p.name+"/"+c.name, func(b *testing.B) {
				s := mustSampler(b, c.cfg, p.schema)
				runDecide(b, s, benchRealisticMixBatch(b, p, traces))
			})
		}
	}
}

// BenchmarkDecide_RowCount sweeps rows per trace. The duration envelope
// cost is per-row (min/max over rows) and the tag-array decode reads one
// row at a time, so this isolates the per-row cost component. All other
// axes (entries=8, all-miss input) are held constant.
func BenchmarkDecide_RowCount(b *testing.B) {
	const traces = 64
	for _, p := range benchPlugins() {
		for _, rows := range []int{1, 4, 16, 64} {
			b.Run(fmt.Sprintf("%s/rows=%d", p.name, rows), func(b *testing.B) {
				s := mustSampler(b, p.scenario, p.schema)
				runDecide(b, s, benchBatch(b, p, traces, rows, 8, false))
			})
		}
	}
}

// BenchmarkDecide_TagMatchPosition sweeps where the matching entry sits
// in the tags array. matchAt=0 hits on the first scan iteration and
// exercises the early-exit; matchAt=mid and last force a full scan. The
// benchmark uses a SMALL rule set (just the schema's matching tag rule,
// no duration/error) so the scan dominates the cost.
func BenchmarkDecide_TagMatchPosition(b *testing.B) {
	const (
		traces  = 64
		rows    = 3
		entries = 32
	)
	for _, p := range benchPlugins() {
		cfg := fmt.Sprintf(`{"keepTagRules":[%s],"healthySampleRate":0}`, tagRuleForPlugin(p))
		for _, matchAt := range []int{0, entries / 2, entries - 1} {
			b.Run(fmt.Sprintf("%s/matchAt=%d", p.name, matchAt), func(b *testing.B) {
				s := mustSampler(b, cfg, p.schema)
				blocks := make([]sdk.TraceBlock, 0, traces)
				for t := 0; t < traces; t++ {
					blk, buildErr := benchTraceWithTagMatchAt(
						p, fmt.Sprintf("pos-trace-%d", t), rows, entries, matchAt,
					)
					if buildErr != nil {
						b.Fatalf("fixture: %v", buildErr)
					}
					blocks = append(blocks, blk)
				}
				runDecide(b, s, sdktest.Batch(blocks...))
			})
		}
	}
}

// BenchmarkDecide_RegexRule compares an equals-rule cost to a regex-rule
// cost on the same workload. The SkyWalking production config carries no
// tag rules, so this benchmark is constructed on a parallel config rather
// than reusing p.scenario. Both schemas run for symmetry: zipkin's
// production scenario already uses a regex, so the equals form is a
// synthetic comparison. The third config (regex_5xx) is a deliberately
// BROAD regex on the same key to expose the regex fast-path's
// full-match cost.
func BenchmarkDecide_RegexRule(b *testing.B) {
	const (
		traces  = 64
		rows    = 3
		entries = 8
	)
	configs := []struct {
		name string
		cfg  string
	}{
		{
			name: "equals",
			cfg:  `{"keepTagRules":[{"tagKey":"db.type","equals":"PostgreSQL"}],"healthySampleRate":0}`,
		},
		{
			name: "regex",
			cfg:  `{"keepTagRules":[{"tagKey":"db.type","regex":"^PostgreSQL$"}],"healthySampleRate":0}`,
		},
		{
			name: "regex_5xx",
			cfg:  `{"keepTagRules":[{"tagKey":"db.type","regex":"PostgreSQL"}],"healthySampleRate":0}`,
		},
	}
	for _, p := range benchPlugins() {
		for _, c := range configs {
			b.Run(p.name+"/"+c.name, func(b *testing.B) {
				s := mustSampler(b, c.cfg, p.schema)
				runDecide(b, s, benchBatch(b, p, traces, rows, entries, false))
			})
		}
	}
}

// BenchmarkDecide_KeepSliceAllocation isolates the make([]bool, n) cost in
// Decide. The sampler is configured with ONLY keepErrors — the cheapest
// rule, since the error column is one int per row and the rule short-
// circuits on the first truthy value. With duration and tag rules off,
// the per-trace work is dominated by the verdict-mask allocation, so
// this benchmark reports the lower bound on Decide's per-batch cost.
//
// The ns/op reading is the wall-clock cost of one batch; ns/trace divides
// by len(batch.Traces). B/op and allocs/op are the per-batch allocation
// profile, dominated by the keep-mask slice.
func BenchmarkDecide_KeepSliceAllocation(b *testing.B) {
	const (
		rows    = 3
		entries = 8
	)
	cfg := `{"keepErrors":true}`
	for _, p := range benchPlugins() {
		for _, n := range []int{1, 16, 64, 256} {
			b.Run(fmt.Sprintf("%s/traces=%d", p.name, n), func(b *testing.B) {
				s := mustSampler(b, cfg, p.schema)
				runDecide(b, s, benchBatch(b, p, n, rows, entries, false))
			})
		}
	}
}

// BenchmarkDecide_LazyDecodeSkip isolates the savings from the lazy tag-array
// decode on the SkyWalking schema, where keepErrors reads the dedicated
// is_error column rather than an array entry. With the realistic-mix input,
// 60% of traces match the error rule before the tag rule ever runs, so the
// eager decode would have paid its full cost for those traces for nothing.
// The lazy closure skips the decode whenever no rule that needs it is
// reached, so the duration+err+tag column drops from ~2,710 ns/trace to
// ~1,100 ns/trace on this benchmark.
//
// Zipkin is included for symmetry: its schema has ErrorTagInArray=true, so
// the error rule ALSO reads the array column and the lazy decode provides
// no per-trace saving on the error path. That contrast is the point of the
// benchmark — it tells you the optimizer's contract by the shape of the
// "no savings" case rather than relying on a comment.
func BenchmarkDecide_LazyDecodeSkip(b *testing.B) {
	const traces = 64
	for _, p := range benchPlugins() {
		cfg := fmt.Sprintf(
			`{"durationThresholdMs":%d,"keepErrors":true,"keepTagRules":[%s],"healthySampleRate":0}`,
			p.durationMs, tagRuleForPlugin(p),
		)
		b.Run(p.name+"/realisticMix", func(b *testing.B) {
			s := mustSampler(b, cfg, p.schema)
			runDecide(b, s, benchRealisticMixBatch(b, p, traces))
		})
	}
}

// BenchmarkDecide_SampleFractionFloor isolates the healthy-sample cost. The
// config carries only healthySampleRate, so no duration, error, or tag rule
// runs and the lazy tag decode never fires: every trace falls straight
// through to sampleFraction. That makes this the floor for the sample path
// and the reference for judging whether hashing is worth optimizing.
//
// It is not: on Go 1.25 the fnv.New64a hash and the []byte(traceID)
// conversion in sampleFraction both stay on the stack, so this benchmark
// reports 1 alloc/op for the whole batch — the make([]bool, traces) verdict
// mask in Decide — and none from the hash itself. Pooling the hash makes it
// strictly worse: sync.Pool forces the hash to escape, turning a 0-alloc
// call into a 1-alloc one.
func BenchmarkDecide_SampleFractionFloor(b *testing.B) {
	const traces = 64
	for _, p := range benchPlugins() {
		b.Run(p.name+"/realisticMix", func(b *testing.B) {
			s := mustSampler(b, `{"healthySampleRate":0.1}`, p.schema)
			runDecide(b, s, benchRealisticMixBatch(b, p, traces))
		})
	}
}

// TODO(call-site-labels): Layer 1.5 should add runtime
// pprof.SetGoroutineLabels inside hasSlowTrace, hasErrorColumn,
// matchEntries, sampleFraction, and the make([]bool, ...) call site in
// Decide. The labels proposed in the v2 design — hasSlowTrace,
// hasErrorColumn, arrayEntries, matchEntries, sampleFraction,
// make([]bool, ...) — let `go tool pprof` attribute per-trace cost to a
// specific function. Labels are goroutine-local, so they would touch
// production code paths; per the design's conservative rule they were
// deferred from Layer 1 to keep the engine diff minimal. Re-enable with:
//
//	import "runtime/pprof"
//	func (s *Sampler) hasSlowTrace(b *sdk.TraceBlock) (bool, error) {
//	    pprof.SetGoroutineLabels(pprof.WithLabels(context.Background(), "call_site", "hasSlowTrace"))
//	    defer pprof.SetGoroutineLabels(context.Background())
//	    // ...existing body...
//	}
//
// and verify sampler_test.go still passes (label mutations are
// goroutine-local and do not affect Verdict values).
