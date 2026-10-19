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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

// latency-probe drives a stream catalog on a fixed tick for a bounded window
// and reports per-query percentiles.
//
// The soak harness's existing authority is parity (result equality against a
// baseline) and survival (heap/goroutine/RSS). It measures NO latency and NO
// CPU utilization — grepping test/soak/cmd for p99 returns nothing, and the
// RSS sampler at scripts/soak-vectorized.sh:508 samples only memory. This probe
// closes the latency half.
//
// Row counts are recorded alongside latency on purpose. A p50 that looks
// excellent because the time pruner dropped 99% of the candidates is a false
// win, not a fast query; the row count is the cheapest end-to-end evidence
// that pruning did not over-prune. Both must be stable.
const (
	probeDefaultMinCalls = 10
	probeDefaultWorkers  = 8
	probeDefaultDuration = 60 * time.Second
	probeDefaultTimeout  = 30 * time.Second
	probeDefaultPause    = 0
)

// latencySample is one probe window's summary for a single catalog query.
// Field order is deliberate (fieldalignment): the three strings, then the
// 64-bit floats, then the 64-bit counters, so no padding lands between them.
type latencySample struct {
	QueryID       string  `json:"query_id"`
	At            string  `json:"at"`
	FirstErr      string  `json:"first_err,omitempty"`
	MeanMs        float64 `json:"mean_ms"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
	MaxMs         float64 `json:"max_ms"`
	Calls         int64   `json:"calls"`
	Errors        int64   `json:"errors"`
	Rows          int64   `json:"rows"`
	RowsMin       int64   `json:"rows_min"`
	RowsMax       int64   `json:"rows_max"`
	LowConfidence bool    `json:"low_confidence,omitempty"`
}

// latencyReport is the artifact latency-probe writes.
type latencyReport struct {
	StartedAt string          `json:"started_at"`
	EndedAt   string          `json:"ended_at"`
	Window    string          `json:"window"`
	Catalog   string          `json:"catalog"`
	Samples   []latencySample `json:"samples"`
	UntilMs   int64           `json:"until_ms"`
	Workers   int             `json:"workers"`
	Pass      bool            `json:"pass"`
}

// probeAccum collects latencies and row counts for one query id across workers.
// Field order follows fieldalignment (pointer-bearing fields first, then the
// pointer-free mutex and counter). The value is always used through a pointer,
// so moving the mutex does not introduce a copy-lock hazard.
type probeAccum struct {
	firstErr string
	latency  []float64
	rows     []int
	mu       sync.Mutex
	errs     int64
}

func (p *probeAccum) record(latencyMs float64, rows int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latency = append(p.latency, latencyMs)
	p.rows = append(p.rows, rows)
}

func (p *probeAccum) recordErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errs++
	if p.firstErr == "" {
		p.firstErr = err.Error()
	}
}

func (p *probeAccum) snapshot() ([]float64, []int, int64, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]float64(nil), p.latency...), append([]int(nil), p.rows...), p.errs, p.firstErr
}

// percentile returns the p-th percentile (0..100) of an already-sorted slice
// using the nearest-rank definition: the smallest value at or above which at
// least p% of the observations fall, i.e. sorted[ceil(p/100*n) - 1].
//
// This is NOT floor((n-1)*p/100), which selects the (n-1)-th order statistic and
// silently drops the slowest observation — with n=8 and p99 that reports the
// 7th value rather than the 8th, understating the tail exactly where a soak
// cares most. Returns 0 for an empty slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// emitProbeWindow runs only after all workers have joined. Each query retains
// its accumulator until it reaches minCalls. A final flush preserves sparse
// tails and errors, marking them unsuitable for percentile comparisons.
func emitProbeWindow(report *latencyReport, accums map[string]*probeAccum, at time.Time, minCalls int, final bool) {
	ids := make([]string, 0, len(accums))
	for id := range accums {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		latencies, rows, errCount, firstErr := accums[id].snapshot()
		if errCount > 0 {
			report.Pass = false
		}
		if len(latencies) == 0 && errCount == 0 {
			continue
		}
		if len(latencies) < minCalls && !final {
			continue
		}
		sample := summarize(id, at, latencies, rows, errCount, firstErr)
		sample.LowConfidence = len(latencies) < minCalls
		report.Samples = append(report.Samples, sample)
		fmt.Printf("[latency-probe] %s calls=%d err=%d p50=%.1fms p99=%.1fms max=%.1fms rows=%d low_confidence=%v\n",
			id, sample.Calls, sample.Errors, sample.P50Ms, sample.P99Ms, sample.MaxMs, sample.Rows, sample.LowConfidence)
		accums[id] = &probeAccum{}
	}
}

// summarize builds one latencySample from an accumulator's snapshot.
func summarize(id string, at time.Time, latencies []float64, rows []int, errCount int64, firstErr string) latencySample {
	sample := latencySample{QueryID: id, At: at.UTC().Format(time.RFC3339), Calls: int64(len(latencies)), Errors: errCount, FirstErr: firstErr}
	if len(rows) > 0 {
		rowsMin, rowsMax := rows[0], rows[0]
		rowTotal := 0
		for _, r := range rows {
			if r < rowsMin {
				rowsMin = r
			}
			if r > rowsMax {
				rowsMax = r
			}
			rowTotal += r
		}
		sample.Rows = int64(rowTotal / len(rows))
		sample.RowsMin = int64(rowsMin)
		sample.RowsMax = int64(rowsMax)
	}
	if len(latencies) == 0 {
		return sample
	}
	sorted := append([]float64(nil), latencies...)
	sort.Float64s(sorted)
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	sample.MeanMs = sum / float64(len(sorted))
	sample.P50Ms = percentile(sorted, 50)
	sample.P95Ms = percentile(sorted, 95)
	sample.P99Ms = percentile(sorted, 99)
	sample.MaxMs = sorted[len(sorted)-1]
	return sample
}

// streamIdxProbeOnce runs every catalog entry once and records the outcome.
// Returns the number of entries that errored.
func streamIdxProbeOnce(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	entries []streamCatalogEntry,
	untilMs int64,
	window time.Duration,
	accums map[string]*probeAccum,
) int {
	failed := 0
	for _, entry := range entries {
		accum := accums[entry.ID]
		req := buildStreamQueryRequest(entry, untilMs, window)
		qctx, cancel := context.WithTimeout(ctx, probeDefaultTimeout)
		start := time.Now()
		resp, queryErr := client.Query(qctx, req)
		elapsed := time.Since(start)
		cancel()
		if queryErr != nil {
			accum.recordErr(queryErr)
			failed++
			continue
		}
		accum.record(float64(elapsed.Nanoseconds())/1e6, len(resp.GetElements()))
	}
	return failed
}

// streamIdxLatencyProbe runs the catalog concurrently for the whole duration,
// sampling on a fixed interval so the report is a timeline rather than one
// aggregate. Returns a report ready to serialize.
func streamIdxLatencyProbe(
	ctx context.Context,
	conn *grpc.ClientConn,
	catalogPath string,
	untilMs int64,
	window time.Duration,
	workers, minCalls int,
	interval, duration time.Duration,
) latencyReport {
	entries, loadErr := loadStreamCatalog(catalogPath)
	if loadErr != nil {
		// A broken catalog is a setup failure, not a soak result: surface it as a
		// report with no samples so the caller still writes an artifact.
		now := time.Now().UTC().Format(time.RFC3339)
		return latencyReport{
			Pass:      false,
			Catalog:   catalogPath,
			StartedAt: now,
			EndedAt:   now,
			Window:    window.String(),
			UntilMs:   untilMs,
			Workers:   workers,
		}
	}
	client := streamv1.NewStreamServiceClient(conn)
	report := latencyReport{
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		Window:    window.String(),
		Workers:   workers,
		Catalog:   catalogPath,
		UntilMs:   untilMs,
		Pass:      true,
	}

	deadline := time.Now().Add(duration)
	sampleAt := time.Now()
	// newAccums builds the per-window accumulator map on ONE goroutine. The
	// workers below all read it concurrently, so creating entries inside them
	// would be an unsynchronized map write.
	newAccums := func() map[string]*probeAccum {
		accums := make(map[string]*probeAccum, len(entries))
		for _, entry := range entries {
			accums[entry.ID] = &probeAccum{}
		}
		return accums
	}
	// One accumulator set per REPORTING window, not per round. Holding it
	// across rounds is what makes the percentiles describe the whole window;
	// reallocating each round silently discarded every round that finished
	// before the window closed, errors included.
	accums := newAccums()
	// Collect one sample per interval across all workers. The deadline test is
	// the loop condition: the final flush below runs after the loop regardless,
	// so the last partial window is still emitted.
	for !time.Now().After(deadline) {
		var wg sync.WaitGroup
		// A single round's worth of concurrent load, so the percentiles describe
		// concurrent execution rather than a serialized approximation of it.
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				streamIdxProbeOnce(ctx, client, entries, untilMs, window, accums)
			}()
		}
		wg.Wait()

		if time.Now().After(sampleAt) || time.Now().After(deadline) {
			emitProbeWindow(&report, accums, time.Now(), minCalls, false)
			sampleAt = time.Now().Add(interval)
		}
	}
	// Preserve every observation, including a final window below the threshold.
	emitProbeWindow(&report, accums, time.Now(), minCalls, true)
	report.EndedAt = time.Now().UTC().Format(time.RFC3339)
	if len(report.Samples) == 0 {
		report.Pass = false
	}
	return report
}

// newLatencyProbeCmd returns the latency-probe subcommand.
func newLatencyProbeCmd() *cobra.Command {
	var addr, catalogPath, outPath string
	var untilMs int64
	var workers, minCalls int
	var intervalMin, durationMin float64
	var windowHours float64

	cmd := &cobra.Command{
		Use:   "latency-probe",
		Short: "Drive a stream catalog on a fixed tick and report per-query percentiles and row counts",
		RunE: func(_ *cobra.Command, _ []string) error {
			if minCalls < 1 {
				return fmt.Errorf("min-calls must be positive: %d", minCalls)
			}
			conn, dialErr := dialInsecure(addr)
			if dialErr != nil {
				return dialErr
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationMin*60)*time.Second+time.Minute)
			defer cancel()

			report := streamIdxLatencyProbe(ctx, conn, catalogPath, untilMs,
				time.Duration(windowHours*float64(time.Hour)), workers, minCalls,
				time.Duration(intervalMin*60)*time.Second,
				time.Duration(durationMin*60)*time.Second)

			if mkdirErr := os.MkdirAll(dirOf(outPath), 0o755); mkdirErr != nil {
				return fmt.Errorf("create output dir for %s: %w", outPath, mkdirErr)
			}
			out, createErr := os.Create(outPath)
			if createErr != nil {
				return fmt.Errorf("create output %s: %w", outPath, createErr)
			}
			defer out.Close()
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if encErr := enc.Encode(report); encErr != nil {
				return fmt.Errorf("encode latency report: %w", encErr)
			}
			fmt.Printf("[latency-probe] wrote %s (%d samples, pass=%v)\n", outPath, len(report.Samples), report.Pass)
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "localhost:17912", "BanyanDB gRPC address")
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Path to stream query catalog JSON")
	cmd.Flags().StringVar(&outPath, "out", "latency.json", "Output path for the latency report JSON")
	cmd.Flags().Int64Var(&untilMs, "until", 0, "Upper bound for time range (unix milliseconds)")
	cmd.Flags().IntVar(&minCalls, "min-calls", probeDefaultMinCalls, "Minimum successful calls per query before emitting a window")
	cmd.Flags().IntVar(&workers, "workers", probeDefaultWorkers, "Concurrent query workers")
	cmd.Flags().Float64Var(&intervalMin, "interval-min", 1, "Sampling interval in minutes")
	cmd.Flags().Float64Var(&durationMin, "duration-min", 1, "Total probe duration in minutes")
	cmd.Flags().Float64Var(&windowHours, "window-hours", 7*24, "Query lookback window in hours (parityWindow)")
	_ = cmd.MarkFlagRequired("catalog")
	_ = cmd.MarkFlagRequired("until")
	return cmd
}

// dirOf returns the parent directory of path (or "." for a bare filename).
func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}
