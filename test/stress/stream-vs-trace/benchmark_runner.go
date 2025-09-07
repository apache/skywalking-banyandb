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

package streamvstrace

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BenchmarkConfig represents configuration for running benchmarks.
type BenchmarkConfig struct {
	Scale         Scale
	Concurrency   int
	BatchSize     int
	TestDuration  time.Duration
	EnableQueries bool
	QueryInterval time.Duration
}

// DefaultBenchmarkConfig returns a default benchmark configuration.
func DefaultBenchmarkConfig(scale Scale) BenchmarkConfig {
	return BenchmarkConfig{
		Scale:         scale,
		Concurrency:   10,
		BatchSize:     100,
		TestDuration:  5 * time.Minute,
		EnableQueries: true,
		QueryInterval: 30 * time.Second,
	}
}

// BenchmarkRunner coordinates the execution of performance benchmarks.
type BenchmarkRunner struct {
	streamClient  *StreamClient
	traceClient   *TraceClient
	streamMetrics *PerformanceMetrics
	traceMetrics  *PerformanceMetrics
	generator     *SpanGenerator
	config        BenchmarkConfig
}

// NewBenchmarkRunner creates a new benchmark runner.
func NewBenchmarkRunner(config BenchmarkConfig, streamClient *StreamClient, traceClient *TraceClient) *BenchmarkRunner {
	return &BenchmarkRunner{
		config:        config,
		streamClient:  streamClient,
		traceClient:   traceClient,
		streamMetrics: NewPerformanceMetrics(),
		traceMetrics:  NewPerformanceMetrics(),
		generator:     NewSpanGenerator(config.Scale),
	}
}

// RunWriteBenchmark runs write performance benchmarks for both models.
func (r *BenchmarkRunner) RunWriteBenchmark(ctx context.Context) error {
	fmt.Printf("Starting write benchmark for scale: %s\n", r.config.Scale)
	fmt.Printf("Concurrency: %d, Batch Size: %d, Duration: %v\n",
		r.config.Concurrency, r.config.BatchSize, r.config.TestDuration)

	// Create context with timeout
	benchCtx, cancel := context.WithTimeout(ctx, r.config.TestDuration)
	defer cancel()

	// Start both benchmarks concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// Stream write benchmark
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.runStreamWriteBenchmark(benchCtx); err != nil {
			errChan <- fmt.Errorf("stream write benchmark failed: %w", err)
		}
	}()

	// Trace write benchmark
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.runTraceWriteBenchmark(benchCtx); err != nil {
			errChan <- fmt.Errorf("trace write benchmark failed: %w", err)
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// Generate reports
	fmt.Println("\n=== Stream Model Write Performance ===")
	streamReport := r.streamMetrics.GenerateReport("Stream Write", r.config.Scale)
	streamReport.PrintReport()

	fmt.Println("\n=== Trace Model Write Performance ===")
	traceReport := r.traceMetrics.GenerateReport("Trace Write", r.config.Scale)
	traceReport.PrintReport()

	return nil
}

// runStreamWriteBenchmark runs the stream write benchmark.
//
//nolint:unparam
func (r *BenchmarkRunner) runStreamWriteBenchmark(ctx context.Context) error {
	// Generate test data
	traces := r.generator.GenerateTraces()
	allSpans := make([]*SpanData, 0)
	for _, trace := range traces {
		allSpans = append(allSpans, trace.Spans...)
	}

	fmt.Printf("Generated %d traces with %d total spans for stream benchmark\n",
		len(traces), len(allSpans))

	// Process spans in batches with concurrency
	spanChan := make(chan []*SpanData, r.config.Concurrency*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.streamWriteWorker(ctx, spanChan)
		}()
	}

	// Send batches to workers
	go func() {
		defer close(spanChan)
		for i := 0; i < len(allSpans); i += r.config.BatchSize {
			end := i + r.config.BatchSize
			if end > len(allSpans) {
				end = len(allSpans)
			}

			select {
			case spanChan <- allSpans[i:end]:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for workers to complete
	wg.Wait()

	return nil
}

// runTraceWriteBenchmark runs the trace write benchmark.
//
//nolint:unparam
func (r *BenchmarkRunner) runTraceWriteBenchmark(ctx context.Context) error {
	// Generate test data (same as stream for fair comparison)
	traces := r.generator.GenerateTraces()
	allSpans := make([]*SpanData, 0)
	for _, trace := range traces {
		allSpans = append(allSpans, trace.Spans...)
	}

	fmt.Printf("Generated %d traces with %d total spans for trace benchmark\n",
		len(traces), len(allSpans))

	// Process spans in batches with concurrency
	spanChan := make(chan []*SpanData, r.config.Concurrency*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < r.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.traceWriteWorker(ctx, spanChan)
		}()
	}

	// Send batches to workers
	go func() {
		defer close(spanChan)
		for i := 0; i < len(allSpans); i += r.config.BatchSize {
			end := i + r.config.BatchSize
			if end > len(allSpans) {
				end = len(allSpans)
			}

			select {
			case spanChan <- allSpans[i:end]:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for workers to complete
	wg.Wait()

	return nil
}

// streamWriteWorker processes batches of spans for stream writes.
func (r *BenchmarkRunner) streamWriteWorker(ctx context.Context, spanChan <-chan []*SpanData) {
	for {
		select {
		case spans, ok := <-spanChan:
			if !ok {
				return
			}

			start := time.Now()
			err := r.streamClient.WriteStreamData(ctx, spans)
			duration := time.Since(start)

			// Calculate data size
			var dataSize int64
			for _, span := range spans {
				dataSize += int64(len(span.DataBinary))
			}

			if err != nil {
				r.streamMetrics.RecordError()
				fmt.Printf("Stream write error: %v\n", err)
			} else {
				r.streamMetrics.RecordWrite(duration, dataSize)
			}

		case <-ctx.Done():
			return
		}
	}
}

// traceWriteWorker processes batches of spans for trace writes.
func (r *BenchmarkRunner) traceWriteWorker(ctx context.Context, spanChan <-chan []*SpanData) {
	for {
		select {
		case spans, ok := <-spanChan:
			if !ok {
				return
			}

			start := time.Now()
			err := r.traceClient.WriteTraceData(ctx, spans)
			duration := time.Since(start)

			// Calculate data size
			var dataSize int64
			for _, span := range spans {
				dataSize += int64(len(span.DataBinary))
			}

			if err != nil {
				r.traceMetrics.RecordError()
				fmt.Printf("Trace write error: %v\n", err)
			} else {
				r.traceMetrics.RecordWrite(duration, dataSize)
			}

		case <-ctx.Done():
			return
		}
	}
}

// RunQueryBenchmark runs query performance benchmarks.
func (r *BenchmarkRunner) RunQueryBenchmark(ctx context.Context) error {
	if !r.config.EnableQueries {
		fmt.Println("Query benchmarks disabled")
		return nil
	}

	fmt.Printf("Starting query benchmark for scale: %s\n", r.config.Scale)

	// Run query benchmarks for both models
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// Stream query benchmark
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.runStreamQueryBenchmark(ctx); err != nil {
			errChan <- fmt.Errorf("stream query benchmark failed: %w", err)
		}
	}()

	// Trace query benchmark
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.runTraceQueryBenchmark(ctx); err != nil {
			errChan <- fmt.Errorf("trace query benchmark failed: %w", err)
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// Generate reports
	fmt.Println("\n=== Stream Model Query Performance ===")
	streamReport := r.streamMetrics.GenerateReport("Stream Query", r.config.Scale)
	streamReport.PrintReport()

	fmt.Println("\n=== Trace Model Query Performance ===")
	traceReport := r.traceMetrics.GenerateReport("Trace Query", r.config.Scale)
	traceReport.PrintReport()

	return nil
}

// runStreamQueryBenchmark runs stream query benchmarks.
func (r *BenchmarkRunner) runStreamQueryBenchmark(ctx context.Context) error {
	// TODO: Implement stream query benchmarks
	// This would include various query patterns like:
	// - Time range queries
	// - Service-based queries
	// - Error queries
	// - Latency-based queries
	fmt.Println("Stream query benchmark not yet implemented")
	return nil
}

// runTraceQueryBenchmark runs trace query benchmarks.
func (r *BenchmarkRunner) runTraceQueryBenchmark(ctx context.Context) error {
	// TODO: Implement trace query benchmarks
	// This would include various query patterns like:
	// - Trace reconstruction queries
	// - Time range queries
	// - Service-based queries
	// - Error queries
	fmt.Println("Trace query benchmark not yet implemented")
	return nil
}

// RunFullBenchmark runs both write and query benchmarks.
func (r *BenchmarkRunner) RunFullBenchmark(ctx context.Context) error {
	fmt.Println("Starting full benchmark suite...")

	// Run write benchmarks
	if err := r.RunWriteBenchmark(ctx); err != nil {
		return fmt.Errorf("write benchmark failed: %w", err)
	}

	// Wait a bit between benchmarks
	time.Sleep(5 * time.Second)

	// Run query benchmarks
	if err := r.RunQueryBenchmark(ctx); err != nil {
		return fmt.Errorf("query benchmark failed: %w", err)
	}

	fmt.Println("Full benchmark suite completed successfully!")
	return nil
}

// CompareResults compares performance results between models.
func (r *BenchmarkRunner) CompareResults() {
	streamReport := r.streamMetrics.GenerateReport("Stream", r.config.Scale)
	traceReport := r.traceMetrics.GenerateReport("Trace", r.config.Scale)

	fmt.Println("\n=== Performance Comparison ===")
	fmt.Printf("Write Throughput Comparison:\n")
	fmt.Printf("  Stream: %.2f ops/sec\n", streamReport.WriteMetrics.Throughput)
	fmt.Printf("  Trace:  %.2f ops/sec\n", traceReport.WriteMetrics.Throughput)
	fmt.Printf("  Winner: %s (%.1fx faster)\n",
		r.getWinner(streamReport.WriteMetrics.Throughput, traceReport.WriteMetrics.Throughput),
		r.getSpeedup(streamReport.WriteMetrics.Throughput, traceReport.WriteMetrics.Throughput))

	fmt.Printf("\nWrite Latency Comparison (P95):\n")
	fmt.Printf("  Stream: %v\n", streamReport.WriteMetrics.LatencyP95)
	fmt.Printf("  Trace:  %v\n", traceReport.WriteMetrics.LatencyP95)
	fmt.Printf("  Winner: %s (%.1fx faster)\n",
		r.getLatencyWinner(streamReport.WriteMetrics.LatencyP95, traceReport.WriteMetrics.LatencyP95),
		r.getLatencySpeedup(streamReport.WriteMetrics.LatencyP95, traceReport.WriteMetrics.LatencyP95))

	fmt.Printf("\nData Throughput Comparison:\n")
	fmt.Printf("  Stream: %.2f MB/sec\n", streamReport.WriteMetrics.DataThroughput)
	fmt.Printf("  Trace:  %.2f MB/sec\n", traceReport.WriteMetrics.DataThroughput)
	fmt.Printf("  Winner: %s (%.1fx faster)\n",
		r.getWinner(streamReport.WriteMetrics.DataThroughput, traceReport.WriteMetrics.DataThroughput),
		r.getSpeedup(streamReport.WriteMetrics.DataThroughput, traceReport.WriteMetrics.DataThroughput))
}

// Helper methods for comparison.
func (r *BenchmarkRunner) getWinner(stream, trace float64) string {
	if stream > trace {
		return "Stream"
	}
	return "Trace"
}

func (r *BenchmarkRunner) getSpeedup(stream, trace float64) float64 {
	if stream > trace {
		return stream / trace
	}
	return trace / stream
}

func (r *BenchmarkRunner) getLatencyWinner(stream, trace time.Duration) string {
	if stream < trace {
		return "Stream"
	}
	return "Trace"
}

func (r *BenchmarkRunner) getLatencySpeedup(stream, trace time.Duration) float64 {
	if stream < trace {
		return float64(trace) / float64(stream)
	}
	return float64(stream) / float64(trace)
}
