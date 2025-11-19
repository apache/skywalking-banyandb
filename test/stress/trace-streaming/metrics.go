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

package tracestreaming

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// PerformanceMetrics tracks query performance metrics.
type PerformanceMetrics struct {
	startTime      time.Time
	queryLatencies []time.Duration
	traceCounts    []int64
	traceSizes     []int
	errorCount     int64
	totalQueries   int64
	mu             sync.RWMutex
}

// NewPerformanceMetrics creates a new performance metrics tracker.
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		queryLatencies: make([]time.Duration, 0),
		traceCounts:    make([]int64, 0),
		traceSizes:     make([]int, 0),
		startTime:      time.Now(),
	}
}

// RecordQuery records a query execution.
func (p *PerformanceMetrics) RecordQuery(latency time.Duration, traceCount int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.queryLatencies = append(p.queryLatencies, latency)
	p.traceCounts = append(p.traceCounts, traceCount)
	p.totalQueries++
}

// RecordTraceSize records the size of a trace (number of spans).
func (p *PerformanceMetrics) RecordTraceSize(spanCount int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.traceSizes = append(p.traceSizes, spanCount)
}

// RecordError records a query error.
func (p *PerformanceMetrics) RecordError() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCount++
}

// MetricsSummary provides a summary of metrics.
type MetricsSummary struct {
	TotalQueries      int64
	ErrorCount        int64
	SuccessRate       float64
	TotalTraces       int64
	AvgTracesPerQuery float64

	// Latency metrics
	LatencyP50 time.Duration
	LatencyP95 time.Duration
	LatencyP99 time.Duration
	LatencyMin time.Duration
	LatencyMax time.Duration
	LatencyAvg time.Duration

	// Trace size metrics
	AvgTraceSize float64
	MinTraceSize int
	MaxTraceSize int

	// Throughput
	QueriesPerSec float64
	Duration      time.Duration
}

// GetSummary returns a summary of the metrics.
func (p *PerformanceMetrics) GetSummary() MetricsSummary {
	p.mu.RLock()
	defer p.mu.RUnlock()

	summary := MetricsSummary{
		TotalQueries: p.totalQueries,
		ErrorCount:   p.errorCount,
		Duration:     time.Since(p.startTime),
	}

	if p.totalQueries > 0 {
		summary.SuccessRate = float64(p.totalQueries-p.errorCount) / float64(p.totalQueries) * 100
	}

	// Calculate trace counts
	var totalTraces int64
	for _, count := range p.traceCounts {
		totalTraces += count
	}
	summary.TotalTraces = totalTraces

	if len(p.traceCounts) > 0 {
		summary.AvgTracesPerQuery = float64(totalTraces) / float64(len(p.traceCounts))
	}

	// Calculate latency percentiles
	if len(p.queryLatencies) > 0 {
		sortedLatencies := make([]time.Duration, len(p.queryLatencies))
		copy(sortedLatencies, p.queryLatencies)
		sort.Slice(sortedLatencies, func(i, j int) bool {
			return sortedLatencies[i] < sortedLatencies[j]
		})

		summary.LatencyMin = sortedLatencies[0]
		summary.LatencyMax = sortedLatencies[len(sortedLatencies)-1]
		summary.LatencyP50 = sortedLatencies[len(sortedLatencies)*50/100]
		summary.LatencyP95 = sortedLatencies[len(sortedLatencies)*95/100]
		summary.LatencyP99 = sortedLatencies[len(sortedLatencies)*99/100]

		var totalLatency time.Duration
		for _, latency := range sortedLatencies {
			totalLatency += latency
		}
		summary.LatencyAvg = totalLatency / time.Duration(len(sortedLatencies))
	}

	// Calculate trace size metrics
	if len(p.traceSizes) > 0 {
		summary.MinTraceSize = p.traceSizes[0]
		summary.MaxTraceSize = p.traceSizes[0]
		var totalSize int
		for _, size := range p.traceSizes {
			totalSize += size
			if size < summary.MinTraceSize {
				summary.MinTraceSize = size
			}
			if size > summary.MaxTraceSize {
				summary.MaxTraceSize = size
			}
		}
		summary.AvgTraceSize = float64(totalSize) / float64(len(p.traceSizes))
	}

	// Calculate throughput
	if summary.Duration > 0 {
		summary.QueriesPerSec = float64(p.totalQueries) / summary.Duration.Seconds()
	}

	return summary
}

// PrintSummary prints a formatted summary.
func (s MetricsSummary) PrintSummary(name string) {
	fmt.Printf("\n=== %s Performance Summary ===\n", name)
	fmt.Printf("Duration: %v\n", s.Duration)
	fmt.Printf("Total Queries: %d\n", s.TotalQueries)
	fmt.Printf("Error Count: %d\n", s.ErrorCount)
	fmt.Printf("Success Rate: %.2f%%\n", s.SuccessRate)
	fmt.Printf("Total Traces Retrieved: %d\n", s.TotalTraces)
	fmt.Printf("Avg Traces Per Query: %.2f\n", s.AvgTracesPerQuery)

	fmt.Printf("\nLatency Metrics:\n")
	fmt.Printf("  Min: %v\n", s.LatencyMin)
	fmt.Printf("  Avg: %v\n", s.LatencyAvg)
	fmt.Printf("  P50: %v\n", s.LatencyP50)
	fmt.Printf("  P95: %v\n", s.LatencyP95)
	fmt.Printf("  P99: %v\n", s.LatencyP99)
	fmt.Printf("  Max: %v\n", s.LatencyMax)

	fmt.Printf("\nTrace Size Metrics:\n")
	fmt.Printf("  Min Spans: %d\n", s.MinTraceSize)
	fmt.Printf("  Avg Spans: %.2f\n", s.AvgTraceSize)
	fmt.Printf("  Max Spans: %d\n", s.MaxTraceSize)

	fmt.Printf("\nThroughput:\n")
	fmt.Printf("  Queries/sec: %.2f\n", s.QueriesPerSec)
	fmt.Printf("========================================\n\n")
}

// MaxTraceSizeComparison compares metrics for different MaxTraceSize values.
type MaxTraceSizeComparison struct {
	MaxTraceSize uint32
	Metrics      MetricsSummary
}

// PrintComparison prints a comparison of different MaxTraceSize configurations.
func PrintComparison(comparisons []MaxTraceSizeComparison) {
	fmt.Printf("\n=== MaxTraceSize Comparison ===\n")
	fmt.Printf("%-15s %-15s %-15s %-15s %-15s %-15s\n",
		"MaxTraceSize", "Avg Latency", "P95 Latency", "Traces/Query", "Queries/sec", "Success Rate")
	fmt.Printf("%s\n", string(make([]byte, 100)))

	for _, comp := range comparisons {
		fmt.Printf("%-15d %-15v %-15v %-15.2f %-15.2f %-15.2f%%\n",
			comp.MaxTraceSize,
			comp.Metrics.LatencyAvg,
			comp.Metrics.LatencyP95,
			comp.Metrics.AvgTracesPerQuery,
			comp.Metrics.QueriesPerSec,
			comp.Metrics.SuccessRate)
	}
	fmt.Printf("========================================\n\n")
}
