// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the License.
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
	"fmt"
	"sort"
	"sync"
	"time"
)

// PerformanceMetrics collects and analyzes performance data.
type PerformanceMetrics struct {
	StartTime      time.Time
	WriteLatencies []time.Duration
	QueryLatencies []time.Duration
	WriteCount     int64
	QueryCount     int64
	ErrorCount     int64
	DataSize       int64
	CompressedSize int64
	WriteDuration  time.Duration
	QueryDuration  time.Duration
	mu             sync.RWMutex
}

// NewPerformanceMetrics creates a new metrics collector.
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		WriteLatencies: make([]time.Duration, 0, 1000),
		QueryLatencies: make([]time.Duration, 0, 1000),
		StartTime:      time.Now(),
	}
}

// RecordWrite records a write operation.
func (m *PerformanceMetrics) RecordWrite(duration time.Duration, dataSize int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.WriteCount++
	m.WriteDuration += duration
	m.WriteLatencies = append(m.WriteLatencies, duration)
	m.DataSize += dataSize
}

// RecordQuery records a query operation.
func (m *PerformanceMetrics) RecordQuery(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.QueryCount++
	m.QueryDuration += duration
	m.QueryLatencies = append(m.QueryLatencies, duration)
}

// RecordError records an error.
func (m *PerformanceMetrics) RecordError() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ErrorCount++
}

// RecordCompression records compression data.
func (m *PerformanceMetrics) RecordCompression(_ int64, compressedSize int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CompressedSize += compressedSize
}

// GetWriteThroughput calculates writes per second.
func (m *PerformanceMetrics) GetWriteThroughput() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.WriteDuration == 0 {
		return 0
	}

	return float64(m.WriteCount) / m.WriteDuration.Seconds()
}

// GetQueryThroughput calculates queries per second.
func (m *PerformanceMetrics) GetQueryThroughput() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.QueryDuration == 0 {
		return 0
	}

	return float64(m.QueryCount) / m.QueryDuration.Seconds()
}

// GetDataThroughput calculates data throughput in MB/s.
func (m *PerformanceMetrics) GetDataThroughput() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.WriteDuration == 0 {
		return 0
	}

	mbPerSecond := float64(m.DataSize) / (1024 * 1024) / m.WriteDuration.Seconds()
	return mbPerSecond
}

// GetCompressionRatio calculates the compression ratio.
func (m *PerformanceMetrics) GetCompressionRatio() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.DataSize == 0 {
		return 0
	}

	return float64(m.CompressedSize) / float64(m.DataSize)
}

// GetWriteLatencyPercentiles calculates latency percentiles for writes.
func (m *PerformanceMetrics) GetWriteLatencyPercentiles() LatencyPercentiles {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return calculatePercentiles(m.WriteLatencies)
}

// GetQueryLatencyPercentiles calculates latency percentiles for queries.
func (m *PerformanceMetrics) GetQueryLatencyPercentiles() LatencyPercentiles {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return calculatePercentiles(m.QueryLatencies)
}

// GetErrorRate calculates the error rate.
func (m *PerformanceMetrics) GetErrorRate() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := m.WriteCount + m.QueryCount
	if total == 0 {
		return 0
	}

	return float64(m.ErrorCount) / float64(total)
}

// LatencyPercentiles represents latency distribution percentiles.
type LatencyPercentiles struct {
	P50  time.Duration
	P95  time.Duration
	P99  time.Duration
	P999 time.Duration
	Min  time.Duration
	Max  time.Duration
	Mean time.Duration
}

// calculatePercentiles calculates percentiles from a slice of durations.
func calculatePercentiles(durations []time.Duration) LatencyPercentiles {
	if len(durations) == 0 {
		return LatencyPercentiles{}
	}

	// Create a copy and sort
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Calculate percentiles
	p50 := sorted[int(float64(len(sorted))*0.5)]
	p95 := sorted[int(float64(len(sorted))*0.95)]
	p99 := sorted[int(float64(len(sorted))*0.99)]
	p999 := sorted[int(float64(len(sorted))*0.999)]

	// Calculate mean
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(len(sorted))

	return LatencyPercentiles{
		P50:  p50,
		P95:  p95,
		P99:  p99,
		P999: p999,
		Min:  sorted[0],
		Max:  sorted[len(sorted)-1],
		Mean: mean,
	}
}

// PerformanceReport represents a comprehensive performance report.
type PerformanceReport struct {
	TestName       string
	Scale          Scale
	Duration       time.Duration
	WriteMetrics   WriteMetrics
	QueryMetrics   QueryMetrics
	StorageMetrics StorageMetrics
	ErrorMetrics   ErrorMetrics
}

// WriteMetrics contains write performance data.
type WriteMetrics struct {
	Count          int64
	Throughput     float64 // operations per second
	DataThroughput float64 // MB per second
	LatencyP50     time.Duration
	LatencyP95     time.Duration
	LatencyP99     time.Duration
	LatencyP999    time.Duration
	LatencyMin     time.Duration
	LatencyMax     time.Duration
	LatencyMean    time.Duration
}

// QueryMetrics contains query performance data.
type QueryMetrics struct {
	Count       int64
	Throughput  float64 // operations per second
	LatencyP50  time.Duration
	LatencyP95  time.Duration
	LatencyP99  time.Duration
	LatencyP999 time.Duration
	LatencyMin  time.Duration
	LatencyMax  time.Duration
	LatencyMean time.Duration
}

// StorageMetrics contains storage efficiency data.
type StorageMetrics struct {
	OriginalSize     int64
	CompressedSize   int64
	CompressionRatio float64
}

// ErrorMetrics contains error statistics.
type ErrorMetrics struct {
	Count     int64
	ErrorRate float64
}

// GenerateReport creates a comprehensive performance report.
func (m *PerformanceMetrics) GenerateReport(testName string, scale Scale) PerformanceReport {
	m.mu.RLock()
	defer m.mu.RUnlock()

	writePercentiles := calculatePercentiles(m.WriteLatencies)
	queryPercentiles := calculatePercentiles(m.QueryLatencies)

	return PerformanceReport{
		TestName: testName,
		Scale:    scale,
		Duration: time.Since(m.StartTime),
		WriteMetrics: WriteMetrics{
			Count:          m.WriteCount,
			Throughput:     m.GetWriteThroughput(),
			DataThroughput: m.GetDataThroughput(),
			LatencyP50:     writePercentiles.P50,
			LatencyP95:     writePercentiles.P95,
			LatencyP99:     writePercentiles.P99,
			LatencyP999:    writePercentiles.P999,
			LatencyMin:     writePercentiles.Min,
			LatencyMax:     writePercentiles.Max,
			LatencyMean:    writePercentiles.Mean,
		},
		QueryMetrics: QueryMetrics{
			Count:       m.QueryCount,
			Throughput:  m.GetQueryThroughput(),
			LatencyP50:  queryPercentiles.P50,
			LatencyP95:  queryPercentiles.P95,
			LatencyP99:  queryPercentiles.P99,
			LatencyP999: queryPercentiles.P999,
			LatencyMin:  queryPercentiles.Min,
			LatencyMax:  queryPercentiles.Max,
			LatencyMean: queryPercentiles.Mean,
		},
		StorageMetrics: StorageMetrics{
			OriginalSize:     m.DataSize,
			CompressedSize:   m.CompressedSize,
			CompressionRatio: m.GetCompressionRatio(),
		},
		ErrorMetrics: ErrorMetrics{
			Count:     m.ErrorCount,
			ErrorRate: m.GetErrorRate(),
		},
	}
}

// PrintReport prints a formatted performance report.
func (r PerformanceReport) PrintReport() {
	fmt.Printf("\n=== Performance Report: %s ===\n", r.TestName)
	fmt.Printf("Scale: %s\n", r.Scale)
	fmt.Printf("Duration: %v\n", r.Duration)

	fmt.Printf("\n--- Write Performance ---\n")
	fmt.Printf("Count: %d\n", r.WriteMetrics.Count)
	fmt.Printf("Throughput: %.2f ops/sec\n", r.WriteMetrics.Throughput)
	fmt.Printf("Data Throughput: %.2f MB/sec\n", r.WriteMetrics.DataThroughput)
	fmt.Printf("Latency P50: %v\n", r.WriteMetrics.LatencyP50)
	fmt.Printf("Latency P95: %v\n", r.WriteMetrics.LatencyP95)
	fmt.Printf("Latency P99: %v\n", r.WriteMetrics.LatencyP99)
	fmt.Printf("Latency P999: %v\n", r.WriteMetrics.LatencyP999)
	fmt.Printf("Latency Min: %v\n", r.WriteMetrics.LatencyMin)
	fmt.Printf("Latency Max: %v\n", r.WriteMetrics.LatencyMax)
	fmt.Printf("Latency Mean: %v\n", r.WriteMetrics.LatencyMean)

	fmt.Printf("\n--- Query Performance ---\n")
	fmt.Printf("Count: %d\n", r.QueryMetrics.Count)
	fmt.Printf("Throughput: %.2f ops/sec\n", r.QueryMetrics.Throughput)
	fmt.Printf("Latency P50: %v\n", r.QueryMetrics.LatencyP50)
	fmt.Printf("Latency P95: %v\n", r.QueryMetrics.LatencyP95)
	fmt.Printf("Latency P99: %v\n", r.QueryMetrics.LatencyP99)
	fmt.Printf("Latency P999: %v\n", r.QueryMetrics.LatencyP999)
	fmt.Printf("Latency Min: %v\n", r.QueryMetrics.LatencyMin)
	fmt.Printf("Latency Max: %v\n", r.QueryMetrics.LatencyMax)
	fmt.Printf("Latency Mean: %v\n", r.QueryMetrics.LatencyMean)

	fmt.Printf("\n--- Storage Efficiency ---\n")
	fmt.Printf("Original Size: %d bytes\n", r.StorageMetrics.OriginalSize)
	fmt.Printf("Compressed Size: %d bytes\n", r.StorageMetrics.CompressedSize)
	fmt.Printf("Compression Ratio: %.2f%%\n", r.StorageMetrics.CompressionRatio*100)

	fmt.Printf("\n--- Error Statistics ---\n")
	fmt.Printf("Error Count: %d\n", r.ErrorMetrics.Count)
	fmt.Printf("Error Rate: %.2f%%\n", r.ErrorMetrics.ErrorRate*100)

	fmt.Printf("\n================================\n")
}
