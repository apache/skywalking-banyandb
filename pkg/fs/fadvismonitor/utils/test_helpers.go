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
//go:build linux
// +build linux

// Package utils provides helper functions for file operations and monitoring in benchmarks and tests.
package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/bpf"
	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/monitor"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Constants for file sizes and thresholds.
const (
	Megabyte      = 1024 * 1024
	Terabyte      = 1024 * 1024 * 1024 * 1024
	LargeFileSize = 200 * Megabyte
)

var fileSystem fs.FileSystem

func init() {
	fileSystem = fs.NewLocalFileSystemWithLogger(logger.GetLogger("fadvis-benchmark"))
}

var sharedReadBuffer = make([]byte, 32*1024)

// CreateTestFile creates a test file with the given size.
func CreateTestFile(tb testing.TB, filePath string, size int64) error {
	tb.Helper()
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		tb.Fatalf("Failed to create directories: %v", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		tb.Fatalf("Failed to create file: %v", err)
	}
	defer f.Close()

	if err := os.Truncate(filePath, size); err != nil {
		tb.Fatalf("Failed to truncate file: %v", err)
	}
	return nil
}

// ReadFileWithFadvise opens a file and reads its contents using the fileSystem interface,
// which applies fadvise hints automatically based on configuration.
func ReadFileWithFadvise(_ testing.TB, filePath string) ([]byte, error) {
	f, err := fileSystem.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = f.Size()
	if err != nil {
		return nil, err
	}

	n, err := f.Read(0, sharedReadBuffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return sharedReadBuffer[:n], nil
}

// ReadFileStreamingWithFadvise reads a file using streaming mode with optional fadvise hints.
// Returns the total number of bytes read.
func ReadFileStreamingWithFadvise(_ testing.TB, filePath string, skipFadvise bool) (int64, error) {
	f, err := fileSystem.OpenFile(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Use cached parameter (true to skip fadvise, false to apply it)
	seqReader := f.SequentialRead(skipFadvise)

	totalBytes := int64(0)
	for {
		n, err := seqReader.Read(sharedReadBuffer)
		if n > 0 {
			totalBytes += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return totalBytes, err
		}
	}

	return totalBytes, nil
}

// AppendToFile appends the given data to a file, creating it if it doesn't exist.
func AppendToFile(filePath string, data []byte) error {
	_, err := os.Stat(filePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open file for append: %w", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// SetupTestEnvironment creates a temporary directory for testing and returns a cleanup function.
func SetupTestEnvironment(t testing.TB) (string, func()) {
	tempDir := t.TempDir()
	return tempDir, func() {}
}

// CreateTestParts creates multiple test files with the specified size and returns their paths.
func CreateTestParts(t testing.TB, testDir string, numParts int, partSize int64) []string {
	parts := make([]string, numParts)
	for i := 0; i < numParts; i++ {
		partPath := filepath.Join(testDir, fmt.Sprintf("part_%d", i))
		err := CreateTestFile(t, partPath, partSize)
		require.NoError(t, err)
		parts[i] = partPath
	}
	return parts
}

// SimulateMergeOperation simulates a merge operation by reading from multiple input files
// and writing to a single output file, applying fadvise hints along the way.
func SimulateMergeOperation(_ testing.TB, parts []string, outputFile string) error {
	outFile, err := fileSystem.CreateFile(outputFile, 0o644)
	if err != nil {
		return err
	}
	defer outFile.Close()

	seqWriter := outFile.SequentialWrite()
	defer seqWriter.Close()

	buffer := make([]byte, 8192)
	for _, part := range parts {
		inFile, err := fileSystem.OpenFile(part)
		if err != nil {
			return err
		}

		// Use explicit cached=false parameter to apply fadvise
		seqReader := inFile.SequentialRead(false)

		for {
			n, err := seqReader.Read(buffer)
			if n == 0 || err != nil {
				if !errors.Is(err, io.EOF) {
					return err
				}
				break
			}

			_, err = seqWriter.Write(buffer[:n])
			if err != nil {
				return err
			}
		}

		seqReader.Close()
		inFile.Close()
	}

	return nil
}

// SetTestThreshold sets a specific threshold value for determining when to apply fadvise.
func SetTestThreshold(threshold int64) {
	provider := &testThresholdProvider{threshold: threshold}
	fs.SetThresholdProvider(provider)
	fs.SetGlobalThreshold(threshold)
}

// SetRealisticThreshold calculates and sets a realistic threshold based on system memory.
func SetRealisticThreshold() {
	threshold := CalculateRealisticThreshold()
	SetTestThreshold(threshold)
}

// CalculateRealisticThreshold calculates a suitable threshold based on the available memory
// and page cache configuration, defaulting to a safe value if it cannot be determined.
func CalculateRealisticThreshold() int64 {
	pageCachePercent := 25

	totalMemory, err := cgroups.MemoryLimit()
	if err != nil {
		return 64 * 1024 * 1024
	}

	pageCacheSize := totalMemory * int64(pageCachePercent) / 100

	threshold := pageCacheSize / 100

	if threshold < 1024*1024 {
		threshold = 1024 * 1024
	}

	return threshold
}

type testThresholdProvider struct {
	threshold int64
}

func (p *testThresholdProvider) GetThreshold() int64 {
	return p.threshold
}

func (p *testThresholdProvider) ShouldApplyFadvis(fileSize int64, maxSize int64) bool {
	// Use the smaller of threshold and maxSize
	threshold := p.threshold
	if maxSize < threshold {
		threshold = maxSize
	}
	return fileSize >= threshold
}

// PageCacheStats contains statistics about page cache usage.
type PageCacheStats struct {
	Rss         int64
	Pss         int64
	SharedClean int64
}

func parseSmapsRollup(r io.Reader) (PageCacheStats, error) {
	var stats PageCacheStats
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		key := strings.TrimSuffix(parts[0], ":")
		value := parts[1]
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "Rss":
			stats.Rss = n
		case "Pss":
			stats.Pss = n
		case "Shared_Clean":
			stats.SharedClean = n
		}
	}
	if err := scanner.Err(); err != nil {
		return stats, err
	}
	return stats, nil
}

// CapturePageCacheStats captures current page cache statistics for the running process.
func CapturePageCacheStats(b *testing.B, phase string) (PageCacheStats, int64) {
	f, err := os.Open("/proc/self/smaps_rollup")
	if err != nil {
		b.Logf("[PAGECACHE] %s: open smaps_rollup failed: %v", phase, err)
		return PageCacheStats{}, 0
	}
	stats, err := parseSmapsRollup(f)
	f.Close()
	if err != nil {
		b.Logf("[PAGECACHE] %s: parse smaps_rollup failed: %v", phase, err)
		return PageCacheStats{}, 0
	}

	memf, err := os.Open("/proc/meminfo")
	if err != nil {
		b.Logf("[MEMINFO] %s: open meminfo failed: %v", phase, err)
		return PageCacheStats{}, 0
	}
	var cachedKB int64
	buf := make([]byte, 32*1024)
	builder := strings.Builder{}
	for {
		n, err := memf.Read(buf)
		if n > 0 {
			builder.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Logf("[MEMINFO] %s: read meminfo failed: %v", phase, err)
			break
		}
	}
	memf.Close()

	for _, line := range strings.Split(builder.String(), "\n") {
		if strings.HasPrefix(line, "Cached:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					cachedKB = v
				}
			}
			break
		}
	}

	b.ReportMetric(float64(cachedKB), "cached_kb")

	b.Logf("[PAGECACHE] %s: Rss=%dKB, Pss=%dKB, SharedClean=%dKB, Cached=%dKB",
		phase, stats.Rss, stats.Pss, stats.SharedClean, cachedKB)

	return stats, cachedKB
}

// CapturePageCacheStatsWithDelay waits for the specified number of seconds before
// capturing page cache statistics.
func CapturePageCacheStatsWithDelay(b *testing.B, phase string, delaySeconds int) {
	b.Logf("[PAGECACHE] Waiting %d seconds before capturing %s...\n", delaySeconds, phase)
	time.Sleep(time.Duration(delaySeconds) * time.Second)
	_, _ = CapturePageCacheStats(b, phase)
}

// MonitorOptions configures the monitoring for benchmark tests.
type MonitorOptions struct {
	EnablePageCacheStats bool
	EnableBPFStats       bool
	DelayAfterBenchmark  time.Duration
}

// WithMonitoring runs a benchmark function with monitoring enabled.
// It captures page cache and eBPF statistics before and after the benchmark.
func WithMonitoring(b *testing.B, opts MonitorOptions, f func(b *testing.B)) {
	var (
		beforeStats  PageCacheStats
		beforeCached int64
		afterStats   PageCacheStats
		afterCached  int64
	)

	var m *monitor.Monitor
	if opts.EnableBPFStats {
		var err error
		m, err = monitor.NewMonitor()
		if err != nil {
			b.Fatalf("failed to start eBPF monitor: %v", err)
		}
		defer m.Close()
	}

	if opts.EnablePageCacheStats {
		beforeStats, beforeCached = CapturePageCacheStats(b, "before")
	}

	b.ResetTimer()
	f(b)
	b.StopTimer()

	if opts.EnablePageCacheStats {
		afterStats, afterCached = CapturePageCacheStats(b, "after")
	}

	if opts.DelayAfterBenchmark > 0 && opts.EnablePageCacheStats {
		CapturePageCacheStatsWithDelay(b, "after_delay", int(opts.DelayAfterBenchmark.Seconds()))
	}

	var (
		fstats map[uint32]uint64
		sstats map[uint32]bpf.BpfLruShrinkInfoT
		cstats map[uint32]monitor.CacheStats
	)

	if opts.EnableBPFStats && m != nil {
		// Show which monitoring mode we're using
		b.Logf("[eBPF] Using kprobe fallback: %v", m.IsUsingKprobe())

		fstats, _ = m.ReadCounts()
		sstats, _ = m.ReadShrinkStats()
		cstats, _ = m.ReadCacheStats()

		b.Logf("[eBPF] Fadvise Stats: %+v", fstats)
		b.Logf("[eBPF] Shrink Stats: %+v", sstats)

		// Display cache statistics summary if available
		if len(cstats) > 0 {
			var (
				totalReads, totalAdds                         uint64
				readProcesses, writeProcesses, mixedProcesses int
				topReaders, topWriters                        []string
			)

			// Categorize processes and collect statistics
			for pid, stats := range cstats {
				totalReads += stats.ReadBatchCalls
				totalAdds += stats.PageCacheAdds

				// Categorize process behavior
				hasReads := stats.ReadBatchCalls > 0
				hasWrites := stats.PageCacheAdds > 0

				if hasReads && hasWrites {
					mixedProcesses++
				    if stats.ReadBatchCalls > 20 || stats.PageCacheAdds > 1000 {
	       				read := stats.ReadBatchCalls
	        			adds := stats.PageCacheAdds
        				var hitRatio float64
        				if read == 0 {
						hitRatio = 0
					} else if adds >= read {
						hitRatio = 0
					} else {
						hitRatio = float64(read - adds) / float64(read) * 100
					}
					topReaders = append(topReaders,
						fmt.Sprintf("PID %d(R:%d,W:%d,HR:%.1f%%)",
							pid, read, adds, hitRatio,
						))
    	            }  
				} else if hasReads {
					readProcesses++
					if stats.ReadBatchCalls > 20 {
						topReaders = append(topReaders, fmt.Sprintf("PID %d(R:%d,HR:%.1f%%)",
							pid, stats.ReadBatchCalls, stats.HitRatio*100))
						var hitRatio float64
						if stats.ReadBatchCalls > 0 {
							hitRatio = float64(stats.ReadBatchCalls - stats.PageCacheAdds) / float64(stats.ReadBatchCalls) * 100
						}
						topReaders = append(topReaders, fmt.Sprintf("PID %d(R:%d,HR:%.1f%%)",
+                           pid, stats.ReadBatchCalls, hitRatio))
					}
				} else if hasWrites {
					writeProcesses++
					if stats.PageCacheAdds > 1000 {
						topWriters = append(topWriters, fmt.Sprintf("PID %d(W:%d)",
							pid, stats.PageCacheAdds))
					}
				}
			}
			var totalHits uint64
			if totalAdds > totalReads {
                totalHits = 0
				b.Logf("[eBPF] Overall Cache: Write-only workload, PageAdds=%d", totalAdds)
            } else {
				totalHits = totalReads - totalAdds
                overallHitRatio := float64(totalHits) / float64(totalReads) * 100
                b.Logf("[eBPF] Overall Cache: Hits=%d, Misses=%d, Reads=%d, Adds=%d, HitRatio=%.2f%%",
                   totalHits, totalAdds, totalReads, totalAdds, overallHitRatio)
                
            }

			// Show top active processes (limit output)
			if len(topReaders) > 0 {
				limit := 5
				if len(topReaders) > limit {
					b.Logf("[eBPF] Top %d Read-Active Processes: %v (and %d more)",
						limit, topReaders[:limit], len(topReaders)-limit)
				} else {
					b.Logf("[eBPF] Read-Active Processes: %v", topReaders)
				}
			}

			if len(topWriters) > 0 {
				limit := 3
				if len(topWriters) > limit {
					b.Logf("[eBPF] Top %d Write-Active Processes: %v (and %d more)",
						limit, topWriters[:limit], len(topWriters)-limit)
				} else {
					b.Logf("[eBPF] Write-Active Processes: %v", topWriters)
				}
			}
		}
		for _, stat := range sstats {
			if stat.NrReclaimed > 0 {
				b.Logf("[eBPF] Shrink Detail: pid=%d comm=%s scanned=%d reclaimed=%d",
					stat.CallerPid,
					int8SliceToString(stat.CallerComm[:]),
					stat.NrScanned,
					stat.NrReclaimed,
				)
			}
		}
		if reclaimStats, err := m.ReadDirectReclaimStats(); err == nil {
			for pid, info := range reclaimStats {
				b.Logf("[eBPF] DirectReclaim: pid=%d comm=%s", pid, info.Comm)
			}
		}
	}

	_ = AppendBenchmarkSummaryToCSV(b.Name(), "before", beforeStats, beforeCached, 0, 0)
	_ = AppendBenchmarkSummaryToCSV(b.Name(), "after", afterStats, afterCached, int64(len(fstats)), int64(len(sstats)))
}

// WithDefaultMonitoring is a convenience function that runs a benchmark with
// default monitoring options (all monitoring enabled).
func WithDefaultMonitoring(b *testing.B, f func(b *testing.B)) {
	WithMonitoring(b, MonitorOptions{
		EnablePageCacheStats: true,
		EnableBPFStats:       true,
		DelayAfterBenchmark:  3 * time.Second,
	}, f)
}

// WithTestMonitoring runs a test function with monitoring enabled.
// It captures page cache and eBPF statistics before and after the test.
func WithTestMonitoring(t *testing.T, opts MonitorOptions, f func(t *testing.T)) {
	var (
		beforeStats  PageCacheStats
		beforeCached int64
		afterStats   PageCacheStats
		afterCached  int64
	)

	var m *monitor.Monitor
	if opts.EnableBPFStats {
		var err error
		m, err = monitor.NewMonitor()
		if err != nil {
			t.Fatalf("failed to start eBPF monitor: %v", err)
		}
		defer m.Close()
	}

	if opts.EnablePageCacheStats {
		beforeStats, beforeCached = CapturePageCacheStatsForTest(t, "before")
	}

	f(t)

	if opts.EnablePageCacheStats {
		afterStats, afterCached = CapturePageCacheStatsForTest(t, "after")
	}

	if opts.DelayAfterBenchmark > 0 && opts.EnablePageCacheStats {
		CapturePageCacheStatsWithDelayForTest(t, "after_delay", int(opts.DelayAfterBenchmark.Seconds()))
	}

	var (
		fstats map[uint32]uint64
		sstats map[uint32]bpf.BpfLruShrinkInfoT
		cstats map[uint32]monitor.CacheStats
	)

	if opts.EnableBPFStats && m != nil {
		// Show which monitoring mode we're using
		t.Logf("[eBPF] Using kprobe fallback: %v", m.IsUsingKprobe())

		fstats, _ = m.ReadCounts()
		sstats, _ = m.ReadShrinkStats()
		cstats, _ = m.ReadCacheStats()

		if len(fstats) > 0 {
			t.Logf("[eBPF] Fadvise calls: %+v", fstats)
		}
		if len(sstats) > 0 {
			t.Logf("[eBPF] Shrink stats: %+v", sstats)
		}
		if len(cstats) > 0 {
			t.Logf("[eBPF] Cache stats: %+v", cstats)
		}
	}

	// Show delta stats
	if opts.EnablePageCacheStats {
		deltaRss := afterStats.Rss - beforeStats.Rss
		deltaPss := afterStats.Pss - beforeStats.Pss
		deltaSharedClean := afterStats.SharedClean - beforeStats.SharedClean
		deltaCached := afterCached - beforeCached

		t.Logf("[DELTA] RSS: %+dKB, PSS: %+dKB, SharedClean: %+dKB, Cached: %+dKB",
			deltaRss, deltaPss, deltaSharedClean, deltaCached)
	}
}

// WithMonitoringMode runs a test function with specified monitoring mode.
// If forceKprobe is true, only kprobe monitoring is used.
// If forceKprobe is false, automatic mode with fallback is used.
func WithMonitoringMode(t *testing.T, forceKprobe bool, f func(t *testing.T)) {
	var m *monitor.Monitor
	var err error

	// Create monitor with specified mode
	opts := monitor.MonitorOptions{
		ForceKprobe:    forceKprobe,
		EnableFallback: !forceKprobe, // Enable fallback unless forcing kprobe
	}
	m, err = monitor.NewMonitorWithOptions(opts)
	if err != nil {
		t.Fatalf("failed to start eBPF monitor with forceKprobe=%v: %v", forceKprobe, err)
	}
	defer m.Close()

	// Log which mode we're actually using
	var modeStr string
	if forceKprobe {
		modeStr = "kprobe-only"
	} else {
		modeStr = "auto-fallback"
	}

	t.Logf("[eBPF] Mode: %s, using kprobe: %v", modeStr, m.IsUsingKprobe())

	f(t)

	// Read and display stats
	fstats, _ := m.ReadCounts()
	sstats, _ := m.ReadShrinkStats()
	cstats, _ := m.ReadCacheStats()

	if len(fstats) > 0 {
		t.Logf("[eBPF] Fadvise calls: %+v", fstats)
	}
	if len(sstats) > 0 {
		t.Logf("[eBPF] Shrink stats: %+v", sstats)
	}
	if len(cstats) > 0 {
		t.Logf("[eBPF] Cache stats: %+v", cstats)
	}
}

// WithKprobeOnlyMonitoring runs a test function with kprobe-only monitoring.
// This is a convenience wrapper for WithMonitoringMode(t, true, f).
func WithKprobeOnlyMonitoring(t *testing.T, f func(t *testing.T)) {
	WithMonitoringMode(t, true, f)
}

// WithDefaultTestMonitoring is a convenience function for tests with default monitoring options.
func WithDefaultTestMonitoring(t *testing.T, f func(t *testing.T)) {
	WithTestMonitoring(t, MonitorOptions{
		EnablePageCacheStats: true,
		EnableBPFStats:       true,
		DelayAfterBenchmark:  3 * time.Second,
	}, f)
}

// CapturePageCacheStatsForTest captures page cache statistics for testing.T.
func CapturePageCacheStatsForTest(t *testing.T, phase string) (PageCacheStats, int64) {
	f, err := os.Open("/proc/self/smaps_rollup")
	if err != nil {
		t.Logf("[PAGECACHE] %s: open smaps_rollup failed: %v", phase, err)
		return PageCacheStats{}, 0
	}
	stats, err := parseSmapsRollup(f)
	f.Close()
	if err != nil {
		t.Logf("[PAGECACHE] %s: parse smaps_rollup failed: %v", phase, err)
		return PageCacheStats{}, 0
	}

	memf, err := os.Open("/proc/meminfo")
	if err != nil {
		t.Logf("[MEMINFO] %s: open meminfo failed: %v", phase, err)
		return PageCacheStats{}, 0
	}
	var cachedKB int64
	buf := make([]byte, 32*1024)
	builder := strings.Builder{}
	for {
		n, err := memf.Read(buf)
		if n > 0 {
			builder.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logf("[MEMINFO] %s: read meminfo failed: %v", phase, err)
			break
		}
	}
	memf.Close()

	for _, line := range strings.Split(builder.String(), "\n") {
		if strings.HasPrefix(line, "Cached:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					cachedKB = v
				}
			}
			break
		}
	}

	t.Logf("[PAGECACHE] %s: Rss=%dKB, Pss=%dKB, SharedClean=%dKB, Cached=%dKB",
		phase, stats.Rss, stats.Pss, stats.SharedClean, cachedKB)

	return stats, cachedKB
}

// CapturePageCacheStatsWithDelayForTest waits before capturing stats for testing.T.
func CapturePageCacheStatsWithDelayForTest(t *testing.T, phase string, delaySeconds int) {
	t.Logf("[PAGECACHE] Waiting %d seconds before capturing %s...\n", delaySeconds, phase)
	time.Sleep(time.Duration(delaySeconds) * time.Second)
	_, _ = CapturePageCacheStatsForTest(t, phase)
}

// AppendBenchmarkSummaryToCSV appends benchmark results to a CSV file.
func AppendBenchmarkSummaryToCSV(benchmark, phase string, stats PageCacheStats, cachedKB, fadviseCount, shrinkReclaimed int64) error {
	outputDir := "test/stress/fadvis/reports"
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create reports dir: %w", err)
	}

	outputFile := filepath.Join(outputDir, "summary.csv")
	isNew := false

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		isNew = true
	}

	f, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open summary.csv: %w", err)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	if isNew {
		if _, err := writer.WriteString("Benchmark,Phase,RSS_KB,PSS_KB,SharedClean_KB,Cached_KB,FadviseCalls,ShrinkReclaimed\n"); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	if _, err := writer.WriteString(fmt.Sprintf(
		"%s,%s,%d,%d,%d,%d,%d,%d\n",
		benchmark, phase, stats.Rss, stats.Pss, stats.SharedClean, cachedKB, fadviseCount, shrinkReclaimed,
	)); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}

	return writer.Flush()
}

// int8SliceToString converts []int8 to string, stopping at the first null character.
func int8SliceToString(s []int8) string {
	b := make([]byte, len(s))
	for i, v := range s {
		if v == 0 {
			return string(b[:i])
		}
		b[i] = byte(v)
	}
	return string(b)
}
