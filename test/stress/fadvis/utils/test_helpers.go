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

// utils package provides helper functions for file operations and monitoring.
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
	"github.com/apache/skywalking-banyandb/pkg/fadvis"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/test/stress/fadvis/bpf"
	"github.com/apache/skywalking-banyandb/test/stress/fadvis/monitor"
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

func ReadFileWithFadvise(t testing.TB, filePath string) ([]byte, error) {
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

func ReadFileStreamingWithFadvise(t testing.TB, filePath string, skipFadvise bool) (int64, error) {
	f, err := fileSystem.OpenFile(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var seqReader fs.SeqReader
	if skipFadvise {
		seqReader = f.SequentialRead(true)
	} else {
		seqReader = f.SequentialRead()
	}

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

func SetupTestEnvironment(t testing.TB) (string, func()) {
	tempDir := t.TempDir()
	return tempDir, func() {}
}

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

func SimulateMergeOperation(t testing.TB, parts []string, outputFile string) error {
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

		seqReader := inFile.SequentialRead()

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

func SetTestThreshold(threshold int64) {
	provider := &testThresholdProvider{threshold: threshold}
	manager := fadvis.NewManager(provider)
	fadvis.SetManager(manager)
	fs.SetGlobalThreshold(threshold)
}

func SetRealisticThreshold() {
	threshold := CalculateRealisticThreshold()
	SetTestThreshold(threshold)
}

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

func CapturePageCacheStatsWithDelay(b *testing.B, phase string, delaySeconds int) {
	b.Logf("[PAGECACHE] Waiting %d seconds before capturing %s...\n", delaySeconds, phase)
	time.Sleep(time.Duration(delaySeconds) * time.Second)
	_, _ = CapturePageCacheStats(b, phase)
}

type MonitorOptions struct {
	EnablePageCacheStats bool
	EnableBPFStats       bool
	DelayAfterBenchmark  time.Duration
}

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
	)

	if opts.EnableBPFStats && m != nil {
		fstats, _ = m.ReadCounts()
		sstats, _ = m.ReadShrinkStats()

		b.Logf("[eBPF] Fadvise Stats: %+v", fstats)
		b.Logf("[eBPF] Shrink Stats: %+v", sstats)
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

func WithMonitoringLegacy(b *testing.B, f func(b *testing.B)) {
	WithMonitoring(b, MonitorOptions{
		EnablePageCacheStats: true,
		EnableBPFStats:       true,
		DelayAfterBenchmark:  3 * time.Second,
	}, f)
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
