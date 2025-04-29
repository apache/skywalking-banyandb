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

package utils

import (
	"bufio"
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
)

// Constants for file sizes and thresholds
const (
	Megabyte = 1024 * 1024
	Terabyte = 1024 * 1024 * 1024 * 1024
	// Large file size (200MB)
	LargeFileSize = 200 * Megabyte
)

// fileSystem is the file system instance used for all operations
var fileSystem fs.FileSystem

func init() {
	// Initialize the file system
	fileSystem = fs.NewLocalFileSystemWithLogger(logger.GetLogger("fadvis-benchmark"))
}

// sharedReadBuffer is a shared buffer used for reading files
var sharedReadBuffer = make([]byte, 32*1024)

// CreateTestFile creates a test file of the specified size.
// It uses the fs package which automatically applies fadvise if the file size exceeds the threshold.
func CreateTestFile(t testing.TB, filePath string, size int64) error {
	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
	}

	// Create the file using the fs package
	file, err := fileSystem.CreateFile(filePath, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Truncate to the desired size
	os.Truncate(filePath, size)

	// No need to manually apply fadvise, the fs package handles it automatically
	// based on the configured threshold

	return nil
}

// ReadFileWithFadvise reads a file with automatic fadvise application.
// It uses the fs package which automatically applies fadvise if the file size exceeds the threshold.
func ReadFileWithFadvise(t testing.TB, filePath string) ([]byte, error) {
	// Use streaming read instead of reading the entire file at once
	f, err := fileSystem.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get file size - not used but kept for reference
	_, err = f.Size()
	if err != nil {
		return nil, err
	}

	// Only read the first 32KB of the file for verification
	// In actual tests we're focused on PageCache effects rather than complete reads
	n, err := f.Read(0, sharedReadBuffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// Return the read data without copying to a new slice
	return sharedReadBuffer[:n], nil
}

// ReadFileStreamingWithFadvise reads a file using streaming to minimize heap allocations
// skipFadvise parameter controls whether to skip fadvise calls
func ReadFileStreamingWithFadvise(t testing.TB, filePath string, skipFadvise bool) (int64, error) {
	f, err := fileSystem.OpenFile(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Create a sequential reader, with option to skip fadvise
	var seqReader fs.SeqReader
	if skipFadvise {
		// Use sequential reader with fadvise disabled
		seqReader = f.SequentialRead(true) // skipFadvise = true
	} else {
		// Use sequential reader with fadvise enabled
		seqReader = f.SequentialRead() // default: skipFadvise = false
	}

	// Use shared buffer for streaming reads
	totalBytes := int64(0)
	for {
		// SeqReader already implements io.Reader
		n, err := seqReader.Read(sharedReadBuffer)
		if n > 0 {
			totalBytes += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, err
		}
	}

	return totalBytes, nil
}

// AppendToFile appends data to a file, creating it if it doesn't exist.
// It uses the fs package which automatically applies fadvise if the file size exceeds the threshold.
func AppendToFile(filePath string, data []byte) error {
	// Check if file exists
	_, err := os.Stat(filePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Open or create the file for append
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for append: %w", err)
	}
	defer file.Close()

	// Write data
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Apply fadvis if needed
	if info, err := file.Stat(); err == nil {
		if manager := fadvis.GetManager(); manager != nil && manager.ShouldApplyFadvis(info.Size()) {
			// File is large enough, apply fadvis
			// Note: In a real implementation, this would be handled by the fs package
		}
	}

	return nil
}

// SetupTestEnvironment creates a test directory and returns a cleanup function.
func SetupTestEnvironment(t testing.TB) (string, func()) {
	tempDir := t.TempDir()
	return tempDir, func() {}
}

// CreateTestParts creates a set of test parts for merge benchmark.
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

// SimulateMergeOperation simulates a merge operation by reading parts and writing to an output file.
func SimulateMergeOperation(t testing.TB, parts []string, outputFile string) error {
	// Create the output file using the fs package
	outFile, err := fileSystem.CreateFile(outputFile, 0644)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Create a sequential writer
	seqWriter := outFile.SequentialWrite()
	defer seqWriter.Close()

	// Read from parts and write to output file
	buffer := make([]byte, 8192)
	for _, part := range parts {
		// Open each part file using the fs package
		inFile, err := fileSystem.OpenFile(part)
		if err != nil {
			return err
		}

		// Create a sequential reader
		seqReader := inFile.SequentialRead()

		for {
			n, err := seqReader.Read(buffer)
			if n == 0 || err != nil {
				if err != io.EOF {
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

	// No need to manually apply fadvise, the fs package handles it automatically
	return nil
}

// SetTestThreshold sets the fadvis threshold used for testing
func SetTestThreshold(threshold int64) {
	// Create a simple threshold provider for testing
	provider := &testThresholdProvider{threshold: threshold}
	// Create a new Manager and set it as the global Manager
	manager := fadvis.NewManager(provider)
	fadvis.SetManager(manager)
}

// SetRealisticThreshold sets a realistic fadvis threshold based on system memory
// It calculates threshold as 1% of page cache (which is 25% of total memory)
// This mimics the actual production logic in fadvis.Manager
func SetRealisticThreshold() {
	// Calculate threshold using the same logic as in protector.Memory.GetThreshold
	threshold := CalculateRealisticThreshold()

	SetTestThreshold(threshold)
}

// CalculateRealisticThreshold calculates a realistic threshold based on system memory
// using the same logic as in protector.Memory.GetThreshold
func CalculateRealisticThreshold() int64 {
	// Default page cache percent (100 - allowedPercent)
	// In production, allowedPercent is typically 75%, so pageCachePercent is 25%
	pageCachePercent := 25

	// Get memory limit from cgroups
	totalMemory, err := cgroups.MemoryLimit()
	if err != nil {
		// Fallback to a reasonable default if we can't get memory info
		return 64 * 1024 * 1024 // 64MB fallback
	}

	// Calculate page cache size (pageCachePercent% of total memory)
	pageCacheSize := totalMemory * int64(pageCachePercent) / 100

	// Calculate threshold as 1% of page cache
	threshold := pageCacheSize / 100

	// Set a minimum threshold to avoid too small values
	if threshold < 1024*1024 { // 1MB minimum
		threshold = 1024 * 1024
	}

	return threshold
}

// testThresholdProvider is a simple threshold provider for testing purposes
type testThresholdProvider struct {
	threshold int64
}

// GetThreshold returns a fixed threshold value
func (p *testThresholdProvider) GetThreshold() int64 {
	return p.threshold
}

// PageCacheStats holds the parsed information from /proc/self/smaps_rollup.
type PageCacheStats struct {
	Rss         int64 // Resident Set Size
	Pss         int64 // Proportional Set Size
	SharedClean int64 // Clean pages that are shared with other processes
}

// parseSmapsRollup parses /proc/self/smaps_rollup into PageCacheStats.
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

// CapturePageCacheStats captures and records page cache memory usage stats.
// It both prints to stdout and saves to a profile file for later analysis.
func CapturePageCacheStats(b *testing.B, phase string) int64 {
	f, err := os.Open("/proc/self/smaps_rollup")
	if err != nil {
		b.Logf("[PAGECACHE] %s: open smaps_rollup failed: %v", phase, err)
		return 0
	}
	stats, err := parseSmapsRollup(f)
	f.Close()
	if err != nil {
		b.Logf("[PAGECACHE] %s: parse smaps_rollup failed: %v", phase, err)
		return 0
	}

	memf, err := os.Open("/proc/meminfo")
	if err != nil {
		b.Logf("[MEMINFO] %s: open meminfo failed: %v", phase, err)
		return 0
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

	return cachedKB
}

// CapturePageCacheStatsWithDelay captures page cache stats after a delay
func CapturePageCacheStatsWithDelay(b *testing.B, phase string, delaySeconds int) {
	b.Logf("[PAGECACHE] Waiting %d seconds before capturing %s...\n", delaySeconds, phase)
	time.Sleep(time.Duration(delaySeconds) * time.Second)
	_ = CapturePageCacheStats(b, phase)
}
