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

package benchmark

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"bufio"
	"os"

	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"

	"github.com/stretchr/testify/require"
)

func runConcurrentReads(b *testing.B, enable bool) {
    testDir, cleanup := utils.SetupTestEnvironment(b)
    defer cleanup()

    numFiles := 10
    files := make([]string, numFiles)
    for i := 0; i < numFiles; i++ {
        path := filepath.Join(testDir, fmt.Sprintf("test_file_%d.dat", i))
        require.NoError(b, utils.CreateTestFile(b, path, 200*1024*1024))
        files[i] = path
    }

    if enable {
        utils.SetRealisticThreshold()
    } else {
        utils.SetTestThreshold(1 << 40)
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        for _, file := range files {
            data, err := utils.ReadFileStreamingWithFadvise(b, file, enable)
            require.NoError(b, err)
            require.NotEmpty(b, data)
        }
    }
    b.StopTimer()
    utils.CapturePageCacheStatsWithDelay(b, fmt.Sprintf("after_concurrent_reads_enable=%v", enable), 3)
}

// BenchmarkConcurrentReads_WithFadvise benchmarks concurrent sequential reads with fadvise enabled
func BenchmarkConcurrentReads_WithFadvise(b *testing.B) {
    utils.WithMonitoringLegacy(b, func(b *testing.B) {
        runConcurrentReads(b, true)
    })
}

// BenchmarkConcurrentReads_WithoutFadvise benchmarks concurrent sequential reads without fadvise
func BenchmarkConcurrentReads_WithoutFadvise(b *testing.B) {
    utils.WithMonitoringLegacy(b, func(b *testing.B) {
        runConcurrentReads(b, false)
    })
}

// helper for concurrent writes
func runConcurrentWrites(b *testing.B, enable bool) {
    testDir, cleanup := utils.SetupTestEnvironment(b)
    defer cleanup()

    if enable {
        utils.SetRealisticThreshold()
    } else {
        utils.SetTestThreshold(1 << 40)
    }

    // Prepare test data: 4MB of 'a'
    data := make([]byte, 4*1024*1024)
    for i := range data {
        data[i] = 'a'
    }

    numFiles := 5
    outputFiles := make([]string, numFiles)
    for i := 0; i < numFiles; i++ {
        outputFiles[i] = filepath.Join(testDir, fmt.Sprintf("write_%d.dat", i))
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        for _, path := range outputFiles {
            f, err := os.Create(path)
            require.NoError(b, err)
            w := bufio.NewWriterSize(f, 4*1024)
            _, err = w.Write(data)
            require.NoError(b, err)
            w.Flush()
            f.Sync()
            f.Close()
        }
    }
    b.StopTimer()
    utils.CapturePageCacheStatsWithDelay(b, fmt.Sprintf("after_concurrent_writes_enable=%v", enable), 3)
}

// BenchmarkConcurrentWrites_WithFadvise benchmarks concurrent writes with fadvise enabled
func BenchmarkConcurrentWrites_WithFadvise(b *testing.B) {
    utils.WithMonitoringLegacy(b, func(b *testing.B) {
        runConcurrentWrites(b, true)
    })
}

// BenchmarkConcurrentWrites_WithoutFadvise benchmarks concurrent writes without fadvise
func BenchmarkConcurrentWrites_WithoutFadvise(b *testing.B) {
    utils.WithMonitoringLegacy(b, func(b *testing.B) {
        runConcurrentWrites(b, false)
    })
}




// BenchmarkThresholdAdaptation tests how the system adapts to changing memory thresholds
func BenchmarkThresholdAdaptation(b *testing.B) {
	tests := []struct {
		name           string
		fileSize       int64
		memoryPressure string
	}{
		{
			name:           "SmallFileNormalMemory",
			fileSize:       10 * 1024 * 1024,
			memoryPressure: "normal",
		},
		{
			name:           "LargeFileHighMemoryPressure",
			fileSize:       200 * 1024 * 1024,
			memoryPressure: "high",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tempDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			// Set a realistic utils threshold based on system memory
			utils.SetRealisticThreshold()

			// Create test file
			testFile := filepath.Join(tempDir, "test_file")
			err := utils.CreateTestFile(b, testFile, tt.fileSize)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tt.memoryPressure == "high" {
					// Simulate high memory pressure by setting a lower threshold
					// In real system, this would be automatically adjusted based on memory pressure
					// Here we simulate by manually setting a lower threshold (25% of normal)
					normalThreshold := utils.CalculateRealisticThreshold()
					lowThreshold := normalThreshold / 4 // 25% of normal threshold
					utils.SetTestThreshold(lowThreshold)
					time.Sleep(100 * time.Millisecond)
					// Reset to normal threshold
					utils.SetRealisticThreshold()
				}
				time.Sleep(100 * time.Millisecond)

				// Read the file using our integrated read function
				data, err := utils.ReadFileStreamingWithFadvise(b, testFile, false)
				require.NoError(b, err)
				require.NotEmpty(b, data)
			}
		})
	}
}
