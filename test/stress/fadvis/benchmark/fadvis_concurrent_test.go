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
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"
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

func BenchmarkConcurrentReads_WithFadvise(b *testing.B) {
	utils.WithMonitoringLegacy(b, func(b *testing.B) {
		runConcurrentReads(b, true)
	})
}

func BenchmarkConcurrentReads_WithoutFadvise(b *testing.B) {
	utils.WithMonitoringLegacy(b, func(b *testing.B) {
		runConcurrentReads(b, false)
	})
}

func runConcurrentWrites(b *testing.B, enable bool) {
	testDir, cleanup := utils.SetupTestEnvironment(b)
	defer cleanup()

	if enable {
		utils.SetRealisticThreshold()
	} else {
		utils.SetTestThreshold(1 << 40)
	}

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

func BenchmarkConcurrentWrites_WithFadvise(b *testing.B) {
	utils.WithMonitoringLegacy(b, func(b *testing.B) {
		runConcurrentWrites(b, true)
	})
}

func BenchmarkConcurrentWrites_WithoutFadvise(b *testing.B) {
	utils.WithMonitoringLegacy(b, func(b *testing.B) {
		runConcurrentWrites(b, false)
	})
}

// BenchmarkThresholdAdaptation tests how the system adapts to changing memory thresholds.
func BenchmarkThresholdAdaptation(b *testing.B) {
	tests := []struct {
		name           string
		memoryPressure string
		fileSize       int64
	}{
		{
			name:           "SmallFileNormalMemory",
			memoryPressure: "normal",
			fileSize:       10 * 1024 * 1024,
		},
		{
			name:           "LargeFileHighMemoryPressure",
			memoryPressure: "high",
			fileSize:       200 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tempDir, cleanup := utils.SetupTestEnvironment(b)
			defer cleanup()

			utils.SetRealisticThreshold()

			testFile := filepath.Join(tempDir, "test_file")
			err := utils.CreateTestFile(b, testFile, tt.fileSize)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tt.memoryPressure == "high" {
					normalThreshold := utils.CalculateRealisticThreshold()
					lowThreshold := normalThreshold / 4
					utils.SetTestThreshold(lowThreshold)
					time.Sleep(100 * time.Millisecond)
					utils.SetRealisticThreshold()
				}
				time.Sleep(100 * time.Millisecond)

				data, err := utils.ReadFileStreamingWithFadvise(b, testFile, false)
				require.NoError(b, err)
				require.NotEmpty(b, data)
			}
		})
	}
}
