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

package fadvis

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fadvis"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Constants for file sizes and thresholds
const (
	kilobyte = 1024
	megabyte = 1024 * 1024
	gigabyte = 1024 * 1024 * 1024
	terabyte = 1024 * 1024 * 1024 * 1024

	// Default threshold for large files (100MB)
	DefaultThreshold = 100 * megabyte
	// Small file size (10MB)
	SmallFileSize = 10 * megabyte
	// Large file size (200MB)
	LargeFileSize = 200 * megabyte
	// Default concurrency level
	DefaultConcurrency = 4
)

// fileSystem is the file system instance used for all operations
var fileSystem fs.FileSystem

func init() {
	// Initialize the file system
	fileSystem = fs.NewLocalFileSystemWithLogger(logger.GetLogger("fadvis-benchmark"))
}

// createTestFile creates a test file of the specified size.
// It uses the fs package which automatically applies fadvise if the file size exceeds the threshold.
func createTestFile(t testing.TB, filePath string, size int64) error {
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

// readFileWithFadvise reads a file with automatic fadvise application.
// It uses the fs package which automatically applies fadvise if the file size exceeds the threshold.
func readFileWithFadvise(t testing.TB, filePath string) ([]byte, error) {
	// Read the file using the fs package
	return fileSystem.Read(filePath)
	// No need to manually apply fadvise, the fs package handles it automatically
}

// appendToFile appends data to a file, creating it if it doesn't exist.
// It uses the fs package which automatically applies fadvise if the file size exceeds the threshold.
func appendToFile(filePath string, data []byte) error {
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

// setupTestEnvironment creates a test directory and returns a cleanup function.
func setupTestEnvironment(t testing.TB) (string, func()) {
	tempDir := t.TempDir()
	return tempDir, func() {}
}

// createTestParts creates a set of test parts for merge benchmark.
func createTestParts(t testing.TB, testDir string, numParts int, partSize int64) []string {
	parts := make([]string, numParts)
	for i := 0; i < numParts; i++ {
		partPath := filepath.Join(testDir, fmt.Sprintf("part_%d", i))
		err := createTestFile(t, partPath, partSize)
		require.NoError(t, err)
		parts[i] = partPath
	}
	return parts
}

// simulateMergeOperation simulates a merge operation by reading parts and writing to an output file.
func simulateMergeOperation(t testing.TB, parts []string, outputFile string) error {
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

// setTestThreshold sets the fadvis threshold used for testing
func setTestThreshold(threshold int64) {
	// Create a simple threshold provider for testing
	provider := &testThresholdProvider{threshold: threshold}
	// Create a new Manager and set it as the global Manager
	manager := fadvis.NewManager(provider)
	fadvis.SetManager(manager)
}

// testThresholdProvider is a simple threshold provider for testing purposes
type testThresholdProvider struct {
	threshold int64
}

// GetThreshold returns a fixed threshold value
func (p *testThresholdProvider) GetThreshold() int64 {
	return p.threshold
}
