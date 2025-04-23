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
	"github.com/apache/skywalking-banyandb/banyand/fadvis"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/fs/fadvise"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"testing"
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

	// Open or create the file using the fs package
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for append: %w", err)
	}

	// Create a fadvise file wrapper
	fadvisFile := fadvise.NewFileWithThreshold(file, filePath, fadvis.GetThreshold())
	defer fadvisFile.Close()

	// Write data using the fadvise wrapper
	_, err = fadvisFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// No need to manually apply fadvise, the fs package handles it automatically

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
func simulateMergeOperation(t testing.TB, outputFile string, parts []string) {
	// Create the output file using the fs package
	outFile, err := fileSystem.CreateFile(outputFile, 0644)
	require.NoError(t, err)
	defer outFile.Close()

	// Create a sequential writer
	seqWriter := outFile.SequentialWrite()
	defer seqWriter.Close()

	// Read from parts and write to output file
	buffer := make([]byte, 8192)
	for _, part := range parts {
		// Open each part file using the fs package
		inFile, err := fileSystem.OpenFile(part)
		require.NoError(t, err)

		// Create a sequential reader
		seqReader := inFile.SequentialRead()

		for {
			n, err := seqReader.Read(buffer)
			if n == 0 || err != nil {
				if err != io.EOF {
					require.NoError(t, err)
				}
				break
			}

			_, err = seqWriter.Write(buffer[:n])
			require.NoError(t, err)
		}

		seqReader.Close()
		inFile.Close()
	}

	// No need to manually apply fadvise, the fs package handles it automatically
}
