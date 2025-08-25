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

package sidx

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

func TestExtractTagNameAndExtension(t *testing.T) {
	tests := []struct {
		fileName      string
		expectedTag   string
		expectedExt   string
		expectedFound bool
	}{
		{
			fileName:      "user_id.td",
			expectedTag:   "user_id",
			expectedExt:   ".td",
			expectedFound: true,
		},
		{
			fileName:      "service_name.tm",
			expectedTag:   "service_name",
			expectedExt:   ".tm",
			expectedFound: true,
		},
		{
			fileName:      "endpoint.tf",
			expectedTag:   "endpoint",
			expectedExt:   ".tf",
			expectedFound: true,
		},
		{
			fileName:      "complex.name.td",
			expectedTag:   "complex.name",
			expectedExt:   ".td",
			expectedFound: true,
		},
		{
			fileName:      "primary.bin",
			expectedTag:   "",
			expectedExt:   "",
			expectedFound: false,
		},
		{
			fileName:      "name",
			expectedTag:   "",
			expectedExt:   "",
			expectedFound: false,
		},
		{
			fileName:      "name.unknown",
			expectedTag:   "",
			expectedExt:   "",
			expectedFound: false,
		},
		{
			fileName:      "primary.bin",
			expectedTag:   "",
			expectedExt:   "",
			expectedFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.fileName, func(t *testing.T) {
			tagName, extension, found := extractTagNameAndExtension(tt.fileName)
			assert.Equal(t, tt.expectedFound, found, "found mismatch")
			assert.Equal(t, tt.expectedTag, tagName, "tag name mismatch")
			assert.Equal(t, tt.expectedExt, extension, "extension mismatch")
		})
	}
}

func TestPartLifecycleManagement(t *testing.T) {
	// Create test filesystem
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Create test part metadata
	pm := generatePartMetadata()
	pm.ID = 12345
	pm.MinKey = 100
	pm.MaxKey = 999
	pm.BlocksCount = 2
	pm.CompressedSizeBytes = 1024
	pm.UncompressedSizeBytes = 2048
	pm.TotalCount = 50

	// Marshal metadata to create test files
	metaData, err := pm.marshal()
	require.NoError(t, err)

	// Create required files
	testFiles := map[string][]byte{
		primaryFilename: []byte("primary data"),
		dataFilename:    []byte("data content"),
		keysFilename:    []byte("keys content"),
		metaFilename:    metaData,
	}

	for fileName, content := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(t, err)
	}

	// Test part opening
	part := mustOpenPart(tempDir, testFS)
	require.NotNil(t, part)
	defer part.close()

	// Verify part properties
	assert.Equal(t, tempDir, part.path)
	assert.Equal(t, testFS, part.fileSystem)
	assert.NotNil(t, part.primary)
	assert.NotNil(t, part.data)
	assert.NotNil(t, part.keys)

	// Verify metadata was loaded
	require.NotNil(t, part.partMetadata)
	assert.Equal(t, uint64(12345), part.partMetadata.ID)
	assert.Equal(t, int64(100), part.partMetadata.MinKey)
	assert.Equal(t, int64(999), part.partMetadata.MaxKey)
	assert.Equal(t, uint64(2), part.partMetadata.BlocksCount)

	// Verify block metadata was initialized
	assert.NotNil(t, part.blockMetadata)
	assert.Equal(t, 0, len(part.blockMetadata)) // Empty since we don't parse primary.bin yet
	assert.Equal(t, 2, cap(part.blockMetadata)) // Capacity based on BlocksCount

	// Verify String method
	expectedString := fmt.Sprintf("sidx part %d at %s", pm.ID, tempDir)
	assert.Equal(t, expectedString, part.String())

	// Test accessors
	assert.Equal(t, part.partMetadata, part.getPartMetadata())
	assert.Equal(t, part.blockMetadata, part.getBlockMetadata())
	assert.Equal(t, tempDir, part.Path())

	// Cleanup
	releasePartMetadata(pm)
}

func TestPartWithTagFiles(t *testing.T) {
	// Create test filesystem
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Create minimal required files first
	pm := generatePartMetadata()
	pm.ID = 67890
	pm.BlocksCount = 1
	metaData, err := pm.marshal()
	require.NoError(t, err)

	requiredFiles := map[string][]byte{
		primaryFilename: []byte("primary"),
		dataFilename:    []byte("data"),
		keysFilename:    []byte("keys"),
		metaFilename:    metaData,
	}

	for fileName, content := range requiredFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(t, err)
	}

	// Create tag files
	tagFiles := map[string][]byte{
		"user_id.td":      []byte("user id tag data"),
		"user_id.tm":      []byte("user id tag metadata"),
		"user_id.tf":      []byte("user id tag filter"),
		"service_name.td": []byte("service name tag data"),
		"service_name.tf": []byte("service name tag filter"),
		"endpoint.tm":     []byte("endpoint tag metadata"),
		// Invalid files that should be ignored
		"invalid":          []byte("invalid tag file"),
		"invalid.unknown":  []byte("unknown extension"),
		"regular_file.txt": []byte("regular file"),
	}

	for fileName, content := range tagFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(t, err)
	}

	// Open part
	part := mustOpenPart(tempDir, testFS)
	require.NotNil(t, part)
	defer part.close()

	// Verify tag files were opened correctly
	assert.Equal(t, 2, len(part.tagData))     // user_id, service_name
	assert.Equal(t, 2, len(part.tagMetadata)) // user_id, endpoint
	assert.Equal(t, 2, len(part.tagFilters))  // user_id, service_name

	// Test tag data readers
	userIDData, exists := part.getTagDataReader("user_id")
	assert.True(t, exists)
	assert.NotNil(t, userIDData)

	serviceNameData, exists := part.getTagDataReader("service_name")
	assert.True(t, exists)
	assert.NotNil(t, serviceNameData)

	_, exists = part.getTagDataReader("endpoint")
	assert.False(t, exists) // Only has metadata, not data

	// Test tag metadata readers
	userIDMeta, exists := part.getTagMetadataReader("user_id")
	assert.True(t, exists)
	assert.NotNil(t, userIDMeta)

	endpointMeta, exists := part.getTagMetadataReader("endpoint")
	assert.True(t, exists)
	assert.NotNil(t, endpointMeta)

	_, exists = part.getTagMetadataReader("service_name")
	assert.False(t, exists) // Only has data and filter, not metadata

	// Test tag filter readers
	userIDFilter, exists := part.getTagFilterReader("user_id")
	assert.True(t, exists)
	assert.NotNil(t, userIDFilter)

	serviceNameFilter, exists := part.getTagFilterReader("service_name")
	assert.True(t, exists)
	assert.NotNil(t, serviceNameFilter)

	_, exists = part.getTagFilterReader("endpoint")
	assert.False(t, exists) // Only has metadata, not filter

	// Test available tag names
	tagNames := part.getAvailableTagNames()
	assert.Equal(t, 3, len(tagNames))
	expectedTags := map[string]bool{
		"user_id":      false,
		"service_name": false,
		"endpoint":     false,
	}
	for _, tagName := range tagNames {
		_, exists := expectedTags[tagName]
		assert.True(t, exists, "unexpected tag name: %s", tagName)
		expectedTags[tagName] = true
	}
	for tagName, found := range expectedTags {
		assert.True(t, found, "missing expected tag name: %s", tagName)
	}

	// Test hasTagFiles
	assert.True(t, part.hasTagFiles("user_id"))      // Has all three types
	assert.True(t, part.hasTagFiles("service_name")) // Has data and filter
	assert.True(t, part.hasTagFiles("endpoint"))     // Has metadata only
	assert.False(t, part.hasTagFiles("nonexistent")) // Doesn't exist

	// Cleanup
	releasePartMetadata(pm)
}

func TestPartErrorHandling(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Test 1: Missing required files should panic
	t.Run("missing_files_panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic when opening part with missing files")
			}
		}()
		mustOpenPart(tempDir, testFS)
	})

	// Test 2: Invalid metadata should panic
	t.Run("invalid_metadata_panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic when loading invalid metadata")
			}
		}()

		// Create required files with invalid metadata
		testFiles := map[string][]byte{
			primaryFilename: []byte("primary"),
			dataFilename:    []byte("data"),
			keysFilename:    []byte("keys"),
			metaFilename:    []byte("invalid json metadata"),
		}

		for fileName, content := range testFiles {
			filePath := filepath.Join(tempDir, fileName)
			_, err := testFS.Write(content, filePath, 0o644)
			require.NoError(t, err)
		}

		mustOpenPart(tempDir, testFS)
	})
}

func TestPartClosingBehavior(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Create test part
	pm := generatePartMetadata()
	pm.ID = 11111
	pm.BlocksCount = 1
	metaData, err := pm.marshal()
	require.NoError(t, err)

	testFiles := map[string][]byte{
		primaryFilename: []byte("primary"),
		dataFilename:    []byte("data"),
		keysFilename:    []byte("keys"),
		metaFilename:    metaData,
		"test.td":       []byte("tag data"),
		"test.tm":       []byte("tag metadata"),
		"test.tf":       []byte("tag filter"),
	}

	for fileName, content := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(t, err)
	}

	// Open part
	part := mustOpenPart(tempDir, testFS)
	require.NotNil(t, part)

	// Verify it's properly opened
	assert.NotNil(t, part.primary)
	assert.NotNil(t, part.data)
	assert.NotNil(t, part.keys)
	assert.Equal(t, 1, len(part.tagData))
	assert.Equal(t, 1, len(part.tagMetadata))
	assert.Equal(t, 1, len(part.tagFilters))
	assert.NotNil(t, part.partMetadata)

	// Close the part
	part.close()

	// Verify resources are cleaned up
	// Note: We can't directly test that files are closed since fs.Reader
	// doesn't expose that state, but we can verify that metadata is released
	assert.Nil(t, part.partMetadata)
	assert.Nil(t, part.blockMetadata)

	// Test closing with defensive programming (nil check in close method)
	// The close method should handle nil pointers gracefully

	// Cleanup
	releasePartMetadata(pm)
}

func TestPartMemoryManagement(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Create test part with block metadata
	pm := generatePartMetadata()
	pm.ID = 22222
	pm.BlocksCount = 3 // Test with multiple blocks
	metaData, err := pm.marshal()
	require.NoError(t, err)

	testFiles := map[string][]byte{
		primaryFilename: []byte("primary with block data"),
		dataFilename:    []byte("data"),
		keysFilename:    []byte("keys"),
		metaFilename:    metaData,
	}

	for fileName, content := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(t, err)
	}

	// Open and immediately close multiple parts to test memory management
	for i := 0; i < 10; i++ {
		part := mustOpenPart(tempDir, testFS)
		require.NotNil(t, part)

		// Verify part was created correctly
		assert.NotNil(t, part.partMetadata)
		assert.Equal(t, 0, len(part.blockMetadata))
		assert.Equal(t, 3, cap(part.blockMetadata))

		// Close immediately
		part.close()

		// Verify cleanup
		assert.Nil(t, part.partMetadata)
		assert.Nil(t, part.blockMetadata)
	}

	// Cleanup
	releasePartMetadata(pm)
}

func TestPartStringRepresentation(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Test 1: Part with valid metadata
	pm := generatePartMetadata()
	pm.ID = 99999
	pm.BlocksCount = 1
	metaData, err := pm.marshal()
	require.NoError(t, err)

	testFiles := map[string][]byte{
		primaryFilename: []byte("primary"),
		dataFilename:    []byte("data"),
		keysFilename:    []byte("keys"),
		metaFilename:    metaData,
	}

	for fileName, content := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(t, err)
	}

	part := mustOpenPart(tempDir, testFS)

	expectedString := fmt.Sprintf("sidx part %d at %s", pm.ID, tempDir)
	assert.Equal(t, expectedString, part.String())

	// Test 2: Part with nil metadata (after close)
	part.close()
	expectedStringAfterClose := fmt.Sprintf("sidx part at %s", tempDir)
	assert.Equal(t, expectedStringAfterClose, part.String())

	// Cleanup
	releasePartMetadata(pm)
}

// Benchmark tests for performance validation.
func BenchmarkPartOpen(b *testing.B) {
	testFS := fs.NewLocalFileSystem()
	tempDir := b.TempDir()

	// Setup test data
	pm := generatePartMetadata()
	pm.ID = 77777
	pm.BlocksCount = 5
	metaData, err := pm.marshal()
	require.NoError(b, err)

	testFiles := map[string][]byte{
		primaryFilename: []byte("primary data for benchmark"),
		dataFilename:    []byte("data content for benchmark"),
		keysFilename:    []byte("keys content for benchmark"),
		metaFilename:    metaData,
	}

	// Add multiple tag files
	for i := 0; i < 10; i++ {
		tagName := fmt.Sprintf("tag_%d", i)
		testFiles[fmt.Sprintf("%s.td", tagName)] = []byte(fmt.Sprintf("tag data %d", i))
		testFiles[fmt.Sprintf("%s.tm", tagName)] = []byte(fmt.Sprintf("tag metadata %d", i))
		testFiles[fmt.Sprintf("%s.tf", tagName)] = []byte(fmt.Sprintf("tag filter %d", i))
	}

	for fileName, content := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		part := mustOpenPart(tempDir, testFS)
		part.close()
	}

	// Cleanup
	releasePartMetadata(pm)
}

func BenchmarkPartTagAccess(b *testing.B) {
	testFS := fs.NewLocalFileSystem()
	tempDir := b.TempDir()

	// Setup test part with many tag files
	pm := generatePartMetadata()
	pm.ID = 88888
	pm.BlocksCount = 1
	metaData, err := pm.marshal()
	require.NoError(b, err)

	testFiles := map[string][]byte{
		primaryFilename: []byte("primary"),
		dataFilename:    []byte("data"),
		keysFilename:    []byte("keys"),
		metaFilename:    metaData,
	}

	// Add many tag files
	numTags := 100
	for i := 0; i < numTags; i++ {
		tagName := fmt.Sprintf("tag_%d", i)
		testFiles[fmt.Sprintf("%s.td", tagName)] = []byte(fmt.Sprintf("data %d", i))
		testFiles[fmt.Sprintf("%s.tm", tagName)] = []byte(fmt.Sprintf("meta %d", i))
		testFiles[fmt.Sprintf("%s.tf", tagName)] = []byte(fmt.Sprintf("filter %d", i))
	}

	for fileName, content := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		_, err := testFS.Write(content, filePath, 0o644)
		require.NoError(b, err)
	}

	part := mustOpenPart(tempDir, testFS)
	defer part.close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tagName := fmt.Sprintf("tag_%d", i%numTags)
		_, exists := part.getTagDataReader(tagName)
		assert.True(b, exists)
	}

	// Cleanup
	releasePartMetadata(pm)
}
