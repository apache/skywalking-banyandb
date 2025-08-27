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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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

func TestPartStringRepresentation(t *testing.T) {
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	// Test 1: Part with valid metadata
	pm := generatePartMetadata()
	pm.ID = 99999
	pm.BlocksCount = 1
	metaData, err := pm.marshal()
	require.NoError(t, err)

	// Create valid primary block metadata
	pbm := primaryBlockMetadata{
		seriesID: 1,
		minKey:   0,
		maxKey:   100,
		dataBlock: dataBlock{
			offset: 0,
			size:   256,
		},
	}

	// Marshal and compress primary block metadata for meta.bin
	primaryData := pbm.marshal(nil)
	compressedPrimaryData := zstd.Compress(nil, primaryData, 1)

	testFiles := map[string][]byte{
		primaryFilename:  []byte("primary"), // placeholder for primary.bin
		dataFilename:     []byte("data"),
		keysFilename:     []byte("keys"),
		metaFilename:     compressedPrimaryData, // meta.bin should contain compressed primary block metadata
		manifestFilename: metaData,              // manifest.json should contain part metadata
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

func TestMemPartInitialization(t *testing.T) {
	// Create simple test elements
	elems := createTestElements([]testElement{
		{
			seriesID: 1,
			userKey:  100,
			data:     []byte("data1"),
			tags: []tag{
				{
					name:      "service",
					value:     []byte("order-service"),
					valueType: pbv1.ValueTypeStr,
					indexed:   true,
				},
			},
		},
	})
	defer releaseElements(elems)

	t.Run("memPart initialization", func(t *testing.T) {
		mp := generateMemPart()
		defer releaseMemPart(mp)

		// Check if memPart is properly initialized from elements
		mp.mustInitFromElements(elems)

		t.Logf("Primary buffer length: %d", len(mp.primary.Buf))
		t.Logf("Data buffer length: %d", len(mp.data.Buf))
		t.Logf("Keys buffer length: %d", len(mp.keys.Buf))

		// Check partMetadata
		if mp.partMetadata != nil {
			t.Logf("Part metadata blocks count: %d", mp.partMetadata.BlocksCount)
			t.Logf("Part metadata total count: %d", mp.partMetadata.TotalCount)
		} else {
			t.Log("Part metadata is nil")
		}

		// Check tag metadata
		for tagName, buf := range mp.tagMetadata {
			t.Logf("Tag metadata for %s: %d bytes", tagName, len(buf.Buf))
		}

		// Check tag data
		for tagName, buf := range mp.tagData {
			t.Logf("Tag data for %s: %d bytes", tagName, len(buf.Buf))
		}

		// Verify primary buffer has some data
		if len(mp.primary.Buf) > 0 {
			t.Log("✓ Primary buffer contains data")
		} else {
			t.Log("⚠ Primary buffer is empty")
		}
	})
}

func TestMemPartFlushAndReadAllRoundTrip(t *testing.T) {
	// Test case that flushes to disk first, then reads back
	// This works around issues with in-memory primary metadata
	testFS := fs.NewLocalFileSystem()
	tempDir := t.TempDir()

	tests := []struct {
		elements *elements
		name     string
	}{
		{
			name: "single series with single element",
			elements: createTestElements([]testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
					},
				},
			}),
		},
		{
			name: "single series with multiple elements",
			elements: createTestElements([]testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
						{
							name:      "method",
							value:     []byte("POST"),
							valueType: pbv1.ValueTypeStr,
							indexed:   false,
						},
					},
				},
				{
					seriesID: 1,
					userKey:  101,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
						{
							name:      "method",
							value:     []byte("GET"),
							valueType: pbv1.ValueTypeStr,
							indexed:   false,
						},
					},
				},
			}),
		},
		{
			name: "multiple series with multiple elements",
			elements: createTestElements([]testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
						{
							name:      "duration",
							value:     []byte{0, 0, 0, 0, 0, 0, 0, 100}, // int64: 100
							valueType: pbv1.ValueTypeInt64,
							indexed:   false,
						},
					},
				},
				{
					seriesID: 1,
					userKey:  101,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
						{
							name:      "duration",
							value:     []byte{0, 0, 0, 0, 0, 0, 0, 150}, // int64: 150
							valueType: pbv1.ValueTypeInt64,
							indexed:   false,
						},
					},
				},
				{
					seriesID: 2,
					userKey:  200,
					data:     []byte("data3"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("payment-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
						{
							name:      "duration",
							value:     []byte{0, 0, 0, 0, 0, 0, 0, 75}, // int64: 75
							valueType: pbv1.ValueTypeInt64,
							indexed:   false,
						},
					},
				},
			}),
		},
		{
			name: "elements with missing tags",
			elements: createTestElements([]testElement{
				{
					seriesID: 1,
					userKey:  100,
					data:     []byte("data1"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
					},
				},
				{
					seriesID: 1,
					userKey:  101,
					data:     []byte("data2"),
					tags: []tag{
						{
							name:      "service",
							value:     []byte("order-service"),
							valueType: pbv1.ValueTypeStr,
							indexed:   true,
						},
						{
							name:      "endpoint",
							value:     []byte("/api/orders"),
							valueType: pbv1.ValueTypeStr,
							indexed:   false,
						},
					},
				},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Create memPart and initialize with elements
			mp := generateMemPart()
			defer releaseMemPart(mp)

			// Create a copy of elements for initialization to avoid modifying the original
			elementsCopy := generateElements()
			defer releaseElements(elementsCopy)

			// Deep copy the elements
			elementsCopy.seriesIDs = append(elementsCopy.seriesIDs, tt.elements.seriesIDs...)
			elementsCopy.userKeys = append(elementsCopy.userKeys, tt.elements.userKeys...)
			elementsCopy.data = append(elementsCopy.data, tt.elements.data...)
			elementsCopy.tags = append(elementsCopy.tags, tt.elements.tags...)

			// Initialize memPart from elements copy
			mp.mustInitFromElements(elementsCopy)

			// Step 2: Flush memPart to disk
			partDir := filepath.Join(tempDir, fmt.Sprintf("part_%s", tt.name))
			mp.mustFlush(testFS, partDir)

			// Step 3: Open the flushed part from disk
			part := mustOpenPart(partDir, testFS)
			defer part.close()

			// Step 4: Read all elements back from part
			resultElements, err := part.readAll()
			require.NoError(t, err, "readAll should not return error")

			// Step 5: Verify the result
			require.NotEmpty(t, resultElements, "should return at least one elements collection")

			// Combine all returned elements into a single collection for comparison
			combined := generateElements()
			defer releaseElements(combined)

			for _, elems := range resultElements {
				combined.seriesIDs = append(combined.seriesIDs, elems.seriesIDs...)
				combined.userKeys = append(combined.userKeys, elems.userKeys...)
				combined.data = append(combined.data, elems.data...)
				for _, tagSlice := range elems.tags {
					newTagSlice := make([]*tag, 0, len(tagSlice))
					for _, t := range tagSlice {
						newTag := generateTag()
						newTag.name = t.name
						newTag.value = append([]byte(nil), t.value...)
						newTag.valueType = t.valueType
						newTag.indexed = t.indexed
						newTagSlice = append(newTagSlice, newTag)
					}
					combined.tags = append(combined.tags, newTagSlice)
				}
			}

			// Create a clean copy of original elements for comparison (avoid sorting corruption)
			originalCopy := generateElements()
			defer releaseElements(originalCopy)

			originalCopy.seriesIDs = append(originalCopy.seriesIDs, tt.elements.seriesIDs...)
			originalCopy.userKeys = append(originalCopy.userKeys, tt.elements.userKeys...)
			originalCopy.data = append(originalCopy.data, tt.elements.data...)
			for _, tagSlice := range tt.elements.tags {
				newTagSlice := make([]*tag, 0, len(tagSlice))
				for _, t := range tagSlice {
					newTag := generateTag()
					newTag.name = t.name
					newTag.value = append([]byte(nil), t.value...)
					newTag.valueType = t.valueType
					newTag.indexed = t.indexed
					newTagSlice = append(newTagSlice, newTag)
				}
				originalCopy.tags = append(originalCopy.tags, newTagSlice)
			}

			// Sort both original copy and result for comparison
			sort.Sort(originalCopy)
			sort.Sort(combined)

			// Verify elements match
			compareElements(t, originalCopy, combined)

			// Clean up individual result elements
			for _, elems := range resultElements {
				releaseElements(elems)
			}
		})
	}
}

// testElement represents a single element for test creation.
type testElement struct {
	data     []byte
	tags     []tag
	seriesID common.SeriesID
	userKey  int64
}

// createTestElements creates an elements collection from test data.
func createTestElements(testElems []testElement) *elements {
	elems := generateElements()

	for _, te := range testElems {
		elems.seriesIDs = append(elems.seriesIDs, te.seriesID)
		elems.userKeys = append(elems.userKeys, te.userKey)

		// Copy data
		dataCopy := make([]byte, len(te.data))
		copy(dataCopy, te.data)
		elems.data = append(elems.data, dataCopy)

		// Copy tags using generateTag()
		tagsCopy := make([]*tag, 0, len(te.tags))
		for _, t := range te.tags {
			newTag := generateTag()
			newTag.name = t.name
			newTag.value = append([]byte(nil), t.value...)
			newTag.valueType = t.valueType
			newTag.indexed = t.indexed
			tagsCopy = append(tagsCopy, newTag)
		}
		elems.tags = append(elems.tags, tagsCopy)
	}

	return elems
}

// compareElements compares two elements collections for equality.
func compareElements(t *testing.T, expected, actual *elements) {
	require.Equal(t, len(expected.seriesIDs), len(actual.seriesIDs), "seriesIDs length mismatch")
	require.Equal(t, len(expected.userKeys), len(actual.userKeys), "userKeys length mismatch")
	require.Equal(t, len(expected.data), len(actual.data), "data length mismatch")
	require.Equal(t, len(expected.tags), len(actual.tags), "tags length mismatch")

	for i := 0; i < len(expected.seriesIDs); i++ {
		assert.Equal(t, expected.seriesIDs[i], actual.seriesIDs[i], "seriesID mismatch at index %d", i)
		assert.Equal(t, expected.userKeys[i], actual.userKeys[i], "userKey mismatch at index %d", i)
		assert.Equal(t, expected.data[i], actual.data[i], "data mismatch at index %d", i)

		// Compare tags
		require.Equal(t, len(expected.tags[i]), len(actual.tags[i]), "tags length mismatch at element %d", i)

		// Sort tags by name for consistent comparison
		expectedTags := make([]*tag, len(expected.tags[i]))
		actualTags := make([]*tag, len(actual.tags[i]))
		copy(expectedTags, expected.tags[i])
		copy(actualTags, actual.tags[i])

		sort.Slice(expectedTags, func(a, b int) bool {
			return expectedTags[a].name < expectedTags[b].name
		})
		sort.Slice(actualTags, func(a, b int) bool {
			return actualTags[a].name < actualTags[b].name
		})
		for i, tag := range expectedTags {
			fmt.Printf("expected tag %d: %s\n", i, tag.name)
			fmt.Printf("actual tag %d: %s\n", i, actualTags[i].name)
		}

		for j := range expectedTags {
			assert.Equal(t, expectedTags[j].name, actualTags[j].name,
				"tag name mismatch at element %d, tag %d", i, j)
			assert.Equal(t, expectedTags[j].value, actualTags[j].value,
				"tag value mismatch at element %d, tag %d (%s)", i, j, expectedTags[j].name)
			assert.Equal(t, expectedTags[j].valueType, actualTags[j].valueType,
				"tag valueType mismatch at element %d, tag %d (%s)", i, j, expectedTags[j].name)
			assert.Equal(t, expectedTags[j].indexed, actualTags[j].indexed,
				"tag indexed mismatch at element %d, tag %d (%s)", i, j, expectedTags[j].name)
		}
	}
}
