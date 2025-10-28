// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestMultiplePrimaryBlockFlushWithPartIter(t *testing.T) {
	// Create enough trace data to trigger multiple primary block flushes
	// maxUncompressedPrimaryBlockSize = 128 * 1024 bytes
	// We need to create enough blocks to exceed this size multiple times

	traces := generateLargeTraceSet()

	// Create a memPart and write all traces
	mp := &memPart{}
	mp.mustInitFromTraces(traces)

	// Flush to disk
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	epoch := uint64(1)
	path := partPath(tmpPath, epoch)
	fileSystem := fs.NewLocalFileSystem()
	mp.mustFlush(fileSystem, path)

	// Open the part
	part := mustOpenFilePart(epoch, tmpPath, fileSystem)
	defer part.close()

	// Verify we have multiple primary block metadata entries
	require.Greater(t, len(part.primaryBlockMetadata), 1,
		"Test requires multiple primary blocks to verify the bug")

	// Check that primary block metadata has correct trace IDs
	// BUG: All primary blocks will have the same traceID because bw.traceIDs is not cleared
	for i, pbm := range part.primaryBlockMetadata {
		t.Logf("Primary block %d: traceID=%s", i, pbm.traceID)
	}

	// Check if all primary blocks have the same traceID (this indicates the bug)
	allSame := true
	firstTraceIDInPrimary := part.primaryBlockMetadata[0].traceID
	for i := 1; i < len(part.primaryBlockMetadata); i++ {
		if part.primaryBlockMetadata[i].traceID != firstTraceIDInPrimary {
			allSame = false
			break
		}
	}
	if allSame {
		t.Logf("BUG DETECTED: All %d primary blocks have the same traceID: %s",
			len(part.primaryBlockMetadata), firstTraceIDInPrimary)
		t.Logf("This is because mustFlushPrimaryBlock uses bw.traceIDs[0] which is never reset")
		t.Logf("FIX: Add tidFirst field (like stream/block_writer.go) and reset it after flush")
	}

	// Now use partIter to search for traces
	// We'll search for trace IDs that should be in different primary blocks
	firstTraceID := traces.traceIDs[0]
	lastTraceID := traces.traceIDs[len(traces.traceIDs)-1]

	// Get unique trace IDs and sort them
	uniqueTraceIDs := make(map[string]bool)
	for _, tid := range traces.traceIDs {
		uniqueTraceIDs[tid] = true
	}
	sortedTraceIDs := make([]string, 0, len(uniqueTraceIDs))
	for tid := range uniqueTraceIDs {
		sortedTraceIDs = append(sortedTraceIDs, tid)
	}
	sort.Strings(sortedTraceIDs)

	t.Logf("First trace ID: %s", firstTraceID)
	t.Logf("Last trace ID: %s", lastTraceID)
	t.Logf("Total unique trace IDs: %d", len(sortedTraceIDs))

	// Test 1: Search for the first trace ID
	pi := &partIter{}
	bma := &blockMetadataArray{}
	pi.init(bma, part, []string{firstTraceID})

	foundFirst := false
	for pi.nextBlock() {
		if pi.curBlock.traceID == firstTraceID {
			foundFirst = true
			break
		}
	}
	assert.NoError(t, pi.error())
	assert.True(t, foundFirst, "Should find first trace ID: %s", firstTraceID)

	// Test 2: Search for the last trace ID
	// This will fail because of the bug: if the last trace ID is in a later primary block,
	// that primary block metadata will have the wrong traceID (the first one),
	// so partIter won't be able to locate it
	pi2 := &partIter{}
	bma2 := &blockMetadataArray{}
	pi2.init(bma2, part, []string{lastTraceID})

	foundLast := false
	for pi2.nextBlock() {
		if pi2.curBlock.traceID == lastTraceID {
			foundLast = true
			break
		}
	}
	assert.NoError(t, pi2.error())
	assert.True(t, foundLast, "Should find last trace ID: %s", lastTraceID)

	// Test 3: Search for a middle trace ID (if we have enough)
	if len(sortedTraceIDs) > 10 {
		middleTraceID := sortedTraceIDs[len(sortedTraceIDs)/2]
		t.Logf("Middle trace ID: %s", middleTraceID)

		pi3 := &partIter{}
		bma3 := &blockMetadataArray{}
		pi3.init(bma3, part, []string{middleTraceID})

		foundMiddle := false
		for pi3.nextBlock() {
			if pi3.curBlock.traceID == middleTraceID {
				foundMiddle = true
				break
			}
		}
		assert.NoError(t, pi3.error())
		assert.True(t, foundMiddle, "Should find middle trace ID: %s", middleTraceID)
	}

	// Test 4: Verify primary block metadata correctness
	// Each primary block metadata should contain the minimum traceID of the blocks it contains
	// With the bug, all primary blocks will have the same traceID (the first one)
	for i := 0; i < len(part.primaryBlockMetadata)-1; i++ {
		curr := part.primaryBlockMetadata[i]
		next := part.primaryBlockMetadata[i+1]
		assert.LessOrEqual(t, curr.traceID, next.traceID,
			"Primary block metadata should be in ascending order: block[%d].traceID=%s should be <= block[%d].traceID=%s",
			i, curr.traceID, i+1, next.traceID)
	}
}

func generateLargeTraceSet() *traces {
	// maxUncompressedPrimaryBlockSize = 128KB
	// Block metadata contains: traceID, timestamps, count, uncompressed size, and tag metadata
	// Each block metadata is roughly 200-500 bytes when marshaled (depending on tags)
	// To exceed 128KB multiple times, we need more blocks
	// Let's create 5000 blocks with more tags to ensure larger metadata

	numTraces := 5000
	traces := &traces{
		traceIDs:   make([]string, 0, numTraces),
		timestamps: make([]int64, 0, numTraces),
		tags:       make([][]*tagValue, 0, numTraces),
		spans:      make([][]byte, 0, numTraces),
		spanIDs:    make([]string, 0, numTraces),
	}

	for i := 0; i < numTraces; i++ {
		// Create trace IDs that are well distributed
		// Using a format that ensures good lexicographic distribution
		traceID := fmt.Sprintf("trace-%08d", i)
		spanData := []byte(fmt.Sprintf("span-data-%d-with-some-more-content-to-make-it-larger", i))
		spanID := fmt.Sprintf("span-%d", i)

		traces.traceIDs = append(traces.traceIDs, traceID)
		traces.timestamps = append(traces.timestamps, int64(i))
		traces.tags = append(traces.tags, []*tagValue{
			{tag: "strTag1", valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("value-%d", i))},
			{tag: "strTag2", valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("another-value-%d", i))},
			{tag: "strTag3", valueType: pbv1.ValueTypeStr, value: []byte(fmt.Sprintf("yet-another-value-%d", i))},
			{tag: "intTag1", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(i))},
			{tag: "intTag2", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(i * 2))},
			{tag: "intTag3", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(int64(i * 3))},
		})
		traces.spans = append(traces.spans, spanData)
		traces.spanIDs = append(traces.spanIDs, spanID)
	}

	return traces
}
