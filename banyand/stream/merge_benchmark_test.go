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

package stream

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	banyanfs "github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// BenchmarkMergeParts measures file-backed compaction, including output flush and sync.
// Fixture creation, full output validation, input integrity checks and cleanup are untimed.
// This benchmark does not depend on a particular merge optimization.
func BenchmarkMergeParts(b *testing.B) {
	for _, scenario := range []string{"full_disjoint", "small_disjoint", "full_overlap", "mixed"} {
		b.Run(scenario, func(b *testing.B) {
			b.StopTimer()
			root := b.TempDir()
			fileSystem := banyanfs.NewLocalFileSystem()
			inputs, expected := mergeBenchmarkFixture(b, fileSystem, root, scenario)
			defer func() {
				for _, input := range inputs {
					input.decRef()
				}
			}()
			inputHash, inputBytes := mergeBenchmarkInputDigest(b, inputs)
			var inputBlocks uint64
			for _, input := range inputs {
				inputBlocks += input.p.partMetadata.BlocksCount
			}
			table := &tsTable{pm: protector.Nop{}}
			closeCh := make(chan struct{})
			var outputBytes int64
			var outputBlocks uint64
			var outputDigest [sha256.Size]byte
			b.SetBytes(inputBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				outputID := uint64(100 + iteration)
				// Keep allocations from untimed validation out of the following merge's GC work.
				runtime.GC()
				b.StartTimer()
				merged, mergeErr := table.mergeParts(fileSystem, closeCh, inputs, outputID, root)
				b.StopTimer()
				if mergeErr != nil {
					b.Fatal(mergeErr)
				}
				// Reopen through the persisted reader instead of relying on writer-owned state.
				merged.decRef()
				output := mustOpenFilePart(outputID, root, fileSystem)
				outputBlocks = output.partMetadata.BlocksCount
				outputDigest = mergeBenchmarkVerify(b, output, expected)
				if scenario == "small_disjoint" && outputBlocks != mergeBenchmarkSeries {
					b.Fatalf("small blocks were not coalesced: got %d blocks, want %d", outputBlocks, mergeBenchmarkSeries)
				}
				output.close()
				_, outputBytes = mergeBenchmarkDirectoryDigest(b, partPath(root, outputID))
				if removeErr := os.RemoveAll(partPath(root, outputID)); removeErr != nil {
					b.Fatal(removeErr)
				}
			}
			afterHash, afterBytes := mergeBenchmarkInputDigest(b, inputs)
			if inputHash != afterHash || inputBytes != afterBytes {
				b.Fatal("merge modified its input files")
			}
			b.ReportMetric(float64(outputBytes), "output-B/op")
			b.ReportMetric(float64(outputBlocks), "blocks/op")
			b.ReportMetric(float64(len(expected)), "rows/op")
			b.Logf("input_sha256=%x output_rows_sha256=%x input_bytes=%d input_blocks=%d output_bytes=%d output_blocks=%d",
				inputHash, outputDigest, inputBytes, inputBlocks, outputBytes, outputBlocks)
		})
	}
}

const (
	mergeBenchmarkParts  = 4
	mergeBenchmarkSeries = 3
)

type mergeBenchmarkRow struct {
	series    common.SeriesID
	timestamp int64
}

func mergeBenchmarkFixture(b *testing.B, fileSystem banyanfs.FileSystem, root, scenario string) ([]*partWrapper, []mergeBenchmarkRow) {
	b.Helper()
	fullRows := mergeBenchmarkFullRows()
	var inputs []*partWrapper
	var expected []mergeBenchmarkRow
	for partIndex := 0; partIndex < mergeBenchmarkParts; partIndex++ {
		partID := uint64(partIndex + 1)
		writer := generateBlockWriter()
		writer.mustInitForFilePart(fileSystem, partPath(root, partID), true)
		for series := common.SeriesID(1); series <= mergeBenchmarkSeries; series++ {
			rows, start, step := fullRows, int64(partIndex*fullRows+1), int64(1)
			switch scenario {
			case "small_disjoint":
				rows = fullRows / 8
			case "full_overlap":
				start, step = int64(partIndex+1), mergeBenchmarkParts
			case "mixed":
				switch partIndex {
				case 1, 2:
					start, step = int64(2*fullRows+partIndex), 2
				case 3:
					rows, start = fullRows/8, int64(5*fullRows+1)
				}
			}
			data := mergeBenchmarkBlock(rows, start, step)
			writer.mustWriteBlock(series, &data)
			for _, timestamp := range data.timestamps {
				expected = append(expected, mergeBenchmarkRow{series, timestamp})
			}
		}
		metadata := &partMetadata{}
		types := make(tagType)
		writer.Flush(metadata, &types)
		releaseBlockWriter(writer)
		types.mustWriteTagType(fileSystem, partPath(root, partID))
		metadata.mustWriteMetadata(fileSystem, partPath(root, partID))
		input := newPartWrapper(nil, mustOpenFilePart(partID, root, fileSystem))
		inputs = append(inputs, input)
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].series != expected[j].series {
			return expected[i].series < expected[j].series
		}
		return expected[i].timestamp < expected[j].timestamp
	})
	return inputs, expected
}

func mergeBenchmarkText(timestamp int64, size int) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("%016x", uint64(timestamp)*0x9e3779b97f4a7c15)), size/16)
}

func mergeBenchmarkArray(timestamp int64) []byte {
	result := marshalVarArray(nil, []byte("GET|200"))
	result = marshalVarArray(result, []byte(`zone\us`))
	return marshalVarArray(result, []byte(fmt.Sprintf("%016x", timestamp)))
}

func mergeBenchmarkInputDigest(b *testing.B, inputs []*partWrapper) ([sha256.Size]byte, int64) {
	b.Helper()
	digest := sha256.New()
	var size int64
	for _, input := range inputs {
		partDigest, partBytes := mergeBenchmarkDirectoryDigest(b, input.p.path)
		_, _ = digest.Write(partDigest[:])
		size += partBytes
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, size
}

func mergeBenchmarkDirectoryDigest(b *testing.B, root string) ([sha256.Size]byte, int64) {
	b.Helper()
	digest := sha256.New()
	var size int64
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, pathErr error) error {
		if pathErr != nil {
			return pathErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, relativeErr := filepath.Rel(root, path)
		if relativeErr != nil {
			return relativeErr
		}
		mergeBenchmarkHashValue(digest, []byte(relative))
		file, openErr := os.Open(path)
		if openErr != nil {
			return openErr
		}
		copied, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		size += copied
		return nil
	})
	if walkErr != nil {
		b.Fatal(walkErr)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, size
}

func mergeBenchmarkHashValue(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func mergeBenchmarkVerify(b *testing.B, output *part, expected []mergeBenchmarkRow) [sha256.Size]byte {
	b.Helper()
	iterator := &partMergeIter{}
	iterator.mustInitFromPart(output)
	defer iterator.reset()
	reader := &blockReader{}
	reader.init([]*partMergeIter{iterator})
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)
	type checkedRow struct {
		key    mergeBenchmarkRow
		digest [sha256.Size]byte
	}
	actual := make([]checkedRow, 0, len(expected))
	var blocks, uncompressed uint64
	var previousSeries common.SeriesID
	var previousMin int64
	for reader.nextBlockMetadata() {
		reader.loadBlockData(decoder)
		data := reader.block
		blocks++
		uncompressed += data.bm.uncompressedSizeBytes
		if data.bm.count != uint64(len(data.timestamps)) || data.bm.timestamps.min != data.timestamps[0] ||
			data.bm.timestamps.max != data.timestamps[len(data.timestamps)-1] || data.bm.uncompressedSizeBytes != data.uncompressedSizeBytes() {
			b.Fatal("block metadata disagrees with decoded values")
		}
		if data.bm.seriesID < previousSeries || (data.bm.seriesID == previousSeries && data.bm.timestamps.min < previousMin) {
			b.Fatal("block minima are not ordered")
		}
		previousSeries, previousMin = data.bm.seriesID, data.bm.timestamps.min
		for row, timestamp := range data.timestamps {
			if row > 0 && timestamp <= data.timestamps[row-1] {
				b.Fatal("rows within a block are not ordered")
			}
			rowDigest := sha256.New()
			mergeBenchmarkHashValue(rowDigest, convert.Int64ToBytes(int64(data.bm.seriesID)))
			mergeBenchmarkHashValue(rowDigest, convert.Int64ToBytes(timestamp))
			mergeBenchmarkVerifyValues(b, rowDigest, &data.block, row, timestamp)
			item := checkedRow{key: mergeBenchmarkRow{data.bm.seriesID, timestamp}}
			copy(item.digest[:], rowDigest.Sum(nil))
			actual = append(actual, item)
		}
		decoder.Reset()
	}
	if readErr := reader.error(); readErr != nil {
		b.Fatal(readErr)
	}
	if len(actual) != len(expected) || output.partMetadata.TotalCount != uint64(len(actual)) || output.partMetadata.BlocksCount != blocks {
		b.Fatalf("wrong output counts: read %d/%d rows and %d/%d blocks", len(actual), len(expected), blocks, output.partMetadata.BlocksCount)
	}
	if output.partMetadata.UncompressedSizeBytes != uncompressed || output.partMetadata.MinTimestamp != expected[0].timestamp ||
		output.partMetadata.MaxTimestamp != expected[len(expected)-1].timestamp {
		b.Fatal("part metadata disagrees with decoded blocks")
	}
	// The existing merger orders block minima; different output blocks may still overlap.
	// Compare logical rows independently of those physical block boundaries.
	sort.Slice(actual, func(i, j int) bool {
		if actual[i].key.series != actual[j].key.series {
			return actual[i].key.series < actual[j].key.series
		}
		return actual[i].key.timestamp < actual[j].key.timestamp
	})
	digest := sha256.New()
	for index, row := range actual {
		if row.key != expected[index] {
			b.Fatalf("unexpected logical row %d: got %+v, want %+v", index, row.key, expected[index])
		}
		_, _ = digest.Write(row.digest[:])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func mergeBenchmarkFullRows() int {
	sample := mergeBenchmarkBlock(1, 1, 1)
	second := mergeBenchmarkBlock(2, 1, 1)
	rowSize := second.uncompressedSizeBytes() - sample.uncompressedSizeBytes()
	overhead := sample.uncompressedSizeBytes() - rowSize
	return int((maxUncompressedBlockSize - overhead + rowSize - 1) / rowSize)
}

func mergeBenchmarkBlock(rows int, start, step int64) block {
	data := block{tagFamilies: []tagFamily{{name: "tags", tags: []tag{
		{name: "number", valueType: pbv1.ValueTypeInt64, uniqueValues: make(map[string]struct{})},
		{name: "text", valueType: pbv1.ValueTypeStr, uniqueValues: make(map[string]struct{})},
		{name: "array", valueType: pbv1.ValueTypeStrArr, uniqueValues: make(map[string]struct{})},
	}}}}
	for row := 0; row < rows; row++ {
		timestamp := start + int64(row)*step
		data.timestamps = append(data.timestamps, timestamp)
		data.elementIDs = append(data.elementIDs, uint64(timestamp*13+7))
		tags := data.tagFamilies[0].tags
		tags[0].values = append(tags[0].values, convert.Int64ToBytes(timestamp*17))
		tags[1].values = append(tags[1].values, mergeBenchmarkText(timestamp, 512))
		tags[2].values = append(tags[2].values, mergeBenchmarkArray(timestamp))
		tags[0].uniqueValues[string(tags[0].values[row])] = struct{}{}
		tags[1].uniqueValues[string(tags[1].values[row])] = struct{}{}
		tags[2].uniqueValues["GET|200"] = struct{}{}
		tags[2].uniqueValues[`zone\us`] = struct{}{}
		tags[2].uniqueValues[fmt.Sprintf("%016x", timestamp)] = struct{}{}
		if row == 0 {
			tags[0].min = tags[0].values[row]
		}
		tags[0].max = tags[0].values[row]
	}
	return data
}

func mergeBenchmarkVerifyValues(b *testing.B, digest hash.Hash, data *block, row int, timestamp int64) {
	b.Helper()
	if data.elementIDs[row] != uint64(timestamp*13+7) || len(data.tagFamilies) != 1 || len(data.tagFamilies[0].tags) != 3 {
		b.Fatal("element ID or tag schema changed")
	}
	mergeBenchmarkHashValue(digest, convert.Int64ToBytes(int64(data.elementIDs[row])))
	for _, tag := range data.tagFamilies[0].tags {
		var expected []byte
		var expectedType pbv1.ValueType
		switch tag.name {
		case "number":
			expected, expectedType = convert.Int64ToBytes(timestamp*17), pbv1.ValueTypeInt64
		case "text":
			expected, expectedType = mergeBenchmarkText(timestamp, 512), pbv1.ValueTypeStr
		case "array":
			expected, expectedType = mergeBenchmarkArray(timestamp), pbv1.ValueTypeStrArr
		default:
			b.Fatalf("unexpected tag %q", tag.name)
		}
		if tag.valueType != expectedType || !bytes.Equal(tag.values[row], expected) {
			b.Fatalf("wrong %s at timestamp %d", tag.name, timestamp)
		}
		mergeBenchmarkHashValue(digest, []byte(tag.name))
		mergeBenchmarkHashValue(digest, tag.values[row])
	}
}
