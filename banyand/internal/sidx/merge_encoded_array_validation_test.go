// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package sidx

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const mergeArrayNeedle = `route=/checkout|details\v2`

type mergedArrayRow struct {
	query    []string
	numbers  []int64
	optional []string
	key      int64
	series   common.SeriesID
}

func makeArrayMergeParts(t *testing.T, fileSystem fs.FileSystem, root string, arrayVariants int) []*partWrapper {
	t.Helper()
	var parts []*partWrapper
	for partNumber := range 3 {
		elements := generateElements()
		for series := common.SeriesID(1); series <= 2; series++ {
			for rowNumber := range 768 {
				sequence := rowNumber*3 + partNumber
				var values [][]byte
				switch sequence % 13 {
				case 0:
				case 1:
					values = [][]byte{}
				case 2:
					values = [][]byte{[]byte("")}
				default:
					values = [][]byte{[]byte(mergeArrayNeedle), []byte(""), []byte(`trailing\`), []byte("|leading")}
					for attribute := range 40 {
						values = append(values, []byte(fmt.Sprintf("attribute.%02d=%032d", attribute, sequence%arrayVariants)))
					}
				}
				tags := []Tag{
					{Name: "query", ValueArr: values, ValueType: pbv1.ValueTypeStrArr},
					{
						Name: "numbers", ValueArr: [][]byte{convert.Int64ToBytes(-int64(sequence)), convert.Int64ToBytes(int64(sequence))},
						ValueType: pbv1.ValueTypeInt64Arr,
					},
				}
				if partNumber == 0 {
					tags = append(tags, Tag{Name: "optional", ValueArr: [][]byte{[]byte("present|only\\here")}, ValueType: pbv1.ValueTypeStrArr})
				}
				payload := fmt.Sprintf("%d/%04d", series, sequence)
				// Adjacent rows share a key, but retain different payloads and array values.
				elements.mustAppend(series, int64(sequence/2), []byte(payload), tags)
			}
		}
		memoryPart := GenerateMemPart()
		memoryPart.mustInitFromElements(elements)
		minimum, maximum := int64(partNumber), int64(100+partNumber)
		memoryPart.partMetadata.MinTimestamp = &minimum
		memoryPart.partMetadata.MaxTimestamp = &maximum
		partID := uint64(partNumber + 1)
		memoryPart.mustFlush(fileSystem, partPath(root, partID))
		ReleaseMemPart(memoryPart)
		releaseElements(elements)
		wrapper := newPartWrapper(nil, mustOpenPart(partID, partPath(root, partID), fileSystem))
		t.Cleanup(wrapper.release)
		parts = append(parts, wrapper)
	}
	return parts
}

func scanMergedArrayRows(t *testing.T, fileSystem fs.FileSystem, root string, partIDs []uint64) map[string]mergedArrayRow {
	t.Helper()
	rows := make(map[string]mergedArrayRow)
	require.NoError(t, ScanRawParts(context.Background(), fileSystem, root, partIDs, func(row RawRow) error {
		value := mergedArrayRow{key: row.Key, series: row.SeriesID}
		for _, tag := range row.Tags {
			for _, item := range tag.ValueArr {
				switch tag.Name {
				case "query":
					value.query = append(value.query, string(item))
				case "numbers":
					value.numbers = append(value.numbers, convert.BytesToInt64(item))
				case "optional":
					value.optional = append(value.optional, string(item))
				}
			}
		}
		id := string(row.Data)
		require.NotContains(t, rows, id)
		rows[id] = value
		return nil
	}))
	return rows
}

type arrayMergeBlockFilter struct {
	mockBlockFilter
	value string
}

func (filter *arrayMergeBlockFilter) ShouldSkip(op index.FilterOp) (bool, error) {
	return !op.Having("query", []string{filter.value}), nil
}

func assertMergedArrayQuery(t *testing.T, parts []*partWrapper, expected map[string]mergedArrayRow) {
	t.Helper()
	storage := &sidx{pm: protector.Nop{}, l: logger.GetLogger(), snapshot: newSnapshot(parts)}
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	for _, needle := range []string{mergeArrayNeedle, `absent|array\member`} {
		matcher := &mockTagFilterMatcher{
			decoder: func(valueType pbv1.ValueType, _ []byte, values [][]byte) *modelv1.TagValue {
				if valueType != pbv1.ValueTypeStrArr {
					return pbv1.NullTagValue
				}
				stringsValue := make([]string, len(values))
				for idx, value := range values {
					stringsValue[idx] = string(value)
				}
				return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: stringsValue}}}
			},
			matchFunc: func(tags []*modelv1.Tag) (bool, error) {
				for _, tag := range tags {
					if tag.Key == "query" && slices.Contains(tag.Value.GetStrArray().GetValue(), needle) {
						return true, nil
					}
				}
				return false, nil
			},
		}
		responses, queryErr := storage.QuerySync(context.Background(), QueryRequest{
			SeriesIDs: []common.SeriesID{1, 2}, Filter: &arrayMergeBlockFilter{value: needle}, TagFilter: matcher, FilterTagNames: []string{"query"},
		})
		require.NoError(t, queryErr)
		want := make(map[string]int64)
		for id, row := range expected {
			if slices.Contains(row.query, needle) {
				want[id] = row.key
			}
		}
		got := make(map[string]int64)
		for _, response := range responses {
			require.NoError(t, response.Error)
			for idx, data := range response.Data {
				got[string(data)] = response.Keys[idx]
			}
		}
		if needle == mergeArrayNeedle {
			require.NotEmpty(t, want)
		}
		require.Equal(t, want, got)
	}
}

func TestMergeArraysPreservesValuesAndFilters(t *testing.T) {
	for _, arrayVariants := range []int{19, 521} {
		t.Run(fmt.Sprintf("distinct_arrays_%d", arrayVariants), func(t *testing.T) {
			fileSystem, root := fs.NewLocalFileSystem(), t.TempDir()
			parts := makeArrayMergeParts(t, fileSystem, root, arrayVariants)
			expected := scanMergedArrayRows(t, fileSystem, root, []uint64{1, 2, 3})
			assertMergedArrayQuery(t, parts, expected)
			keep := func(data []byte) bool { return !strings.HasSuffix(string(data), "7") }
			for id := range expected {
				if !keep([]byte(id)) {
					delete(expected, id)
				}
			}
			for _, outputID := range []uint64{10, 11} {
				merged, mergeErr := (&sidx{pm: protector.Nop{}}).mergeParts(fileSystem, nil, parts, outputID, root, keep)
				require.NoError(t, mergeErr)
				t.Cleanup(merged.release)
				require.Equal(t, uint64(len(expected)), merged.p.partMetadata.TotalCount)
				require.Equal(t, int64(0), merged.p.partMetadata.MinKey)
				require.Equal(t, int64(1151), merged.p.partMetadata.MaxKey)
				require.Equal(t, int64(0), *merged.p.partMetadata.MinTimestamp)
				require.Equal(t, int64(102), *merged.p.partMetadata.MaxTimestamp)
				require.Greater(t, merged.p.partMetadata.BlocksCount, uint64(2))
				require.Equal(t, expected, scanMergedArrayRows(t, fileSystem, root, []uint64{outputID}))
				assertMergedArrayQuery(t, []*partWrapper{merged}, expected)
				parts = []*partWrapper{merged}
			}
		})
	}
}

func TestMergeArraysCancellationPreservesInputs(t *testing.T) {
	fileSystem, root := fs.NewLocalFileSystem(), t.TempDir()
	parts := makeArrayMergeParts(t, fileSystem, root, 521)
	expected := scanMergedArrayRows(t, fileSystem, root, []uint64{1, 2, 3})
	closed := make(chan struct{})
	visited := 0
	keep := func([]byte) bool {
		visited++
		if visited == 2400 {
			close(closed)
		}
		return true
	}
	merger := &sidx{pm: protector.Nop{}}
	merged, mergeErr := merger.mergeParts(fileSystem, closed, parts, 10, root, keep)
	require.ErrorIs(t, mergeErr, errClosed)
	require.Nil(t, merged)
	_, statErr := os.Stat(partPath(root, 10))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	require.Equal(t, expected, scanMergedArrayRows(t, fileSystem, root, []uint64{1, 2, 3}))
	merged, mergeErr = merger.mergeParts(fileSystem, nil, parts, 11, root, nil)
	require.NoError(t, mergeErr)
	t.Cleanup(merged.release)
	require.Equal(t, expected, scanMergedArrayRows(t, fileSystem, root, []uint64{11}))
}
