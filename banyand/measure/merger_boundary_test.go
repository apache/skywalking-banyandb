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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestMergeFullBlocksWithEqualBoundary(t *testing.T) {
	tests := []struct {
		name          string
		firstVersion  int64
		secondVersion int64
	}{
		{name: "newer version in first block", firstVersion: 2, secondVersion: 1},
		{name: "newer version in second block", firstVersion: 1, secondVersion: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			fileSystem := fs.NewLocalFileSystem()
			var inputs []*partWrapper
			defer func() {
				for _, input := range inputs {
					input.decRef()
				}
			}()
			for partIndex, version := range []int64{tt.firstVersion, tt.secondVersion} {
				points := &dataPoints{}
				// Both full blocks contain maxBlockLength as their shared boundary timestamp.
				start := int64(partIndex*(maxBlockLength-1) + 1)
				for row := 0; row < maxBlockLength; row++ {
					timestamp := start + int64(row)
					points.seriesIDs = append(points.seriesIDs, 1)
					points.timestamps = append(points.timestamps, timestamp)
					points.versions = append(points.versions, version)
					points.tagFamilies = append(points.tagFamilies, nil)
					points.fields = append(points.fields, nameValues{values: []*nameValue{
						{name: "number", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(timestamp * version)},
					}})
				}
				partID := uint64(partIndex + 1)
				memoryPart := generateMemPart()
				memoryPart.mustInitFromDataPoints(points)
				memoryPart.mustFlush(fileSystem, partPath(root, partID))
				releaseMemPart(memoryPart)
				input := newPartWrapper(nil, mustOpenFilePart(partID, root, fileSystem))
				inputs = append(inputs, input)
				require.Equal(t, uint64(1), input.p.partMetadata.BlocksCount)
				require.Equal(t, uint64(maxBlockLength), input.p.partMetadata.TotalCount)
			}

			table := &tsTable{pm: protector.Nop{}}
			merged, err := table.mergeParts(fileSystem, make(chan struct{}), inputs, 3, root)
			require.NoError(t, err)
			defer merged.decRef()
			count := 0
			walkMergeTestPart(t, merged.p, func(b *blockPointer) {
				require.LessOrEqual(t, len(b.timestamps), maxBlockLength)
				require.Len(t, b.field.columns, 1)
				require.Equal(t, "number", b.field.columns[0].name)
				for row, timestamp := range b.timestamps {
					count++
					require.Equal(t, int64(count), timestamp, "timestamps must be unique and ordered")
					wantVersion := tt.firstVersion
					if timestamp > maxBlockLength {
						wantVersion = tt.secondVersion
					} else if timestamp == maxBlockLength {
						wantVersion = max(tt.firstVersion, tt.secondVersion)
					}
					require.Equal(t, wantVersion, b.versions[row], "version at timestamp %d", timestamp)
					require.Equal(t, convert.Int64ToBytes(timestamp*wantVersion), b.field.columns[0].values[row],
						"field value at timestamp %d", timestamp)
				}
			})
			require.Equal(t, 2*maxBlockLength-1, count)
			require.Equal(t, uint64(count), merged.p.partMetadata.TotalCount)
		})
	}
}
