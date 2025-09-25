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
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func Test_blockReader_nextBlock(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		tsList  []*traces
		want    []blockMetadata
	}{
		{
			name:   "Test with no data points",
			tsList: []*traces{},
		},
		{
			name:   "Test with single part",
			tsList: []*traces{tsTS1},
			want: []blockMetadata{
				{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
		{
			name:   "Test with multiple parts",
			tsList: []*traces{tsTS1, tsTS2},
			want: []blockMetadata{
				{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace1", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace2", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
				{traceID: "trace3", count: 1, uncompressedSpanSizeBytes: 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, pp []*part) {
				var pii []*partMergeIter
				for _, p := range pp {
					pmi := &partMergeIter{}
					pmi.mustInitFromPart(p)
					pii = append(pii, pmi)
				}
				reader := generateBlockReader()
				defer releaseBlockReader(reader)
				reader.init(pii)
				var got []blockMetadata
				for reader.nextBlockMetadata() {
					if reader.block.bm.traceID == "" {
						t.Errorf("Expected curBlock to be initialized, but it was nil")
					}
					var tagNames []string
					for _, cf := range reader.block.block.tags {
						tagNames = append(tagNames, cf.name)
					}
					if !sort.IsSorted(sort.StringSlice(tagNames)) {
						t.Errorf("Expected tagNames %q to be sorted, but it was not", tagNames)
					}
					got = append(got, reader.block.bm)
				}

				if !errors.Is(reader.error(), tt.wantErr) {
					t.Errorf("Unexpected error: got %v, want %v", reader.err, tt.wantErr)
				}

				if diff := cmp.Diff(got, tt.want,
					cmpopts.IgnoreFields(blockMetadata{}, "spans"),
					cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
					cmpopts.IgnoreFields(blockMetadata{}, "traceID"),
					cmpopts.IgnoreFields(blockMetadata{}, "tags"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
					cmp.AllowUnexported(blockMetadata{}),
				); diff != "" {
					t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
				}
			}

			t.Run("memory parts", func(t *testing.T) {
				var mpp []*memPart
				defer func() {
					for _, mp := range mpp {
						releaseMemPart(mp)
					}
				}()
				var pp []*part
				for _, ts := range tt.tsList {
					mp := generateMemPart()
					mpp = append(mpp, mp)
					mp.mustInitFromTraces(ts)
					pp = append(pp, openMemPart(mp))
				}
				verify(t, pp)
			})

			t.Run("file parts", func(t *testing.T) {
				var mpp []*memPart
				var fpp []*partWrapper
				tmpPath, defFn := test.Space(require.New(t))
				defer func() {
					for _, mp := range mpp {
						releaseMemPart(mp)
					}
					for _, pw := range fpp {
						pw.decRef()
					}
					defFn()
				}()
				var pp []*part
				fileSystem := fs.NewLocalFileSystem()
				for i, ts := range tt.tsList {
					mp := generateMemPart()
					mpp = append(mpp, mp)
					mp.mustInitFromTraces(ts)
					mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
					filePW := newPartWrapper(nil, mustOpenFilePart(uint64(i), tmpPath, fileSystem))
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					pp = append(pp, filePW.p)
				}
				verify(t, pp)
			})
		})
	}
}

func Test_blockReader_TagTypePerPart(t *testing.T) {
	// Build two file parts for the same traceID but with disjoint tag sets and types.
	// The reader should use each part's own tagType when reading its blocks, so the
	// decoded tag names must match the keys present in bm.tags for that block.

	// Helper to build traces with a single span and one tag.
	buildTraces := func(traceID string, ts int64, tagName string, vt pbv1.ValueType, val []byte) *traces {
		tv := &tagValue{tag: tagName, valueType: vt, value: val}
		tr := &traces{
			traceIDs:   []string{traceID},
			timestamps: []int64{ts},
			tags:       [][]*tagValue{{tv}},
			spans:      [][]byte{[]byte("span")},
		}
		return tr
	}

	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	// Part 1: tagA as string
	mp1 := generateMemPart()
	defer releaseMemPart(mp1)
	ts1 := buildTraces("trace-1", 1, "tagA", pbv1.ValueTypeStr, []byte("v1"))
	mp1.mustInitFromTraces(ts1)
	mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
	pw1 := newPartWrapper(nil, mustOpenFilePart(1, tmpPath, fileSystem))
	defer pw1.decRef()

	// Part 2: tagB as int64
	mp2 := generateMemPart()
	defer releaseMemPart(mp2)
	ts2 := buildTraces("trace-1", 2, "tagB", pbv1.ValueTypeInt64, convert.Int64ToBytes(123))
	mp2.mustInitFromTraces(ts2)
	mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
	pw2 := newPartWrapper(nil, mustOpenFilePart(2, tmpPath, fileSystem))
	defer pw2.decRef()

	// Initialize block reader over both parts
	pmi1 := &partMergeIter{}
	pmi1.mustInitFromPart(pw1.p)
	pmi2 := &partMergeIter{}
	pmi2.mustInitFromPart(pw2.p)

	br := generateBlockReader()
	defer releaseBlockReader(br)
	br.init([]*partMergeIter{pmi1, pmi2})

	dec := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(dec)

	var seen int
	for br.nextBlockMetadata() {
		// Load block data for current metadata
		br.loadBlockData(dec)

		// Collect expected tag names from bm.tags keys (sorted for stability)
		var expected []string
		for name := range br.block.bm.tags {
			expected = append(expected, name)
		}
		sort.Strings(expected)

		// Collect actual decoded tag names from block
		var actual []string
		for _, t := range br.block.block.tags {
			actual = append(actual, t.name)
		}
		sort.Strings(actual)

		require.Equal(t, expected, actual, "decoded tag names must match bm.tags keys for each block")
		seen++
	}

	// We should have seen two blocks (one from each part)
	require.Equal(t, 2, seen)
}
