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

package measure

import (
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func Test_blockReader_nextBlock(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		dpsList []*dataPoints
		want    []blockMetadata
	}{
		{
			name:    "Test with no data points",
			dpsList: []*dataPoints{},
		},
		{
			name:    "Test with single part",
			dpsList: []*dataPoints{dpsTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1676},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 24},
			},
		},
		{
			name:    "Test with multiple parts with different ts",
			dpsList: []*dataPoints{dpsTS1, dpsTS2},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1676},
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1676},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 24},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 24},
			},
		},
		{
			name:    "Test with multiple parts with same ts",
			dpsList: []*dataPoints{dpsTS1, dpsTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1676},
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1676},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 55},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 24},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 24},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(pp []*part) {
				var pii []*partMergeIter
				for _, p := range pp {
					pmi := &partMergeIter{}
					pmi.mustInitFromPart(p)
					pii = append(pii, pmi)
				}
				reader := &blockReader{}
				reader.init(pii)
				var got []blockMetadata
				for reader.nextBlock() {
					if reader.block.bm.seriesID == 0 {
						t.Errorf("Expected curBlock to be initialized, but it was nil")
					}
					var tagFamilyNames []string
					for _, cf := range reader.block.block.tagFamilies {
						tagFamilyNames = append(tagFamilyNames, cf.name)
					}
					if !sort.IsSorted(sort.StringSlice(tagFamilyNames)) {
						t.Errorf("Expected tagFamilyNames %q to be sorted, but it was not", tagFamilyNames)
					}
					got = append(got, reader.block.bm)
				}

				if !errors.Is(reader.error(), tt.wantErr) {
					t.Errorf("Unexpected error: got %v, want %v", reader.err, tt.wantErr)
				}

				if diff := cmp.Diff(got, tt.want,
					cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
					cmpopts.IgnoreFields(blockMetadata{}, "field"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
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
				for _, dps := range tt.dpsList {
					mp := generateMemPart()
					mpp = append(mpp, mp)
					mp.mustInitFromDataPoints(dps)
					pp = append(pp, openMemPart(mp))
				}
				verify(pp)
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
				for i, dps := range tt.dpsList {
					mp := generateMemPart()
					mpp = append(mpp, mp)
					mp.mustInitFromDataPoints(dps)
					mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
					filePW := newPartWrapper(nil, mustOpenFilePart(uint64(i), tmpPath, fileSystem))
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					pp = append(pp, filePW.p)
				}
				verify(pp)
			})
		})
	}
}
