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

package sidx

import (
	"errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var (
	elementsSet1 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 100, make([]byte, 1600), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(2, 200, make([]byte, 40), []Tag{
			{Name: "env", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(3, 300, make([]byte, 25), []Tag{
			{Name: "region", Value: []byte("us"), ValueType: pbv1.ValueTypeStr},
		})
		return es
	}()

	elementsSet2 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 150, make([]byte, 1600), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(2, 250, make([]byte, 40), []Tag{
			{Name: "env", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(3, 350, make([]byte, 25), []Tag{
			{Name: "region", Value: []byte("us"), ValueType: pbv1.ValueTypeStr},
		})
		return es
	}()

	sameKeyElements = func() *elements {
		es := generateElements()
		es.mustAppend(1, 100, make([]byte, 25), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(1, 100, make([]byte, 35), []Tag{
			{Name: "service", Value: []byte("service2"), ValueType: pbv1.ValueTypeStr},
		})
		return es
	}()
)

func Test_blockReader_nextBlock(t *testing.T) {
	tests := []struct {
		wantErr   error
		name      string
		elemsList []*elements
		want      []blockMetadata
	}{
		{
			name:      "Test with no elements",
			elemsList: []*elements{},
		},
		{
			name:      "Test with single part",
			elemsList: []*elements{elementsSet1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSize: 1623},
				{seriesID: 2, count: 1, uncompressedSize: 55},
				{seriesID: 3, count: 1, uncompressedSize: 41},
			},
		},
		{
			name:      "Test with multiple parts with different keys",
			elemsList: []*elements{elementsSet1, elementsSet2},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSize: 1623},
				{seriesID: 1, count: 1, uncompressedSize: 1623},
				{seriesID: 2, count: 1, uncompressedSize: 55},
				{seriesID: 2, count: 1, uncompressedSize: 55},
				{seriesID: 3, count: 1, uncompressedSize: 41},
				{seriesID: 3, count: 1, uncompressedSize: 41},
			},
		},
		{
			name:      "Test with single part with same key",
			elemsList: []*elements{sameKeyElements},
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSize: 106},
			},
		},
		{
			name:      "Test with multiple parts with same keys",
			elemsList: []*elements{elementsSet1, elementsSet1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSize: 1623},
				{seriesID: 1, count: 1, uncompressedSize: 1623},
				{seriesID: 2, count: 1, uncompressedSize: 55},
				{seriesID: 2, count: 1, uncompressedSize: 55},
				{seriesID: 3, count: 1, uncompressedSize: 41},
				{seriesID: 3, count: 1, uncompressedSize: 41},
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
					if reader.block.bm.seriesID == 0 {
						t.Errorf("Expected curBlock to be initialized, but it was nil")
					}
					var tagNames []string
					for tagName := range reader.block.bm.tagsBlocks {
						tagNames = append(tagNames, tagName)
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
					cmpopts.IgnoreFields(blockMetadata{}, "tagsBlocks"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagProjection"),
					cmpopts.IgnoreFields(blockMetadata{}, "dataBlock"),
					cmpopts.IgnoreFields(blockMetadata{}, "keysBlock"),
					cmpopts.IgnoreFields(blockMetadata{}, "minKey"),
					cmpopts.IgnoreFields(blockMetadata{}, "maxKey"),
					cmpopts.IgnoreFields(blockMetadata{}, "keysEncodeType"),
					cmp.AllowUnexported(blockMetadata{}),
				); diff != "" {
					t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
				}
			}

			t.Run("memory parts", func(t *testing.T) {
				var mpp []*memPart
				defer func() {
					for _, mp := range mpp {
						ReleaseMemPart(mp)
					}
				}()
				var pp []*part
				for _, elems := range tt.elemsList {
					mp := GenerateMemPart()
					mpp = append(mpp, mp)
					mp.mustInitFromElements(elems)
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
						ReleaseMemPart(mp)
					}
					for _, pw := range fpp {
						pw.release()
					}
					defFn()
				}()
				var pp []*part
				fileSystem := fs.NewLocalFileSystem()
				for i, elems := range tt.elemsList {
					mp := GenerateMemPart()
					mpp = append(mpp, mp)
					mp.mustInitFromElements(elems)
					mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
					filePW := newPartWrapper(nil, mustOpenPart(partPath(tmpPath, uint64(i)), fileSystem))
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					pp = append(pp, filePW.p)
				}
				verify(t, pp)
			})
		})
	}
}
