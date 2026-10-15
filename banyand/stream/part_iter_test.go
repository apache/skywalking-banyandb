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

package stream

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func Test_partIter_nextBlock(t *testing.T) {
	tests := []struct {
		wantErr error
		opt     *queryOptions
		name    string
		sids    []common.SeriesID
		want    []blockMetadata
	}{
		{
			name: "Test with all seriesIDs",
			sids: []common.SeriesID{1, 2, 3},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want: []blockMetadata{
				{seriesID: 1, count: 2}, {seriesID: 2, count: 2}, {seriesID: 3, count: 2},
			},
		},
		{
			name: "Test with no seriesIDs",
			sids: []common.SeriesID{},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want: nil,
		},
		{
			name: "Test with a single seriesID",
			sids: []common.SeriesID{1},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want: []blockMetadata{{seriesID: 1, count: 2}},
		},
		{
			name: "Test with non-sequential seriesIDs",
			sids: []common.SeriesID{1, 3},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want: []blockMetadata{{seriesID: 1, count: 2}, {seriesID: 3, count: 2}},
		},
		{
			name: "Test with seriesID not in data",
			sids: []common.SeriesID{4},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "Test with multiple seriesIDs not in data",
			sids: []common.SeriesID{4, 5, 6},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "Test with some seriesIDs in data and some not",
			sids: []common.SeriesID{1, 4},
			opt: &queryOptions{
				minTimestamp: 1,
				maxTimestamp: 220,
			},
			want:    []blockMetadata{{seriesID: 1, count: 2}},
			wantErr: nil,
		},
	}
	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifyPart := func(p *part) {
				defer p.close()
				pi := partIter{}
				pi.init(bma, p, tt.sids, tt.opt.minTimestamp, tt.opt.maxTimestamp, nil)

				var got []blockMetadata
				for pi.nextBlock() {
					if pi.curBlock.seriesID == 0 {
						t.Errorf("Expected currBlock to be initialized, but it was nil")
					}
					var bm blockMetadata
					bm.copyFrom(pi.curBlock)
					got = append(got, bm)
				}

				if !errors.Is(pi.error(), tt.wantErr) {
					t.Errorf("Unexpected error: got %v, want %v", pi.err, tt.wantErr)
				}

				if diff := cmp.Diff(got, tt.want,
					cmpopts.IgnoreFields(blockMetadata{}, "uncompressedSizeBytes"),
					cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
					cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
					cmp.AllowUnexported(blockMetadata{}),
				); diff != "" {
					t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
				}
			}
			mp := generateMemPart()
			releaseMemPart(mp)
			mp.mustInitFromElements(es)

			p := openMemPart(mp)
			verifyPart(p)
			tmpDir, defFn := test.Space(require.New(t))
			defer defFn()
			epoch := uint64(1)
			partPath := partPath(tmpDir, epoch)
			fileSystem := fs.NewLocalFileSystem()
			mp.mustFlush(fileSystem, partPath)
			p = mustOpenFilePart(epoch, tmpDir, fileSystem)
			verifyPart(p)
		})
	}
}

// testEqFilter mirrors logicalstream's eq node: it skips a block unless the
// block's own tag filters report the value as possibly present.
type testEqFilter struct {
	name  string
	value string
}

func (f testEqFilter) String() string { return "eq(" + f.name + ", " + f.value + ")" }

func (testEqFilter) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	return nil, nil, nil
}

func (f testEqFilter) ShouldSkip(tagFamilyFilters index.FilterOp) (bool, error) {
	return !tagFamilyFilters.Eq(f.name, f.value), nil
}

// buildMultiBlockMemPart writes three blocks: series 1 holds block "a" (ts 1-2)
// then block "b" (ts 3-4), series 2 holds block "a2" (ts 5-6). Series 1 having
// two blocks is what mustInitFromElements itself produces once a series exceeds
// maxUncompressedBlockSize; writing the blocks directly keeps the fixture small.
func buildMultiBlockMemPart() *memPart {
	rowTagFamilies := func(value string, rows int) [][]tagValues {
		row := []tagValues{{
			tag: "singleTag",
			values: []*tagValue{
				{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte(value)},
			},
		}}
		out := make([][]tagValues, rows)
		for i := range out {
			out[i] = row
		}
		return out
	}
	mp := generateMemPart()
	mp.reset()
	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp)
	bsw.MustWriteElements(1, []int64{1, 2}, []uint64{1, 2}, rowTagFamilies("a", 2))
	bsw.MustWriteElements(1, []int64{3, 4}, []uint64{3, 4}, rowTagFamilies("b", 2))
	bsw.MustWriteElements(2, []int64{5, 6}, []uint64{5, 6}, rowTagFamilies("a2", 2))
	bsw.Flush(&mp.partMetadata, &mp.tagType)
	releaseBlockWriter(bsw)
	return mp
}

func Test_partIter_nextBlock_blockFilterSkipsOnlyCurrentBlock(t *testing.T) {
	tests := []struct {
		name       string
		sids       []common.SeriesID
		filter     testEqFilter
		wantSeries []common.SeriesID
		wantMinTS  []int64
	}{
		{
			name:       "matching value lives in the second block of the series",
			sids:       []common.SeriesID{1},
			filter:     testEqFilter{name: "strTag", value: "b"},
			wantSeries: []common.SeriesID{1},
			wantMinTS:  []int64{3},
		},
		{
			name:       "skip on the first series must not hide its second block from a later series query",
			sids:       []common.SeriesID{1, 2},
			filter:     testEqFilter{name: "strTag", value: "b"},
			wantSeries: []common.SeriesID{1},
			wantMinTS:  []int64{3},
		},
		{
			name:       "value absent from every block skips all blocks",
			sids:       []common.SeriesID{1, 2},
			filter:     testEqFilter{name: "strTag", value: "nonexistent"},
			wantSeries: nil,
			wantMinTS:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifyPart := func(p *part) {
				defer p.close()
				bma := generateBlockMetadataArray()
				defer releaseBlockMetadataArray(bma)
				pi := partIter{}
				pi.init(bma, p, tt.sids, 0, 10, tt.filter)

				var gotSeries []common.SeriesID
				var gotMinTS []int64
				for pi.nextBlock() {
					gotSeries = append(gotSeries, pi.curBlock.seriesID)
					gotMinTS = append(gotMinTS, pi.curBlock.timestamps.min)
				}
				require.NoError(t, pi.error())
				require.Equal(t, tt.wantSeries, gotSeries, "unexpected series of yielded blocks")
				require.Equal(t, tt.wantMinTS, gotMinTS, "unexpected blocks yielded")
			}

			mp := buildMultiBlockMemPart()
			verifyPart(openMemPart(mp))

			tmpDir, defFn := test.Space(require.New(t))
			defer defFn()
			fileSystem := fs.NewLocalFileSystem()
			partPath := partPath(tmpDir, 1)
			mp.mustFlush(fileSystem, partPath)
			releaseMemPart(mp)
			verifyPart(mustOpenFilePart(1, tmpDir, fileSystem))
		})
	}
}

func Test_partMergeIter_nextBlock(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		es      *elements
		want    []blockMetadata
	}{
		{
			name: "Test with all data",
			es:   es,
			want: []blockMetadata{
				{seriesID: 1, count: 2}, {seriesID: 2, count: 2}, {seriesID: 3, count: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifyPart := func(p *part, decoder *encoding.BytesBlockDecoder) {
				defer p.close()
				pi := generatePartMergeIter()
				defer releasePartMergeIter(pi)
				pi.mustInitFromPart(p)
				var got []blockMetadata
				for pi.nextBlockMetadata() {
					got = append(got, pi.block.bm)
					require.Nil(t, pi.block.bm.tagProjection)
					pi.mustLoadBlockData(decoder, &pi.block)
					require.Equal(t, len(pi.block.bm.tagFamilies), len(pi.block.tagFamilies))
				}

				if !errors.Is(pi.error(), tt.wantErr) {
					t.Errorf("Unexpected error: got %v, want %v", pi.err, tt.wantErr)
				}

				if diff := cmp.Diff(got, tt.want,
					cmpopts.IgnoreFields(blockMetadata{}, "uncompressedSizeBytes"),
					cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
					cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagType"),
					cmp.AllowUnexported(blockMetadata{}),
				); diff != "" {
					t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
				}
			}
			mp := generateMemPart()
			releaseMemPart(mp)
			mp.mustInitFromElements(tt.es)

			decoder := generateColumnValuesDecoder()
			defer releaseColumnValuesDecoder(decoder)
			p := openMemPart(mp)
			verifyPart(p, decoder)
			tmpDir, defFn := test.Space(require.New(t))
			defer defFn()
			epoch := uint64(1)
			partPath := partPath(tmpDir, epoch)
			fileSystem := fs.NewLocalFileSystem()
			mp.mustFlush(fileSystem, partPath)
			p = mustOpenFilePart(epoch, tmpDir, fileSystem)
			verifyPart(p, decoder)
		})
	}
}
