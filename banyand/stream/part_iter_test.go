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

	"github.com/apache/skywalking-banyandb/api/common"
)

// TODO: test more scenarios.
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := generateMemPart()
			releaseMemPart(mp)
			mp.mustInitFromDataPoints(dps)

			p := openMemPart(mp)

			pi := partIter{}
			pi.init(p, tt.sids, tt.opt.minTimestamp, tt.opt.maxTimestamp)

			var got []blockMetadata
			for pi.nextBlock() {
				if pi.curBlock.seriesID == 0 {
					t.Errorf("Expected currBlock to be initialized, but it was nil")
				}
				got = append(got, pi.curBlock)
			}

			if !errors.Is(pi.error(), tt.wantErr) {
				t.Errorf("Unexpected error: got %v, want %v", pi.err, tt.wantErr)
			}

			if diff := cmp.Diff(got, tt.want,
				cmpopts.IgnoreFields(blockMetadata{}, "uncompressedSizeBytes"),
				cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
				cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
				cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
				cmp.AllowUnexported(blockMetadata{}),
			); diff != "" {
				t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
			}
		})
	}
}
