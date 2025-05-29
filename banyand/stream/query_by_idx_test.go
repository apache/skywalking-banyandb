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
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	itest "github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestQueryResult_QuotaExceeded(t *testing.T) {
	type testCtx struct {
		name                string
		esList              []*elements
		sids                []common.SeriesID
		want                []blockMetadata
		minTimestamp        int64
		maxTimestamp        int64
		expectQuotaExceeded bool
	}

	tests := []testCtx{
		{
			name:                "TestQuotaNotExceeded_ExpectSuccess",
			esList:              []*elements{esTS1},
			sids:                []common.SeriesID{1, 2, 3},
			minTimestamp:        1,
			maxTimestamp:        1,
			expectQuotaExceeded: false,
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 889},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 16},
			},
		},
		{
			name:                "TestQuotaExceeded_ExpectError",
			esList:              []*elements{esTS1},
			sids:                []common.SeriesID{1, 2, 3},
			minTimestamp:        1,
			maxTimestamp:        1,
			expectQuotaExceeded: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize a tstIter object.
			tmpPath, defFn := test.Space(require.New(t))
			fileSystem := fs.NewLocalFileSystem()
			defer defFn()
			tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
				logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting()}, nil)
			require.NoError(t, err)
			for _, es := range tt.esList {
				tst.mustAddElements(es)
				time.Sleep(100 * time.Millisecond)
			}
			// wait until the introducer is done
			if len(tt.esList) > 0 {
				for {
					snp := tst.currentSnapshot()
					if snp == nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if snp.creator == snapshotCreatorMemPart {
						snp.decRef()
						time.Sleep(100 * time.Millisecond)
						continue
					}
					snp.decRef()
					tst.Close()
					break
				}
			}

			// reopen the table
			tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
				logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting()}, nil)
			require.NoError(t, err)

			defer tst.Close()
			qr := &idxResult{
				pm:   &itest.MockMemoryProtector{ExpectQuotaExceeded: tt.expectQuotaExceeded},
				tabs: []*tsTable{tst},
			}
			qo := queryOptions{
				minTimestamp: tt.minTimestamp,
				maxTimestamp: tt.maxTimestamp,
				sortedSids:   tt.sids,
			}
			err = qr.scanParts(context.TODO(), qo)
			if tt.expectQuotaExceeded {
				require.Error(t, err)
				require.Contains(t, err.Error(), "quota exceeded", "expected quota to be exceeded but got: %v", err)
				return
			}
			require.NoError(t, err)
			var got []blockMetadata
			for _, data := range qr.data {
				got = append(got, data.bm)
			}
			if diff := cmp.Diff(got, tt.want,
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
