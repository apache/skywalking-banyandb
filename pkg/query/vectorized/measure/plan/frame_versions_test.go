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

package plan

import (
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// TestLiaisonMergeHoldsForEveryFrameVersion reruns the liaison merge, group-by and top tests with
// the sources sent as version 3 frames: the fixtures default to version 4, and a half-upgraded
// cluster sends either.
func TestLiaisonMergeHoldsForEveryFrameVersion(t *testing.T) {
	tests := map[string]func(*testing.T){
		"dedup":              TestMergeDistributedRows_DedupsEqualSortKeyBySidTimestampHighestVersion,
		"no dedup":           TestMergeDistributedRows_DoesNotDedupSidTimestampAcrossSortGroups,
		"index mode":         TestMergeDistributedRows_IndexModeSuppressesRepeatedSidAcrossSortGroups,
		"desc":               TestMergeDistributedRows_DescTimestampOrder,
		"order by string":    TestMergeDistributedRows_OrderByString_AscDescAcrossSources,
		"order by int":       TestMergeDistributedRows_OrderByInt64_PreservesPhase15Invariants,
		"raw group by":       TestMergeDistributedRows_RawGroupBy_FirstSeenPerGroup,
		"group by then top":  TestAnalyzeDistributed_GroupByTop_PerNodeRankingPreservesGlobalWinners,
		"multi group":        TestMergeDistributedRows_MultiGroup_NullFillsMissingColumn,
		"multi group index":  TestMergeDistributedRows_IndexMode_PerGroupSidIsolation,
		"top without agg":    TestMergeDistributedRows_TopWithoutAgg_LiaisonSelectsTopN,
		"top unknown field":  TestMergeDistributedRows_TopWithoutAgg_UnknownFieldErrors,
		"group by unknown":   TestApplyBatchGroupByFirstToRows_UnknownTag,
		"top uint16 bound":   TestApplyBatchTopToRows_OverUint16Boundary_SelectsCorrectTopN,
		"group by uint16 bd": TestApplyBatchGroupByFirstToRows_OverUint16Boundary_KeepsEveryGroup,
	}
	for _, version := range []uint8{frame.WireVersion, frame.WireVersionColumnar} {
		prev := testWireVersion
		testWireVersion = version
		for name, run := range tests {
			t.Run(fmt.Sprintf("v%d/%s", version, name), run)
		}
		testWireVersion = prev
	}
}
