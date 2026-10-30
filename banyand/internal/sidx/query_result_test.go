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

package sidx

import (
	"math"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
)

func TestMergeWithHeapCapsPreallocation(t *testing.T) {
	shard := &QueryResponse{Keys: []int64{1, 2}, Data: [][]byte{nil, nil}, SIDs: []common.SeriesID{1, 1}, PartIDs: []uint64{1, 1}}
	merged := mergeQueryResponseShards([]*QueryResponse{shard}, math.MaxInt32)
	if merged.Len() != 2 {
		t.Fatalf("want 2 merged rows, got %d", merged.Len())
	}
	if cap(merged.Keys) > mergeCapacityHintMax {
		t.Fatalf("preallocation not capped: %d", cap(merged.Keys))
	}
	if mergeCapacityHint(-1) != 0 || mergeCapacityHint(5) != 5 {
		t.Fatal("unexpected hint")
	}
}
