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

package cluster

import (
	"testing"

	"google.golang.org/protobuf/proto"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

func TestMergeShardPieces_AddsContinuationsToTheShard(t *testing.T) {
	merged := mergeShardPieces([]*transferv1.ShardStat{
		{ShardId: 3, Parts: []*transferv1.PartStat{{Id: 5}}},
		{
			ShardId: 1, MinTimestamp: 10, MaxTimestamp: 20, PartsCount: 3, TotalCount: 30, EstimatedCompressedBytes: 7, EstimatedUncompressedBytes: 9,
			Parts: []*transferv1.PartStat{{Id: 1}},
		},
		{ShardId: 3, MinTimestamp: 1, MaxTimestamp: 2, PartsCount: 1, TotalCount: 4, Parts: []*transferv1.PartStat{{Id: 6}}},
		{ShardId: 1, Parts: []*transferv1.PartStat{{Id: 2}, {Id: 3}}},
	})
	want := []*transferv1.ShardStat{
		{
			ShardId: 1, MinTimestamp: 10, MaxTimestamp: 20, PartsCount: 3, TotalCount: 30, EstimatedCompressedBytes: 7, EstimatedUncompressedBytes: 9,
			Parts: []*transferv1.PartStat{{Id: 1}, {Id: 2}, {Id: 3}},
		},
		{ShardId: 3, MinTimestamp: 1, MaxTimestamp: 2, PartsCount: 1, TotalCount: 4, Parts: []*transferv1.PartStat{{Id: 5}, {Id: 6}}},
	}
	if len(merged) != len(want) {
		t.Fatalf("want %d shards, got %d", len(want), len(merged))
	}
	for i := range want {
		if !proto.Equal(merged[i], want[i]) {
			t.Fatalf("shard %d: got %v, want %v", i, merged[i], want[i])
		}
	}
}
