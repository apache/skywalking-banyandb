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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// fakeNodeMetadata is a metadata.Repo stub whose NodeRegistry resolves any
// node name to a minimal Node. CollectDataInfo needs nothing else from the
// metadata repository.
type fakeNodeMetadata struct {
	metadata.Repo
}

func (fakeNodeMetadata) NodeRegistry() schema.Node { return fakeNodeRegistry{} }

type fakeNodeRegistry struct {
	schema.Node
}

func (fakeNodeRegistry) GetNode(_ context.Context, name string) (*databasev1.Node, error) {
	return &databasev1.Node{Metadata: &commonv1.Metadata{Name: name}}, nil
}

// TestCollectDataInfo_OpenSegmentReportsRealShardID verifies the open-segment
// path of CollectDataInfo reports each live table under its real shard ID.
// Shards are appended in creation order, so a table's slice index is not its
// shard ID: a node that owns only shard 1 holds that table at index 0.
func TestCollectDataInfo_OpenSegmentReportsRealShardID(t *testing.T) {
	tests := []struct {
		name   string
		create []common.ShardID
		want   []uint32
	}{
		{name: "node owning only shard 1", create: []common.ShardID{1}, want: []uint32{1}},
		{name: "shards created out of order", create: []common.ShardID{1, 0}, want: []uint32{1, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpPath, defFn := test.Space(require.New(t))
			defer defFn()
			// Open the TSDB with a shard range that covers every shard ID created below.
			db := openTestTSDBForRefTest(t, tmpPath, 2, nil)
			defer db.Close()

			seg, err := db.CreateSegmentIfNotExist(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err)
			defer seg.DecRef()
			for _, id := range tt.create {
				_, err = seg.CreateTSTableIfNotExist(id)
				require.NoError(t, err)
			}

			sr := newTestSchemaRepo(db, "test-group")
			sr.nodeID = "node-1"
			sr.metadata = fakeNodeMetadata{}

			info, err := sr.CollectDataInfo(context.Background(), "test-group")
			require.NoError(t, err)
			require.Len(t, info.SegmentInfo, 1)
			shards := info.SegmentInfo[0].ShardInfo
			got := make([]uint32, 0, len(shards))
			for _, shard := range shards {
				got = append(got, shard.ShardId)
			}
			require.Equal(t, tt.want, got, "open-segment path must report real shard IDs, not table indexes")
		})
	}
}
