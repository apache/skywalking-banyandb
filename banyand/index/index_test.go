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

package index

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/event"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func Test_service_Insert(t *testing.T) {
	tester := assert.New(t)
	type args struct {
		series  common.Metadata
		shardID uint
		field   *Field
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "str field",
			args: args{
				series:  *common.NewMetadataByNameAndGroup("sw", "default"),
				shardID: 0,
				field: &Field{
					ChunkID: common.ChunkID(1),
					Name:    "endpoint",
					Value:   []byte("/test"),
				},
			},
		},
		{
			name: "int field",
			args: args{
				series:  *common.NewMetadataByNameAndGroup("sw", "default"),
				shardID: 1,
				field: &Field{
					ChunkID: common.ChunkID(2),
					Name:    "duration",
					Value:   convert.Int64ToBytes(500),
				},
			},
		},
		{
			name: "unknown series",
			args: args{
				series:  *common.NewMetadataByNameAndGroup("unknown", "default"),
				shardID: 0,
				field: &Field{
					ChunkID: common.ChunkID(2),
					Name:    "duration",
					Value:   convert.Int64ToBytes(500),
				},
			},
			wantErr: true,
		},
		{
			name: "unknown shard",
			args: args{
				series:  *common.NewMetadataByNameAndGroup("sw", "default"),
				shardID: math.MaxInt64,
				field: &Field{
					ChunkID: common.ChunkID(2),
					Name:    "duration",
					Value:   convert.Int64ToBytes(500),
				},
			},
			wantErr: true,
		},
		{
			name: "unknown field",
			args: args{
				series:  *common.NewMetadataByNameAndGroup("sw", "default"),
				shardID: 0,
				field: &Field{
					ChunkID: common.ChunkID(2),
					Name:    "unknown",
					Value:   convert.Int64ToBytes(500),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := setUpModules(tester)
			if err := s.Insert(tt.args.series, tt.args.shardID, tt.args.field); (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_service_Init(t *testing.T) {
	tester := assert.New(t)
	s := setUpModules(tester)
	tester.Equal(1, len(s.meta.meta))
	tester.Equal(2, len(s.meta.meta["sw-default"].repo))
}

func setUpModules(tester *assert.Assertions) *service {
	_ = logger.Bootstrap()
	repo, err := discovery.NewServiceRepo(context.TODO())
	tester.NoError(err)
	svc, err := NewService(context.TODO(), repo)
	tester.NoError(err)
	tester.NoError(svc.PreRun())

	rules := []*databasev1.IndexRule{
		{
			Objects: []*databasev1.IndexObject{
				{
					Name:   "endpoint",
					Fields: []string{"endpoint"},
				},
				{
					Name:   "duration",
					Fields: []string{"duration"},
				},
			},
		},
	}
	seriesID := &commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	}
	_, err = repo.Publish(event.TopicIndexRule, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &databasev1.IndexRuleEvent{
		Series: seriesID,
		Rules: []*databasev1.IndexRuleEvent_ShardedIndexRule{
			{
				ShardId: 0,
				Rules:   rules,
			},
			{
				ShardId: 1,
				Rules:   rules,
			},
		},
		Action: databasev1.Action_ACTION_PUT,
		Time:   timestamppb.Now(),
	}))
	tester.NoError(err)
	s, ok := svc.(*service)
	tester.True(ok)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	tester.True(svc.Ready(ctx, MetaExists("default", "sw")))
	return s
}
