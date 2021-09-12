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
	"encoding/base64"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func Test_Stream_Write(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(tester)
	defer deferFunc()

	type args struct {
		shardID uint
		ele     *streamv2.ElementValue
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "golden path",
			args: args{
				shardID: 0,
				ele: getEle(
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
				),
			},
		},
		{
			name: "minimal",
			args: args{
				shardID: 1,
				ele: getEle(
					nil,
					1,
					"webapp_id",
					"10.0.0.1_id",
				),
			},
		},
		{
			name: "http",
			args: args{
				shardID: 0,
				ele: getEle(
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					"GET",
					"200",
				),
			},
		},
		{
			name: "database",
			args: args{
				shardID: 0,
				ele: getEle(
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					nil,
					nil,
					"MySQL",
					"10.1.1.2",
				),
			},
		},
		{
			name: "mq",
			args: args{
				shardID: 0,
				ele: getEle(
					"trace_id-xxfff.111323",
					1,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					nil,
					nil,
					nil,
					nil,
					"test_topic",
					"10.0.0.1",
					"broker",
				),
			},
		},
		{
			name: "invalid trace id",
			args: args{
				shardID: 1,
				ele: getEle(
					1212323,
					1,
					"webapp_id",
					"10.0.0.1_id",
				),
			},
			wantErr: true,
		},
		{
			name:    "empty input",
			args:    args{},
			wantErr: true,
		},
		{
			name: "invalid shard id",
			args: args{
				shardID: math.MaxUint64,
				ele: getEle(
					"trace_id-xxfff.111323",
					0,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
				),
			},
			wantErr: true,
		},
		{
			name: "unknown tags",
			args: args{
				shardID: 0,
				ele: getEle(
					"trace_id-xxfff.111323",
					1,
					"webapp_id",
					"10.0.0.1_id",
					"/home_id",
					300,
					1622933202000000000,
					nil,
					nil,
					nil,
					nil,
					"test_topic",
					"10.0.0.1",
					"broker",
					"unknown",
				),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.write(common.ShardID(tt.args.shardID), tt.args.ele)
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
		})
	}

}

func setup(t *assert.Assertions) (*stream, func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "trace",
	}))
	tempDir, deferFunc := test.Space(t)
	streamRepo, err := schema.NewStream()
	t.NoError(err)
	sa, err := streamRepo.Get(context.TODO(), &commonv2.Metadata{
		Name:  "sw",
		Group: "default",
	})
	t.NoError(err)
	mService, err := metadata.NewService(context.TODO())
	t.NoError(err)
	iRules, err := mService.IndexRules(context.TODO(), sa.Metadata)
	t.NoError(err)
	sSpec := streamSpec{
		schema:     sa,
		indexRules: iRules,
	}
	s, err := openStream(tempDir, sSpec, logger.GetLogger("test"))
	t.NoError(err)
	return s, func() {
		_ = s.Close()
		deferFunc()
	}
}

func getEle(tags ...interface{}) *streamv2.ElementValue {
	searchableTags := make([]*modelv2.TagValue, 0)
	for _, tag := range tags {
		searchableTags = append(searchableTags, getTag(tag))
	}
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	e := &streamv2.ElementValue{
		ElementId: "1231.dfd.123123ssf",
		Timestamp: timestamppb.Now(),
		TagFamilies: []*streamv2.ElementValue_TagFamily{
			{
				Tags: []*modelv2.TagValue{
					{
						Value: &modelv2.TagValue_BinaryData{
							BinaryData: bb,
						},
					},
				},
			},
			{
				Tags: searchableTags,
			},
		},
	}
	return e
}

func getTag(tag interface{}) *modelv2.TagValue {
	if tag == nil {
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Null{},
		}
	}
	switch t := tag.(type) {
	case int:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Int{
				Int: &modelv2.Int{
					Value: int64(t),
				},
			},
		}
	case string:
		return &modelv2.TagValue{
			Value: &modelv2.TagValue_Str{
				Str: &modelv2.Str{
					Value: t,
				},
			},
		}
	}
	return nil
}
