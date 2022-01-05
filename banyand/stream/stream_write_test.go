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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

func Test_Stream_Write(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(t)
	defer deferFunc()

	type args struct {
		ele *streamv1.ElementValue
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "golden path",
			args: args{
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
			name: "unknown tags",
			args: args{
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
			err := s.Write(tt.args.ele)
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
		})
	}

}

func setup(t *testing.T) (*stream, test.StopFunc) {
	flow := test.NewTestFlow()
	req := require.New(t)
	req.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	}))
	tempDir, deferFunc := test.Space(req)

	var mService metadata.Service
	var etcdRootDir string
	var defaultStream *databasev1.Stream
	var rules []*databasev1.IndexRule
	var s *stream

	flow.
		PushErrorHandler(func() {
			deferFunc()
		}).
		Run(context.TODO(), func() (err error) {
			mService, err = metadata.NewService(context.TODO())
			return
		}, func() {
			if mService != nil {
				mService.GracefulStop()
			}
		}).
		RunWithoutSideEffect(context.TODO(), func() error {
			etcdRootDir = teststream.RandomTempDir()
			return mService.FlagSet().Parse([]string{"--metadata-root-path=" + etcdRootDir})
		}).
		Run(context.TODO(), func() error {
			return mService.PreRun()
		}, func() {
			if len(etcdRootDir) > 0 {
				_ = os.RemoveAll(etcdRootDir)
			}
		}).
		RunWithoutSideEffect(context.TODO(), func() error {
			return teststream.PreloadSchema(mService.SchemaRegistry())
		}).
		RunWithoutSideEffect(context.TODO(), func() (err error) {
			defaultStream, err = mService.StreamRegistry().GetStream(context.TODO(), &commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			return
		}).
		RunWithoutSideEffect(context.TODO(), func() (err error) {
			rules, err = mService.IndexRules(context.TODO(), defaultStream.GetMetadata())
			return
		}).
		Run(context.TODO(), func() (err error) {
			sSpec := streamSpec{
				schema:     defaultStream,
				indexRules: rules,
			}
			s, err = openStream(tempDir, sSpec, logger.GetLogger("test"))
			return
		}, func() {
			if s != nil {
				_ = s.Close()
			}
		})

	req.NoError(flow.Error())

	return s, flow.Shutdown()
}

func getEle(tags ...interface{}) *streamv1.ElementValue {
	searchableTags := make([]*modelv1.TagValue, 0)
	for _, tag := range tags {
		searchableTags = append(searchableTags, getTag(tag))
	}
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	e := &streamv1.ElementValue{
		ElementId: "1231.dfd.123123ssf",
		Timestamp: timestamppb.Now(),
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{
				Tags: []*modelv1.TagValue{
					{
						Value: &modelv1.TagValue_BinaryData{
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

func getTag(tag interface{}) *modelv1.TagValue {
	if tag == nil {
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Null{},
		}
	}
	switch t := tag.(type) {
	case int:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{
					Value: int64(t),
				},
			},
		}
	case string:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: t,
				},
			},
		}
	}
	return nil
}
