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
//
package traffic_test

import (
	"context"
	"crypto/rand"
	_ "embed"
	"strconv"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	stream_test "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

type Service interface {
	run.PreRunner
	run.Config
	run.Service
}

var _ Service = (*service)(nil)

// service to preload stream
type service struct {
	metaSvc   metadata.Service
	streamSvc stream.Service
	l         *logger.Logger
	stopCh    chan struct{}
}

func NewService(ctx context.Context, metaSvc metadata.Service, streamSvc stream.Service) Service {
	return &service{
		metaSvc:   metaSvc,
		streamSvc: streamSvc,
		stopCh:    make(chan struct{}),
	}
}

func (s *service) Name() string {
	return "stream-traffic-gen"
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("stream-traffic-gen")
	return flagS
}

func (*service) Validate() error {
	return nil
}

func (s *service) PreRun() error {
	s.l = logger.GetLogger(s.Name())
	return stream_test.PreloadSchema(s.metaSvc.SchemaRegistry())
}

//go:embed searchable_template.json
var content string

func (s *service) Serve() run.StopNotify {
	searchTagFamily := &modelv1.TagFamilyForWrite{}
	err := jsonpb.UnmarshalString(content, searchTagFamily)
	if err != nil {
		s.l.Err(err).Msg("unmarshal template")
		close(s.stopCh)
		return s.stopCh
	}
	stream, err := s.streamSvc.Stream(&commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	})
	if err != nil {
		s.l.Err(err).Msg("get the stream")
		close(s.stopCh)
		return s.stopCh
	}
	for i := 0; i < 5; i++ {
		svc := "svc-" + strconv.Itoa(i)
		for j := 0; j < 10; j++ {
			instance := "instance-" + strconv.Itoa(j)
			go func() {
				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						tf := proto.Clone(searchTagFamily).(*modelv1.TagFamilyForWrite)
						tf.Tags[2] = &modelv1.TagValue{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: svc,
								},
							},
						}
						tf.Tags[3] = &modelv1.TagValue{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: instance,
								},
							},
						}
						data := make([]byte, 10*1024)
						_, _ = rand.Read(data)
						e := &streamv1.ElementValue{
							ElementId: strconv.Itoa(i),
							Timestamp: timestamppb.Now(),
							TagFamilies: []*modelv1.TagFamilyForWrite{
								{
									Tags: []*modelv1.TagValue{
										{
											Value: &modelv1.TagValue_BinaryData{
												BinaryData: data,
											},
										},
									},
								},
							},
						}
						e.TagFamilies = append(e.TagFamilies, tf)
						errInner := stream.Write(e)
						if err != nil {
							s.l.Err(errInner).Msg("writing to the stream")
						}
					case <-s.stopCh:
						return
					}
				}
			}()

		}

	}
	return s.stopCh
}

func (s *service) GracefulStop() {
}
