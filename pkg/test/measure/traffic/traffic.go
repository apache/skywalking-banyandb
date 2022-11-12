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

package traffic

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var l = logger.GetLogger("test_measure_traffic")

type TestCase struct {
	Addr                string
	SvcNum              int
	InstanceNumEverySvc int
	MetricNum           int
}

func SendWrites(ts TestCase) (*z.Closer, error) {
	conn, err := grpchelper.Conn(ts.Addr, 1*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := measurev1.NewMeasureServiceClient(conn)
	closer := z.NewCloser(0)
	for i := 0; i < ts.SvcNum; i++ {
		for j := 0; j < ts.InstanceNumEverySvc; j++ {
			data := make([]byte, 10*1024)
			_, _ = rand.Read(data)
			sender, err := newSender(i, j, client, ts.MetricNum)
			if err != nil {
				l.Err(err).Msg("failed to create a new sender")
				return nil, err
			}
			closer.AddRunning(1)
			go func() {
				ticker := time.NewTicker(1 * time.Minute)
				for {
					select {
					case <-ticker.C:
						sender.write()
						l.Info().Msg(sender.String())
					case <-closer.HasBeenClosed():
						ticker.Stop()
						closer.Done()
						return
					}
				}
			}()
		}
	}
	go func() {
		closer.Wait()
		conn.Close()
	}()
	return closer, nil
}

type sender struct {
	svc        string
	instance   string
	succeed    uint64
	sendFailed uint64
	revFailed  uint64
	client     measurev1.MeasureService_WriteClient
	metricNum  int
}

func newSender(svcID, instanceID int, client measurev1.MeasureServiceClient, metricNum int) (*sender, error) {
	wc, err := client.Write(context.Background())
	if err != nil {
		l.Error().Err(err).Msg("creating write client failed")
		return nil, err
	}
	return &sender{
		svc:       "svc-" + strconv.Itoa(svcID),
		instance:  "instance-" + strconv.Itoa(instanceID),
		client:    wc,
		metricNum: metricNum,
	}, nil
}

func (s *sender) write() {
	ts := timestamppb.New(timestamp.NowMilli())
	dp := &measurev1.DataPointValue{
		Timestamp: ts,
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{
				Tags: []*modelv1.TagValue{
					{
						Value: &modelv1.TagValue_Id{
							Id: &modelv1.ID{
								Value: s.instance,
							},
						},
					},
					{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{
								Value: s.svc,
							},
						},
					},
				},
			},
		},
		Fields: []*modelv1.FieldValue{
			{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{
						Value: 10,
					},
				},
			},
			{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{
						Value: 100,
					},
				},
			},
		},
	}
	for i := 0; i < s.metricNum; i++ {
		name := "service_cpm_minute"
		if i > 0 {
			name = name + "_" + strconv.Itoa(i)
		}
		err := s.client.Send(&measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Name:  name,
				Group: "sw_metric",
			},

			DataPoint: dp,
		})
		if err != nil {
			l.Error().Str("svc", s.svc).Str("instance", s.instance).Err(err).Msg("writing failed")
			s.sendFailed++
			return
		}
		_, err = s.client.Recv()
		if err != nil && err != io.EOF {
			l.Error().Str("svc", s.svc).Str("instance", s.instance).Err(err).Msg("receiving failed")
			s.revFailed++
			return
		}
		s.succeed++
	}
}

func (s *sender) String() string {
	return fmt.Sprintf("%s %s writing succeed %d failed %d(sending) %d(receiving)",
		s.svc, s.instance, s.succeed, s.sendFailed, s.revFailed)
}
