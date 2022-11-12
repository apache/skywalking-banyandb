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
	// Load some tag templates
	_ "embed"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	//go:embed searchable_template.json
	content []byte
	l       = logger.GetLogger("test_stream_traffic")
)

type TestCase struct {
	Addr                string
	SvcNum              int
	InstanceNumEverySvc int
}

func SendWrites(ts TestCase) (*z.Closer, error) {
	searchTagFamily := &modelv1.TagFamilyForWrite{}
	err := protojson.Unmarshal(content, searchTagFamily)
	if err != nil {
		l.Err(err).Msg("unmarshal template")
		return nil, err
	}
	conn, err := grpchelper.Conn(ts.Addr, 1*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := streamv1.NewStreamServiceClient(conn)
	closer := z.NewCloser(0)
	for i := 0; i < ts.SvcNum; i++ {
		for j := 0; j < ts.InstanceNumEverySvc; j++ {
			data := make([]byte, 10*1024)
			_, _ = rand.Read(data)
			sender, err := newSender(i, j, client, data)
			if err != nil {
				l.Err(err).Msg("failed to create a new sender")
				return nil, err
			}
			closer.AddRunning(1)
			go func() {
				ticker := time.NewTicker(1 * time.Second)
				step := 0
				for {
					select {
					case <-ticker.C:
						sender.write(searchTagFamily)
						step++
						if step%10 == 0 {
							l.Info().Msg(sender.String())
						}
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
	client     streamv1.StreamService_WriteClient
	data       []byte
}

func newSender(svcID, instanceID int, client streamv1.StreamServiceClient, data []byte) (*sender, error) {
	wc, err := client.Write(context.Background())
	if err != nil {
		l.Error().Err(err).Msg("creating write client failed")
		return nil, err
	}
	return &sender{
		svc:      "svc-" + strconv.Itoa(svcID),
		instance: "instance-" + strconv.Itoa(instanceID),
		client:   wc,
		data:     data,
	}, nil
}

func (s *sender) write(searchTagFamily *modelv1.TagFamilyForWrite) {
	tf := proto.Clone(searchTagFamily).(*modelv1.TagFamilyForWrite)
	tf.Tags[2] = &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: s.svc,
			},
		},
	}
	tf.Tags[3] = &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: s.instance,
			},
		},
	}

	ts := timestamppb.New(timestamp.NowMilli())

	eleID := base64.StdEncoding.EncodeToString([]byte(strings.Join(
		[]string{s.svc, s.instance, strconv.Itoa(int(ts.AsTime().UnixMilli()))}, "-")))
	e := &streamv1.ElementValue{
		ElementId: eleID,
		Timestamp: ts,
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{
				Tags: []*modelv1.TagValue{
					{
						Value: &modelv1.TagValue_BinaryData{
							BinaryData: s.data,
						},
					},
				},
			},
		},
	}
	e.TagFamilies = append(e.TagFamilies, tf)
	err := s.client.Send(&streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{
			Name:  "sw",
			Group: "default",
		},
		Element: e,
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

func (s *sender) String() string {
	return fmt.Sprintf("%s %s writing succeed %d failed %d(sending) %d(receiving)",
		s.svc, s.instance, s.succeed, s.sendFailed, s.revFailed)
}
