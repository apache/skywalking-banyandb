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

package capacity

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	streamElements   = 400
	streamPayload    = 64 << 10
	streamName       = "sw"
	streamGroup      = "default"
	streamCountSlice = 8
	streamSearchable = 17
)

// StreamHotSeries is a stream whose elements all belong to one series, the shape of a busy service's log: 400 elements of
// 64 KiB, about 25 MiB, queried newest first. The data set is many times what a query may hold, and a query with a limit has
// to read only the newest elements of it.
func StreamHotSeries(poolBytes uint64) *Scenario {
	return &Scenario{
		Name:      "stream, one hot series, newest first",
		PoolBytes: poolBytes,
		Total:     streamElements,
		Write:     writeHotStream,
		Count: func(ctx context.Context, conn *grpc.ClientConn, base time.Time) (int, error) {
			total := 0
			for from := 0; from < streamElements; from += streamCountSlice {
				resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, streamQuery(base, from, from+streamCountSlice, streamCountSlice))
				if err != nil {
					return 0, err
				}
				total += len(resp.GetElements())
			}
			return total, nil
		},
		Probe: func(ctx context.Context, conn *grpc.ClientConn, base time.Time, limit int) (Outcome, error) {
			resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, streamQuery(base, 0, streamElements, limit))
			if err != nil {
				return Outcome{}, refused(err)
			}
			outcome := Outcome{Returned: len(resp.GetElements())}
			for _, element := range resp.GetElements() {
				for _, family := range element.GetTagFamilies() {
					for _, tag := range family.GetTags() {
						outcome.Bytes += len(tag.GetValue().GetBinaryData())
					}
				}
			}
			return outcome, nil
		},
	}
}

func streamQuery(base time.Time, from, to, limit int) *streamv1.QueryRequest {
	return &streamv1.QueryRequest{
		Name:   streamName,
		Groups: []string{streamGroup},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(time.Duration(from)*time.Second - 500*time.Millisecond)),
			End:   timestamppb.New(base.Add(time.Duration(to)*time.Second - 500*time.Millisecond)),
		},
		Limit:   uint32(limit),
		OrderBy: &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC},
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "data", Tags: []string{"data_binary"}},
			{Name: "searchable", Tags: []string{"trace_id"}},
		}},
	}
}

func writeHotStream(conn *grpc.ClientConn, base time.Time) {
	resp, err := databasev1.NewStreamRegistryServiceClient(conn).Get(context.Background(),
		&databasev1.StreamRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: streamName, Group: streamGroup}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	metadata := resp.GetStream().GetMetadata()
	payload := make([]byte, streamPayload)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	client := streamv1.NewStreamServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*flags.EventuallyTimeout)
		defer cancel()
		writeClient, writeErr := client.Write(ctx)
		innerGm.Expect(writeErr).NotTo(gomega.HaveOccurred())
		for i := 0; i < streamElements; i++ {
			searchable := make([]*modelv1.TagValue, streamSearchable)
			for t := range searchable {
				searchable[t] = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
			}
			// The searchable family starts trace_id, state, service_id, service_instance_id: the entity is the same for every
			// element, so they all belong to one series.
			searchable[0] = str(fmt.Sprintf("trace-%04d", i))
			searchable[1] = &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}}
			searchable[2] = str("hot-service")
			searchable[3] = str("hot-instance")
			sendErr := writeClient.Send(&streamv1.WriteRequest{
				Metadata:  metadata,
				MessageId: uint64(time.Now().UnixNano()) + uint64(i),
				Element: &streamv1.ElementValue{
					ElementId: strconv.Itoa(i),
					Timestamp: timestamppb.New(base.Add(time.Duration(i) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: payload}}}},
						{Tags: searchable},
					},
				},
			})
			innerGm.Expect(sendErr).NotTo(gomega.HaveOccurred())
		}
		innerGm.Expect(writeClient.CloseSend()).To(gomega.Succeed())
		succeeded := 0
		for {
			ack, recvErr := writeClient.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			innerGm.Expect(recvErr).NotTo(gomega.HaveOccurred())
			if ack.GetStatus() == modelv1.Status_STATUS_SUCCEED.String() {
				succeeded++
			}
		}
		innerGm.Expect(succeeded).To(gomega.Equal(streamElements), "every element must be accepted")
	}, 3*flags.EventuallyTimeout).Should(gomega.Succeed())
}
