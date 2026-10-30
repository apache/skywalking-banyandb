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

// Package budget holds the stream query-budget integration case shared by the
// standalone and distributed suites: a query over more block bytes than the
// per-query allowance must fail with RESOURCE_EXHAUSTED, not a generic error.
package budget

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	// QueryPoolBytes is the query pool size the node under test is given. A query may reserve an eighth of it, of which
	// the first MiB is the fixed admission overhead, so a query has about 1 MiB to spend.
	QueryPoolBytes = 16 << 20
	elementCount   = 16
	payloadBytes   = 64 << 10
	// oversizedBytes is the payload of the last element: a single value larger than the whole query allowance. Values are
	// charged by their size, so a scan only runs out of budget on data that really is larger than the allowance.
	oversizedBytes = 2 << 20
	searchableTags = 17
	streamName     = "sw"
	groupName      = "default"
)

// Write stores elementCount elements, one second apart from baseTime, each in its own series and with a
// payloadBytes data_binary tag, except the last one, whose oversizedBytes tag alone is larger than the node's query allowance.
func Write(conn *grpc.ClientConn, baseTime time.Time) {
	registry := databasev1.NewStreamRegistryServiceClient(conn)
	resp, err := registry.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{
		Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	metadata := resp.GetStream().GetMetadata()
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	oversized := make([]byte, oversizedBytes)
	for i := range oversized {
		oversized[i] = byte(i * 31)
	}
	client := streamv1.NewStreamServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		writeClient, writeErr := client.Write(ctx)
		innerGm.Expect(writeErr).NotTo(gomega.HaveOccurred())
		for i := 0; i < elementCount; i++ {
			data := payload
			if i == elementCount-1 {
				data = oversized
			}
			sendErr := writeClient.Send(&streamv1.WriteRequest{
				Metadata:  metadata,
				MessageId: uint64(time.Now().UnixNano()),
				Element: &streamv1.ElementValue{
					ElementId: strconv.Itoa(i),
					Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: data}}}},
						{Tags: searchableTagValues(i)},
					},
				},
			})
			innerGm.Expect(sendErr).NotTo(gomega.HaveOccurred())
		}
		innerGm.Expect(writeClient.CloseSend()).To(gomega.Succeed())
		var succeeded int
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
		innerGm.Expect(succeeded).To(gomega.Equal(elementCount), "every element must be accepted")
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
}

// searchableTagValues fills the searchable family of the sw stream: the entity tags identify series i and every
// other tag is null.
func searchableTagValues(i int) []*modelv1.TagValue {
	null := func() *modelv1.TagValue { return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}} }
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	tags := make([]*modelv1.TagValue, searchableTags)
	for t := range tags {
		tags[t] = null()
	}
	// The searchable family declares trace_id, state, service_id, service_instance_id, ...
	tags[0] = str(fmt.Sprintf("trace-%d", i))
	tags[1] = &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}}
	tags[2] = str(fmt.Sprintf("service-%d", i))
	tags[3] = str("instance")
	return tags
}

func query(baseTime time.Time, window time.Duration) *streamv1.QueryRequest {
	return &streamv1.QueryRequest{
		Name:   streamName,
		Groups: []string{groupName},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(baseTime.Add(-time.Second)),
			End:   timestamppb.New(baseTime.Add(window)),
		},
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "data", Tags: []string{"data_binary"}},
			{Name: "searchable", Tags: []string{"trace_id", "service_id"}},
		}},
	}
}

// ExpectFitsAndExceeds asserts the contract against a node whose query allowance is about 1 MiB: a query that reads
// one element succeeds, and a query that reads all of them, including the oversized one, fails with codes.ResourceExhausted.
func ExpectFitsAndExceeds(conn *grpc.ClientConn, baseTime time.Time) {
	client := streamv1.NewStreamServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := client.Query(ctx, query(baseTime, 500*time.Millisecond))
		innerGm.Expect(err).NotTo(gomega.HaveOccurred(), "a query within the allowance must succeed")
		innerGm.Expect(resp.GetElements()).To(gomega.HaveLen(1))
	}, flags.EventuallyTimeout).Should(gomega.Succeed())

	// In a cluster the elements reach the data node through the liaison's asynchronous part sync, so the node may hold
	// only some of them yet. A query that succeeds is accepted only while it returns fewer than all elements: reading
	// fewer may fit the allowance. Once every element is stored, the query must fail with RESOURCE_EXHAUSTED.
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := client.Query(ctx, query(baseTime, time.Duration(elementCount)*time.Second))
		if err == nil {
			gomega.Expect(len(resp.GetElements())).To(gomega.BeNumerically("<", elementCount),
				"a query reading all %d elements must exceed its memory allowance", elementCount)
			innerGm.Expect(err).To(gomega.HaveOccurred(), "only %d of %d elements are stored yet", len(resp.GetElements()), elementCount)
			return
		}
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.ResourceExhausted),
			"a query over its memory allowance must be reported as RESOURCE_EXHAUSTED, got: %v", err)
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
}
