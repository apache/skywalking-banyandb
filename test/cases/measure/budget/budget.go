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

// Package budget holds the measure query-budget integration case shared by the
// standalone and distributed suites: a query over more block bytes than the
// per-query allowance must fail with RESOURCE_EXHAUSTED, not a generic error.
package budget

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	// QueryPoolBytes is the query pool size the node under test is given. A query may reserve an eighth of it, of which
	// the first MiB is the fixed admission overhead. Reading the series index charges its buffers (about 2.6 MiB) to the
	// query as well, so this pool leaves about 1.8 MiB to spend on blocks: less than the oversized series' block, more than any other.
	QueryPoolBytes = 64 << 20
	pointCount     = 16
	payloadBytes   = 64 << 10
	// oversizedCount points of oversizedBytes each share one series, so they form one block larger than the allowance.
	oversizedCount = 4
	oversizedBytes = 900 << 10
	measureName    = "service_cpm_minute"
	groupName      = "sw_metric"
)

// Write stores pointCount data points, one second apart from baseTime, each in its own series and with a payloadBytes
// id tag, except the last oversizedCount ones, which share one series and form a block larger than the node's query allowance.
func Write(conn *grpc.ClientConn, baseTime time.Time) {
	client := measurev1.NewMeasureServiceClient(conn)
	metadata := &commonv1.Metadata{Name: measureName, Group: groupName}
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	payload := func(size int) string {
		b := make([]byte, size)
		for i := range b {
			b[i] = 'a' + byte(i*31%26)
		}
		return string(b)
	}
	small, large := payload(payloadBytes), payload(oversizedBytes)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		writeClient, writeErr := client.Write(ctx)
		innerGm.Expect(writeErr).NotTo(gomega.HaveOccurred())
		for i := 0; i < pointCount; i++ {
			id, entity := small, fmt.Sprintf("entity-%d", i)
			if i >= pointCount-oversizedCount {
				id, entity = large, "entity-oversized"
			}
			sendErr := writeClient.Send(&measurev1.WriteRequest{
				Metadata:  metadata,
				MessageId: uint64(time.Now().UnixNano()),
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
						str(id), str(entity),
					}}},
					Fields: []*modelv1.FieldValue{
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i)}}},
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}},
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
		innerGm.Expect(succeeded).To(gomega.Equal(pointCount), "every data point must be accepted")
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
}

func query(baseTime time.Time, window time.Duration) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:   measureName,
		Groups: []string{groupName},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(baseTime.Add(-time.Second)),
			End:   timestamppb.New(baseTime.Add(window)),
		},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{"id", "entity_id"}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"total"}},
	}
}

// ExpectFitsAndExceeds asserts the contract against a node whose query allowance is about 1 MiB: a query that reads
// one data point succeeds, and a query that reads all of them, including the oversized one, fails with
// codes.ResourceExhausted.
func ExpectFitsAndExceeds(conn *grpc.ClientConn, baseTime time.Time) {
	client := measurev1.NewMeasureServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := client.Query(ctx, query(baseTime, 500*time.Millisecond))
		innerGm.Expect(err).NotTo(gomega.HaveOccurred(), "a query within the allowance must succeed")
		innerGm.Expect(resp.GetDataPoints()).To(gomega.HaveLen(1))
	}, flags.EventuallyTimeout).Should(gomega.Succeed())

	// In a cluster the points reach the data node through the liaison's asynchronous part sync, so the node may hold
	// only some of them yet. A query that succeeds is accepted only while it returns fewer than all points. Once every
	// point is stored, the query must fail with RESOURCE_EXHAUSTED.
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := client.Query(ctx, query(baseTime, time.Duration(pointCount)*time.Second))
		if err == nil {
			gomega.Expect(len(resp.GetDataPoints())).To(gomega.BeNumerically("<", pointCount),
				"a query reading all %d points must exceed its memory allowance", pointCount)
			innerGm.Expect(err).To(gomega.HaveOccurred(), "only %d of %d points are stored yet", len(resp.GetDataPoints()), pointCount)
			return
		}
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.ResourceExhausted),
			"a query over its memory allowance must be reported as RESOURCE_EXHAUSTED, got: %v", err)
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
}
