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
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	traceCount      = 300
	spansPerTrace   = 8
	spanPayload     = 16 << 10
	traceName       = "sw"
	traceGroup      = "test-trace-group"
	traceCountSlice = 4
)

// TraceNewest is a trace group of 300 traces of 8 spans of 16 KiB each, about 38 MiB, queried newest first: the shape of a
// trace list, where every trace comes back whole and the limit counts traces.
func TraceNewest(poolBytes uint64) *Scenario {
	return &Scenario{
		Name:      "trace, newest first",
		PoolBytes: poolBytes,
		Total:     traceCount,
		Write:     writeTraces,
		Count: func(ctx context.Context, conn *grpc.ClientConn, base time.Time) (int, error) {
			total := 0
			for from := 0; from < traceCount; from += traceCountSlice {
				resp, err := tracev1.NewTraceServiceClient(conn).Query(ctx, traceQuery(base, from, from+traceCountSlice, traceCountSlice))
				if err != nil {
					return 0, err
				}
				total += len(resp.GetTraces())
			}
			return total, nil
		},
		Probe: func(ctx context.Context, conn *grpc.ClientConn, base time.Time, limit int) (Outcome, error) {
			resp, err := tracev1.NewTraceServiceClient(conn).Query(ctx, traceQuery(base, 0, traceCount, limit))
			if err != nil {
				return Outcome{}, refused(err)
			}
			outcome := Outcome{Returned: len(resp.GetTraces())}
			for _, tr := range resp.GetTraces() {
				for _, span := range tr.GetSpans() {
					outcome.Bytes += len(span.GetSpan())
				}
			}
			return outcome, nil
		},
	}
}

func traceQuery(base time.Time, from, to, limit int) *tracev1.QueryRequest {
	return &tracev1.QueryRequest{
		Name:   traceName,
		Groups: []string{traceGroup},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(time.Duration(from)*time.Second - 500*time.Millisecond)),
			End:   timestamppb.New(base.Add(time.Duration(to)*time.Second - 500*time.Millisecond)),
		},
		Limit:         uint32(limit),
		OrderBy:       &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC},
		TagProjection: []string{"trace_id"},
	}
}

func writeTraces(conn *grpc.ClientConn, base time.Time) {
	resp, err := databasev1.NewTraceRegistryServiceClient(conn).Get(context.Background(),
		&databasev1.TraceRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: traceName, Group: traceGroup}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	metadata := resp.GetTrace().GetMetadata()
	span := make([]byte, spanPayload)
	for i := range span {
		span[i] = byte(i * 31)
	}
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	num := func(v int64) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
	}
	client := tracev1.NewTraceServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*flags.EventuallyTimeout)
		defer cancel()
		writeClient, writeErr := client.Write(ctx)
		innerGm.Expect(writeErr).NotTo(gomega.HaveOccurred())
		version := uint64(1)
		for trace := 0; trace < traceCount; trace++ {
			traceID := fmt.Sprintf("trace-%05d", trace)
			for s := 0; s < spansPerTrace; s++ {
				// The sw trace declares trace_id, state, service_id, service_instance_id, endpoint_id, duration, span_id, timestamp.
				sendErr := writeClient.Send(&tracev1.WriteRequest{
					Metadata: metadata,
					Tags: []*modelv1.TagValue{
						str(traceID), num(1), str("capacity-service"), str("capacity-instance"), str("/capacity"), num(int64(100 + s)),
						str(fmt.Sprintf("%s-span-%d", traceID, s)),
						{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(trace)*time.Second + time.Duration(s)*time.Millisecond))}},
					},
					Span:    span,
					Version: version,
				})
				version++
				innerGm.Expect(sendErr).NotTo(gomega.HaveOccurred())
			}
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
		innerGm.Expect(succeeded).To(gomega.Equal(traceCount*spansPerTrace), "every span must be accepted")
	}, 3*flags.EventuallyTimeout).Should(gomega.Succeed())
}
