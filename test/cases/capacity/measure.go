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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	pointCount        = 8000
	pointPayload      = 8 << 10
	pointEntities     = 8
	pointCountSlice   = 32
	measureName       = "service_cpm_minute"
	measureGroup      = "sw_metric"
	endpointCount     = 20000
	endpointCountStep = 50
	endpointMeasure   = "service_traffic"
	endpointGroup     = "index_mode"
	// endpointPitch is the time between two documents; the windows of Count sit halfway between documents, so no document
	// is on the edge of two windows.
	endpointPitch = 10 * time.Millisecond
)

// MeasurePoints is a measure of 8000 data points of 8 KiB spread over 8 entities, about 63 MiB, queried newest first: a
// measure whose points are large, so the limit has to bound what is read.
func MeasurePoints(poolBytes uint64) *Scenario {
	return &Scenario{
		Name:      "measure, newest points first",
		PoolBytes: poolBytes,
		Total:     pointCount,
		Write:     writePoints,
		Count: func(ctx context.Context, conn *grpc.ClientConn, base time.Time) (int, error) {
			total := 0
			for from := 0; from < pointCount; from += pointCountSlice {
				resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, pointQuery(base, from, from+pointCountSlice, pointCountSlice))
				if err != nil {
					return 0, err
				}
				total += len(resp.GetDataPoints())
			}
			return total, nil
		},
		Probe: func(ctx context.Context, conn *grpc.ClientConn, base time.Time, limit int) (Outcome, error) {
			resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, pointQuery(base, 0, pointCount, limit))
			if err != nil {
				return Outcome{}, refused(err)
			}
			return measureOutcome(resp), nil
		},
	}
}

// MeasureIndexMode is an index-mode measure of 20000 small documents, one series each, queried with a limit and no order:
// the shape of the endpoint list whose limit used to be ignored until every document had been read.
func MeasureIndexMode(poolBytes uint64) *Scenario {
	return &Scenario{
		Name:      "measure, index mode, limit without order",
		PoolBytes: poolBytes,
		Total:     endpointCount,
		Write:     writeEndpoints,
		Count: func(ctx context.Context, conn *grpc.ClientConn, base time.Time) (int, error) {
			total := 0
			for from := 0; from < endpointCount; from += endpointCountStep {
				resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, endpointQuery(base, from, from+endpointCountStep, endpointCountStep))
				if err != nil {
					return 0, err
				}
				total += len(resp.GetDataPoints())
			}
			return total, nil
		},
		Probe: func(ctx context.Context, conn *grpc.ClientConn, base time.Time, limit int) (Outcome, error) {
			resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, endpointQuery(base, 0, endpointCount, limit))
			if err != nil {
				return Outcome{}, refused(err)
			}
			return measureOutcome(resp), nil
		},
	}
}

func measureOutcome(resp *measurev1.QueryResponse) Outcome {
	outcome := Outcome{Returned: len(resp.GetDataPoints())}
	for _, point := range resp.GetDataPoints() {
		for _, family := range point.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				outcome.Bytes += len(tag.GetValue().GetStr().GetValue())
			}
		}
	}
	return outcome
}

func pointQuery(base time.Time, from, to, limit int) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:   measureName,
		Groups: []string{measureGroup},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(time.Duration(from)*time.Second - 500*time.Millisecond)),
			End:   timestamppb.New(base.Add(time.Duration(to)*time.Second - 500*time.Millisecond)),
		},
		Limit:   uint32(limit),
		OrderBy: &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{"id", "entity_id"}},
		}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"total"}},
	}
}

func endpointQuery(base time.Time, from, to, limit int) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:   endpointMeasure,
		Groups: []string{endpointGroup},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(time.Duration(from)*endpointPitch - endpointPitch/2)),
			End:   timestamppb.New(base.Add(time.Duration(to)*endpointPitch - endpointPitch/2)),
		},
		Limit: uint32(limit),
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{"id", "name"}},
		}},
	}
}

func writePoints(conn *grpc.ClientConn, base time.Time) {
	payload := make([]byte, pointPayload)
	for i := range payload {
		payload[i] = 'a' + byte(i*31%26)
	}
	value := string(payload)
	writeMeasure(conn, pointCount, func(i int) *measurev1.DataPointValue {
		return &measurev1.DataPointValue{
			Timestamp: timestamppb.New(base.Add(time.Duration(i) * time.Second)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
				strValue(value), strValue(fmt.Sprintf("entity-%d", i%pointEntities)),
			}}},
			Fields: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i)}}},
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}},
			},
		}
	}, &commonv1.Metadata{Name: measureName, Group: measureGroup})
}

func writeEndpoints(conn *grpc.ClientConn, base time.Time) {
	writeMeasure(conn, endpointCount, func(i int) *measurev1.DataPointValue {
		return &measurev1.DataPointValue{
			Timestamp: timestamppb.New(base.Add(time.Duration(i) * endpointPitch)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
				strValue(fmt.Sprintf("endpoint-%06d", i)), strValue(fmt.Sprintf("service-%d", i%50)),
				strValue(fmt.Sprintf("/api/v1/resource/%06d", i)), strValue("short"), strValue("group"),
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
			}}},
		}
	}, &commonv1.Metadata{Name: endpointMeasure, Group: endpointGroup})
}

func strValue(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

// writeMeasure sends count data points and requires every one to be accepted.
func writeMeasure(conn *grpc.ClientConn, count int, point func(i int) *measurev1.DataPointValue, metadata *commonv1.Metadata) {
	client := measurev1.NewMeasureServiceClient(conn)
	gomega.Eventually(func(innerGm gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*flags.EventuallyTimeout)
		defer cancel()
		writeClient, writeErr := client.Write(ctx)
		innerGm.Expect(writeErr).NotTo(gomega.HaveOccurred())
		for i := 0; i < count; i++ {
			sendErr := writeClient.Send(&measurev1.WriteRequest{
				Metadata:  metadata,
				MessageId: uint64(time.Now().UnixNano()) + uint64(i),
				DataPoint: point(i),
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
		innerGm.Expect(succeeded).To(gomega.Equal(count), "every data point must be accepted")
	}, 3*flags.EventuallyTimeout).Should(gomega.Succeed())
}
