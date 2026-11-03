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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package querybench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

func preloadQLBT14Schema(ctx context.Context, registry schema.Registry) error {
	if err := preloadQLBT07Schema(ctx, registry); err != nil {
		return err
	}
	original, err := registry.GetMeasure(ctx, &commonv1.Metadata{Name: qlbT07Measure, Group: benchGroupName})
	if err != nil {
		return err
	}
	measure := proto.Clone(original).(*databasev1.Measure)
	measure.Metadata = &commonv1.Metadata{Name: qlbT14MembershipMeasure, Group: benchGroupName}
	measure.TagFamilies = []*databasev1.TagFamilySpec{{Name: benchTagFamily, Tags: []*databasev1.TagSpec{
		{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "width", Type: databasev1.TagType_TAG_TYPE_INT},
		{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
	}}}
	// Width is an entity tag so native pruning does not require an array index.
	measure.Entity.TagNames = []string{"host", "width"}
	measure.Fields = measure.Fields[1:2]
	_, err = registry.CreateMeasure(ctx, measure)
	return err
}

func writeQLBT14Data(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (writeSummary, error) {
	summary, err := writeQLBT07Data(ctx, conn, cfg, base)
	if err != nil || cfg.Cardinality != 1024 {
		return summary, err
	}
	started := time.Now()
	writeCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	stream, err := measurev1.NewMeasureServiceClient(conn).Write(writeCtx)
	if err != nil {
		return summary, err
	}
	received := make(chan error, 1)
	run.Go(writeCtx, "querybench.t14-write-receive", logger.GetLogger("querybench"), func(context.Context) {
		for {
			response, receiveErr := stream.Recv()
			if errors.Is(receiveErr, io.EOF) {
				received <- nil
				return
			}
			if receiveErr != nil {
				received <- fmt.Errorf("T14 membership acknowledgement: %w", receiveErr)
				return
			}
			if response.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				received <- fmt.Errorf("T14 membership write %d failed: %s", response.GetMessageId(), response.GetStatus())
				return
			}
		}
	})
	for row := 0; row < qlbT14MembershipRows; row++ {
		width := []int{0, 1, 16, 256}[row%4]
		ordinal := row / 4
		labels := make([]string, width)
		for index := range labels {
			labels[index] = fmt.Sprintf("item_%03d", index)
		}
		if width > 0 && ordinal%2 == 0 {
			labels[0] = "needle"
		}
		request := &measurev1.WriteRequest{MessageId: uint64(row + 1), DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
				strTagValue(fmt.Sprintf("member_%02d", ordinal%16)),
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(width)}}},
				{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: labels}}},
			}}}, Fields: []*modelv1.FieldValue{qlbT07Float(1)},
		}}
		if row == 0 {
			request.Metadata = &commonv1.Metadata{Name: qlbT14MembershipMeasure, Group: benchGroupName}
			request.DataPointSpec = &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: benchTagFamily, TagNames: []string{"host", "width", "labels"}}},
				FieldNames:    []string{"f"},
			}
		}
		if sendErr := stream.Send(request); sendErr != nil {
			return summary, fmt.Errorf("T14 membership write: %w", sendErr)
		}
	}
	if err = stream.CloseSend(); err != nil {
		return summary, err
	}
	if err = <-received; err != nil {
		return summary, err
	}
	summary.Rows += qlbT14MembershipRows
	summary.Duration += time.Since(started)
	summary.RowsPerSec = float64(summary.Rows) / summary.Duration.Seconds()
	return summary, nil
}

func waitForQLBT14Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cardinality int, base time.Time) error {
	// Bounded, independent raw reads prove every original input row is visible,
	// including for large native aggregate probes which return only a few groups.
	if err := waitForQLBT07RawVisibility(ctx, client, cardinality, 8192, base); err != nil {
		return err
	}
	if cardinality != 1024 {
		return nil
	}
	request := &bydbqlv1.QueryRequest{Query: "SELECT host, width, labels, f::FIELD FROM " + benchGroupName + "." + qlbT14MembershipMeasure +
		" WHERE time >= ? AND time < ?", Params: []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(qlbT14MembershipRows * time.Millisecond))}},
	}}
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		response, err := client.Query(ctx, request)
		last = err
		if err == nil {
			last = checkQLBT14MembershipFixture(response.GetMeasureResult(), base)
			if last == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("T14 membership visibility: %w", last)
}

func checkQLBT14MembershipFixture(response *measurev1.QueryResponse, base time.Time) error {
	if len(response.GetDataPoints()) != qlbT14MembershipRows {
		return fmt.Errorf("T14 membership fixture incomplete")
	}
	seen := make([]bool, qlbT14MembershipRows)
	for _, point := range response.DataPoints {
		nanos := point.GetTimestamp().AsTime().UnixNano() - base.UnixNano()
		row := int(nanos / int64(time.Millisecond))
		if nanos < 0 || nanos%int64(time.Millisecond) != 0 || row >= len(seen) || seen[row] {
			return fmt.Errorf("T14 membership fixture duplicate/invalid row")
		}
		seen[row] = true
		if len(point.TagFamilies) != 1 || len(point.TagFamilies[0].Tags) != 3 || len(point.Fields) != 1 {
			return fmt.Errorf("T14 membership fixture shape differs")
		}
		tags := point.TagFamilies[0].Tags
		width := []int{0, 1, 16, 256}[row%4]
		labels := tags[2].Value.GetStrArray().GetValue()
		if tags[0].Value.GetStr().GetValue() != fmt.Sprintf("member_%02d", row/4%16) || tags[1].Value.GetInt().GetValue() != int64(width) || len(labels) != width ||
			point.Fields[0].Value.GetFloat().GetValue() != 1 {
			return fmt.Errorf("T14 membership fixture values differ")
		}
		for index, value := range labels {
			expected := fmt.Sprintf("item_%03d", index)
			if index == 0 && row/4%2 == 0 {
				expected = "needle"
			}
			if value != expected {
				return fmt.Errorf("T14 membership fixture label differs")
			}
		}
	}
	return nil
}
