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

	"golang.org/x/sync/errgroup"
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
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

func preloadQLBT12Schema(ctx context.Context, registry schema.Registry) error {
	if err := testmeasure.PreloadSchema(ctx, registry); err != nil {
		return err
	}
	original, err := registry.GetMeasure(ctx, &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName})
	if err != nil {
		return err
	}
	measure := proto.Clone(original).(*databasev1.Measure)
	measure.Metadata = &commonv1.Metadata{Name: qlbT12Measure, Group: benchGroupName}
	measure.TagFamilies = []*databasev1.TagFamilySpec{{Name: benchTagFamily, Tags: []*databasev1.TagSpec{
		{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "one", Type: databasev1.TagType_TAG_TYPE_INT},
		{Name: "key1024", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
	}}}
	measure.Entity = &databasev1.Entity{TagNames: []string{"host"}}
	measure.Interval = "1ms"
	measure.Fields = []*databasev1.FieldSpec{
		{
			Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		},
		{
			Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		},
	}
	measure.Fields = append(measure.Fields, &databasev1.FieldSpec{
		Name: "score", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
		EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	})
	_, err = registry.CreateMeasure(ctx, measure)
	return err
}

func qlbT12DataPoint(row int, base time.Time) *measurev1.DataPointValue {
	point := qlbT07DataPoint(row, base)
	point.Fields = append(point.Fields, intFieldValue(int64(row%100)))
	return point
}

func writeQLBT12Data(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (writeSummary, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	workers, writerCtx := errgroup.WithContext(ctx)
	rowsPerWriter := (cfg.Cardinality + cfg.Writers - 1) / cfg.Writers
	started := time.Now()
	for writer := 0; writer < cfg.Writers; writer++ {
		first, end := writer*rowsPerWriter, min((writer+1)*rowsPerWriter, cfg.Cardinality)
		if first >= end {
			continue
		}
		workers.Go(func() error { return writeQLBT12Range(writerCtx, client, first, end, base) })
	}
	if err := workers.Wait(); err != nil {
		return writeSummary{}, err
	}
	elapsed := time.Since(started)
	return writeSummary{Rows: cfg.Cardinality, Duration: elapsed, RowsPerSec: float64(cfg.Cardinality) / elapsed.Seconds()}, nil
}

func writeQLBT12Range(ctx context.Context, client measurev1.MeasureServiceClient, first, end int, base time.Time) error {
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Write(writeCtx)
	if err != nil {
		return fmt.Errorf("open T12 write stream: %w", err)
	}
	received := make(chan error, 1)
	run.Go(writeCtx, "querybench.t12-write-receive", logger.GetLogger("querybench"), func(context.Context) {
		for {
			response, receiveErr := stream.Recv()
			if errors.Is(receiveErr, io.EOF) {
				received <- nil
				return
			}
			if receiveErr != nil {
				received <- fmt.Errorf("receive T12 write acknowledgement: %w", receiveErr)
				return
			}
			if response.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				received <- fmt.Errorf("T12 write %d failed: %s", response.GetMessageId(), response.GetStatus())
				return
			}
		}
	})
	metadata := &commonv1.Metadata{Name: qlbT12Measure, Group: benchGroupName}
	spec := &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{{Name: benchTagFamily, TagNames: []string{"host", "one", "key1024", "rid"}}},
		FieldNames:    []string{"v", "f", "score"},
	}
	for row := first; row < end; row++ {
		request := &measurev1.WriteRequest{Metadata: metadata, DataPointSpec: spec, DataPoint: qlbT12DataPoint(row, base), MessageId: uint64(row + 1)}
		if row%10000 == 0 {
			// Duplicate series/timestamps exercise native version deduplication before local aggregation.
			if sendErr := stream.Send(request); sendErr != nil {
				return fmt.Errorf("send T12 duplicate version: %w", sendErr)
			}
			request.Metadata, request.DataPointSpec = nil, nil
		}
		if sendErr := stream.Send(request); sendErr != nil {
			return fmt.Errorf("send T12 row: %w", sendErr)
		}
		metadata, spec = nil, nil
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("close T12 write stream: %w", err)
	}
	return <-received
}

func waitForQLBT12Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, rows int, base time.Time) error {
	query := "SELECT score::FIELD AS result FROM " + benchGroupName + "." + qlbT12Measure + " WHERE time >= ? AND time < ?"
	request := &bydbqlv1.QueryRequest{Query: query, Params: []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(rows) * time.Millisecond))}},
	}}
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		response, err := client.Query(ctx, request)
		last = err
		if err == nil {
			last = checkQLBT12Scores(response.GetMeasureResult(), rows, base)
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
	return fmt.Errorf("T12 independent raw visibility: %w", last)
}

func checkQLBT12Scores(response *measurev1.QueryResponse, rows int, base time.Time) error {
	if len(response.GetDataPoints()) != rows {
		return fmt.Errorf("T12 score readiness rows=%d expected=%d", len(response.GetDataPoints()), rows)
	}
	seen := make([]bool, rows)
	for _, point := range response.DataPoints {
		nanos := point.GetTimestamp().AsTime().UnixNano() - base.UnixNano()
		row := nanos / int64(time.Millisecond)
		if nanos < 0 || nanos%int64(time.Millisecond) != 0 || row >= int64(rows) || seen[row] {
			return fmt.Errorf("T12 score readiness row identity mismatch")
		}
		seen[row] = true
		if len(point.Fields) != 1 || point.Fields[0].Name != "result" {
			return fmt.Errorf("T12 score readiness columns mismatch")
		}
		if err := checkQLBT10Value(point.Fields[0].Value, intFieldValue(row%100)); err != nil {
			return err
		}
	}
	return nil
}
