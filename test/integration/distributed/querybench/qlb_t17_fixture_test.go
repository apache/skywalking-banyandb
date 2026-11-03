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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

func preloadQLBT17Schema(ctx context.Context, registry schema.Registry) error {
	if err := testmeasure.PreloadSchema(ctx, registry); err != nil {
		return err
	}
	original, err := registry.GetMeasure(ctx, &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName})
	if err != nil {
		return err
	}
	measure := proto.Clone(original).(*databasev1.Measure)
	measure.Metadata = &commonv1.Metadata{Name: qlbT17Measure, Group: benchGroupName}
	measure.TagFamilies = []*databasev1.TagFamilySpec{{Name: benchTagFamily, Tags: []*databasev1.TagSpec{
		{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
	}}}
	measure.Entity = &databasev1.Entity{TagNames: []string{"host"}}
	measure.Interval = "1ms"
	measure.Fields = nil
	for _, name := range []string{"rid", "key0", "key1", "v"} {
		measure.Fields = append(measure.Fields, &databasev1.FieldSpec{
			Name: name, FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		})
	}
	_, err = registry.CreateMeasure(ctx, measure)
	return err
}

func qlbT17DataPoint(row int, base time.Time) *measurev1.DataPointValue {
	key0, key1, valid0, valid1 := qlbT17Keys(row)
	first := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	second := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	if valid0 {
		first = intFieldValue(key0)
	}
	if valid1 {
		second = intFieldValue(key1)
	}
	if row%5 != 0 {
		value = intFieldValue(int64(row%97 - 48))
	}
	return &measurev1.DataPointValue{
		Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
		TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", row%16)),
		}}}, Fields: []*modelv1.FieldValue{intFieldValue(int64(row)), first, second, value},
	}
}

func writeQLBT17Data(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (writeSummary, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	workers, writerCtx := errgroup.WithContext(ctx)
	rowsPerWriter := (cfg.Cardinality + cfg.Writers - 1) / cfg.Writers
	started := time.Now()
	for writer := 0; writer < cfg.Writers; writer++ {
		first, end := writer*rowsPerWriter, min((writer+1)*rowsPerWriter, cfg.Cardinality)
		if first >= end {
			continue
		}
		workers.Go(func() error { return writeQLBT17Range(writerCtx, client, first, end, base) })
	}
	if err := workers.Wait(); err != nil {
		return writeSummary{}, err
	}
	elapsed := time.Since(started)
	return writeSummary{Rows: cfg.Cardinality, Duration: elapsed, RowsPerSec: float64(cfg.Cardinality) / elapsed.Seconds()}, nil
}

func writeQLBT17Range(ctx context.Context, client measurev1.MeasureServiceClient, first, end int, base time.Time) error {
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Write(writeCtx)
	if err != nil {
		return fmt.Errorf("open T17 write stream: %w", err)
	}
	received := make(chan error, 1)
	run.Go(writeCtx, "querybench.t16-write-receive", logger.GetLogger("querybench"), func(context.Context) {
		for {
			response, receiveErr := stream.Recv()
			if errors.Is(receiveErr, io.EOF) {
				received <- nil
				return
			}
			if receiveErr != nil {
				received <- fmt.Errorf("receive T17 write acknowledgement: %w", receiveErr)
				return
			}
			if response.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				received <- fmt.Errorf("T17 write %d failed: %s", response.GetMessageId(), response.GetStatus())
				return
			}
		}
	})
	metadata := &commonv1.Metadata{Name: qlbT17Measure, Group: benchGroupName}
	pointSpec := &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{{Name: benchTagFamily, TagNames: []string{"host"}}},
		FieldNames:    []string{"rid", "key0", "key1", "v"},
	}
	for row := first; row < end; row++ {
		request := &measurev1.WriteRequest{Metadata: metadata, DataPointSpec: pointSpec, DataPoint: qlbT17DataPoint(row, base), MessageId: uint64(row + 1)}

		if row%10000 == 0 {
			// Repeat the same series/timestamp so native version deduplication
			// remains part of every owning group-key workload.
			if sendErr := stream.Send(request); sendErr != nil {
				return fmt.Errorf("send T17 duplicate version: %w", sendErr)
			}
			request.Metadata, request.DataPointSpec = nil, nil
		}
		if sendErr := stream.Send(request); sendErr != nil {
			return fmt.Errorf("send T17 row: %w", sendErr)
		}
		metadata, pointSpec = nil, nil
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("close T17 write stream: %w", err)
	}
	return <-received
}
