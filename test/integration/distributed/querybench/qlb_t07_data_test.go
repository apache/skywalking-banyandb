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

const qlbT07Measure = "qlb_t07"

var qlbT07Variants = []string{"ql_sum_scalar", "ql_multi_1024_groups", "ql_avg_one_group", "ql_shared_n_groups"}

func preloadQLBT07Schema(ctx context.Context, registry schema.Registry) error {
	if err := testmeasure.PreloadSchema(ctx, registry); err != nil {
		return err
	}
	original, err := registry.GetMeasure(ctx, &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName})
	if err != nil {
		return err
	}
	measure := proto.Clone(original).(*databasev1.Measure)
	measure.Metadata = &commonv1.Metadata{Name: qlbT07Measure, Group: benchGroupName}
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
	_, err = registry.CreateMeasure(ctx, measure)
	return err
}

func qlbT07Key(row int) *modelv1.TagValue {
	switch row % 1024 {
	case 0:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	case 1:
		return strTagValue("")
	default:
		return strTagValue(fmt.Sprintf("key_%04d", row%1024))
	}
}

func qlbT07Values(row int) (int64, bool, float64, bool) {
	return int64(row%97 - 48), row%5 != 0, float64(row%29-14) / 4, row%7 != 0
}

func qlbT07DataPoint(row int, base time.Time) *measurev1.DataPointValue {
	integer, integerValid, floating, floatingValid := qlbT07Values(row)
	integerValue := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	if integerValid {
		integerValue = intFieldValue(integer)
	}
	floatValue := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	if floatingValid {
		floatValue = qlbT07Float(floating)
	}
	return &measurev1.DataPointValue{
		Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
		TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", row%16)),
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
			qlbT07Key(row),
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(row)}}},
		}}},
		Fields: []*modelv1.FieldValue{integerValue, floatValue},
	}
}

func qlbT07Float(value float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: value}}}
}

func writeQLBT07Data(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (writeSummary, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	workers, writerCtx := errgroup.WithContext(ctx)
	rowsPerWriter := (cfg.Cardinality + cfg.Writers - 1) / cfg.Writers
	started := time.Now()
	for writer := 0; writer < cfg.Writers; writer++ {
		first, end := writer*rowsPerWriter, min((writer+1)*rowsPerWriter, cfg.Cardinality)
		if first >= end {
			continue
		}
		workers.Go(func() error { return writeQLBT07Range(writerCtx, client, first, end, base) })
	}
	if err := workers.Wait(); err != nil {
		return writeSummary{}, err
	}
	elapsed := time.Since(started)
	return writeSummary{Rows: cfg.Cardinality, Duration: elapsed, RowsPerSec: float64(cfg.Cardinality) / elapsed.Seconds()}, nil
}

func writeQLBT07Range(ctx context.Context, client measurev1.MeasureServiceClient, first, end int, base time.Time) error {
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Write(writeCtx)
	if err != nil {
		return fmt.Errorf("open T07 write stream: %w", err)
	}
	received := make(chan error, 1)
	run.Go(writeCtx, "querybench.t07-write-receive", logger.GetLogger("querybench"), func(context.Context) {
		for {
			response, receiveErr := stream.Recv()
			if errors.Is(receiveErr, io.EOF) {
				received <- nil
				return
			}
			if receiveErr != nil {
				received <- fmt.Errorf("receive T07 write acknowledgement: %w", receiveErr)
				return
			}
			if response.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				received <- fmt.Errorf("T07 write %d failed: %s", response.GetMessageId(), response.GetStatus())
				return
			}
		}
	})
	metadata := &commonv1.Metadata{Name: qlbT07Measure, Group: benchGroupName}
	spec := &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{{Name: benchTagFamily, TagNames: []string{"host", "one", "key1024", "rid"}}},
		FieldNames:    []string{"v", "f"},
	}
	for row := first; row < end; row++ {
		request := &measurev1.WriteRequest{Metadata: metadata, DataPointSpec: spec, DataPoint: qlbT07DataPoint(row, base), MessageId: uint64(row + 1)}
		if row%10000 == 0 {
			// Duplicate series/timestamps exercise native version deduplication before local aggregation.
			if sendErr := stream.Send(request); sendErr != nil {
				return fmt.Errorf("send T07 duplicate version: %w", sendErr)
			}
			request.Metadata, request.DataPointSpec = nil, nil
		}
		if sendErr := stream.Send(request); sendErr != nil {
			return fmt.Errorf("send T07 row: %w", sendErr)
		}
		metadata, spec = nil, nil
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("close T07 write stream: %w", err)
	}
	return <-received
}

func qlbT07Query(name string, cardinality int, base time.Time) (*bydbqlv1.QueryRequest, error) {
	projection, group, limit := "", "", ""
	switch name {
	case "ql_sum_scalar":
		projection = "SUM(v)::FIELD AS sum_value"
	case "ql_multi_1024_groups":
		projection = "key1024, COUNT(v)::FIELD AS count_value, SUM(v)::FIELD AS sum_value, MIN(v)::FIELD AS min_value, MAX(v)::FIELD AS max_value"
		group = " GROUP BY key1024"
	case "ql_avg_one_group":
		projection, group = "one, AVG(f)::FIELD AS avg_value", " GROUP BY one"
	case "ql_shared_n_groups":
		projection = "rid, SUM(f)::FIELD AS sum_value, COUNT(f)::FIELD AS count_value, AVG(f)::FIELD AS avg_value"
		group, limit = " GROUP BY rid", " LIMIT 1"
	default:
		return nil, fmt.Errorf("unknown T07 variant %q", name)
	}
	return &bydbqlv1.QueryRequest{
		Query: "SELECT " + projection + " FROM " + benchGroupName + "." + qlbT07Measure + " WHERE time >= ? AND time < ?" + group + limit,
		Params: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
		},
	}, nil
}
