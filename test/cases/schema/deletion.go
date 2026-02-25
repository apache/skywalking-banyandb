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

// Package schema contains shared test cases for schema-related functionality.
package schema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// SharedContext is set by the test environment (standalone or distributed).
var SharedContext helpers.SharedContext

// Clients holds all necessary gRPC clients for deletion tests.
type Clients struct {
	GroupClient        databasev1.GroupRegistryServiceClient
	MeasureRegClient   databasev1.MeasureRegistryServiceClient
	StreamRegClient    databasev1.StreamRegistryServiceClient
	TraceRegClient     databasev1.TraceRegistryServiceClient
	MeasureWriteClient measurev1.MeasureServiceClient
	StreamWriteClient  streamv1.StreamServiceClient
	TraceWriteClient   tracev1.TraceServiceClient
}

// Shared test cases. Automatically registered when this package is imported.
var _ = g.Describe("Schema deletion", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		conn := SharedContext.Connection
		clients = &Clients{
			GroupClient:        databasev1.NewGroupRegistryServiceClient(conn),
			MeasureRegClient:   databasev1.NewMeasureRegistryServiceClient(conn),
			StreamRegClient:    databasev1.NewStreamRegistryServiceClient(conn),
			TraceRegClient:     databasev1.NewTraceRegistryServiceClient(conn),
			MeasureWriteClient: measurev1.NewMeasureServiceClient(conn),
			StreamWriteClient:  streamv1.NewStreamServiceClient(conn),
			TraceWriteClient:   tracev1.NewTraceServiceClient(conn),
		}
	})

	g.It("should delete measure correctly", func() {
		groupName := fmt.Sprintf("del-measure-%d", time.Now().UnixNano())
		measureName := "test_measure"

		g.By("Creating measure group")
		_, err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: groupName},
				Catalog:  commonv1.Catalog_CATALOG_MEASURE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum:        2,
					SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
					Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				},
			},
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Creating measure schema")
		err = createMeasureSchema(ctx, clients.MeasureRegClient, groupName, measureName)
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying measure deletion")
		err = VerifyMeasureDeletion(ctx, clients, groupName, measureName)
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	g.It("should delete stream correctly", func() {
		groupName := fmt.Sprintf("del-stream-%d", time.Now().UnixNano())
		streamName := "test_stream"

		g.By("Creating stream group")
		_, err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: groupName},
				Catalog:  commonv1.Catalog_CATALOG_STREAM,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum:        2,
					SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
					Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				},
			},
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Creating stream schema")
		err = createStreamSchema(ctx, clients.StreamRegClient, groupName, streamName)
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying stream deletion")
		err = VerifyStreamDeletion(ctx, clients, groupName, streamName)
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	g.It("should delete trace correctly", func() {
		groupName := fmt.Sprintf("del-trace-%d", time.Now().UnixNano())
		traceName := "test_trace"

		g.By("Creating trace group")
		_, err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: groupName},
				Catalog:  commonv1.Catalog_CATALOG_TRACE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum:        2,
					SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
					Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				},
			},
		})
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Creating trace schema")
		err = createTraceSchema(ctx, clients.TraceRegClient, groupName, traceName)
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		g.By("Verifying trace deletion")
		err = VerifyTraceDeletion(ctx, clients, groupName, traceName)
		gm.Expect(err).ShouldNot(gm.HaveOccurred())

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})

// VerifyMeasureDeletion implements the complete deletion test process for measures.
func VerifyMeasureDeletion(ctx context.Context, clients *Clients, groupName, measureName string) error {
	// Step 1: Write initial data to target measure
	if err := writeMeasureData(ctx, clients.MeasureWriteClient, groupName, measureName, 5); err != nil {
		return fmt.Errorf("step 1 failed - write initial data: %w", err)
	}

	// Step 2: Delete the measure
	deleteResp, err := clients.MeasureRegClient.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{
		Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
	})
	if err != nil {
		return fmt.Errorf("step 2 failed - delete measure: %w", err)
	}
	if !deleteResp.Deleted {
		return fmt.Errorf("step 2 failed - deletion not confirmed")
	}

	// Step 3: Verify rejection and invisibility
	if err := verifyMeasureDeletionEffects(ctx, clients, groupName, measureName); err != nil {
		return fmt.Errorf("step 3 failed: %w", err)
	}

	// Step 4 & 5: Write to different measure and verify
	secondMeasureName := measureName + "_second"
	if err := createMeasureSchema(ctx, clients.MeasureRegClient, groupName, secondMeasureName); err != nil {
		return fmt.Errorf("step 4 failed - create second measure: %w", err)
	}
	for i := 0; i < 20; i++ {
		if err := writeMeasureData(ctx, clients.MeasureWriteClient, groupName, secondMeasureName, 5); err != nil {
			return fmt.Errorf("step 4 failed - write batch %d: %w", i, err)
		}
	}
	time.Sleep(2 * time.Second)
	if err := verifyMeasureQuery(ctx, clients.MeasureWriteClient, groupName, secondMeasureName, 100); err != nil {
		return fmt.Errorf("step 5 failed - verify query: %w", err)
	}

	return nil
}

// VerifyStreamDeletion implements the complete deletion test process for streams.
func VerifyStreamDeletion(ctx context.Context, clients *Clients, groupName, streamName string) error {
	if err := writeStreamData(ctx, clients.StreamWriteClient, groupName, streamName, 5); err != nil {
		return fmt.Errorf("step 1 failed - write initial data: %w", err)
	}

	deleteResp, err := clients.StreamRegClient.Delete(ctx, &databasev1.StreamRegistryServiceDeleteRequest{
		Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
	})
	if err != nil {
		return fmt.Errorf("step 2 failed - delete stream: %w", err)
	}
	if !deleteResp.Deleted {
		return fmt.Errorf("step 2 failed - deletion not confirmed")
	}

	if err := verifyStreamDeletionEffects(ctx, clients, groupName, streamName); err != nil {
		return fmt.Errorf("step 3 failed: %w", err)
	}

	secondStreamName := streamName + "_second"
	if err := createStreamSchema(ctx, clients.StreamRegClient, groupName, secondStreamName); err != nil {
		return fmt.Errorf("step 4 failed - create second stream: %w", err)
	}
	for i := 0; i < 20; i++ {
		if err := writeStreamData(ctx, clients.StreamWriteClient, groupName, secondStreamName, 5); err != nil {
			return fmt.Errorf("step 4 failed - write batch %d: %w", i, err)
		}
	}
	time.Sleep(2 * time.Second)
	if err := verifyStreamQuery(ctx, clients.StreamWriteClient, groupName, secondStreamName, 100); err != nil {
		return fmt.Errorf("step 5 failed - verify query: %w", err)
	}

	return nil
}

// VerifyTraceDeletion implements the complete deletion test process for traces.
func VerifyTraceDeletion(ctx context.Context, clients *Clients, groupName, traceName string) error {
	if err := writeTraceData(ctx, clients.TraceWriteClient, groupName, traceName, 5); err != nil {
		return fmt.Errorf("step 1 failed - write initial data: %w", err)
	}

	deleteResp, err := clients.TraceRegClient.Delete(ctx, &databasev1.TraceRegistryServiceDeleteRequest{
		Metadata: &commonv1.Metadata{Name: traceName, Group: groupName},
	})
	if err != nil {
		return fmt.Errorf("step 2 failed - delete trace: %w", err)
	}
	if !deleteResp.Deleted {
		return fmt.Errorf("step 2 failed - deletion not confirmed")
	}

	if err := verifyTraceDeletionEffects(ctx, clients, groupName, traceName); err != nil {
		return fmt.Errorf("step 3 failed: %w", err)
	}

	secondTraceName := traceName + "_second"
	if err := createTraceSchema(ctx, clients.TraceRegClient, groupName, secondTraceName); err != nil {
		return fmt.Errorf("step 4 failed - create second trace: %w", err)
	}
	for i := 0; i < 20; i++ {
		if err := writeTraceData(ctx, clients.TraceWriteClient, groupName, secondTraceName, 5); err != nil {
			return fmt.Errorf("step 4 failed - write batch %d: %w", i, err)
		}
	}
	time.Sleep(2 * time.Second)
	if err := verifyTraceQuery(ctx, clients.TraceWriteClient, groupName, secondTraceName, 100); err != nil {
		return fmt.Errorf("step 5 failed - verify query: %w", err)
	}

	return nil
}

// Helper functions.

func createMeasureSchema(ctx context.Context, client databasev1.MeasureRegistryServiceClient, groupName, measureName string) error {
	_, err := client.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
			Entity:   &databasev1.Entity{TagNames: []string{"id"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: "default",
				Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}},
			}},
			Fields: []*databasev1.FieldSpec{{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			}},
		},
	})
	time.Sleep(2 * time.Second)
	return err
}

func createStreamSchema(ctx context.Context, client databasev1.StreamRegistryServiceClient, groupName, streamName string) error {
	_, err := client.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
		Stream: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
			Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: "default",
				Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}},
			}},
		},
	})
	time.Sleep(2 * time.Second)
	return err
}

func createTraceSchema(ctx context.Context, client databasev1.TraceRegistryServiceClient, groupName, traceName string) error {
	_, err := client.Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{
		Trace: &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: traceName, Group: groupName},
			Tags: []*databasev1.TraceTagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
				{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			},
			TraceIdTagName:   "trace_id",
			SpanIdTagName:    "span_id",
			TimestampTagName: "timestamp",
		},
	})
	time.Sleep(2 * time.Second)
	return err
}

func writeMeasureData(ctx context.Context, client measurev1.MeasureServiceClient, groupName, measureName string, count int) error {
	writeClient, err := client.Write(ctx)
	if err != nil {
		return err
	}
	metadata := &commonv1.Metadata{Name: measureName, Group: groupName}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < count; idx++ {
		if err := writeClient.Send(&measurev1.WriteRequest{
			Metadata: metadata,
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id_" + strconv.Itoa(idx)}},
					}},
				}},
				Fields: []*modelv1.FieldValue{{
					Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(idx * 100)}},
				}},
			},
			MessageId: uint64(time.Now().UnixNano() + int64(idx)),
		}); err != nil {
			return err
		}
	}
	if err := writeClient.CloseSend(); err != nil {
		return err
	}
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return recvErr
		}
		if resp != nil && resp.Status != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("write failed with status: %s", resp.Status)
		}
	}
	return nil
}

func writeStreamData(ctx context.Context, client streamv1.StreamServiceClient, groupName, streamName string, count int) error {
	writeClient, err := client.Write(ctx)
	if err != nil {
		return err
	}
	metadata := &commonv1.Metadata{Name: streamName, Group: groupName}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < count; idx++ {
		if err := writeClient.Send(&streamv1.WriteRequest{
			Metadata: metadata,
			Element: &streamv1.ElementValue{
				ElementId: strconv.Itoa(int(time.Now().UnixNano()) + idx),
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_" + strconv.Itoa(idx)}},
					}},
				}},
			},
			MessageId: uint64(time.Now().UnixNano() + int64(idx)),
		}); err != nil {
			return err
		}
	}
	if err := writeClient.CloseSend(); err != nil {
		return err
	}
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return recvErr
		}
		if resp != nil && resp.Status != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("write failed with status: %s", resp.Status)
		}
	}
	return nil
}

func writeTraceData(ctx context.Context, client tracev1.TraceServiceClient, groupName, traceName string, count int) error {
	writeClient, err := client.Write(ctx)
	if err != nil {
		return err
	}
	metadata := &commonv1.Metadata{Name: traceName, Group: groupName}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < count; idx++ {
		if err := writeClient.Send(&tracev1.WriteRequest{
			Metadata: metadata,
			Tags: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("trace_%d", time.Now().UnixNano()+int64(idx))}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("span_%d", time.Now().UnixNano()+int64(idx))}}},
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second))}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test_service"}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(idx * 10)}}},
			},
			Span:    []byte(fmt.Sprintf("span_data_%d", idx)),
			Version: uint64(idx + 1),
		}); err != nil {
			return err
		}
	}
	if err := writeClient.CloseSend(); err != nil {
		return err
	}
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return recvErr
		}
		if resp != nil && resp.Status != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("write failed with status: %s", resp.Status)
		}
	}
	return nil
}

func verifyMeasureDeletionEffects(ctx context.Context, clients *Clients, groupName, measureName string) error {
	metadata := &commonv1.Metadata{Name: measureName, Group: groupName}

	_, getErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: metadata})
	if getErr == nil {
		return fmt.Errorf("get should return error for deleted measure")
	}
	st, ok := status.FromError(getErr)
	if !ok || st.Code() != codes.NotFound {
		return fmt.Errorf("get should return NotFound, got: %v", st.Code())
	}

	existResp, existErr := clients.MeasureRegClient.Exist(ctx, &databasev1.MeasureRegistryServiceExistRequest{Metadata: metadata})
	if existErr != nil {
		return fmt.Errorf("exist call failed: %w", existErr)
	}
	if existResp.HasMeasure {
		return fmt.Errorf("exist should return false for deleted measure")
	}

	listResp, listErr := clients.MeasureRegClient.List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: groupName})
	if listErr != nil {
		return fmt.Errorf("list call failed: %w", listErr)
	}
	for _, m := range listResp.Measure {
		if m.Metadata.Name == measureName {
			return fmt.Errorf("deleted measure should not appear in list")
		}
	}

	if err := writeMeasureData(ctx, clients.MeasureWriteClient, groupName, measureName, 1); err == nil {
		return fmt.Errorf("write to deleted measure should fail")
	}

	return nil
}

func verifyStreamDeletionEffects(ctx context.Context, clients *Clients, groupName, streamName string) error {
	metadata := &commonv1.Metadata{Name: streamName, Group: groupName}

	_, getErr := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: metadata})
	if getErr == nil {
		return fmt.Errorf("get should return error for deleted stream")
	}
	st, ok := status.FromError(getErr)
	if !ok || st.Code() != codes.NotFound {
		return fmt.Errorf("get should return NotFound, got: %v", st.Code())
	}

	existResp, existErr := clients.StreamRegClient.Exist(ctx, &databasev1.StreamRegistryServiceExistRequest{Metadata: metadata})
	if existErr != nil {
		return fmt.Errorf("exist call failed: %w", existErr)
	}
	if existResp.HasStream {
		return fmt.Errorf("exist should return false for deleted stream")
	}

	listResp, listErr := clients.StreamRegClient.List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: groupName})
	if listErr != nil {
		return fmt.Errorf("list call failed: %w", listErr)
	}
	for _, s := range listResp.Stream {
		if s.Metadata.Name == streamName {
			return fmt.Errorf("deleted stream should not appear in list")
		}
	}

	if err := writeStreamData(ctx, clients.StreamWriteClient, groupName, streamName, 1); err == nil {
		return fmt.Errorf("write to deleted stream should fail")
	}

	return nil
}

func verifyTraceDeletionEffects(ctx context.Context, clients *Clients, groupName, traceName string) error {
	metadata := &commonv1.Metadata{Name: traceName, Group: groupName}

	_, getErr := clients.TraceRegClient.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{Metadata: metadata})
	if getErr == nil {
		return fmt.Errorf("get should return error for deleted trace")
	}
	st, ok := status.FromError(getErr)
	if !ok || st.Code() != codes.NotFound {
		return fmt.Errorf("get should return NotFound, got: %v", st.Code())
	}

	existResp, existErr := clients.TraceRegClient.Exist(ctx, &databasev1.TraceRegistryServiceExistRequest{Metadata: metadata})
	if existErr != nil {
		return fmt.Errorf("exist call failed: %w", existErr)
	}
	if existResp.HasTrace {
		return fmt.Errorf("exist should return false for deleted trace")
	}

	listResp, listErr := clients.TraceRegClient.List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: groupName})
	if listErr != nil {
		return fmt.Errorf("list call failed: %w", listErr)
	}
	for _, t := range listResp.Trace {
		if t.Metadata.Name == traceName {
			return fmt.Errorf("deleted trace should not appear in list")
		}
	}

	if err := writeTraceData(ctx, clients.TraceWriteClient, groupName, traceName, 1); err == nil {
		return fmt.Errorf("write to deleted trace should fail")
	}

	return nil
}

func verifyMeasureQuery(ctx context.Context, client measurev1.MeasureServiceClient, groupName, measureName string, _ int) error {
	now := time.Now()
	resp, err := client.Query(ctx, &measurev1.QueryRequest{
		Groups: []string{groupName},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-1 * time.Hour)),
			End:   timestamppb.New(now.Add(1 * time.Hour)),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"id"}},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"value"},
		},
	})
	if err != nil {
		return fmt.Errorf("measure query failed: %w", err)
	}
	if len(resp.DataPoints) == 0 {
		return fmt.Errorf("expected measure data points but got none")
	}
	return nil
}

func verifyStreamQuery(ctx context.Context, client streamv1.StreamServiceClient, groupName, streamName string, _ int) error {
	now := time.Now()
	resp, err := client.Query(ctx, &streamv1.QueryRequest{
		Groups: []string{groupName},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-1 * time.Hour)),
			End:   timestamppb.New(now.Add(1 * time.Hour)),
		},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"svc"}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("stream query failed: %w", err)
	}
	if len(resp.Elements) == 0 {
		return fmt.Errorf("expected stream elements but got none")
	}
	return nil
}

func verifyTraceQuery(ctx context.Context, client tracev1.TraceServiceClient, groupName, traceName string, _ int) error {
	now := time.Now()
	resp, err := client.Query(ctx, &tracev1.QueryRequest{
		Groups: []string{groupName},
		Name:   traceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-1 * time.Hour)),
			End:   timestamppb.New(now.Add(1 * time.Hour)),
		},
	})
	if err != nil {
		return fmt.Errorf("trace query failed: %w", err)
	}
	if len(resp.Traces) == 0 {
		return fmt.Errorf("expected trace data but got none")
	}
	return nil
}
