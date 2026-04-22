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

package benchmark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const (
	benchGroupName    = "bench_measure_group"
	benchMeasure      = "bench_measure"
	benchTagFamily    = "default"
	benchTagName      = "entity"
	benchFieldName    = "value"
	queryLimit        = 100
	stabilizeTimeout  = 3 * time.Minute
	writeReadyTimeout = 2 * time.Minute
)

func createMeasureSchema(ctx context.Context, conn *grpc.ClientConn, rf int) error {
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: benchGroupName},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        uint32(3),
				Replicas:        uint32(rf),
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			},
		},
	})
	if err != nil {
		return err
	}

	measureClient := databasev1.NewMeasureRegistryServiceClient(conn)
	_, err = measureClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: benchMeasure, Group: benchGroupName},
			Entity:   &databasev1.Entity{TagNames: []string{benchTagName}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: benchTagFamily,
				Tags: []*databasev1.TagSpec{{
					Name: benchTagName,
					Type: databasev1.TagType_TAG_TYPE_STRING,
				}},
			}},
			Fields: []*databasev1.FieldSpec{{
				Name:              benchFieldName,
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			}},
		},
	})
	return err
}

func writeMeasureData(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (WriteResult, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	writers := cfg.Writers
	entities := cfg.Entities
	points := cfg.PointsPerEntity

	entityPerWriter := int(math.Ceil(float64(entities) / float64(writers)))
	group, gctx := errgroup.WithContext(ctx)
	start := time.Now()

	for w := 0; w < writers; w++ {
		writerIndex := w
		startEntity := writerIndex * entityPerWriter
		endEntity := (writerIndex + 1) * entityPerWriter
		if endEntity > entities {
			endEntity = entities
		}
		if startEntity >= endEntity {
			continue
		}
		group.Go(func() error {
			stream, err := client.Write(gctx)
			if err != nil {
				return err
			}
			recvErr := make(chan error, 1)
			go func() {
				for {
					resp, recvErrInner := stream.Recv()
					if recvErrInner != nil {
						if errors.Is(recvErrInner, io.EOF) {
							recvErr <- nil
							return
						}
						recvErr <- recvErrInner
						return
					}
					if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
						recvErr <- fmt.Errorf("write failed for message_id=%d status=%s", resp.GetMessageId(), resp.GetStatus())
						return
					}
				}
			}()
			spec := &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{
					Name:     benchTagFamily,
					TagNames: []string{benchTagName},
				}},
				FieldNames: []string{benchFieldName},
			}
			metadata := &commonv1.Metadata{Name: benchMeasure, Group: benchGroupName}
			for entityIdx := startEntity; entityIdx < endEntity; entityIdx++ {
				entityID := fmt.Sprintf("entity-%d", entityIdx)
				for pointIdx := 0; pointIdx < points; pointIdx++ {
					messageID := uint64(entityIdx*points + pointIdx + 1)
					req := &measurev1.WriteRequest{
						Metadata:      metadata,
						DataPointSpec: spec,
						DataPoint: &measurev1.DataPointValue{
							Timestamp: timestamppb.New(base.Add(time.Duration(pointIdx) * time.Second)),
							TagFamilies: []*modelv1.TagFamilyForWrite{{
								Tags: []*modelv1.TagValue{{
									Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityID}},
								}},
							}},
							Fields: []*modelv1.FieldValue{{
								Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(pointIdx)}},
							}},
						},
						MessageId: messageID,
					}
					if err := stream.Send(req); err != nil {
						return err
					}
					metadata = nil
					spec = nil
				}
			}
			if err := stream.CloseSend(); err != nil {
				return err
			}
			return <-recvErr
		})
	}

	if err := group.Wait(); err != nil {
		return WriteResult{}, err
	}
	elapsed := time.Since(start)
	totalPoints := entities * points
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(totalPoints) / elapsed.Seconds()
	}
	return WriteResult{
		TotalPoints:   totalPoints,
		DurationSec:   elapsed.Seconds(),
		ThroughputPps: throughput,
	}, nil
}

func waitForWriteReady(ctx context.Context, conn *grpc.ClientConn, probes int) error {
	if probes <= 0 {
		probes = 1
	}
	client := measurev1.NewMeasureServiceClient(conn)
	deadline := time.Now().Add(writeReadyTimeout)
	var lastErr error

	for attempt := 0; time.Now().Before(deadline); attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ready := true
		for i := 0; i < probes; i++ {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := probeWrite(probeCtx, client, fmt.Sprintf("__bench-probe-%d-%d", i, attempt))
			cancel()
			if err != nil {
				ready = false
				lastErr = err
				break
			}
		}
		if ready {
			return nil
		}
		time.Sleep(2 * time.Second)
	}

	if lastErr != nil {
		return fmt.Errorf("write path not ready: %w", lastErr)
	}
	return fmt.Errorf("write path not ready before timeout")
}

func probeWrite(ctx context.Context, client measurev1.MeasureServiceClient, entityID string) error {
	stream, err := client.Write(ctx)
	if err != nil {
		return err
	}
	req := &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: benchMeasure, Group: benchGroupName},
		DataPointSpec: &measurev1.DataPointSpec{
			TagFamilySpec: []*measurev1.TagFamilySpec{{
				Name:     benchTagFamily,
				TagNames: []string{benchTagName},
			}},
			FieldNames: []string{benchFieldName},
		},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Second)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{
					Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityID}},
				}},
			}},
			Fields: []*modelv1.FieldValue{{
				Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}},
			}},
		},
		MessageId: 1,
	}
	if err := stream.Send(req); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("probe write failed status=%s", resp.GetStatus())
		}
	}
}

func waitForVisibility(ctx context.Context, conn *grpc.ClientConn, base time.Time, expected int) error {
	client := measurev1.NewMeasureServiceClient(conn)
	deadline := time.Now().Add(stabilizeTimeout)
	limit := uint32(queryLimit)
	if expected > queryLimit {
		limit = uint32(expected)
	}
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req := buildQueryRequest(base, "entity-0", limit)
		resp, err := client.Query(ctx, req)
		if err == nil && len(resp.DataPoints) >= expected {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for data visibility")
}

func runReadQueries(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) ([]time.Duration, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	nTargets := int(math.Min(20, float64(cfg.Entities)))
	if nTargets <= 0 {
		return nil, fmt.Errorf("no read targets configured")
	}
	entityTargets := make([]string, nTargets)
	for i := 0; i < nTargets; i++ {
		entityTargets[i] = fmt.Sprintf("entity-%d", i)
	}
	iterations := cfg.QueryIterations
	workers := cfg.QueryWorkers
	if workers <= 0 {
		workers = 1
	}
	jobs := make(chan string, iterations)
	for i := 0; i < iterations; i++ {
		jobs <- entityTargets[i%len(entityTargets)]
	}
	close(jobs)

	group, gctx := errgroup.WithContext(ctx)
	results := make(chan time.Duration, iterations)
	for w := 0; w < workers; w++ {
		group.Go(func() error {
			for entity := range jobs {
				req := buildQueryRequest(base, entity, queryLimit)
				start := time.Now()
				_, err := client.Query(gctx, req)
				if err != nil {
					return err
				}
				results <- time.Since(start)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	close(results)
	var durations []time.Duration
	for d := range results {
		durations = append(durations, d)
	}
	return durations, nil
}

func buildQueryRequest(base time.Time, entity string, limit uint32) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Groups: []string{benchGroupName},
		Name:   benchMeasure,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(-time.Minute)),
			End:   timestamppb.New(base.Add(48 * time.Hour)),
		},
		Criteria: &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name:  benchTagName,
					Op:    modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entity}}},
				},
			},
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{
				Name: benchTagFamily,
				Tags: []string{benchTagName},
			}},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldName}},
		Limit:           limit,
	}
}

func connectGRPC(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := grpc_health_v1.NewHealthClient(conn)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			_ = conn.Close()
			return nil, ctx.Err()
		}
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = client.Check(checkCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		cancel()
		if err == nil {
			return conn, nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	_ = conn.Close()
	return nil, fmt.Errorf("gRPC health check timeout for %s", addr)
}
