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
	"hash/fnv"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const (
	benchGroupName   = "sw_metric"
	benchMeasureName = "service_cpm_minute"
	benchTagFamily   = "default"
	benchTagID       = "id"
	benchTagEntityID = "entity_id"
	benchFieldTotal  = "total"
	benchFieldValue  = "value"
	benchServiceMod  = 16
)

type writeSummary struct {
	Rows       int
	Duration   time.Duration
	RowsPerSec float64
}

type queryRunSummary struct {
	Latencies     []time.Duration
	SampleDPText  string
	Rows          int
	Hash          uint64
	Elapsed       time.Duration
}

func writeBenchmarkData(ctx context.Context, conn *grpc.ClientConn, cfg Config, cardinality int, base time.Time) (writeSummary, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	entities, pointsEach := splitCardinality(cardinality)
	writers := cfg.Writers
	if writers <= 0 {
		writers = 1
	}
	entitiesPerWriter := int(math.Ceil(float64(entities) / float64(writers)))
	group, groupCtx := errgroup.WithContext(ctx)
	started := time.Now()
	for writerIdx := 0; writerIdx < writers; writerIdx++ {
		writerIndex := writerIdx
		startEntity := writerIndex * entitiesPerWriter
		endEntity := (writerIndex + 1) * entitiesPerWriter
		if endEntity > entities {
			endEntity = entities
		}
		if startEntity >= endEntity {
			continue
		}
		group.Go(func() error {
			return writeEntityRange(groupCtx, client, startEntity, endEntity, pointsEach, base)
		})
	}
	if waitErr := group.Wait(); waitErr != nil {
		return writeSummary{}, waitErr
	}
	elapsed := time.Since(started)
	rows := entities * pointsEach
	rowsPerSec := 0.0
	if elapsed > 0 {
		rowsPerSec = float64(rows) / elapsed.Seconds()
	}
	return writeSummary{Rows: rows, Duration: elapsed, RowsPerSec: rowsPerSec}, nil
}

func writeEntityRange(ctx context.Context, client measurev1.MeasureServiceClient, startEntity, endEntity, pointsEach int, base time.Time) error {
	stream, streamErr := client.Write(ctx)
	if streamErr != nil {
		return fmt.Errorf("open measure write stream: %w", streamErr)
	}
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			resp, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				recvErrCh <- nil
				return
			}
			if recvErr != nil {
				recvErrCh <- fmt.Errorf("receive measure write response: %w", recvErr)
				return
			}
			if resp.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
				recvErrCh <- fmt.Errorf("write failed for message_id=%d status=%s", resp.GetMessageId(), resp.GetStatus())
				return
			}
		}
	}()
	metadata := &commonv1.Metadata{Name: benchMeasureName, Group: benchGroupName}
	spec := benchmarkDataPointSpec()
	for entityIdx := startEntity; entityIdx < endEntity; entityIdx++ {
		serviceID := fmt.Sprintf("svc%d", entityIdx%benchServiceMod)
		entityID := fmt.Sprintf("entity_%d", entityIdx)
		for pointIdx := 0; pointIdx < pointsEach; pointIdx++ {
			messageID := uint64(entityIdx*pointsEach + pointIdx + 1)
			req := &measurev1.WriteRequest{
				Metadata:      metadata,
				DataPointSpec: spec,
				DataPoint:     benchmarkDataPoint(base, serviceID, entityID, entityIdx, pointIdx),
				MessageId:     messageID,
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return fmt.Errorf("send measure write request: %w", sendErr)
			}
			metadata = nil
			spec = nil
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return fmt.Errorf("close measure write stream: %w", closeErr)
	}
	if recvErr := <-recvErrCh; recvErr != nil {
		return recvErr
	}
	return nil
}

func benchmarkDataPointSpec() *measurev1.DataPointSpec {
	return &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{{
			Name:     benchTagFamily,
			TagNames: []string{benchTagID, benchTagEntityID},
		}},
		FieldNames: []string{benchFieldTotal, benchFieldValue},
	}
}

func benchmarkDataPoint(base time.Time, serviceID, entityID string, entityIdx, pointIdx int) *measurev1.DataPointValue {
	return &measurev1.DataPointValue{
		Timestamp: timestamppb.New(base.Add(time.Duration(pointIdx) * time.Second)),
		TagFamilies: []*modelv1.TagFamilyForWrite{{
			Tags: []*modelv1.TagValue{
				strTagValue(serviceID),
				strTagValue(entityID),
			},
		}},
		Fields: []*modelv1.FieldValue{
			intFieldValue(int64(100 + pointIdx%100)),
			intFieldValue(int64((entityIdx%benchServiceMod+1)*1000 + pointIdx%1000)),
		},
	}
}

func buildScenarioQuery(scenario Scenario, cardinality int, base time.Time) (*measurev1.QueryRequest, error) {
	_, pointsEach := splitCardinality(cardinality)
	req := &measurev1.QueryRequest{
		Groups: []string{benchGroupName},
		Name:   benchMeasureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(-time.Minute)),
			End:   timestamppb.New(base.Add(time.Duration(pointsEach)*time.Second + time.Minute)),
		},
	}
	switch scenario {
	case ScenarioScanAll:
		req.TagProjection = tagProjection(benchTagID, benchTagEntityID)
		req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldTotal, benchFieldValue}}
		req.Limit = uint32(cardinality)
	case ScenarioTopWithFilter:
		req.TagProjection = tagProjection(benchTagID)
		req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldValue}}
		req.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name:  benchTagID,
			Op:    modelv1.Condition_BINARY_OP_NE,
			Value: strTagValue("svc3"),
		}}}
		req.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: tagProjection(benchTagID), FieldName: benchFieldValue}
		req.Agg = &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN, FieldName: benchFieldValue}
		req.Top = &measurev1.QueryRequest_Top{Number: 2, FieldName: benchFieldValue, FieldValueSort: modelv1.Sort_SORT_DESC}
		// Keep the response bounded by the Top-N fixture shape while all matching rows still contribute to aggregation.
		req.Limit = 2
	default:
		return nil, fmt.Errorf("unsupported scenario %q", scenario)
	}
	return req, nil
}

func waitForScenarioVisibility(ctx context.Context, conn *grpc.ClientConn, req *measurev1.QueryRequest, scenario Scenario, cardinality int) error {
	client := measurev1.NewMeasureServiceClient(conn)
	deadline := time.Now().Add(2 * time.Minute)
	expectedRows := cardinality
	if scenario == ScenarioTopWithFilter {
		expectedRows = int(req.GetTop().GetNumber())
	}
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		queryReq := proto.Clone(req).(*measurev1.QueryRequest)
		queryCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
		resp, queryErr := client.Query(queryCtx, queryReq)
		cancel()
		if queryErr == nil && len(resp.GetDataPoints()) >= expectedRows {
			return nil
		}
		if queryErr != nil {
			lastErr = queryErr
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %s visibility: %w", scenario, lastErr)
	}
	return fmt.Errorf("timeout waiting for %s visibility with at least %d rows", scenario, expectedRows)
}

func runScenarioQueries(ctx context.Context, conn *grpc.ClientConn, req *measurev1.QueryRequest, cfg Config, cardinality int) (queryRunSummary, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	for warmupIdx := 0; warmupIdx < cfg.WarmupIterations; warmupIdx++ {
		warmupCtx, cancel := context.WithTimeout(ctx, queryTimeout(cardinality))
		_, warmupErr := client.Query(warmupCtx, proto.Clone(req).(*measurev1.QueryRequest))
		cancel()
		if warmupErr != nil {
			return queryRunSummary{}, fmt.Errorf("warmup query %d failed: %w", warmupIdx, warmupErr)
		}
	}
	// One serial sample query captures a prototext of the first DataPoint
	// before the timed parallel loop starts. The merge pass dumps the row
	// and vec samples side by side when the correctness gate fires so we
	// can see what diverged (TagFamily order, oneof variant, etc.) without
	// re-instrumenting the harness.
	var sampleDPText string
	sampleCtx, sampleCancel := context.WithTimeout(ctx, queryTimeout(cardinality))
	sampleResp, sampleErr := client.Query(sampleCtx, proto.Clone(req).(*measurev1.QueryRequest))
	sampleCancel()
	if sampleErr != nil {
		return queryRunSummary{}, fmt.Errorf("sample query failed: %w", sampleErr)
	}
	if dps := sampleResp.GetDataPoints(); len(dps) > 0 {
		var b strings.Builder
		for idx, dp := range dps {
			fmt.Fprintf(&b, "# datapoint[%d]\n%s", idx, prototext.Format(dp))
		}
		sampleDPText = b.String()
	}
	iterations := cfg.QueryIterations
	jobs := make(chan int, iterations)
	for iteration := 0; iteration < iterations; iteration++ {
		jobs <- iteration
	}
	close(jobs)
	latencies := make(chan time.Duration, iterations)
	rows := make(chan int, iterations)
	hashes := make(chan uint64, iterations)
	group, groupCtx := errgroup.WithContext(ctx)
	started := time.Now()
	for workerIdx := 0; workerIdx < cfg.QueryWorkers; workerIdx++ {
		group.Go(func() error {
			for range jobs {
				queryReq := proto.Clone(req).(*measurev1.QueryRequest)
				queryCtx, cancel := context.WithTimeout(groupCtx, queryTimeout(cardinality))
				queryStart := time.Now()
				resp, queryErr := client.Query(queryCtx, queryReq)
				latency := time.Since(queryStart)
				cancel()
				if queryErr != nil {
					return fmt.Errorf("query failed: %w", queryErr)
				}
				latencies <- latency
				rows <- len(resp.GetDataPoints())
				hashes <- hashResponse(resp)
			}
			return nil
		})
	}
	if waitErr := group.Wait(); waitErr != nil {
		return queryRunSummary{}, waitErr
	}
	elapsed := time.Since(started)
	close(latencies)
	close(rows)
	close(hashes)
	out := queryRunSummary{Elapsed: elapsed}
	for latency := range latencies {
		out.Latencies = append(out.Latencies, latency)
	}
	firstRows := -1
	for rowCount := range rows {
		if firstRows < 0 {
			firstRows = rowCount
			continue
		}
		if rowCount != firstRows {
			return queryRunSummary{}, fmt.Errorf("query row counts diverged across iterations: first=%d got=%d", firstRows, rowCount)
		}
	}
	if firstRows >= 0 {
		out.Rows = firstRows
	}
	hashSet := false
	var firstHash uint64
	for hashValue := range hashes {
		if !hashSet {
			firstHash = hashValue
			hashSet = true
			continue
		}
		if hashValue != firstHash {
			return queryRunSummary{}, fmt.Errorf("query response hashes diverged across iterations: first=0x%x got=0x%x", firstHash, hashValue)
		}
	}
	out.Hash = firstHash
	out.SampleDPText = sampleDPText
	return out, nil
}

func tagProjection(tags ...string) *modelv1.TagProjection {
	return &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: benchTagFamily, Tags: tags}}}
}

func strTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

func intFieldValue(value int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: value}}}
}

func hashResponse(resp *measurev1.QueryResponse) uint64 {
	h := fnv.New64a()
	points := append([]*measurev1.DataPoint(nil), resp.GetDataPoints()...)
	sort.Slice(points, func(left, right int) bool {
		leftBytes, leftErr := proto.MarshalOptions{Deterministic: true}.Marshal(points[left])
		rightBytes, rightErr := proto.MarshalOptions{Deterministic: true}.Marshal(points[right])
		if leftErr != nil || rightErr != nil {
			return left < right
		}
		return string(leftBytes) < string(rightBytes)
	})
	for _, point := range points {
		body, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(point)
		if marshalErr != nil {
			continue
		}
		_, _ = h.Write(body)
	}
	return h.Sum64()
}
