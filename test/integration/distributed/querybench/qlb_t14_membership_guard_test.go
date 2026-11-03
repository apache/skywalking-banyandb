//go:build querymetrics

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
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// TestQLBT14MembershipRejectsNewIndexOnCachedPlan is a correctness probe, not a timing matrix.
func TestQLBT14MembershipRejectsNewIndexOnCachedPlan(t *testing.T) {
	if os.Getenv("DQB_T14_MEMBERSHIP_GUARD_PROBE") != "1" {
		t.Skip("set DQB_T14_MEMBERSHIP_GUARD_PROBE=1 for the index-change RPC regression")
	}
	cfg := LoadConfig()
	if !cfg.RunBench || !cfg.InContainer || cfg.Engine != engineMeasure {
		t.Fatal("membership guard probe requires an opted-in measure container")
	}
	if err := logger.Init(logger.Logging{Env: "dev", Level: "info"}); err != nil {
		t.Fatal(err)
	}
	gomega.RegisterTestingT(t)
	cfg.Scenario, cfg.Mode, cfg.Cardinality, cfg.QueryMemoryMiB = ScenarioQLBT14, modeVec, 1, 256
	cluster, base, err := startBenchCluster(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	writeQLBT14MembershipGuardPoint(t, ctx, cluster, base)
	client := bydbqlv1.NewBydbQLServiceClient(cluster.conn)
	params := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Second))}},
	}
	prefix := "SELECT host, f::FIELD FROM " + benchGroupName + "." + qlbT14MembershipMeasure + " WHERE time >= ? AND time < ? AND host = 'probe'"
	for attempts := 0; ; attempts++ {
		response, queryErr := client.Query(ctx, &bydbqlv1.QueryRequest{Query: prefix, Params: params})
		if queryErr == nil && len(response.GetMeasureResult().GetDataPoints()) == 1 {
			break
		}
		if attempts == 99 {
			t.Fatalf("one-row visibility failed: %v", queryErr)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	params = append(params, strTagValue("needle"))
	queries := []string{prefix + " AND labels::TAG CONTAINS ? AND f::FIELD > 0", prefix + " AND labels HAVING ? AND f::FIELD > 0"}
	for _, query := range queries {
		for attempt := 0; attempt < 2; attempt++ {
			response, queryErr := client.Query(ctx, &bydbqlv1.QueryRequest{Query: query, Params: params})
			if queryErr != nil || len(response.GetMeasureResult().GetDataPoints()) != 1 {
				t.Fatalf("unindexed membership must retain written row: %v", queryErr)
			}
		}
	}
	createQLBT14MembershipGuardIndex(t, ctx, cluster)
	beforeCache, beforeNative := liaisongrpc.BydbQLPlanCacheStats(), vmeasure.BatchExecutionStats()
	for _, query := range queries {
		_, queryErr := client.Query(ctx, &bydbqlv1.QueryRequest{Query: query, Params: params})
		if status.Code(queryErr) != codes.InvalidArgument || !strings.Contains(queryErr.Error(), "indexed array") {
			t.Fatalf("cached membership must reject the newly indexed array: %v", queryErr)
		}
	}
	afterCache, afterNative := liaisongrpc.BydbQLPlanCacheStats(), vmeasure.BatchExecutionStats()
	if afterCache.Hits-beforeCache.Hits != 2 || afterCache.Compiles != beforeCache.Compiles {
		t.Fatal("late-index checks must cover reused compiled plans")
	}
	if afterNative.NativeLeafCalls != beforeNative.NativeLeafCalls || afterNative.QLBatchExecutions != beforeNative.QLBatchExecutions {
		t.Fatal("indexed membership reached data execution")
	}
	if err = checkQLBT14ResourceCleanup(); err != nil {
		t.Fatal(err)
	}
}

func writeQLBT14MembershipGuardPoint(t *testing.T, ctx context.Context, cluster benchCluster, base time.Time) {
	t.Helper()
	writer, err := measurev1.NewMeasureServiceClient(cluster.conn).Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: qlbT14MembershipMeasure, Group: benchGroupName}, MessageId: 1,
		DataPointSpec: &measurev1.DataPointSpec{
			TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"host", "width", "labels"}}},
			FieldNames:    []string{"f"},
		},
		DataPoint: &measurev1.DataPointValue{Timestamp: timestamppb.New(base), TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
			strTagValue("probe"),
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
			{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"needle", "other"}}}},
		}}}, Fields: []*modelv1.FieldValue{qlbT07Float(1)}},
	}
	if err = writer.Send(request); err != nil {
		t.Fatal(err)
	}
	if err = writer.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for {
		response, receiveErr := writer.Recv()
		if errors.Is(receiveErr, io.EOF) {
			return
		}
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		if response.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
			t.Fatalf("membership guard fixture rejected: %s", response.GetStatus())
		}
	}
}

func createQLBT14MembershipGuardIndex(t *testing.T, ctx context.Context, cluster benchCluster) {
	t.Helper()
	_, err := databasev1.NewIndexRuleRegistryServiceClient(cluster.conn).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "t14_guard_labels", Group: benchGroupName}, Tags: []string{"labels"}, Type: databasev1.IndexRule_TYPE_INVERTED,
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Native caches ignore binding validity windows, so an expired binding must also reject.
	_, err = databasev1.NewIndexRuleBindingRegistryServiceClient(cluster.conn).Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Name: "t14_guard_binding", Group: benchGroupName},
			Rules:    []string{"t14_guard_labels"}, Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: qlbT14MembershipMeasure},
			BeginAt: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), ExpireAt: timestamppb.New(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}
