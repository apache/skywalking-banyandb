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

package dpo85bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func cpuTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second + time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

func stringValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

func listValue(values []string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
}

func BenchmarkPatternsRPC(b *testing.B) {
	if !vectorized.QueryMetricsEnabled {
		b.Fatal("querymetrics tag required")
	}
	gomega.RegisterTestingT(b)
	testflags.LogLevel = "error"
	if err := logger.Init(logger.Logging{Env: "dev", Level: "error"}); err != nil {
		b.Fatal(err)
	}
	size := 1000
	if text := os.Getenv("PATTERN_BENCH_NAMES"); text != "" {
		value, err := strconv.Atoi(text)
		if err != nil || (value != 1000 && value != 10000) {
			b.Fatal("PATTERN_BENCH_NAMES must be 1000 or 10000")
		}
		size = value
	}
	dir := b.TempDir()
	ports, err := test.AllocateFreePorts(5)
	if err != nil {
		b.Fatal(err)
	}
	addr, _, stop := setup.EmptyClosableStandalone(setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(dir)), dir, ports, "--allowed-bytes=3GiB")
	defer stop()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			b.Error(closeErr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	group := &commonv1.Group{}
	groupJSON := `{"metadata":{"name":"patternbench"},"catalog":"CATALOG_MEASURE",` +
		`"resource_opts":{"shard_num":1,"segment_interval":{"unit":"UNIT_DAY","num":1},` +
		`"ttl":{"unit":"UNIT_DAY","num":7},"replicas":1}}`
	if err = protojson.Unmarshal([]byte(groupJSON), group); err != nil {
		b.Fatal(err)
	}
	if _, err = databasev1.NewGroupRegistryServiceClient(conn).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: group}); err != nil {
		b.Fatal(err)
	}
	meta := &commonv1.Metadata{Name: "names", Group: "patternbench"}
	schema := &databasev1.Measure{
		Metadata: meta, Interval: "1s", Entity: &databasev1.Entity{TagNames: []string{"name"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
		Fields: []*databasev1.FieldSpec{{
			Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
	}
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "name_idx", Group: meta.Group},
		Tags:     []string{"name"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword", NoSort: true,
	}
	if _, err = databasev1.NewIndexRuleRegistryServiceClient(conn).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule}); err != nil {
		b.Fatal(err)
	}
	binding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "names_binding", Group: meta.Group},
		Rules:    []string{"name_idx"}, Subject: &databasev1.Subject{Name: meta.Name, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
	if _, err = bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: binding}); err != nil {
		b.Fatal(err)
	}
	if _, err = databasev1.NewMeasureRegistryServiceClient(conn).Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema}); err != nil {
		b.Fatal(err)
	}
	names := make([]string, size)
	var selected []string
	for i := range names {
		names[i] = fmt.Sprintf("ordinary-%05d", i)
		if i < 8 {
			names[i] = fmt.Sprintf("item-MaTcH-%05d", i)
		}
		// Independent fixture-level selection; discovery is deliberately outside timing.
		if strings.Contains(strings.ToLower(names[i]), "match") {
			selected = append(selected, names[i])
		}
	}
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour)
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	request := func(predicate string, param *modelv1.TagValue) *bydbqlv1.QueryRequest {
		return &bydbqlv1.QueryRequest{
			Query:  "SELECT COUNT(DISTINCT name) AS total FROM MEASURE names IN patternbench WHERE time >= ? AND time < ? AND " + predicate,
			Params: []*modelv1.TagValue{stringValue(base.Format(time.RFC3339)), stringValue(base.Add(2 * time.Second).Format(time.RFC3339)), param},
		}
	}
	var readyErr error
	for deadline := time.Now().Add(time.Minute); ; {
		readyErr = writeNames(ctx, measurev1.NewMeasureServiceClient(conn), meta, base, names)
		if readyErr == nil {
			readyErr = verifyNamePages(ctx, client, names, request)
		}
		if readyErr == nil {
			break
		}
		if time.Now().After(deadline) {
			b.Fatal(readyErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	type arm struct {
		param           *modelv1.TagValue
		name, predicate string
		want            int64
	}
	arms := []arm{
		{stringValue("%MaTcH%"), "LIKE", "name LIKE ?", 8},
		{stringValue("%match%"), "ILIKE", "name ILIKE ?", 8},
		{listValue(selected), "IN", "name IN (?)", 8},
		{stringValue("%absent%"), "LIKE_no_match", "name LIKE ?", 0},
		{stringValue("%absent%"), "ILIKE_no_match", "name ILIKE ?", 0},
		{listValue([]string{"absent-name"}), "IN_no_match", "name IN (?)", 0},
		{stringValue(names[0]), "exact_control", "name = ?", 1},
	}
	if os.Getenv("PATTERN_BENCH_ONLY_EXACT") == "1" {
		arms = arms[len(arms)-1:]
	}
	round, _ := strconv.Atoi(os.Getenv("PATTERN_BENCH_ROUND"))
	rotation := round % len(arms)
	arms = append(arms[rotation:], arms[:rotation]...)
	for _, a := range arms {
		req := request(a.predicate, a.param)
		if err = verifyCount(ctx, client, req, a.want); err != nil {
			b.Fatalf("warm %s: %v", a.name, err)
		}
		b.Run(fmt.Sprintf("names_%d/%s", size, a.name), func(b *testing.B) {
			if !vectorized.ResetQueryMemoryPeak() {
				b.Fatal("query memory still live before timing")
			}
			before := vmeasure.BatchExecutionStats()
			b.ReportAllocs()
			b.ResetTimer()
			startedCPU := cpuTime()
			for i := 0; i < b.N; i++ {
				if queryErr := verifyCount(ctx, client, req, a.want); queryErr != nil {
					b.Fatal(queryErr)
				}
			}
			b.StopTimer()
			elapsedCPU := cpuTime() - startedCPU
			after := vmeasure.BatchExecutionStats()
			memory := vectorized.QueryMemoryStats()
			if memory.LiveReservedBytes != 0 || after.LiveResults != before.LiveResults {
				b.Fatal("query memory/result leak")
			}
			b.ReportMetric(float64(elapsedCPU.Nanoseconds())/float64(b.N), "cpu-ns/op")
			b.ReportMetric(float64(memory.PeakReservedBytes), "peak-tracked-B")
			b.ReportMetric(float64(after.NativeInputRows-before.NativeInputRows)/float64(b.N), "native-rows/op")
		})
	}
}

func verifyNamePages(ctx context.Context, client bydbqlv1.BydbQLServiceClient, names []string,
	request func(string, *modelv1.TagValue) *bydbqlv1.QueryRequest,
) error {
	for start := 0; start < len(names); start += 128 {
		end := min(start+128, len(names))
		readiness := request("name IN (?)", listValue(names[start:end]))
		readiness.Query = strings.Replace(readiness.Query, "COUNT(DISTINCT name)", "COUNT(name)", 1)
		if err := verifyCount(ctx, client, readiness, int64((end-start)*2)); err != nil {
			return err
		}
	}
	return nil
}

func verifyCount(ctx context.Context, client bydbqlv1.BydbQLServiceClient, request *bydbqlv1.QueryRequest, want int64) error {
	response, err := client.Query(ctx, request)
	if err != nil {
		return err
	}
	points := response.GetMeasureResult().GetDataPoints()
	if len(points) != 1 || len(points[0].Fields) != 1 || points[0].Fields[0].Value.GetInt() == nil {
		return fmt.Errorf("expected one integer count, got %v", response)
	}
	if got := points[0].Fields[0].Value.GetInt().Value; got != want {
		return fmt.Errorf("count got %d, want %d", got, want)
	}
	return nil
}

func writeNames(ctx context.Context, client measurev1.MeasureServiceClient, meta *commonv1.Metadata, base time.Time, names []string) error {
	for start := 0; start < len(names); start += 128 {
		if err := writeNameBatch(ctx, client, meta, base, names[start:min(start+128, len(names))]); err != nil {
			return err
		}
	}
	return nil
}

func writeNameBatch(ctx context.Context, client measurev1.MeasureServiceClient, meta *commonv1.Metadata, base time.Time, names []string) error {
	stream, err := client.Write(ctx)
	if err != nil {
		return err
	}
	for i, name := range names {
		for second := 0; second < 2; second++ {
			request := &measurev1.WriteRequest{
				MessageId:     uint64(i*2 + second + 1),
				DataPointSpec: &measurev1.DataPointSpec{TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"name"}}}, FieldNames: []string{"value"}},
				DataPoint: &measurev1.DataPointValue{
					Timestamp:   timestamppb.New(base.Add(time.Duration(second) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{stringValue(name)}}},
					Fields:      []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}}},
				},
			}
			if i == 0 && second == 0 {
				request.Metadata = meta
			}
			if err = stream.Send(request); err != nil {
				return err
			}
		}
	}
	if err = stream.CloseSend(); err != nil {
		return err
	}
	count := 0
	for {
		ack, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return recvErr
		}
		if ack.Status != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("write status %s", ack.Status)
		}
		count++
	}
	if count != len(names)*2 {
		return fmt.Errorf("write acknowledgements: %d", count)
	}
	return nil
}
