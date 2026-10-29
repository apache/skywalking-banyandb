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

package qg3bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
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
	"github.com/apache/skywalking-banyandb/pkg/test"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func cpuTime() time.Duration {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
}

func BenchmarkQG3RPC(b *testing.B) {
	gomega.RegisterTestingT(b)
	testflags.LogLevel = "error"
	if err := logger.Init(logger.Logging{Env: "dev", Level: "error"}); err != nil {
		b.Fatal(err)
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
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	group := &commonv1.Group{}
	groupJSON := `{"metadata":{"name":"qg3bench"},"catalog":"CATALOG_MEASURE",` +
		`"resource_opts":{"shard_num":1,"segment_interval":{"unit":"UNIT_DAY","num":1},` +
		`"ttl":{"unit":"UNIT_DAY","num":7},"replicas":1}}`
	if err = protojson.Unmarshal([]byte(groupJSON), group); err != nil {
		b.Fatal(err)
	}
	if _, err = databasev1.NewGroupRegistryServiceClient(conn).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: group}); err != nil {
		b.Fatal(err)
	}
	meta := &commonv1.Metadata{Name: "m", Group: "qg3bench"}
	schema := &databasev1.Measure{
		Metadata: meta, Interval: "1s", Entity: &databasev1.Entity{TagNames: []string{"rid"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "rid", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{
			{
				Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
	}
	if _, err = databasev1.NewMeasureRegistryServiceClient(conn).Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema}); err != nil {
		b.Fatal(err)
	}
	rows := benchmarkRows(b)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour)
	writer := measurev1.NewMeasureServiceClient(conn)

	client := bydbqlv1.NewBydbQLServiceClient(conn)
	var writeErr error
	for deadline := time.Now().Add(40 * time.Second); ; {
		writeErr = writeFixture(ctx, writer, meta, base, rows)
		if writeErr == nil {
			break
		}
		if time.Now().After(deadline) {
			b.Fatal(writeErr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	expected := waitForFixture(b, ctx, client, base, rows)
	runPaired(b, ctx, client, base, rows, expected, expectedGroups(rows))
}

func writeFixture(ctx context.Context, writer measurev1.MeasureServiceClient, meta *commonv1.Metadata, base time.Time, rows int) error {
	stream, e := writer.Write(ctx)
	if e != nil {
		return e
	}
	for i := 0; i < rows; i++ {
		req := &measurev1.WriteRequest{
			MessageId: uint64(i + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"rid"}}},
				FieldNames:    []string{"value"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(i%(rows/8)) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("r%d", i/(rows/8))}}},
				}}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i + 1)}}},
				},
			},
		}
		if i == 0 {
			req.Metadata = meta
		}
		if e = stream.Send(req); e != nil {
			return e
		}
	}
	if e = stream.CloseSend(); e != nil {
		return e
	}
	count := 0
	for {
		ack, e := stream.Recv()
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return e
		}
		if ack.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("write status %s", ack.GetStatus())
		}
		count++
	}
	if count != rows {
		return fmt.Errorf("write acknowledgements: %d", count)
	}
	return nil
}

func runQuery(ctx context.Context, client bydbqlv1.BydbQLServiceClient, base time.Time, selection, grouping string, cached bool) (string, error) {
	request := &bydbqlv1.QueryRequest{}
	if cached {
		request.Query = fmt.Sprintf("SELECT %s FROM qg3bench.m WHERE time >= ? AND time < ?%s", selection, grouping)
		request.Params = []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Hour))}},
		}
	} else {
		request.Query = fmt.Sprintf("SELECT %s FROM qg3bench.m WHERE time >= '%s' AND time < '%s'%s",
			selection, base.Format(time.RFC3339), base.Add(time.Hour).Format(time.RFC3339), grouping)
	}
	result, e := client.Query(ctx, request)
	if e != nil {
		return "", e
	}
	if result.GetMeasureResult() == nil {
		return "", fmt.Errorf("missing measure result")
	}
	values := make([]string, 0, len(result.GetMeasureResult().DataPoints))
	for _, point := range result.GetMeasureResult().DataPoints {
		if len(point.Fields) != 1 {
			return "", fmt.Errorf("invalid fields: %v", point)
		}
		key := ""
		for _, family := range point.TagFamilies {
			for _, tag := range family.Tags {
				if tag.Key == "rid" {
					key = tag.Value.GetStr().GetValue()
				}
			}
		}
		if value := point.Fields[0].Value.GetInt(); value != nil {
			values = append(values, fmt.Sprintf("%s:%d", key, value.Value))
		} else if value := point.Fields[0].Value.GetStr(); value != nil {
			values = append(values, fmt.Sprintf("%s:%s", key, value.Value))
		} else {
			return "", fmt.Errorf("invalid field: %v", point.Fields[0])
		}
	}
	sort.Strings(values)
	return strings.Join(values, ";"), nil
}

func benchmarkRows(b *testing.B) int {
	b.Helper()
	rows := 64
	if value := os.Getenv("QG3_ROWS"); value != "" {
		var err error
		rows, err = strconv.Atoi(value)
		if err != nil || rows < 8 || rows%8 != 0 || rows > 8192 {
			b.Fatal("invalid QG3_ROWS")
		}
	}
	return rows
}

func waitForFixture(b *testing.B, ctx context.Context, client bydbqlv1.BydbQLServiceClient, base time.Time, rows int) string {
	b.Helper()
	for deadline := time.Now().Add(60 * time.Second); ; {
		expected, err := runQuery(ctx, client, base, "SUM(value)", "", false)
		if err == nil && expected == fmt.Sprintf(":%d", rows*(rows+1)/2) {
			return expected
		}
		if time.Now().After(deadline) {
			b.Fatalf("fixture incomplete: %s %v", expected, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
