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

package capture_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	metadata "github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

func TestCaptureTrace(t *testing.T) {
	if os.Getenv("CAPTURE_TRACE_WANT_FIXTURES") != "1" {
		t.Skip("skipped: set CAPTURE_TRACE_WANT_FIXTURES=1 (or run `make capture-trace-test-cases`) to re-baseline trace data/want/*.yml")
	}
	gomega.RegisterTestingT(t)
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: "warn"})).To(gomega.Succeed())

	outputDir := filepath.Join("..", "..", "data")
	for argIdx, arg := range os.Args {
		if arg == "-args" && argIdx+1 < len(os.Args) {
			outputDir = os.Args[argIdx+1]
			break
		}
	}
	inputDirPath := filepath.Join(outputDir, "input")
	wantDirPath := filepath.Join(outputDir, "want")
	gomega.Expect(os.MkdirAll(wantDirPath, 0o755)).To(gomega.Succeed())

	addr, _, deferFn := setup.Standalone(nil)
	defer deferFn()
	conn, connErr := grpchelper.Conn(addr, 30*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()

	ns := timestamp.NowMilli().UnixNano()
	baseTime := time.Unix(0, ns-ns%int64(time.Minute))
	interval := 500 * time.Millisecond
	casestrace.SeedAll(conn, baseTime, interval)
	assertTraceSmoke(t, conn, baseTime)

	entries, readErr := os.ReadDir(inputDirPath)
	gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
	headerBytes := []byte(casestrace.LicenseHeader)
	capturedCount := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" {
			continue
		}
		testName := strings.TrimSuffix(entry.Name(), ".yml")
		if !isGeneratedTest(testName) {
			continue
		}
		if isErrorTest(testName) {
			t.Logf("  [SKIP] %s: error case, no want file needed", testName)
			continue
		}
		inputPath := filepath.Join(inputDirPath, entry.Name())
		inputData, readInputErr := os.ReadFile(inputPath)
		if readInputErr != nil {
			t.Errorf("Error reading %s: %v", inputPath, readInputErr)
			continue
		}
		queryReq := &tracev1.QueryRequest{}
		jsonBytes, yamlErr := yaml.YAMLToJSON(inputData)
		if yamlErr != nil {
			t.Errorf("Error converting YAML to JSON for %s: %v", testName, yamlErr)
			continue
		}
		if unmarshalErr := protojson.Unmarshal(jsonBytes, queryReq); unmarshalErr != nil {
			t.Errorf("Error unmarshaling request for %s: %v", testName, unmarshalErr)
			continue
		}
		qlPath := filepath.Join(inputDirPath, testName+".ql")
		if verifyErr := verifyQLWithRequest(t, qlPath, queryReq, conn); verifyErr != nil {
			t.Errorf("  [ERROR] %s: QL/proto mismatch: %v", testName, verifyErr)
			continue
		}
		queryReq.TimeRange = captureTimeRange(baseTime)
		client := tracev1.NewTraceServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		resp, queryErr := client.Query(ctx, queryReq)
		cancel()
		if queryErr != nil {
			t.Logf("  [SKIP] %s: query returned error: %v", testName, queryErr)
			continue
		}
		if len(resp.GetTraces()) == 0 {
			t.Logf("  [SKIP] %s: empty response, no want file needed", testName)
			continue
		}
		respJSON, marshalErr := casestrace.MarshalToJSONWithStringBytes(resp)
		if marshalErr != nil {
			t.Errorf("  [ERROR] %s: marshal failed: %v", testName, marshalErr)
			continue
		}
		respYAML, yamlConvertErr := yaml.JSONToYAML(respJSON)
		if yamlConvertErr != nil {
			t.Errorf("  [ERROR] %s: YAML conversion failed: %v", testName, yamlConvertErr)
			continue
		}
		wantPath := filepath.Join(wantDirPath, testName+".yml")
		wantContent := make([]byte, len(headerBytes)+len(respYAML))
		copy(wantContent, headerBytes)
		copy(wantContent[len(headerBytes):], respYAML)
		if writeErr := os.WriteFile(wantPath, wantContent, 0o600); writeErr != nil {
			t.Errorf("  [ERROR] %s: write failed: %v", testName, writeErr)
			continue
		}
		t.Logf("  [OK] %s: %d traces", testName, len(resp.GetTraces()))
		capturedCount++
	}
	t.Logf("Capture complete: %d want files written to %s", capturedCount, wantDirPath)
}

func assertTraceSmoke(t *testing.T, conn *grpc.ClientConn, baseTime time.Time) {
	t.Helper()
	client := tracev1.NewTraceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, queryErr := client.Query(ctx, &tracev1.QueryRequest{
		Name: "sw", Groups: []string{"test-trace-group"}, TimeRange: captureTimeRange(baseTime), TagProjection: []string{"trace_id"},
		Criteria: buildCriteriaFromCondition(buildCondition("trace_id", modelv1.Condition_BINARY_OP_EQ, tagValueStr("trace_001"))),
	})
	gomega.Expect(queryErr).NotTo(gomega.HaveOccurred())
	spanCount := 0
	for _, trace := range resp.GetTraces() {
		spanCount += len(trace.GetSpans())
	}
	gomega.Expect(spanCount).To(gomega.BeNumerically(">=", 5), "trace_001 smoke should include sw + sw_mixed_traces spans")
}

func captureTimeRange(baseTime time.Time) *modelv1.TimeRange {
	return &modelv1.TimeRange{Begin: timestamppb.New(baseTime.Add(-5 * time.Minute)), End: timestamppb.New(baseTime.Add(time.Hour))}
}

func verifyQLWithRequest(t *testing.T, qlPath string, yamlQuery *tracev1.QueryRequest, conn *grpc.ClientConn) error {
	t.Helper()
	qlContent, readErr := os.ReadFile(qlPath)
	if readErr != nil {
		return readErr
	}
	var qlQueryStr string
	for _, line := range strings.Split(string(qlContent), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if qlQueryStr != "" {
			qlQueryStr += " "
		}
		qlQueryStr += trimmed
	}
	re := regexp.MustCompile(`(?i)\s+IN\s+(\([^)]+\)|[a-zA-Z0-9_-]+(?:\s*,\s*[a-zA-Z0-9_-]+)*)`)
	qlQueryStr = re.ReplaceAllString(qlQueryStr, " IN $1")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRepo := metadata.NewMockRepo(ctrl)
	traceRegistry := schema.NewMockTrace(ctrl)
	traceRegistry.EXPECT().GetTrace(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Trace, error) {
		client := databasev1.NewTraceRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{Metadata: metadata})
		if getErr != nil {
			return nil, getErr
		}
		return resp.GetTrace(), nil
	}).AnyTimes()
	mockRepo.EXPECT().TraceRegistry().AnyTimes().Return(traceRegistry)

	parsed, errStrs := bydbql.ParseQuery(qlQueryStr)
	if errStrs != nil {
		return fmt.Errorf("failed to parse QL: %w", errStrs)
	}
	transformer := bydbql.NewTransformer(mockRepo)
	transformCtx, transformCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer transformCancel()
	result, transformErr := transformer.Transform(transformCtx, parsed)
	if transformErr != nil {
		return transformErr
	}
	qlQuery, ok := result.QueryRequest.(*tracev1.QueryRequest)
	if !ok {
		return fmt.Errorf("transformed QL is not a trace query request")
	}
	if !cmp.Equal(qlQuery, yamlQuery, protocmp.IgnoreUnknown(), protocmp.IgnoreFields(&tracev1.QueryRequest{}, "time_range", "stages"), protocmp.Transform()) {
		return fmt.Errorf("QL/proto requests differ after transform")
	}
	bydbqlClient := bydbqlv1.NewBydbQLServiceClient(conn)
	qlCtx, qlCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer qlCancel()
	bydbqlResp, queryErr := bydbqlClient.Query(qlCtx, &bydbqlv1.QueryRequest{Query: qlQueryStr})
	if queryErr != nil {
		return queryErr
	}
	if _, ok := bydbqlResp.GetResult().(*bydbqlv1.QueryResponse_TraceResult); !ok {
		return fmt.Errorf("BydbQL response is not a trace result")
	}
	return nil
}

func isGeneratedTest(name string) bool {
	return strings.HasPrefix(name, "gen_")
}

func isErrorTest(name string) bool {
	return strings.HasPrefix(name, "gen_err_")
}

func buildCondition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Condition {
	return &modelv1.Condition{Name: name, Op: op, Value: value}
}

func buildCriteriaFromCondition(cond *modelv1.Condition) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: cond}}
}

func tagValueStr(val string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: val}}}
}
