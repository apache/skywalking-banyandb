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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

const licenseHeader = "# Licensed to Apache Software Foundation (ASF) under one or more contributor\n" +
	"# license agreements. See the NOTICE file distributed with\n" +
	"# this work for additional information regarding copyright\n" +
	"# ownership. Apache Software Foundation (ASF) licenses this file to you under\n" +
	"# the Apache License, Version 2.0 (the \"License\"); you may\n" +
	"# not use this file except in compliance with the License.\n" +
	"# You may obtain a copy of the License at\n" +
	"#\n" +
	"#     http://www.apache.org/licenses/LICENSE-2.0\n" +
	"#\n" +
	"# Unless required by applicable law or agreed to in writing,\n" +
	"# software distributed under the License is distributed on an\n" +
	"# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n" +
	"# KIND, either express or implied.  See the License for the\n" +
	"# specific language governing permissions and limitations\n" +
	"# under the License.\n\n"

// TestCaptureStream boots a standalone server, seeds the stream fixtures, and
// re-baselines the generated want/*.yaml golden files from live query output.
func TestCaptureStream(t *testing.T) {
	if os.Getenv("CAPTURE_STREAM_WANT_FIXTURES") != "1" {
		t.Skip("skipped: set CAPTURE_STREAM_WANT_FIXTURES=1 (or run `make capture-stream-test-cases`) to re-baseline stream data/want/*.yaml")
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
	casesstreamdata.SeedAll(conn, baseTime, interval)
	assertStreamSmoke(t, conn, baseTime)

	entries, readErr := os.ReadDir(inputDirPath)
	gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
	headerBytes := []byte(licenseHeader)
	var mismatches []predictionMismatch
	emptyVerified := 0
	goldensWritten := 0
	errorsVerified := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		testName := strings.TrimSuffix(entry.Name(), ".yaml")
		if !isGeneratedTest(testName) {
			continue
		}
		inputPath := filepath.Join(inputDirPath, entry.Name())
		queryReq, parseErr := loadQueryRequest(inputPath)
		if parseErr != nil {
			t.Errorf("Error loading request for %s: %v", testName, parseErr)
			continue
		}
		wantPath := filepath.Join(wantDirPath, testName+".yaml")

		// Error-predicted: gen_err_ prefix. Run the query and expect a gRPC error
		// whose message contains the ErrUnsupportedConditionOp sentinel substring.
		if isErrorTest(testName) {
			queryReq.TimeRange = captureTimeRange(baseTime)
			client := streamv1.NewStreamServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			resp, queryErr := client.Query(ctx, queryReq)
			cancel()
			if queryErr == nil {
				mismatches = append(mismatches, predictionMismatch{
					name:   testName,
					kind:   "error",
					detail: fmt.Sprintf("predicted error, got success (%d elements)", len(resp.GetElements())),
				})
				continue
			}
			if !strings.Contains(queryErr.Error(), "unsupported condition operation") {
				mismatches = append(mismatches, predictionMismatch{
					name:   testName,
					kind:   "error-msg",
					detail: fmt.Sprintf("predicted ErrUnsupportedConditionOp, got different error: %v", queryErr),
				})
				continue
			}
			t.Logf("  [OK] %s: predicted error, got error: %v", testName, queryErr)
			errorsVerified++
			continue
		}

		// QL/proto equivalence check applies to every non-error case.
		qlPath := filepath.Join(inputDirPath, testName+".ql")
		if verifyErr := verifyQLWithRequest(t, qlPath, queryReq, conn); verifyErr != nil {
			mismatches = append(mismatches, predictionMismatch{
				name:   testName,
				kind:   "ql",
				detail: fmt.Sprintf("QL/proto mismatch: %v", verifyErr),
			})
			continue
		}

		queryReq.TimeRange = captureTimeRange(baseTime)
		client := streamv1.NewStreamServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		resp, queryErr := client.Query(ctx, queryReq)
		cancel()
		if queryErr != nil {
			mismatches = append(mismatches, predictionMismatch{
				name:   testName,
				kind:   "query",
				detail: fmt.Sprintf("query returned error: %v", queryErr),
			})
			continue
		}
		elementCount := len(resp.GetElements())
		predictedNonEmpty := wantFileExists(wantPath)

		if predictedNonEmpty {
			// Non-empty-predicted: a want file (placeholder or golden) exists.
			if elementCount == 0 {
				mismatches = append(mismatches, predictionMismatch{
					name:   testName,
					kind:   "non-empty",
					detail: "predicted non-empty, got empty (0 elements)",
				})
				continue
			}
			if writeErr := writeGolden(wantPath, headerBytes, resp); writeErr != nil {
				t.Errorf("  [ERROR] %s: %v", testName, writeErr)
				continue
			}
			t.Logf("  [OK] %s: predicted non-empty, wrote golden (%d elements)", testName, elementCount)
			goldensWritten++
			continue
		}

		// Empty-predicted: no want file, not gen_err_.
		if elementCount > 0 {
			mismatches = append(mismatches, predictionMismatch{
				name:   testName,
				kind:   "empty",
				detail: fmt.Sprintf("predicted empty, got %d elements", elementCount),
			})
			continue
		}
		t.Logf("  [OK] %s: predicted empty, got empty", testName)
		emptyVerified++
	}

	if len(mismatches) > 0 {
		var report strings.Builder
		report.WriteString(fmt.Sprintf("prediction verification failed: %d mismatch(es) between seed-prediction and engine reality:\n", len(mismatches)))
		for _, mismatch := range mismatches {
			report.WriteString(fmt.Sprintf("  [%s] %s: %s\n", mismatch.kind, mismatch.name, mismatch.detail))
		}
		t.Errorf("%s", report.String())
		return
	}
	t.Logf("Prediction verification passed: %d empty verified, %d non-empty goldens written, %d error cases verified (want dir %s)",
		emptyVerified, goldensWritten, errorsVerified, wantDirPath)
}

// predictionMismatch records a single divergence between the generator's seed
// prediction and the live engine result.
type predictionMismatch struct {
	name   string
	kind   string
	detail string
}

// wantFileExists reports whether a want file is present, which encodes the
// generator's non-empty prediction. A placeholder file (carrying the
// "# Placeholder - run 'capture' subcommand to fill this file" marker) and a
// real golden both count as predicted non-empty; either way the case is
// re-run and the golden is rewritten, so their presence alone is sufficient.
func wantFileExists(wantPath string) bool {
	_, statErr := os.Stat(wantPath)
	return statErr == nil
}

// loadQueryRequest reads an input YAML fixture and decodes it into a stream
// query request.
func loadQueryRequest(inputPath string) (*streamv1.QueryRequest, error) {
	inputData, readInputErr := os.ReadFile(inputPath)
	if readInputErr != nil {
		return nil, fmt.Errorf("reading %s: %w", inputPath, readInputErr)
	}
	jsonBytes, yamlErr := yaml.YAMLToJSON(inputData)
	if yamlErr != nil {
		return nil, fmt.Errorf("converting YAML to JSON: %w", yamlErr)
	}
	queryReq := &streamv1.QueryRequest{}
	if unmarshalErr := protojson.Unmarshal(jsonBytes, queryReq); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling request: %w", unmarshalErr)
	}
	return queryReq, nil
}

// writeGolden marshals the response to YAML with the license header and writes
// it to the want path, overwriting any placeholder or stale golden.
func writeGolden(wantPath string, headerBytes []byte, resp *streamv1.QueryResponse) error {
	respJSON, marshalErr := protojson.Marshal(resp)
	if marshalErr != nil {
		return fmt.Errorf("marshal failed: %w", marshalErr)
	}
	respYAML, yamlConvertErr := yaml.JSONToYAML(respJSON)
	if yamlConvertErr != nil {
		return fmt.Errorf("YAML conversion failed: %w", yamlConvertErr)
	}
	wantContent := make([]byte, len(headerBytes)+len(respYAML))
	copy(wantContent, headerBytes)
	copy(wantContent[len(headerBytes):], respYAML)
	if writeErr := os.WriteFile(wantPath, wantContent, 0o600); writeErr != nil {
		return fmt.Errorf("write failed: %w", writeErr)
	}
	return nil
}

func assertStreamSmoke(t *testing.T, conn *grpc.ClientConn, baseTime time.Time) {
	t.Helper()
	client := streamv1.NewStreamServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, queryErr := client.Query(ctx, &streamv1.QueryRequest{
		Name:      "sw",
		Groups:    []string{"default"},
		TimeRange: captureTimeRange(baseTime),
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "searchable", Tags: []string{"trace_id"}},
			},
		},
	})
	gomega.Expect(queryErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.GetElements()).NotTo(gomega.BeEmpty(), "sw smoke query should return seeded elements")
}

func captureTimeRange(baseTime time.Time) *modelv1.TimeRange {
	return &modelv1.TimeRange{Begin: timestamppb.New(baseTime.Add(-5 * time.Minute)), End: timestamppb.New(baseTime.Add(time.Hour))}
}

func verifyQLWithRequest(t *testing.T, qlPath string, yamlQuery *streamv1.QueryRequest, conn *grpc.ClientConn) error {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRepo := metadata.NewMockRepo(ctrl)
	streamRegistry := schema.NewMockStream(ctrl)
	streamRegistry.EXPECT().GetStream(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, md *commonv1.Metadata) (*databasev1.Stream, error) {
		client := databasev1.NewStreamRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: md})
		if getErr != nil {
			return nil, getErr
		}
		return resp.GetStream(), nil
	}).AnyTimes()
	mockRepo.EXPECT().StreamRegistry().AnyTimes().Return(streamRegistry)

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
	qlQuery, ok := result.QueryRequest.(*streamv1.QueryRequest)
	if !ok {
		return fmt.Errorf("transformed QL is not a stream query request")
	}
	if !cmp.Equal(qlQuery, yamlQuery, protocmp.IgnoreUnknown(), protocmp.IgnoreFields(&streamv1.QueryRequest{}, "time_range", "stages"), protocmp.Transform()) {
		return fmt.Errorf("QL/proto requests differ after transform")
	}
	bydbqlClient := bydbqlv1.NewBydbQLServiceClient(conn)
	qlCtx, qlCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer qlCancel()
	bydbqlResp, queryErr := bydbqlClient.Query(qlCtx, &bydbqlv1.QueryRequest{Query: qlQueryStr})
	if queryErr != nil {
		return queryErr
	}
	if _, ok := bydbqlResp.GetResult().(*bydbqlv1.QueryResponse_StreamResult); !ok {
		return fmt.Errorf("BydbQL response is not a stream result")
	}
	return nil
}

func isGeneratedTest(name string) bool {
	return strings.HasPrefix(name, "gen_")
}

func isErrorTest(name string) bool {
	return strings.HasPrefix(name, "gen_err_")
}
