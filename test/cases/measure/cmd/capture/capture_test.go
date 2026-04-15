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

// Package capture_test runs a standalone server, loads seed data, executes
// generated queries, and writes responses as want/*.yaml files.
//
// Usage:
//
//	go test -run TestCapture ./test/cases/measure/cmd/capture/ -args [output-dir]
//
// output-dir defaults to ../../data (i.e. test/cases/measure/data).
package capture_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

func TestCapture(t *testing.T) {
	gomega.RegisterTestingT(t)

	// Initialize logger
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: "warn"})).To(gomega.Succeed())

	// Determine output directory
	outputDir := filepath.Join("..", "..", "data")
	if len(os.Args) > 1 {
		// The first extra arg after -args is the output dir
		for idx, arg := range os.Args {
			if arg == "-args" && idx+1 < len(os.Args) {
				outputDir = os.Args[idx+1]
				break
			}
		}
	}

	inputDirPath := filepath.Join(outputDir, "input")
	wantDirPath := filepath.Join(outputDir, "want")

	// Ensure want directory exists
	gomega.Expect(os.MkdirAll(wantDirPath, 0o755)).To(gomega.Succeed())

	// Start standalone server
	addr, _, deferFn := setup.Standalone(nil)
	defer deferFn()

	// Connect to server
	conn, connErr := grpchelper.Conn(addr, 30*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
	defer func() { _ = conn.Close() }()

	// Write seed data - matching the integration test pattern
	ns := timestamp.NowMilli().UnixNano()
	baseTime := time.Unix(0, ns-ns%int64(time.Minute))
	interval := 500 * time.Millisecond
	casesMeasureData.Write(conn, "service_traffic", "index_mode", "service_traffic_data.json", baseTime, interval)
	casesMeasureData.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", baseTime, interval)
	casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval)

	// Default time range matching test setup
	duration := 25 * time.Minute
	offset := -20 * time.Minute

	// Discover generated input YAML files
	entries, readErr := os.ReadDir(inputDirPath)
	gomega.Expect(readErr).NotTo(gomega.HaveOccurred())

	capturedCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".yaml" {
			continue
		}
		testName := name[:len(name)-len(".yaml")]
		if !isGeneratedTest(testName) {
			continue
		}
		if isErrorTest(testName) {
			t.Logf("  [SKIP] %s: error case, no want file needed", testName)
			continue
		}

		// Read input YAML
		inputPath := filepath.Join(inputDirPath, name)
		inputData, readInputErr := os.ReadFile(inputPath)
		if readInputErr != nil {
			t.Errorf("Error reading %s: %v", inputPath, readInputErr)
			continue
		}

		// Parse QueryRequest from YAML
		jsonBytes, yamlErr := yaml.YAMLToJSON(inputData)
		if yamlErr != nil {
			t.Errorf("Error converting YAML to JSON for %s: %v", testName, yamlErr)
			continue
		}

		queryReq := &measurev1.QueryRequest{}
		if unmarshalErr := protojson.Unmarshal(jsonBytes, queryReq); unmarshalErr != nil {
			t.Errorf("Error unmarshaling request for %s: %v", testName, unmarshalErr)
			continue
		}

		// Set time range
		begin := baseTime.Add(offset)
		queryReq.TimeRange = &modelv1.TimeRange{
			Begin: timestamppb.New(begin),
			End:   timestamppb.New(begin.Add(duration)),
		}

		// Execute query
		client := measurev1.NewMeasureServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		resp, queryErr := client.Query(ctx, queryReq)
		cancel()

		if queryErr != nil {
			t.Logf("  [SKIP] %s: query returned error (likely WantErr case): %v", testName, queryErr)
			continue
		}

		// Write response as want YAML
		marshaler := protojson.MarshalOptions{Multiline: true}
		respJSON, marshalErr := marshaler.Marshal(resp)
		if marshalErr != nil {
			t.Errorf("  [ERROR] %s: marshal failed: %v", testName, marshalErr)
			continue
		}

		respYAML, yamlConvertErr := yaml.JSONToYAML(respJSON)
		if yamlConvertErr != nil {
			t.Errorf("  [ERROR] %s: YAML conversion failed: %v", testName, yamlConvertErr)
			continue
		}

		wantPath := filepath.Join(wantDirPath, testName+".yaml")
		if writeErr := os.WriteFile(wantPath, respYAML, 0o600); writeErr != nil {
			t.Errorf("  [ERROR] %s: write failed: %v", testName, writeErr)
			continue
		}

		pointCount := len(resp.GetDataPoints())
		t.Logf("  [OK] %s: %d data points", testName, pointCount)
		capturedCount++
	}

	t.Logf("Capture complete: %d want files written to %s", capturedCount, wantDirPath)
}

func isGeneratedTest(name string) bool {
	return len(name) > 4 && name[:4] == "gen_"
}

func isErrorTest(name string) bool {
	return len(name) > 8 && name[:8] == "gen_err_"
}
