package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
)

// runCapture connects to a live BanyanDB server, executes all generated queries,
// and writes the responses as want/*.yaml files.
func runCapture(outputDir, serverAddr string) {
	inputDirPath := filepath.Join(outputDir, "input")
	wantDirPath := filepath.Join(outputDir, "want")

	// Ensure want directory exists
	if mkdirErr := os.MkdirAll(wantDirPath, 0o755); mkdirErr != nil {
		fmt.Fprintf(os.Stderr, "Error creating want directory: %v\n", mkdirErr)
		os.Exit(1)
	}

	// Connect to server
	fmt.Printf("Connecting to %s ...\n", serverAddr)
	conn, connErr := grpchelper.Conn(serverAddr, 30*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if connErr != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to server: %v\n", connErr)
		os.Exit(1)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: error closing connection: %v\n", closeErr)
		}
	}()

	// Discover all generated input YAML files
	entries, readErr := os.ReadDir(inputDirPath)
	if readErr != nil {
		fmt.Fprintf(os.Stderr, "Error reading input directory: %v\n", readErr)
		os.Exit(1)
	}

	// Default time range parameters matching test setup
	baseTime := time.Now()
	duration := 25 * time.Minute
	offset := -20 * time.Minute

	capturedCount := 0
	skippedCount := 0
	errorCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".yaml" {
			continue
		}

		testName := name[:len(name)-len(".yaml")]

		// Only process generated test cases (prefixed with gen_)
		if !isGeneratedTest(testName) {
			continue
		}

		// Read input YAML
		inputPath := filepath.Join(inputDirPath, name)
		inputData, readInputErr := os.ReadFile(inputPath)
		if readInputErr != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", inputPath, readInputErr)
			errorCount++
			continue
		}

		// Parse QueryRequest from YAML
		jsonBytes, yamlErr := yaml.YAMLToJSON(inputData)
		if yamlErr != nil {
			fmt.Fprintf(os.Stderr, "Error converting YAML to JSON for %s: %v\n", testName, yamlErr)
			errorCount++
			continue
		}

		queryReq := &measurev1.QueryRequest{}
		if unmarshalErr := protojson.Unmarshal(jsonBytes, queryReq); unmarshalErr != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshaling request for %s: %v\n", testName, unmarshalErr)
			errorCount++
			continue
		}

		// Set time range
		begin := baseTime.Add(offset)
		queryReq.TimeRange = &modelv1.TimeRange{
			Begin: timestamppb.New(begin),
			End:   timestamppb.New(begin.Add(duration)),
		}

		// Determine if this is an error case
		isErrorCase := isErrorTest(testName)

		// Execute query
		client := measurev1.NewMeasureServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		resp, queryErr := client.Query(ctx, queryReq)
		cancel()

		if isErrorCase {
			if queryErr != nil {
				fmt.Printf("  [ERR-OK] %s: got expected error\n", testName)
				skippedCount++
			} else {
				fmt.Printf("  [ERR-UNEXPECTED] %s: expected error but got response\n", testName)
			}
			continue
		}

		if queryErr != nil {
			fmt.Fprintf(os.Stderr, "  [ERROR] %s: query failed: %v\n", testName, queryErr)
			errorCount++
			continue
		}

		// Write response as want YAML
		marshaler := protojson.MarshalOptions{Multiline: true}
		respJSON, marshalErr := marshaler.Marshal(resp)
		if marshalErr != nil {
			fmt.Fprintf(os.Stderr, "  [ERROR] %s: marshal failed: %v\n", testName, marshalErr)
			errorCount++
			continue
		}

		respYAML, yamlConvertErr := yaml.JSONToYAML(respJSON)
		if yamlConvertErr != nil {
			fmt.Fprintf(os.Stderr, "  [ERROR] %s: YAML conversion failed: %v\n", testName, yamlConvertErr)
			errorCount++
			continue
		}

		wantPath := filepath.Join(wantDirPath, testName+".yaml")
		if writeErr := os.WriteFile(wantPath, respYAML, 0o644); writeErr != nil {
			fmt.Fprintf(os.Stderr, "  [ERROR] %s: write failed: %v\n", testName, writeErr)
			errorCount++
			continue
		}

		pointCount := len(resp.GetDataPoints())
		fmt.Printf("  [OK] %s: %d data points\n", testName, pointCount)
		capturedCount++
	}

	fmt.Printf("\nCapture complete: %d captured, %d skipped (error cases), %d errors\n",
		capturedCount, skippedCount, errorCount)
}

// isGeneratedTest checks if a test name was produced by the generator.
func isGeneratedTest(name string) bool {
	return len(name) > 4 && name[:4] == "gen_"
}

// isErrorTest checks if a generated test is an error case.
func isErrorTest(name string) bool {
	return len(name) > 8 && name[:8] == "gen_err_"
}
