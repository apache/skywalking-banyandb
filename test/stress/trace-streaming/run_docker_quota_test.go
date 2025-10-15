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

//go:build manual
// +build manual

package main

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"time"

	tracestreaming "github.com/apache/skywalking-banyandb/test/stress/trace-streaming"
)

// This is a standalone test program that runs against a Docker container
// with cgroup memory limits to verify quota handling.
//
// Usage:
//   1. Start Docker: docker-compose -f docker-compose-quota-stress.yml up -d
//   2. Run this: go run -tags manual run_docker_quota_test.go

func main() {
	fmt.Println("=== Docker Quota Stress Test ===\n")

	// Check if Docker container is running
	if err := checkDocker(); err != nil {
		log.Fatalf("Docker check failed: %v\n\nPlease start container with:\n  docker-compose -f docker-compose-quota-stress.yml up -d", err)
	}

	banyandbAddr := "localhost:17912"
	pprofAddr := "localhost:6060"

	ctx := context.Background()

	// Setup schema via gRPC
	fmt.Println("Setting up schema...")
	if err := setupSchema(ctx, banyandbAddr); err != nil {
		log.Fatalf("Schema setup failed: %v", err)
	}

	// Generate aggressive test data (100K traces with large spans for quota testing)
	fmt.Println("\nGenerating 100K traces with 50-100 spans each...")
	generator := tracestreaming.NewTraceGenerator(tracestreaming.SmallScale)
	config := tracestreaming.GetScaleConfig(tracestreaming.SmallScale)

	// Use all traces from small scale config
	totalTraces := config.TotalTraces
	traces := make([]*tracestreaming.TraceData, totalTraces)

	for i := 0; i < totalTraces; i++ {
		// Force larger traces to create bigger blocks
		spanCount := 50 + (i % 51) // 50-100 spans
		traces[i] = generator.GenerateTrace(spanCount)

		if (i+1)%100000 == 0 {
			fmt.Printf("Generated %d traces...\n", i+1)
		}
	}

	fmt.Printf("\nGenerated %d traces (avg ~75 spans each)\n", len(traces))

	// Start write memory monitoring
	fmt.Println("\nStarting write memory monitor...")
	writeMemMonitor := tracestreaming.NewMemoryMonitor(5*time.Second, fmt.Sprintf("http://%s", pprofAddr), "profiles/write")
	writeMonitorCtx, cancelWriteMonitor := context.WithCancel(ctx)
	go writeMemMonitor.Start(writeMonitorCtx)

	// Write data
	fmt.Println("Writing traces to Docker container...")
	writeClient, err := tracestreaming.NewTraceWriteClient(banyandbAddr)
	if err != nil {
		log.Fatalf("Failed to create write client: %v", err)
	}
	defer writeClient.Close()

	startWrite := time.Now()
	batchSize := 1000
	for i := 0; i < len(traces); i += batchSize {
		end := i + batchSize
		if end > len(traces) {
			end = len(traces)
		}

		if err := writeClient.WriteTraces(ctx, traces[i:end]); err != nil {
			log.Fatalf("Write failed at trace %d: %v", i, err)
		}

		if (i+batchSize)%50000 == 0 {
			fmt.Printf("Written %d/%d traces...\n", i+batchSize, totalTraces)
		}
	}

	writeTime := time.Since(startWrite)
	fmt.Printf("Write completed in %v\n", writeTime)

	// Stop write monitor
	cancelWriteMonitor()

	// Get write memory stats
	writeMemStats := writeMemMonitor.GetStats()
	writePeakHeap, writePeakRSS := writeMemMonitor.GetPeakMemory()
	fmt.Printf("\n=== Write Phase Memory Stats ===\n")
	fmt.Printf("Peak Heap: %.2f MB\n", float64(writePeakHeap)/1024/1024)
	fmt.Printf("Peak RSS: %.2f MB\n", float64(writePeakRSS)/1024/1024)
	fmt.Printf("Avg Heap: %.2f MB\n", float64(writeMemStats.AvgHeapAlloc)/1024/1024)

	// Export write memory data
	if err := writeMemMonitor.ExportToCSV("quota_stress_write_memory.csv"); err == nil {
		fmt.Println("Write memory data: quota_stress_write_memory.csv")
	}

	// Wait for indexing
	fmt.Println("\nWaiting for data to be indexed (30 seconds)...")
	time.Sleep(30 * time.Second)

	// Check Docker logs for quota warnings
	fmt.Println("\n=== Checking Docker logs for quota warnings ===")
	checkDockerLogs()

	// Run queries with different MaxTraceSize values
	fmt.Println("\n=== Running Quota Stress Queries ===")

	// Start query memory monitoring
	queryMemMonitor := tracestreaming.NewMemoryMonitor(5*time.Second, fmt.Sprintf("http://%s", pprofAddr), "profiles/query")
	queryMonitorCtx, cancelQueryMonitor := context.WithCancel(ctx)
	go queryMemMonitor.Start(queryMonitorCtx)

	queryRunner, err := tracestreaming.NewQueryRunner(banyandbAddr)
	if err != nil {
		log.Fatalf("Failed to create query runner: %v", err)
	}
	defer queryRunner.Close()

	maxTraceSizes := []uint32{10, 50, 100}

	for _, maxTraceSize := range maxTraceSizes {
		fmt.Printf("\n--- Testing MaxTraceSize=%d ---\n", maxTraceSize)

		queryConfig := tracestreaming.QueryConfig{
			StartTime:    generator.baseTime,
			EndTime:      generator.baseTime.Add(config.TimeRange),
			MaxTraceSize: maxTraceSize,
		}

		// Execute queries
		numQueries := 30
		successCount := 0
		for i := 0; i < numQueries; i++ {
			count, err := queryRunner.ExecuteQuery(ctx, queryConfig)
			if err != nil {
				fmt.Printf("Query %d FAILED: %v\n", i+1, err)
				if i == 0 {
					log.Fatalf("First query failed, aborting test")
				}
			} else {
				successCount++
			}

			if i%10 == 9 {
				fmt.Printf("Completed %d queries, %d succeeded\n", i+1, successCount)
			}
		}

		metrics := queryRunner.GetMetrics().GetSummary()
		fmt.Printf("Results: %d/%d succeeded (%.1f%%), P95 latency=%v\n",
			successCount, numQueries, metrics.SuccessRate, metrics.LatencyP95)
	}

	// Stop query monitor
	cancelQueryMonitor()

	// Final Docker log check
	fmt.Println("\n=== Final Docker Log Check ===")
	checkDockerLogs()

	// Query memory stats
	queryMemStats := queryMemMonitor.GetStats()
	queryPeakHeap, queryPeakRSS := queryMemMonitor.GetPeakMemory()

	fmt.Printf("\n=== Query Phase Memory Stats ===\n")
	fmt.Printf("Peak Heap: %.2f MB\n", float64(queryPeakHeap)/1024/1024)
	fmt.Printf("Peak RSS: %.2f MB\n", float64(queryPeakRSS)/1024/1024)
	fmt.Printf("Avg Heap: %.2f MB\n", float64(queryMemStats.AvgHeapAlloc)/1024/1024)
	fmt.Printf("Container Limit: 1024 MB\n")

	// Export query memory data
	if err := queryMemMonitor.ExportToCSV("quota_stress_query_memory.csv"); err == nil {
		fmt.Println("\nQuery memory data: quota_stress_query_memory.csv")
		exec.Command("python3", "plot_memory.py", "quota_stress_query_memory.csv", "--output", "quota_stress_query_memory.png").Run()
		fmt.Println("Query memory chart: quota_stress_query_memory.png")
	}

	// Print memory comparison
	fmt.Printf("\n=== Memory Usage Comparison ===\n")
	fmt.Printf("Write Phase:\n")
	fmt.Printf("  Peak Heap: %.2f MB | Peak RSS: %.2f MB\n", float64(writePeakHeap)/1024/1024, float64(writePeakRSS)/1024/1024)
	fmt.Printf("Query Phase:\n")
	fmt.Printf("  Peak Heap: %.2f MB | Peak RSS: %.2f MB\n", float64(queryPeakHeap)/1024/1024, float64(queryPeakRSS)/1024/1024)

	fmt.Println("\n✅ Test Complete")
}

func checkDocker() error {
	cmd := exec.Command("docker", "ps", "--filter", "name=banyandb-quota-stress", "--format", "{{.Status}}")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("docker ps failed: %w", err)
	}

	if len(output) == 0 {
		return fmt.Errorf("container not running")
	}

	return nil
}

func checkDockerLogs() {
	cmd := exec.Command("docker", "logs", "banyandb-quota-stress", "--tail", "100")
	output, _ := cmd.CombinedOutput()

	// Look for quota errors
	lines := string(output)
	if found := grep(lines, "quota exceeded"); found != "" {
		fmt.Println("⚠️  QUOTA EXCEEDED errors found:")
		fmt.Println(found)
	} else {
		fmt.Println("✅ No quota exceeded errors")
	}

	if found := grep(lines, "falling back"); found != "" {
		fmt.Println("⚠️  FALLBACK to normal query:")
		fmt.Println(found)
	} else {
		fmt.Println("✅ No query fallbacks")
	}

	// Show memory protector status
	if found := grep(lines, "memory protector"); found != "" {
		fmt.Println("\nMemory protector status:")
		fmt.Println(found)
	}
}

func grep(text, pattern string) string {
	// Simple grep implementation
	result := ""
	for _, line := range splitLines(text) {
		if contains(line, pattern) {
			result += line + "\n"
		}
	}
	return result
}

func splitLines(s string) []string {
	lines := []string{}
	current := ""
	for _, c := range s {
		if c == '\n' {
			lines = append(lines, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func setupSchema(ctx context.Context, addr string) error {
	// TODO: Implement schema setup via gRPC
	// For now, rely on Docker container to load schema from /schema directory
	fmt.Println("Schema setup delegated to container startup")
	return nil
}
