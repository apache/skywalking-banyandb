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

package tracestreaming

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func TestTraceStreamingPerformance(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Trace Streaming Performance Suite", g.Label("integration", "performance", "slow"))
}

var _ = g.Describe("Trace Streaming Performance", func() {
	g.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).To(gomega.Succeed())
	})

	g.Context("Small Scale (100K traces, 5 min)", func() {
		g.It("should handle bounded memory with MaxTraceSize=[10, 50, 100]", func() {
			testPath, deferFn, err := test.NewSpace()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			g.DeferCleanup(deferFn)

			ports, err := test.AllocateFreePorts(4)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Start BanyanDB with schema
			addr, pprofAddr, closerServerFunc := setup.ClosableStandaloneWithSchemaLoaders(
				testPath, ports,
				[]setup.SchemaLoader{NewSchemaLoader("trace-streaming")},
				"--logging-level", "info")

			g.DeferCleanup(func() {
				closerServerFunc()
				helpers.PrintDiskUsage(testPath, 5, 0)
			})

			// Wait for server to be ready
			gomega.Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
				flags.EventuallyTimeout).Should(gomega.Succeed())

			// Create gRPC connection
			conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer conn.Close()

			ctx := context.Background()

			// Setup for memory monitoring
			profileDir := filepath.Join(testPath, "profiles")
			_ = os.MkdirAll(profileDir, 0o755)
			pprofURL := fmt.Sprintf("http://%s", pprofAddr)

			// Generate and write small scale data
			generator := NewTraceGenerator(SmallScale)
			fmt.Printf("Generating %s scale data...\n", SmallScale)
			traces := generator.GenerateTraces()
			fmt.Printf("Generated %d traces\n", len(traces))

			// Create write memory monitor
			writeMemMonitor := NewMemoryMonitor(5*time.Second, pprofURL, filepath.Join(profileDir, "write"))
			writeMonitorCtx, cancelWriteMonitor := context.WithCancel(ctx)
			go writeMemMonitor.Start(writeMonitorCtx)

			// Write data
			writeClient, err := NewTraceWriteClient(addr)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer writeClient.Close()

			fmt.Println("Writing traces...")
			startWrite := time.Now()

			// Write in batches to avoid overwhelming the server
			batchSize := 100
			for i := 0; i < len(traces); i += batchSize {
				end := i + batchSize
				if end > len(traces) {
					end = len(traces)
				}

				batch := traces[i:end]
				err = writeClient.WriteTraces(ctx, batch)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				if (i+batchSize)%10000 == 0 {
					fmt.Printf("Written %d traces...\n", i+batchSize)
				}
			}

			writeTime := time.Since(startWrite)
			fmt.Printf("Write completed in %v\n", writeTime)

			// Stop write monitor
			cancelWriteMonitor()

			// Get write memory stats
			writeMemStats := writeMemMonitor.GetStats()
			writePeakHeap, writePeakRSS := writeMemMonitor.GetPeakMemory()
			fmt.Printf("\nWrite Phase Memory Stats:\n")
			fmt.Printf("  Peak Heap: %.2f MB\n", float64(writePeakHeap)/1024/1024)
			fmt.Printf("  Peak RSS: %.2f MB\n", float64(writePeakRSS)/1024/1024)
			fmt.Printf("  Avg Heap: %.2f MB\n", float64(writeMemStats.AvgHeapAlloc)/1024/1024)
			fmt.Printf("  Max Goroutines: %d\n", writeMemStats.MaxGoroutines)
			fmt.Printf("  Total GC: %d\n", writeMemStats.TotalGC)

			// Export write memory data
			writeCSVFile := filepath.Join(testPath, "memory_write_small.csv")
			err = writeMemMonitor.ExportToCSV(writeCSVFile)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fmt.Printf("Write memory data exported to: %s\n", writeCSVFile)

			// Also save to current directory
			_ = writeMemMonitor.ExportToCSV("memory_write_small.csv")

			// Wait for data to be indexed
			time.Sleep(5 * time.Second)

			// Create query memory monitor
			queryMemMonitor := NewMemoryMonitor(5*time.Second, pprofURL, filepath.Join(profileDir, "query"))
			queryMonitorCtx, cancelQueryMonitor := context.WithCancel(ctx)
			defer cancelQueryMonitor()
			go queryMemMonitor.Start(queryMonitorCtx)

			// Test different MaxTraceSize values
			maxTraceSizes := []uint32{10, 50, 100}
			config := GetScaleConfig(SmallScale)
			comparisons := make([]MaxTraceSizeComparison, 0, len(maxTraceSizes))

			for _, maxTraceSize := range maxTraceSizes {
				fmt.Printf("\nTesting with MaxTraceSize=%d\n", maxTraceSize)

				var queryRunner *QueryRunner
				queryRunner, err = NewQueryRunner(addr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				defer queryRunner.Close()

				// Execute queries
				queryConfig := QueryConfig{
					StartTime:    generator.baseTime,
					EndTime:      generator.baseTime.Add(config.TimeRange),
					MaxTraceSize: maxTraceSize,
				}

				// Run multiple queries to get meaningful metrics
				numQueries := 20
				for i := 0; i < numQueries; i++ {
					var count int
					count, err = queryRunner.ExecuteQuery(ctx, queryConfig)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					gomega.Expect(count).To(gomega.BeNumerically(">=", 0))
					gomega.Expect(count).To(gomega.BeNumerically("<=", int(maxTraceSize)))
				}

				// Get metrics summary
				metrics := queryRunner.GetMetrics().GetSummary()
				metrics.PrintSummary(fmt.Sprintf("MaxTraceSize=%d", maxTraceSize))

				// Store for comparison
				comparisons = append(comparisons, MaxTraceSizeComparison{
					MaxTraceSize: maxTraceSize,
					Metrics:      metrics,
				})

				// Verify latency is reasonable
				gomega.Expect(metrics.LatencyP95).To(gomega.BeNumerically("<", 10*time.Second))
			}

			// Print comparison
			PrintComparison(comparisons)

			// Stop query monitor
			cancelQueryMonitor()

			// Get query memory stats
			queryMemStats := queryMemMonitor.GetStats()
			queryPeakHeap, queryPeakRSS := queryMemMonitor.GetPeakMemory()
			fmt.Printf("\nQuery Phase Memory Stats:\n")
			fmt.Printf("  Peak Heap: %.2f MB\n", float64(queryPeakHeap)/1024/1024)
			fmt.Printf("  Peak RSS: %.2f MB\n", float64(queryPeakRSS)/1024/1024)
			fmt.Printf("  Avg Heap: %.2f MB\n", float64(queryMemStats.AvgHeapAlloc)/1024/1024)
			fmt.Printf("  Max Goroutines: %d\n", queryMemStats.MaxGoroutines)
			fmt.Printf("  Total GC: %d\n", queryMemStats.TotalGC)

			// Export query memory data
			queryCSVFile := filepath.Join(testPath, "memory_query_small.csv")
			err = queryMemMonitor.ExportToCSV(queryCSVFile)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fmt.Printf("Query memory data exported to: %s\n", queryCSVFile)

			// Also save to current directory
			_ = queryMemMonitor.ExportToCSV("memory_query_small.csv")

			// Print summary comparison
			fmt.Printf("\n=== Memory Usage Comparison ===\n")
			fmt.Printf("Write Phase:\n")
			fmt.Printf("  Peak Heap: %.2f MB | Peak RSS: %.2f MB\n", float64(writePeakHeap)/1024/1024, float64(writePeakRSS)/1024/1024)
			fmt.Printf("Query Phase:\n")
			fmt.Printf("  Peak Heap: %.2f MB | Peak RSS: %.2f MB\n", float64(queryPeakHeap)/1024/1024, float64(queryPeakRSS)/1024/1024)

			// Suggest plotting command
			fmt.Printf("\n💡 To generate memory plots, run:\n")
			fmt.Printf("   python3 plot_memory_comparison.py memory_write_small.csv memory_query_small.csv\n")
		})
	})
})
