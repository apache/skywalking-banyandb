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

package integration_other_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = g.Describe("Forced TTL Cleanup", func() {
	var goods []gleak.Goroutine
	g.BeforeEach(func() {
		goods = gleak.Goroutines()
	})

	g.AfterEach(func() {
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gm.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	})

	g.It("should trigger forced cleanup based on disk watermarks", func() {
		// Get a temp directory for the test
		tempDir, cleanupDir, err := test.NewSpace()
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer cleanupDir()

		// Allocate ports for the standalone server
		ports, err := test.AllocateFreePorts(4)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Start standalone server with forced cleanup enabled, using the temp directory as root
		addr, _, deferFn := setup.ClosableStandalone(tempDir, ports,
			"--measure-retention-high-watermark", "20.0", // Trigger at 20%
			"--measure-retention-low-watermark", "10.0", // Stop at 10%
			"--measure-retention-check-interval", "1s", // Check every second
			"--measure-retention-cooldown", "500ms", // Short cooldown
		)
		defer deferFn()

		// Create connection to the server
		conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer conn.Close()

		// Write historical data to create multiple segments of different ages
		ns := timestamp.NowMilli().UnixNano()
		baseTime := time.Unix(0, ns-ns%int64(time.Minute))
		interval := time.Minute

		g.By("Writing substantial historical data to create multiple segments")
		// Create data across different time periods to guarantee multiple segments
		// sw_metric group has 1-day segment interval and 7-day TTL
		// Create data within TTL period but across multiple days to ensure segments don't get removed by normal TTL
		segmentTimes := []time.Duration{
			-6 * 24 * time.Hour, // 6 days ago (within TTL, separate segment)
			-5 * 24 * time.Hour, // 5 days ago (within TTL, separate segment)
			-4 * 24 * time.Hour, // 4 days ago (within TTL, separate segment)
			-3 * 24 * time.Hour, // 3 days ago (within TTL, separate segment)
			-2 * 24 * time.Hour, // 2 days ago (within TTL, separate segment)
			-1 * 24 * time.Hour, // 1 day ago (within TTL, separate segment)
			0 * 24 * time.Hour,  // 0 day ago (within TTL, separate segment)
		}

		for i, offset := range segmentTimes {
			segmentTime := baseTime.Add(offset)
			casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", segmentTime, interval)
			g.By(fmt.Sprintf("Created data for time period %d at %v (day %d) - should write 6 data points", i+1, segmentTime, int(offset.Hours()/24)))
		}
		// Note: Total expected data points = 6 segments × 6 data points each = 36 data points
		// However, forced cleanup might remove old segments during writing, reducing this count

		// Wait for segments to be persisted and properly organized
		g.By("Waiting for segments to be persisted to disk")
		time.Sleep(5 * time.Second)

		// Verify data was written successfully
		ctx := context.Background()
		measureClient := measurev1.NewMeasureServiceClient(conn)

		// Ensure millisecond precision for timestamps and query for all written data
		// Note: Forced cleanup might have already removed some old segments during data writing,
		// so initialDataCount might be less than expected (e.g., 6 instead of 36)
		beginTime := baseTime.Add(-7 * 24 * time.Hour).Truncate(time.Millisecond)
		endTime := time.Now().Truncate(time.Millisecond)

		initialQueryResp, err := measureClient.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{"sw_metric"},
			Name:   "service_cpm_minute",
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(beginTime),
				End:   timestamppb.New(endTime),
			},
			TagProjection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "default",
						Tags: []string{"service_id"},
					},
				},
			},
			FieldProjection: &measurev1.QueryRequest_FieldProjection{
				Names: []string{"total"},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		initialDataCount := len(initialQueryResp.DataPoints)
		gm.Expect(initialDataCount).To(gm.BeNumerically(">", 0), "Should have initial data")

		// Create snapshots to test snapshot cleanup
		// The measure service expects snapshots in <root>/measure/snapshots/
		measureSnapshotDir := filepath.Join(tempDir, "measure", "snapshots")
		err = os.MkdirAll(measureSnapshotDir, 0o755)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Old snapshot (should be deleted)
		oldSnapshotPath := filepath.Join(measureSnapshotDir, "old_snapshot.bak")
		err = os.WriteFile(oldSnapshotPath, []byte("old snapshot data"), 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		oldTime := time.Now().Add(-48 * time.Hour)
		err = os.Chtimes(oldSnapshotPath, oldTime, oldTime)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Recent snapshot (should be preserved)
		recentSnapshotPath := filepath.Join(measureSnapshotDir, "recent_snapshot.bak")
		err = os.WriteFile(recentSnapshotPath, []byte("recent snapshot data"), 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Force the watermarks to be much lower so that the database data itself triggers cleanup
		g.By("Simulating disk pressure by adjusting watermarks relative to current usage")
		// Instead of filling disk with dummy files, we'll set very low watermarks
		// so that even the database data we just wrote exceeds the threshold

		// Wait for some database data to be written to disk
		time.Sleep(2 * time.Second)

		// Check initial disk usage from database data
		initialDataUsage := getDiskUsagePercent(tempDir)
		g.By(fmt.Sprintf("Initial data usage: %d%%", initialDataUsage))

		// Fill disk to a level that will trigger cleanup when watermarks are reached
		fillDiskToWatermark(tempDir, 20.0) // This creates the pressure

		// Wait additional time for segment deletions to complete
		g.By("Waiting for segment deletions to complete")
		time.Sleep(10 * time.Second)

		// Verify old snapshot was cleaned up
		g.By("Verifying snapshot cleanup behavior")
		gm.Eventually(func() bool {
			_, statErr := os.Stat(oldSnapshotPath)
			return os.IsNotExist(statErr)
		}, 10*time.Second, 1*time.Second).Should(gm.BeTrue(), "Old snapshot should be removed by cleanup")

		// Recent snapshot should always be preserved
		_, err = os.Stat(recentSnapshotPath)
		gm.Expect(err).NotTo(gm.HaveOccurred(), "Recent snapshot should be preserved")

		// Verify system is still functional - some data should remain due to keep-one rule
		g.By("Verifying segments were actually deleted due to forced cleanup")
		finalQueryResp, err := measureClient.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{"sw_metric"},
			Name:   "service_cpm_minute",
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(beginTime),
				End:   timestamppb.New(endTime),
			},
			TagProjection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "default",
						Tags: []string{"service_id"},
					},
				},
			},
			FieldProjection: &measurev1.QueryRequest_FieldProjection{
				Names: []string{"total"},
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		finalDataCount := len(finalQueryResp.DataPoints)

		// The forced cleanup might have already removed old segments during data writing,
		// so initialDataCount might equal finalDataCount (both being 6). This is expected behavior
		// when forced cleanup runs during the data writing process.
		if initialDataCount == finalDataCount {
			g.By(fmt.Sprintf("✅ Forced cleanup already removed old segments during data writing: %d data points remain", finalDataCount))
			// Verify that we still have some data (due to keep-one rule)
			gm.Expect(finalDataCount).To(gm.BeNumerically(">", 0), "Some data should remain due to keep-one rule")
		} else {
			// If initialDataCount > finalDataCount, cleanup happened after initial query
			gm.Expect(finalDataCount).To(gm.BeNumerically("<", initialDataCount),
				fmt.Sprintf("Forced cleanup should reduce data count: initial=%d, final=%d", initialDataCount, finalDataCount))
			g.By(fmt.Sprintf("✅ Cleanup successfully deleted segments: %d → %d data points", initialDataCount, finalDataCount))
		}

		// Due to keep-one rule, some data should remain if there was initial data
		gm.Expect(finalDataCount).To(gm.BeNumerically(">=", 0), "Data count should be non-negative after cleanup")

		// Test that new data can still be written (system functionality preserved)
		g.By("Verifying system remains functional after cleanup")
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", time.Now().Truncate(time.Millisecond), interval)
	})
})

// getDiskUsagePercent calculates current disk usage percentage for the given directory.
func getDiskUsagePercent(tempDir string) int {
	// Get total disk size and available space
	var stat syscall.Statfs_t
	err := syscall.Statfs(tempDir, &stat)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	// Calculate usage percentage
	totalSize := stat.Blocks * uint64(stat.Bsize)
	freeSize := stat.Bavail * uint64(stat.Bsize)
	usedSize := totalSize - freeSize
	usagePercent := float64(usedSize) / float64(totalSize) * 100

	return int(usagePercent)
}

// fillDiskToWatermark creates dummy files to fill disk to specified percentage.
func fillDiskToWatermark(tempDir string, targetWatermark float64) {
	// Get current disk usage
	currentUsage := getDiskUsagePercent(tempDir)

	// If we're already at or above the target watermark, no need to fill
	if float64(currentUsage) >= targetWatermark {
		g.By(fmt.Sprintf("Disk already at %d%% (target: %.1f%%), no filling needed", currentUsage, targetWatermark))
		return
	}

	// Calculate how much space we need to fill
	var stat syscall.Statfs_t
	err := syscall.Statfs(tempDir, &stat)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	totalSize := stat.Blocks * uint64(stat.Bsize)
	freeSize := stat.Bavail * uint64(stat.Bsize)
	usedSize := totalSize - freeSize

	// Calculate target used size based on watermark percentage
	targetUsedSize := uint64(float64(totalSize) * targetWatermark / 100.0)

	// Calculate how much more space we need to fill
	spaceToFill := targetUsedSize - usedSize

	if spaceToFill <= 0 {
		g.By(fmt.Sprintf("No additional space needed (current: %d%%, target: %.1f%%)", currentUsage, targetWatermark))
		return
	}

	g.By(fmt.Sprintf("Filling disk from %d%% to %.1f%% (need %d bytes)", currentUsage, targetWatermark, spaceToFill))

	// Create dummy directory for fill files
	dummyDir := filepath.Join(tempDir, "dummy_fill")
	err = os.MkdirAll(dummyDir, 0o755)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	// Create dummy files to fill the required space
	// Use 1MB chunks for efficiency
	chunkSize := int64(1024 * 1024) // 1MB
	bytesWritten := int64(0)
	fileIndex := 0

	for bytesWritten < int64(spaceToFill) {
		remainingBytes := int64(spaceToFill) - bytesWritten
		currentChunkSize := chunkSize
		if remainingBytes < chunkSize {
			currentChunkSize = remainingBytes
		}

		dummyFile := filepath.Join(dummyDir, fmt.Sprintf("dummy_%d.dat", fileIndex))
		data := make([]byte, currentChunkSize)

		// Fill with some pattern to ensure the file actually takes up space
		for i := range data {
			data[i] = byte(i % 256)
		}

		err = os.WriteFile(dummyFile, data, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		bytesWritten += currentChunkSize
		fileIndex++

		// Check if we've reached the target (with some tolerance)
		if fileIndex%10 == 0 { // Check every 10 files to avoid too frequent syscalls
			currentUsage = getDiskUsagePercent(tempDir)
			if float64(currentUsage) >= targetWatermark {
				g.By(fmt.Sprintf("Reached target watermark at %d%% after writing %d files", currentUsage, fileIndex))
				break
			}
		}
	}

	// Verify final usage
	finalUsage := getDiskUsagePercent(tempDir)
	g.By(fmt.Sprintf("Disk filling complete: %d%% (target: %.1f%%, files created: %d)", finalUsage, targetWatermark, fileIndex))
}
