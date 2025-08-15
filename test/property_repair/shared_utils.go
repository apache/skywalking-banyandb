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

package property_repair

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

const (
	DataSize     = 2048 // 2KB per property
	LiaisonAddr  = "localhost:17912"
	Concurrency  = 6
	GroupName    = "perf-test-group"
	PropertyName = "perf-test-property"
)

// GenerateLargeData creates a string of specified size filled with random characters
func GenerateLargeData(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Generate some random bytes
	randomBytes := make([]byte, 32)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp-based data
		baseData := fmt.Sprintf("timestamp-%d-", time.Now().UnixNano())
		repeats := size / len(baseData)
		if repeats == 0 {
			repeats = 1
		}
		return strings.Repeat(baseData, repeats)[:size]
	}

	// Create base string from random bytes
	var baseBuilder strings.Builder
	for _, b := range randomBytes {
		baseBuilder.WriteByte(charset[b%byte(len(charset))])
	}
	baseData := baseBuilder.String()

	// Repeat to reach desired size
	repeats := (size / len(baseData)) + 1
	result := strings.Repeat(baseData, repeats)

	if len(result) > size {
		return result[:size]
	}
	return result
}

// FormatDuration formats a duration to a human-readable string
func FormatDuration(duration time.Duration) string {
	if duration < time.Second {
		return fmt.Sprintf("%dms", duration.Milliseconds())
	}
	if duration < time.Minute {
		return fmt.Sprintf("%.1fs", duration.Seconds())
	}
	return fmt.Sprintf("%.1fm", duration.Minutes())
}

// FormatThroughput calculates and formats throughput
func FormatThroughput(count int64, duration time.Duration) string {
	if duration == 0 {
		return "N/A"
	}
	throughput := float64(count) / duration.Seconds()
	return fmt.Sprintf("%.1f/s", throughput)
}

// LogProgress prints progress information
func LogProgress(current, total int64, startTime time.Time, operation string) {
	elapsed := time.Since(startTime)
	percentage := float64(current) / float64(total) * 100
	throughput := FormatThroughput(current, elapsed)

	fmt.Printf("[%s] Progress: %d/%d (%.1f%%) - Elapsed: %s - Throughput: %s\n",
		operation, current, total, percentage, FormatDuration(elapsed), throughput)
}

// CreateGroupName generates group name with timestamp
func CreateGroupName(prefix, suffix string) string {
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

// CreatePropertyName generates property name with timestamp
func CreatePropertyName(prefix, suffix string) string {
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

// GetTimestampSuffix returns a timestamp-based suffix
func GetTimestampSuffix() string {
	return strings.ReplaceAll(time.Now().Format("2006-01-02-15-04-05"), "-", "")
}

// CreateGroup creates a property group with specified parameters
func CreateGroup(ctx context.Context, groupClient databasev1.GroupRegistryServiceClient, replicaNum uint32) {
	fmt.Printf("Creating group %s with %d replicas...\n", GroupName, replicaNum)
	_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: GroupName,
			},
			Catalog: commonv1.Catalog_CATALOG_PROPERTY,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				Replicas: replicaNum,
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
}

// UpdateGroupReplicas updates the replica number of an existing group
func UpdateGroupReplicas(ctx context.Context, groupClient databasev1.GroupRegistryServiceClient, newReplicaNum uint32) {
	fmt.Printf("Updating group %s to %d replicas...\n", GroupName, newReplicaNum)
	_, err := groupClient.Update(ctx, &databasev1.GroupRegistryServiceUpdateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: GroupName,
			},
			Catalog: commonv1.Catalog_CATALOG_PROPERTY,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				Replicas: newReplicaNum,
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
}

// CreatePropertySchema creates a property schema
func CreatePropertySchema(ctx context.Context, propertyClient databasev1.PropertyRegistryServiceClient) {
	fmt.Printf("Creating property schema %s...\n", PropertyName)
	_, err := propertyClient.Create(ctx, &databasev1.PropertyRegistryServiceCreateRequest{
		Property: &databasev1.Property{
			Metadata: &commonv1.Metadata{
				Name:  PropertyName,
				Group: GroupName,
			},
			Tags: []*databasev1.TagSpec{
				{Name: "data", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
}

// WriteProperties writes a batch of properties concurrently
func WriteProperties(ctx context.Context, propertyServiceClient propertyv1.PropertyServiceClient,
	startIdx int, endIdx int,
) error {
	fmt.Printf("Starting to write %d-%d properties using %d goroutines...\n",
		startIdx, endIdx, Concurrency)

	startTime := time.Now()

	// Channel to generate property data
	dataChannel := make(chan int, 1000) // Buffer for property indices
	var wg sync.WaitGroup
	var totalProcessed int64

	// Start data producer goroutine
	go func() {
		defer close(dataChannel)
		for i := startIdx; i < endIdx; i++ {
			dataChannel <- i
		}
	}()

	// Start consumer goroutines
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()
			var count int64

			for propertyIndex := range dataChannel {
				propertyID := fmt.Sprintf("property-%d", propertyIndex)
				largeData := GenerateLargeData(DataSize)
				timestamp := time.Now().Format(time.RFC3339Nano)

				_, writeErr := propertyServiceClient.Apply(ctx, &propertyv1.ApplyRequest{
					Property: &propertyv1.Property{
						Metadata: &commonv1.Metadata{
							Name:  PropertyName,
							Group: GroupName,
						},
						Id: propertyID,
						Tags: []*modelv1.Tag{
							{Key: "data", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: largeData}}}},
							{Key: "timestamp", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: timestamp}}}},
						},
					},
				})
				Expect(writeErr).NotTo(HaveOccurred())

				count++
				atomic.AddInt64(&totalProcessed, 1)

				if atomic.LoadInt64(&totalProcessed)%500 == 0 {
					elapsed := time.Since(startTime)
					totalCount := atomic.LoadInt64(&totalProcessed)
					fmt.Printf("total processed: %d, use: %v\n", totalCount, elapsed)
				}
			}

			fmt.Printf("Worker %d completed: processed %d properties total\n", workerID, count)
		}(i)
	}

	wg.Wait()
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("Write completed: %d properties in %s (%s props/sec)\n",
		endIdx, FormatDuration(duration), FormatThroughput(int64(endIdx), duration))
	return nil
}
