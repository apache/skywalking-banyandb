// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	grpcAddr = flag.String("grpc-addr", "localhost:17912", "gRPC server address")
	group    = flag.String("group", "test_group", "Group name")
	measure  = flag.String("measure", "test_measure", "Measure name")
	dataPath = flag.String("data-path", "/tmp/measure", "BanyanDB data path to check series-metadata.bin")
	mode     = flag.String("mode", "standalone", "BanyanDB mode: 'standalone' or 'cluster' (liaison node)")
)

func main() {
	flag.Parse()

	// Connect to BanyanDB
	conn, err := grpc.Dial(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Create group
	if err := createGroup(ctx, conn, *group); err != nil {
		log.Fatalf("Failed to create group: %v", err)
	}
	log.Printf("✓ Group '%s' created or already exists", *group)

	// Create measure (non-IndexMode)
	if err := createMeasure(ctx, conn, *group, *measure); err != nil {
		log.Fatalf("Failed to create measure: %v", err)
	}
	log.Printf("✓ Measure '%s' created or already exists", *measure)

	// Wait for group to be loaded (group loading is asynchronous)
	log.Printf("Waiting for group '%s' to be loaded (3 seconds)...", *group)
	time.Sleep(3 * time.Second)

	// Write data points
	if err := writeDataPoints(ctx, conn, *group, *measure); err != nil {
		log.Fatalf("Failed to write data points: %v", err)
	}
	log.Printf("✓ Data points written successfully")

	// Wait for group to be loaded and data to be flushed
	// Note: In cluster mode, parts may be deleted after syncing to data nodes,
	// so we need to check before sync happens (usually within 5-10 seconds)
	waitTime := 8 * time.Second
	if *mode == "cluster" {
		waitTime = 8 * time.Second // Give enough time for data to flush
		log.Printf("Waiting for group to be loaded and data to be flushed (%v)...", waitTime)
		log.Println("Note: In cluster mode, parts may be deleted after syncing to data nodes.")
		log.Println("      If data node is stopped, parts will accumulate in liaison and files will be preserved.")
		log.Println("      If data node is running, parts will be synced and deleted quickly.")
	} else {
		log.Printf("Waiting for group to be loaded and data to be flushed (%v)...", waitTime)
	}
	time.Sleep(waitTime)

	// Verify series-metadata.bin file
	if err := verifySeriesMetadata(*dataPath, *group, *mode); err != nil {
		log.Fatalf("Verification failed: %v", err)
	}

	log.Println("✓ Verification completed successfully!")
}

func createGroup(ctx context.Context, conn *grpc.ClientConn, groupName string) error {
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)

	// Check if group exists
	existResp, err := groupClient.Exist(ctx, &databasev1.GroupRegistryServiceExistRequest{
		Group: groupName,
	})
	if err == nil && existResp.HasGroup {
		log.Printf("Group '%s' already exists, skipping creation", groupName)
		return nil
	}

	// Create group
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name:  groupName,
			Group: groupName,
		},
		Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_HOUR,
				Num:  1, // 1 hour
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  7, // 7 days
			},
		},
	}

	_, err = groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: group,
	})
	return err
}

func createMeasure(ctx context.Context, conn *grpc.ClientConn, groupName, measureName string) error {
	measureClient := databasev1.NewMeasureRegistryServiceClient(conn)

	// Check if measure exists
	existResp, err := measureClient.Exist(ctx, &databasev1.MeasureRegistryServiceExistRequest{
		Metadata: &commonv1.Metadata{
			Name:  measureName,
			Group: groupName,
		},
	})
	if err == nil && existResp.HasMeasure {
		log.Printf("Measure '%s' already exists, skipping creation", measureName)
		return nil
	}

	// Create measure (non-IndexMode)
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  measureName,
			Group: groupName,
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{
						Name: "id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "service_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "name",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "total",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
			{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"id", "service_id"},
		},
		Interval:  "1m",  // 1 minute (format: number + unit, e.g., "1m", "1h", "30s")
		IndexMode: false, // Important: non-IndexMode to generate metadataDocs
	}

	_, err = measureClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: measure,
	})
	return err
}

func writeDataPoints(ctx context.Context, conn *grpc.ClientConn, groupName, measureName string) error {
	client := measurev1.NewMeasureServiceClient(conn)
	writeClient, err := client.Write(ctx)
	if err != nil {
		return fmt.Errorf("failed to create write client: %w", err)
	}
	defer writeClient.CloseSend()

	metadata := &commonv1.Metadata{
		Name:  measureName,
		Group: groupName,
	}

	// Use millisecond precision timestamp as required by BanyanDB
	now := timestamp.NowMilli()
	messageID := uint64(1)

	// Write 10 data points with different series
	for i := 0; i < 10; i++ {
		// Add milliseconds (not seconds) to maintain millisecond precision
		timestamp := now.Add(time.Duration(i) * time.Second).Truncate(time.Millisecond)
		dataPoint := &measurev1.DataPointValue{
			Timestamp: timestamppb.New(timestamp),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: fmt.Sprintf("id_%d", i),
								},
							},
						},
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: fmt.Sprintf("service_%d", i%3),
								},
							},
						},
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: fmt.Sprintf("name_%d", i),
								},
							},
						},
					},
				},
			},
			Fields: []*modelv1.FieldValue{
				{
					Value: &modelv1.FieldValue_Int{
						Int: &modelv1.Int{
							Value: int64(100 + i),
						},
					},
				},
				{
					Value: &modelv1.FieldValue_Float{
						Float: &modelv1.Float{
							Value: 10.5 + float64(i),
						},
					},
				},
			},
			Version: int64(messageID),
		}

		request := &measurev1.WriteRequest{
			Metadata:  metadata,
			DataPoint: dataPoint,
			MessageId: messageID,
		}

		if err := writeClient.Send(request); err != nil {
			return fmt.Errorf("failed to send data point %d: %w", i, err)
		}

		messageID++
	}

	// Receive responses
	go func() {
		for {
			resp, err := writeClient.Recv()
			if err != nil {
				return
			}
			if resp.Status != "SUCCEED" {
				log.Printf("Warning: Write response status: %s", resp.Status)
			}
		}
	}()

	return nil
}

func verifySeriesMetadata(dataPath, groupName, mode string) error {
	log.Printf("Checking for series-metadata.bin in: %s (group: %s, mode: %s)", dataPath, groupName, mode)

	// BanyanDB stores data in different structures based on mode:
	// - Standalone mode: {dataPath}/data/{group}/seg-{date}/shard-{id}/{part_id}
	// - Cluster mode (liaison): {dataPath}/measure/data/{group}/shard-{id}/{part_id} (no seg-* directory)
	var groupDataPath string
	switch mode {
	case "standalone":
		groupDataPath = filepath.Join(dataPath, "data", groupName)
	case "cluster":
		groupDataPath = filepath.Join(dataPath, "measure", "data", groupName)
	default:
		return fmt.Errorf("invalid mode: %s. Must be 'standalone' or 'cluster'", mode)
	}

	if _, err := os.Stat(groupDataPath); os.IsNotExist(err) {
		return fmt.Errorf("group data directory not found: %s\nMake sure:\n  1. Group has been created\n  2. Data has been written successfully\n  3. Data has been flushed\n  4. The mode is correct (%s)", groupDataPath, mode)
	}

	log.Printf("Found group data directory: %s", groupDataPath)

	var partDirs []string
	err := filepath.Walk(groupDataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Continue on error
		}
		if !info.IsDir() {
			return nil
		}
		// Check if directory name is 16 hex digits (part ID)
		dirName := filepath.Base(path)
		if len(dirName) == 16 {
			// Verify it's all hex digits
			isHex := true
			for _, c := range dirName {
				if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
					isHex = false
					break
				}
			}
			if isHex {
				partDirs = append(partDirs, path)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(partDirs) == 0 {
		if mode == "cluster" {
			return fmt.Errorf("no part directories found in %s. In cluster mode, parts may have been synced to data nodes and deleted from liaison node. To verify series-metadata.bin file: 1) Stop the data node before writing data, 2) Write data to liaison, 3) Check the liaison directory - parts will accumulate because sync fails, 4) Files will be preserved in the part directories. Make sure: 1) Data has been written successfully, 2) Data node is stopped (so sync fails and parts are preserved), 3) The group name is correct, 4) Wait 8+ seconds for data to flush", groupDataPath)
		}
		return fmt.Errorf("no part directories found in %s. Make sure:\n  1. Data has been written successfully\n  2. Data has been flushed (wait 5+ seconds)\n  3. The group name is correct", groupDataPath)
	}

	// Verify based on mode
	switch mode {
	case "standalone":
		// In standalone mode, series-metadata.bin should NOT exist
		latestPart := partDirs[len(partDirs)-1]
		seriesMetadataPath := filepath.Join(latestPart, "series-metadata.bin")
		log.Printf("Checking latest part: %s", latestPart)
		log.Printf("Looking for: %s", seriesMetadataPath)

		_, err := os.Stat(seriesMetadataPath)
		fileExists := err == nil && !os.IsNotExist(err)
		if fileExists {
			return fmt.Errorf("series-metadata.bin should NOT exist in standalone mode, but found at %s\nThis indicates a bug: standalone mode should not generate series-metadata.bin", seriesMetadataPath)
		}
		log.Printf("✓ Verified: series-metadata.bin does NOT exist (correct for standalone mode)")

		// List all files in the part directory for reference
		log.Printf("\nAll files in part directory:")
		files, err := os.ReadDir(latestPart)
		if err == nil {
			for _, f := range files {
				if !f.IsDir() {
					fileInfo, _ := f.Info()
					log.Printf("  - %s (%d bytes)", f.Name(), fileInfo.Size())
				}
			}
		}

	case "cluster":
		// In cluster mode (liaison), check for series-metadata.bin in part directories
		// When data node is stopped, parts accumulate and files are preserved
		var foundMetadataFiles []string

		// Check each part directory for series-metadata.bin
		for _, partDir := range partDirs {
			seriesMetadataPath := filepath.Join(partDir, "series-metadata.bin")
			if info, err := os.Stat(seriesMetadataPath); err == nil && !info.IsDir() {
				if info.Size() > 0 {
					foundMetadataFiles = append(foundMetadataFiles, seriesMetadataPath)
				}
			}
		}

		if len(foundMetadataFiles) == 0 {
			// List what files are actually in the part directories for debugging
			log.Printf("\nDebug: Checking part directories for files...")
			for i, partDir := range partDirs {
				if i >= 3 { // Only show first 3 parts
					break
				}
				log.Printf("  Part directory: %s", partDir)
				files, err := os.ReadDir(partDir)
				if err == nil {
					for _, f := range files {
						if !f.IsDir() {
							fileInfo, _ := f.Info()
							log.Printf("    - %s (%d bytes)", f.Name(), fileInfo.Size())
						}
					}
				}
			}

			return fmt.Errorf("series-metadata.bin not found in part directories under %s. Make sure: 1) Data node is STOPPED (so sync fails and parts are preserved), 2) You're using the updated code, 3) The measure is non-IndexMode (IndexMode: false), 4) Data has been flushed (wait 8+ seconds), 5) You're checking the liaison node's data directory. If data node is running, parts will be synced and deleted quickly, making it hard to verify files", groupDataPath)
		}

		// Check the most recent metadata file
		latestMetadataFile := foundMetadataFiles[len(foundMetadataFiles)-1]
		info, err := os.Stat(latestMetadataFile)
		if err != nil {
			return fmt.Errorf("failed to stat series-metadata file: %w", err)
		}
		if info.Size() == 0 {
			return fmt.Errorf("series-metadata.bin exists but is empty")
		}
		log.Printf("✓ Found series-metadata.bin")
		log.Printf("  Path: %s", latestMetadataFile)
		log.Printf("  Size: %d bytes", info.Size())
		log.Printf("  Note: File found because data node is stopped, so parts accumulated in liaison")

	default:
		return fmt.Errorf("invalid mode: %s. Must be 'standalone' or 'cluster'", mode)
	}

	return nil
}
