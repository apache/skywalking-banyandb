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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

func main() {
	var (
		path    = flag.String("path", "/tmp/fodc-flight-recorder.bin", "Path to flight recorder file")
		format  = flag.String("format", "pretty", "Output format: pretty, json, or stats")
		limit   = flag.Int("limit", 0, "Limit number of snapshots to display (0 = all)")
		recent  = flag.Bool("recent", false, "Show only recent snapshots (last 10)")
		recentN = flag.Uint("recent-n", 10, "Number of recent snapshots to show when using --recent")
		output  = flag.String("output", "", "Output file path (default: stdout)")
		stream  = flag.Bool("stream", false, "Stream JSON output (one snapshot per line) for large datasets")
		verbose = flag.Bool("verbose", false, "Show timing and performance information")
		clear   = flag.Bool("clear", false, "Clear all snapshots from the flight recorder")
	)
	flag.Parse()

	startTime := time.Now()

	// Open flight recorder
	fr, err := flightrecorder.NewFlightRecorder(*path, 0)
	if err != nil {
		log.Fatalf("Failed to open flight recorder file: %v", err)
	}
	defer fr.Close()

	// Get stats
	totalCount, bufferSize, writeIndex := fr.GetStats()

	// If clear requested, clear and exit
	if *clear {
		if err := fr.Clear(); err != nil {
			log.Fatalf("Failed to clear flight recorder: %v", err)
		}
		fmt.Printf("Flight recorder cleared successfully\n")
		fmt.Printf("  Path: %s\n", *path)
		fmt.Printf("  Buffer size: %d\n", bufferSize)
		return
	}

	// If only stats requested, show and exit
	if *format == "stats" {
		// Read snapshots to get time range
		allSnapshots, err := fr.ReadAll()
		if err == nil && len(allSnapshots) > 0 {
			var earliestTime, latestTime time.Time
			earliestTime = allSnapshots[0].Timestamp
			latestTime = allSnapshots[0].Timestamp
			for _, s := range allSnapshots {
				if s.Timestamp.Before(earliestTime) {
					earliestTime = s.Timestamp
				}
				if s.Timestamp.After(latestTime) {
					latestTime = s.Timestamp
				}
			}
			timeSpan := latestTime.Sub(earliestTime)
			
			fmt.Printf("Flight Recorder Statistics:\n")
			fmt.Printf("  Path: %s\n", *path)
			fmt.Printf("  Total snapshots recorded: %d\n", totalCount)
			fmt.Printf("  Buffer size: %d\n", bufferSize)
			fmt.Printf("  Current write index: %d\n", writeIndex)
			fmt.Printf("  Time Range:\n")
			fmt.Printf("    Earliest: %s\n", earliestTime.Format(time.RFC3339))
			fmt.Printf("    Latest:   %s\n", latestTime.Format(time.RFC3339))
			fmt.Printf("    Duration: %v\n", timeSpan.Round(time.Second))
			if len(allSnapshots) > 1 {
				avgInterval := timeSpan / time.Duration(len(allSnapshots)-1)
				fmt.Printf("    Average interval: %v\n", avgInterval.Round(time.Second))
			}
		} else {
			fmt.Printf("Flight Recorder Statistics:\n")
			fmt.Printf("  Path: %s\n", *path)
			fmt.Printf("  Total snapshots recorded: %d\n", totalCount)
			fmt.Printf("  Buffer size: %d\n", bufferSize)
			fmt.Printf("  Current write index: %d\n", writeIndex)
		}
		return
	}

	// Read snapshots
	readStartTime := time.Now()
	var allSnapshots []interface{}
	var typedSnapshots []poller.MetricsSnapshot
	if *recent {
		recentSnapshots, err := fr.ReadRecent(uint32(*recentN))
		if err != nil {
			log.Fatalf("Failed to read recent snapshots: %v", err)
		}
		typedSnapshots = recentSnapshots
		for _, s := range recentSnapshots {
			allSnapshots = append(allSnapshots, s)
		}
	} else {
		snapshots, err := fr.ReadAll()
		if err != nil {
			log.Fatalf("Failed to read snapshots: %v", err)
		}
		typedSnapshots = snapshots
		for _, s := range snapshots {
			allSnapshots = append(allSnapshots, s)
		}
	}
	snapshots := allSnapshots
	readDuration := time.Since(readStartTime)
	
	// Calculate time range
	var earliestTime, latestTime time.Time
	if len(typedSnapshots) > 0 {
		earliestTime = typedSnapshots[0].Timestamp
		latestTime = typedSnapshots[0].Timestamp
		for _, s := range typedSnapshots {
			if s.Timestamp.Before(earliestTime) {
				earliestTime = s.Timestamp
			}
			if s.Timestamp.After(latestTime) {
				latestTime = s.Timestamp
			}
		}
	}

	if len(snapshots) == 0 {
		fmt.Printf("No snapshots found in flight recorder file: %s\n", *path)
		fmt.Printf("Total recorded: %d, Buffer size: %d\n", totalCount, bufferSize)
		return
	}
	
	// Print time range information before output
	if len(typedSnapshots) > 0 {
		timeSpan := latestTime.Sub(earliestTime)
		fmt.Fprintf(os.Stderr, "Time Range: %s to %s (Duration: %v)\n", 
			earliestTime.Format(time.RFC3339), 
			latestTime.Format(time.RFC3339),
			timeSpan.Round(time.Second))
		if len(typedSnapshots) > 1 {
			avgInterval := timeSpan / time.Duration(len(typedSnapshots)-1)
			fmt.Fprintf(os.Stderr, "Average interval: %v (%d snapshots)\n", 
				avgInterval.Round(time.Second), len(typedSnapshots))
		}
	}

	// Apply limit if specified
	if *limit > 0 && *limit < len(snapshots) {
		snapshots = snapshots[:*limit]
	}

	// Determine output destination
	var outputWriter *os.File = os.Stdout
	if *output != "" {
		var err error
		outputWriter, err = os.Create(*output)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outputWriter.Close()
	}

	// Output based on format
	encodeStartTime := time.Now()
	var encodeDuration time.Duration
	switch *format {
	case "json":
		if *stream {
			// Stream JSON output: one snapshot per line (JSONL format)
			encoder := json.NewEncoder(outputWriter)
			for _, snapshot := range snapshots {
				if err := encoder.Encode(snapshot); err != nil {
					log.Fatalf("Failed to encode snapshot: %v", err)
				}
			}
			encodeDuration = time.Since(encodeStartTime)
			if *verbose {
				log.Printf("Performance: Streamed %d snapshots in %v (%.2f snapshots/sec)",
					len(snapshots), encodeDuration, float64(len(snapshots))/encodeDuration.Seconds())
			}
		} else {
			// Use MarshalIndent for better control and ensure complete output
			data, err := json.MarshalIndent(snapshots, "", "  ")
			if err != nil {
				log.Fatalf("Failed to encode JSON: %v", err)
			}
			encodeDuration = time.Since(encodeStartTime)

			// Write directly to output and ensure it's flushed
			writeStartTime := time.Now()
			if _, err := outputWriter.Write(data); err != nil {
				log.Fatalf("Failed to write JSON: %v", err)
			}
			// Add newline at the end
			fmt.Fprintln(outputWriter)
			writeDuration := time.Since(writeStartTime)

			if *verbose {
				fileInfo, _ := os.Stat(*output)
				var fileSize int64
				if fileInfo != nil {
					fileSize = fileInfo.Size()
				}
				log.Printf("Performance: Encoded %d snapshots in %v (%.2f snapshots/sec)",
					len(snapshots), encodeDuration, float64(len(snapshots))/encodeDuration.Seconds())
				log.Printf("Performance: Wrote %d bytes in %v (%.2f MB/s)",
					fileSize, writeDuration, float64(fileSize)/(1024*1024)/writeDuration.Seconds())
			}
		}
		// Ensure output is flushed
		if err := outputWriter.Sync(); err != nil {
			log.Printf("Warning: failed to sync output: %v", err)
		}
	case "pretty":
		fmt.Fprintf(outputWriter, "Flight Recorder Contents (%d snapshots):\n\n", len(snapshots))
		for i, snapshot := range snapshots {
			// Convert to JSON for pretty printing
			data, err := json.MarshalIndent(snapshot, "", "  ")
			if err != nil {
				fmt.Fprintf(outputWriter, "Snapshot %d: [Error encoding: %v]\n", i+1, err)
				continue
			}
			fmt.Fprintf(outputWriter, "=== Snapshot %d/%d ===\n", i+1, len(snapshots))
			fmt.Fprintln(outputWriter, string(data))
			fmt.Fprintln(outputWriter)
		}
		// Ensure output is flushed
		if err := outputWriter.Sync(); err != nil {
			log.Printf("Warning: failed to sync output: %v", err)
		}
	default:
		log.Fatalf("Unknown format: %s (use: pretty, json, or stats)", *format)
	}

	// Print timing information
	if *verbose {
		totalDuration := time.Since(startTime)
		log.Printf("Performance Summary:")
		log.Printf("  Total snapshots: %d", len(snapshots))
		log.Printf("  Read from flight recorder: %v (%.2f snapshots/sec)",
			readDuration, float64(len(snapshots))/readDuration.Seconds())
		if *format == "json" {
			log.Printf("  JSON encoding: %v (%.2f snapshots/sec)",
				encodeDuration, float64(len(snapshots))/encodeDuration.Seconds())
		}
		log.Printf("  Total time: %v", totalDuration)
		log.Printf("  Overall throughput: %.2f snapshots/sec", float64(len(snapshots))/totalDuration.Seconds())
	}
}
