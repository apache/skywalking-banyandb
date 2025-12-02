package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
)

func main() {
	var (
		path    = flag.String("path", "/tmp/fodc-flight-recorder.bin", "Path to flight recorder file")
		format  = flag.String("format", "pretty", "Output format: pretty, json, or stats")
		limit   = flag.Int("limit", 0, "Limit number of snapshots to display (0 = all)")
		recent  = flag.Bool("recent", false, "Show only recent snapshots (last 10)")
		recentN = flag.Uint("recent-n", 10, "Number of recent snapshots to show when using --recent")
	)
	flag.Parse()

	// Open flight recorder
	fr, err := flightrecorder.NewFlightRecorder(*path, 0)
	if err != nil {
		log.Fatalf("Failed to open flight recorder file: %v", err)
	}
	defer fr.Close()

	// Get stats
	totalCount, bufferSize, writeIndex := fr.GetStats()

	// If only stats requested, show and exit
	if *format == "stats" {
		fmt.Printf("Flight Recorder Statistics:\n")
		fmt.Printf("  Path: %s\n", *path)
		fmt.Printf("  Total snapshots recorded: %d\n", totalCount)
		fmt.Printf("  Buffer size: %d\n", bufferSize)
		fmt.Printf("  Current write index: %d\n", writeIndex)
		return
	}

	// Read snapshots
	var allSnapshots []interface{}
	if *recent {
		recentSnapshots, err := fr.ReadRecent(uint32(*recentN))
		if err != nil {
			log.Fatalf("Failed to read recent snapshots: %v", err)
		}
		for _, s := range recentSnapshots {
			allSnapshots = append(allSnapshots, s)
		}
	} else {
		snapshots, err := fr.ReadAll()
		if err != nil {
			log.Fatalf("Failed to read snapshots: %v", err)
		}
		for _, s := range snapshots {
			allSnapshots = append(allSnapshots, s)
		}
	}
	snapshots := allSnapshots

	if len(snapshots) == 0 {
		fmt.Printf("No snapshots found in flight recorder file: %s\n", *path)
		fmt.Printf("Total recorded: %d, Buffer size: %d\n", totalCount, bufferSize)
		return
	}

	// Apply limit if specified
	if *limit > 0 && *limit < len(snapshots) {
		snapshots = snapshots[:*limit]
	}

	// Output based on format
	switch *format {
	case "json":
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(snapshots); err != nil {
			log.Fatalf("Failed to encode JSON: %v", err)
		}
	case "pretty":
		fmt.Printf("Flight Recorder Contents (%d snapshots):\n\n", len(snapshots))
		for i, snapshot := range snapshots {
			// Convert to JSON for pretty printing
			data, err := json.MarshalIndent(snapshot, "", "  ")
			if err != nil {
				fmt.Printf("Snapshot %d: [Error encoding: %v]\n", i+1, err)
				continue
			}
			fmt.Printf("=== Snapshot %d/%d ===\n", i+1, len(snapshots))
			fmt.Println(string(data))
			fmt.Println()
		}
	default:
		log.Fatalf("Unknown format: %s (use: pretty, json, or stats)", *format)
	}
}
