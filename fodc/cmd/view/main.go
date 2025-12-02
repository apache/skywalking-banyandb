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
		output  = flag.String("output", "", "Output file path (default: stdout)")
		stream  = flag.Bool("stream", false, "Stream JSON output (one snapshot per line) for large datasets")
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
		} else {
			// Use MarshalIndent for better control and ensure complete output
			data, err := json.MarshalIndent(snapshots, "", "  ")
			if err != nil {
				log.Fatalf("Failed to encode JSON: %v", err)
			}
			// Write directly to output and ensure it's flushed
			if _, err := outputWriter.Write(data); err != nil {
				log.Fatalf("Failed to write JSON: %v", err)
			}
			// Add newline at the end
			fmt.Fprintln(outputWriter)
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
}
