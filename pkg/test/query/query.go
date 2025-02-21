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

// Package query provides functions for analyzing and collecting metrics.
package query

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	metricsFile = "data.csv"
	resultFile  = "result.csv"
)

func init() {
	_ = logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	})
}

func analyze(metricNames []string, rootPath string) {
	file, err := os.Open(filepath.Join(rootPath, metricsFile))
	if err != nil {
		fmt.Println("Error opening metrics file:", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading metrics file:", err)
		return
	}
	if reader == nil {
		fmt.Println("No records found in metrics file.")
		return
	}

	// Transpose the records to handle column-based data.
	transposed := transpose(records)

	// Open the results file.
	resultsFile, err := os.OpenFile(filepath.Join(rootPath, resultFile), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Println("Error opening results file:", err)
		return
	}
	defer resultsFile.Close()
	if err = resultsFile.Truncate(0); err != nil {
		fmt.Println("Error truncating results file:", err)
		return
	}
	// Write the header to the results file.
	writeHeader(resultsFile)

	for i, record := range transposed {
		// Convert the records to a slice of floats for analysis.
		data := make([]float64, 0, len(record))
		for _, r := range record {
			d := atof(r) // Convert the string to a float.
			if d < 0 {
				continue
			}
			data = append(data, d)
		}

		// Calculate the statistics.
		minVal, _ := stats.Min(data)
		maxVal, _ := stats.Max(data)
		mean, _ := stats.Mean(data)
		median, _ := stats.Median(data)
		p95, _ := stats.Percentile(data, 95)

		// Write the results to another file and print them to the console.
		writeResults(resultsFile, metricNames[i], minVal, maxVal, mean, median, p95)
	}
}

func writeHeader(file *os.File) {
	header := "Metric Name, Min, Max, Mean, Median, P95\n"
	_, err := file.WriteString(header)
	if err != nil {
		fmt.Println("Error writing to results file:", err)
		return
	}
}

func writeResults(file *os.File, metricName string, minVal, maxVal, mean, median, p95 float64) {
	results := fmt.Sprintf("%s, %f, %f, %f, %f, %f\n",
		metricName, minVal, maxVal, mean, median, p95)

	_, err := file.WriteString(results)
	if err != nil {
		fmt.Println("Error writing to results file:", err)
		return
	}

	fmt.Print(results)
}

func transpose(slice [][]string) [][]string {
	xl := len(slice[0])
	yl := len(slice)
	result := make([][]string, xl)
	for i := range result {
		result[i] = make([]string, yl)
	}
	for i, row := range slice {
		for j, col := range row {
			result[j][i] = col
		}
	}
	return result
}

func atof(s string) float64 {
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(err)
	}
	return value
}

type scrape func() ([]float64, error)

func collect(rootPath string, scrape scrape, interval time.Duration, closeCh <-chan struct{}) {
	file, err := os.OpenFile(filepath.Join(rootPath, metricsFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if err = file.Truncate(0); err != nil {
		panic(err)
	}
	lastCollectTime := time.Now()
	for {
		metrics, err := scrape()
		if err != nil {
			logger.Errorf("Error scraping metrics: %v", err)
			select {
			case <-closeCh:
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		select {
		case <-closeCh:
			return
		default:
		}
		if time.Since(lastCollectTime) < time.Minute {
			select {
			case <-closeCh:
				return
			case <-time.After(interval):
			}
			continue
		}
		lastCollectTime = time.Now()

		err = writeMetrics(file, metrics)
		if err != nil {
			fmt.Println("Error writing metrics:", err)
		}
		select {
		case <-closeCh:
			return
		case <-time.After(interval):
		}
	}
}

func writeMetrics(file *os.File, metrics []float64) error {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := make([]string, len(metrics))
	for i := range metrics {
		record[i] = fmt.Sprintf("%f", metrics[i])
	}

	return writer.Write(record)
}
