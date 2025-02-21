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

package metric

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
)

func analyzeMetrics() {
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

	// Open the intermediate file.
	intermediateFile, err := os.OpenFile(filepath.Join(rootPath, intermediateFile), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Println("Error opening intermediate file:", err)
		return
	}
	defer intermediateFile.Close()
	if err = intermediateFile.Truncate(0); err != nil {
		fmt.Println("Error truncating intermediate file:", err)
		return
	}

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
		data := make([]float64, len(record))
		for j, r := range record {
			data[j] = atof(r) // Convert the string to a float.
		}
		if _, ok := rateMetrics[metricsToCollect[i]]; ok {
			// Calculate the rate of the metric.
			data, err = rate(data, 5*time.Minute)
			if err != nil {
				fmt.Println("Error calculating rate:", err)
				return
			}
			// Write the rates to the intermediate file.
			intermediateWriter := csv.NewWriter(intermediateFile)
			defer intermediateWriter.Flush()
			row := []string{metricsToCollect[i], "rate[5m]"}
			for _, r := range data {
				row = append(row, strconv.FormatFloat(r, 'f', -1, 64))
			}
			if err = intermediateWriter.Write(row); err != nil {
				fmt.Println("Error writing to intermediate file:", err)
				return
			}
		}

		// Calculate the statistics.
		minVal, _ := stats.Min(data)
		maxVal, _ := stats.Max(data)
		mean, _ := stats.Mean(data)
		median, _ := stats.Median(data)
		p90, _ := stats.Percentile(data, 90)
		p95, _ := stats.Percentile(data, 95)
		p98, _ := stats.Percentile(data, 98)
		p99, _ := stats.Percentile(data, 99)

		// Write the results to another file and print them to the console.
		writeResults(resultsFile, metricsToCollect[i], minVal, maxVal, mean, median, p90, p95, p98, p99)
	}
}

func writeHeader(file *os.File) {
	header := "Metric Name, Min, Max, Mean, Median, P90, P95, P98, P99\n"
	_, err := file.WriteString(header)
	if err != nil {
		fmt.Println("Error writing to results file:", err)
		return
	}
}

func writeResults(file *os.File, metricName string, minVal, maxVal, mean, median, p90, p95, p98, p99 float64) {
	results := fmt.Sprintf("%s, %f, %f, %f, %f, %f, %f, %f, %f\n",
		metricName, minVal, maxVal, mean, median, p90, p95, p98, p99)

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

const interval = 15 * time.Second

func rate(metricData []float64, timeWindow time.Duration) ([]float64, error) {
	pointsInWindow := int(timeWindow.Seconds() / interval.Seconds())

	if len(metricData) < pointsInWindow {
		return nil, fmt.Errorf("not enough data points to calculate rate")
	}

	rates := make([]float64, len(metricData)-pointsInWindow+1)
	for i := 0; i <= len(metricData)-pointsInWindow; i++ {
		start := metricData[i]
		end := metricData[i+pointsInWindow-1]
		rates[i] = (end - start) / float64(pointsInWindow*int(interval.Seconds()))
	}

	return rates, nil
}
