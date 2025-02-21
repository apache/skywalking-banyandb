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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
)

func collectMetrics(path string, closeCh <-chan struct{}) {
	file, err := os.OpenFile(filepath.Join(rootPath, metricsFile), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if err = file.Truncate(0); err != nil {
		panic(err)
	}
	for {
		metrics, err := scrape()
		if err != nil {
			fmt.Println("Error scraping metrics:", err)
			select {
			case <-closeCh:
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		usage, err := disk.Usage(path)
		if err != nil {
			fmt.Println("Error getting disk usage:", err)
		} else {
			metrics[diskUsed] = float64(usage.Used)
			metrics[diskTotal] = float64(usage.Total)
		}
		ioMetrics, err := disk.IOCounters("sda")
		if err != nil {
			fmt.Println("Error getting disk IO:", err)
		} else {
			metrics[readCount] = float64(ioMetrics["sda"].ReadCount)
			metrics[mergedReadCount] = float64(ioMetrics["sda"].MergedReadCount)
			metrics[writeCount] = float64(ioMetrics["sda"].WriteCount)
			metrics[mergedWriteCount] = float64(ioMetrics["sda"].MergedWriteCount)
			metrics[readBytes] = float64(ioMetrics["sda"].ReadBytes)
			metrics[writeBytes] = float64(ioMetrics["sda"].WriteBytes)
			metrics[ioTime] = float64(ioMetrics["sda"].IoTime)
			metrics[weightedIO] = float64(ioMetrics["sda"].WeightedIO)
		}

		select {
		case <-closeCh:
			return
		default:
		}

		err = writeMetrics(file, metricsToCollect, metrics)
		if err != nil {
			fmt.Println("Error writing metrics:", err)
		}
		select {
		case <-closeCh:
			return
		case <-time.After(15 * time.Second):
		}
	}
}

func scrape() (map[string]float64, error) {
	resp, err := http.Get("http://localhost:2121/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseMetrics(string(body)), nil
}

func writeMetrics(file *os.File, header []string, metrics map[string]float64) error {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := make([]string, len(header))
	for i, h := range header {
		record[i] = fmt.Sprintf("%f", metrics[h])
	}

	return writer.Write(record)
}

func parseMetrics(metrics string) map[string]float64 {
	lines := strings.Split(metrics, "\n")
	r := make(map[string]float64)
	for _, line := range lines {
		if strings.Contains(line, "#") {
			continue
		}
		n, v := parseMetricValue(line)
		if n == "" {
			continue
		}
		r[n] = v
	}
	return r
}

func parseMetricValue(line string) (string, float64) {
	parts := strings.Split(line, " ")
	if !slices.Contains[[]string](metricsToCollect, parts[0]) {
		return "", 0
	}
	value, _ := strconv.ParseFloat(parts[1], 64)
	return parts[0], value
}
