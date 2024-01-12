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

// Package metric provides functions for collecting and analyzing metrics for stress testing.
package metric

import (
	"os"
	"time"
)

const (
	rootPath         = "testdata/metrics"
	metricsFile      = "data.csv"
	resultFile       = "result.csv"
	intermediateFile = "intermediate.csv"

	// disk usage metrics.
	diskUsed  = "disk_used"
	diskTotal = "disk_total"

	// io metrics.
	readCount        = "read_count"
	mergedReadCount  = "merged_read_count"
	writeCount       = "write_count"
	mergedWriteCount = "merged_write_count"
	readBytes        = "read_bytes"
	writeBytes       = "write_bytes"
	ioTime           = "io_time"
	weightedIO       = "weighted_io"
)

var (
	metricsToCollect []string
	rateMetrics      map[string]struct{}
)

var (
	memStats  = []string{"go_memstats_alloc_bytes", "go_memstats_heap_inuse_bytes", "go_memstats_sys_bytes", "go_memstats_stack_inuse_bytes"}
	cpuStats  = []string{"process_cpu_seconds_total"}
	diskStats = []string{diskUsed, diskTotal}
	ioStats   = []string{readCount, mergedReadCount, writeCount, mergedWriteCount, readBytes, writeBytes, ioTime, weightedIO}
)

func init() {
	if err := os.MkdirAll(rootPath, 0o755); err != nil {
		panic(err)
	}
	metricsToCollect = append(metricsToCollect, memStats...)
	metricsToCollect = append(metricsToCollect, cpuStats...)
	metricsToCollect = append(metricsToCollect, diskStats...)
	metricsToCollect = append(metricsToCollect, ioStats...)
	rateMetrics = make(map[string]struct{})
	for _, v := range cpuStats {
		rateMetrics[v] = struct{}{}
	}
	for _, v := range ioStats {
		rateMetrics[v] = struct{}{}
	}
}

// Start starts the metric collection.
func Start(path string) chan struct{} {
	closeCh := make(chan struct{})
	go func() {
		select {
		case <-closeCh:
			return
		case <-time.After(10 * time.Second):
		}
		defer func() {
			analyzeMetrics()
		}()
		collectMetrics(path, closeCh)
	}()
	return closeCh
}
