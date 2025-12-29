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

package integration_test

import (
	"runtime"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

// getConstantRawMetrics returns a constant set of rawMetrics for testing.
func getConstantRawMetrics() []metrics.RawMetric {
	return []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Desc:   "CPU usage percentage",
			Labels: []metrics.Label{{Name: "host", Value: "server1"}},
			Value:  75.5,
		},
		{
			Name:   "memory_usage",
			Desc:   "Memory usage in bytes",
			Labels: []metrics.Label{{Name: "host", Value: "server1"}},
			Value:  1024 * 1024 * 512,
		},
		{
			Name: "http_requests_total",
			Desc: "Total HTTP requests",
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
				{Name: "status", Value: "200"},
			},
			Value: 1234.0,
		},
		{
			Name: "http_request_duration_seconds",
			Desc: "HTTP request duration in seconds",
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
				{Name: "endpoint", Value: "/api/v1/health"},
			},
			Value: 0.123,
		},
		{
			Name:   "disk_usage",
			Desc:   "Disk usage percentage",
			Labels: []metrics.Label{{Name: "device", Value: "/dev/sda1"}},
			Value:  45.2,
		},
		{
			Name:   "network_bytes_sent",
			Desc:   "Network bytes sent",
			Labels: []metrics.Label{{Name: "interface", Value: "eth0"}},
			Value:  1024 * 1024 * 100,
		},
		{
			Name:   "network_bytes_received",
			Desc:   "Network bytes received",
			Labels: []metrics.Label{{Name: "interface", Value: "eth0"}},
			Value:  1024 * 1024 * 200,
		},
		{
			Name:   "active_connections",
			Desc:   "Number of active connections",
			Labels: []metrics.Label{{Name: "protocol", Value: "tcp"}},
			Value:  42.0,
		},
		{
			Name:   "go_memstats_heap_alloc_bytes",
			Desc:   "Bytes allocated on the heap",
			Labels: []metrics.Label{},
			Value:  1024 * 1024 * 50,
		},
		{
			Name:   "go_memstats_heap_inuse_bytes",
			Desc:   "Bytes in use on the heap",
			Labels: []metrics.Label{},
			Value:  1024 * 1024 * 48,
		},
		{
			Name:   "cpu_usage",
			Desc:   "CPU usage percentage",
			Labels: []metrics.Label{{Name: "host", Value: "server2"}},
			Value:  82.3,
		},
		{
			Name:   "cpu_usage",
			Desc:   "CPU usage percentage",
			Labels: []metrics.Label{{Name: "host", Value: "server3"}, {Name: "core", Value: "0"}},
			Value:  65.7,
		},
		{
			Name:   "memory_usage",
			Desc:   "Memory usage in bytes",
			Labels: []metrics.Label{{Name: "host", Value: "server2"}},
			Value:  1024 * 1024 * 256,
		},
		{
			Name:   "memory_usage",
			Desc:   "Memory usage in bytes",
			Labels: []metrics.Label{{Name: "host", Value: "server3"}},
			Value:  1024 * 1024 * 768,
		},
		{
			Name: "http_requests_total",
			Desc: "Total HTTP requests",
			Labels: []metrics.Label{
				{Name: "method", Value: "POST"},
				{Name: "status", Value: "201"},
			},
			Value: 567.0,
		},
		{
			Name: "http_requests_total",
			Desc: "Total HTTP requests",
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
				{Name: "status", Value: "404"},
			},
			Value: 89.0,
		},
		{
			Name: "http_requests_total",
			Desc: "Total HTTP requests",
			Labels: []metrics.Label{
				{Name: "method", Value: "PUT"},
				{Name: "status", Value: "200"},
			},
			Value: 234.0,
		},
		{
			Name: "http_request_duration_seconds",
			Desc: "HTTP request duration in seconds",
			Labels: []metrics.Label{
				{Name: "method", Value: "POST"},
				{Name: "endpoint", Value: "/api/v1/users"},
			},
			Value: 0.456,
		},
		{
			Name: "http_request_duration_seconds",
			Desc: "HTTP request duration in seconds",
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
				{Name: "endpoint", Value: "/api/v1/metrics"},
			},
			Value: 0.089,
		},
		{
			Name:   "disk_usage",
			Desc:   "Disk usage percentage",
			Labels: []metrics.Label{{Name: "device", Value: "/dev/sda2"}},
			Value:  67.8,
		},
		{
			Name:   "disk_usage",
			Desc:   "Disk usage percentage",
			Labels: []metrics.Label{{Name: "device", Value: "/dev/sdb1"}},
			Value:  23.4,
		},
		{
			Name:   "disk_io_read_bytes",
			Desc:   "Disk I/O read bytes",
			Labels: []metrics.Label{{Name: "device", Value: "/dev/sda1"}},
			Value:  1024 * 1024 * 500, // 500MB
		},
		{
			Name:   "disk_io_write_bytes",
			Desc:   "Disk I/O write bytes",
			Labels: []metrics.Label{{Name: "device", Value: "/dev/sda1"}},
			Value:  1024 * 1024 * 300, // 300MB
		},
		{
			Name:   "network_bytes_sent",
			Desc:   "Network bytes sent",
			Labels: []metrics.Label{{Name: "interface", Value: "eth1"}},
			Value:  1024 * 1024 * 150, // 150MB
		},
		{
			Name:   "network_bytes_received",
			Desc:   "Network bytes received",
			Labels: []metrics.Label{{Name: "interface", Value: "eth1"}},
			Value:  1024 * 1024 * 250, // 250MB
		},
		{
			Name:   "network_packets_sent",
			Desc:   "Network packets sent",
			Labels: []metrics.Label{{Name: "interface", Value: "eth0"}},
			Value:  1234567.0,
		},
		{
			Name:   "network_packets_received",
			Desc:   "Network packets received",
			Labels: []metrics.Label{{Name: "interface", Value: "eth0"}},
			Value:  2345678.0,
		},
		{
			Name:   "active_connections",
			Desc:   "Number of active connections",
			Labels: []metrics.Label{{Name: "protocol", Value: "udp"}},
			Value:  15.0,
		},
		{
			Name:   "active_connections",
			Desc:   "Number of active connections",
			Labels: []metrics.Label{{Name: "protocol", Value: "http"}},
			Value:  128.0,
		},
		{
			Name:   "go_memstats_heap_alloc_bytes",
			Desc:   "Bytes allocated on the heap",
			Labels: []metrics.Label{{Name: "service", Value: "api"}},
			Value:  1024 * 1024 * 75, // 75MB
		},
		{
			Name:   "go_memstats_heap_sys_bytes",
			Desc:   "Bytes obtained from system for heap",
			Labels: []metrics.Label{},
			Value:  1024 * 1024 * 100, // 100MB
		},
		{
			Name:   "go_memstats_gc_duration_seconds",
			Desc:   "GC duration in seconds",
			Labels: []metrics.Label{},
			Value:  0.012,
		},
		{
			Name:   "go_memstats_num_gc",
			Desc:   "Number of GC cycles",
			Labels: []metrics.Label{},
			Value:  42.0,
		},
		{
			Name:   "database_connections_active",
			Desc:   "Active database connections",
			Labels: []metrics.Label{{Name: "database", Value: "postgres"}},
			Value:  25.0,
		},
		{
			Name:   "database_connections_idle",
			Desc:   "Idle database connections",
			Labels: []metrics.Label{{Name: "database", Value: "postgres"}},
			Value:  5.0,
		},
		{
			Name:   "database_query_duration_seconds",
			Desc:   "Database query duration",
			Labels: []metrics.Label{{Name: "query", Value: "SELECT"}},
			Value:  0.045,
		},
		{
			Name:   "database_query_duration_seconds",
			Desc:   "Database query duration",
			Labels: []metrics.Label{{Name: "query", Value: "INSERT"}},
			Value:  0.123,
		},
		{
			Name:   "cache_hits_total",
			Desc:   "Total cache hits",
			Labels: []metrics.Label{{Name: "cache", Value: "redis"}},
			Value:  98765.0,
		},
		{
			Name:   "cache_misses_total",
			Desc:   "Total cache misses",
			Labels: []metrics.Label{{Name: "cache", Value: "redis"}},
			Value:  1234.0,
		},
		{
			Name:   "cache_size_bytes",
			Desc:   "Cache size in bytes",
			Labels: []metrics.Label{{Name: "cache", Value: "redis"}},
			Value:  1024 * 1024 * 64, // 64MB
		},
		{
			Name:   "queue_length",
			Desc:   "Queue length",
			Labels: []metrics.Label{{Name: "queue", Value: "tasks"}},
			Value:  156.0,
		},
		{
			Name:   "queue_length",
			Desc:   "Queue length",
			Labels: []metrics.Label{{Name: "queue", Value: "events"}},
			Value:  89.0,
		},
		{
			Name:   "queue_processing_duration_seconds",
			Desc:   "Queue processing duration",
			Labels: []metrics.Label{{Name: "queue", Value: "tasks"}},
			Value:  0.234,
		},
		{
			Name:   "application_errors_total",
			Desc:   "Total application errors",
			Labels: []metrics.Label{{Name: "type", Value: "validation"}},
			Value:  12.0,
		},
		{
			Name:   "application_errors_total",
			Desc:   "Total application errors",
			Labels: []metrics.Label{{Name: "type", Value: "timeout"}},
			Value:  3.0,
		},
		{
			Name:   "application_requests_in_flight",
			Desc:   "Requests currently being processed",
			Labels: []metrics.Label{{Name: "service", Value: "api"}},
			Value:  45.0,
		},
		{
			Name:   "application_requests_total",
			Desc:   "Total application requests",
			Labels: []metrics.Label{{Name: "service", Value: "api"}},
			Value:  123456.0,
		},
		{
			Name:   "file_descriptors_open",
			Desc:   "Number of open file descriptors",
			Labels: []metrics.Label{{Name: "process", Value: "main"}},
			Value:  234.0,
		},
		{
			Name:   "goroutines_total",
			Desc:   "Total number of goroutines",
			Labels: []metrics.Label{{Name: "service", Value: "api"}},
			Value:  128.0,
		},
		{
			Name:   "threads_total",
			Desc:   "Total number of threads",
			Labels: []metrics.Label{},
			Value:  16.0,
		},
		{
			Name:   "process_cpu_seconds_total",
			Desc:   "Total CPU time consumed",
			Labels: []metrics.Label{{Name: "mode", Value: "user"}},
			Value:  1234.567,
		},
		{
			Name:   "process_cpu_seconds_total",
			Desc:   "Total CPU time consumed",
			Labels: []metrics.Label{{Name: "mode", Value: "system"}},
			Value:  234.567,
		},
		{
			Name:   "process_resident_memory_bytes",
			Desc:   "Resident memory size",
			Labels: []metrics.Label{},
			Value:  1024 * 1024 * 512, // 512MB
		},
		{
			Name:   "process_virtual_memory_bytes",
			Desc:   "Virtual memory size",
			Labels: []metrics.Label{},
			Value:  1024 * 1024 * 1024, // 1GB
		},
		{
			Name:   "system_load_average",
			Desc:   "System load average",
			Labels: []metrics.Label{{Name: "period", Value: "1m"}},
			Value:  1.23,
		},
		{
			Name:   "system_load_average",
			Desc:   "System load average",
			Labels: []metrics.Label{{Name: "period", Value: "5m"}},
			Value:  1.45,
		},
		{
			Name:   "system_load_average",
			Desc:   "System load average",
			Labels: []metrics.Label{{Name: "period", Value: "15m"}},
			Value:  1.12,
		},
	}
}

var _ = Describe("Test Case 4: Capacity Size and Heap Inuse Size", func() {
	var fr *flightrecorder.FlightRecorder

	It("should track HeapInuseSize relative to capacitySize", func() {
		// Step 1: Create FlightRecorder with a specific capacity size
		capacitySize := int64(100 * 1024) // 100KB
		fr = flightrecorder.NewFlightRecorder(capacitySize)

		// Verify initial capacity size
		initialCapacitySize := fr.GetCapacitySize()
		Expect(initialCapacitySize).To(Equal(capacitySize), "Initial capacity size should match configured value")

		// Step 2: Measure baseline heap inuse size (before any data)
		// Force GC multiple times and wait for heap size to stabilize
		var prevHeapSize int64
		Eventually(func() bool {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			currentHeapSize := fr.HeapInuseSize()

			if prevHeapSize > 0 {
				diff := currentHeapSize - prevHeapSize
				if diff < 0 {
					diff = -diff
				}
				if diff < 1024 {
					return true
				}
			}
			prevHeapSize = currentHeapSize
			return false
		}, 5*time.Second, 200*time.Millisecond).Should(BeTrue())
		baselineHeapInuseSize := fr.HeapInuseSize()
		Expect(baselineHeapInuseSize).To(BeNumerically(">", 0), "Baseline heap inuse size should be positive")

		GinkgoWriter.Printf("Baseline heap inuse size: %d bytes, Capacity size: %d bytes\n",
			baselineHeapInuseSize, capacitySize)

		// Step 3: Get constant rawMetrics
		rawMetrics := getConstantRawMetrics()
		Expect(rawMetrics).NotTo(BeEmpty(), "RawMetrics should not be empty")

		// Step 4: Update FlightRecorder with metrics multiple times to simulate polling cycles
		numUpdates := 10
		for i := 0; i < numUpdates; i++ {
			updateErr := fr.Update(rawMetrics)
			Expect(updateErr).NotTo(HaveOccurred(), "Update should succeed")
		}

		// Step 5: Verify datasource has metrics
		datasources := fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty(), "FlightRecorder should have at least one datasource")

		ds := datasources[0]
		metricsMap := ds.GetMetrics()
		Expect(metricsMap).NotTo(BeEmpty(), "Datasource should have buffered metrics")

		// Step 6: Measure heap inuse size after data collection
		heapInuseSizeAfterData := fr.HeapInuseSize()
		Expect(heapInuseSizeAfterData).To(BeNumerically(">", 0), "Heap inuse size after data should be positive")

		// Calculate actual memory used by FlightRecorder data
		actualMemoryUsed := heapInuseSizeAfterData - baselineHeapInuseSize

		GinkgoWriter.Printf("Heap inuse size after data: %d bytes, Memory used: %d bytes, Capacity size: %d bytes\n",
			heapInuseSizeAfterData, actualMemoryUsed, capacitySize)

		// Step 7: Verify that memory usage is within reasonable bounds relative to capacity
		// Allow some tolerance for overhead (maps, mutexes, etc.) - should be within 50% of capacity
		Expect(actualMemoryUsed).To(BeNumerically(">", 0), "Actual memory consumption should be positive")
		Expect(actualMemoryUsed).To(BeNumerically("<=", capacitySize*150/100),
			"Actual memory consumption should not exceed capacitySize by more than 50%")

		// Step 8: Verify capacity size is still correct
		currentCapacitySize := fr.GetCapacitySize()
		Expect(currentCapacitySize).To(Equal(capacitySize), "Capacity size should remain unchanged")

		// Step 9: Verify that if data exists, memory usage is reasonable
		if len(metricsMap) > 0 {
			// Memory usage should be at least 10% of capacity if data exists
			// This ensures we're actually using memory for data storage
			Expect(actualMemoryUsed).To(BeNumerically(">=", capacitySize*10/100),
				"Actual memory consumption should use at least 10%% of capacitySize when data exists")
		}
	})
})
