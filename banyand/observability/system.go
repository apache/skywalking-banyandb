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

package observability

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var systemInfoInstance *SystemInfo

func init() {
	systemInfoInstance = &SystemInfo{
		Addresses:  make(map[string]string),
		DiskUsages: make(map[string]*DiskUsage),
	}
}

// UpdateAddress updates the address of the given name.
func UpdateAddress(name, address string) {
	systemInfoInstance.Lock()
	defer systemInfoInstance.Unlock()
	systemInfoInstance.Addresses[name] = address
}

func getAddresses() map[string]string {
	systemInfoInstance.RLock()
	defer systemInfoInstance.RUnlock()
	return systemInfoInstance.Addresses
}

// UpdatePath updates a path to monitoring its disk usage.
func UpdatePath(path string) {
	systemInfoInstance.Lock()
	defer systemInfoInstance.Unlock()
	systemInfoInstance.DiskUsages[path] = nil
}

func getPath() map[string]*DiskUsage {
	systemInfoInstance.RLock()
	defer systemInfoInstance.RUnlock()
	return systemInfoInstance.DiskUsages
}

// SystemInfo represents the system information of a node.
type SystemInfo struct {
	Addresses   map[string]string     `json:"addresses"`
	DiskUsages  map[string]*DiskUsage `json:"disk_usages"`
	NodeID      string                `json:"node_id"`
	Hostname    string                `json:"hostname"`
	Roles       []string              `json:"roles"`
	Uptime      uint64                `json:"uptime"`
	CPUUsage    float64               `json:"cpu_usage"`
	MemoryUsage float64               `json:"memory_usage"`
	sync.RWMutex
}

// DiskUsage represents the disk usage for a given path.
type DiskUsage struct {
	Capacity uint64 `json:"capacity"`
	Used     uint64 `json:"used"`
}

// ErrorResponse represents the error response.
type ErrorResponse struct {
	Message       string `json:"message"`
	OriginalError string `json:"original_error,omitempty"`
}

func init() {
	mux.HandleFunc("/system", systemInfoHandler)
}

func systemInfoHandler(w http.ResponseWriter, _ *http.Request) {
	hostname, _ := os.Hostname()
	uptime, _ := getUptime()
	cpuUsage, _ := getCPUUsage()
	memoryUsage, _ := getMemoryUsage()

	systemInfo := &SystemInfo{
		NodeID:      "1",
		Roles:       []string{"meta", "ingest", "query", "data"},
		Hostname:    hostname,
		Uptime:      uptime,
		CPUUsage:    cpuUsage,
		MemoryUsage: memoryUsage,
		Addresses:   getAddresses(),
		DiskUsages:  make(map[string]*DiskUsage),
	}
	for k := range getPath() {
		usage, _ := getDiskUsage(k)
		systemInfo.DiskUsages[k] = &usage
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode([]*SystemInfo{systemInfo}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		errorResponse := &ErrorResponse{
			Message:       "Error encoding JSON response",
			OriginalError: err.Error(),
		}
		if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
			logger.GetLogger().Error().Err(err).Msg("Error encoding JSON response")
		}
	}
}

func getUptime() (uint64, error) {
	uptime, err := host.Uptime()
	if err != nil {
		return 0, err
	}
	return uptime, nil
}

func getCPUUsage() (float64, error) {
	percentages, err := cpu.Percent(0, false)
	if err != nil {
		return 0, err
	}
	return percentages[0], nil
}

func getMemoryUsage() (float64, error) {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return vm.UsedPercent, nil
}

func getDiskUsage(path string) (DiskUsage, error) {
	usage, err := disk.Usage(path)
	if err != nil {
		return DiskUsage{}, err
	}
	return DiskUsage{Capacity: usage.Total, Used: usage.Used}, nil
}
