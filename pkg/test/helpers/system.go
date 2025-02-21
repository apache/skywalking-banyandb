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

package helpers

import (
	"fmt"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
)

// CPUInfo returns the CPU information of the current machine.
func CPUInfo() string {
	info, err := cpu.Info()
	if err != nil {
		return fmt.Sprintf("failed to get CPU info: %v", err)
	}

	var result string
	for _, cpuInfo := range info {
		result += fmt.Sprintf("CPU: %v, VendorID: %v, Family: %v, Model: %v, Cores: %v\n",
			cpuInfo.ModelName, cpuInfo.VendorID, cpuInfo.Family, cpuInfo.Model, cpuInfo.Cores)
	}
	return result
}

// MemoryInfo returns the memory information of the current machine.
func MemoryInfo() string {
	v, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Sprintf("failed to get memory info: %v", err)
	}

	// convert to MB
	total := v.Total / 1024 / 1024
	available := v.Available / 1024 / 1024
	used := v.Used / 1024 / 1024

	result := fmt.Sprintf("Total: %v MB, Available: %v MB, Used: %v MB\n", total, available, used)
	return result
}

// OSInfo returns the OS information of the current machine.
func OSInfo() string {
	info, err := host.Info()
	if err != nil {
		return fmt.Sprintf("failed to get OS info: %v", err)
	}

	result := fmt.Sprintf("OS: %v, Platform: %v, Platform Family: %v, Platform Version: %v\n",
		info.OS, info.Platform, info.PlatformFamily, info.PlatformVersion)
	return result
}
