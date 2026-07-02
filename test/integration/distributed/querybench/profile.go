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

package querybench

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
)

type processSnapshot struct {
	MemStats   runtime.MemStats
	RSSBytes   uint64
	CPUSeconds float64
}

type profileRecorder struct {
	cpuFile *os.File
	paths   map[string]string
}

func captureProcessSnapshot() processSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return processSnapshot{MemStats: stats, RSSBytes: readRSSBytes(), CPUSeconds: readCPUSeconds()}
}

func resourceDelta(before, after processSnapshot) ResourceStats {
	numGC := after.MemStats.NumGC - before.MemStats.NumGC
	return ResourceStats{
		CPUSecondsDelta: after.CPUSeconds - before.CPUSeconds,
		RSSBytes:        after.RSSBytes,
		HeapAllocBytes:  after.MemStats.HeapAlloc,
		HeapSysBytes:    after.MemStats.HeapSys,
		NumGC:           numGC,
		MetricSource:    "runtime.ReadMemStats+/proc/self/statm",
	}
}

func allocationDelta(before, after processSnapshot, queries int) AllocationStats {
	mallocsDelta := after.MemStats.Mallocs - before.MemStats.Mallocs
	totalAllocDelta := after.MemStats.TotalAlloc - before.MemStats.TotalAlloc
	allocs := AllocationStats{
		MallocsDelta:    mallocsDelta,
		TotalAllocDelta: totalAllocDelta,
		MetricSource:    "runtime.ReadMemStats",
	}
	if queries > 0 {
		allocs.MallocsPerQuery = float64(mallocsDelta) / float64(queries)
		allocs.AllocBytesPerQuery = float64(totalAllocDelta) / float64(queries)
	}
	return allocs
}

func startProfile(reportDir string, scenario Scenario, cardinality int, mode string, enabled bool) (*profileRecorder, error) {
	return startProfileWithVariant(reportDir, scenario, cardinality, mode, "", enabled)
}

func startProfileWithVariant(reportDir string, scenario Scenario, cardinality int, mode, variant string, enabled bool) (*profileRecorder, error) {
	recorder := &profileRecorder{paths: make(map[string]string)}
	if !enabled {
		return recorder, nil
	}
	pathParts := []string{reportDir, "profiles", string(scenario), strconv.Itoa(cardinality)}
	if variant != "" {
		pathParts = append(pathParts, sanitizeProfilePath(variant))
	}
	pathParts = append(pathParts, mode)
	profileDir := filepath.Join(pathParts...)
	if mkdirErr := os.MkdirAll(profileDir, 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("create profile directory: %w", mkdirErr)
	}
	cpuPath := filepath.Join(profileDir, "cpu.pprof")
	cpuFile, createErr := os.Create(cpuPath)
	if createErr != nil {
		return nil, fmt.Errorf("create cpu profile: %w", createErr)
	}
	if profileErr := pprof.StartCPUProfile(cpuFile); profileErr != nil {
		closeErr := cpuFile.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("start cpu profile: %w; close profile file: %w", profileErr, closeErr)
		}
		return nil, fmt.Errorf("start cpu profile: %w", profileErr)
	}
	recorder.cpuFile = cpuFile
	recorder.paths["cpu"] = cpuPath
	recorder.paths["heap"] = filepath.Join(profileDir, "heap.pprof")
	return recorder, nil
}

func sanitizeProfilePath(value string) string {
	replacer := strings.NewReplacer(",", "_", "=", "-", " ", "", "/", "_")
	return replacer.Replace(value)
}

func (r *profileRecorder) stop() (map[string]string, error) {
	if r == nil {
		return nil, nil
	}
	if r.cpuFile != nil {
		pprof.StopCPUProfile()
		if closeErr := r.cpuFile.Close(); closeErr != nil {
			return r.paths, fmt.Errorf("close cpu profile: %w", closeErr)
		}
	}
	if heapPath := r.paths["heap"]; heapPath != "" {
		heapFile, createErr := os.Create(heapPath)
		if createErr != nil {
			return r.paths, fmt.Errorf("create heap profile: %w", createErr)
		}
		runtime.GC()
		if writeErr := pprof.Lookup("heap").WriteTo(heapFile, 0); writeErr != nil {
			closeErr := heapFile.Close()
			if closeErr != nil {
				return r.paths, fmt.Errorf("write heap profile: %w; close heap profile: %w", writeErr, closeErr)
			}
			return r.paths, fmt.Errorf("write heap profile: %w", writeErr)
		}
		if closeErr := heapFile.Close(); closeErr != nil {
			return r.paths, fmt.Errorf("close heap profile: %w", closeErr)
		}
	}
	return r.paths, nil
}

func readCPUSeconds() float64 {
	body, readErr := os.ReadFile("/proc/self/stat")
	if readErr != nil {
		return 0
	}
	raw := string(body)
	endComm := strings.LastIndex(raw, ")")
	if endComm < 0 || endComm+2 >= len(raw) {
		return 0
	}
	fields := strings.Fields(raw[endComm+2:])
	// Fields after comm start at procfs field 3; utime/stime are fields 14/15.
	if len(fields) < 13 {
		return 0
	}
	utimeTicks, utimeErr := strconv.ParseUint(fields[11], 10, 64)
	if utimeErr != nil {
		return 0
	}
	stimeTicks, stimeErr := strconv.ParseUint(fields[12], 10, 64)
	if stimeErr != nil {
		return 0
	}
	const linuxClockTicksPerSecond = 100
	return float64(utimeTicks+stimeTicks) / linuxClockTicksPerSecond
}

func readRSSBytes() uint64 {
	body, readErr := os.ReadFile("/proc/self/statm")
	if readErr != nil {
		return 0
	}
	fields := strings.Fields(string(body))
	if len(fields) < 2 {
		return 0
	}
	pages, parseErr := strconv.ParseUint(fields[1], 10, 64)
	if parseErr != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
