// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package controller provides the external publication boundary for trace merge benchmarks.
package controller

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
)

const linuxCPUSetSize = 1024

// Move describes one immutable directory rename.
type Move struct {
	Source      string
	Destination string
	SHA256      string
}

// PublishedMove records one completed atomic rename.
type PublishedMove struct {
	Source      string
	Destination string
	Manifest    benchmark.Manifest
}

// PublishReport records all completed moves in manifest order.
type PublishReport struct {
	Moves []PublishedMove
}

// ResourceIdentity identifies a process's measured cgroup and CPU affinity.
type ResourceIdentity struct {
	Cgroup string `json:"cgroup"`
	CPUs   []int  `json:"cpus"`
	PID    int    `json:"pid"`
}

// AtomicPublish validates all moves before atomically renaming each directory.
func AtomicPublish(moves []Move) (PublishReport, error) {
	manifests := make([]benchmark.Manifest, len(moves))
	for moveIdx, move := range moves {
		if move.Source == "" || move.Destination == "" || move.SHA256 == "" {
			return PublishReport{}, fmt.Errorf("move %d requires source, destination, and SHA-256", moveIdx)
		}
		if _, statErr := os.Stat(move.Destination); statErr == nil {
			return PublishReport{}, fmt.Errorf("move %d destination already exists: %s", moveIdx, move.Destination)
		} else if !os.IsNotExist(statErr) {
			return PublishReport{}, fmt.Errorf("cannot inspect move %d destination: %w", moveIdx, statErr)
		}
		manifest, manifestErr := benchmark.TreeManifest(move.Source)
		if manifestErr != nil {
			return PublishReport{}, fmt.Errorf("cannot validate move %d source: %w", moveIdx, manifestErr)
		}
		if manifest.SHA256 != move.SHA256 {
			return PublishReport{}, fmt.Errorf("move %d source checksum mismatch: got %s, want %s", moveIdx, manifest.SHA256, move.SHA256)
		}
		if sameErr := requireSameFilesystem(move.Source, filepath.Dir(move.Destination)); sameErr != nil {
			return PublishReport{}, fmt.Errorf("move %d is not an atomic same-filesystem publication: %w", moveIdx, sameErr)
		}
		manifests[moveIdx] = manifest
	}

	report := PublishReport{Moves: make([]PublishedMove, 0, len(moves))}
	for moveIdx, move := range moves {
		if renameErr := os.Rename(move.Source, move.Destination); renameErr != nil {
			return report, fmt.Errorf("cannot publish move %d: %w", moveIdx, renameErr)
		}
		after, manifestErr := benchmark.TreeManifest(move.Destination)
		if manifestErr != nil {
			return report, fmt.Errorf("cannot validate published move %d: %w", moveIdx, manifestErr)
		}
		if after != manifests[moveIdx] {
			return report, fmt.Errorf("published move %d changed bytes: before=%s after=%s", moveIdx, manifests[moveIdx].SHA256, after.SHA256)
		}
		report.Moves = append(report.Moves, PublishedMove{
			Manifest:    after,
			Source:      move.Source,
			Destination: move.Destination,
		})
	}
	return report, nil
}

// CurrentResourceIdentity reads cgroup and CPU affinity for a process.
func CurrentResourceIdentity(pid int) (ResourceIdentity, error) {
	if pid <= 0 {
		pid = os.Getpid()
	}
	cgroupData, readErr := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if readErr != nil {
		return ResourceIdentity{}, fmt.Errorf("cannot read cgroup for PID %d: %w", pid, readErr)
	}
	cgroup, parseErr := normalizedCgroup(string(cgroupData))
	if parseErr != nil {
		return ResourceIdentity{}, fmt.Errorf("cannot parse cgroup for PID %d: %w", pid, parseErr)
	}
	if cgroup == "" {
		return ResourceIdentity{}, fmt.Errorf("cannot determine cgroup for PID %d", pid)
	}
	var cpuSet unix.CPUSet
	if affinityErr := unix.SchedGetaffinity(pid, &cpuSet); affinityErr != nil {
		return ResourceIdentity{}, fmt.Errorf("cannot read CPU affinity for PID %d: %w", pid, affinityErr)
	}
	availableCPUs := make([]int, 0, cpuSet.Count())
	for cpu := 0; cpu < linuxCPUSetSize; cpu++ {
		if cpuSet.IsSet(cpu) {
			availableCPUs = append(availableCPUs, cpu)
		}
	}
	return ResourceIdentity{PID: pid, Cgroup: cgroup, CPUs: availableCPUs}, nil
}

// PinToCPUs restricts the current process to the requested logical CPUs.
func PinToCPUs(cpus []int) error {
	if len(cpus) == 0 {
		return fmt.Errorf("controller CPU set cannot be empty")
	}
	var cpuSet unix.CPUSet
	for _, cpu := range cpus {
		if cpu < 0 || cpu >= linuxCPUSetSize {
			return fmt.Errorf("controller CPU %d is outside the supported range", cpu)
		}
		cpuSet.Set(cpu)
	}
	if affinityErr := unix.SchedSetaffinity(0, &cpuSet); affinityErr != nil {
		return fmt.Errorf("cannot set controller CPU affinity: %w", affinityErr)
	}
	return nil
}

// ValidateResourceIsolation rejects controller resources included in data-node measurements.
func ValidateResourceIsolation(dataNode, externalController ResourceIdentity) error {
	if dataNode.PID <= 0 || externalController.PID <= 0 || dataNode.PID == externalController.PID {
		return fmt.Errorf("data node and controller must be distinct processes")
	}
	if dataNode.Cgroup == "" || externalController.Cgroup == "" || dataNode.Cgroup == externalController.Cgroup {
		return fmt.Errorf("data node and controller must use distinct cgroups")
	}
	if len(dataNode.CPUs) == 0 || len(externalController.CPUs) == 0 {
		return fmt.Errorf("data node and controller CPU sets must be known")
	}
	dataCPUs := make(map[int]struct{}, len(dataNode.CPUs))
	for _, cpu := range dataNode.CPUs {
		dataCPUs[cpu] = struct{}{}
	}
	for _, cpu := range externalController.CPUs {
		if _, overlaps := dataCPUs[cpu]; overlaps {
			return fmt.Errorf("controller CPU %d overlaps the data-node CPU set", cpu)
		}
	}
	return nil
}

func requireSameFilesystem(source, destinationParent string) error {
	var sourceStat, destinationStat unix.Stat_t
	if statErr := unix.Stat(source, &sourceStat); statErr != nil {
		return fmt.Errorf("cannot stat source %q: %w", source, statErr)
	}
	if statErr := unix.Stat(destinationParent, &destinationStat); statErr != nil {
		return fmt.Errorf("cannot stat destination parent %q: %w", destinationParent, statErr)
	}
	if sourceStat.Dev != destinationStat.Dev {
		return fmt.Errorf("source device %d differs from destination device %d", sourceStat.Dev, destinationStat.Dev)
	}
	return nil
}

func normalizedCgroup(raw string) (string, error) {
	var entries []string
	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		entry := strings.TrimSpace(scanner.Text())
		if entry != "" {
			entries = append(entries, entry)
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return "", fmt.Errorf("cannot scan cgroup entries: %w", scanErr)
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n"), nil
}
