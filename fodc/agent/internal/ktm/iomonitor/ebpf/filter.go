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

//go:build linux

package ebpf

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
)

// configureFilters sets up the layered filtering strategy:
// Layer 1: Cgroup ID (primary correctness boundary) - REQUIRED.
// Layer 2: PID allowlist (performance cache + health signal) - OPTIONAL.
func configureFilters(objs *generated.IomonitorObjects, cgroupPath string, targetComm string) error {
	// Layer 1: Resolve and configure cgroup filter (required for correctness)
	targetPath, err := resolveTargetCgroupPath(cgroupPath)
	if err != nil {
		return fmt.Errorf("resolve target cgroup: %w", err)
	}

	cgID, err := getCgroupIDFromPath(targetPath)
	if err != nil {
		return fmt.Errorf("read cgroup id: %w", err)
	}

	if updateErr := updateConfigMap(objs.ConfigMap, cgID); updateErr != nil {
		return fmt.Errorf("program config map: %w", updateErr)
	}

	// Layer 2: Initialize PID cache (optional, for performance + diagnostics)
	// This is NOT required for correctness, but helps with:
	// - Fast path optimization (avoids repeated comm checks)
	// - Health monitoring (can report "target process alive")
	pids, err := findPIDsByCommPrefix(targetComm)
	if err != nil {
		// Non-fatal: cgroup filter is sufficient for correctness
		// Log warning but continue - eBPF will use comm check as fallback
		return nil
	}

	if err := replaceAllowedPIDs(objs.AllowedPids, pids); err != nil {
		// Non-fatal: PID cache update failed but cgroup filter still works
		return nil
	}

	return nil
}

// refreshAllowedPIDs updates the PID cache for health monitoring purposes.
// This function is primarily for observability rather than correctness:
// - Provides a health signal (are target processes running?)
// - Maintains PID cache for performance optimization
// - Does NOT affect filtering correctness (cgroup filter handles that)
// If no target processes are found, it clears the PID cache to avoid stale entries.
func refreshAllowedPIDs(objs *generated.IomonitorObjects, prefix string) error {
	pids, err := findPIDsByCommPrefix(prefix)
	if err != nil {
		return err
	}

	// If no processes found, clear the cache to remove stale PIDs
	if len(pids) == 0 {
		return clearAllowedPIDs(objs.AllowedPids)
	}

	return replaceAllowedPIDs(objs.AllowedPids, pids)
}

func resolveTargetCgroupPath(cfgPath string) (string, error) {
	if cfgPath != "" {
		return resolveCgroupPath(cfgPath)
	}

	// Try to detect banyand process cgroup (requires shareProcessNamespace)
	if path, err := detectBanyanDBCgroupPath(); err == nil {
		return path, nil
	}

	// Fallback: derive Pod-level cgroup from self
	// This works in Kubernetes even without shareProcessNamespace
	cgRel, err := readCgroupV2Path("self")
	if err != nil {
		return "", fmt.Errorf("failed to read self cgroup: %w", err)
	}

	// Extract Pod-level cgroup path (remove container-specific suffix)
	podCgRel := extractPodLevelCgroup(cgRel)
	if podCgRel == "" {
		return "", fmt.Errorf("failed to extract Pod-level cgroup from: %s", cgRel)
	}

	return filepath.Join("/sys/fs/cgroup", podCgRel), nil
}

func updateConfigMap(configMap *ebpf.Map, cgroupID uint64) error {
	if configMap == nil {
		return fmt.Errorf("config_map not available")
	}

	key := uint32(0)
	return configMap.Update(key, cgroupID, ebpf.UpdateAny)
}

func replaceAllowedPIDs(pidMap *ebpf.Map, pids []uint32) error {
	if pidMap == nil {
		return fmt.Errorf("allowed_pids map not available")
	}

	desired := make(map[uint32]struct{}, len(pids))
	for _, pid := range pids {
		desired[pid] = struct{}{}
		val := uint8(1)
		if err := pidMap.Update(pid, val, ebpf.UpdateAny); err != nil {
			return fmt.Errorf("update allowed pid %d: %w", pid, err)
		}
	}

	// Remove stale PIDs that are no longer in the desired set
	iter := pidMap.Iterate()
	var pid uint32
	var flag uint8
	for iter.Next(&pid, &flag) {
		if _, ok := desired[pid]; !ok {
			_ = pidMap.Delete(pid)
		}
	}

	return iter.Err()
}

// clearAllowedPIDs removes all entries from the PID cache.
// Used when target process disappears to avoid stale PID entries.
func clearAllowedPIDs(pidMap *ebpf.Map) error {
	if pidMap == nil {
		return fmt.Errorf("allowed_pids map not available")
	}

	iter := pidMap.Iterate()
	var pid uint32
	var flag uint8
	for iter.Next(&pid, &flag) {
		_ = pidMap.Delete(pid)
	}

	return iter.Err()
}

func findPIDsByCommPrefix(prefix string) ([]uint32, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read /proc: %w", err)
	}

	var pids []uint32
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pidVal, err := strconv.Atoi(ent.Name())
		if err != nil || pidVal <= 0 {
			continue
		}

		commBytes, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "comm"))
		if err != nil {
			continue
		}
		comm := strings.TrimSpace(string(commBytes))
		if strings.HasPrefix(comm, prefix) {
			pids = append(pids, uint32(pidVal))
		}
	}

	return pids, nil
}

func getCgroupIDFromPath(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat cgroup path %s: %w", path, err)
	}

	stat, ok := info.Sys().(*unix.Stat_t)
	if !ok {
		return 0, fmt.Errorf("failed to get stat_t for %s", path)
	}
	return stat.Ino, nil
}

// extractPodLevelCgroup extracts the Pod-level cgroup path from a container-level path.
// Example:
//
//	Input:  /kubepods.slice/kubepods-pod<uuid>.slice/cri-containerd-<id>.scope
//	Output: /kubepods.slice/kubepods-pod<uuid>.slice
func extractPodLevelCgroup(containerPath string) string {
	parts := strings.Split(strings.TrimPrefix(containerPath, "/"), "/")

	// Search backwards for Pod-level cgroup marker
	for i := len(parts) - 1; i >= 0; i-- {
		part := parts[i]

		// Kubernetes cgroup patterns:
		// - kubepods-pod<uuid>.slice (systemd)
		// - pod<uuid> (cgroupfs)
		if strings.HasPrefix(part, "kubepods-pod") ||
			(strings.HasPrefix(part, "pod") && len(part) > 3) {
			// Return path up to and including the Pod level
			return "/" + strings.Join(parts[:i+1], "/")
		}
	}

	// If no Pod marker found, return empty (caller will error)
	return ""
}
