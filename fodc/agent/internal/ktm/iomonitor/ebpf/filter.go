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
	"strings"

	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
)

// configureFilters sets up the cgroup filtering strategy.
// Returns (degraded bool, degradedReason string, error) where degraded=true means cgroup filtering is disabled.
func configureFilters(objs *generated.IomonitorObjects, cgroupPath string) (bool, string, error) {
	// Resolve and configure cgroup filter (preferred for correctness)
	targetPath, err := resolveTargetCgroupPath(cgroupPath)
	if err != nil {
		// Cgroup resolution failed - fall back to comm-only filtering
		// Set config_map to 0 to signal eBPF to skip cgroup check
		if updateErr := updateConfigMap(objs.ConfigMap, 0); updateErr != nil {
			return false, "", fmt.Errorf("failed to configure degraded mode: %w", updateErr)
		}
		return true, fmt.Sprintf("cgroup path resolution failed: %v", err), nil
	}

	// Get cgroup ID
	cgID, cgErr := getCgroupIDFromPath(targetPath)
	if cgErr != nil {
		// Failed to get cgroup ID - fall back to comm-only
		if updateErr := updateConfigMap(objs.ConfigMap, 0); updateErr != nil {
			return false, "", fmt.Errorf("failed to configure degraded mode: %w", updateErr)
		}
		return true, fmt.Sprintf("failed to get cgroup ID from %s: %v", targetPath, cgErr), nil
	}

	// Successfully got cgroup ID - enable strict filtering
	if updateErr := updateConfigMap(objs.ConfigMap, cgID); updateErr != nil {
		return false, "", fmt.Errorf("failed to program config map: %w", updateErr)
	}

	return false, "", nil
}

func resolveTargetCgroupPath(cfgPath string) (string, error) {
	if cfgPath != "" {
		return resolveCgroupPath(cfgPath)
	}

	// Derive Pod-level cgroup from self.
	// This works in Kubernetes even without shareProcessNamespace.
	cgRel, err := readCgroupV2Path("self")
	if err != nil {
		return "", fmt.Errorf("failed to read self cgroup: %w", err)
	}

	// Extract Pod-level cgroup path (remove container-specific suffix)
	podCgRel := extractPodLevelCgroup(cgRel)
	if podCgRel == "" {
		// Fallback for non-Kubernetes environments: use self cgroup path.
		return filepath.Join("/sys/fs/cgroup", cgRel), nil
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
