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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	generated "github.com/SkyAPM/ktm-ebpf/iomonitor/ebpf/generated"
	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"
)

const targetComm = "banyand"

// ErrProcessNotFound is returned when no process with the given comm name is found.
var ErrProcessNotFound = errors.New("process not found")

// configureFilters sets up the cgroup filtering strategy.
// Returns (degraded bool, degradedReason string, error) where degraded=true means cgroup filtering is disabled.
func configureFilters(objs *generated.IomonitorObjects, cgroupPath string) (bool, string, error) {
	targetPath, resolveErr := resolveTargetCgroupPath(cgroupPath)
	if resolveErr != nil {
		if updateErr := updateConfigMap(objs.ConfigMap, 0); updateErr != nil {
			return false, "", fmt.Errorf("failed to configure degraded mode: %w", updateErr)
		}
		return true, fmt.Sprintf("cgroup path resolution failed: %v", resolveErr), nil
	}

	cgID, cgErr := cgroupID(targetPath)
	if cgErr != nil {
		if updateErr := updateConfigMap(objs.ConfigMap, 0); updateErr != nil {
			return false, "", fmt.Errorf("failed to configure degraded mode: %w", updateErr)
		}
		return true, fmt.Sprintf("failed to get cgroup ID from %s: %v", targetPath, cgErr), nil
	}

	if updateErr := updateConfigMap(objs.ConfigMap, cgID); updateErr != nil {
		return false, "", fmt.Errorf("failed to program config map: %w", updateErr)
	}

	return false, "", nil
}

// resolveTargetCgroupPath returns the absolute cgroup v2 directory path of the
// banyand container to use as the eBPF cgroup filter target.
//
// If cfgPath is non-empty it is used directly (after validation).
// Otherwise the function performs pod-scoped discovery:
//  1. Read fodc-agent's own cgroup from /proc/self/cgroup.
//  2. Extract the pod-level cgroup (strip container suffix).
//  3. Scan child cgroups under the pod cgroup for the banyand container.
//  4. Return the banyand container's cgroup path so that
//     bpf_get_current_cgroup_id() in the kernel produces an exact match.
func resolveTargetCgroupPath(cfgPath string) (string, error) {
	if cfgPath != "" {
		return resolveCgroupPath(cfgPath)
	}

	cgMount, mountErr := findCgroup2Mount()
	if mountErr != nil {
		return "", fmt.Errorf("failed to find cgroup2 mount: %w", mountErr)
	}

	selfCgRel, selfErr := readSelfCgroupV2Path()
	if selfErr != nil {
		return "", fmt.Errorf("failed to read self cgroup: %w", selfErr)
	}

	// Inside a cgroup-namespaced container /proc/self/cgroup returns "/".
	// Fall back to walking the host cgroup tree to find the banyand container.
	if selfCgRel == "/" {
		return findContainerCgroupInTree(cgMount, targetComm)
	}

	podCgRel := extractPodLevelCgroup(selfCgRel)
	if podCgRel == "" {
		return "", fmt.Errorf("failed to extract pod-level cgroup from %q", selfCgRel)
	}

	podCgAbs := filepath.Join(cgMount, podCgRel)
	banyandCg, findErr := findContainerCgroupByComm(podCgAbs, targetComm)
	if findErr != nil {
		return "", fmt.Errorf("failed to find %s container cgroup under %s: %w", targetComm, podCgAbs, findErr)
	}

	return banyandCg, nil
}

// findContainerCgroupByComm scans child cgroup directories under podCgroupDir
// and returns the absolute path of the first child cgroup that contains a
// process whose comm matches the given name.
func findContainerCgroupByComm(podCgroupDir, commName string) (string, error) {
	children, readErr := os.ReadDir(podCgroupDir)
	if readErr != nil {
		return "", fmt.Errorf("failed to read pod cgroup dir %s: %w", podCgroupDir, readErr)
	}

	for _, child := range children {
		if !child.IsDir() {
			continue
		}
		childPath := filepath.Join(podCgroupDir, child.Name())
		if cgroupContainsComm(childPath, commName) {
			return childPath, nil
		}
	}

	return "", fmt.Errorf("%w: comm=%q not found in any child cgroup of %s", ErrProcessNotFound, commName, podCgroupDir)
}

// cgroupContainsComm checks whether any PID listed in the cgroup's
// cgroup.procs has the given comm name.
func cgroupContainsComm(cgroupDir, commName string) bool {
	procsPath := filepath.Join(cgroupDir, "cgroup.procs")
	data, readErr := os.ReadFile(procsPath)
	if readErr != nil {
		return false
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		pid, parseErr := strconv.Atoi(line)
		if parseErr != nil {
			continue
		}
		comm, commErr := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
		if commErr != nil {
			continue
		}
		if strings.TrimSpace(string(comm)) == commName {
			return true
		}
	}
	return false
}

// extractPodLevelCgroup extracts the pod-level cgroup path from a container-level path.
//
// Example:
//
//	Input:  /kubepods.slice/kubepods-pod<uuid>.slice/cri-containerd-<id>.scope
//	Output: /kubepods.slice/kubepods-pod<uuid>.slice
func extractPodLevelCgroup(containerPath string) string {
	parts := strings.Split(strings.TrimPrefix(containerPath, "/"), "/")
	for idx := len(parts) - 1; idx >= 0; idx-- {
		part := parts[idx]
		// Kubernetes cgroup patterns:
		// - kubepods-pod<uuid>.slice (systemd driver)
		// - pod<uuid> (cgroupfs driver)
		if strings.HasPrefix(part, "kubepods-pod") ||
			(strings.HasPrefix(part, "pod") && len(part) > 3) {
			return "/" + strings.Join(parts[:idx+1], "/")
		}
	}
	return ""
}

// readSelfCgroupV2Path reads /proc/self/cgroup and returns the cgroup v2
// relative path for the current process.
func readSelfCgroupV2Path() (string, error) {
	data, readErr := os.ReadFile("/proc/self/cgroup")
	if readErr != nil {
		return "", readErr
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || parts[0] != "0" {
			continue
		}
		cgPath := parts[2]
		if cgPath == "" {
			cgPath = "/"
		}
		return cgPath, nil
	}
	return "", errors.New("cgroup v2 entry not found in /proc/self/cgroup")
}

func updateConfigMap(configMap *ebpf.Map, cgroupID uint64) error {
	if configMap == nil {
		return fmt.Errorf("config_map not available")
	}

	key := uint32(0)
	return configMap.Update(key, cgroupID, ebpf.UpdateAny)
}

// cgroupID returns the inode number (used as cgroup ID by the kernel) for the
// given cgroup directory path.
func cgroupID(path string) (uint64, error) {
	var st unix.Stat_t
	if statErr := unix.Stat(path, &st); statErr != nil {
		return 0, statErr
	}
	return st.Ino, nil
}
