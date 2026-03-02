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
	"strings"
)

// findCgroup2Mount locates the cgroup v2 unified mount point.
func findCgroup2Mount() (string, error) {
	data, readErr := os.ReadFile("/proc/mounts")
	if readErr != nil {
		return "", readErr
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[2] == "cgroup2" {
			if _, statErr := os.Stat(filepath.Join(fields[1], "cgroup.controllers")); statErr == nil {
				return fields[1], nil
			}
		}
	}
	return "", errors.New("cgroup2 mount not found")
}

// findContainerCgroupInTree walks the host cgroup tree rooted at cgroupRoot
// looking for a cgroup directory that contains a process whose comm matches
// commName. The walk stops at the first match.
//
// This is used as a fallback when /proc/self/cgroup returns "/" due to
// cgroup namespace isolation, making pod-scoped discovery impossible.
func findContainerCgroupInTree(cgroupRoot, commName string) (string, error) {
	var result string
	walkErr := filepath.WalkDir(cgroupRoot, func(path string, d os.DirEntry, walkDirErr error) error {
		if walkDirErr != nil || !d.IsDir() {
			return nil
		}
		if cgroupContainsComm(path, commName) {
			result = path
			return filepath.SkipAll
		}
		return nil
	})
	if walkErr != nil {
		return "", fmt.Errorf("failed to walk cgroup tree: %w", walkErr)
	}
	if result == "" {
		return "", fmt.Errorf("%w: comm=%q not found in cgroup tree %s", ErrProcessNotFound, commName, cgroupRoot)
	}
	return result, nil
}
