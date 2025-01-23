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

package cgroups

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	procCgroupPath    = "proc/self/cgroup"
	procMountInfoPath = "proc/self/mountinfo"
)

// MemoryLimit returns the memory limit in bytes for the current process.
func MemoryLimit() (int64, error) {
	return getCgroupMemoryLimit(os.DirFS("/"))
}

func getCgroupMemoryLimit(fsys fs.FS) (int64, error) {
	isV2, err := isCGroupV2(fsys)
	if err != nil {
		return 0, fmt.Errorf("failed to determine cgroup version: %w", err)
	}

	cgroupData, err := fs.ReadFile(fsys, procCgroupPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read cgroup data: %w", err)
	}

	memoryCgroupPath, err := findMemoryCgroupPath(string(cgroupData), isV2)
	if err != nil {
		return 0, fmt.Errorf("failed to find memory cgroup path: %w", err)
	}

	var limitPath string
	if isV2 {
		limitPath = filepath.Join("sys/fs/cgroup", memoryCgroupPath, "memory.max")
	} else {
		limitPath = filepath.Join("sys/fs/cgroup/memory", memoryCgroupPath, "memory.limit_in_bytes")
	}

	limit, err := parseMemoryLimit(fsys, limitPath)
	if err != nil {
		return 0, fmt.Errorf("failed to parse memory limit: %w", err)
	}

	if !isV2 {
		hierarchicalLimitPath := filepath.Join("sys/fs/cgroup/memory", memoryCgroupPath, "memory.hierarchical_memory_limit")
		hierarchicalLimit, err := parseMemoryLimit(fsys, hierarchicalLimitPath)
		if err != nil {
			return 0, fmt.Errorf("failed to parse hierarchical memory limit: %w", err)
		}
		if hierarchicalLimit < limit {
			limit = hierarchicalLimit
		}
	}

	return limit, nil
}

func isCGroupV2(fsys fs.FS) (bool, error) {
	file, err := fsys.Open(procMountInfoPath)
	if err != nil {
		return false, fmt.Errorf("failed to open mountinfo: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mountPoint := fields[4]
		if mountPoint == "/sys/fs/cgroup" {
			for _, field := range fields {
				if strings.HasPrefix(field, "cgroup2") {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func findMemoryCgroupPath(cgroupData string, isV2 bool) (string, error) {
	lines := strings.Split(cgroupData, "\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) >= 3 {
			if isV2 && fields[1] == "" {
				// For cgroup v2, the second field is empty
				return fields[2], nil
			} else if !isV2 && fields[1] == "memory" {
				// For cgroup v1, the second field is "memory"
				return fields[2], nil
			}
		}
	}
	return "", fmt.Errorf("memory cgroup not found")
}

func parseMemoryLimit(fsys fs.FS, path string) (int64, error) {
	limitData, err := fs.ReadFile(fsys, path)
	if err != nil {
		return 0, fmt.Errorf("failed to read memory limit: %w", err)
	}

	limitStr := strings.TrimSpace(string(limitData))
	if limitStr == "max" {
		return -1, nil // -1 represents unlimited
	}

	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse memory limit: %w", err)
	}

	return limit, nil
}
