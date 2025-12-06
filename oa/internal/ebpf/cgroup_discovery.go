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
)

// detectBanyanDBCgroupPath best-effort discovers the cgroup v2 path of a BanyanDB process.
// It scans processes whose comm contains "banyandb" and returns the first cgroup v2 path.
func detectBanyanDBCgroupPath() (string, error) {
	if !isCgroupV2Enabled() {
		return "", fmt.Errorf("cgroup v2 is not available")
	}

	entries, err := os.ReadDir("/proc")
	if err != nil {
		return "", fmt.Errorf("failed to read /proc: %w", err)
	}

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(ent.Name())
		if err != nil || pid <= 0 {
			continue
		}

		commBytes, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "comm"))
		if err != nil {
			continue
		}
		comm := strings.TrimSpace(string(commBytes))
		if !strings.Contains(comm, "banyandb") {
			continue
		}

		cgPath, err := readCgroupV2Path(ent.Name())
		if err != nil {
			continue
		}

		return filepath.Join("/sys/fs/cgroup", cgPath), nil
	}

	return "", fmt.Errorf("no banyandb process cgroup found")
}

// readCgroupV2Path returns the cgroup v2 relative path for a pid.
func readCgroupV2Path(pid string) (string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", pid, "cgroup"))
	if err != nil {
		return "", fmt.Errorf("failed to read cgroup for pid %s: %w", pid, err)
	}

	lines := strings.Split(string(data), "\n")
	for _, l := range lines {
		if l == "" {
			continue
		}
		parts := strings.SplitN(l, "::", 2)
		if len(parts) != 2 {
			continue
		}
		// cgroup v2 has empty controller field, so we expect format "0::/..."
		return strings.TrimSpace(parts[1]), nil
	}

	return "", fmt.Errorf("no cgroup v2 entry for pid %s", pid)
}
