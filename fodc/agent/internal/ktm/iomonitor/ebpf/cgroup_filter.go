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
)

// isCgroupV2Enabled returns true if a cgroup v2 hierarchy is available.
func isCgroupV2Enabled() bool {
	// Presence of cgroup.controllers indicates cgroup v2
	_, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	return err == nil
}

// resolveCgroupPath normalizes a configured cgroup path.
// If the path is relative, it is treated as relative to the cgroup v2 mountpoint.
func resolveCgroupPath(cfgPath string) (string, error) {
	if cfgPath == "" {
		return "", nil
	}

	if !isCgroupV2Enabled() {
		return "", fmt.Errorf("cgroup v2 not available on this host")
	}

	if filepath.IsAbs(cfgPath) {
		return cfgPath, nil
	}

	// Treat relative paths as under the default mountpoint.
	return filepath.Join("/sys/fs/cgroup", cfgPath), nil
}
