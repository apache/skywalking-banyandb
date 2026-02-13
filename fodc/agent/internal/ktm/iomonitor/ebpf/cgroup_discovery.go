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
