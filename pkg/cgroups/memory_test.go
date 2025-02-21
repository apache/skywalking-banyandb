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
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
)

func TestGetCgroupMemoryLimit(t *testing.T) {
	tests := []struct {
		fs            fs.FS
		expectedError error
		name          string
		expectedLimit int64
	}{
		{
			name: "cgroup v1 with explicit memory limit",
			fs: fstest.MapFS{
				"proc/self/cgroup": &fstest.MapFile{
					Data: []byte("0::/user.slice/user-1000.slice/session-1.scope\n1:memory:/user.slice/user-1000.slice/session-1.scope"),
				},
				"proc/self/mountinfo": &fstest.MapFile{
					Data: []byte("36 35 0:33 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,memory"),
				},
				"sys/fs/cgroup/memory/user.slice/user-1000.slice/session-1.scope/memory.limit_in_bytes": &fstest.MapFile{
					Data: []byte("1073741824"),
				},
				"sys/fs/cgroup/memory/user.slice/user-1000.slice/session-1.scope/memory.hierarchical_memory_limit": &fstest.MapFile{
					Data: []byte("1073741824"),
				},
			},
			expectedLimit: 1073741824,
			expectedError: nil,
		},
		{
			name: "cgroup v1 with inherited memory limit",
			fs: fstest.MapFS{
				"proc/self/cgroup": &fstest.MapFile{
					Data: []byte("0::/user.slice/user-1000.slice/session-1.scope\n1:memory:/user.slice/user-1000.slice/session-1.scope"),
				},
				"proc/self/mountinfo": &fstest.MapFile{
					Data: []byte("36 35 0:33 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:18 - cgroup cgroup rw,memory"),
				},
				"sys/fs/cgroup/memory/user.slice/user-1000.slice/session-1.scope/memory.limit_in_bytes": &fstest.MapFile{
					Data: []byte("9223372036854771712"), // Very high value
				},
				"sys/fs/cgroup/memory/user.slice/user-1000.slice/session-1.scope/memory.hierarchical_memory_limit": &fstest.MapFile{
					Data: []byte("536870912"),
				},
			},
			expectedLimit: 536870912,
			expectedError: nil,
		},
		{
			name: "cgroup v2 with explicit memory limit",
			fs: fstest.MapFS{
				"proc/self/cgroup": &fstest.MapFile{
					Data: []byte("0::/user.slice/user-1000.slice/session-1.scope"),
				},
				"proc/self/mountinfo": &fstest.MapFile{
					Data: []byte("36 35 0:33 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:18 - cgroup2 cgroup rw"),
				},
				"sys/fs/cgroup/user.slice/user-1000.slice/session-1.scope/memory.max": &fstest.MapFile{
					Data: []byte("2147483648"),
				},
			},
			expectedLimit: 2147483648,
			expectedError: nil,
		},
		{
			name: "cgroup v2 with unlimited memory",
			fs: fstest.MapFS{
				"proc/self/cgroup": &fstest.MapFile{
					Data: []byte("0::/user.slice/user-1000.slice/session-1.scope"),
				},
				"proc/self/mountinfo": &fstest.MapFile{
					Data: []byte("36 35 0:33 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:18 - cgroup2 cgroup rw"),
				},
				"sys/fs/cgroup/user.slice/user-1000.slice/session-1.scope/memory.max": &fstest.MapFile{
					Data: []byte("max"),
				},
			},
			expectedLimit: -1,
			expectedError: nil,
		},
		{
			name: "missing cgroup file",
			fs: fstest.MapFS{
				"proc/self/cgroup": &fstest.MapFile{
					Data: []byte("0::/user.slice/user-1000.slice/session-1.scope"),
				},
				"proc/self/mountinfo": &fstest.MapFile{
					Data: []byte("36 35 0:33 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:18 - cgroup2 cgroup rw"),
				},
			},
			expectedLimit: 0,
			expectedError: errors.New("failed to read memory limit"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit, err := getCgroupMemoryLimit(tt.fs)
			if err != nil && tt.expectedError == nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && tt.expectedError != nil {
				t.Fatalf("expected error: %v, got nil", tt.expectedError)
			}
			if limit != tt.expectedLimit {
				t.Fatalf("expected limit: %d, got: %d", tt.expectedLimit, limit)
			}
		})
	}
}
