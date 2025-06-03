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
// +build linux

package monitor

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// KernelVersion represents a Linux kernel version.
type KernelVersion struct {
	Major int
	Minor int
	Patch int
}

// String returns a string representation of the kernel version.
func (kv KernelVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", kv.Major, kv.Minor, kv.Patch)
}

// Compare compares two kernel versions. Returns:
// -1 if kv < other
//  0 if kv == other
//  1 if kv > other
func (kv KernelVersion) Compare(other KernelVersion) int {
	if kv.Major != other.Major {
		if kv.Major < other.Major {
			return -1
		}
		return 1
	}
	if kv.Minor != other.Minor {
		if kv.Minor < other.Minor {
			return -1
		}
		return 1
	}
	if kv.Patch != other.Patch {
		if kv.Patch < other.Patch {
			return -1
		}
		return 1
	}
	return 0
}

// GetKernelVersion reads and parses the current kernel version.
func GetKernelVersion() (KernelVersion, error) {
	data, err := os.ReadFile("/proc/version")
	if err != nil {
		return KernelVersion{}, fmt.Errorf("failed to read /proc/version: %w", err)
	}

	version := string(data)
	// Example: "Linux version 5.15.0-1-amd64 (debian-kernel@lists.debian.org) ..."
	parts := strings.Fields(version)
	if len(parts) < 3 {
		return KernelVersion{}, fmt.Errorf("unexpected /proc/version format: %s", version)
	}

	versionStr := parts[2]
	// Remove any suffix like "-amd64" or "+
	dashIdx := strings.Index(versionStr, "-")
	if dashIdx > 0 {
		versionStr = versionStr[:dashIdx]
	}
	plusIdx := strings.Index(versionStr, "+")
	if plusIdx > 0 {
		versionStr = versionStr[:plusIdx]
	}

	versionParts := strings.Split(versionStr, ".")
	if len(versionParts) < 3 {
		return KernelVersion{}, fmt.Errorf("unexpected version format: %s", versionStr)
	}

	major, err := strconv.Atoi(versionParts[0])
	if err != nil {
		return KernelVersion{}, fmt.Errorf("invalid major version: %s", versionParts[0])
	}

	minor, err := strconv.Atoi(versionParts[1])
	if err != nil {
		return KernelVersion{}, fmt.Errorf("invalid minor version: %s", versionParts[1])
	}

	patch, err := strconv.Atoi(versionParts[2])
	if err != nil {
		return KernelVersion{}, fmt.Errorf("invalid patch version: %s", versionParts[2])
	}

	return KernelVersion{
		Major: major,
		Minor: minor,
		Patch: patch,
	}, nil
}

// ShouldUseCORE determines if CO-RE (Compile Once, Run Everywhere) should be used
// based on kernel version and BTF availability.
func ShouldUseCORE() bool {
	// Check if BTF is available
	if !isBTFAvailable() {
		return false
	}

	// Check kernel version - CO-RE requires at least 5.4
	version, err := GetKernelVersion()
	if err != nil {
		return false
	}

	minVersion := KernelVersion{Major: 5, Minor: 4, Patch: 0}
	return version.Compare(minVersion) >= 0
}

// isBTFAvailable checks if BTF (BPF Type Format) information is available.
func isBTFAvailable() bool {
	// Check for kernel BTF
	if _, err := os.Stat("/sys/kernel/btf/vmlinux"); err == nil {
		return true
	}

	// Check for BTF in boot directory
	if _, err := os.Stat("/boot/vmlinux-" + getKernelRelease()); err == nil {
		return true
	}

	return false
}

// getKernelRelease returns the kernel release string.
func getKernelRelease() string {
	data, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// getFadviseFunctionNames returns a list of possible fadvise function names
// to try for kprobe attachment, ordered by preference.
func getFadviseFunctionNames() []string {
	version, err := GetKernelVersion()
	if err != nil {
		// Default list if we can't determine version
		return []string{"ksys_fadvise64_64", "__x64_sys_fadvise64", "sys_fadvise64"}
	}

	// Kernel 4.17+ uses ksys_* naming
	if version.Compare(KernelVersion{Major: 4, Minor: 17, Patch: 0}) >= 0 {
		return []string{"ksys_fadvise64_64", "__x64_sys_fadvise64", "sys_fadvise64"}
	}

	// Older kernels use sys_* naming
	return []string{"sys_fadvise64", "__x64_sys_fadvise64", "ksys_fadvise64_64"}
}

// getFilemapFunctionNames returns possible filemap function names for different kernel versions.
func getFilemapFunctionNames() ([]string, []string) {
	version, err := GetKernelVersion()
	if err != nil {
		// Default lists
		return []string{"filemap_get_read_batch", "filemap_get_pages"},
			[]string{"add_to_page_cache_lru", "__add_to_page_cache_locked"}
	}

	readFuncs := []string{"filemap_get_read_batch"}
	addFuncs := []string{"add_to_page_cache_lru"}

	// In older kernels, different function names might be used
	if version.Compare(KernelVersion{Major: 5, Minor: 10, Patch: 0}) < 0 {
		readFuncs = append(readFuncs, "filemap_get_pages")
		addFuncs = append(addFuncs, "__add_to_page_cache_locked")
	}

	return readFuncs, addFuncs
}
