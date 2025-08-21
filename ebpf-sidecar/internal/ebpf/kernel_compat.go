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
	"strconv"
	"strings"

	"github.com/cilium/ebpf/btf"
)

// KernelFeatures represents available kernel features for eBPF
type KernelFeatures struct {
	KernelVersion   KernelVersion
	HasBTF          bool
	HasFentry       bool
	HasTracepoints  bool
	HasKprobes      bool
}

// KernelVersion represents a parsed kernel version
type KernelVersion struct {
	Major int
	Minor int
	Patch int
}

// DetectKernelFeatures detects available kernel features for eBPF attachment
func DetectKernelFeatures() (*KernelFeatures, error) {
	features := &KernelFeatures{
		HasTracepoints: true, // Available since 4.7
		HasKprobes:     true, // Available since ancient times
	}

	// Get kernel version
	version, err := GetKernelVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get kernel version: %w", err)
	}
	features.KernelVersion = version

	// Check BTF support (required for fentry/fexit)
	features.HasBTF = checkBTFSupport()

	// fentry/fexit requires kernel >= 5.5 and BTF support
	if version.Major > 5 || (version.Major == 5 && version.Minor >= 5) {
		features.HasFentry = features.HasBTF
	}

	return features, nil
}

// GetKernelVersion parses the kernel version from /proc/version_signature or uname
func GetKernelVersion() (KernelVersion, error) {
	// Try Ubuntu's version_signature first (more reliable)
	if data, err := os.ReadFile("/proc/version_signature"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) >= 2 {
			return parseVersionString(parts[1])
		}
	}

	// Fallback to /proc/sys/kernel/osrelease
	data, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return KernelVersion{}, fmt.Errorf("failed to read kernel version: %w", err)
	}

	versionStr := strings.TrimSpace(string(data))
	return parseVersionString(versionStr)
}

// parseVersionString parses a kernel version string like "5.15.0-91-generic"
func parseVersionString(versionStr string) (KernelVersion, error) {
	// Remove any suffix after the version numbers
	if idx := strings.IndexAny(versionStr, "-_"); idx != -1 {
		versionStr = versionStr[:idx]
	}

	parts := strings.Split(versionStr, ".")
	if len(parts) < 2 {
		return KernelVersion{}, fmt.Errorf("invalid kernel version format: %s", versionStr)
	}

	var version KernelVersion
	var err error

	version.Major, err = strconv.Atoi(parts[0])
	if err != nil {
		return KernelVersion{}, fmt.Errorf("invalid major version: %w", err)
	}

	version.Minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return KernelVersion{}, fmt.Errorf("invalid minor version: %w", err)
	}

	if len(parts) >= 3 {
		version.Patch, _ = strconv.Atoi(parts[2]) // Ignore error for patch
	}

	return version, nil
}

// checkBTFSupport checks if the kernel has BTF support
func checkBTFSupport() bool {
	// Check if BTF is available in /sys/kernel/btf/vmlinux
	if _, err := os.Stat("/sys/kernel/btf/vmlinux"); err == nil {
		return true
	}

	// Try to load BTF from kernel
	_, err := btf.LoadKernelSpec()
	return err == nil
}

// GetFadviseFunctionNames returns possible function names for fadvise based on kernel version
func GetFadviseFunctionNames(version KernelVersion) []string {
	// Kernel >= 5.11: ksys_fadvise64_64 is the primary internal function
	if version.Major > 5 || (version.Major == 5 && version.Minor >= 11) {
		return []string{
			"ksys_fadvise64_64",
			"__x64_sys_fadvise64",
			"__do_sys_fadvise64", // Some kernels use this
			"sys_fadvise64",
		}
	}

	// Kernel 5.0 - 5.10
	if version.Major == 5 {
		return []string{
			"ksys_fadvise64_64",
			"__x64_sys_fadvise64",
			"sys_fadvise64",
		}
	}

	// Kernel 4.x
	return []string{
		"sys_fadvise64",
		"SyS_fadvise64", // Older naming convention
		"sys_fadvise64_64",
	}
}

// GetFilemapFunctionNames returns possible function names for filemap operations
func GetFilemapFunctionNames(version KernelVersion) (readFuncs []string, addFuncs []string) {
	// Read batch functions (varies by kernel version)
	if version.Major > 5 || (version.Major == 5 && version.Minor >= 15) {
		readFuncs = []string{
			"filemap_get_read_batch",
			"filemap_read_folio", // Newer kernels use folio
			"filemap_read_page",
		}
	} else {
		readFuncs = []string{
			"filemap_get_read_batch",
			"do_generic_file_read",
			"generic_file_buffered_read",
		}
	}

	// Page cache add functions
	if version.Major > 5 || (version.Major == 5 && version.Minor >= 16) {
		addFuncs = []string{
			"filemap_add_folio", // Newer kernels use folio
			"__filemap_add_folio",
			"add_to_page_cache_lru",
		}
	} else {
		addFuncs = []string{
			"add_to_page_cache_lru",
			"add_to_page_cache_locked",
			"__add_to_page_cache_locked",
		}
	}

	return readFuncs, addFuncs
}

// ShouldUseFentry determines if fentry should be used for a given function
func ShouldUseFentry(features *KernelFeatures, funcName string) bool {
	if !features.HasFentry {
		return false
	}

	// Check if the function exists in BTF
	spec, err := btf.LoadKernelSpec()
	if err != nil {
		return false
	}

	var found bool
	err = spec.TypeByName(funcName, &found)
	return err == nil && found
}

// AttachmentModeString returns a string representation of the best available attachment mode
func AttachmentModeString(features *KernelFeatures) string {
	if features.HasFentry {
		return "fentry/fexit (optimal)"
	}
	if features.HasTracepoints {
		return "tracepoints (stable)"
	}
	if features.HasKprobes {
		return "kprobes (fallback)"
	}
	return "none available"
}