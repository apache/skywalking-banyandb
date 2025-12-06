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

// This file contains the eBPF program loader for the I/O monitoring module.
// eBPF bindings are generated automatically by the Makefile.

import (
	"fmt"
	"os"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"

	"github.com/apache/skywalking-banyandb/oa/internal/ebpf/generated"
)

// Loader handles loading and managing eBPF programs.
type Loader struct {
	spec    *ebpf.CollectionSpec
	objects *generated.IomonitorObjects
	links   []link.Link
	// Optional cgroup filter support
	cgroupPath string
	cgroupFD   *os.File
}

// NewLoader creates a new eBPF program loader.
func NewLoader() (*Loader, error) {
	// Remove memory limit for eBPF
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("failed to remove memlock limit: %w", err)
	}

	return &Loader{
		links: make([]link.Link, 0),
	}, nil
}

// SetCgroupPath enables cgroup-based PID filtering using a cgroup v2 path.
// An empty path disables the filter.
func (l *Loader) SetCgroupPath(path string) {
	l.cgroupPath = path
}

// LoadPrograms loads the eBPF programs.
func (l *Loader) LoadPrograms() error {
	var err error

	// Load the eBPF program collection
	l.spec, err = generated.LoadIomonitor()
	if err != nil {
		return fmt.Errorf("failed to load eBPF spec: %w", err)
	}

	// Check if BTF is available
	hasBTF := checkBTFSupport()

	// If BTF is not available, remove fentry/fexit programs from spec
	if !hasBTF {
		fmt.Fprintf(os.Stderr, "BTF not available, removing fentry/fexit programs\n")
		removeFentryPrograms(l.spec)
	}

	// Load eBPF objects
	l.objects = &generated.IomonitorObjects{}
	if err := l.spec.LoadAndAssign(l.objects, nil); err != nil {
		return fmt.Errorf("failed to load eBPF objects: %w", err)
	}

	// Configure cgroup filter if requested.
	if err := l.applyCgroupFilter(); err != nil {
		return fmt.Errorf("failed to apply cgroup filter: %w", err)
	}

	return nil
}

// applyCgroupFilter writes the configured cgroup into the BPF cgroup array.
func (l *Loader) applyCgroupFilter() error {
	if l.objects == nil || l.objects.CgroupFilter == nil {
		return fmt.Errorf("cgroup filter map not available in eBPF objects")
	}

	path := l.cgroupPath
	if path == "" {
		var err error
		path, err = detectBanyanDBCgroupPath()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: cgroup filter disabled (auto-detect failed): %v\n", err)
			return nil
		}
		fmt.Fprintf(os.Stderr, "Auto-detected BanyanDB cgroup: %s\n", path)
	}

	resolved, err := resolveCgroupPath(path)
	if err != nil {
		return err
	}

	fd, err := openCgroupPath(resolved)
	if err != nil {
		return err
	}

	key := uint32(0)
	val := uint32(fd.Fd())
	if err := l.objects.CgroupFilter.Update(key, val, ebpf.UpdateAny); err != nil {
		_ = fd.Close()
		return fmt.Errorf("failed to populate cgroup filter map: %w", err)
	}

	l.cgroupFD = fd
	return nil
}

// removeFentryPrograms removes fentry/fexit programs from the spec.
func removeFentryPrograms(spec *ebpf.CollectionSpec) {
	// Remove fentry/fexit programs that require BTF
	programsToRemove := []string{
		"fentry_ksys_fadvise64_64",
		"fexit_ksys_fadvise64_64",
		"fentry_filemap_get_read_batch",
		"fentry_add_to_page_cache_lru",
	}

	for _, progName := range programsToRemove {
		delete(spec.Programs, progName)
	}
}

// AttachTracepoints attaches the eBPF programs to tracepoints and kprobes.
func (l *Loader) AttachTracepoints() error {
	if l.objects == nil {
		return fmt.Errorf("eBPF objects not loaded")
	}

	// Attach fadvise tracepoints
	if err := l.attachFadviseTracepoints(); err != nil {
		return fmt.Errorf("failed to attach fadvise tracepoints: %w", err)
	}

	// Attach memory tracepoints
	l.attachMemoryTracepoints()

	// Attach cache tracepoints (with fallback to kprobes)
	l.attachCacheTracepoints()

	return nil
}

// attachFadviseTracepoints attaches fadvise-related tracepoints.
func (l *Loader) attachFadviseTracepoints() error {
	// Try tracepoints first
	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", l.objects.TraceEnterFadvise64, nil)
	if err != nil {
		// Fallback to kprobe
		return l.attachFadviseKprobes()
	}
	l.links = append(l.links, tpEnter)

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", l.objects.TraceExitFadvise64, nil)
	if err != nil {
		// Fallback to kprobe
		return l.attachFadviseKprobes()
	}
	l.links = append(l.links, tpExit)

	return nil
}

// attachFadviseKprobes attaches fadvise kprobes as fallback.
func (l *Loader) attachFadviseKprobes() error {
	kpEnter, err := link.Kprobe("ksys_fadvise64_64", l.objects.KprobeKsysFadvise6464, nil)
	if err != nil {
		return fmt.Errorf("failed to attach fadvise kprobe: %w", err)
	}
	l.links = append(l.links, kpEnter)

	kpExit, err := link.Kretprobe("ksys_fadvise64_64", l.objects.KretprobeKsysFadvise6464, nil)
	if err != nil {
		return fmt.Errorf("failed to attach fadvise kretprobe: %w", err)
	}
	l.links = append(l.links, kpExit)

	return nil
}

// attachMemoryTracepoints attaches memory-related tracepoints.
func (l *Loader) attachMemoryTracepoints() {
	// LRU shrink tracepoint
	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", l.objects.TraceLruShrinkInactive, nil)
	if err != nil {
		// This is optional, continue without it
		fmt.Fprintf(os.Stderr, "Warning: failed to attach LRU shrink tracepoint: %v\n", err)
	} else {
		l.links = append(l.links, tpLru)
	}

	// Direct reclaim tracepoint
	tpReclaim, err := link.Tracepoint("vmscan", "mm_vmscan_direct_reclaim_begin", l.objects.TraceDirectReclaimBegin, nil)
	if err != nil {
		// This is optional, continue without it
		fmt.Fprintf(os.Stderr, "Warning: failed to attach direct reclaim tracepoint: %v\n", err)
	} else {
		l.links = append(l.links, tpReclaim)
	}
}

// attachCacheTracepoints attaches cache-related tracepoints with kprobe fallback.
func (l *Loader) attachCacheTracepoints() {
	// Try filemap tracepoints first
	tpReadBatch, err := link.Tracepoint("filemap", "filemap_get_read_batch", l.objects.TraceFilemapGetReadBatch, nil)
	if err != nil {
		// Fallback to kprobe
		kpReadBatch, kpErr := link.Kprobe("filemap_get_read_batch", l.objects.KprobeFilemapGetReadBatch, nil)
		if kpErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to attach read batch probe: %v\n", kpErr)
		} else {
			l.links = append(l.links, kpReadBatch)
		}
	} else {
		l.links = append(l.links, tpReadBatch)
	}

	// Try page cache add tracepoint
	tpPageAdd, err := link.Tracepoint("filemap", "mm_filemap_add_to_page_cache", l.objects.TraceMmFilemapAddToPageCache, nil)
	if err != nil {
		// Fallback to kprobe
		kpPageAdd, kpErr := link.Kprobe("add_to_page_cache_lru", l.objects.KprobeAddToPageCacheLru, nil)
		if kpErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to attach page cache add probe: %v\n", kpErr)
		} else {
			l.links = append(l.links, kpPageAdd)
		}
	} else {
		l.links = append(l.links, tpPageAdd)
	}
}

// GetObjects returns the loaded eBPF objects.
func (l *Loader) GetObjects() *generated.IomonitorObjects {
	return l.objects
}

// Close cleans up all resources.
func (l *Loader) Close() error {
	// Close all links
	for _, lnk := range l.links {
		if err := lnk.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close link: %v\n", err)
		}
	}
	l.links = nil

	// Close eBPF objects
	if l.objects != nil {
		if err := l.objects.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close eBPF objects: %v\n", err)
		}
		l.objects = nil
	}

	// Close cgroup FD if opened
	if l.cgroupFD != nil {
		if err := l.cgroupFD.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close cgroup fd: %v\n", err)
		}
		l.cgroupFD = nil
	}

	return nil
}
