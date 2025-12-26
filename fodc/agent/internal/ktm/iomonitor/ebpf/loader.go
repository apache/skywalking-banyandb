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

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
)

// Loader handles loading and managing eBPF programs.
type Loader struct {
	spec    *ebpf.CollectionSpec
	objects *generated.IomonitorObjects
	links   []link.Link
	cgroupPath string
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

// SetCgroupPath configures the target cgroup path for filtering.
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

	return nil
}

// ConfigureFilters populates the cgroup id and allowed pid map.
func (l *Loader) ConfigureFilters(targetComm string) error {
	if l.objects == nil {
		return fmt.Errorf("eBPF objects not loaded")
	}
	return configureFilters(l.objects, l.cgroupPath, targetComm)
}

// RefreshAllowedPIDs updates the allowed pid map using the given comm prefix.
func (l *Loader) RefreshAllowedPIDs(prefix string) error {
	if l.objects == nil {
		return fmt.Errorf("eBPF objects not loaded")
	}
	return refreshAllowedPIDs(l.objects, prefix)
}

// removeFentryPrograms removes fentry/fexit programs from the spec.
func removeFentryPrograms(spec *ebpf.CollectionSpec) {
	// Remove fentry/fexit programs that require BTF
	programsToRemove := []string{
		"fentry_read",
		"fexit_read",
		"fentry_pread64",
		"fexit_pread64",
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

	// Attach read/pread latency
	if err := l.attachReadLatency(); err != nil {
		return fmt.Errorf("failed to attach read latency probes: %w", err)
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

// attachReadLatency attaches read/pread latency probes with fentry preference.
func (l *Loader) attachReadLatency() error {
	if err := l.attachSyscallLatency("read",
		l.objects.FentryRead, l.objects.FexitRead,
		l.objects.TraceEnterRead, l.objects.TraceExitRead,
	); err != nil {
		return err
	}

	if err := l.attachSyscallLatency("pread64",
		l.objects.FentryPread64, l.objects.FexitPread64,
		l.objects.TraceEnterPread64, l.objects.TraceExitPread64,
	); err != nil {
		return err
	}

	return nil
}

func (l *Loader) attachSyscallLatency(syscallName string, fentryProg, fexitProg, tpEnterProg, tpExitProg *ebpf.Program) error {
	// Prefer fentry/fexit when available
	if fentryProg != nil && fexitProg != nil {
		fentry, err := link.AttachTracing(link.TracingOptions{
			Program: fentryProg,
		})
		if err == nil {
			l.links = append(l.links, fentry)

			fexit, errExit := link.AttachTracing(link.TracingOptions{
				Program: fexitProg,
			})
			if errExit == nil {
				l.links = append(l.links, fexit)
				return nil
			}

			_ = fentry.Close()
			l.links = l.links[:len(l.links)-1]
		}
	}

	// Fallback to tracepoints
	if tpEnterProg == nil || tpExitProg == nil {
		return fmt.Errorf("no programs available for syscall %s", syscallName)
	}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_"+syscallName, tpEnterProg, nil)
	if err != nil {
		return fmt.Errorf("failed to attach sys_enter_%s tracepoint: %w", syscallName, err)
	}
	l.links = append(l.links, tpEnter)

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_"+syscallName, tpExitProg, nil)
	if err != nil {
		_ = tpEnter.Close()
		l.links = l.links[:len(l.links)-1]
		return fmt.Errorf("failed to attach sys_exit_%s tracepoint: %w", syscallName, err)
	}
	l.links = append(l.links, tpExit)

	return nil
}

// attachFadviseTracepoints attaches fadvise-related tracepoints.
func (l *Loader) attachFadviseTracepoints() error {
	// Try tracepoints next
	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", l.objects.TraceEnterFadvise64, nil)
	if err != nil {
		return fmt.Errorf("failed to attach sys_enter_fadvise64 tracepoint: %w", err)
	}
	l.links = append(l.links, tpEnter)

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", l.objects.TraceExitFadvise64, nil)
	if err != nil {
		_ = tpEnter.Close()
		l.links = l.links[:len(l.links)-1]
		return fmt.Errorf("failed to attach sys_exit_fadvise64 tracepoint: %w", err)
	}
	l.links = append(l.links, tpExit)

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

// attachCacheTracepoints attaches cache-related tracepoints.
func (l *Loader) attachCacheTracepoints() {
	// mm_filemap_get_pages
	if l.objects.TraceMmFilemapGetPages != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_get_pages", l.objects.TraceMmFilemapGetPages, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to attach mm_filemap_get_pages tracepoint: %v\n", err)
		} else {
			l.links = append(l.links, tp)
		}
	}

	// mm_filemap_add_to_page_cache
	if l.objects.TraceMmFilemapAddToPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_add_to_page_cache", l.objects.TraceMmFilemapAddToPageCache, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to attach mm_filemap_add_to_page_cache tracepoint: %v\n", err)
		} else {
			l.links = append(l.links, tp)
		}
	}

	// mm_filemap_delete_from_page_cache
	if l.objects.TraceMmFilemapDeleteFromPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_delete_from_page_cache", l.objects.TraceMmFilemapDeleteFromPageCache, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to attach mm_filemap_delete_from_page_cache tracepoint: %v\n", err)
		} else {
			l.links = append(l.links, tp)
		}
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

	return nil
}
