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

// Enhanced loader with tracepoint-based eBPF program attachment.

import (
	"fmt"
	"os"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"
	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
)

// Attachment mode constants for tracking how programs are attached.
const (
	// attachModeTracepoint indicates attachment via tracepoint.
	attachModeTracepoint = "tracepoint"
)

// EnhancedLoader handles loading and managing eBPF programs with intelligent fallback.
type EnhancedLoader struct {
	attachmentModes map[string]string
	spec            *ebpf.CollectionSpec
	objects         *generated.IomonitorObjects
	features        *KernelFeatures
	logger          zerolog.Logger
	cgroupPath      string
	links           []link.Link
	degraded        bool
}

// NewEnhancedLoader creates a new enhanced eBPF program loader.
func NewEnhancedLoader(log zerolog.Logger) (*EnhancedLoader, error) {
	// Remove memory limit for eBPF
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("failed to remove memlock limit: %w", err)
	}

	// Detect kernel features
	features, err := DetectKernelFeatures()
	if err != nil {
		return nil, fmt.Errorf("failed to detect kernel features: %w", err)
	}

	log.Info().
		Str("version", fmt.Sprintf("%d.%d.%d",
			features.KernelVersion.Major,
			features.KernelVersion.Minor,
			features.KernelVersion.Patch)).
		Bool("BTF", features.HasBTF).
		Msg("Detected kernel features")

	return &EnhancedLoader{
		links:           make([]link.Link, 0),
		features:        features,
		logger:          log,
		attachmentModes: make(map[string]string),
		cgroupPath:      "",
	}, nil
}

// SetCgroupPath enables cgroup-based PID filtering using a cgroup v2 path.
// An empty path disables the filter.
func (l *EnhancedLoader) SetCgroupPath(path string) {
	l.cgroupPath = path
}

// LoadPrograms loads the eBPF programs.
func (l *EnhancedLoader) LoadPrograms() error {
	var err error

	// Load the eBPF program collection
	l.spec, err = generated.LoadIomonitor()
	if err != nil {
		return fmt.Errorf("failed to load eBPF spec: %w", err)
	}

	// Load eBPF objects
	l.objects = &generated.IomonitorObjects{}
	if err = l.spec.LoadAndAssign(l.objects, nil); err != nil {
		return fmt.Errorf("failed to load eBPF objects: %w", err)
	}

	// Configure cgroup filter if requested.
	degraded, degradedReason, err := l.ConfigureFilters()
	if err != nil {
		return fmt.Errorf("failed to configure filters: %w", err)
	}
	l.setDegradedState(degraded, degradedReason)

	return nil
}

// ConfigureFilters sets cgroup id for filtering.
// Returns (degraded bool, degradedReason string, error) where degraded=true means cgroup filtering is disabled.
func (l *EnhancedLoader) ConfigureFilters() (bool, string, error) {
	if l.objects == nil {
		return false, "", fmt.Errorf("eBPF objects not loaded")
	}

	return configureFilters(l.objects, l.cgroupPath)
}

// Degraded reports whether KTM is running in comm-only (degraded) mode.
func (l *EnhancedLoader) Degraded() bool {
	return l.degraded
}

// AttachPrograms attaches the eBPF programs with intelligent fallback.
func (l *EnhancedLoader) AttachPrograms() error {
	if l.objects == nil {
		return fmt.Errorf("eBPF objects not loaded")
	}

	// Attach fadvise monitoring
	if err := l.attachFadviseWithFallback(); err != nil {
		l.logger.Error().Err(err).Msg("Failed to attach fadvise monitoring")
		return err
	}

	// Attach read latency monitoring
	if err := l.attachReadLatencyWithFallback(); err != nil {
		l.logger.Error().Err(err).Msg("Failed to attach read latency monitoring")
		return err
	}

	// Attach memory monitoring (optional, continue on failure)
	l.attachMemoryTracepoints()

	// Attach cache monitoring
	l.attachCacheWithFallback()

	// Log attachment summary
	l.logger.Info().Any("attachment_modes", l.attachmentModes).Msg("eBPF programs attached successfully")

	return nil
}

// attachFadviseWithFallback attaches fadvise monitoring using tracepoints.
func (l *EnhancedLoader) attachFadviseWithFallback() error {
	funcName := "fadvise64"

	if l.objects.TraceEnterFadvise64 == nil || l.objects.TraceExitFadvise64 == nil {
		return fmt.Errorf("fadvise tracepoint programs not loaded")
	}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64",
		l.objects.TraceEnterFadvise64, nil)
	if err != nil {
		return fmt.Errorf("failed to attach sys_enter_fadvise64: %w", err)
	}
	l.links = append(l.links, tpEnter)

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64",
		l.objects.TraceExitFadvise64, nil)
	if err != nil {
		// Cleanup enter if exit fails
		tpEnter.Close()
		l.links = l.links[:len(l.links)-1]
		return fmt.Errorf("failed to attach sys_exit_fadvise64: %w", err)
	}
	l.links = append(l.links, tpExit)
	l.attachmentModes[funcName] = attachModeTracepoint
	l.logger.Info().Msg("Attached fadvise monitoring using tracepoints")
	return nil
}

// attachReadLatencyWithFallback attaches tracepoints for read and pread64.
func (l *EnhancedLoader) attachReadLatencyWithFallback() error {
	if err := l.attachSyscallLatency("read",
		l.objects.TraceEnterReadTp, l.objects.TraceExitReadTp); err != nil {
		return err
	}

	return l.attachSyscallLatency("pread64",
		l.objects.TraceEnterPread64Tp, l.objects.TraceExitPread64Tp)
}

func (l *EnhancedLoader) attachSyscallLatency(syscallName string, tpEnterProg, tpExitProg *ebpf.Program) error {
	attachmentKey := "syscall_" + syscallName

	if tpEnterProg == nil || tpExitProg == nil {
		return fmt.Errorf("tracepoint programs for %s not loaded", syscallName)
	}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_"+syscallName, tpEnterProg, nil)
	if err != nil {
		return fmt.Errorf("failed to attach sys_enter_%s: %w", syscallName, err)
	}
	l.links = append(l.links, tpEnter)

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_"+syscallName, tpExitProg, nil)
	if err != nil {
		_ = tpEnter.Close()
		l.links = l.links[:len(l.links)-1]
		return fmt.Errorf("failed to attach sys_exit_%s: %w", syscallName, err)
	}
	l.links = append(l.links, tpExit)
	l.attachmentModes[attachmentKey] = attachModeTracepoint
	l.logger.Info().Str("syscall", syscallName).Msg("Attached syscall latency using tracepoints")
	return nil
}

// attachCacheWithFallback attaches cache monitoring tracepoints.
func (l *EnhancedLoader) attachCacheWithFallback() {
	if l.objects.TraceMmFilemapGetPages != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_get_pages", l.objects.TraceMmFilemapGetPages, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["filemap_lookup"] = attachModeTracepoint
			l.logger.Info().Msg("Attached cache lookup monitoring using tracepoint")
		} else {
			l.logger.Debug().Err(err).Msg("Failed to attach mm_filemap_get_pages")
		}
	}

	if l.objects.TraceMmFilemapAddToPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_add_to_page_cache", l.objects.TraceMmFilemapAddToPageCache, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["page_cache_add"] = attachModeTracepoint
			l.logger.Info().Msg("Attached cache fill monitoring using tracepoint")
		} else {
			l.logger.Debug().Err(err).Msg("Failed to attach mm_filemap_add_to_page_cache")
		}
	}

	if l.objects.TraceMmFilemapDeleteFromPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_delete_from_page_cache", l.objects.TraceMmFilemapDeleteFromPageCache, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["page_cache_delete"] = attachModeTracepoint
			l.logger.Info().Msg("Attached cache delete monitoring using tracepoint")
		} else {
			l.logger.Debug().Err(err).Msg("Failed to attach mm_filemap_delete_from_page_cache")
		}
	}
}

// attachMemoryTracepoints attaches memory-related tracepoints (no fallback needed).
func (l *EnhancedLoader) attachMemoryTracepoints() {
	// LRU shrink tracepoint
	if l.objects.TraceLruShrinkInactive != nil {
		tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive",
			l.objects.TraceLruShrinkInactive, nil)
		if err == nil {
			l.links = append(l.links, tpLru)
			l.attachmentModes["lru_shrink"] = attachModeTracepoint
		}
	}

	// Direct reclaim tracepoint
	if l.objects.TraceDirectReclaimBegin != nil {
		tpReclaim, err := link.Tracepoint("vmscan", "mm_vmscan_direct_reclaim_begin",
			l.objects.TraceDirectReclaimBegin, nil)
		if err == nil {
			l.links = append(l.links, tpReclaim)
			l.attachmentModes["direct_reclaim"] = attachModeTracepoint
		}
	}
}

// GetObjects returns the loaded eBPF objects.
func (l *EnhancedLoader) GetObjects() *generated.IomonitorObjects {
	return l.objects
}

// GetAttachmentModes returns the attachment modes used for each function.
func (l *EnhancedLoader) GetAttachmentModes() map[string]string {
	return l.attachmentModes
}

// GetKernelFeatures returns detected kernel features.
func (l *EnhancedLoader) GetKernelFeatures() *KernelFeatures {
	return l.features
}

func (l *EnhancedLoader) setDegradedState(degraded bool, reason string) {
	if degraded == l.degraded {
		return
	}
	l.degraded = degraded
	if degraded {
		l.logger.Warn().
			Str("cgroup_path", l.cgroupPath).
			Str("reason", reason).
			Msg("KTM is running in Degraded mode (comm-only) due to Cgroup detection failure. Data isolation in K8s pods cannot be guaranteed.")
		return
	}
	l.logger.Info().Msg("KTM recovered: cgroup scoping restored")
}

// Close cleans up all resources.
func (l *EnhancedLoader) Close() error {
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
