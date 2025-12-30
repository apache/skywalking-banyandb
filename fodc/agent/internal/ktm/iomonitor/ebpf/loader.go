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

// Enhanced loader with fentry/fexit support and intelligent fallback.

import (
	"fmt"
	"os"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"
	"go.uber.org/zap"

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
	logger          *zap.Logger
	cgroupPath      string
	links           []link.Link
	degraded        bool
}

// NewEnhancedLoader creates a new enhanced eBPF program loader.
func NewEnhancedLoader(logger *zap.Logger) (*EnhancedLoader, error) {
	// Remove memory limit for eBPF
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("failed to remove memlock limit: %w", err)
	}

	// Detect kernel features
	features, err := DetectKernelFeatures()
	if err != nil {
		return nil, fmt.Errorf("failed to detect kernel features: %w", err)
	}

	logger.Info("Detected kernel features",
		zap.String("version", fmt.Sprintf("%d.%d.%d",
			features.KernelVersion.Major,
			features.KernelVersion.Minor,
			features.KernelVersion.Patch)),
		zap.Bool("BTF", features.HasBTF),
		zap.Bool("fentry", features.HasFentry),
		zap.String("best_mode", AttachmentModeString(features)),
	)

	return &EnhancedLoader{
		links:           make([]link.Link, 0),
		features:        features,
		logger:          logger,
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
		l.logger.Warn("Failed to configure filters", zap.Error(err))
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
		l.logger.Error("Failed to attach fadvise monitoring", zap.Error(err))
		return err
	}

	// Attach read latency monitoring
	if err := l.attachReadLatencyWithFallback(); err != nil {
		l.logger.Error("Failed to attach read latency monitoring", zap.Error(err))
		return err
	}

	// Attach memory monitoring (optional, continue on failure)
	l.attachMemoryTracepoints()

	// Attach cache monitoring
	l.attachCacheWithFallback()

	// Log attachment summary
	l.logger.Info("eBPF programs attached successfully",
		zap.Any("attachment_modes", l.attachmentModes))

	return nil
}

// attachFadviseWithFallback tries fentry -> tracepoint -> kprobe.
func (l *EnhancedLoader) attachFadviseWithFallback() error {
	funcName := "fadvise64"

	// Try tracepoints (stable API)
	if l.objects.TraceEnterFadvise64 != nil && l.objects.TraceExitFadvise64 != nil {
		tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64",
			l.objects.TraceEnterFadvise64, nil)
		if err == nil {
			l.links = append(l.links, tpEnter)

			tpExit, exitErr := link.Tracepoint("syscalls", "sys_exit_fadvise64",
				l.objects.TraceExitFadvise64, nil)
			if exitErr == nil {
				l.links = append(l.links, tpExit)
				l.attachmentModes[funcName] = attachModeTracepoint
				l.logger.Info("Attached fadvise monitoring using tracepoints")
				return nil
			}
			// Cleanup enter if exit fails
			tpEnter.Close()
			l.links = l.links[:len(l.links)-1]
		}
		l.logger.Debug("Tracepoint attachment failed, trying kprobe", zap.Error(err))
	}

	return fmt.Errorf("failed to attach fadvise monitoring")
}

// attachReadLatencyWithFallback tries fentry -> tracepoint for read and pread64.
func (l *EnhancedLoader) attachReadLatencyWithFallback() error {
	if err := l.attachSyscallLatencyWithFallback("read",
		l.objects.FentryRead, l.objects.FexitRead,
		l.objects.TraceEnterReadTp, l.objects.TraceExitReadTp); err != nil {
		return err
	}

	return l.attachSyscallLatencyWithFallback("pread64",
		l.objects.FentryPread64, l.objects.FexitPread64,
		l.objects.TraceEnterPread64Tp, l.objects.TraceExitPread64Tp)
}

func (l *EnhancedLoader) attachSyscallLatencyWithFallback(syscallName string, fentryProg, fexitProg, tpEnterProg, tpExitProg *ebpf.Program) error {
	attachmentKey := "syscall_" + syscallName

	if l.features.HasFentry && fentryProg != nil && fexitProg != nil {
		fentryLink, err := link.AttachTracing(link.TracingOptions{
			Program: fentryProg,
		})
		if err == nil {
			fexitLink, exitErr := link.AttachTracing(link.TracingOptions{
				Program: fexitProg,
			})
			if exitErr == nil {
				l.links = append(l.links, fentryLink, fexitLink)
				l.attachmentModes[attachmentKey] = "fentry/fexit"
				l.logger.Info("Attached syscall latency using fentry/fexit", zap.String("syscall", syscallName))
				return nil
			}
			_ = fentryLink.Close()
			l.logger.Debug("fexit attachment failed, falling back to tracepoint",
				zap.String("syscall", syscallName),
				zap.Error(exitErr))
		} else {
			l.logger.Debug("fentry attachment failed, falling back to tracepoint",
				zap.String("syscall", syscallName),
				zap.Error(err))
		}
	}

	if tpEnterProg != nil && tpExitProg != nil {
		tpEnter, err := link.Tracepoint("syscalls", "sys_enter_"+syscallName, tpEnterProg, nil)
		if err == nil {
			tpExit, exitErr := link.Tracepoint("syscalls", "sys_exit_"+syscallName, tpExitProg, nil)
			if exitErr == nil {
				l.links = append(l.links, tpEnter, tpExit)
				l.attachmentModes[attachmentKey] = attachModeTracepoint
				l.logger.Info("Attached syscall latency using tracepoints", zap.String("syscall", syscallName))
				return nil
			}
			_ = tpEnter.Close()
			l.logger.Debug("Tracepoint exit attachment failed",
				zap.String("syscall", syscallName),
				zap.Error(exitErr))
		} else {
			l.logger.Debug("Tracepoint enter attachment failed",
				zap.String("syscall", syscallName),
				zap.Error(err))
		}
	}

	return fmt.Errorf("failed to attach latency probes for syscall %s", syscallName)
}

// attachCacheWithFallback attaches cache monitoring tracepoints.
func (l *EnhancedLoader) attachCacheWithFallback() {
	if l.objects.TraceMmFilemapGetPages != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_get_pages", l.objects.TraceMmFilemapGetPages, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["filemap_lookup"] = attachModeTracepoint
			l.logger.Info("Attached cache lookup monitoring using tracepoint")
		} else {
			l.logger.Debug("Failed to attach mm_filemap_get_pages", zap.Error(err))
		}
	}

	if l.objects.TraceMmFilemapAddToPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_add_to_page_cache", l.objects.TraceMmFilemapAddToPageCache, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["page_cache_add"] = attachModeTracepoint
			l.logger.Info("Attached cache fill monitoring using tracepoint")
		} else {
			l.logger.Debug("Failed to attach mm_filemap_add_to_page_cache", zap.Error(err))
		}
	}

	if l.objects.TraceMmFilemapDeleteFromPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_delete_from_page_cache", l.objects.TraceMmFilemapDeleteFromPageCache, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["page_cache_delete"] = attachModeTracepoint
			l.logger.Info("Attached cache delete monitoring using tracepoint")
		} else {
			l.logger.Debug("Failed to attach mm_filemap_delete_from_page_cache", zap.Error(err))
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
		l.logger.Warn("KTM degraded: failed to resolve target cgroup, falling back to comm-only",
			zap.String("cgroup_path", l.cgroupPath),
			zap.String("reason", reason),
			zap.String("risk", "metrics may mix across pods if multiple processes match comm"))
		return
	}
	l.logger.Info("KTM recovered: cgroup scoping restored")
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
