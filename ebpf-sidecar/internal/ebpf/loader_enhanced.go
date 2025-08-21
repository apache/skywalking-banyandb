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

// Enhanced loader with fentry/fexit support and intelligent fallback

import (
	"fmt"
	"os"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"
	"go.uber.org/zap"
	
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/ebpf/generated"
)

// EnhancedLoader handles loading and managing eBPF programs with intelligent fallback
type EnhancedLoader struct {
	spec     *ebpf.CollectionSpec
	objects  *generated.IomonitorObjects
	links    []link.Link
	features *KernelFeatures
	logger   *zap.Logger
	
	// Track attachment modes for monitoring
	attachmentModes map[string]string // function -> mode (fentry/tracepoint/kprobe)
}

// NewEnhancedLoader creates a new enhanced eBPF program loader
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
	}, nil
}

// LoadPrograms loads the eBPF programs
func (l *EnhancedLoader) LoadPrograms() error {
	var err error

	// Load the eBPF program collection
	l.spec, err = generated.LoadIomonitor()
	if err != nil {
		return fmt.Errorf("failed to load eBPF spec: %w", err)
	}

	// Load eBPF objects
	l.objects = &generated.IomonitorObjects{}
	if err := l.spec.LoadAndAssign(l.objects, nil); err != nil {
		return fmt.Errorf("failed to load eBPF objects: %w", err)
	}

	return nil
}

// AttachPrograms attaches the eBPF programs with intelligent fallback
func (l *EnhancedLoader) AttachPrograms() error {
	if l.objects == nil {
		return fmt.Errorf("eBPF objects not loaded")
	}

	// Attach fadvise monitoring
	if err := l.attachFadviseWithFallback(); err != nil {
		l.logger.Error("Failed to attach fadvise monitoring", zap.Error(err))
		return err
	}

	// Attach memory monitoring (optional, continue on failure)
	if err := l.attachMemoryTracepoints(); err != nil {
		l.logger.Warn("Failed to attach memory monitoring (non-critical)", zap.Error(err))
	}

	// Attach cache monitoring
	if err := l.attachCacheWithFallback(); err != nil {
		l.logger.Warn("Failed to attach cache monitoring (non-critical)", zap.Error(err))
	}

	// Log attachment summary
	l.logger.Info("eBPF programs attached successfully",
		zap.Any("attachment_modes", l.attachmentModes))

	return nil
}

// attachFadviseWithFallback tries fentry -> tracepoint -> kprobe
func (l *EnhancedLoader) attachFadviseWithFallback() error {
	funcName := "fadvise64"
	
	// Try fentry/fexit first (best performance)
	if l.features.HasFentry {
		if l.objects.FentryKsysFadvise6464 != nil && l.objects.FexitKsysFadvise6464 != nil {
			fentryLink, err := link.AttachTracing(link.TracingOptions{
				Program: l.objects.FentryKsysFadvise6464,
			})
			if err == nil {
				l.links = append(l.links, fentryLink)
				
				fexitLink, err := link.AttachTracing(link.TracingOptions{
					Program: l.objects.FexitKsysFadvise6464,
				})
				if err == nil {
					l.links = append(l.links, fexitLink)
					l.attachmentModes[funcName] = "fentry/fexit"
					l.logger.Info("Attached fadvise monitoring using fentry/fexit")
					return nil
				}
				// Cleanup fentry if fexit fails
				fentryLink.Close()
				l.links = l.links[:len(l.links)-1]
			}
			l.logger.Debug("fentry/fexit attachment failed, trying tracepoint", zap.Error(err))
		}
	}

	// Try tracepoints (stable API)
	if l.objects.TraceEnterFadvise64 != nil && l.objects.TraceExitFadvise64 != nil {
		tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", 
			l.objects.TraceEnterFadvise64, nil)
		if err == nil {
			l.links = append(l.links, tpEnter)
			
			tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", 
				l.objects.TraceExitFadvise64, nil)
			if err == nil {
				l.links = append(l.links, tpExit)
				l.attachmentModes[funcName] = "tracepoint"
				l.logger.Info("Attached fadvise monitoring using tracepoints")
				return nil
			}
			// Cleanup enter if exit fails
			tpEnter.Close()
			l.links = l.links[:len(l.links)-1]
		}
		l.logger.Debug("Tracepoint attachment failed, trying kprobe", zap.Error(err))
	}

	// Fallback to kprobes with multiple symbol attempts
	return l.attachFadviseKprobesWithSymbolFallback()
}

// attachFadviseKprobesWithSymbolFallback tries multiple kernel symbols
func (l *EnhancedLoader) attachFadviseKprobesWithSymbolFallback() error {
	symbolNames := GetFadviseFunctionNames(l.features.KernelVersion)
	
	for _, symbol := range symbolNames {
		l.logger.Debug("Trying kprobe attachment", zap.String("symbol", symbol))
		
		if l.objects.KprobeKsysFadvise6464 == nil || l.objects.KretprobeKsysFadvise6464 == nil {
			continue
		}
		
		kpEnter, err := link.Kprobe(symbol, l.objects.KprobeKsysFadvise6464, nil)
		if err != nil {
			l.logger.Debug("Kprobe attachment failed", 
				zap.String("symbol", symbol), 
				zap.Error(err))
			continue
		}
		
		kpExit, err := link.Kretprobe(symbol, l.objects.KretprobeKsysFadvise6464, nil)
		if err != nil {
			kpEnter.Close()
			l.logger.Debug("Kretprobe attachment failed", 
				zap.String("symbol", symbol), 
				zap.Error(err))
			continue
		}
		
		// Success!
		l.links = append(l.links, kpEnter, kpExit)
		l.attachmentModes["fadvise64"] = fmt.Sprintf("kprobe/%s", symbol)
		l.logger.Info("Attached fadvise monitoring using kprobe", 
			zap.String("symbol", symbol))
		return nil
	}
	
	return fmt.Errorf("failed to attach fadvise monitoring: tried %d symbols", len(symbolNames))
}

// attachCacheWithFallback tries fentry -> tracepoint -> kprobe for cache monitoring
func (l *EnhancedLoader) attachCacheWithFallback() error {
	// Try fentry first for filemap operations
	if l.features.HasFentry && l.objects.FentryFilemapGetReadBatch != nil {
		fentryLink, err := link.AttachTracing(link.TracingOptions{
			Program: l.objects.FentryFilemapGetReadBatch,
		})
		if err == nil {
			l.links = append(l.links, fentryLink)
			l.attachmentModes["filemap_read"] = "fentry"
			l.logger.Info("Attached cache read monitoring using fentry")
		}
	}
	
	// If fentry failed, try tracepoint
	if _, ok := l.attachmentModes["filemap_read"]; !ok {
		if l.objects.TraceFilemapGetReadBatch != nil {
			tp, err := link.Tracepoint("filemap", "filemap_get_read_batch", 
				l.objects.TraceFilemapGetReadBatch, nil)
			if err == nil {
				l.links = append(l.links, tp)
				l.attachmentModes["filemap_read"] = "tracepoint"
				l.logger.Info("Attached cache read monitoring using tracepoint")
			}
		}
	}
	
	// If both failed, try kprobe with multiple symbols
	if _, ok := l.attachmentModes["filemap_read"]; !ok {
		readFuncs, _ := GetFilemapFunctionNames(l.features.KernelVersion)
		for _, symbol := range readFuncs {
			if l.objects.KprobeFilemapGetReadBatch != nil {
				kp, err := link.Kprobe(symbol, l.objects.KprobeFilemapGetReadBatch, nil)
				if err == nil {
					l.links = append(l.links, kp)
					l.attachmentModes["filemap_read"] = fmt.Sprintf("kprobe/%s", symbol)
					l.logger.Info("Attached cache read monitoring using kprobe", 
						zap.String("symbol", symbol))
					break
				}
			}
		}
	}
	
	// Similar logic for page cache add operations
	if l.features.HasFentry && l.objects.FentryAddToPageCacheLru != nil {
		fentryLink, err := link.AttachTracing(link.TracingOptions{
			Program: l.objects.FentryAddToPageCacheLru,
		})
		if err == nil {
			l.links = append(l.links, fentryLink)
			l.attachmentModes["page_cache_add"] = "fentry"
			l.logger.Info("Attached page cache add monitoring using fentry")
			return nil
		}
	}
	
	// Fallback to tracepoint and then kprobe
	if l.objects.TraceMmFilemapAddToPageCache != nil {
		tp, err := link.Tracepoint("filemap", "mm_filemap_add_to_page_cache", 
			l.objects.TraceMmFilemapAddToPageCache, nil)
		if err == nil {
			l.links = append(l.links, tp)
			l.attachmentModes["page_cache_add"] = "tracepoint"
			l.logger.Info("Attached page cache add monitoring using tracepoint")
			return nil
		}
	}
	
	// Final fallback to kprobe
	_, addFuncs := GetFilemapFunctionNames(l.features.KernelVersion)
	for _, symbol := range addFuncs {
		if l.objects.KprobeAddToPageCacheLru != nil {
			kp, err := link.Kprobe(symbol, l.objects.KprobeAddToPageCacheLru, nil)
			if err == nil {
				l.links = append(l.links, kp)
				l.attachmentModes["page_cache_add"] = fmt.Sprintf("kprobe/%s", symbol)
				l.logger.Info("Attached page cache add monitoring using kprobe", 
					zap.String("symbol", symbol))
				return nil
			}
		}
	}
	
	return nil // Cache monitoring is optional
}

// attachMemoryTracepoints attaches memory-related tracepoints (no fallback needed)
func (l *EnhancedLoader) attachMemoryTracepoints() error {
	// LRU shrink tracepoint
	if l.objects.TraceLruShrinkInactive != nil {
		tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", 
			l.objects.TraceLruShrinkInactive, nil)
		if err == nil {
			l.links = append(l.links, tpLru)
			l.attachmentModes["lru_shrink"] = "tracepoint"
		}
	}

	// Direct reclaim tracepoint
	if l.objects.TraceDirectReclaimBegin != nil {
		tpReclaim, err := link.Tracepoint("vmscan", "mm_vmscan_direct_reclaim_begin", 
			l.objects.TraceDirectReclaimBegin, nil)
		if err == nil {
			l.links = append(l.links, tpReclaim)
			l.attachmentModes["direct_reclaim"] = "tracepoint"
		}
	}

	return nil
}

// GetObjects returns the loaded eBPF objects
func (l *EnhancedLoader) GetObjects() *generated.IomonitorObjects {
	return l.objects
}

// GetAttachmentModes returns the attachment modes used for each function
func (l *EnhancedLoader) GetAttachmentModes() map[string]string {
	return l.attachmentModes
}

// GetKernelFeatures returns detected kernel features
func (l *EnhancedLoader) GetKernelFeatures() *KernelFeatures {
	return l.features
}

// Close cleans up all resources
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