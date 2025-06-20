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

// Package protector provides a set of protectors that stop the query services when the resource usage exceeds the limit.
package protector

import (
	"context"
	"errors"
	"fmt"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var scope = observability.RootScope.SubScope("memory_protector")

// Memory is an interface for monitoring and limiting memory usage to prevent OOM.
type Memory interface {
	AvailableBytes() int64
	GetLimit() uint64
	AcquireResource(ctx context.Context, size uint64) error
	ShouldApplyFadvis(fileSize int64) bool
	ShouldCache(path string) bool
	run.PreRunner
	run.Config
	run.Service
}

var _ Memory = (*memory)(nil)

// Memory is a protector that stops the query services when the memory usage exceeds the limit.
type memory struct {
	omr            observability.MetricsRegistry
	limitGauge     meter.Gauge
	usageGauge     meter.Gauge
	l              *logger.Logger
	closed         chan struct{}
	blockedChan    chan struct{}
	allowedPercent int
	allowedBytes   run.Bytes
	limit          atomic.Uint64
	usage          uint64
}

// NewMemory creates a new Memory protector.
func NewMemory(omr observability.MetricsRegistry) Memory {
	queueSize := cgroups.CPUs()
	factory := omr.With(scope)

	return &memory{
		omr:         omr,
		blockedChan: make(chan struct{}, queueSize),
		closed:      make(chan struct{}),

		limitGauge: factory.NewGauge("limit"),
		usageGauge: factory.NewGauge("usage"),
	}
}

// AcquireResource attempts to acquire a `size` amount of memory.
func (m *memory) AcquireResource(ctx context.Context, size uint64) error {
	if m.limit.Load() == 0 {
		return nil
	}
	start := time.Now()

	select {
	case m.blockedChan <- struct{}{}:
		defer func() { <-m.blockedChan }()
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting for blocked queue slot: %w", ctx.Err())
	}

	for {
		currentUsage := atomic.LoadUint64(&m.usage)
		if currentUsage+size <= m.limit.Load() {
			return nil
		}

		select {
		case <-time.After(100 * time.Millisecond):
			continue
		case <-ctx.Done():
			return fmt.Errorf(
				"context canceled: memory acquisition failed (currentUsage: %d, limit: %d, size: %d, blockedDuration: %v): %w",
				currentUsage, m.limit.Load(), size, time.Since(start), ctx.Err(),
			)
		}
	}
}

// GetLimit returns the memory limit of the protector.
func (m *memory) GetLimit() uint64 {
	return m.limit.Load()
}

// AvailableBytes returns the available memory (limit - usage).
func (m *memory) AvailableBytes() int64 {
	if m.limit.Load() == 0 {
		return -1
	}
	usage := atomic.LoadUint64(&m.usage)
	if usage >= m.limit.Load() {
		return 0
	}
	return int64(m.limit.Load() - usage)
}

// Name returns the name of the protector.
func (m *memory) Name() string {
	return "memory-protector"
}

// FlagSet returns the flag set for the protector.
func (m *memory) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet(m.Name())
	flagS.IntVarP(&m.allowedPercent, "allowed-percent", "", 75,
		"Allowed percentage of total memory usage. If usage exceeds this value, the query services will stop. "+
			"This takes effect only if `allowed-bytes` is 0. If usage is too high, it may cause OS page cache eviction.")
	flagS.VarP(&m.allowedBytes, "allowed-bytes", "", "Allowed bytes of memory usage. If the memory usage exceeds this value, the query services will stop. "+
		"Setting a large value may evict data from the OS page cache, causing high disk I/O.")
	return flagS
}

// Validate validates the protector's flags.
func (m *memory) Validate() error {
	if m.allowedPercent <= 0 || m.allowedPercent > 100 {
		if m.allowedBytes <= 0 {
			return errors.New("allowed-bytes must be greater than 0")
		}
		return errors.New("allowed-percent must be in the range (0, 100]")
	}
	return nil
}

// PreRun initializes the protector.
func (m *memory) PreRun(context.Context) error {
	m.l = logger.GetLogger(m.Name())
	if m.allowedBytes > 0 {
		m.limit.Store(uint64(m.allowedBytes))
		m.l.Info().
			Str("limit", humanize.Bytes(m.limit.Load())).
			Msg("memory protector enabled")
	} else {
		cgLimit, err := cgroups.MemoryLimit()
		if err != nil {
			m.l.Warn().Err(err).Msg("failed to get memory limit from cgroups, disable memory protector")
			return nil
		}
		if cgLimit <= 0 || cgLimit > 1e18 {
			m.l.Warn().Int64("cgroup_memory_limit", cgLimit).Msg("cgroup memory limit is invalid, disable memory protector")
			return nil
		}
		m.limit.Store(uint64(cgLimit) * uint64(m.allowedPercent) / 100)
		m.l.Info().
			Str("limit", humanize.Bytes(m.limit.Load())).
			Str("cgroup_limit", humanize.Bytes(uint64(cgLimit))).
			Int("percent", m.allowedPercent).
			Msg("memory protector enabled")
	}
	m.limitGauge.Set(float64(m.limit.Load()))
	return nil
}

// GracefulStop stops the protector.
func (m *memory) GracefulStop() {
	close(m.closed)
}

// Serve starts the protector.
func (m *memory) Serve() run.StopNotify {
	if m.limit.Load() == 0 {
		return m.closed
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-m.closed:
				return
			case <-ticker.C:
				samples := []metrics.Sample{
					{Name: "/memory/classes/heap/objects:bytes"},
					{Name: "/memory/classes/heap/stacks:bytes"},
					{Name: "/memory/classes/metadata/mcache/inuse:bytes"},
					{Name: "/memory/classes/metadata/mspan/inuse:bytes"},
					{Name: "/memory/classes/metadata/other:bytes"},
					{Name: "/memory/classes/os-stacks:bytes"},
					{Name: "/memory/classes/other:bytes"},
				}
				metrics.Read(samples)
				var usedBytes uint64
				for _, sample := range samples {
					usedBytes += sample.Value.Uint64()
				}

				atomic.StoreUint64(&m.usage, usedBytes)

				if usedBytes > m.limit.Load() {
					m.l.Warn().Str("used", humanize.Bytes(usedBytes)).Str("limit", humanize.Bytes(m.limit.Load())).Msg("memory usage exceeds limit")
				}
			}
		}
	}()
	return m.closed
}

// GetThreshold returns the threshold for large file detection (1% of page cache).
func (m *memory) GetThreshold() int64 {
	// Try reading cgroup memory limit
	cgLimit, err := cgroups.MemoryLimit()
	if err != nil {
		m.l.Warn().Err(err).Msg("failed to get memory limit from cgroups, using default threshold")
		// Fallback default threshold of 64MB
		return 64 << 20
	}

	// Determine effective memory to use based on flags
	var totalMemory int64
	if m.allowedBytes > 0 {
		totalMemory = cgLimit - int64(m.allowedBytes)
	} else {
		totalMemory = cgLimit * int64(m.allowedPercent) / 100
	}

	// Compute 1% of that memory as page cache threshold
	threshold := totalMemory / 100
	const minThreshold = 10 << 20 // 10MB
	if threshold < minThreshold {
		threshold = minThreshold
	}
	return threshold
}

// ShouldApplyFadvis implements the fs.ThresholdProvider interface.
// It checks if the file size exceeds the threshold for large file detection.
func (m *memory) ShouldApplyFadvis(fileSize int64) bool {
	return fileSize >= m.GetThreshold()
}

// ShouldCache returns whether a file at the given path should be cached.
// Currently always returns true as the cache decision is made based on file size.
func (m *memory) ShouldCache(_ string) bool {
	return true
}

// Global memory protector instance used by components that need threshold decisions.
var (
	globalMemoryProtector atomic.Pointer[Memory]
	globalMemoryOnce      sync.Once
)

// GetMemoryProtector returns the global memory protector instance.
// If no instance is set, it creates a default one for threshold decisions.
func GetMemoryProtector() Memory {
	// Fast path: atomic load
	if mp := globalMemoryProtector.Load(); mp != nil {
		return *mp
	}

	// Slow path: initialize once
	globalMemoryOnce.Do(func() {
		mp := Memory(&memory{
			allowedPercent: 75, // Default 75% threshold
		})
		globalMemoryProtector.Store(&mp)
	})

	return *globalMemoryProtector.Load()
}

// SetMemoryProtector sets the global memory protector instance.
func SetMemoryProtector(mp Memory) {
	globalMemoryProtector.Store(&mp)
}
