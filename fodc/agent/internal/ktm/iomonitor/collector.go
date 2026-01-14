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

// Package iomonitor implements I/O monitoring using eBPF.
package iomonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/metrics"
)

// CollectorConfig defines the collector configuration.
// Field order chosen to minimize padding (large -> small).
type CollectorConfig struct {
	EBPF     EBPFConfig    `mapstructure:"ebpf"`
	Modules  []string      `mapstructure:"modules"`
	Interval time.Duration `mapstructure:"interval"`
}

// EBPFConfig defines eBPF-specific configuration.
type EBPFConfig struct {
	CgroupPath string `mapstructure:"cgroup_path"` // Optional cgroup v2 path to filter PIDs
}

// Collector manages eBPF program lifecycle and metrics collection.
type Collector struct {
	logger   zerolog.Logger
	modules  map[string]Module
	metrics  *metrics.Store
	ticker   *time.Ticker
	stopChan chan struct{}
	config   CollectorConfig
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

// Module represents an eBPF monitoring module.
type Module interface {
	Name() string
	Start() error
	Stop() error
	Collect() (*metrics.MetricSet, error)
}

// New creates a new collector instance.
func New(cfg CollectorConfig, log zerolog.Logger) (*Collector, error) {
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("collector interval must be positive, got %v", cfg.Interval)
	}

	c := &Collector{
		config:   cfg,
		logger:   log,
		modules:  make(map[string]Module),
		metrics:  metrics.NewStore(),
		stopChan: make(chan struct{}),
	}

	// Initialize modules based on configuration
	for _, moduleName := range cfg.Modules {
		module, err := c.createModule(moduleName, cfg.EBPF)
		if err != nil {
			return nil, fmt.Errorf("failed to create module %s: %w", moduleName, err)
		}
		c.modules[moduleName] = module
	}

	return c, nil
}

// createModule creates a module instance by name.
func (c *Collector) createModule(name string, ebpfCfg EBPFConfig) (Module, error) {
	switch name {
	case "iomonitor":
		// Create the comprehensive I/O monitor module
		module, err := newModule(c.logger, ebpfCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create iomonitor module: %w", err)
		}
		return module, nil
	case "fadvise", "memory", "cache":
		// These are all handled by iomonitor now
		c.logger.Warn().Str("module", name).Msg("Module is deprecated, use 'iomonitor' instead")
		return nil, fmt.Errorf("module %s is deprecated, use 'iomonitor' instead", name)
	default:
		return nil, fmt.Errorf("unknown module: %s", name)
	}
}

// Start starts the collector.
func (c *Collector) Start(ctx context.Context) error {
	c.logger.Info().Msg("Starting collector")

	// Start all modules
	for name, module := range c.modules {
		if err := module.Start(); err != nil {
			return fmt.Errorf("failed to start module %s: %w", name, err)
		}
		c.logger.Info().Str("module", name).Msg("Started module")
	}

	// Start collection ticker
	c.ticker = time.NewTicker(c.config.Interval)
	c.wg.Add(1)
	go c.collectLoop(ctx)

	return nil
}

// collectLoop runs the collection loop.
func (c *Collector) collectLoop(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-c.ticker.C:
			c.Collect()
		}
	}
}

// Collect collects metrics from all modules.
func (c *Collector) Collect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name, module := range c.modules {
		metricSet, err := module.Collect()
		if err != nil {
			c.logger.Error().Str("module", name).Err(err).Msg("Failed to collect metrics")
			continue
		}

		// Store metrics
		c.metrics.Update(name, metricSet)
		c.logger.Debug().Str("module", name).Int("count", metricSet.Count()).Msg("Collected metrics")
	}
}

// GetMetrics returns current metrics.
func (c *Collector) GetMetrics() *metrics.Store {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metrics
}

// IsDegraded returns whether any module is running in degraded mode.
func (c *Collector) IsDegraded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, mod := range c.modules {
		if ioMod, ok := mod.(interface{ Degraded() bool }); ok {
			if ioMod.Degraded() {
				return true
			}
		}
	}
	return false
}

// Close stops the collector and cleans up resources.
func (c *Collector) Close() error {
	c.logger.Info().Msg("Stopping collector")

	// Stop ticker
	if c.ticker != nil {
		c.ticker.Stop()
	}

	// Signal stop
	close(c.stopChan)

	// Wait for collection loop to finish
	c.wg.Wait()

	// Stop all modules
	for name, module := range c.modules {
		if err := module.Stop(); err != nil {
			c.logger.Error().Str("module", name).Err(err).Msg("Failed to stop module")
		}
	}

	return nil
}
