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

package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/config"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/metrics"
)

// Collector manages eBPF program lifecycle and metrics collection.
type Collector struct {
	logger   *zap.Logger
	modules  map[string]Module
	metrics  *metrics.Store
	ticker   *time.Ticker
	stopChan chan struct{}
	config   config.CollectorConfig
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
func New(cfg config.CollectorConfig, logger *zap.Logger) (*Collector, error) {
	c := &Collector{
		config:   cfg,
		logger:   logger,
		modules:  make(map[string]Module),
		metrics:  metrics.NewStore(),
		stopChan: make(chan struct{}),
	}

	// Initialize modules based on configuration
	for _, moduleName := range cfg.Modules {
		module, err := c.createModule(moduleName)
		if err != nil {
			return nil, fmt.Errorf("failed to create module %s: %w", moduleName, err)
		}
		c.modules[moduleName] = module
	}

	return c, nil
}

// createModule creates a module instance by name.
func (c *Collector) createModule(name string) (Module, error) {
	switch name {
	case "iomonitor":
		// Create the comprehensive I/O monitor module
		module, err := NewIOMonitorModule(c.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create iomonitor module: %w", err)
		}
		// Configure cleanup strategy for Prometheus (clear after read)
		module.SetCleanupStrategy(ClearAfterRead, 60*time.Second)
		return module, nil
	case "fadvise", "memory", "cache":
		// These are all handled by iomonitor now
		c.logger.Warn("Module is deprecated, use 'iomonitor' instead",
			zap.String("module", name))
		return nil, fmt.Errorf("module %s is deprecated, use 'iomonitor' instead", name)
	default:
		return nil, fmt.Errorf("unknown module: %s", name)
	}
}

// Start starts the collector.
func (c *Collector) Start(ctx context.Context) error {
	c.logger.Info("Starting collector")

	// Start all modules
	for name, module := range c.modules {
		if err := module.Start(); err != nil {
			return fmt.Errorf("failed to start module %s: %w", name, err)
		}
		c.logger.Info("Started module", zap.String("module", name))
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
			c.collect()
		}
	}
}

// collect collects metrics from all modules.
func (c *Collector) collect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name, module := range c.modules {
		metricSet, err := module.Collect()
		if err != nil {
			c.logger.Error("Failed to collect metrics",
				zap.String("module", name),
				zap.Error(err))
			continue
		}

		// Store metrics
		c.metrics.Update(name, metricSet)
		c.logger.Debug("Collected metrics",
			zap.String("module", name),
			zap.Int("count", metricSet.Count()))
	}
}

// GetMetrics returns current metrics.
func (c *Collector) GetMetrics() *metrics.Store {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metrics
}

// Close stops the collector and cleans up resources.
func (c *Collector) Close() error {
	c.logger.Info("Stopping collector")

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
			c.logger.Error("Failed to stop module",
				zap.String("module", name),
				zap.Error(err))
		}
	}

	return nil
}
