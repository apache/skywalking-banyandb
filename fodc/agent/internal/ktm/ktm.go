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

package ktm

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor"
	fodcmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

// KTM represents the Kernel Trace Module.
type KTM struct {
	logger    zerolog.Logger
	collector *iomonitor.Collector
	stopCh    chan struct{}
	config    Config
}

// NewKTM creates a new KTM instance.
func NewKTM(cfg Config, log zerolog.Logger) (*KTM, error) {
	if !cfg.Enabled {
		return &KTM{config: cfg, logger: log}, nil
	}

	// Convert KTM config to Collector config
	collectorConfig := iomonitor.CollectorConfig{
		Modules:  cfg.Modules,
		EBPF:     cfg.EBPF,
		Interval: cfg.Interval,
	}

	col, err := iomonitor.New(collectorConfig, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create collector: %w", err)
	}

	return &KTM{
		config:    cfg,
		logger:    log,
		collector: col,
		stopCh:    make(chan struct{}),
	}, nil
}

// Start starts the KTM module.
func (k *KTM) Start(ctx context.Context) error {
	if !k.config.Enabled {
		k.logger.Info().Msg("KTM module is disabled")
		return nil
	}

	k.logger.Info().Msg("Starting KTM module")
	if err := k.collector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start collector: %w", err)
	}

	return nil
}

// Stop stops the KTM module.
func (k *KTM) Stop() error {
	if !k.config.Enabled {
		return nil
	}

	k.logger.Info().Msg("Stopping KTM module")
	close(k.stopCh)
	return k.collector.Close()
}

// GetMetrics returns the collected metrics.
func (k *KTM) GetMetrics() []fodcmetrics.RawMetric {
	if !k.config.Enabled || k.collector == nil {
		return nil
	}
	return k.collector.GetMetrics()
}

// IsDegraded returns whether KTM is running in degraded (comm-only) mode.
func (k *KTM) IsDegraded() bool {
	if !k.config.Enabled || k.collector == nil {
		return false
	}
	return k.collector.IsDegraded()
}
