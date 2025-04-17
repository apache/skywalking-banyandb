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

package fadvis

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ThresholdProvider is an interface for providing file size thresholds.
type ThresholdProvider interface {
	// GetThreshold returns the file size threshold.
	GetThreshold() int64
}

// Global FadvisManager instance
var defaultManager *Manager

// SetManager sets the global FadvisManager instance.
func SetManager(m *Manager) {
	defaultManager = m
}

// GetManager returns the global FadvisManager instance.
func GetManager() *Manager {
	return defaultManager
}

// Manager manages the fadvis threshold and periodically updates it.
type Manager struct {
	// threshold is the current threshold for large file detection.
	threshold atomic.Int64
	// thresholdProvider is used to calculate the threshold.
	thresholdProvider ThresholdProvider
	// updateInterval is the interval for updating the threshold.
	updateInterval time.Duration
	// logger for the fadvis manager.
	l *logger.Logger
	// closed channel for stopping the manager.
	closed chan struct{}
}

// NewManager creates a new fadvis manager.
func NewManager(provider ThresholdProvider) *Manager {
	m := &Manager{
		thresholdProvider: provider,
		updateInterval:    30 * time.Minute, // Update threshold every 30 minutes
		closed:            make(chan struct{}),
		l:                 logger.GetLogger("fadvis-manager"),
	}
	// Default 64MB
	m.threshold.Store(64 * 1024 * 1024)
	return m
}

// Name returns the name of the manager.
func (m *Manager) Name() string {
	return "fadvis-manager"
}

// FlagSet returns the flag set for the manager.
func (m *Manager) FlagSet() *run.FlagSet {
	// We don't need our own flags since we use the threshold provider's configuration
	return run.NewFlagSet(m.Name())
}

// Validate validates the manager's flags.
func (m *Manager) Validate() error {
	return nil
}

// PreRun initializes the manager.
func (m *Manager) PreRun(context.Context) error {
	// Update the threshold immediately
	m.updateThreshold()
	return nil
}

// GracefulStop stops the manager.
func (m *Manager) GracefulStop() {
	close(m.closed)
}

// Serve starts the manager.
func (m *Manager) Serve() run.StopNotify {
	go func() {
		ticker := time.NewTicker(m.updateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-m.closed:
				return
			case <-ticker.C:
				m.updateThreshold()
			}
		}
	}()
	return m.closed
}

// updateThreshold updates the threshold from the threshold provider.
// The threshold is 1% of the page cache size, which is (100-allowedPercent)% of total memory.
func (m *Manager) updateThreshold() {
	if m.thresholdProvider == nil {
		m.l.Warn().Msg("threshold provider is not available, using default threshold")
		return
	}

	threshold := m.thresholdProvider.GetThreshold()
	if threshold <= 0 {
		m.l.Warn().Msg("invalid threshold from threshold provider, using default")
		return
	}

	m.threshold.Store(threshold)
	m.l.Info().
		Str("threshold", humanize.Bytes(uint64(threshold))).
		Msg("updated fadvis threshold")
}

// GetThreshold returns the current threshold.
func (m *Manager) GetThreshold() int64 {
	return m.threshold.Load()
}

// ShouldApplyFadvis checks if fadvis should be applied to a file of the given size.
func (m *Manager) ShouldApplyFadvis(fileSize int64) bool {
	return fileSize > m.threshold.Load()
}

// SetMemoryProtector sets the global Memory protector instance for fadvis threshold management.
func SetMemoryProtector(mp *protector.Memory) {
	manager := NewManager(mp)
	SetManager(manager)
}
