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

// Package fadvis manages file access strategies based on runtime memory thresholds.
package fadvis

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ThresholdProvider is an interface for providing file size thresholds.
type ThresholdProvider interface {
	// GetThreshold returns the file size threshold.
	GetThreshold() int64
}

// Global FadvisManager instance.
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
	// logger for the fadvis manager.
	l *logger.Logger
	// closed channel for stopping the manager.
	closed chan struct{}
	// thresholdProvider is used to calculate the threshold.
	thresholdProvider ThresholdProvider
	// updateInterval is the interval for updating the threshold.
	updateInterval time.Duration
	// threshold is the current threshold for large file detection.
	threshold atomic.Int64
}

// NewManager creates a new fadvis manager.
func NewManager(provider ThresholdProvider) *Manager {
	m := &Manager{
		thresholdProvider: provider,
		updateInterval:    30 * time.Minute, // Update threshold every 30 minutes
		closed:            make(chan struct{}),
		l:                 logger.GetLogger("fadvis-manager"),
	}
	m.threshold.Store(64 * 1024 * 1024)
	return m
}

// Name returns the name of the fadvis manager.
// This implements the run.Unit interface.
func (m *Manager) Name() string {
	return "fadvis-manager"
}

// FlagSet returns a flagset for the fadvis manager configuration.
// This implements the run.Unit interface.
func (m *Manager) FlagSet() *run.FlagSet {
	return run.NewFlagSet(m.Name())
}

// Validate validates configuration for the fadvis manager.
// This implements the run.Unit interface.
func (m *Manager) Validate() error {
	return nil
}

// PreRun initializes the fadvis manager by updating the threshold.
// This implements the run.Unit interface.
func (m *Manager) PreRun(context.Context) error {
	m.updateThreshold()
	return nil
}

// GracefulStop stops the fadvis manager gracefully.
// This implements the run.Unit interface.
func (m *Manager) GracefulStop() {
	close(m.closed)
}

// Serve starts the fadvis manager to periodically update thresholds.
// This implements the run.Unit interface.
func (m *Manager) Serve() run.StopNotify {
	go func() {
		ticker := time.NewTicker(m.updateInterval)
		defer ticker.Stop()

		m.updateThreshold()

		for {
			select {
			case <-m.closed:
				m.l.Info().Msg("fadvis manager stopped")
				return
			case <-ticker.C:
				m.updateThreshold()
			}
		}
	}()
	return m.closed
}

// updateThreshold updates the threshold from the threshold provider.
// The threshold is calculated based on memory usage from the provider.
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

// GetThreshold returns the current file size threshold.
// Files larger than this threshold will be candidates for fadvis optimization.
func (m *Manager) GetThreshold() int64 {
	return m.threshold.Load()
}

// ShouldApplyFadvis checks if fadvis should be applied to a file of the given size.
// It returns true if the file size exceeds the current threshold.
func (m *Manager) ShouldApplyFadvis(fileSize int64) bool {
	return fileSize > m.threshold.Load()
}

// ShouldCache returns whether a file at the given path should be cached.
// Currently always returns true as the cache decision is made later based on file size.
func (m *Manager) ShouldCache(_ string) bool {
	return true
}

// SetMemoryProtector sets the global Memory protector instance for fadvis threshold management.
// It creates a new Manager with the provided memory protector and registers it with the fs package.
func SetMemoryProtector(mp *protector.Memory) {
	manager := NewManager(mp)
	SetManager(manager)

	fs.SetThresholdProvider(manager)
}

// CleanupForTesting stops the default manager if it exists.
// This function is intended to be called in test teardown functions to prevent goroutine leaks.
func CleanupForTesting() {
	if defaultManager != nil {
		defaultManager.GracefulStop()
		time.Sleep(100 * time.Millisecond)
	}
}
