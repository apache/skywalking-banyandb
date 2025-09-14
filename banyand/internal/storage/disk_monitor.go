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

package storage

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// RetentionService defines the interface that services must implement
// to support forced retention cleanup.
type RetentionService interface {
	// GetDataPath returns the service's data directory path
	GetDataPath() string
	// GetSnapshotDir returns the service's snapshot directory path
	GetSnapshotDir() string
	// LoadAllGroups returns all groups managed by this service
	LoadAllGroups() []resourceSchema.Group
	// DeleteOldestSegmentInGroup deletes the oldest segment in the specified group
	// Returns true if a segment was deleted, false if no segments to delete
	DeleteOldestSegmentInGroup(group string) (bool, error)
	// CleanupOldSnapshots removes snapshots older than the specified duration
	CleanupOldSnapshots(maxAge time.Duration) error
	// GetServiceName returns the service name for metrics and logging
	GetServiceName() string
}

// RetentionConfig holds the configuration for forced retention cleanup.
type RetentionConfig struct {
	// HighWatermark is the disk usage percentage that triggers forced cleanup (0-100)
	HighWatermark float64
	// LowWatermark is the disk usage percentage where cleanup stops (0-100)
	LowWatermark float64
	// CheckInterval is how often to check disk usage
	CheckInterval time.Duration
	// Cooldown is the sleep duration between segment deletions
	Cooldown time.Duration
}

// DiskMonitor monitors disk usage and orchestrates forced retention cleanup
// for a service when disk usage exceeds configured watermarks.
type DiskMonitor struct {
	service        RetentionService
	logger         *logger.Logger
	ticker         *time.Ticker
	stopCh         chan struct{}
	metrics        *diskMonitorMetrics
	config         RetentionConfig
	snapshotMaxAge time.Duration
	isActive       atomic.Bool
}

type diskMonitorMetrics struct {
	forcedRetentionActive          meter.Gauge
	forcedRetentionRunsTotal       meter.Counter
	forcedRetentionSegmentsDeleted meter.Counter
	forcedRetentionLastRunSeconds  meter.Gauge
	forcedRetentionCooldownSeconds meter.Gauge
	diskUsagePercent               meter.Gauge
	snapshotsDeletedTotal          meter.Counter
}

// NewDiskMonitor creates a new disk monitor for the given service.
func NewDiskMonitor(service RetentionService, config RetentionConfig, omr observability.MetricsRegistry) *DiskMonitor {
	serviceName := service.GetServiceName()
	logger := logger.GetLogger("disk-monitor").Named(serviceName)

	// Create metrics
	factory := omr.With(observability.RootScope.SubScope("storage").SubScope("retention"))
	metrics := &diskMonitorMetrics{
		forcedRetentionActive:          factory.NewGauge("forced_retention_active", "service"),
		forcedRetentionRunsTotal:       factory.NewCounter("forced_retention_runs_total", "service"),
		forcedRetentionSegmentsDeleted: factory.NewCounter("forced_retention_segments_deleted_total", "service"),
		forcedRetentionLastRunSeconds:  factory.NewGauge("forced_retention_last_run_seconds", "service"),
		forcedRetentionCooldownSeconds: factory.NewGauge("forced_retention_cooldown_seconds", "service"),
		diskUsagePercent:               factory.NewGauge("disk_usage_percent", "service"),
		snapshotsDeletedTotal:          factory.NewCounter("snapshots_deleted_total", "service"),
	}

	// Initialize cooldown metric
	metrics.forcedRetentionCooldownSeconds.Set(config.Cooldown.Seconds(), serviceName)

	return &DiskMonitor{
		service:        service,
		config:         config,
		logger:         logger,
		stopCh:         make(chan struct{}),
		metrics:        metrics,
		snapshotMaxAge: 24 * time.Hour, // Always keep snapshots newer than 24h
	}
}

// Start begins monitoring disk usage and starts the forced retention process.
func (dm *DiskMonitor) Start(ctx context.Context) {
	if dm.config.CheckInterval <= 0 {
		dm.logger.Warn().Msg("disk monitor check interval is 0 or negative, monitor disabled")
		return
	}

	dm.ticker = time.NewTicker(dm.config.CheckInterval)
	serviceName := dm.service.GetServiceName()

	dm.logger.Info().
		Float64("high_watermark", dm.config.HighWatermark).
		Float64("low_watermark", dm.config.LowWatermark).
		Dur("check_interval", dm.config.CheckInterval).
		Dur("cooldown", dm.config.Cooldown).
		Msg("starting disk monitor")

	go dm.monitorLoop(serviceName)
}

// Stop stops the disk monitor gracefully.
func (dm *DiskMonitor) Stop() {
	if dm.ticker != nil {
		dm.ticker.Stop()
	}
	close(dm.stopCh)

	// Wait for any active cleanup to finish
	for dm.isActive.Load() {
		time.Sleep(100 * time.Millisecond)
	}

	dm.logger.Info().Msg("disk monitor stopped")
}

func (dm *DiskMonitor) monitorLoop(serviceName string) {
	defer func() {
		if r := recover(); r != nil {
			dm.logger.Error().Interface("panic", r).Msg("disk monitor panic recovered")
		}
	}()

	for {
		select {
		case <-dm.stopCh:
			return
		case <-dm.ticker.C:
			dm.checkAndCleanup(serviceName)
		}
	}
}

func (dm *DiskMonitor) checkAndCleanup(serviceName string) {
	// Check disk usage
	diskPercent := observability.GetPathUsedPercent(dm.service.GetDataPath())
	dm.metrics.diskUsagePercent.Set(float64(diskPercent), serviceName)

	dm.logger.Debug().Int("disk_percent", diskPercent).Msg("checking disk usage")

	// If usage is below high watermark and no cleanup is active, nothing to do
	if float64(diskPercent) < dm.config.HighWatermark && !dm.isActive.Load() {
		return
	}

	// If usage is above high watermark, start forced cleanup
	if float64(diskPercent) >= dm.config.HighWatermark && !dm.isActive.Load() {
		dm.logger.Info().
			Int("disk_percent", diskPercent).
			Float64("high_watermark", dm.config.HighWatermark).
			Msg("disk usage above high watermark, starting forced cleanup")

		dm.isActive.Store(true)
		dm.metrics.forcedRetentionActive.Set(1, serviceName)
		dm.metrics.forcedRetentionRunsTotal.Inc(1, serviceName)
	}

	// If cleanup is active, continue until below low watermark
	if dm.isActive.Load() {
		dm.runForcedCleanup(serviceName, diskPercent)
	}
}

func (dm *DiskMonitor) runForcedCleanup(serviceName string, _ int) {
	startTime := time.Now()
	defer func() {
		dm.metrics.forcedRetentionLastRunSeconds.Set(time.Since(startTime).Seconds(), serviceName)
	}()

	// First, clean up old snapshots
	if err := dm.cleanupSnapshots(serviceName); err != nil {
		dm.logger.Error().Err(err).Msg("failed to cleanup old snapshots")
	}

	// Check if snapshot cleanup was enough
	diskPercent := observability.GetPathUsedPercent(dm.service.GetDataPath())
	if float64(diskPercent) <= dm.config.LowWatermark {
		dm.logger.Info().
			Int("disk_percent", diskPercent).
			Float64("low_watermark", dm.config.LowWatermark).
			Msg("disk usage below low watermark after snapshot cleanup, stopping forced cleanup")

		dm.isActive.Store(false)
		dm.metrics.forcedRetentionActive.Set(0, serviceName)
		return
	}

	// Delete segments iteratively
	deleted := dm.deleteOldestSegment(serviceName)
	if deleted {
		dm.metrics.forcedRetentionSegmentsDeleted.Inc(1, serviceName)

		// Check if we're now below low watermark
		diskPercent = observability.GetPathUsedPercent(dm.service.GetDataPath())
		if float64(diskPercent) <= dm.config.LowWatermark {
			dm.logger.Info().
				Int("disk_percent", diskPercent).
				Float64("low_watermark", dm.config.LowWatermark).
				Msg("disk usage below low watermark, stopping forced cleanup")

			dm.isActive.Store(false)
			dm.metrics.forcedRetentionActive.Set(0, serviceName)
			return
		}

		// Sleep for cooldown period
		dm.logger.Debug().Dur("cooldown", dm.config.Cooldown).Msg("cooling down between deletions")
		time.Sleep(dm.config.Cooldown)
	} else {
		// No more segments to delete, stop cleanup
		dm.logger.Warn().Msg("no more segments available for deletion, stopping forced cleanup")
		dm.isActive.Store(false)
		dm.metrics.forcedRetentionActive.Set(0, serviceName)
	}
}

func (dm *DiskMonitor) cleanupSnapshots(serviceName string) error {
	err := dm.service.CleanupOldSnapshots(dm.snapshotMaxAge)
	if err == nil {
		dm.metrics.snapshotsDeletedTotal.Inc(1, serviceName)
	}
	return err
}

func (dm *DiskMonitor) deleteOldestSegment(_ string) bool {
	groups := dm.service.LoadAllGroups()
	if len(groups) == 0 {
		return false
	}

	// Find the group with the oldest segment globally
	oldestGroup := dm.findGroupWithOldestSegment(groups)
	if oldestGroup == "" {
		return false
	}

	// Delete the oldest segment from that group
	deleted, err := dm.service.DeleteOldestSegmentInGroup(oldestGroup)
	if err != nil {
		dm.logger.Error().Err(err).Str("group", oldestGroup).Msg("failed to delete oldest segment")
		return false
	}

	if deleted {
		dm.logger.Info().Str("group", oldestGroup).Msg("deleted oldest segment")
	}

	return deleted
}

func (dm *DiskMonitor) findGroupWithOldestSegment(groups []resourceSchema.Group) string {
	type groupSegmentTime struct {
		endTime   time.Time
		groupName string
	}

	var candidates []groupSegmentTime

	// For now, we'll implement a simple round-robin approach across groups
	// since we don't have the storage API extensions yet to peek segment times
	// TODO: Replace with actual oldest-segment-finding logic when storage APIs are ready
	for _, group := range groups {
		candidates = append(candidates, groupSegmentTime{
			groupName: group.GetSchema().Metadata.Name,
			endTime:   time.Now(), // Placeholder - will be replaced with actual segment time
		})
	}

	if len(candidates) == 0 {
		return ""
	}

	// Sort by end time to find oldest
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].endTime.Before(candidates[j].endTime)
	})

	return candidates[0].groupName
}
