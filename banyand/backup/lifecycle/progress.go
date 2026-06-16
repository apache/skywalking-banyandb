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

package lifecycle

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Progress tracks the lifecycle migration progress to support resume after crash.
type Progress struct {
	logger                      *logger.Logger                                           `json:"-"`
	CompletedGroups             map[string]bool                                          `json:"completed_groups"`
	DeletedStreamGroups         map[string]bool                                          `json:"deleted_stream_groups"`
	DeletedMeasureGroups        map[string]bool                                          `json:"deleted_measure_groups"`
	DeletedTraceGroups          map[string]bool                                          `json:"deleted_trace_groups"`
	CompletedStreamParts        map[string]map[string]map[common.ShardID]map[uint64]bool `json:"completed_stream_parts"`
	CompletedStreamSeries       map[string]map[string]map[common.ShardID]bool            `json:"completed_stream_series"`
	CompletedStreamElementIndex map[string]map[string]map[common.ShardID]bool            `json:"completed_stream_element_index"`
	StreamPartCounts            map[string]int                                           `json:"stream_part_counts"`
	StreamPartProgress          map[string]int                                           `json:"stream_part_progress"`
	StreamSeriesCounts          map[string]int                                           `json:"stream_series_counts"`
	StreamSeriesProgress        map[string]int                                           `json:"stream_series_progress"`
	StreamElementIndexCounts    map[string]int                                           `json:"stream_element_index_counts"`
	StreamElementIndexProgress  map[string]int                                           `json:"stream_element_index_progress"`
	// Measure part-specific progress tracking
	CompletedMeasureParts  map[string]map[string]map[common.ShardID]map[uint64]bool `json:"completed_measure_parts"`
	MeasurePartCounts      map[string]int                                           `json:"measure_part_counts"`
	MeasurePartProgress    map[string]int                                           `json:"measure_part_progress"`
	CompletedMeasureSeries map[string]map[string]map[common.ShardID]bool            `json:"completed_measure_series"`
	MeasureSeriesCounts    map[string]int                                           `json:"measure_series_counts"`
	MeasureSeriesProgress  map[string]int                                           `json:"measure_series_progress"`
	// Trace part-specific progress tracking
	CompletedTraceShards map[string]map[string]map[common.ShardID]bool `json:"completed_trace_shards"`
	TraceShardCounts     map[string]int                                `json:"trace_shard_counts"`
	TraceShardProgress   map[string]int                                `json:"trace_shard_progress"`
	CompletedTraceSeries map[string]map[string]map[common.ShardID]bool `json:"completed_trace_series"`
	TraceSeriesCounts    map[string]int                                `json:"trace_series_counts"`
	TraceSeriesProgress  map[string]int                                `json:"trace_series_progress"`

	// Per-source completion tracking (advances *Progress idempotently once per source item).
	SourceCompletedStreamParts        map[string]map[string]map[common.ShardID]map[uint64]bool `json:"source_completed_stream_parts"`
	SourceCompletedStreamSeries       map[string]map[string]map[common.ShardID]bool            `json:"source_completed_stream_series"`
	SourceCompletedStreamElementIndex map[string]map[string]map[common.ShardID]bool            `json:"source_completed_stream_element_index"`
	SourceCompletedMeasureParts       map[string]map[string]map[common.ShardID]map[uint64]bool `json:"source_completed_measure_parts"`
	SourceCompletedMeasureSeries      map[string]map[string]map[common.ShardID]bool            `json:"source_completed_measure_series"`
	SourceCompletedTraceShards        map[string]map[string]map[common.ShardID]bool            `json:"source_completed_trace_shards"`
	SourceCompletedTraceSeries        map[string]map[string]map[common.ShardID]bool            `json:"source_completed_trace_series"`

	// Sync-mode breakdown per group: how many parts (measure/stream) or shards
	// (trace) were migrated via chunk-sync vs row-replay, plus the real number
	// of rows replayed. Accumulated across resume cycles, fed into the report's
	// summary.<catalog>.sync_breakdown.
	StreamChunkSyncParts  map[string]uint64 `json:"stream_chunk_sync_parts"`
	StreamRowReplayParts  map[string]uint64 `json:"stream_row_replay_parts"`
	StreamRowReplayRows   map[string]uint64 `json:"stream_row_replay_rows"`
	MeasureChunkSyncParts map[string]uint64 `json:"measure_chunk_sync_parts"`
	MeasureRowReplayParts map[string]uint64 `json:"measure_row_replay_parts"`
	MeasureRowReplayRows  map[string]uint64 `json:"measure_row_replay_rows"`
	TraceChunkSyncShards  map[string]uint64 `json:"trace_chunk_sync_shards"`
	TraceRowReplayParts   map[string]uint64 `json:"trace_row_replay_parts"`
	TraceRowReplayRows    map[string]uint64 `json:"trace_row_replay_rows"`

	// OrphanRows reports, per catalog -> group -> deleted measure/stream subject,
	// how many rows were archived or discarded because the schema was deleted from
	// the registry. Accumulated across resume cycles (orphan parts are marked
	// completed, so each row is counted once), fed into the report's orphan_handling.
	OrphanRows map[string]map[string]map[string]uint64 `json:"orphan_rows"`

	// MigrationErrors is the single source of structured migration errors for both
	// the report (stage/group/catalog/scope/segment/interval/location/message) and
	// the per-resource error counts. Reset by ClearErrors at the start of each run
	// so the report reflects the current attempt; persisted only for crash
	// durability within a run.
	MigrationErrors []MigrationError `json:"migration_errors"`

	progressFilePath   string     `json:"-"`
	SnapshotStreamDir  string     `json:"snapshot_stream_dir"`
	SnapshotMeasureDir string     `json:"snapshot_measure_dir"`
	SnapshotTraceDir   string     `json:"snapshot_trace_dir"`
	GroupsToProcess    []string   `json:"groups_to_process"`
	mu                 sync.Mutex `json:"-"`
}

// SetGroupsToProcess records the set of groups picked up by this migration cycle.
// Used as the denominator for migration_status.completion_rate so the report
// reflects "completed / scheduled" rather than overlapping per-catalog buckets.
func (p *Progress) SetGroupsToProcess(groups []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.GroupsToProcess = append(p.GroupsToProcess[:0], groups...)
}

// AllGroupsNotFullyCompleted find is there have any group not fully completed.
func (p *Progress) AllGroupsNotFullyCompleted(groups []*commonv1.Group) []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]string, 0)
	for _, group := range groups {
		if !p.CompletedGroups[group.Metadata.Name] {
			result = append(result, group.Metadata.Name)
		}
	}
	return result
}

// NewProgress creates a new Progress tracker.
func NewProgress(path string, l *logger.Logger) *Progress {
	return &Progress{
		CompletedGroups:             make(map[string]bool),
		DeletedStreamGroups:         make(map[string]bool),
		DeletedMeasureGroups:        make(map[string]bool),
		DeletedTraceGroups:          make(map[string]bool),
		CompletedStreamParts:        make(map[string]map[string]map[common.ShardID]map[uint64]bool),
		StreamPartCounts:            make(map[string]int),
		StreamPartProgress:          make(map[string]int),
		CompletedStreamSeries:       make(map[string]map[string]map[common.ShardID]bool),
		StreamSeriesCounts:          make(map[string]int),
		StreamSeriesProgress:        make(map[string]int),
		CompletedStreamElementIndex: make(map[string]map[string]map[common.ShardID]bool),
		StreamElementIndexCounts:    make(map[string]int),
		StreamElementIndexProgress:  make(map[string]int),
		CompletedMeasureParts:       make(map[string]map[string]map[common.ShardID]map[uint64]bool),
		MeasurePartCounts:           make(map[string]int),
		MeasurePartProgress:         make(map[string]int),
		CompletedMeasureSeries:      make(map[string]map[string]map[common.ShardID]bool),
		MeasureSeriesCounts:         make(map[string]int),
		MeasureSeriesProgress:       make(map[string]int),
		CompletedTraceShards:        make(map[string]map[string]map[common.ShardID]bool),
		TraceShardCounts:            make(map[string]int),
		TraceShardProgress:          make(map[string]int),
		CompletedTraceSeries:        make(map[string]map[string]map[common.ShardID]bool),
		TraceSeriesCounts:           make(map[string]int),
		TraceSeriesProgress:         make(map[string]int),

		SourceCompletedStreamParts:        make(map[string]map[string]map[common.ShardID]map[uint64]bool),
		SourceCompletedStreamSeries:       make(map[string]map[string]map[common.ShardID]bool),
		SourceCompletedStreamElementIndex: make(map[string]map[string]map[common.ShardID]bool),
		SourceCompletedMeasureParts:       make(map[string]map[string]map[common.ShardID]map[uint64]bool),
		SourceCompletedMeasureSeries:      make(map[string]map[string]map[common.ShardID]bool),
		SourceCompletedTraceShards:        make(map[string]map[string]map[common.ShardID]bool),
		SourceCompletedTraceSeries:        make(map[string]map[string]map[common.ShardID]bool),

		StreamChunkSyncParts:  make(map[string]uint64),
		StreamRowReplayParts:  make(map[string]uint64),
		StreamRowReplayRows:   make(map[string]uint64),
		MeasureChunkSyncParts: make(map[string]uint64),
		MeasureRowReplayParts: make(map[string]uint64),
		MeasureRowReplayRows:  make(map[string]uint64),
		TraceChunkSyncShards:  make(map[string]uint64),
		TraceRowReplayParts:   make(map[string]uint64),
		TraceRowReplayRows:    make(map[string]uint64),
		OrphanRows:            make(map[string]map[string]map[string]uint64),
		MigrationErrors:       make([]MigrationError, 0),

		progressFilePath: path,
		logger:           l,
	}
}

// AddOrphanRows accumulates per-subject orphan row counts for one (catalog,
// group) into the report's orphan_handling section. Additive so resume cycles —
// each replaying a different subset of parts — sum to the true total.
func (p *Progress) AddOrphanRows(group, catalog string, bySubject map[string]uint64) {
	if len(bySubject) == 0 {
		return
	}
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.OrphanRows == nil {
		p.OrphanRows = make(map[string]map[string]map[string]uint64)
	}
	byGroup := p.OrphanRows[catalog]
	if byGroup == nil {
		byGroup = make(map[string]map[string]uint64)
		p.OrphanRows[catalog] = byGroup
	}
	bySub := byGroup[group]
	if bySub == nil {
		bySub = make(map[string]uint64)
		byGroup[group] = bySub
	}
	for sub, n := range bySubject {
		bySub[sub] += n
	}
}

// AddMigrationError appends a structured migration error to the report-facing
// list under the progress lock.
func (p *Progress) AddMigrationError(e MigrationError) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	// Overwrite by location so re-recording the same artifact (e.g. a part
	// replayed to multiple target segments) replaces the prior entry instead of
	// duplicating it.
	key := e.locationKey()
	for i := range p.MigrationErrors {
		if p.MigrationErrors[i].locationKey() == key {
			p.MigrationErrors[i] = e
			return
		}
	}
	p.MigrationErrors = append(p.MigrationErrors, e)
}

// LoadProgress loads progress from a file if it exists.
func LoadProgress(path string, l *logger.Logger) *Progress {
	if path == "" {
		return NewProgress(path, l)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			l.Info().Msgf("No existing progress file at %s, starting fresh", path)
		} else {
			l.Warn().Err(err).Msgf("Failed to read progress file at %s, starting fresh", path)
		}
		return NewProgress(path, l)
	}

	progress := NewProgress(path, l)
	if err := json.Unmarshal(data, progress); err != nil {
		l.Warn().Err(err).Msgf("Failed to parse progress file at %s, starting fresh", path)
		return NewProgress(path, l)
	}

	l.Info().Msgf("Loaded existing progress from %s", path)
	return progress
}

// Save writes the progress to the specified file.
func (p *Progress) Save(path string, l *logger.Logger) {
	// Use stored values if parameters are empty
	if path == "" {
		path = p.progressFilePath
	}
	if l == nil {
		l = p.logger
	}

	if path == "" || l == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		l.Error().Err(err).Msg("Failed to marshal progress data")
		return
	}

	err = os.WriteFile(path, data, 0o600)
	if err != nil {
		l.Error().Err(err).Msgf("Failed to write progress file at %s", path)
		return
	}
	l.Debug().Msg("Progress saved successfully")
}

// saveProgress is a convenience method that uses stored path and logger.
func (p *Progress) saveProgress() {
	p.Save("", nil)
}

// MarkGroupCompleted marks a group as completed.
func (p *Progress) MarkGroupCompleted(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.CompletedGroups[group] = true
}

// IsGroupCompleted checks if a group has been completed.
func (p *Progress) IsGroupCompleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.CompletedGroups[group]
}

// MarkStreamGroupDeleted marks a stream group segments as deleted.
func (p *Progress) MarkStreamGroupDeleted(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.DeletedStreamGroups[group] = true
}

// IsStreamGroupDeleted checks if a stream group segments have been deleted.
func (p *Progress) IsStreamGroupDeleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.DeletedStreamGroups[group]
}

// MarkMeasureGroupDeleted marks a measure group segments as deleted.
func (p *Progress) MarkMeasureGroupDeleted(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.DeletedMeasureGroups[group] = true
}

// IsMeasureGroupDeleted checks if a measure group segments have been deleted.
func (p *Progress) IsMeasureGroupDeleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.DeletedMeasureGroups[group]
}

// Remove deletes the progress file.
func (p *Progress) Remove(path string, l *logger.Logger) {
	if path == "" {
		return
	}

	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			l.Warn().Err(err).Msgf("Failed to remove progress file at %s", path)
		}
		return
	}
	l.Info().Msgf("Removed progress file at %s", path)
}

// MarkStreamPartCompleted marks a specific part of a stream as completed.
func (p *Progress) MarkStreamPartCompleted(group string, segmentID string, shardID common.ShardID, partID uint64) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamParts[group] == nil {
		p.CompletedStreamParts[group] = make(map[string]map[common.ShardID]map[uint64]bool)
	}
	if p.CompletedStreamParts[group][segmentID] == nil {
		p.CompletedStreamParts[group][segmentID] = make(map[common.ShardID]map[uint64]bool)
	}
	if p.CompletedStreamParts[group][segmentID][shardID] == nil {
		p.CompletedStreamParts[group][segmentID][shardID] = make(map[uint64]bool)
	}

	// Mark part as completed
	p.CompletedStreamParts[group][segmentID][shardID][partID] = true
}

// IsStreamPartCompleted checks if a specific part of a stream has been completed.
func (p *Progress) IsStreamPartCompleted(group string, segmentID string, shardID common.ShardID, partID uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedStreamParts[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			if parts, ok := shards[shardID]; ok {
				return parts[partID]
			}
		}
	}
	return false
}

// SetStreamPartCount sets the total number of parts for a stream.
func (p *Progress) SetStreamPartCount(group string, totalParts int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.StreamPartCounts[group] = totalParts
}

// GetStreamPartCount returns the total number of parts for a stream.
func (p *Progress) GetStreamPartCount(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.StreamPartCounts[group]; ok {
		return counts
	}
	return 0
}

// GetStreamPartProgress returns the number of completed parts for a stream.
func (p *Progress) GetStreamPartProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.StreamPartProgress[group]; ok {
		return progress
	}
	return 0
}

// MarkStreamSeriesCompleted marks a specific series segment of a stream as completed.
func (p *Progress) MarkStreamSeriesCompleted(group string, segmentID string, shardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamSeries[group] == nil {
		p.CompletedStreamSeries[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.CompletedStreamSeries[group][segmentID] == nil {
		p.CompletedStreamSeries[group][segmentID] = make(map[common.ShardID]bool)
	}

	// Mark series segment as completed
	p.CompletedStreamSeries[group][segmentID][shardID] = true
}

// IsStreamSeriesCompleted checks if a specific series segment of a stream has been completed.
func (p *Progress) IsStreamSeriesCompleted(group string, segmentID string, shardID common.ShardID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedStreamSeries[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			return shards[shardID]
		}
	}
	return false
}

// SetStreamSeriesCount sets the total number of series segments for a stream.
func (p *Progress) SetStreamSeriesCount(group string, totalSegments int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.StreamSeriesCounts[group] = totalSegments
}

// GetStreamSeriesCount returns the total number of series segments for a stream.
func (p *Progress) GetStreamSeriesCount(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.StreamSeriesCounts[group]; ok {
		return counts
	}
	return 0
}

// GetStreamSeriesProgress returns the number of completed series segments for a stream.
func (p *Progress) GetStreamSeriesProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.StreamSeriesProgress[group]; ok {
		return progress
	}
	return 0
}

// MarkStreamElementIndexCompleted marks a specific element index file of a stream as completed.
func (p *Progress) MarkStreamElementIndexCompleted(group string, segmentID string, shardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamElementIndex[group] == nil {
		p.CompletedStreamElementIndex[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.CompletedStreamElementIndex[group][segmentID] == nil {
		p.CompletedStreamElementIndex[group][segmentID] = make(map[common.ShardID]bool)
	}

	// Mark shard as completed
	p.CompletedStreamElementIndex[group][segmentID][shardID] = true
}

// IsStreamElementIndexCompleted checks if a specific element index file of a stream has been completed.
func (p *Progress) IsStreamElementIndexCompleted(group string, segmentID string, shardID common.ShardID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedStreamElementIndex[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			return shards[shardID]
		}
	}
	return false
}

// SetStreamElementIndexCount sets the total number of element index files for a stream.
func (p *Progress) SetStreamElementIndexCount(group string, totalIndexFiles int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.StreamElementIndexCounts[group] = totalIndexFiles
}

// GetStreamElementIndexCount returns the total number of element index files for a stream.
func (p *Progress) GetStreamElementIndexCount(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.StreamElementIndexCounts[group]; ok {
		return counts
	}
	return 0
}

// GetStreamElementIndexProgress returns the number of completed element index files for a stream.
func (p *Progress) GetStreamElementIndexProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.StreamElementIndexProgress[group]; ok {
		return progress
	}
	return 0
}

// MarkMeasurePartCompleted marks a specific part of a measure as completed.
func (p *Progress) MarkMeasurePartCompleted(group string, segmentID string, shardID common.ShardID, partID uint64) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedMeasureParts[group] == nil {
		p.CompletedMeasureParts[group] = make(map[string]map[common.ShardID]map[uint64]bool)
	}
	if p.CompletedMeasureParts[group][segmentID] == nil {
		p.CompletedMeasureParts[group][segmentID] = make(map[common.ShardID]map[uint64]bool)
	}
	if p.CompletedMeasureParts[group][segmentID][shardID] == nil {
		p.CompletedMeasureParts[group][segmentID][shardID] = make(map[uint64]bool)
	}

	// Mark part as completed
	p.CompletedMeasureParts[group][segmentID][shardID][partID] = true
}

// IsMeasurePartCompleted checks if a specific part of a measure has been completed.
func (p *Progress) IsMeasurePartCompleted(group string, segmentID string, shardID common.ShardID, partID uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedMeasureParts[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			if parts, ok := shards[shardID]; ok {
				return parts[partID]
			}
		}
	}
	return false
}

// SetMeasurePartCount sets the total number of parts for a measure.
func (p *Progress) SetMeasurePartCount(group string, totalParts int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.MeasurePartCounts[group] = totalParts
}

// GetMeasurePartCount returns the total number of parts for a measure.
func (p *Progress) GetMeasurePartCount(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.MeasurePartCounts[group]; ok {
		return counts
	}
	return 0
}

// GetMeasurePartProgress returns the number of completed parts for a measure.
func (p *Progress) GetMeasurePartProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.MeasurePartProgress[group]; ok {
		return progress
	}
	return 0
}

// ClearErrors resets the recorded migration errors at the start of a run so a
// fresh cycle does not inherit the previous run's errors.
func (p *Progress) ClearErrors() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.MigrationErrors = p.MigrationErrors[:0]
}

// AddMeasureChunkSyncPart records one measure part migrated via chunk-sync.
func (p *Progress) AddMeasureChunkSyncPart(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.MeasureChunkSyncParts == nil {
		p.MeasureChunkSyncParts = make(map[string]uint64)
	}

	p.MeasureChunkSyncParts[group]++
}

// AddMeasureRowReplay records one measure part migrated via row-replay and the
// number of rows it republished.
func (p *Progress) AddMeasureRowReplay(group string, rows int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.MeasureRowReplayParts == nil {
		p.MeasureRowReplayParts = make(map[string]uint64)
	}
	if p.MeasureRowReplayRows == nil {
		p.MeasureRowReplayRows = make(map[string]uint64)
	}

	p.MeasureRowReplayParts[group]++
	if rows > 0 {
		p.MeasureRowReplayRows[group] += uint64(rows)
	}
}

// AddStreamChunkSyncPart records one stream part migrated via chunk-sync.
func (p *Progress) AddStreamChunkSyncPart(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamChunkSyncParts == nil {
		p.StreamChunkSyncParts = make(map[string]uint64)
	}

	p.StreamChunkSyncParts[group]++
}

// AddStreamRowReplay records one stream part migrated via row-replay and the
// number of rows it republished.
func (p *Progress) AddStreamRowReplay(group string, rows int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamRowReplayParts == nil {
		p.StreamRowReplayParts = make(map[string]uint64)
	}
	if p.StreamRowReplayRows == nil {
		p.StreamRowReplayRows = make(map[string]uint64)
	}

	p.StreamRowReplayParts[group]++
	if rows > 0 {
		p.StreamRowReplayRows[group] += uint64(rows)
	}
}

// AddTraceChunkSyncShard records one trace shard migrated via chunk-sync.
func (p *Progress) AddTraceChunkSyncShard(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TraceChunkSyncShards == nil {
		p.TraceChunkSyncShards = make(map[string]uint64)
	}

	p.TraceChunkSyncShards[group]++
}

// AddTraceRowReplay records the parts and rows of a trace shard migrated via
// row-replay (one shard may contain several parts).
func (p *Progress) AddTraceRowReplay(group string, parts, rows int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TraceRowReplayParts == nil {
		p.TraceRowReplayParts = make(map[string]uint64)
	}
	if p.TraceRowReplayRows == nil {
		p.TraceRowReplayRows = make(map[string]uint64)
	}

	if parts > 0 {
		p.TraceRowReplayParts[group] += uint64(parts)
	}
	if rows > 0 {
		p.TraceRowReplayRows[group] += uint64(rows)
	}
}

// MarkMeasureSeriesCompleted marks a specific series segment of a measure as completed.
func (p *Progress) MarkMeasureSeriesCompleted(group string, segmentID string, shardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedMeasureSeries[group] == nil {
		p.CompletedMeasureSeries[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.CompletedMeasureSeries[group][segmentID] == nil {
		p.CompletedMeasureSeries[group][segmentID] = make(map[common.ShardID]bool)
	}

	// Mark series segment as completed
	p.CompletedMeasureSeries[group][segmentID][shardID] = true
}

// IsMeasureSeriesCompleted checks if a specific series segment of a measure has been completed.
func (p *Progress) IsMeasureSeriesCompleted(group string, segmentID string, shardID common.ShardID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedMeasureSeries[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			return shards[shardID]
		}
	}
	return false
}

// SetMeasureSeriesCount sets the total number of series segments for a measure.
func (p *Progress) SetMeasureSeriesCount(group string, totalSegments int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.MeasureSeriesCounts[group] = totalSegments
}

// GetMeasureSeriesCount returns the total number of series segments for a measure.
func (p *Progress) GetMeasureSeriesCount(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.MeasureSeriesCounts[group]; ok {
		return counts
	}
	return 0
}

// GetMeasureSeriesProgress returns the number of completed series segments for a measure.
func (p *Progress) GetMeasureSeriesProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.MeasureSeriesProgress[group]; ok {
		return progress
	}
	return 0
}

// MarkTraceGroupDeleted marks a trace group as deleted.
func (p *Progress) MarkTraceGroupDeleted(group string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.DeletedTraceGroups[group] = true
}

// IsTraceGroupDeleted checks if a trace group has been deleted.
func (p *Progress) IsTraceGroupDeleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.DeletedTraceGroups[group]
}

// MarkTraceShardCompleted marks a specific shard of a trace as completed.
func (p *Progress) MarkTraceShardCompleted(group string, segmentID string, shardID common.ShardID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.CompletedTraceShards[group] == nil {
		p.CompletedTraceShards[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.CompletedTraceShards[group][segmentID] == nil {
		p.CompletedTraceShards[group][segmentID] = make(map[common.ShardID]bool)
	}
	p.CompletedTraceShards[group][segmentID][shardID] = true
}

// IsTraceShardCompleted checks if a specific part of a trace has been completed.
func (p *Progress) IsTraceShardCompleted(group string, segmentID string, shardID common.ShardID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedTraceShards[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			return shards[shardID]
		}
	}
	return false
}

// SetTraceShardCount sets the total number of shards for the current trace.
func (p *Progress) SetTraceShardCount(group string, totalShards int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.TraceShardCounts[group] = totalShards
}

// GetTraceShards gets the total number of shards for the current trace.
func (p *Progress) GetTraceShards(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if count, ok := p.TraceShardCounts[group]; ok {
		return count
	}
	return 0
}

// GetTraceShardProgress gets the number of completed shards for the current trace.
func (p *Progress) GetTraceShardProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.TraceShardProgress[group]; ok {
		return progress
	}
	return 0
}

// MarkTraceSeriesCompleted marks a specific series segment of a trace as completed.
func (p *Progress) MarkTraceSeriesCompleted(group string, segmentID string, shardID common.ShardID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.CompletedTraceSeries[group] == nil {
		p.CompletedTraceSeries[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.CompletedTraceSeries[group][segmentID] == nil {
		p.CompletedTraceSeries[group][segmentID] = make(map[common.ShardID]bool)
	}
	p.CompletedTraceSeries[group][segmentID][shardID] = true
}

// IsTraceSeriesCompleted checks if a specific series segment of a trace has been completed.
func (p *Progress) IsTraceSeriesCompleted(group string, segmentID string, shardID common.ShardID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedTraceSeries[group]; ok {
		if shards, ok := segments[segmentID]; ok {
			return shards[shardID]
		}
	}
	return false
}

// SetTraceSeriesCount sets the total number of series segments for the current trace.
func (p *Progress) SetTraceSeriesCount(group string, totalSegments int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.TraceSeriesCounts[group] = totalSegments
}

// GetTraceSeriesCount gets the total number of series segments for the current trace.
func (p *Progress) GetTraceSeriesCount(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if count, ok := p.TraceSeriesCounts[group]; ok {
		return count
	}
	return 0
}

// GetTraceSeriesProgress gets the number of completed series segments for the current trace.
func (p *Progress) GetTraceSeriesProgress(group string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.TraceSeriesProgress[group]; ok {
		return progress
	}
	return 0
}

// MarkSourceStreamPartCompleted records that one source stream part finished
// every target write successfully; idempotent (++Progress on first call only).
func (p *Progress) MarkSourceStreamPartCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID, partID uint64) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedStreamParts[group] == nil {
		p.SourceCompletedStreamParts[group] = make(map[string]map[common.ShardID]map[uint64]bool)
	}
	if p.SourceCompletedStreamParts[group][sourceSegmentID] == nil {
		p.SourceCompletedStreamParts[group][sourceSegmentID] = make(map[common.ShardID]map[uint64]bool)
	}
	if p.SourceCompletedStreamParts[group][sourceSegmentID][sourceShardID] == nil {
		p.SourceCompletedStreamParts[group][sourceSegmentID][sourceShardID] = make(map[uint64]bool)
	}
	if !p.SourceCompletedStreamParts[group][sourceSegmentID][sourceShardID][partID] {
		p.SourceCompletedStreamParts[group][sourceSegmentID][sourceShardID][partID] = true
		p.StreamPartProgress[group]++
	}
}

// MarkSourceStreamSeriesCompleted records that one source stream series file finished
// every target write successfully; idempotent.
func (p *Progress) MarkSourceStreamSeriesCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedStreamSeries[group] == nil {
		p.SourceCompletedStreamSeries[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.SourceCompletedStreamSeries[group][sourceSegmentID] == nil {
		p.SourceCompletedStreamSeries[group][sourceSegmentID] = make(map[common.ShardID]bool)
	}
	if !p.SourceCompletedStreamSeries[group][sourceSegmentID][sourceShardID] {
		p.SourceCompletedStreamSeries[group][sourceSegmentID][sourceShardID] = true
		p.StreamSeriesProgress[group]++
	}
}

// MarkSourceStreamElementIndexCompleted records that one source stream element-index visit finished
// every target write successfully; idempotent.
func (p *Progress) MarkSourceStreamElementIndexCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedStreamElementIndex[group] == nil {
		p.SourceCompletedStreamElementIndex[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.SourceCompletedStreamElementIndex[group][sourceSegmentID] == nil {
		p.SourceCompletedStreamElementIndex[group][sourceSegmentID] = make(map[common.ShardID]bool)
	}
	if !p.SourceCompletedStreamElementIndex[group][sourceSegmentID][sourceShardID] {
		p.SourceCompletedStreamElementIndex[group][sourceSegmentID][sourceShardID] = true
		p.StreamElementIndexProgress[group]++
	}
}

// MarkSourceMeasurePartCompleted records that one source measure part finished
// every target write successfully; idempotent.
func (p *Progress) MarkSourceMeasurePartCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID, partID uint64) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedMeasureParts[group] == nil {
		p.SourceCompletedMeasureParts[group] = make(map[string]map[common.ShardID]map[uint64]bool)
	}
	if p.SourceCompletedMeasureParts[group][sourceSegmentID] == nil {
		p.SourceCompletedMeasureParts[group][sourceSegmentID] = make(map[common.ShardID]map[uint64]bool)
	}
	if p.SourceCompletedMeasureParts[group][sourceSegmentID][sourceShardID] == nil {
		p.SourceCompletedMeasureParts[group][sourceSegmentID][sourceShardID] = make(map[uint64]bool)
	}
	if !p.SourceCompletedMeasureParts[group][sourceSegmentID][sourceShardID][partID] {
		p.SourceCompletedMeasureParts[group][sourceSegmentID][sourceShardID][partID] = true
		p.MeasurePartProgress[group]++
	}
}

// MarkSourceMeasureSeriesCompleted records that one source measure series file finished
// every target write successfully; idempotent.
func (p *Progress) MarkSourceMeasureSeriesCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedMeasureSeries[group] == nil {
		p.SourceCompletedMeasureSeries[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.SourceCompletedMeasureSeries[group][sourceSegmentID] == nil {
		p.SourceCompletedMeasureSeries[group][sourceSegmentID] = make(map[common.ShardID]bool)
	}
	if !p.SourceCompletedMeasureSeries[group][sourceSegmentID][sourceShardID] {
		p.SourceCompletedMeasureSeries[group][sourceSegmentID][sourceShardID] = true
		p.MeasureSeriesProgress[group]++
	}
}

// MarkSourceTraceShardCompleted records that one source trace shard finished
// every target write successfully; idempotent.
func (p *Progress) MarkSourceTraceShardCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedTraceShards[group] == nil {
		p.SourceCompletedTraceShards[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.SourceCompletedTraceShards[group][sourceSegmentID] == nil {
		p.SourceCompletedTraceShards[group][sourceSegmentID] = make(map[common.ShardID]bool)
	}
	if !p.SourceCompletedTraceShards[group][sourceSegmentID][sourceShardID] {
		p.SourceCompletedTraceShards[group][sourceSegmentID][sourceShardID] = true
		p.TraceShardProgress[group]++
	}
}

// MarkSourceTraceSeriesCompleted records that one source trace series file finished
// every target write successfully; idempotent.
func (p *Progress) MarkSourceTraceSeriesCompleted(group string, sourceSegmentID string, sourceShardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.SourceCompletedTraceSeries[group] == nil {
		p.SourceCompletedTraceSeries[group] = make(map[string]map[common.ShardID]bool)
	}
	if p.SourceCompletedTraceSeries[group][sourceSegmentID] == nil {
		p.SourceCompletedTraceSeries[group][sourceSegmentID] = make(map[common.ShardID]bool)
	}
	if !p.SourceCompletedTraceSeries[group][sourceSegmentID][sourceShardID] {
		p.SourceCompletedTraceSeries[group][sourceSegmentID][sourceShardID] = true
		p.TraceSeriesProgress[group]++
	}
}
