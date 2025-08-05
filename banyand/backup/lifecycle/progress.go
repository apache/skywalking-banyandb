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
	logger                      *logger.Logger                                             `json:"-"`
	CompletedGroups             map[string]bool                                            `json:"completed_groups"`
	DeletedStreamGroups         map[string]bool                                            `json:"deleted_stream_groups"`
	DeletedMeasureGroups        map[string]bool                                            `json:"deleted_measure_groups"`
	CompletedStreamParts        map[string]map[string]map[common.ShardID]map[uint64]bool   `json:"completed_stream_parts"`
	StreamPartErrors            map[string]map[string]map[common.ShardID]map[uint64]string `json:"stream_part_errors"`
	CompletedStreamSeries       map[string]map[string]map[common.ShardID]bool              `json:"completed_stream_series"`
	StreamSeriesErrors          map[string]map[string]map[common.ShardID]string            `json:"stream_series_errors"`
	CompletedStreamElementIndex map[string]map[string]map[common.ShardID]bool              `json:"completed_stream_element_index"`
	StreamElementIndexErrors    map[string]map[string]map[common.ShardID]string            `json:"stream_element_index_errors"`
	StreamPartCounts            map[string]int                                             `json:"stream_part_counts"`
	StreamPartProgress          map[string]int                                             `json:"stream_part_progress"`
	StreamSeriesCounts          map[string]int                                             `json:"stream_series_counts"`
	StreamSeriesProgress        map[string]int                                             `json:"stream_series_progress"`
	StreamElementIndexCounts    map[string]int                                             `json:"stream_element_index_counts"`
	StreamElementIndexProgress  map[string]int                                             `json:"stream_element_index_progress"`
	// Measure part-specific progress tracking
	CompletedMeasureParts  map[string]map[string]map[common.ShardID]map[uint64]bool   `json:"completed_measure_parts"`
	MeasurePartErrors      map[string]map[string]map[common.ShardID]map[uint64]string `json:"measure_part_errors"`
	MeasurePartCounts      map[string]int                                             `json:"measure_part_counts"`
	MeasurePartProgress    map[string]int                                             `json:"measure_part_progress"`
	CompletedMeasureSeries map[string]map[string]map[common.ShardID]bool              `json:"completed_measure_series"`
	MeasureSeriesErrors    map[string]map[string]map[common.ShardID]string            `json:"measure_series_errors"`
	MeasureSeriesCounts    map[string]int                                             `json:"measure_series_counts"`
	MeasureSeriesProgress  map[string]int                                             `json:"measure_series_progress"`
	progressFilePath       string                                                     `json:"-"`
	SnapshotStreamDir      string                                                     `json:"snapshot_stream_dir"`
	SnapshotMeasureDir     string                                                     `json:"snapshot_measure_dir"`
	mu                     sync.Mutex                                                 `json:"-"`
}

// AllGroupsFullyCompleted checks if all groups are fully completed.
func (p *Progress) AllGroupsFullyCompleted(groups []*commonv1.Group) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, group := range groups {
		if !p.CompletedGroups[group.Metadata.Name] {
			return false
		}
	}
	return true
}

// NewProgress creates a new Progress tracker.
func NewProgress(path string, l *logger.Logger) *Progress {
	return &Progress{
		CompletedGroups:             make(map[string]bool),
		DeletedStreamGroups:         make(map[string]bool),
		DeletedMeasureGroups:        make(map[string]bool),
		CompletedStreamParts:        make(map[string]map[string]map[common.ShardID]map[uint64]bool),
		StreamPartErrors:            make(map[string]map[string]map[common.ShardID]map[uint64]string),
		StreamPartCounts:            make(map[string]int),
		StreamPartProgress:          make(map[string]int),
		CompletedStreamSeries:       make(map[string]map[string]map[common.ShardID]bool),
		StreamSeriesErrors:          make(map[string]map[string]map[common.ShardID]string),
		StreamSeriesCounts:          make(map[string]int),
		StreamSeriesProgress:        make(map[string]int),
		CompletedStreamElementIndex: make(map[string]map[string]map[common.ShardID]bool),
		StreamElementIndexErrors:    make(map[string]map[string]map[common.ShardID]string),
		StreamElementIndexCounts:    make(map[string]int),
		StreamElementIndexProgress:  make(map[string]int),
		CompletedMeasureParts:       make(map[string]map[string]map[common.ShardID]map[uint64]bool),
		MeasurePartErrors:           make(map[string]map[string]map[common.ShardID]map[uint64]string),
		MeasurePartCounts:           make(map[string]int),
		MeasurePartProgress:         make(map[string]int),
		CompletedMeasureSeries:      make(map[string]map[string]map[common.ShardID]bool),
		MeasureSeriesErrors:         make(map[string]map[string]map[common.ShardID]string),
		MeasureSeriesCounts:         make(map[string]int),
		MeasureSeriesProgress:       make(map[string]int),
		progressFilePath:            path,
		logger:                      l,
	}
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

	// Update progress count
	p.StreamPartProgress[group]++
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

// MarkStreamPartError records an error for a specific part of a stream.
func (p *Progress) MarkStreamPartError(group string, segmentID string, shardID common.ShardID, partID uint64, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamPartErrors[group] == nil {
		p.StreamPartErrors[group] = make(map[string]map[common.ShardID]map[uint64]string)
	}
	if p.StreamPartErrors[group][segmentID] == nil {
		p.StreamPartErrors[group][segmentID] = make(map[common.ShardID]map[uint64]string)
	}
	if p.StreamPartErrors[group][segmentID][shardID] == nil {
		p.StreamPartErrors[group][segmentID][shardID] = make(map[uint64]string)
	}

	// Record the error
	p.StreamPartErrors[group][segmentID][shardID][partID] = errorMsg
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

	// Update progress count
	p.StreamSeriesProgress[group]++
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

// MarkStreamSeriesError records an error for a specific series segment of a stream.
func (p *Progress) MarkStreamSeriesError(group string, segmentID string, shardID common.ShardID, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamSeriesErrors[group] == nil {
		p.StreamSeriesErrors[group] = make(map[string]map[common.ShardID]string)
	}
	if p.StreamSeriesErrors[group][segmentID] == nil {
		p.StreamSeriesErrors[group][segmentID] = make(map[common.ShardID]string)
	}

	// Record the error
	p.StreamSeriesErrors[group][segmentID][shardID] = errorMsg
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

// GetStreamSeriesErrors returns all errors for a specific stream series.
func (p *Progress) GetStreamSeriesErrors(group string) map[string]map[common.ShardID]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.StreamSeriesErrors[group]; ok {
		// Return a copy to avoid concurrent access issues
		result := make(map[string]map[common.ShardID]string)
		for segmentID, shards := range segments {
			if shards != nil {
				result[segmentID] = make(map[common.ShardID]string)
				for shardID, errorMsg := range shards {
					result[segmentID][shardID] = errorMsg
				}
			}
		}
		return result
	}
	return nil
}

// ClearStreamSeriesErrors clears all errors for a specific stream series.
func (p *Progress) ClearStreamSeriesErrors(group string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.StreamSeriesErrors, group)
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

	// Update progress count
	p.StreamElementIndexProgress[group]++
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

// MarkStreamElementIndexError records an error for a specific element index file of a stream.
func (p *Progress) MarkStreamElementIndexError(group string, segmentID string, shardID common.ShardID, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamElementIndexErrors[group] == nil {
		p.StreamElementIndexErrors[group] = make(map[string]map[common.ShardID]string)
	}
	if p.StreamElementIndexErrors[group][segmentID] == nil {
		p.StreamElementIndexErrors[group][segmentID] = make(map[common.ShardID]string)
	}

	// Record the error
	p.StreamElementIndexErrors[group][segmentID][shardID] = errorMsg
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

	// Update progress count
	p.MeasurePartProgress[group]++
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

// MarkMeasurePartError records an error for a specific part of a measure.
func (p *Progress) MarkMeasurePartError(group string, segmentID string, shardID common.ShardID, partID uint64, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.MeasurePartErrors[group] == nil {
		p.MeasurePartErrors[group] = make(map[string]map[common.ShardID]map[uint64]string)
	}
	if p.MeasurePartErrors[group][segmentID] == nil {
		p.MeasurePartErrors[group][segmentID] = make(map[common.ShardID]map[uint64]string)
	}
	if p.MeasurePartErrors[group][segmentID][shardID] == nil {
		p.MeasurePartErrors[group][segmentID][shardID] = make(map[uint64]string)
	}

	// Record the error
	p.MeasurePartErrors[group][segmentID][shardID][partID] = errorMsg
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

// ClearErrors clears all errors for a specific group.
func (p *Progress) ClearErrors() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.StreamPartErrors = make(map[string]map[string]map[common.ShardID]map[uint64]string)
	p.StreamSeriesErrors = make(map[string]map[string]map[common.ShardID]string)
	p.StreamElementIndexErrors = make(map[string]map[string]map[common.ShardID]string)
	p.MeasurePartErrors = make(map[string]map[string]map[common.ShardID]map[uint64]string)
	p.MeasureSeriesErrors = make(map[string]map[string]map[common.ShardID]string)
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

	// Update progress count
	p.MeasureSeriesProgress[group]++
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

// MarkMeasureSeriesError records an error for a specific series segment of a measure.
func (p *Progress) MarkMeasureSeriesError(group string, segmentID string, shardID common.ShardID, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.MeasureSeriesErrors[group] == nil {
		p.MeasureSeriesErrors[group] = make(map[string]map[common.ShardID]string)
	}
	if p.MeasureSeriesErrors[group][segmentID] == nil {
		p.MeasureSeriesErrors[group][segmentID] = make(map[common.ShardID]string)
	}

	// Record the error
	p.MeasureSeriesErrors[group][segmentID][shardID] = errorMsg
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
