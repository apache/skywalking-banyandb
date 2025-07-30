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
	logger                      *logger.Logger                       `json:"-"`
	CompletedGroups             map[string]bool                      `json:"completed_groups"`
	CompletedMeasures           map[string]map[string]bool           `json:"completed_measures"`
	DeletedStreamGroups         map[string]bool                      `json:"deleted_stream_groups"`
	DeletedMeasureGroups        map[string]bool                      `json:"deleted_measure_groups"`
	MeasureErrors               map[string]map[string]string         `json:"measure_errors"`
	MeasureCounts               map[string]map[string]int            `json:"measure_counts"`
	CompletedStreamParts        map[string]map[uint64]bool           `json:"completed_stream_parts"`
	StreamPartErrors            map[string]map[uint64]string         `json:"stream_part_errors"`
	CompletedStreamSeries       map[string]map[common.ShardID]bool   `json:"completed_stream_series"`
	StreamSeriesErrors          map[string]map[common.ShardID]string `json:"stream_series_errors"`
	CompletedStreamElementIndex map[string]bool                      `json:"completed_stream_element_index"`
	StreamElementIndexErrors    map[string]string                    `json:"stream_element_index_errors"`
	StreamPartCounts            map[string]int                       `json:"stream_part_counts"`
	StreamPartProgress          map[string]int                       `json:"stream_part_progress"`
	StreamSeriesCounts          map[string]int                       `json:"stream_series_counts"`
	StreamSeriesProgress        map[string]int                       `json:"stream_series_progress"`
	StreamElementIndexCounts    map[string]int                       `json:"stream_element_index_counts"`
	StreamElementIndexProgress  map[string]int                       `json:"stream_element_index_progress"`
	progressFilePath            string                               `json:"-"`
	SnapshotStreamDir           string                               `json:"snapshot_stream_dir"`
	SnapshotMeasureDir          string                               `json:"snapshot_measure_dir"`
	mu                          sync.Mutex                           `json:"-"`
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
		CompletedMeasures:           make(map[string]map[string]bool),
		DeletedStreamGroups:         make(map[string]bool),
		DeletedMeasureGroups:        make(map[string]bool),
		MeasureErrors:               make(map[string]map[string]string),
		MeasureCounts:               make(map[string]map[string]int),
		CompletedStreamParts:        make(map[string]map[uint64]bool),
		StreamPartErrors:            make(map[string]map[uint64]string),
		StreamPartCounts:            make(map[string]int),
		StreamPartProgress:          make(map[string]int),
		CompletedStreamSeries:       make(map[string]map[common.ShardID]bool),
		StreamSeriesErrors:          make(map[string]map[common.ShardID]string),
		StreamSeriesCounts:          make(map[string]int),
		StreamSeriesProgress:        make(map[string]int),
		CompletedStreamElementIndex: make(map[string]bool),
		StreamElementIndexErrors:    make(map[string]string),
		StreamElementIndexCounts:    make(map[string]int),
		StreamElementIndexProgress:  make(map[string]int),
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

// MarkMeasureCompleted marks a measure as completed.
func (p *Progress) MarkMeasureCompleted(group, measure string, count int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.CompletedMeasures[group] == nil {
		p.CompletedMeasures[group] = make(map[string]bool)
	}
	p.CompletedMeasures[group][measure] = true
	if p.MeasureCounts[group] == nil {
		p.MeasureCounts[group] = make(map[string]int)
	}
	p.MeasureCounts[group][measure] = count
}

// IsMeasureCompleted checks if a measure has been completed.
func (p *Progress) IsMeasureCompleted(group, measure string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if measures, ok := p.CompletedMeasures[group]; ok {
		return measures[measure]
	}
	return false
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

// ClearErrors resets all prior measure error records.
func (p *Progress) ClearErrors() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.MeasureErrors = make(map[string]map[string]string)
}

// MarkMeasureError records an error message for a specific measure.
func (p *Progress) MarkMeasureError(group, measure, msg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.MeasureErrors[group] == nil {
		p.MeasureErrors[group] = make(map[string]string)
	}
	p.MeasureErrors[group][measure] = msg
	if p.CompletedMeasures[group] == nil {
		p.CompletedMeasures[group] = make(map[string]bool)
	}
	p.CompletedMeasures[group][measure] = false
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
func (p *Progress) MarkStreamPartCompleted(group string, partID uint64) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamParts[group] == nil {
		p.CompletedStreamParts[group] = make(map[uint64]bool)
	}

	// Mark part as completed
	p.CompletedStreamParts[group][partID] = true

	// Update progress count
	if p.StreamPartProgress[group] == 0 {
		p.StreamPartProgress[group] = 0
	}
	p.StreamPartProgress[group]++
}

// IsStreamPartCompleted checks if a specific part of a stream has been completed.
func (p *Progress) IsStreamPartCompleted(group string, partID uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if parts, ok := p.CompletedStreamParts[group]; ok {
		return parts[partID]
	}
	return false
}

// MarkStreamPartError records an error for a specific part of a stream.
func (p *Progress) MarkStreamPartError(group string, partID uint64, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamPartErrors[group] == nil {
		p.StreamPartErrors[group] = make(map[uint64]string)
	}

	// Record the error
	p.StreamPartErrors[group][partID] = errorMsg
}

// SetStreamPartCount sets the total number of parts for a stream.
func (p *Progress) SetStreamPartCount(group string, totalParts int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamPartCounts[group] == 0 {
		p.StreamPartCounts[group] = 0
	}
	p.StreamPartCounts[group] = totalParts

	// Initialize progress tracking
	if p.StreamPartProgress[group] == 0 {
		p.StreamPartProgress[group] = 0
	}
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

// IsStreamFullyCompleted checks if all parts of a stream have been completed.
func (p *Progress) IsStreamFullyCompleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalParts := p.StreamPartCounts[group]
	completedParts := p.StreamPartProgress[group]

	return totalParts > 0 && completedParts >= totalParts
}

// GetStreamPartErrors returns all errors for a specific stream.
func (p *Progress) GetStreamPartErrors(group string) map[uint64]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamPartErrors[group]; ok {
		// Return a copy to avoid concurrent access issues
		result := make(map[uint64]string)
		for k, v := range errors {
			result[k] = v
		}
		return result
	}
	return nil
}

// ClearStreamPartErrors clears all errors for a specific stream.
func (p *Progress) ClearStreamPartErrors(group string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.StreamPartErrors, group)
}

// MarkStreamSeriesCompleted marks a specific series segment of a stream as completed.
func (p *Progress) MarkStreamSeriesCompleted(group string, shardID common.ShardID) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamSeries[group] == nil {
		p.CompletedStreamSeries[group] = make(map[common.ShardID]bool)
	}

	// Mark series segment as completed
	p.CompletedStreamSeries[group][shardID] = true

	// Update progress count
	if p.StreamSeriesProgress[group] == 0 {
		p.StreamSeriesProgress[group] = 0
	}
	p.StreamSeriesProgress[group]++
}

// IsStreamSeriesCompleted checks if a specific series segment of a stream has been completed.
func (p *Progress) IsStreamSeriesCompleted(group string, shardID common.ShardID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if segments, ok := p.CompletedStreamSeries[group]; ok {
		return segments[shardID]
	}
	return false
}

// MarkStreamSeriesError records an error for a specific series segment of a stream.
func (p *Progress) MarkStreamSeriesError(group string, shardID common.ShardID, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamSeriesErrors[group] == nil {
		p.StreamSeriesErrors[group] = make(map[common.ShardID]string)
	}

	// Record the error
	p.StreamSeriesErrors[group][shardID] = errorMsg
}

// SetStreamSeriesCount sets the total number of series segments for a stream.
func (p *Progress) SetStreamSeriesCount(group string, totalSegments int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamSeriesCounts[group] == 0 {
		p.StreamSeriesCounts[group] = 0
	}
	p.StreamSeriesCounts[group] = totalSegments

	// Initialize progress tracking
	if p.StreamSeriesProgress[group] == 0 {
		p.StreamSeriesProgress[group] = 0
	}
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

// IsStreamSeriesFullyCompleted checks if all series segments of a stream have been completed.
func (p *Progress) IsStreamSeriesFullyCompleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalSegments := p.StreamSeriesCounts[group]
	completedSegments := p.StreamSeriesProgress[group]

	return totalSegments > 0 && completedSegments >= totalSegments
}

// GetStreamSeriesErrors returns all errors for a specific stream series.
func (p *Progress) GetStreamSeriesErrors(group string) map[common.ShardID]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamSeriesErrors[group]; ok {
		// Return a copy to avoid concurrent access issues
		result := make(map[common.ShardID]string)
		for k, v := range errors {
			result[k] = v
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
func (p *Progress) MarkStreamElementIndexCompleted(group string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Mark group as completed
	p.CompletedStreamElementIndex[group] = true

	// Update progress count
	if p.StreamElementIndexProgress[group] == 0 {
		p.StreamElementIndexProgress[group] = 0
	}
	p.StreamElementIndexProgress[group]++
}

// IsStreamElementIndexCompleted checks if a specific element index file of a stream has been completed.
func (p *Progress) IsStreamElementIndexCompleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.CompletedStreamElementIndex[group]
}

// MarkStreamElementIndexError records an error for a specific element index file of a stream.
func (p *Progress) MarkStreamElementIndexError(group string, errorMsg string) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	// Record the error
	p.StreamElementIndexErrors[group] = errorMsg
}

// SetStreamElementIndexCount sets the total number of element index files for a stream.
func (p *Progress) SetStreamElementIndexCount(group string, totalIndexFiles int) {
	defer p.saveProgress()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamElementIndexCounts[group] == 0 {
		p.StreamElementIndexCounts[group] = 0
	}
	p.StreamElementIndexCounts[group] = totalIndexFiles

	// Initialize progress tracking
	if p.StreamElementIndexProgress[group] == 0 {
		p.StreamElementIndexProgress[group] = 0
	}
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

// IsStreamElementIndexFullyCompleted checks if all element index files of a stream have been completed.
func (p *Progress) IsStreamElementIndexFullyCompleted(group string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalIndexFiles := p.StreamElementIndexCounts[group]
	completedIndexFiles := p.StreamElementIndexProgress[group]

	return totalIndexFiles > 0 && completedIndexFiles >= totalIndexFiles
}

// GetStreamElementIndexErrors returns all errors for a specific stream element index.
func (p *Progress) GetStreamElementIndexErrors(group string) map[uint64]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errorMsg, ok := p.StreamElementIndexErrors[group]; ok && errorMsg != "" {
		// Return a map with a single entry since we now store errors at group level
		result := make(map[uint64]string)
		result[0] = errorMsg // Use 0 as a placeholder since we no longer track individual indexFileIDs
		return result
	}
	return nil
}

// ClearStreamElementIndexErrors clears all errors for a specific stream element index.
func (p *Progress) ClearStreamElementIndexErrors(group string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.StreamElementIndexErrors, group)
}
