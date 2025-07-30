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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Progress tracks the lifecycle migration progress to support resume after crash.
type Progress struct {
	CompletedGroups      map[string]bool              `json:"completed_groups"`
	CompletedMeasures    map[string]map[string]bool   `json:"completed_measures"`
	DeletedStreamGroups  map[string]bool              `json:"deleted_stream_groups"`
	DeletedMeasureGroups map[string]bool              `json:"deleted_measure_groups"`
	MeasureErrors        map[string]map[string]string `json:"measure_errors"`
	MeasureCounts        map[string]map[string]int    `json:"measure_counts"`
	// Part-level tracking for stream migration
	CompletedStreamParts map[string]map[string]map[uint64]bool   `json:"completed_stream_parts"` // group -> stream -> partID -> completed
	StreamPartErrors     map[string]map[string]map[uint64]string `json:"stream_part_errors"`     // group -> stream -> partID -> error
	StreamPartCounts     map[string]map[string]int               `json:"stream_part_counts"`     // group -> stream -> total parts
	StreamPartProgress   map[string]map[string]int               `json:"stream_part_progress"`   // group -> stream -> completed parts count
	// Series-level tracking for stream migration
	CompletedStreamSeries map[string]map[string]map[uint64]bool   `json:"completed_stream_series"` // group -> stream -> segmentID -> completed
	StreamSeriesErrors    map[string]map[string]map[uint64]string `json:"stream_series_errors"`    // group -> stream -> segmentID -> error
	StreamSeriesCounts    map[string]map[string]int               `json:"stream_series_counts"`    // group -> stream -> total series segments
	StreamSeriesProgress  map[string]map[string]int               `json:"stream_series_progress"`  // group -> stream -> completed series segments count
	// Element index-level tracking for stream migration
	CompletedStreamElementIndex map[string]map[string]map[uint64]bool   `json:"completed_stream_element_index"` // group -> stream -> indexFileID -> completed
	StreamElementIndexErrors    map[string]map[string]map[uint64]string `json:"stream_element_index_errors"`    // group -> stream -> indexFileID -> error
	StreamElementIndexCounts    map[string]map[string]int               `json:"stream_element_index_counts"`    // group -> stream -> total index files
	StreamElementIndexProgress  map[string]map[string]int               `json:"stream_element_index_progress"`  // group -> stream -> completed index files count
	SnapshotStreamDir           string                                  `json:"snapshot_stream_dir"`
	SnapshotMeasureDir          string                                  `json:"snapshot_measure_dir"`
	mu                          sync.Mutex                              `json:"-"`
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
func NewProgress() *Progress {
	return &Progress{
		CompletedGroups:             make(map[string]bool),
		CompletedMeasures:           make(map[string]map[string]bool),
		DeletedStreamGroups:         make(map[string]bool),
		DeletedMeasureGroups:        make(map[string]bool),
		MeasureErrors:               make(map[string]map[string]string),
		MeasureCounts:               make(map[string]map[string]int),
		CompletedStreamParts:        make(map[string]map[string]map[uint64]bool),
		StreamPartErrors:            make(map[string]map[string]map[uint64]string),
		StreamPartCounts:            make(map[string]map[string]int),
		StreamPartProgress:          make(map[string]map[string]int),
		CompletedStreamSeries:       make(map[string]map[string]map[uint64]bool),
		StreamSeriesErrors:          make(map[string]map[string]map[uint64]string),
		StreamSeriesCounts:          make(map[string]map[string]int),
		StreamSeriesProgress:        make(map[string]map[string]int),
		CompletedStreamElementIndex: make(map[string]map[string]map[uint64]bool),
		StreamElementIndexErrors:    make(map[string]map[string]map[uint64]string),
		StreamElementIndexCounts:    make(map[string]map[string]int),
		StreamElementIndexProgress:  make(map[string]map[string]int),
	}
}

// LoadProgress loads progress from a file if it exists.
func LoadProgress(path string, l *logger.Logger) *Progress {
	if path == "" {
		return NewProgress()
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			l.Info().Msgf("No existing progress file at %s, starting fresh", path)
		} else {
			l.Warn().Err(err).Msgf("Failed to read progress file at %s, starting fresh", path)
		}
		return NewProgress()
	}

	progress := NewProgress()
	if err := json.Unmarshal(data, progress); err != nil {
		l.Warn().Err(err).Msgf("Failed to parse progress file at %s, starting fresh", path)
		return NewProgress()
	}

	l.Info().Msgf("Loaded existing progress from %s", path)
	return progress
}

// Save writes the progress to the specified file.
func (p *Progress) Save(path string, l *logger.Logger) {
	if path == "" {
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

// MarkGroupCompleted marks a group as completed.
func (p *Progress) MarkGroupCompleted(group string) {
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
func (p *Progress) MarkStreamPartCompleted(group, stream string, partID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamParts[group] == nil {
		p.CompletedStreamParts[group] = make(map[string]map[uint64]bool)
	}
	if p.CompletedStreamParts[group][stream] == nil {
		p.CompletedStreamParts[group][stream] = make(map[uint64]bool)
	}

	// Mark part as completed
	p.CompletedStreamParts[group][stream][partID] = true

	// Update progress count
	if p.StreamPartProgress[group] == nil {
		p.StreamPartProgress[group] = make(map[string]int)
	}
	p.StreamPartProgress[group][stream]++
}

// IsStreamPartCompleted checks if a specific part of a stream has been completed.
func (p *Progress) IsStreamPartCompleted(group, stream string, partID uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if streams, ok := p.CompletedStreamParts[group]; ok {
		if parts, ok := streams[stream]; ok {
			return parts[partID]
		}
	}
	return false
}

// MarkStreamPartError records an error for a specific part of a stream.
func (p *Progress) MarkStreamPartError(group, stream string, partID uint64, errorMsg string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamPartErrors[group] == nil {
		p.StreamPartErrors[group] = make(map[string]map[uint64]string)
	}
	if p.StreamPartErrors[group][stream] == nil {
		p.StreamPartErrors[group][stream] = make(map[uint64]string)
	}

	// Record the error
	p.StreamPartErrors[group][stream][partID] = errorMsg
}

// SetStreamPartCount sets the total number of parts for a stream.
func (p *Progress) SetStreamPartCount(group, stream string, totalParts int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamPartCounts[group] == nil {
		p.StreamPartCounts[group] = make(map[string]int)
	}
	p.StreamPartCounts[group][stream] = totalParts

	// Initialize progress tracking
	if p.StreamPartProgress[group] == nil {
		p.StreamPartProgress[group] = make(map[string]int)
	}
	if p.StreamPartProgress[group][stream] == 0 {
		p.StreamPartProgress[group][stream] = 0
	}
}

// GetStreamPartCount returns the total number of parts for a stream.
func (p *Progress) GetStreamPartCount(group, stream string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.StreamPartCounts[group]; ok {
		return counts[stream]
	}
	return 0
}

// GetStreamPartProgress returns the number of completed parts for a stream.
func (p *Progress) GetStreamPartProgress(group, stream string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.StreamPartProgress[group]; ok {
		return progress[stream]
	}
	return 0
}

// IsStreamFullyCompleted checks if all parts of a stream have been completed.
func (p *Progress) IsStreamFullyCompleted(group, stream string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalParts := p.StreamPartCounts[group][stream]
	completedParts := p.StreamPartProgress[group][stream]

	return totalParts > 0 && completedParts >= totalParts
}

// GetStreamPartErrors returns all errors for a specific stream.
func (p *Progress) GetStreamPartErrors(group, stream string) map[uint64]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamPartErrors[group]; ok {
		if streamErrors, ok := errors[stream]; ok {
			// Return a copy to avoid concurrent access issues
			result := make(map[uint64]string)
			for k, v := range streamErrors {
				result[k] = v
			}
			return result
		}
	}
	return nil
}

// ClearStreamPartErrors clears all errors for a specific stream.
func (p *Progress) ClearStreamPartErrors(group, stream string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamPartErrors[group]; ok {
		delete(errors, stream)
	}
}

// MarkStreamSeriesCompleted marks a specific series segment of a stream as completed.
func (p *Progress) MarkStreamSeriesCompleted(group, stream string, segmentID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamSeries[group] == nil {
		p.CompletedStreamSeries[group] = make(map[string]map[uint64]bool)
	}
	if p.CompletedStreamSeries[group][stream] == nil {
		p.CompletedStreamSeries[group][stream] = make(map[uint64]bool)
	}

	// Mark series segment as completed
	p.CompletedStreamSeries[group][stream][segmentID] = true

	// Update progress count
	if p.StreamSeriesProgress[group] == nil {
		p.StreamSeriesProgress[group] = make(map[string]int)
	}
	p.StreamSeriesProgress[group][stream]++
}

// IsStreamSeriesCompleted checks if a specific series segment of a stream has been completed.
func (p *Progress) IsStreamSeriesCompleted(group, stream string, segmentID uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if streams, ok := p.CompletedStreamSeries[group]; ok {
		if segments, ok := streams[stream]; ok {
			return segments[segmentID]
		}
	}
	return false
}

// MarkStreamSeriesError records an error for a specific series segment of a stream.
func (p *Progress) MarkStreamSeriesError(group, stream string, segmentID uint64, errorMsg string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamSeriesErrors[group] == nil {
		p.StreamSeriesErrors[group] = make(map[string]map[uint64]string)
	}
	if p.StreamSeriesErrors[group][stream] == nil {
		p.StreamSeriesErrors[group][stream] = make(map[uint64]string)
	}

	// Record the error
	p.StreamSeriesErrors[group][stream][segmentID] = errorMsg
}

// SetStreamSeriesCount sets the total number of series segments for a stream.
func (p *Progress) SetStreamSeriesCount(group, stream string, totalSegments int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamSeriesCounts[group] == nil {
		p.StreamSeriesCounts[group] = make(map[string]int)
	}
	p.StreamSeriesCounts[group][stream] = totalSegments

	// Initialize progress tracking
	if p.StreamSeriesProgress[group] == nil {
		p.StreamSeriesProgress[group] = make(map[string]int)
	}
	if p.StreamSeriesProgress[group][stream] == 0 {
		p.StreamSeriesProgress[group][stream] = 0
	}
}

// GetStreamSeriesCount returns the total number of series segments for a stream.
func (p *Progress) GetStreamSeriesCount(group, stream string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.StreamSeriesCounts[group]; ok {
		return counts[stream]
	}
	return 0
}

// GetStreamSeriesProgress returns the number of completed series segments for a stream.
func (p *Progress) GetStreamSeriesProgress(group, stream string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.StreamSeriesProgress[group]; ok {
		return progress[stream]
	}
	return 0
}

// IsStreamSeriesFullyCompleted checks if all series segments of a stream have been completed.
func (p *Progress) IsStreamSeriesFullyCompleted(group, stream string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalSegments := p.StreamSeriesCounts[group][stream]
	completedSegments := p.StreamSeriesProgress[group][stream]

	return totalSegments > 0 && completedSegments >= totalSegments
}

// GetStreamSeriesErrors returns all errors for a specific stream series.
func (p *Progress) GetStreamSeriesErrors(group, stream string) map[uint64]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamSeriesErrors[group]; ok {
		if streamErrors, ok := errors[stream]; ok {
			// Return a copy to avoid concurrent access issues
			result := make(map[uint64]string)
			for k, v := range streamErrors {
				result[k] = v
			}
			return result
		}
	}
	return nil
}

// ClearStreamSeriesErrors clears all errors for a specific stream series.
func (p *Progress) ClearStreamSeriesErrors(group, stream string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamSeriesErrors[group]; ok {
		delete(errors, stream)
	}
}

// MarkStreamElementIndexCompleted marks a specific element index file of a stream as completed.
func (p *Progress) MarkStreamElementIndexCompleted(group, stream string, indexFileID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.CompletedStreamElementIndex[group] == nil {
		p.CompletedStreamElementIndex[group] = make(map[string]map[uint64]bool)
	}
	if p.CompletedStreamElementIndex[group][stream] == nil {
		p.CompletedStreamElementIndex[group][stream] = make(map[uint64]bool)
	}

	// Mark element index file as completed
	p.CompletedStreamElementIndex[group][stream][indexFileID] = true

	// Update progress count
	if p.StreamElementIndexProgress[group] == nil {
		p.StreamElementIndexProgress[group] = make(map[string]int)
	}
	p.StreamElementIndexProgress[group][stream]++
}

// IsStreamElementIndexCompleted checks if a specific element index file of a stream has been completed.
func (p *Progress) IsStreamElementIndexCompleted(group, stream string, indexFileID uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if streams, ok := p.CompletedStreamElementIndex[group]; ok {
		if indexFiles, ok := streams[stream]; ok {
			return indexFiles[indexFileID]
		}
	}
	return false
}

// MarkStreamElementIndexError records an error for a specific element index file of a stream.
func (p *Progress) MarkStreamElementIndexError(group, stream string, indexFileID uint64, errorMsg string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Initialize nested maps if they don't exist
	if p.StreamElementIndexErrors[group] == nil {
		p.StreamElementIndexErrors[group] = make(map[string]map[uint64]string)
	}
	if p.StreamElementIndexErrors[group][stream] == nil {
		p.StreamElementIndexErrors[group][stream] = make(map[uint64]string)
	}

	// Record the error
	p.StreamElementIndexErrors[group][stream][indexFileID] = errorMsg
}

// SetStreamElementIndexCount sets the total number of element index files for a stream.
func (p *Progress) SetStreamElementIndexCount(group, stream string, totalIndexFiles int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.StreamElementIndexCounts[group] == nil {
		p.StreamElementIndexCounts[group] = make(map[string]int)
	}
	p.StreamElementIndexCounts[group][stream] = totalIndexFiles

	// Initialize progress tracking
	if p.StreamElementIndexProgress[group] == nil {
		p.StreamElementIndexProgress[group] = make(map[string]int)
	}
	if p.StreamElementIndexProgress[group][stream] == 0 {
		p.StreamElementIndexProgress[group][stream] = 0
	}
}

// GetStreamElementIndexCount returns the total number of element index files for a stream.
func (p *Progress) GetStreamElementIndexCount(group, stream string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if counts, ok := p.StreamElementIndexCounts[group]; ok {
		return counts[stream]
	}
	return 0
}

// GetStreamElementIndexProgress returns the number of completed element index files for a stream.
func (p *Progress) GetStreamElementIndexProgress(group, stream string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if progress, ok := p.StreamElementIndexProgress[group]; ok {
		return progress[stream]
	}
	return 0
}

// IsStreamElementIndexFullyCompleted checks if all element index files of a stream have been completed.
func (p *Progress) IsStreamElementIndexFullyCompleted(group, stream string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalIndexFiles := p.StreamElementIndexCounts[group][stream]
	completedIndexFiles := p.StreamElementIndexProgress[group][stream]

	return totalIndexFiles > 0 && completedIndexFiles >= totalIndexFiles
}

// GetStreamElementIndexErrors returns all errors for a specific stream element index.
func (p *Progress) GetStreamElementIndexErrors(group, stream string) map[uint64]string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamElementIndexErrors[group]; ok {
		if streamErrors, ok := errors[stream]; ok {
			// Return a copy to avoid concurrent access issues
			result := make(map[uint64]string)
			for k, v := range streamErrors {
				result[k] = v
			}
			return result
		}
	}
	return nil
}

// ClearStreamElementIndexErrors clears all errors for a specific stream element index.
func (p *Progress) ClearStreamElementIndexErrors(group, stream string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if errors, ok := p.StreamElementIndexErrors[group]; ok {
		delete(errors, stream)
	}
}
