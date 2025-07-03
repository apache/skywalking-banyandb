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
	CompletedStreams     map[string]map[string]bool   `json:"completed_streams"`
	CompletedMeasures    map[string]map[string]bool   `json:"completed_measures"`
	DeletedStreamGroups  map[string]bool              `json:"deleted_stream_groups"`
	DeletedMeasureGroups map[string]bool              `json:"deleted_measure_groups"`
	StreamErrors         map[string]map[string]string `json:"stream_errors"`
	MeasureErrors        map[string]map[string]string `json:"measure_errors"`
	StreamCounts         map[string]map[string]int    `json:"stream_counts"`
	MeasureCounts        map[string]map[string]int    `json:"measure_counts"`
	SnapshotStreamDir    string                       `json:"snapshot_stream_dir"`
	SnapshotMeasureDir   string                       `json:"snapshot_measure_dir"`
	mu                   sync.Mutex                   `json:"-"`
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
		CompletedGroups:      make(map[string]bool),
		CompletedStreams:     make(map[string]map[string]bool),
		CompletedMeasures:    make(map[string]map[string]bool),
		DeletedStreamGroups:  make(map[string]bool),
		DeletedMeasureGroups: make(map[string]bool),
		StreamErrors:         make(map[string]map[string]string),
		MeasureErrors:        make(map[string]map[string]string),
		StreamCounts:         make(map[string]map[string]int),
		MeasureCounts:        make(map[string]map[string]int),
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

// MarkStreamCompleted marks a stream as completed.
func (p *Progress) MarkStreamCompleted(group, stream string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.CompletedStreams[group] == nil {
		p.CompletedStreams[group] = make(map[string]bool)
	}
	p.CompletedStreams[group][stream] = true
	if p.StreamCounts[group] == nil {
		p.StreamCounts[group] = make(map[string]int)
	}
	p.StreamCounts[group][stream] = count
}

// IsStreamCompleted checks if a stream has been completed.
func (p *Progress) IsStreamCompleted(group, stream string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if streams, ok := p.CompletedStreams[group]; ok {
		return streams[stream]
	}
	return false
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

// ClearErrors resets all prior stream/measure error records.
func (p *Progress) ClearErrors() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.StreamErrors = make(map[string]map[string]string)
	p.MeasureErrors = make(map[string]map[string]string)
}

// MarkStreamError records an error message for a specific stream.
func (p *Progress) MarkStreamError(group, stream, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.StreamErrors[group] == nil {
		p.StreamErrors[group] = make(map[string]string)
	}
	p.StreamErrors[group][stream] = msg
	if p.CompletedStreams[group] == nil {
		p.CompletedStreams[group] = make(map[string]bool)
	}
	p.CompletedStreams[group][stream] = false
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
