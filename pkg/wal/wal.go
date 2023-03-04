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

// Package wal (Write-ahead logging) is an independent component to ensure data reliability.
package wal

import (
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
)

// SegmentID identities a segment in a WAL.
type SegmentID uint64

// Options for creating Write-ahead Logging.
type Options struct{}

// Segment allows reading underlying segments that hold WAl entities.
type Segment interface {
	GetSegmentID() SegmentID
}

// WAL denotes a Write-ahead logging.
// Modules who want their data reliable could write data to an instance of WAL.
// A WAL combines several segments, ingesting data on a single opened one.
// Rotating the WAL will create a new segment, marking it as opened and persisting previous segments on the disk.
type WAL interface {
	// Write a logging entity.
	// It will return immediately when the data is written in the buffer,
	// The returned function will be called when the entity is flushed on the persistent storage.
	Write(seriesID common.SeriesID, timestamp time.Time, data []byte) (func(), error)
	// Read specified segment by SegmentID.
	Read(segmentID SegmentID) (*Segment, error)
	// ReadAllSegments reads all segments sorted by their creation time in ascending order.
	ReadAllSegments() ([]*Segment, error)
	// Rotate closes the open segment and opens a new one, returning the closed segment details.
	Rotate() (*Segment, error)
	// Delete the specified segment.
	Delete(segmentID SegmentID) error
}

// New creates a WAL instance in the specified path.
func New(_ string, _ Options) (WAL, error) {
	return nil, nil
}
