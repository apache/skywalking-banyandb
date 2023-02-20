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

// Options for Write-ahead Logging.
type Options struct{}

// Segment stands for a segment instance of Write-ahead log.
type Segment interface{}

// WAL includes exposed interfaces.
type WAL interface {
	// Write request to the WAL buffer.
	// It will return synchronously when the request write in the WAL buffer,
	// and trigger asynchronous callback when return when the buffer is flushed to disk successfully.
	Write(data []byte) (func(), error)
	// Read specified segment by index.
	Read(index int) (*Segment, error)
	// ReadAllSegments operation reads all segments.
	ReadAllSegments() ([]*Segment, error)
	// Rotate closes the open segment and opens a new one, returning the closed segment details.
	Rotate() (*Segment, error)
	// Delete the specified segment.
	Delete(index int) error
}

// New creates a Log instance in the specified root directory.
func New(_ string, _ *Options) (*WAL, error) {
	return nil, nil
}
