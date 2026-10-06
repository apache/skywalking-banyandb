// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// ErrExternalSegmentState reports an invalid receiver lifecycle transition.
var ErrExternalSegmentState = errors.New("native: invalid external segment state")

const externalStatusFailed = "failed"

// ExternalSegmentStreamer receives one complete immutable native ICE segment.
// Chunks are staged in a temporary file until CompleteSegment validates and
// atomically introduces the segment into the owner's next publication root.
type ExternalSegmentStreamer interface {
	StartSegment() error
	WriteChunk([]byte) error
	CompleteSegment() error
	Status() string
	BytesReceived() uint64
}

//nolint:govet // lifecycle fields are grouped by ownership and synchronization role.
type externalSegmentStreamer struct {
	owner      *Owner
	mu         sync.Mutex
	file       fs.File
	path       string
	bytes      uint64
	status     string
	started    bool
	completing bool
	done       chan struct{}
}

// EnableExternalSegments creates a native segment receiver. It does not
// publish a root until CompleteSegment succeeds.
func (o *Owner) EnableExternalSegments() (ExternalSegmentStreamer, error) {
	if o == nil || o.options.Path == "" || o.persistQ == nil {
		return nil, ErrPersistenceConfiguration
	}
	if err := o.validateLease(); err != nil {
		return nil, fmt.Errorf("validate native root lease: %w", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.closing {
		return nil, ErrOwnerClosed
	}
	streamer := &externalSegmentStreamer{owner: o, status: "idle", done: make(chan struct{})}
	o.externalMu.Lock()
	o.external[streamer] = struct{}{}
	o.externalMu.Unlock()
	return streamer, nil
}

func (s *externalSegmentStreamer) StartSegment() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.completing {
		return fmt.Errorf("start external segment: %w", ErrExternalSegmentState)
	}
	s.owner.mu.Lock()
	if s.owner.closed || s.owner.closing {
		s.owner.mu.Unlock()
		return ErrOwnerClosed
	}
	if err := s.owner.validateLease(); err != nil {
		s.owner.mu.Unlock()
		return fmt.Errorf("validate native root lease: %w", err)
	}
	if mkdirErr := fileSystem.MkdirAll(s.owner.options.Path, 0o750); mkdirErr != nil {
		s.owner.mu.Unlock()
		return fmt.Errorf("create native external staging directory: %w", mkdirErr)
	}
	stagedPath := filepath.Join(s.owner.options.Path, fmt.Sprintf(".native-external-%d-%d", os.Getpid(), externalStagingSequence.Add(1)))
	file, createErr := fileSystem.CreateFile(stagedPath, 0o600)
	if createErr != nil {
		s.owner.mu.Unlock()
		return fmt.Errorf("stage external segment: %w", createErr)
	}
	s.file, s.path, s.bytes = file, stagedPath, 0
	s.done = make(chan struct{})
	s.started, s.status = true, "receiving"
	s.owner.externalMu.Lock()
	s.owner.external[s] = struct{}{}
	s.owner.externalMu.Unlock()
	s.owner.mu.Unlock()
	return nil
}

func (s *externalSegmentStreamer) WriteChunk(chunk []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || s.completing || s.status != "receiving" || s.file == nil {
		return fmt.Errorf("write external segment: %w", ErrExternalSegmentState)
	}
	written, writeErr := s.file.Write(chunk)
	s.bytes += uint64(written)
	if writeErr != nil {
		s.status = externalStatusFailed
		return fmt.Errorf("write external segment: %w", writeErr)
	}
	if written != len(chunk) {
		s.status = externalStatusFailed
		return io.ErrShortWrite
	}
	return nil
}

func (s *externalSegmentStreamer) CompleteSegment() error {
	s.mu.Lock()
	if !s.started || s.completing || s.status != "receiving" || s.file == nil {
		s.mu.Unlock()
		return fmt.Errorf("complete external segment: %w", ErrExternalSegmentState)
	}
	s.completing = true
	file, stagedPath := s.file, s.path
	s.mu.Unlock()
	defer close(s.done)

	// Closing a written pkg/fs file fsyncs it before the descriptor is released.
	closeErr := file.Close()
	if closeErr == nil {
		closeErr = s.owner.introduceExternalSegment(context.Background(), stagedPath)
	}
	s.mu.Lock()
	s.file, s.path, s.started, s.completing = nil, "", false, false
	if closeErr != nil {
		s.status = externalStatusFailed
	} else {
		s.status = "complete"
	}
	s.mu.Unlock()
	if closeErr != nil {
		_ = fileSystem.DeleteFile(stagedPath)
		s.owner.removeExternalStreamer(s)
		return closeErr
	}
	s.owner.removeExternalStreamer(s)
	return nil
}

func (s *externalSegmentStreamer) abort() {
	s.mu.Lock()
	if s.completing {
		done := s.done
		s.mu.Unlock()
		<-done
		return
	}
	file, path := s.file, s.path
	s.file, s.path, s.started, s.completing, s.status = nil, "", false, false, externalStatusFailed
	s.mu.Unlock()
	if file != nil {
		_ = file.Close()
	}
	if path != "" {
		_ = fileSystem.DeleteFile(path)
	}
}

// externalStagingSequence names staged external segments uniquely within this
// process; the root lease keeps other processes out of the directory.
var externalStagingSequence atomic.Uint64

func (o *Owner) removeExternalStreamer(streamer *externalSegmentStreamer) {
	o.externalMu.Lock()
	delete(o.external, streamer)
	o.externalMu.Unlock()
}

func (o *Owner) abortExternalStreamers() {
	o.externalMu.Lock()
	streamers := make([]*externalSegmentStreamer, 0, len(o.external))
	for streamer := range o.external {
		streamers = append(streamers, streamer)
	}
	o.externalMu.Unlock()
	for _, streamer := range streamers {
		streamer.abort()
		o.removeExternalStreamer(streamer)
	}
}

func (s *externalSegmentStreamer) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *externalSegmentStreamer) BytesReceived() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func (o *Owner) introduceExternalSegment(ctx context.Context, stagedPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue := o.persistQ
	if queue == nil || o.options.Path == "" {
		return ErrPersistenceConfiguration
	}
	segmentReader, openErr := nativeice.OpenSegmentFile(stagedPath)
	if openErr != nil {
		return fmt.Errorf("validate external native segment: %w", openErr)
	}
	identifiers := make([][]byte, 0)
	var visitErr error
	if o.options.DeduplicateExternal {
		visitErr = segmentReader.VisitTerms(ctx, identifierField, func(identifier []byte) bool {
			identifiers = append(identifiers, bytes.Clone(identifier))
			return true
		})
	}
	metadata := segmentReader.SnapshotMetadata()
	closeErr := segmentReader.Close()
	if visitErr != nil {
		return fmt.Errorf("enumerate external native identifiers: %w", visitErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close external native segment: %w", closeErr)
	}
	if len(metadata.Segments) != 1 {
		return fmt.Errorf("external native segment has invalid metadata: %w", ErrCorrupt)
	}
	segmentMetadata := metadata.Segments[0]

	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		return ErrOwnerClosed
	}
	if o.collecting {
		o.mu.Unlock()
		return ErrPersistenceBusy
	}
	if o.durabilityFault != nil {
		fault := o.durabilityFault
		o.mu.Unlock()
		return fault
	}
	if err := o.validateLease(); err != nil {
		o.mu.Unlock()
		return fmt.Errorf("validate native root lease: %w", err)
	}
	if o.root.generation == ^uint64(0) || o.nextSegmentID == ^uint64(0) {
		o.mu.Unlock()
		return fmt.Errorf("external native segment identifiers exhausted: %w", ErrInvalidDocument)
	}
	external, segmentErr := newSegmentFromFile(stagedPath, segmentMetadata, o.nextSegmentID)
	if segmentErr != nil {
		o.mu.Unlock()
		return fmt.Errorf("open external native segment: %w", segmentErr)
	}
	next := &publishedRoot{generation: o.root.generation + 1, segments: append([]rootSegment(nil), o.root.segments...), nextNumber: o.root.nextNumber}
	next.refs.Store(1)
	for _, current := range next.segments {
		current.retain()
	}
	if o.options.DeduplicateExternal {
		for _, identifier := range identifiers {
			for index, current := range next.segments {
				updated, changed, deleteErr := current.Delete(identifier)
				if deleteErr != nil {
					releaseSegments(next.segments)
					external.release()
					o.mu.Unlock()
					return deleteErr
				}
				if changed {
					current.release()
					next.segments[index] = updated
				}
			}
		}
	}
	next.segments = append(next.segments, external)
	next.nextNumber += external.Len()
	old := o.root
	o.root = next
	o.roots[next] = struct{}{}
	o.nextSegmentID++
	old.release()
	o.pruneRootsLocked()
	o.schedulePersistLocked(queue, nil, false)
	o.mu.Unlock()
	o.requestMaintenance()
	return nil
}
