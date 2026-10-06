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
	"fmt"
	"path/filepath"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// Stats returns the current live document count and immutable segment bytes.
// It observes the current in-memory root and does not reopen a cold directory.
func (o *Owner) Stats() (dataCount int64, dataSizeBytes int64) {
	if o == nil {
		return 0, 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.root == nil {
		return 0, 0
	}
	for _, current := range o.root.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			continue
		}
		live := segment.handle.count - uint64(len(segment.deleted))
		dataCount += int64(live)
		dataSizeBytes += int64(segment.handle.size)
	}
	return dataCount, dataSizeBytes
}

// Reset publishes an empty root. Existing pinned views remain readable until
// they close; the reset itself is persisted through the same background
// worker as normal writes.
func (o *Owner) Reset() error {
	if o == nil {
		return nil
	}
	queue := o.persistQ
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		return ErrOwnerClosed
	}
	if o.durabilityFault != nil {
		fault := o.durabilityFault
		o.mu.Unlock()
		return fault
	}
	if o.root.generation == ^uint64(0) {
		o.mu.Unlock()
		return fmt.Errorf("native generation exhausted: %w", ErrInvalidDocument)
	}
	next := &publishedRoot{generation: o.root.generation + 1}
	next.refs.Store(1)
	old := o.root
	o.root = next
	o.admittedIdentifiers, o.admittedSegments = nil, nil
	o.roots[next] = struct{}{}
	old.release()
	o.pruneRootsLocked()
	if queue == nil {
		o.mu.Unlock()
		return nil
	}
	o.schedulePersistLocked(queue, nil, false)
	o.mu.Unlock()
	return nil
}

// TakeFileSnapshot publishes a copy of the current pinned root. Unlike a
// committed-directory copy, this includes admitted NRT segments even when
// their asynchronous persistence has not completed. The destination manifest
// is installed last by nativeice.PublishSnapshot.
func (o *Owner) TakeFileSnapshot(destination string) error {
	if o == nil || o.options.Path == "" {
		return ErrPersistenceConfiguration
	}
	o.mu.Lock()
	if o.closed || o.closing {
		o.mu.Unlock()
		return ErrOwnerClosed
	}
	if err := o.validateLease(); err != nil {
		o.mu.Unlock()
		return fmt.Errorf("validate native root lease for snapshot: %w", err)
	}
	o.activeOps++
	root := o.root
	root.refs.Add(1)
	o.mu.Unlock()
	defer o.endOperation()
	defer root.release()

	segments := make([]nativeice.SnapshotSegmentPayload, 0, len(root.segments))
	for _, current := range root.segments {
		segment, ok := current.(*memorySegment)
		if !ok {
			return fmt.Errorf("snapshot native root: unsupported segment type %T", current)
		}
		if segmentHasNoLiveDocuments(segment) {
			continue
		}
		metadata := nativeice.SnapshotSegment{
			ID: segment.handle.id, Size: segment.handle.size, DocumentCount: segment.handle.count,
			TimeMin: segment.handle.timeMin, TimeMax: segment.handle.timeMax,
		}
		if len(segment.deleted) != 0 {
			deleted := roaringpkg.New()
			for number := range segment.deleted {
				if number > uint64(^uint32(0)) {
					return fmt.Errorf("snapshot deletion %d exceeds mask range: %w", number, ErrInvalidDocument)
				}
				deleted.Add(uint32(number))
			}
			bitmap, marshalErr := deleted.MarshalBinary()
			if marshalErr != nil {
				return fmt.Errorf("snapshot deletion mask: %w", marshalErr)
			}
			metadata.DeletionBitmap = bitmap
		}
		payload := segment.handle.payload
		sourcePath := segment.handle.sourcePath
		if payload == nil && sourcePath == "" {
			var readErr error
			payload, readErr = fileSystem.Read(filepath.Join(o.options.Path, fmt.Sprintf("%012x.seg", segment.handle.id)))
			if readErr != nil {
				return fmt.Errorf("read persisted native segment %d for snapshot: %w", segment.handle.id, readErr)
			}
		}
		segments = append(segments, nativeice.SnapshotSegmentPayload{
			SnapshotSegment: metadata,
			Payload:         bytes.Clone(payload),
			SourcePath:      sourcePath,
		})
	}
	return nativeice.PublishSnapshot(destination, root.generation, segments)
}
