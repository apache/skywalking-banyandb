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

package nativeice

import (
	"errors"
	"fmt"
	"os"
)

// OpenSegmentFile opens and validates one standalone immutable segment. It is
// used by external-segment ingestion before the segment is introduced into a
// published snapshot.
func OpenSegmentFile(path string) (*Reader, error) {
	return openStandaloneSegmentFile(path, true)
}

// OpenSegmentFileOnDisk opens and validates one standalone immutable segment
// like OpenSegmentFile, but never reads the segment into memory: the Reader
// serves every access from the file, however small it is. It is used for
// merge outputs, which should cost page cache rather than heap.
func OpenSegmentFileOnDisk(path string) (*Reader, error) {
	return openStandaloneSegmentFile(path, false)
}

func openStandaloneSegmentFile(path string, residentOK bool) (*Reader, error) {
	file, size, openErr := openSegmentFile(path)
	if openErr != nil {
		return nil, fmt.Errorf("open segment %q: %w", path, openErr)
	}
	footer, footerErr := readSegmentFooter(file, size, path)
	if closeErr := file.Close(); footerErr == nil && closeErr != nil {
		footerErr = closeErr
	}
	if footerErr != nil {
		return nil, footerErr
	}
	record := segmentRecord{path: path, documentCount: footer.documentCount, timeMin: footer.timeMin, timeMax: footer.timeMax}
	pinned, physicalCount, pinErr := pinSegment(record, residentOK)
	if pinErr != nil {
		return nil, pinErr
	}
	if physicalCount != footer.documentCount {
		_ = pinned.file.Close()
		return nil, corruptError("segment %q document count differs from footer", path)
	}
	if validationErr := validatePinnedSegments([]pinnedSegment{pinned}); validationErr != nil {
		_ = pinned.file.Close()
		return nil, validationErr
	}
	if physicalCount > ^uint64(0)>>1 {
		_ = pinned.file.Close()
		return nil, corruptError("segment %q document count overflows int64", path)
	}
	return &Reader{
		segments:        []pinnedSegment{pinned},
		visibleDocCount: int64(physicalCount),
	}, nil
}

// OpenSnapshotSegment opens one immutable segment named by snapshot metadata.
// The returned Reader retains the segment file descriptor and validates its
// footer, field index, stored sections, and deletion-mask bounds without
// copying the segment payload. The caller owns the Reader and must Close it.
// DeletionBitmap is retained in the Reader's private record; callers that need
// to apply masks to another root should keep their own copy of the metadata.
func OpenSnapshotSegment(path string, metadata SnapshotSegment) (*Reader, error) {
	entryInfo, lstatErr := segmentFileSystem.Lstat(path)
	if errors.Is(lstatErr, os.ErrNotExist) {
		return nil, corruptError("open missing segment %q", path)
	}
	if lstatErr != nil {
		return nil, fmt.Errorf("inspect segment %q: %w", path, lstatErr)
	}
	if !entryInfo.Mode().IsRegular() {
		return nil, corruptError("segment %q is not a regular file", path)
	}
	if metadata.Size != uint64(entryInfo.Size()) {
		return nil, corruptError("segment %d size differs from metadata", metadata.ID)
	}
	if deletionErr := validateDeletionBitmap(metadata); deletionErr != nil {
		return nil, deletionErr
	}
	record := segmentRecord{
		path: path, id: metadata.ID, documentCount: metadata.DocumentCount,
		timeMin: metadata.TimeMin, timeMax: metadata.TimeMax,
		deletionBitmap: append([]byte(nil), metadata.DeletionBitmap...),
	}
	pinned, physicalCount, pinErr := pinSegment(record, true)
	if pinErr != nil {
		return nil, pinErr
	}
	if physicalCount != metadata.DocumentCount {
		_ = pinned.file.Close()
		return nil, corruptError("segment %d document count differs from metadata", metadata.ID)
	}
	if validationErr := validatePinnedSegments([]pinnedSegment{pinned}); validationErr != nil {
		_ = pinned.file.Close()
		return nil, validationErr
	}
	if physicalCount > ^uint64(0)>>1 {
		_ = pinned.file.Close()
		return nil, corruptError("segment %d document count overflows int64", metadata.ID)
	}
	deletedCount, deletionErr := deletionCount(record.deletionBitmap, record.documentCount)
	if deletionErr != nil {
		_ = pinned.file.Close()
		return nil, deletionErr
	}
	visibleCount := physicalCount - deletedCount
	return &Reader{
		segments:        []pinnedSegment{pinned},
		visibleDocCount: int64(visibleCount),
	}, nil
}
