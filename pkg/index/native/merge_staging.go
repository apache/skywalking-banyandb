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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// mergeStagingPrefix names a merge output, and its spill files, while it is
// written and until the segment it backs is released. Like the
// ".native-external-" staging names it is never a "<hex>.seg" or "<hex>.snp"
// name, so garbage collection, publication identifier allocation, snapshot
// selection and backups never treat it as a segment. Publication hard-links
// the staged file into its "<id>.seg" name; the staged name itself is removed
// when its segment is released, or at the next owner start after a crash.
const mergeStagingPrefix = ".native-merge-"

// mergeStagingSequence names staged merge outputs uniquely within this
// process; the root lease keeps other processes out of the directory.
var mergeStagingSequence atomic.Uint64

func (o *Owner) mergeStagingPath() string {
	return filepath.Join(o.options.Path, fmt.Sprintf("%s%d-%d", mergeStagingPrefix, os.Getpid(), mergeStagingSequence.Add(1)))
}

// removeStagedMerges deletes the staged merge outputs a previous process left
// behind. It runs only from NewOwner, after the root lease is validated, and
// before any merge of this owner can stage a file: a staged output that was
// already published survives under its "<id>.seg" name, and one that was not
// published was never referenced by any manifest.
func removeStagedMerges(path string) error {
	if !fileSystem.IsExist(path) {
		return nil
	}
	entries, readErr := fileSystem.ReadDirLimit(path, 0)
	if readErr != nil {
		return fmt.Errorf("read native owner directory: %w", readErr)
	}
	var removeErr error
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), mergeStagingPrefix) {
			continue
		}
		removeErr = errors.Join(removeErr, fileSystem.DeleteFile(filepath.Join(path, entry.Name())))
	}
	return removeErr
}

// mergeToStagedSegment streams a merge into a staged file in the owner
// directory and opens it as a file-backed segment, so neither the merge nor
// the merged segment holds the segment's bytes in memory. The staged file is
// removed on every failure, and otherwise when the returned segment is
// released.
func (o *Owner) mergeToStagedSegment(ctx context.Context, inputs []nativeice.MergeInput) (rootSegment, error) {
	if mkdirErr := fileSystem.MkdirAll(o.options.Path, 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("create native owner directory: %w", mkdirErr)
	}
	stagedPath := o.mergeStagingPath()
	stats, mergeErr := nativeice.MergeSegmentsToFile(ctx, inputs, stagedPath)
	if mergeErr != nil {
		return nil, mergeErr
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, fileSystem.DeleteFile(stagedPath))
	}
	segment, openErr := newSegmentFromMergeFile(stagedPath, stats)
	if openErr != nil {
		return nil, errors.Join(openErr, fileSystem.DeleteFile(stagedPath))
	}
	return segment, nil
}

func newSegmentFromMergeFile(path string, stats nativeice.MergeStats) (rootSegment, error) {
	reader, openErr := nativeice.OpenSegmentFileOnDisk(path)
	if openErr != nil {
		return nil, fmt.Errorf("open merged native segment: %w", openErr)
	}
	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("enumerate merged native segment fields: %w", fieldsErr)
	}
	handle := &segmentHandle{
		reader: reader, sourcePath: path, linkSource: true,
		count: stats.DocumentCount, size: stats.Size, indexedFields: fields,
	}
	handle.refs.Store(1)
	// Best effort, see newMemorySegment. A merged segment is exactly the case
	// this warm-up matters most for: it is opened once (here, before the
	// owner lock is taken) and then stays live for a long time.
	_ = reader.PrepareTermFilter(identifierField)
	return &memorySegment{handle: handle}, nil
}
