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

// mergeStagingPrefix names a merge output while it is written and until
// persistence publishes it. Like the ".native-external-" staging names it is
// never a "<hex>.seg" or "<hex>.snp" name, so garbage collection, publication
// identifier allocation, snapshot selection and backups never treat it as a
// segment, and the previous release's engine, which only lists names with
// those two extensions, ignores it.
//
// Publication renames the staged file to "<id>.seg", so no staged name
// outlives the publication of its segment; the reader keeps serving the same
// open file across the rename. The crash windows, and what is left behind by
// each, are:
//
//   - During the merge: the partial ".native-merge-<pid>-<n>" output. The
//     merge's spill files are unlinked as soon as they are created, so only
//     a crash inside that one create-then-unlink step can leave a
//     ".native-merge-<pid>-<n>.spill-<k>" name.
//   - After the merge, before persistence renames it: the complete staged
//     output, still under its staged name.
//   - After the rename, before the manifest is linked: an unreferenced
//     "<id>.seg". This is the same window every new segment's publication
//     already has (and the previous release's own persister has between
//     writing a segment and its snapshot). NextPublicationIDs counts it, so
//     its identifier is never reused, and CollectGarbage removes it once a
//     newer kept segment exists.
//
// A restarted owner removes every staged name under its lease, in NewOwner,
// before it can stage anything itself (removeStagedFiles). The previous
// release ignores those names, and an unreferenced "<id>.seg" it does not
// know: none of them is ever read, and its segment-number allocation lists
// every "<id>.seg", so it never reuses one either; they are removed with
// their storage segment's directory at retention.
const mergeStagingPrefix = ".native-merge-"

// stagingPrefixes are every staged or temporary name an owner, or the
// publisher it drives, creates in its directory.
var stagingPrefixes = []string{mergeStagingPrefix, externalStagingPrefix, publishTemporaryPrefix}

const (
	externalStagingPrefix  = ".native-external-"
	publishTemporaryPrefix = ".nativeice-"
)

// mergeStagingSequence names staged merge outputs uniquely within this
// process; the root lease keeps other processes out of the directory.
var mergeStagingSequence atomic.Uint64

// Test seams marking crash points: after a merge output is staged, and after
// persistence renamed staged segments but before their manifest.
var (
	mergeStagedHook    func(path string)
	persistRenamedHook func()
)

func (o *Owner) mergeStagingPath() string {
	return filepath.Join(o.options.Path, fmt.Sprintf("%s%d-%d", mergeStagingPrefix, os.Getpid(), mergeStagingSequence.Add(1)))
}

// removeStagedFiles deletes the staged and temporary files a previous process
// left behind. It runs only from NewOwner, after the root lease is
// validated, and before this owner can stage anything: a staged segment that
// was published already carries its "<id>.seg" name, and one that was not
// was never referenced by any manifest.
func removeStagedFiles(path string) error {
	if !fileSystem.IsExist(path) {
		return nil
	}
	entries, readErr := fileSystem.ReadDirLimit(path, 0)
	if readErr != nil {
		return fmt.Errorf("read native owner directory: %w", readErr)
	}
	var removeErr error
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !isStagedName(entry.Name()) {
			continue
		}
		removeErr = errors.Join(removeErr, fileSystem.DeleteFile(filepath.Join(path, entry.Name())))
	}
	return removeErr
}

func isStagedName(name string) bool {
	for _, prefix := range stagingPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// renameStagedSegment publishes a staged handle's file under its final
// "<id>.seg" name. It reports whether the handle's file now carries that
// name, including from an earlier attempt. The caller syncs the directory
// before linking a manifest that references it.
func (o *Owner) renameStagedSegment(handle *segmentHandle) (bool, error) {
	if !handle.staged {
		return false, nil
	}
	handle.pathMu.Lock()
	defer handle.pathMu.Unlock()
	if handle.sourcePath == "" {
		return true, nil
	}
	finalPath := filepath.Join(o.options.Path, fmt.Sprintf("%012x.seg", handle.id))
	// The lease keeps every other writer out of the directory, so nothing can
	// create the final name between this check and the rename.
	if fileSystem.IsExist(finalPath) {
		return false, fmt.Errorf("publish staged segment %d: %w", handle.id, nativeice.ErrPublishConflict)
	}
	// The reader renames its own file, so it keeps serving it -- across a
	// close and reopen on platforms that cannot rename an open file.
	if renameErr := handle.reader.RenameSegmentFile(finalPath); renameErr != nil {
		return false, fmt.Errorf("publish staged segment %d: %w", handle.id, renameErr)
	}
	handle.sourcePath = ""
	return true, nil
}

func (h *segmentHandle) currentSourcePath() string {
	h.pathMu.RLock()
	defer h.pathMu.RUnlock()
	return h.sourcePath
}

func (h *segmentHandle) clearSourcePath() string {
	h.pathMu.Lock()
	defer h.pathMu.Unlock()
	path := h.sourcePath
	h.sourcePath = ""
	return path
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
	if mergeStagedHook != nil {
		mergeStagedHook(stagedPath)
	}
	return segment, nil
}

func newSegmentFromMergeFile(path string, stats nativeice.MergeStats) (rootSegment, error) {
	reader, openErr := nativeice.OpenSegmentFile(path)
	if openErr != nil {
		return nil, fmt.Errorf("open merged native segment: %w", openErr)
	}
	fields, fieldsErr := reader.Fields()
	if fieldsErr != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("enumerate merged native segment fields: %w", fieldsErr)
	}
	handle := &segmentHandle{
		reader: reader, sourcePath: path, staged: true,
		count: stats.DocumentCount, size: stats.Size, indexedFields: fields,
		// The merge folds these from the surviving timestamped documents, so
		// unlike a flush segment a merged one always knows its bounds unless no
		// document carried a timestamp.
		timeMin: stats.TimeMin, timeMax: stats.TimeMax, hasTime: stats.HasTime,
	}
	handle.refs.Store(1)
	// Best effort, see newMemorySegment. A merged segment is exactly the case
	// this warm-up matters most for: it is opened once (here, before the
	// owner lock is taken) and then stays live for a long time.
	_ = reader.PrepareTermFilter(identifierField)
	return &memorySegment{handle: handle}, nil
}
