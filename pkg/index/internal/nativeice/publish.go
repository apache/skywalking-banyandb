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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// ErrPublishConflict reports a snapshot or segment identifier that is already
// published in the destination directory. A conflicting publication never
// replaces an existing immutable file.
var ErrPublishConflict = errors.New("nativeice: publication conflict")

// PublishError describes a publication failure and whether its manifest was
// linked. Published is true only after the manifest link succeeds; a later
// directory-sync error therefore leaves an uncertain, already-visible
// generation and must not be cleaned up or blindly retried by the caller.
// Published is false for every failure before the manifest becomes visible.
//
//nolint:govet // the exported bool and wrapped error keep publication state explicit.
type PublishError struct {
	SnapshotID uint64
	Published  bool
	Err        error
}

func (e *PublishError) Error() string {
	if e == nil {
		return "<nil>"
	}
	state := "not published"
	if e.Published {
		state = "manifest linked"
	}
	return fmt.Sprintf("publish snapshot %d (%s): %v", e.SnapshotID, state, e.Err)
}

// Unwrap preserves the underlying filesystem or validation failure for
// errors.Is/errors.As callers.
func (e *PublishError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NextPublicationIDs returns the first unused segment and snapshot identifiers
// in path. It considers both committed and orphaned files, so an interrupted
// publication cannot cause a later write to reuse an immutable filename. Empty
// or absent directories return zero for each identifier.
func NextPublicationIDs(path string) (uint64, uint64, error) {
	entries, readErr := readDirectoryEntries(path)
	if errors.Is(readErr, ErrNoSnapshot) {
		return 0, 0, nil
	}
	if readErr != nil {
		return 0, 0, readErr
	}
	var maxSegment, maxSnapshot uint64
	var haveSegment, haveSnapshot bool
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if identifier, valid := parseFinalName(entry.Name(), ".seg"); valid {
			if !haveSegment || identifier > maxSegment {
				maxSegment, haveSegment = identifier, true
			}
			continue
		}
		if identifier, valid := parseFinalName(entry.Name(), ".snp"); valid && (!haveSnapshot || identifier > maxSnapshot) {
			maxSnapshot, haveSnapshot = identifier, true
		}
	}
	nextSegment, segmentErr := nextIdentifier(maxSegment, haveSegment)
	if segmentErr != nil {
		return 0, 0, fmt.Errorf("allocate segment identifier: %w", segmentErr)
	}
	nextSnapshot, snapshotErr := nextIdentifier(maxSnapshot, haveSnapshot)
	if snapshotErr != nil {
		return 0, 0, fmt.Errorf("allocate snapshot identifier: %w", snapshotErr)
	}
	return nextSegment, nextSnapshot, nil
}

func nextIdentifier(maximum uint64, found bool) (uint64, error) {
	if !found {
		return 0, nil
	}
	if maximum == ^uint64(0) {
		return 0, fmt.Errorf("identifier space exhausted: %w", ErrPublishConflict)
	}
	return maximum + 1, nil
}

// SnapshotSegmentPayload is one immutable segment reference in a new
// snapshot. A non-nil Payload is written as a new segment; a nil Payload
// references an already-published segment with the metadata in SnapshotSegment.
// Payload is consumed synchronously and is not retained by this package.
//
//nolint:govet // payload and metadata fields mirror the publication contract.
type SnapshotSegmentPayload struct {
	SnapshotSegment
	Payload []byte
	// SourcePath is a validated standalone segment file to copy as a new
	// immutable segment. It is mutually exclusive with Payload.
	SourcePath string
	// LinkSource publishes SourcePath by hard-linking it into place instead
	// of validating and copying it. It is valid only for a fsynced, immutable
	// source in the same directory that the caller has already opened and
	// validated itself, as an owner does with its merge output; publication
	// still checks the source's regular-file kind and size.
	LinkSource bool
	// TrustedExisting skips reparsing an existing segment's dictionaries. It
	// is valid only for a nil Payload after the caller has opened the same
	// immutable directory with OpenStrict and still holds the owner lease.
	// The publication path still checks regular-file kind and size; the owner
	// lease, not those checks, is what guarantees the already-validated file
	// identity remains immutable.
	TrustedExisting bool
}

// PublishSnapshot durably publishes one multi-segment ICE v3 snapshot. Every
// new segment is validated and linked before the snapshot manifest is linked,
// making the manifest the sole visibility point. Existing referenced segments
// are never rewritten. On a pre-manifest failure, segments linked by this call
// are removed while unrelated files and prior snapshots remain untouched.
// A write or sync failure is returned as *PublishError; when Published is true,
// the manifest link completed and the caller must reconcile or enter a
// terminal state without deleting the linked files.
//
// The caller owns Payload and must not mutate it until this function returns.
// A nil Payload means that the named segment must already exist, which lets a
// new root retain disk-backed segments without copying their bytes.
func PublishSnapshot(path string, snapshotID uint64, segments []SnapshotSegmentPayload) error {
	prepared, validationErr := prepareSnapshotPublication(path, segments)
	if validationErr != nil {
		return validationErr
	}
	if mkdirErr := segmentFileSystem.MkdirAll(path, 0o755); mkdirErr != nil {
		return fmt.Errorf("create index directory %q: %w", path, mkdirErr)
	}
	manifestPath := filepath.Join(path, nativeICEFileName(snapshotID, ".snp"))
	if segmentFileSystem.IsExist(manifestPath) {
		return fmt.Errorf("snapshot %d already exists: %w", snapshotID, ErrPublishConflict)
	}

	linkedSegments := make([]string, 0, len(prepared))
	cleanup := func() error {
		var cleanupErr error
		for _, segmentPath := range linkedSegments {
			cleanupErr = errors.Join(cleanupErr, segmentFileSystem.DeleteFile(segmentPath))
		}
		return cleanupErr
	}
	staged, stageErr := stageSnapshotSegments(path, prepared)
	if stageErr != nil {
		return &PublishError{SnapshotID: snapshotID, Err: stageErr}
	}
	for index, file := range staged {
		linked, linkErr := file.link()
		if linked {
			linkedSegments = append(linkedSegments, file.finalPath)
		}
		if linkErr != nil {
			discardErr := discardStagedFiles(staged[index+1:])
			return &PublishError{
				SnapshotID: snapshotID,
				Err:        errors.Join(fmt.Errorf("publish segment %q: %w", filepath.Base(file.finalPath), linkErr), cleanup(), discardErr),
			}
		}
	}
	// One directory sync makes every segment link durable before the
	// manifest that references them can be.
	if len(staged) > 0 {
		if syncErr := syncNativeICEDirectoryForPublish(path); syncErr != nil {
			return &PublishError{
				SnapshotID: snapshotID,
				Err:        errors.Join(fmt.Errorf("sync segment directory %q: %w", path, syncErr), cleanup()),
			}
		}
	}
	manifest := encodeNativeSnapshotSegments(prepared)
	manifestLinked, publishErr := publishNativeICEFileTracked(path, nativeICEFileName(snapshotID, ".snp"), manifest)
	if publishErr != nil {
		if manifestLinked {
			return &PublishError{
				SnapshotID: snapshotID,
				Published:  true,
				Err:        fmt.Errorf("publish snapshot %d after linking manifest: %w", snapshotID, publishErr),
			}
		}
		return &PublishError{
			SnapshotID: snapshotID,
			Err:        errors.Join(fmt.Errorf("publish snapshot %d: %w", snapshotID, publishErr), cleanup()),
		}
	}
	return nil
}

// syncNativeICEDirectoryForPublish is a narrow filesystem seam used by
// package tests to exercise the post-link fsync boundary. Production uses the
// real directory sync implementation.
var syncNativeICEDirectoryForPublish = fs.SyncDir

//nolint:govet // prepared fields mirror SnapshotSegmentPayload for validation.
type preparedSnapshotSegment struct {
	metadata SnapshotSegment
	payload  []byte
	source   string
	trusted  bool
	link     bool
}

func prepareSnapshotPublication(path string, segments []SnapshotSegmentPayload) ([]preparedSnapshotSegment, error) {
	prepared := make([]preparedSnapshotSegment, len(segments))
	for index, candidate := range segments {
		prepared[index] = preparedSnapshotSegment{metadata: SnapshotSegment{
			ID: candidate.ID, Size: candidate.Size, DocumentCount: candidate.DocumentCount,
			TimeMin: candidate.TimeMin, TimeMax: candidate.TimeMax,
			DeletionBitmap: append([]byte(nil), candidate.DeletionBitmap...),
		}, payload: candidate.Payload, source: candidate.SourcePath, trusted: candidate.TrustedExisting, link: candidate.LinkSource}
	}
	seenIDs := make(map[uint64]struct{}, len(prepared))
	for index := range prepared {
		if _, seen := seenIDs[prepared[index].metadata.ID]; seen {
			return nil, fmt.Errorf("segment %d is listed more than once: %w", prepared[index].metadata.ID, ErrPublishConflict)
		}
		seenIDs[prepared[index].metadata.ID] = struct{}{}
		segment := &prepared[index]
		if segment.trusted && (segment.payload != nil || segment.source != "") {
			return nil, fmt.Errorf("segment %d cannot mark a new payload as trusted", segment.metadata.ID)
		}
		if segment.payload != nil && segment.source != "" {
			return nil, fmt.Errorf("segment %d has both payload and source path: %w", segment.metadata.ID, ErrPublishConflict)
		}
		if segment.link && segment.source == "" {
			return nil, fmt.Errorf("segment %d links no source path: %w", segment.metadata.ID, ErrPublishConflict)
		}
		if deletionErr := validateDeletionBitmap(segment.metadata); deletionErr != nil {
			return nil, deletionErr
		}
		segmentPath := filepath.Join(path, nativeICEFileName(segment.metadata.ID, ".seg"))
		if segment.payload != nil || segment.source != "" {
			if segmentFileSystem.IsExist(segmentPath) {
				return nil, fmt.Errorf("segment %d already exists: %w", segment.metadata.ID, ErrPublishConflict)
			}
			if segment.source != "" {
				validate := validateExistingSegment
				if segment.link {
					validate = validateExistingSegmentMetadata
				}
				if validationErr := validate(segment.source, segment.metadata); validationErr != nil {
					return nil, validationErr
				}
			} else {
				if uint64(len(segment.payload)) != segment.metadata.Size {
					return nil, fmt.Errorf("segment %d payload size %d differs from metadata size %d: %w", segment.metadata.ID, len(segment.payload), segment.metadata.Size, ErrCorrupt)
				}
				if validationErr := validateSegmentPayload(segment.payload, segment.metadata); validationErr != nil {
					return nil, validationErr
				}
			}
			continue
		}
		var validationErr error
		if segment.trusted {
			validationErr = validateExistingSegmentMetadata(segmentPath, segment.metadata)
		} else {
			validationErr = validateExistingSegment(segmentPath, segment.metadata)
		}
		if validationErr != nil {
			return nil, validationErr
		}
	}
	return prepared, nil
}

func validateDeletionBitmap(metadata SnapshotSegment) error {
	deleted, deletionErr := deletionCount(metadata.DeletionBitmap, metadata.DocumentCount)
	if deletionErr != nil {
		return deletionErr
	}
	if deleted > metadata.DocumentCount {
		return corruptError("segment %d deletes more documents than it contains", metadata.ID)
	}
	return nil
}

func validateSegmentPayload(payload []byte, metadata SnapshotSegment) error {
	reader, readerErr := openSegmentBytes(payload, true)
	if readerErr != nil {
		return fmt.Errorf("validate segment %d payload: %w", metadata.ID, readerErr)
	}
	defer func() { _ = reader.Close() }()
	segment := reader.segments[0]
	segment.record.id = metadata.ID
	segment.record.documentCount = metadata.DocumentCount
	segment.record.timeMin = metadata.TimeMin
	segment.record.timeMax = metadata.TimeMax
	if validationErr := validatePinnedSegments([]pinnedSegment{segment}); validationErr != nil {
		return fmt.Errorf("validate segment %d payload: %w", metadata.ID, validationErr)
	}
	return nil
}

func validateExistingSegment(path string, metadata SnapshotSegment) error {
	reader, readerErr := OpenSnapshotSegment(path, metadata)
	if readerErr != nil {
		return fmt.Errorf("validate segment %d: %w", metadata.ID, readerErr)
	}
	if closeErr := reader.Close(); closeErr != nil {
		return fmt.Errorf("close segment %d after validation: %w", metadata.ID, closeErr)
	}
	return nil
}

func validateExistingSegmentMetadata(path string, metadata SnapshotSegment) error {
	entryInfo, lstatErr := segmentFileSystem.Lstat(path)
	if errors.Is(lstatErr, os.ErrNotExist) {
		return fmt.Errorf("snapshot references missing segment %d: %w", metadata.ID, ErrCorrupt)
	}
	if lstatErr != nil {
		return fmt.Errorf("inspect segment %q: %w", path, lstatErr)
	}
	if !entryInfo.Mode().IsRegular() {
		return fmt.Errorf("segment %q is not a regular file: %w", path, ErrCorrupt)
	}
	if uint64(entryInfo.Size()) != metadata.Size {
		return fmt.Errorf("segment %d size differs from metadata: %w", metadata.ID, ErrCorrupt)
	}
	return nil
}

func encodeNativeSnapshotSegments(segments []preparedSnapshotSegment) []byte {
	manifest := make([]byte, 0, 64)
	manifest = appendNativeUvarint(manifest, snapshotVersion)
	manifest = appendNativeUvarint(manifest, uint64(len(segments)))
	for _, segment := range segments {
		metadata := segment.metadata
		manifest = appendNativeUvarint(manifest, uint64(len("ice")))
		manifest = append(manifest, "ice"...)
		manifest = appendNativeUint32(manifest, segmentVersion)
		manifest = appendNativeUvarint(manifest, metadata.ID)
		manifest = appendNativeUint64(manifest, metadata.Size)
		manifest = appendNativeUint64(manifest, metadata.DocumentCount)
		manifest = appendNativeUint64(manifest, metadata.TimeMin)
		manifest = appendNativeUint64(manifest, metadata.TimeMax)
		manifest = appendNativeUvarint(manifest, uint64(len(metadata.DeletionBitmap)))
		manifest = append(manifest, metadata.DeletionBitmap...)
	}
	return appendNativeUint32(manifest, crc32.ChecksumIEEE(manifest))
}

// publishConcurrency bounds how many segment files one publication writes and
// fsyncs at once. Concurrent fsyncs share journal commits on the local file
// system, so a root with many new segments pays a few sync latencies instead
// of one per segment.
const publishConcurrency = 16

// stagedNativeICEFile is a written and fsynced temporary file awaiting its
// final name.
type stagedNativeICEFile struct {
	temporaryPath string
	finalPath     string
}

// stageSnapshotSegments writes and fsyncs every new segment of a publication
// under temporary names, concurrently. On failure no staged file remains.
func stageSnapshotSegments(directory string, prepared []preparedSnapshotSegment) ([]stagedNativeICEFile, error) {
	pending := make([]preparedSnapshotSegment, 0, len(prepared))
	for _, segment := range prepared {
		if segment.payload != nil || segment.source != "" {
			pending = append(pending, segment)
		}
	}
	staged := make([]stagedNativeICEFile, len(pending))
	stageErrs := make([]error, len(pending))
	slots := make(chan struct{}, publishConcurrency)
	var wait sync.WaitGroup
	for index := range pending {
		slots <- struct{}{}
		wait.Add(1)
		go func(index int) {
			defer func() {
				<-slots
				wait.Done()
			}()
			segment := pending[index]
			segmentName := nativeICEFileName(segment.metadata.ID, ".seg")
			var stageErr error
			switch {
			case segment.link:
				staged[index], stageErr = stageNativeICEFileLink(directory, segmentName, segment.source)
				if stageErr != nil {
					// A file system without hard links still publishes, by copy.
					staged[index], stageErr = stageNativeICEFileFromPath(directory, segmentName, segment.source)
				}
			case segment.source != "":
				staged[index], stageErr = stageNativeICEFileFromPath(directory, segmentName, segment.source)
			default:
				staged[index], stageErr = stageNativeICEFile(directory, segmentName, payloadWriter(segment.payload))
			}
			if stageErr != nil {
				stageErrs[index] = fmt.Errorf("publish segment %q: %w", segmentName, stageErr)
			}
		}(index)
	}
	wait.Wait()
	if joined := errors.Join(stageErrs...); joined != nil {
		succeeded := make([]stagedNativeICEFile, 0, len(staged))
		for index, file := range staged {
			if stageErrs[index] == nil {
				succeeded = append(succeeded, file)
			}
		}
		return nil, errors.Join(joined, discardStagedFiles(succeeded))
	}
	return staged, nil
}

func discardStagedFiles(files []stagedNativeICEFile) error {
	var discardErr error
	for _, file := range files {
		discardErr = errors.Join(discardErr, segmentFileSystem.DeleteFile(file.temporaryPath))
	}
	return discardErr
}

// link gives a staged file its final name and removes the temporary one. It
// reports whether the final name now exists.
func (f stagedNativeICEFile) link() (bool, error) {
	if linkErr := segmentFileSystem.CreateHardLink(f.temporaryPath, f.finalPath, nil); linkErr != nil {
		removeErr := segmentFileSystem.DeleteFile(f.temporaryPath)
		if errors.Is(linkErr, os.ErrExist) {
			return false, errors.Join(fmt.Errorf("%q already exists: %w", f.finalPath, ErrPublishConflict), removeErr)
		}
		return false, errors.Join(fmt.Errorf("link temporary file as %q: %w", f.finalPath, linkErr), removeErr)
	}
	if removeErr := segmentFileSystem.DeleteFile(f.temporaryPath); removeErr != nil {
		return true, fmt.Errorf("remove temporary file %q: %w", f.temporaryPath, removeErr)
	}
	return true, nil
}

func payloadWriter(payload []byte) func(io.Writer) error {
	return func(writer io.Writer) error {
		_, writeErr := writer.Write(payload)
		return writeErr
	}
}

func publishNativeICEFileTracked(directory, name string, payload []byte) (bool, error) {
	staged, stageErr := stageNativeICEFile(directory, name, payloadWriter(payload))
	if stageErr != nil {
		return false, stageErr
	}
	linked, linkErr := staged.link()
	if linkErr != nil {
		return linked, linkErr
	}
	return true, syncNativeICEDirectoryForPublish(directory)
}

// stageNativeICEFileLink stages an already fsynced source by hard-linking it
// under a temporary name, so the final link and the cleanup paths treat it
// exactly like a written temporary file. The source name is left in place;
// both names refer to the same immutable file.
func stageNativeICEFileLink(directory, name, sourcePath string) (stagedNativeICEFile, error) {
	temporaryPath := filepath.Join(directory, fmt.Sprintf(".nativeice-%d-%d", os.Getpid(), temporaryFileSequence.Add(1)))
	if linkErr := segmentFileSystem.CreateHardLink(sourcePath, temporaryPath, nil); linkErr != nil {
		return stagedNativeICEFile{}, fmt.Errorf("link source segment %q: %w", sourcePath, linkErr)
	}
	return stagedNativeICEFile{temporaryPath: temporaryPath, finalPath: filepath.Join(directory, name)}, nil
}

func stageNativeICEFileFromPath(directory, name, sourcePath string) (stagedNativeICEFile, error) {
	source, openErr := segmentFileSystem.OpenFile(sourcePath)
	if openErr != nil {
		return stagedNativeICEFile{}, fmt.Errorf("open source segment %q: %w", sourcePath, openErr)
	}
	defer func() { _ = source.Close() }()
	// The source keeps serving reads until its segment is released, so the
	// copy must not drop its pages from the page cache.
	fs.SetCached(source, true)
	return stageNativeICEFile(directory, name, func(writer io.Writer) error {
		reader := source.SequentialRead()
		_, copyErr := io.Copy(writer, reader)
		return errors.Join(copyErr, reader.Close())
	})
}

// temporaryFileSequence names publication temporaries uniquely within this
// process; the root lease keeps other processes out of the directory.
var temporaryFileSequence atomic.Uint64

// stageNativeICEFile writes a file under a temporary name and fsyncs it, so
// linking it as name never exposes a partial file. The written pages stay in
// the page cache: the published segment is read back by the next query or
// merge.
func stageNativeICEFile(directory, name string, write func(io.Writer) error) (stagedNativeICEFile, error) {
	temporaryPath := filepath.Join(directory, fmt.Sprintf(".nativeice-%d-%d", os.Getpid(), temporaryFileSequence.Add(1)))
	temporaryFile, createErr := segmentFileSystem.CreateFile(temporaryPath, 0o600)
	if createErr != nil {
		return stagedNativeICEFile{}, fmt.Errorf("create temporary file: %w", createErr)
	}
	fs.SetCached(temporaryFile, true)
	removeTemporary := func() error { return segmentFileSystem.DeleteFile(temporaryPath) }
	writer := temporaryFile.SequentialWrite()
	// Closing the sequential writer flushes and fsyncs the file.
	if writeErr := errors.Join(write(writer), writer.Close()); writeErr != nil {
		return stagedNativeICEFile{}, errors.Join(fmt.Errorf("write temporary file: %w", writeErr), temporaryFile.Close(), removeTemporary())
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return stagedNativeICEFile{}, errors.Join(fmt.Errorf("close temporary file: %w", closeErr), removeTemporary())
	}
	return stagedNativeICEFile{temporaryPath: temporaryPath, finalPath: filepath.Join(directory, name)}, nil
}
