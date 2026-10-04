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
	if mkdirErr := os.MkdirAll(path, 0o755); mkdirErr != nil {
		return fmt.Errorf("create index directory %q: %w", path, mkdirErr)
	}
	manifestPath := filepath.Join(path, nativeICEFileName(snapshotID, ".snp"))
	if _, statErr := os.Stat(manifestPath); statErr == nil {
		return fmt.Errorf("snapshot %d already exists: %w", snapshotID, ErrPublishConflict)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect snapshot %q: %w", manifestPath, statErr)
	}

	linkedSegments := make([]string, 0, len(prepared))
	cleanup := func() error {
		var cleanupErr error
		for _, segmentPath := range linkedSegments {
			cleanupErr = errors.Join(cleanupErr, os.Remove(segmentPath))
		}
		return cleanupErr
	}
	for _, segment := range prepared {
		if segment.payload == nil && segment.source == "" {
			continue
		}
		segmentName := nativeICEFileName(segment.metadata.ID, ".seg")
		var linked bool
		var publishErr error
		if segment.source != "" {
			linked, publishErr = publishNativeICEFileFromPath(path, segmentName, segment.source)
		} else {
			linked, publishErr = publishNativeICEFileTracked(path, segmentName, segment.payload)
		}
		if linked {
			linkedSegments = append(linkedSegments, filepath.Join(path, segmentName))
		}
		if publishErr != nil {
			return &PublishError{
				SnapshotID: snapshotID,
				Err:        errors.Join(fmt.Errorf("publish segment %q: %w", segmentName, publishErr), cleanup()),
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
var syncNativeICEDirectoryForPublish = syncNativeICEDirectory

//nolint:govet // prepared fields mirror SnapshotSegmentPayload for validation.
type preparedSnapshotSegment struct {
	metadata SnapshotSegment
	payload  []byte
	source   string
	trusted  bool
}

func prepareSnapshotPublication(path string, segments []SnapshotSegmentPayload) ([]preparedSnapshotSegment, error) {
	prepared := make([]preparedSnapshotSegment, len(segments))
	for index, candidate := range segments {
		prepared[index] = preparedSnapshotSegment{metadata: SnapshotSegment{
			ID: candidate.ID, Size: candidate.Size, DocumentCount: candidate.DocumentCount,
			TimeMin: candidate.TimeMin, TimeMax: candidate.TimeMax,
			DeletionBitmap: append([]byte(nil), candidate.DeletionBitmap...),
		}, payload: candidate.Payload, source: candidate.SourcePath, trusted: candidate.TrustedExisting}
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
		if deletionErr := validateDeletionBitmap(segment.metadata); deletionErr != nil {
			return nil, deletionErr
		}
		segmentPath := filepath.Join(path, nativeICEFileName(segment.metadata.ID, ".seg"))
		if segment.payload != nil || segment.source != "" {
			if _, statErr := os.Stat(segmentPath); statErr == nil {
				return nil, fmt.Errorf("segment %d already exists: %w", segment.metadata.ID, ErrPublishConflict)
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return nil, fmt.Errorf("inspect segment %q: %w", segmentPath, statErr)
			}
			if segment.source != "" {
				if validationErr := validateExistingSegment(segment.source, segment.metadata); validationErr != nil {
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
	entryInfo, lstatErr := os.Lstat(path)
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

func publishNativeICEFileTracked(directory, name string, payload []byte) (bool, error) {
	temporaryFile, createErr := os.CreateTemp(directory, ".nativeice-")
	if createErr != nil {
		return false, fmt.Errorf("create temporary file: %w", createErr)
	}
	temporaryPath := temporaryFile.Name()
	removeTemporary := func() error {
		return errors.Join(temporaryFile.Close(), os.Remove(temporaryPath))
	}
	if writeErr := writeNativeICEFile(temporaryFile, payload); writeErr != nil {
		return false, errors.Join(fmt.Errorf("write temporary file: %w", writeErr), removeTemporary())
	}
	if syncErr := temporaryFile.Sync(); syncErr != nil {
		return false, errors.Join(fmt.Errorf("sync temporary file: %w", syncErr), removeTemporary())
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return false, errors.Join(fmt.Errorf("close temporary file: %w", closeErr), os.Remove(temporaryPath))
	}
	finalPath := filepath.Join(directory, name)
	if linkErr := os.Link(temporaryPath, finalPath); linkErr != nil {
		if errors.Is(linkErr, os.ErrExist) {
			return false, errors.Join(fmt.Errorf("%q already exists: %w", finalPath, ErrPublishConflict), os.Remove(temporaryPath))
		}
		return false, errors.Join(fmt.Errorf("link temporary file as %q: %w", finalPath, linkErr), os.Remove(temporaryPath))
	}
	if removeErr := os.Remove(temporaryPath); removeErr != nil {
		return true, errors.Join(fmt.Errorf("remove temporary file %q: %w", temporaryPath, removeErr), os.Remove(temporaryPath))
	}
	return true, syncNativeICEDirectoryForPublish(directory)
}

func publishNativeICEFileFromPath(directory, name, sourcePath string) (bool, error) {
	source, openErr := os.Open(sourcePath)
	if openErr != nil {
		return false, fmt.Errorf("open source segment %q: %w", sourcePath, openErr)
	}
	defer func() { _ = source.Close() }()
	temporaryFile, createErr := os.CreateTemp(directory, ".nativeice-")
	if createErr != nil {
		return false, fmt.Errorf("create temporary file: %w", createErr)
	}
	temporaryPath := temporaryFile.Name()
	removeTemporary := func() error {
		return errors.Join(temporaryFile.Close(), os.Remove(temporaryPath))
	}
	if _, copyErr := io.Copy(temporaryFile, source); copyErr != nil {
		return false, errors.Join(fmt.Errorf("copy source segment: %w", copyErr), removeTemporary())
	}
	if syncErr := temporaryFile.Sync(); syncErr != nil {
		return false, errors.Join(fmt.Errorf("sync temporary file: %w", syncErr), removeTemporary())
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return false, errors.Join(fmt.Errorf("close temporary file: %w", closeErr), os.Remove(temporaryPath))
	}
	finalPath := filepath.Join(directory, name)
	if linkErr := os.Link(temporaryPath, finalPath); linkErr != nil {
		if errors.Is(linkErr, os.ErrExist) {
			return false, errors.Join(fmt.Errorf("%q already exists: %w", finalPath, ErrPublishConflict), os.Remove(temporaryPath))
		}
		return false, errors.Join(fmt.Errorf("link temporary file as %q: %w", finalPath, linkErr), os.Remove(temporaryPath))
	}
	if removeErr := os.Remove(temporaryPath); removeErr != nil {
		return true, errors.Join(fmt.Errorf("remove temporary file %q: %w", temporaryPath, removeErr), os.Remove(temporaryPath))
	}
	return true, syncNativeICEDirectoryForPublish(directory)
}
