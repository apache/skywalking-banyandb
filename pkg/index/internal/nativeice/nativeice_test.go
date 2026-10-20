// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

const repeatedOpenCount = 4

func TestOpenVisibleDocCount(t *testing.T) {
	directory, _ := writeCommittedIndex(t)

	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != 2 {
		t.Fatalf("VisibleDocCount() = %d, want 2", count)
	}
}

func TestPublishSnapshotRetainsExistingSegmentsAndOpensStrictly(t *testing.T) {
	directory := t.TempDir()
	firstGeneration := Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		{Identifier: []byte("old-live")},
		{Identifier: []byte("old-deleted"), Deleted: true},
	}}
	if encodeErr := Encode(directory, firstGeneration); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	firstReader, openErr := OpenStrict(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	firstMetadata := firstReader.SnapshotMetadata()
	if closeErr := firstReader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if len(firstMetadata.Segments) != 1 {
		t.Fatalf("first snapshot segments = %d, want 1", len(firstMetadata.Segments))
	}
	secondGeneration := Generation{SegmentID: 2, SnapshotID: 2, Documents: []EncodeDocument{{Identifier: []byte("new")}}}
	secondPayload, payloadErr := EncodeSegment(secondGeneration)
	if payloadErr != nil {
		t.Fatal(payloadErr)
	}
	secondMetadata := SnapshotSegment{ID: 2, Size: uint64(len(secondPayload)), DocumentCount: 1}
	if publishErr := PublishSnapshot(directory, 2, []SnapshotSegmentPayload{
		{SnapshotSegment: secondMetadata, Payload: secondPayload},
		{SnapshotSegment: firstMetadata.Segments[0], TrustedExisting: true},
	}); publishErr != nil {
		t.Fatal(publishErr)
	}
	reader, openErr := OpenStrict(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	metadata := reader.SnapshotMetadata()
	if metadata.ID != 2 || len(metadata.Segments) != 2 {
		t.Fatalf("snapshot metadata = %#v, want snapshot 2 with 2 segments", metadata)
	}
	if metadata.Segments[0].ID != 2 || metadata.Segments[1].ID != 1 {
		t.Fatalf("snapshot segment order = (%d, %d), want publication order (2, 1)", metadata.Segments[0].ID, metadata.Segments[1].ID)
	}
	physicalCount := 0
	deletedCount := 0
	if visitErr := reader.VisitPhysicalDocuments(context.Background(), func(_ StoredDocument, deleted bool) error {
		physicalCount++
		if deleted {
			deletedCount++
		}
		return nil
	}); visitErr != nil {
		t.Fatal(visitErr)
	}
	if physicalCount != 3 || deletedCount != 1 {
		t.Fatalf("physical documents = %d (deleted %d), want 3 (deleted 1)", physicalCount, deletedCount)
	}
}

func TestPublishSnapshotCopiesValidatedSourcePath(t *testing.T) {
	sourceDirectory := t.TempDir()
	destinationDirectory := t.TempDir()
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("source")}}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	sourcePath := filepath.Join(sourceDirectory, "incoming.seg")
	if writeErr := os.WriteFile(sourcePath, payload, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if publishErr := PublishSnapshot(destinationDirectory, 1, []SnapshotSegmentPayload{{
		SnapshotSegment: SnapshotSegment{ID: 1, Size: uint64(len(payload)), DocumentCount: 1}, SourcePath: sourcePath,
	}}); publishErr != nil {
		t.Fatal(publishErr)
	}
	reader, openErr := OpenStrict(destinationDirectory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	posting, found, postingErr := reader.TermPosting("_id", []byte("source"))
	if postingErr != nil || !found || !posting.OneHit {
		t.Fatalf("source posting = (%+v, %v, %v), want one hit", posting, found, postingErr)
	}
}

func TestPublishSnapshotReportsPostManifestSyncFailure(t *testing.T) {
	directory := t.TempDir()
	payload, payloadErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("live")}}})
	if payloadErr != nil {
		t.Fatal(payloadErr)
	}
	metadata := SnapshotSegment{ID: 1, Size: uint64(len(payload)), DocumentCount: 1}
	syncErr := errors.New("injected directory fsync failure")
	originalSync := syncNativeICEDirectoryForPublish
	syncCalls := 0
	syncNativeICEDirectoryForPublish = func(path string) error {
		syncCalls++
		if syncCalls == 2 {
			return syncErr
		}
		return originalSync(path)
	}
	t.Cleanup(func() { syncNativeICEDirectoryForPublish = originalSync })

	publishErr := PublishSnapshot(directory, 1, []SnapshotSegmentPayload{{SnapshotSegment: metadata, Payload: payload}})
	if publishErr == nil {
		t.Fatal("PublishSnapshot() error = nil, want post-link fsync error")
	}
	var typedErr *PublishError
	if !errors.As(publishErr, &typedErr) {
		t.Fatalf("PublishSnapshot() error = %T %v, want *PublishError", publishErr, publishErr)
	}
	if !typedErr.Published {
		t.Fatalf("PublishError.Published = false, want true after manifest link")
	}
	if !errors.Is(publishErr, syncErr) {
		t.Fatalf("PublishSnapshot() error = %v, want injected sync error", publishErr)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "000000000001.seg")); statErr != nil {
		t.Fatalf("post-link segment missing after uncertain publication: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "000000000001.snp")); statErr != nil {
		t.Fatalf("post-link manifest missing after uncertain publication: %v", statErr)
	}
	reader, openErr := OpenStrict(directory)
	if openErr != nil {
		t.Fatalf("OpenStrict() after post-link sync failure: %v", openErr)
	}
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
}

func TestPublishSnapshotCleansUpBeforeManifestLink(t *testing.T) {
	directory := t.TempDir()
	payload, payloadErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("live")}}})
	if payloadErr != nil {
		t.Fatal(payloadErr)
	}
	metadata := SnapshotSegment{ID: 1, Size: uint64(len(payload)), DocumentCount: 1}
	syncErr := errors.New("injected segment directory fsync failure")
	originalSync := syncNativeICEDirectoryForPublish
	syncNativeICEDirectoryForPublish = func(string) error { return syncErr }
	t.Cleanup(func() { syncNativeICEDirectoryForPublish = originalSync })

	publishErr := PublishSnapshot(directory, 1, []SnapshotSegmentPayload{{SnapshotSegment: metadata, Payload: payload}})
	if publishErr == nil {
		t.Fatal("PublishSnapshot() error = nil, want pre-link fsync error")
	}
	var typedErr *PublishError
	if !errors.As(publishErr, &typedErr) {
		t.Fatalf("PublishSnapshot() error = %T %v, want *PublishError", publishErr, publishErr)
	}
	if typedErr.Published {
		t.Fatalf("PublishError.Published = true before manifest link")
	}
	if !errors.Is(publishErr, syncErr) {
		t.Fatalf("PublishSnapshot() error = %v, want injected sync error", publishErr)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "000000000001.seg")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pre-link segment was not cleaned up, stat error = %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "000000000001.snp")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pre-link manifest exists, stat error = %v", statErr)
	}
}

func TestOpenStrictDoesNotFallBackFromNewestCorruptSnapshot(t *testing.T) {
	directory := t.TempDir()
	if encodeErr := Encode(directory, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{{Identifier: []byte("live")}}}); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(directory, "000000000002.snp"), []byte{snapshotVersion}, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, openErr := OpenStrict(directory); !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("OpenStrict() error = %v, want ErrCorrupt", openErr)
	}
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	if reader.SnapshotID() != 1 {
		t.Fatalf("fallback snapshot ID = %d, want 1", reader.SnapshotID())
	}
}

// TestPublishSnapshotProcessCuts terminates a real publisher at the two
// directory-sync boundaries. The hook is private to this package's tests, so
// production publication has no fault-injection surface.
func TestPublishSnapshotProcessCuts(t *testing.T) {
	if mode := os.Getenv("NATIVEICE_CRASH_HELPER"); mode != "" {
		runPublishSnapshotCrashHelper(t, mode, os.Getenv("NATIVEICE_CRASH_PATH"))
		return
	}
	for _, mode := range []string{"pre-manifest", "post-manifest"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			if encodeErr := Encode(directory, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{{Identifier: []byte("old-live")}}}); encodeErr != nil {
				t.Fatal(encodeErr)
			}
			//nolint:gosec // the subprocess is this test binary and the arguments
			// are fixed test flags.
			command := exec.Command(os.Args[0], "-test.run", "^TestPublishSnapshotProcessCuts$", "-test.v")
			command.Env = append(os.Environ(), "NATIVEICE_CRASH_HELPER="+mode, "NATIVEICE_CRASH_PATH="+directory)
			output, runErr := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 77 {
				t.Fatalf("crash helper error = %v, output = %s", runErr, output)
			}
			if mode == "pre-manifest" {
				reader, openErr := OpenStrict(directory)
				if openErr != nil {
					t.Fatalf("OpenStrict() after pre-manifest cut: %v", openErr)
				}
				defer func() { _ = reader.Close() }()
				if reader.SnapshotID() != 1 {
					t.Fatalf("pre-manifest snapshot ID = %d, want prior snapshot 1", reader.SnapshotID())
				}
				assertCrashPosting(t, reader, "old-live", true)
				assertCrashPosting(t, reader, "crash-live", false)
				return
			}
			reader, openErr := OpenStrict(directory)
			if openErr != nil {
				t.Fatalf("OpenStrict() after post-manifest cut: %v", openErr)
			}
			defer func() { _ = reader.Close() }()
			if reader.SnapshotID() != 2 {
				t.Fatalf("post-manifest snapshot ID = %d, want new snapshot 2", reader.SnapshotID())
			}
			assertCrashPosting(t, reader, "old-live", true)
			assertCrashPosting(t, reader, "crash-live", true)
		})
	}
}

func assertCrashPosting(t *testing.T, reader *Reader, identifier string, wantFound bool) {
	t.Helper()
	found := false
	visitErr := reader.VisitPhysicalDocuments(context.Background(), func(document StoredDocument, deleted bool) error {
		if deleted {
			return nil
		}
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if name == "_id" && string(value) == identifier {
				found = true
			}
			return true
		})
	})
	if visitErr != nil || found != wantFound {
		t.Fatalf("stored identifier %q = (found=%v, err=%v), want found=%v", identifier, found, visitErr, wantFound)
	}
}

func runPublishSnapshotCrashHelper(t *testing.T, mode, directory string) {
	t.Helper()
	payload, encodeErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte("crash-live")}}})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	metadata := SnapshotSegment{ID: 1, Size: uint64(len(payload)), DocumentCount: 1}
	oldInfo, statErr := os.Stat(filepath.Join(directory, nativeICEFileName(1, ".seg")))
	if statErr != nil {
		t.Fatal(statErr)
	}
	metadata.ID = 2
	oldMetadata := SnapshotSegment{ID: 1, Size: uint64(oldInfo.Size()), DocumentCount: 1}
	syncCalls := 0
	syncNativeICEDirectoryForPublish = func(path string) error {
		syncCalls++
		if (mode == "pre-manifest" && syncCalls == 1) || (mode == "post-manifest" && syncCalls == 2) {
			_ = path
			os.Exit(77)
		}
		return fs.SyncDir(path)
	}
	if publishErr := PublishSnapshot(directory, 2, []SnapshotSegmentPayload{
		{SnapshotSegment: metadata, Payload: payload},
		{SnapshotSegment: oldMetadata, TrustedExisting: true},
	}); publishErr != nil {
		t.Fatal(publishErr)
	}
	t.Fatal("crash helper returned without terminating")
}

func TestOpenSnapshotSegmentRetainsDiskFileAndMask(t *testing.T) {
	directory := t.TempDir()
	if encodeErr := Encode(directory, Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{
		{Identifier: []byte("live")}, {Identifier: []byte("deleted"), Deleted: true},
	}}); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := OpenStrict(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	metadata := reader.SnapshotMetadata().Segments[0]
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	segmentReader, segmentErr := OpenSnapshotSegment(filepath.Join(directory, "000000000001.seg"), metadata)
	if segmentErr != nil {
		t.Fatal(segmentErr)
	}
	defer func() { _ = segmentReader.Close() }()
	visible, visibleErr := segmentReader.VisibleDocCount()
	if visibleErr != nil {
		t.Fatal(visibleErr)
	}
	if visible != 1 {
		t.Fatalf("segment visible count = %d, want 1", visible)
	}
}

func TestDecodePrefixCodedInt64(t *testing.T) {
	value, decodeErr := DecodePrefixCodedInt64([]byte{0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64})
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if value != 100 {
		t.Fatalf("decoded timestamp = %d, want 100", value)
	}
	for _, want := range []int64{0, 1, 100, -1, -100, math.MaxInt64, math.MinInt64} {
		got, roundTripErr := DecodePrefixCodedInt64(EncodePrefixCodedInt64(want))
		if roundTripErr != nil || got != want {
			t.Fatalf("prefix-coded round trip %d = %d, err %v", want, got, roundTripErr)
		}
	}
	if _, decodeErr := DecodePrefixCodedInt64([]byte{0x20, 0x01}); !errors.Is(decodeErr, ErrCorrupt) {
		t.Fatalf("invalid timestamp error = %v, want ErrCorrupt", decodeErr)
	}
	overflow := []byte{0x20, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if _, decodeErr := DecodePrefixCodedInt64(overflow); !errors.Is(decodeErr, ErrCorrupt) {
		t.Fatalf("overflow timestamp error = %v, want ErrCorrupt", decodeErr)
	}
}

func TestPublishSnapshotRejectsConflictWithoutOrphaningSegment(t *testing.T) {
	directory := t.TempDir()
	generation := Generation{SegmentID: 1, SnapshotID: 1, Documents: []EncodeDocument{{Identifier: []byte("first")}}}
	payload, payloadErr := EncodeSegment(generation)
	if payloadErr != nil {
		t.Fatal(payloadErr)
	}
	metadata := SnapshotSegment{ID: 1, Size: uint64(len(payload)), DocumentCount: 1}
	if publishErr := PublishSnapshot(directory, 1, []SnapshotSegmentPayload{{SnapshotSegment: metadata, Payload: payload}}); publishErr != nil {
		t.Fatal(publishErr)
	}
	secondPayload, payloadErr := EncodeSegment(Generation{SegmentID: 2, Documents: []EncodeDocument{{Identifier: []byte("second")}}})
	if payloadErr != nil {
		t.Fatal(payloadErr)
	}
	conflictErr := PublishSnapshot(directory, 1, []SnapshotSegmentPayload{
		{SnapshotSegment: metadata},
		{SnapshotSegment: SnapshotSegment{ID: 2, Size: uint64(len(secondPayload)), DocumentCount: 1}, Payload: secondPayload},
	})
	if !errors.Is(conflictErr, ErrPublishConflict) {
		t.Fatalf("conflicting PublishSnapshot() error = %v, want ErrPublishConflict", conflictErr)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "000000000002.seg")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("conflicting publication left segment 2, stat error = %v", statErr)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".nativeice-") {
			t.Fatalf("conflicting publication left temporary file %q", entry.Name())
		}
	}
}

func TestNextPublicationIDsIncludesOrphans(t *testing.T) {
	directory := t.TempDir()
	segmentID, snapshotID, idErr := NextPublicationIDs(directory)
	if idErr != nil {
		t.Fatal(idErr)
	}
	if segmentID != 0 || snapshotID != 0 {
		t.Fatalf("empty directory IDs = (%d, %d), want (0, 0)", segmentID, snapshotID)
	}
	for _, name := range []string{"000000000009.seg", "000000000007.snp", "000000000003.seg", "ignored.seg.tmp"} {
		if writeErr := os.WriteFile(filepath.Join(directory, name), nil, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	segmentID, snapshotID, idErr = NextPublicationIDs(directory)
	if idErr != nil {
		t.Fatal(idErr)
	}
	if segmentID != 10 || snapshotID != 8 {
		t.Fatalf("next IDs = (%d, %d), want (10, 8)", segmentID, snapshotID)
	}
}

func TestOpenMissingReferencedSegmentIsCorrupt(t *testing.T) {
	directory, segmentPath := writeCommittedIndex(t)
	if removeErr := os.Remove(segmentPath); removeErr != nil {
		t.Fatal(removeErr)
	}

	_, openErr := Open(directory)
	if !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("Open() error = %v, want error wrapping ErrCorrupt", openErr)
	}
}

func TestOpenFallsBackToOlderStructurallyCompleteSnapshot(t *testing.T) {
	directory, _ := writeCommittedIndex(t)
	manifest := []byte{3, 1, 3, 'i', 'c', 'e', 0, 0, 0, 3, 3}
	metadata := make([]byte, 32)
	binary.BigEndian.PutUint64(metadata[8:16], 2)
	manifest = append(manifest, metadata...)
	manifest = append(manifest, 0, 0, 0, 0, 0)
	if writeErr := os.WriteFile(filepath.Join(directory, "000000000002.snp"), manifest, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != 2 {
		t.Fatalf("VisibleDocCount() = %d, want 2", count)
	}
}

func TestOpenReenumeratesAfterStaleListing(t *testing.T) {
	directory, _ := writeCommittedIndex(t)
	originalSnapshotPath := filepath.Join(directory, "000000000001.snp")
	manifest, readErr := os.ReadFile(originalSnapshotPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	listCount := 0
	reader, openErr := openWithSnapshots(directory, func(path string) ([]string, map[uint64]string, error) {
		snapshotPaths, segmentPaths, snapshotErr := committedSnapshots(path)
		if snapshotErr != nil {
			return nil, nil, snapshotErr
		}
		listCount++
		if listCount == 1 {
			if removeErr := os.Remove(originalSnapshotPath); removeErr != nil {
				t.Fatal(removeErr)
			}
			if writeErr := os.WriteFile(filepath.Join(directory, "000000000002.snp"), manifest, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		return snapshotPaths, segmentPaths, nil
	})
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if listCount != 2 {
		t.Fatalf("snapshot listings = %d, want 2", listCount)
	}
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != 2 {
		t.Fatalf("VisibleDocCount() = %d, want 2", count)
	}
}

func TestOpenRetainsLastCandidateFailure(t *testing.T) {
	directory, segmentPath := writeCommittedIndex(t)
	if removeErr := os.Remove(segmentPath); removeErr != nil {
		t.Fatal(removeErr)
	}

	_, openErr := Open(directory)
	if !errors.Is(openErr, ErrCorrupt) {
		t.Fatalf("Open() error = %v, want error wrapping ErrCorrupt", openErr)
	}
	if !strings.Contains(openErr.Error(), "snapshot references missing segment 2") {
		t.Fatalf("Open() error = %v, want missing segment rejection", openErr)
	}
	if !strings.Contains(openErr.Error(), "000000000001.snp") {
		t.Fatalf("Open() error = %v, want rejected snapshot path", openErr)
	}
}

func TestOpenCloseDoesNotLeakFileHandles(t *testing.T) {
	directory, _ := writeCommittedIndex(t)
	before := openFileDescriptorCount(t)
	for openIndex := 0; openIndex < repeatedOpenCount; openIndex++ {
		reader, openErr := Open(directory)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatalf("second Close() failed: %v", closeErr)
		}
	}
	after := openFileDescriptorCount(t)
	if after != before {
		t.Fatalf("file descriptors after Close() = %d, want %d", after, before)
	}
}

func TestParseSnapshotSegmentsClosesPinsAfterLaterRecordFails(t *testing.T) {
	directory, segmentPath := writeCommittedIndex(t)
	manifest, readErr := os.ReadFile(filepath.Join(directory, "000000000001.snp"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	record := manifest[2 : len(manifest)-4]
	missingRecord := append([]byte(nil), record...)
	missingRecord[8] = 99
	payload := append([]byte{snapshotVersion, 2}, record...)
	payload = append(payload, missingRecord...)
	payload = append(payload, make([]byte, 4)...)

	before := openFileDescriptorCount(t)
	_, _, parseErr := parseSnapshotSegments(map[uint64]string{2: segmentPath}, payload, func(record segmentRecord) (pinnedSegment, uint64, error) {
		return pinSegment(record, true)
	})
	if !errors.Is(parseErr, ErrCorrupt) {
		t.Fatalf("parseSnapshotSegments() error = %v, want ErrCorrupt", parseErr)
	}
	after := openFileDescriptorCount(t)
	if after != before {
		t.Fatalf("file descriptors after failed parse = %d, want %d", after, before)
	}
}

func writeCommittedIndex(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	segmentID := uint64(2)
	segmentPath := filepath.Join(directory, "000000000002.seg")
	segment := make([]byte, 76)
	footer := segment[len(segment)-60:]
	binary.BigEndian.PutUint64(footer[0:8], 2)
	binary.BigEndian.PutUint64(footer[8:16], 0)
	binary.BigEndian.PutUint64(footer[16:24], 16)
	binary.BigEndian.PutUint64(footer[24:32], 16)
	binary.BigEndian.PutUint32(footer[32:36], 1025)
	binary.BigEndian.PutUint32(footer[52:56], 3)
	if writeErr := os.WriteFile(segmentPath, segment, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	manifest := []byte{3, 1, 3, 'i', 'c', 'e', 0, 0, 0, 3, byte(segmentID)}
	metadata := make([]byte, 32)
	binary.BigEndian.PutUint64(metadata[0:8], uint64(len(segment)))
	binary.BigEndian.PutUint64(metadata[8:16], 2)
	manifest = append(manifest, metadata...)
	manifest = append(manifest, 0, 0, 0, 0, 0)
	if writeErr := os.WriteFile(filepath.Join(directory, "000000000001.snp"), manifest, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	return directory, segmentPath
}

func openFileDescriptorCount(t *testing.T) int {
	t.Helper()
	descriptors, readErr := os.ReadDir("/proc/self/fd")
	if errors.Is(readErr, os.ErrNotExist) {
		t.Skip("the operating system does not expose process file descriptors")
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	return len(descriptors)
}

// TestPersistedSegmentsHoldOneFileDescriptorWhileOpen keeps many small
// segments open and in use at once, the shape of a burst of tiny segments
// (for example OAP's schema-registry preload). Every persisted segment is
// read through its file, so each holds exactly one descriptor, and closing
// releases it.
func TestPersistedSegmentsHoldOneFileDescriptorWhileOpen(t *testing.T) {
	const segmentCount = 20
	directory := t.TempDir()
	readers := make([]*Reader, 0, segmentCount)
	t.Cleanup(func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	})
	before := openFileDescriptorCount(t)
	for segmentIndex := uint64(1); segmentIndex <= segmentCount; segmentIndex++ {
		segmentDir := filepath.Join(directory, fmt.Sprintf("%d", segmentIndex))
		if encodeErr := Encode(segmentDir, Generation{
			SegmentID: segmentIndex, SnapshotID: segmentIndex,
			Documents: []EncodeDocument{{Identifier: []byte("series")}},
		}); encodeErr != nil {
			t.Fatal(encodeErr)
		}
		reader, openErr := OpenStrict(segmentDir)
		if openErr != nil {
			t.Fatal(openErr)
		}
		readers = append(readers, reader)
	}
	after := openFileDescriptorCount(t)
	// Each OpenStrict also opens the .snp manifest transiently while
	// parsing, but that handle does not outlive the call.
	if after-before != segmentCount {
		t.Fatalf("file descriptors grew by %d across %d simultaneously open segments, want one per segment", after-before, segmentCount)
	}
	for segmentIndex, reader := range readers {
		var identifier []byte
		visitErr := reader.VisitDocument(0, func(document StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				if name == identifierField {
					identifier = append([]byte(nil), value...)
				}
				return true
			})
		})
		if visitErr != nil {
			t.Fatalf("segment %d: %v", segmentIndex, visitErr)
		}
		if string(identifier) != "series" {
			t.Fatalf("segment %d: identifier = %q, want %q", segmentIndex, identifier, "series")
		}
	}
	for _, reader := range readers {
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if closed := openFileDescriptorCount(t); closed != before {
		t.Fatalf("closing every segment left %d descriptors open", closed-before)
	}
}

func TestPublishSnapshotStagesManySegmentsWithOneDirectorySyncBeforeManifest(t *testing.T) {
	directory := t.TempDir()
	const segmentCount = 3*publishConcurrency + 1
	segments := make([]SnapshotSegmentPayload, segmentCount)
	for index := range segments {
		payload, payloadErr := EncodeSegment(Generation{Documents: []EncodeDocument{{Identifier: []byte(fmt.Sprintf("doc-%d", index))}}})
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		segments[index] = SnapshotSegmentPayload{
			SnapshotSegment: SnapshotSegment{ID: uint64(index + 1), Size: uint64(len(payload)), DocumentCount: 1},
			Payload:         payload,
		}
	}
	originalSync := syncNativeICEDirectoryForPublish
	syncCalls := 0
	syncNativeICEDirectoryForPublish = func(path string) error {
		syncCalls++
		return originalSync(path)
	}
	t.Cleanup(func() { syncNativeICEDirectoryForPublish = originalSync })

	if publishErr := PublishSnapshot(directory, 1, segments); publishErr != nil {
		t.Fatal(publishErr)
	}
	if syncCalls != 2 {
		t.Fatalf("directory syncs = %d, want one for the segments and one for the manifest", syncCalls)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".nativeice-") {
			t.Fatalf("publication left temporary file %q", entry.Name())
		}
	}
	reader, openErr := OpenStrict(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = reader.Close() }()
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != segmentCount {
		t.Fatalf("VisibleDocCount() = %d, want %d", count, segmentCount)
	}
}
