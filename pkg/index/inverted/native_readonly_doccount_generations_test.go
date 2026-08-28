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

package inverted

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// combinedGenerationCount is what a reader that summed generation 10's records
// together with generation 11's would report. It is stated here so an
// assertion that rejects it reads as the requirement it proves rather than as
// an arbitrary number.
const combinedGenerationCount = nidx01bVisibleCount + nidx01bRestoredCount

// TestReadOnlyDocCountCommittedGenerations is the boundary contract for
// NIDX-01B. It exercises the committed-generation counter at
// inverted.ReadOnlyDocCount, and nothing behind it, against the checked-in ICE
// v3 / snapshot v3 corpus described by issue #14009: a valid generation 10 over
// two segments holding five physical documents with one snapshot deletion for
// doc-22, and an invalid newer generation 11 that references a segment the
// directory withholds.
//
// Every expected value is a literal from issue #14009 or from the corpus
// provenance manifest, which the compatibility writer's own serialization
// produced; none is recomputed the way the reader computes it.
//
// Requirements proved here:
//
//	R1 -- the newest structurally complete generation is selected and counted.
//	      That is generation 10 and the count is 4; restoring the segment
//	      generation 11 withholds moves the answer to generation 11 and 5;
//	      records from the two generations are never combined.
//	R2 -- the count comes from segment document counts and deletion masks
//	      alone, so damaged stored-document bytes change neither the answer nor
//	      the memory it costs.
//	R4 -- a missing segment with no complete generation behind it, an
//	      out-of-range deletion ordinal, a truncated deletion payload, and an
//	      unsatisfiable document count each produce a bounded, typed error.
//	      Reserved CRC32 bytes decide neither the count nor the classification.
func TestReadOnlyDocCountCommittedGenerations(t *testing.T) {
	manifest := loadNIDX01BProvenance(t)

	t.Run("corpus_matches_its_provenance_manifest", func(t *testing.T) {
		tester := require.New(t)
		assertMatchesNIDX01BProvenance(t, manifest)
		tester.Equal(nidx01bVisibleCount, manifest.VisibleCount)
		tester.Equal(nidx01bRestoredCount, manifest.RestoredCount)
		tester.Equal(nidx01bDeletedDocID, manifest.DeletedDocID)
		tester.Len(manifest.Generations, 2, "the corpus declares generation 10 and generation 11")

		selected := generationByID(t, manifest, nidx01bSelectedGeneration)
		tester.True(selected.StructurallyComplete, "generation 10 is the complete one")
		tester.Len(selected.Segments, nidx01bSelectedSegments)
		tester.Equal(nidx01bPhysicalDocuments, declaredPhysicalDocuments(selected))
		tester.Equal(nidx01bDeletions, declaredDeletions(selected),
			"generation 10 carries exactly the deletion of doc-22")

		newer := generationByID(t, manifest, nidx01bNewerGeneration)
		tester.False(newer.StructurallyComplete, "generation 11 references a segment index/ withholds")
		tester.Equal(nidx01bPhysicalDocuments, declaredPhysicalDocuments(newer))
		tester.Zero(declaredDeletions(newer))
		tester.Len(withheldSegments(newer), 1, "exactly one referenced segment is absent")
	})

	t.Run("R1_newest_complete_generation_counts_four", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01bIndexDir)
		before := dirInventory(t, dir)

		observed := countInChildProcess(t, dir)

		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		// 4 is the visible count issue #14009 declares for this corpus: five
		// physical documents across two segments, one of them deleted.
		tester.Equal(nidx01bVisibleCount, observed.Count)
		tester.Equal(before, dirInventory(t, dir),
			"selecting a generation must not add, remove, or rewrite an entry")
		assertNoReaderRuntimeFiles(t, dir)
	})

	t.Run("R1_never_combines_generations", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01bIndexDir)

		observed := countInChildProcess(t, dir)

		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.NotEqual(combinedGenerationCount, observed.Count,
			"a reader that merged generation 10's records with generation 11's would report 9")
		tester.Equal(nidx01bVisibleCount, observed.Count)
	})

	t.Run("R1_generation_10_alone_still_counts_four", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01bIndexDir)
		// Removing the incomplete newer generation leaves generation 10 as the
		// only candidate, so an unchanged answer pins which generation supplied
		// the four.
		tester.NoError(os.Remove(filepath.Join(dir, generationFileName(nidx01bNewerGeneration))))

		observed := countInChildProcess(t, dir)

		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.Equal(nidx01bVisibleCount, observed.Count)
	})

	t.Run("R1_newest_valid_generation_wins_once_it_is_complete", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01bIndexDir)
		restoreDetachedSegment(t, manifest, dir)

		observed := countInChildProcess(t, dir)

		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		// 5 is the count of generation 11 once it is complete: five physical
		// documents, none deleted. Selection is newest-valid, not oldest-valid.
		tester.Equal(nidx01bRestoredCount, observed.Count)
	})

	t.Run("R2_damaged_stored_document_bytes_do_not_change_the_count", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01bIndexDir)
		for _, segment := range generationByID(t, manifest, nidx01bSelectedGeneration).Segments {
			damageStoredDocumentRegion(t, filepath.Join(dir, segmentFileName(segment.ID)))
		}

		observed := countInChildProcess(t, dir)

		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.Equal(nidx01bVisibleCount, observed.Count,
			"a count built from segment counts and deletion masks cannot depend on stored document bytes")
		tester.Less(observed.AllocBytes, sectionAllocationCeiling,
			"counting must not materialize documents")
	})

	t.Run("R4_missing_segment_with_no_fallback_is_typed_corruption", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01bIndexDir)
		// Strip the one complete generation, leaving only the generation whose
		// referenced segment the directory withholds.
		tester.NoError(os.Remove(filepath.Join(dir, generationFileName(nidx01bSelectedGeneration))))

		observed := countInChildProcess(t, dir)

		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
		tester.Zero(observed.Count)
	})

	t.Run("R4_out_of_range_deletion_ordinal_is_typed_corruption", func(t *testing.T) {
		tester := require.New(t)
		dir := onlySelectedGeneration(t)
		deleted := deletedSegmentOf(t, manifest)
		// One past the last local document number the segment can hold.
		setDeletionPayload(t, selectedGenerationPath(dir), deleted.ID,
			encodeDeletedOrdinals(t, uint32(deleted.PhysicalDocuments)))

		observed := countInChildProcess(t, dir)

		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
		tester.Zero(observed.Count)
		tester.Less(observed.AllocBytes, sectionAllocationCeiling)
	})

	t.Run("R4_truncated_deletion_payload_is_typed_corruption", func(t *testing.T) {
		tester := require.New(t)
		dir := onlySelectedGeneration(t)
		deleted := deletedSegmentOf(t, manifest)
		whole := deletionPayloadOf(t, selectedGenerationPath(dir), deleted.ID)
		tester.NotEmpty(whole, "the selected generation must carry a deletion payload to truncate")
		setDeletionPayload(t, selectedGenerationPath(dir), deleted.ID, whole[:len(whole)/2])

		observed := countInChildProcess(t, dir)

		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
		tester.Zero(observed.Count)
		tester.Less(observed.AllocBytes, sectionAllocationCeiling)
	})

	t.Run("R4_unsatisfiable_document_count_is_bounded_typed_corruption", func(t *testing.T) {
		tester := require.New(t)
		dir := onlySelectedGeneration(t)
		deleted := deletedSegmentOf(t, manifest)
		// A per-segment count no arithmetic may carry: summing it would
		// overflow the signed total the boundary returns.
		setPhysicalDocumentCount(t, selectedGenerationPath(dir), deleted.ID, math.MaxUint64)

		observed := countInChildProcess(t, dir)

		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
		tester.Zero(observed.Count, "an unsatisfiable count must never surface as a wrapped or negative total")
		tester.Less(observed.AllocBytes, sectionAllocationCeiling)
	})

	t.Run("R4_reserved_crc32_decides_neither_count_nor_classification", func(t *testing.T) {
		tester := require.New(t)

		// Generation selection is unchanged by arbitrary reserved bytes...
		arbitrary := copyIndexDir(t, nidx01bIndexDir)
		fillReservedCRC32(t, arbitrary, []byte{0x01, 0x02, 0x03, 0x04})
		observed := countInChildProcess(t, arbitrary)
		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.Equal(nidx01bVisibleCount, observed.Count)

		// ...and by the one value a reader might read as "no checksum recorded".
		zeroed := copyIndexDir(t, nidx01bIndexDir)
		fillReservedCRC32(t, zeroed, []byte{0x00, 0x00, 0x00, 0x00})
		observed = countInChildProcess(t, zeroed)
		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.Equal(nidx01bVisibleCount, observed.Count)

		// Corruption stays corruption whatever the reserved bytes say, so the
		// classification cannot be coming from the CRC32 field. The selected
		// generation's own segment is left shorter than the footer a reader has
		// to bootstrap from, with no complete generation behind it.
		damaged := onlySelectedGeneration(t)
		tester.NoError(os.Truncate(filepath.Join(damaged, segmentFileName(deletedSegmentOf(t, manifest).ID)), iceFooterLength-20))
		fillReservedCRC32(t, damaged, []byte{0x01, 0x02, 0x03, 0x04})
		observed = countInChildProcess(t, damaged)
		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
	})
}

// TestReadOnlyDocCountHistoricalCorpusStillReads keeps the NIDX-01A corpus
// readable across the NIDX-01B change.
//
// Requirement proved here:
//
//	R5 -- a directory written before this milestone still opens and still
//	      reports the count it always reported. NIDX-01B emits no index bytes,
//	      so the only compatibility risk it carries is a reader that stops
//	      accepting older directories, and rolling back to the retained legacy
//	      reader must remain a code change alone.
func TestReadOnlyDocCountHistoricalCorpusStillReads(t *testing.T) {
	tester := require.New(t)
	dir := copyIndexDir(t, nidx01aIndexDir)
	before := dirInventory(t, dir)

	observed := countInChildProcess(t, dir)

	tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
	// 2 is the count declared for the single-segment corpus by issue #14008.
	tester.Equal(nidx01aVisibleCount, observed.Count)
	tester.Equal(before, dirInventory(t, dir))
	assertNoReaderRuntimeFiles(t, dir)
}

// onlySelectedGeneration copies the corpus and removes the incomplete newer
// generation, so a subtest damaging generation 10 has no complete generation to
// fall back to and the damage is what the boundary reports on.
func onlySelectedGeneration(t *testing.T) string {
	t.Helper()
	dir := copyIndexDir(t, nidx01bIndexDir)
	require.NoError(t, os.Remove(filepath.Join(dir, generationFileName(nidx01bNewerGeneration))))
	return dir
}

func selectedGenerationPath(dir string) string {
	return filepath.Join(dir, generationFileName(nidx01bSelectedGeneration))
}

// restoreDetachedSegment copies the segment generation 11 references, and the
// corpus withholds, into the index directory, completing that generation.
func restoreDetachedSegment(t *testing.T, manifest nidx01bProvenance, dir string) {
	t.Helper()
	require.NotEmpty(t, manifest.DetachedSegment, "the manifest must name the withheld segment")
	payload, err := os.ReadFile(filepath.Join(nidx01bRoot, manifest.DetachedSegment))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.Base(manifest.DetachedSegment)), payload, 0o600))
}

// damageStoredDocumentRegion flips bytes inside a segment's stored-document
// data, the region before the stored-fields index root the footer records. A
// reader that counts from segment accounting and deletion masks never looks
// there; a reader that decodes documents cannot survive it.
func damageStoredDocumentRegion(t *testing.T, segmentPath string) {
	t.Helper()
	payload, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Greater(t, len(payload), iceFooterLength)
	footer := payload[len(payload)-iceFooterLength:]
	storedIndex := binary.BigEndian.Uint64(footer[iceFooterStoredIndexStart:iceFooterStoredIndexEnd])
	require.Positive(t, storedIndex, "segment %s holds no stored document region to damage", segmentPath)
	require.LessOrEqual(t, storedIndex, uint64(len(payload)-iceFooterLength))
	for offset := range int(storedIndex) {
		payload[offset] ^= 0xFF
	}
	require.NoError(t, os.WriteFile(segmentPath, payload, 0o600))
}

// deletionPayloadOf returns the deletion payload a manifest records for one
// segment.
func deletionPayloadOf(t *testing.T, manifestPath string, segmentID uint64) []byte {
	t.Helper()
	_, record := readManifestRecord(t, manifestPath, segmentID)
	return record.deletionBytes
}

// setDeletionPayload rewrites the deletion payload a manifest records for one
// segment, adjusting the record's length prefix and leaving every other byte,
// including the reserved CRC32, untouched.
func setDeletionPayload(t *testing.T, manifestPath string, segmentID uint64, deletion []byte) {
	t.Helper()
	payload, record := readManifestRecord(t, manifestPath, segmentID)
	lengthPrefix := make([]byte, binary.MaxVarintLen64)
	lengthPrefix = lengthPrefix[:binary.PutUvarint(lengthPrefix, uint64(len(deletion)))]
	rewritten := make([]byte, 0, len(payload))
	rewritten = append(rewritten, payload[:record.deletionLenStart]...)
	rewritten = append(rewritten, lengthPrefix...)
	rewritten = append(rewritten, deletion...)
	rewritten = append(rewritten, payload[record.deletionEnd:]...)
	require.NoError(t, os.WriteFile(manifestPath, rewritten, 0o600))
}

// setPhysicalDocumentCount rewrites the physical document count a manifest
// records for one segment, leaving every other byte untouched.
func setPhysicalDocumentCount(t *testing.T, manifestPath string, segmentID, count uint64) {
	t.Helper()
	payload, record := readManifestRecord(t, manifestPath, segmentID)
	binary.BigEndian.PutUint64(payload[record.docCountOffset:record.docCountOffset+8], count)
	require.NoError(t, os.WriteFile(manifestPath, payload, 0o600))
}

// readManifestRecord returns a manifest's bytes and the record it holds for one
// segment.
func readManifestRecord(t *testing.T, manifestPath string, segmentID uint64) ([]byte, snapshotSegmentRecord) {
	t.Helper()
	payload, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	records, parseErr := parseCompatibilityManifest(payload)
	require.NoError(t, parseErr)
	for _, record := range records {
		if record.id == segmentID {
			return payload, record
		}
	}
	t.Fatalf("manifest %s holds no record for segment %d", manifestPath, segmentID)
	return nil, snapshotSegmentRecord{}
}

// assertNoReaderRuntimeFiles checks that a directory holds only committed index
// files and, at most, the index-local runtime file a writer owns. Counting is a
// read-only operation and must contribute nothing of its own.
func assertNoReaderRuntimeFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		extension := filepath.Ext(entry.Name())
		if extension == segExt || extension == snpExt || entry.Name() == LockFilename {
			continue
		}
		t.Fatalf("a read-only count left the runtime file %s in %s", entry.Name(), dir)
	}
}

// assertMatchesNIDX01BProvenance checks that the corpus bytes under test still
// hash to what the provenance manifest recorded, so a failure caused by a
// damaged or accidentally regenerated corpus is reported as that rather than as
// a contract violation.
func assertMatchesNIDX01BProvenance(t *testing.T, manifest nidx01bProvenance) {
	t.Helper()
	require.NotEmpty(t, manifest.FileSHA256, "the manifest must pin every corpus file")
	for name, want := range manifest.FileSHA256 {
		payload, err := os.ReadFile(filepath.Join(nidx01bRoot, name))
		require.NoError(t, err)
		sum := sha256.Sum256(payload)
		require.Equal(t, want, hex.EncodeToString(sum[:]), "corpus file %s does not match its manifest hash", name)
	}
	var checkedIn []string
	for _, root := range []string{nidx01bIndexDir, nidx01bDetachedDir} {
		entries, err := os.ReadDir(root)
		require.NoError(t, err)
		for _, entry := range entries {
			checkedIn = append(checkedIn, filepath.Base(root)+"/"+entry.Name())
		}
	}
	pinned := make([]string, 0, len(manifest.FileSHA256))
	for name := range manifest.FileSHA256 {
		pinned = append(pinned, name)
	}
	sort.Strings(checkedIn)
	sort.Strings(pinned)
	require.Equal(t, pinned, checkedIn, "every checked-in corpus file must be pinned by the manifest")
}

func generationByID(t *testing.T, manifest nidx01bProvenance, id uint64) nidx01bGeneration {
	t.Helper()
	for _, generation := range manifest.Generations {
		if generation.ID == id {
			return generation
		}
	}
	t.Fatalf("the provenance manifest declares no generation %d", id)
	return nidx01bGeneration{}
}

// deletedSegmentOf returns the selected generation's segment that carries the
// deletion of doc-22.
func deletedSegmentOf(t *testing.T, manifest nidx01bProvenance) nidx01bSegment {
	t.Helper()
	for _, segment := range generationByID(t, manifest, nidx01bSelectedGeneration).Segments {
		if len(segment.DeletedOrdinals) > 0 {
			return segment
		}
	}
	t.Fatal("the selected generation declares no deleted ordinal")
	return nidx01bSegment{}
}

func declaredPhysicalDocuments(generation nidx01bGeneration) uint64 {
	var total uint64
	for _, segment := range generation.Segments {
		total += segment.PhysicalDocuments
	}
	return total
}

func declaredDeletions(generation nidx01bGeneration) int {
	total := 0
	for _, segment := range generation.Segments {
		total += len(segment.DeletedOrdinals)
	}
	return total
}

func withheldSegments(generation nidx01bGeneration) []nidx01bSegment {
	var withheld []nidx01bSegment
	for _, segment := range generation.Segments {
		if !segment.PresentInIndexDir {
			withheld = append(withheld, segment)
		}
	}
	return withheld
}
