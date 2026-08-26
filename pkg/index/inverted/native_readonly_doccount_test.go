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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Footer geometry from BDB-NIDX-SPEC-001 revision 0.2 section 08: a segment
// ends in a fixed 60-byte big-endian footer whose fields appear in the order
// numDocs, storedIndex, fieldsIndex, docValues, chunkMode, timeMin, timeMax,
// version, reserved CRC32. Tests use it to damage one named field at a time.
const (
	iceFooterLength           = 60
	iceFooterFieldsIndexStart = 16
	iceFooterFieldsIndexEnd   = 24

	// oversizeSectionOffset is far past the end of any corpus file, yet small
	// enough that a reader which trusted it would succeed in allocating for
	// it. That separates "rejected the offset" from "died on the offset".
	oversizeSectionOffset = uint64(1) << 31

	// sectionAllocationCeiling bounds the heap one count of a 414-byte corpus
	// may consume. A reader that sized a buffer from an unchecked section
	// offset would pass it by orders of magnitude.
	sectionAllocationCeiling = uint64(64) << 20
)

// TestReadOnlyDocCountSingleSegment is the boundary contract for NIDX-01A. It
// exercises inverted.ReadOnlyDocCount, and nothing behind it, against the
// checked-in ICE v3 / snapshot v3 corpus produced through BanyanDB's
// compatibility writer boundary: one committed segment, numeric document IDs
// 11 and 12, no deletion record, and arbitrary non-zero bytes in every reserved
// CRC32 field.
//
// Requirements proved here:
//
//	R1 -- a committed single-segment generation counts 2, and the call leaves
//	      every path, byte, size, hash, mtime, and directory entry alone.
//	R3 -- a truncated footer and an out-of-bounds section offset each report
//	      the typed corruption sentinel, without panicking, aborting the
//	      process, hanging, or allocating past the configured section bound.
//	R4 -- the reserved CRC32 bytes decide neither the count nor the corruption
//	      classification, in either the segment footer or the manifest.
func TestReadOnlyDocCountSingleSegment(t *testing.T) {
	manifest := loadNIDX01AProvenance(t)

	t.Run("R1_committed_generation_counts_two_and_changes_nothing", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01aIndexDir)
		assertMatchesProvenance(t, dir, manifest)
		before := dirInventory(t, dir)

		observed := countInChildProcess(t, dir)

		tester.Empty(observed.Err)
		tester.True(observed.Succeeded)
		// 2 is the count declared by issue #14008 and recorded in the corpus
		// manifest: two documents were written and none was deleted.
		tester.Equal(manifest.VisibleCount, observed.Count)
		tester.Equal(nidx01aVisibleCount, observed.Count)
		tester.Equal(before, dirInventory(t, dir), "the read-only count must not disturb the directory")
	})

	t.Run("R3_truncated_footer_is_typed_corruption", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01aIndexDir)
		// Leave the segment shorter than the 60-byte footer a reader has to
		// bootstrap from, while the manifest still claims it is usable.
		tester.NoError(os.Truncate(newestSegmentFile(t, dir), iceFooterLength-20))
		before := dirInventory(t, dir)

		observed := countInChildProcess(t, dir)

		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
		tester.False(observed.NoCommitted, "damaged committed bytes are corruption, not an absent index")
		tester.Zero(observed.Count)
		tester.Equal(before, dirInventory(t, dir), "rejecting a damaged directory must not repair or rewrite it")
	})

	t.Run("R3_out_of_bounds_section_offset_is_typed_corruption", func(t *testing.T) {
		tester := require.New(t)
		dir := copyIndexDir(t, nidx01aIndexDir)
		setFieldsIndexOffset(t, newestSegmentFile(t, dir), oversizeSectionOffset)

		observed := countInChildProcess(t, dir)

		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
		tester.Zero(observed.Count)
		tester.Less(observed.AllocBytes, sectionAllocationCeiling,
			"a section offset past the end of the file must be rejected before it sizes an allocation")
	})

	t.Run("R4_reserved_crc32_bytes_decide_neither_count_nor_classification", func(t *testing.T) {
		tester := require.New(t)

		// A healthy generation whose reserved CRC32 slots hold different
		// arbitrary bytes still counts 2: the field is carried, never checked.
		healthy := copyIndexDir(t, nidx01aIndexDir)
		fillReservedCRC32(t, healthy, []byte{0x01, 0x02, 0x03, 0x04})
		observed := countInChildProcess(t, healthy)
		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.Equal(nidx01aVisibleCount, observed.Count)

		// Zeroing the same slots -- the one value a reader might read as "no
		// checksum recorded" -- changes nothing either.
		zeroed := copyIndexDir(t, nidx01aIndexDir)
		fillReservedCRC32(t, zeroed, []byte{0x00, 0x00, 0x00, 0x00})
		observed = countInChildProcess(t, zeroed)
		tester.True(observed.Succeeded, "want a count, got %q", observed.Err)
		tester.Equal(nidx01aVisibleCount, observed.Count)

		// Corruption stays corruption whatever the reserved bytes say, so the
		// classification cannot be coming from the CRC32 field.
		damaged := copyIndexDir(t, nidx01aIndexDir)
		setFieldsIndexOffset(t, newestSegmentFile(t, damaged), oversizeSectionOffset)
		fillReservedCRC32(t, damaged, []byte{0x01, 0x02, 0x03, 0x04})
		observed = countInChildProcess(t, damaged)
		tester.True(observed.Corrupt, "want the corruption sentinel, got %q", observed.Err)
	})
}

// assertMatchesProvenance checks that the corpus bytes under test still hash to
// what the provenance manifest recorded, so a failure caused by a damaged or
// accidentally regenerated corpus is reported as that rather than as a contract
// violation.
func assertMatchesProvenance(t *testing.T, dir string, manifest nidx01aProvenance) {
	t.Helper()
	require.Len(t, manifest.FileSHA256, 2, "the manifest must pin every corpus file")
	for name, want := range manifest.FileSHA256 {
		payload, err := os.ReadFile(filepath.Join(dir, filepath.Base(name)))
		require.NoError(t, err)
		sum := sha256.Sum256(payload)
		require.Equal(t, want, hex.EncodeToString(sum[:]), "corpus file %s does not match its manifest hash", name)
	}
}

// setFieldsIndexOffset rewrites the fieldsIndex root offset in a segment's
// footer, leaving every other byte of the file intact.
func setFieldsIndexOffset(t *testing.T, segmentPath string, offset uint64) {
	t.Helper()
	payload, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Greater(t, len(payload), iceFooterLength)
	footer := payload[len(payload)-iceFooterLength:]
	binary.BigEndian.PutUint64(footer[iceFooterFieldsIndexStart:iceFooterFieldsIndexEnd], offset)
	require.NoError(t, os.WriteFile(segmentPath, payload, 0o600))
}

// fillReservedCRC32 overwrites the trailing reserved CRC32 slot of every
// segment and manifest in dir with fill.
func fillReservedCRC32(t *testing.T, dir string, fill []byte) {
	t.Helper()
	require.Len(t, fill, 4)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		ext := filepath.Ext(entry.Name())
		if ext != segExt && ext != snpExt {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		payload, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		copy(payload[len(payload)-len(fill):], fill)
		require.NoError(t, os.WriteFile(path, payload, 0o600))
	}
}
