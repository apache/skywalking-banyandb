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
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/icev3"
)

const (
	// fixtureRoot holds the legacy-writer corpus and its provenance manifest.
	// See pkg/index/inverted/internal/icev3/fixturegen for how it is produced.
	fixtureRoot = "testdata/icev3/single-segment"
	// fixtureCorpusDir is the corpus exactly as the pinned legacy writer left
	// it: one committed segment, no deletion record, arbitrary non-zero bytes
	// in every reserved CRC32 field.
	fixtureCorpusDir = fixtureRoot + "/index"
	// fixtureManifestPath records the corpus provenance.
	fixtureManifestPath = fixtureRoot + "/provenance.json"

	// fixtureVisibleCount is the visible document count the NIDX-01A
	// specification declares for this corpus as the literal 2. It is taken
	// from the issue text, never read back from the corpus or recomputed the
	// way the reader computes it.
	fixtureVisibleCount = int64(2)

	// callBudget bounds one ReadOnlyDocCount call. The work is a bounded read
	// of a few hundred bytes, so any call that outlives this is hung.
	callBudget = 30 * time.Second

	// segmentFooterLen is the fixed ICE v3 segment footer width, and
	// reservedCRCWidth the reserved CRC32 field at the end of both file kinds.
	// Both come from BDB-NIDX-SPEC-001 revision 0.2 section 08.
	segmentFooterLen = 60
	reservedCRCWidth = 4

	// oversizedSectionLength is the section length a malformed manifest
	// declares. It is far beyond icev3.MaxSectionBytes, so a reader that
	// allocates before validating is caught by the allocation budget.
	oversizedSectionLength = 1 << 30
)

// logicalFixtureDocuments are the document identities the NIDX-01A
// specification requires the corpus to contain.
var logicalFixtureDocuments = []string{"doc-11", "doc-12"}

// TestReadOnlyDocCountSingleSegment is the boundary contract for
// ReadOnlyDocCount over a single committed ICE v3 generation with no deletion
// mask. Every subtest drives the production seam ReadOnlyDocCount and asserts
// only what that seam observably does.
//
// R1 ReadOnlyDocCount returns the corpus's declared visible count with no
// error, acquires no bluge.pid, and leaves every path, byte, size, mode, mtime
// and directory entry of the corpus unchanged.
//
// R3 A copy whose segment footer is truncated, whose root section offset
// leaves the file, or whose segment version is not 3 yields an error matching
// icev3.ErrCorruptSegment, and does so without panicking, without hanging, and
// without allocating beyond icev3.MaxSectionBytes even when the bytes declare
// a section far larger than that.
//
// R4 The reserved CRC32 field of a segment file and of a snapshot manifest has
// no effect on the open, the count, or the corruption classification.
//
// R6 The checked-in corpus is the corpus its provenance manifest describes and
// the corpus the specification calls for.
//
// R7 A generation torn by a crash between publishing a manifest and completing
// it does not lose the previously committed count.
//
// R8 A directory that holds no committed generation reports a zero count with
// an error that is not a corruption classification, which is what the existing
// callers rely on to treat an unflushed index as empty.
func TestReadOnlyDocCountSingleSegment(t *testing.T) {
	t.Run("R1_legacy_corpus_reads_without_touching_it", func(t *testing.T) {
		before := inventoryOf(t, fixtureCorpusDir)
		require.NotEmpty(t, before, "the checked-in corpus is missing")

		outcome := callReadOnlyDocCount(t, fixtureCorpusDir)

		require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
		require.NoError(t, outcome.err)
		require.Equal(t, fixtureVisibleCount, outcome.count)
		assert.Equal(t, before, inventoryOf(t, fixtureCorpusDir),
			"ReadOnlyDocCount must not change any path, byte, size, mode, mtime or directory entry")
		assert.NoFileExists(t, filepath.Join(fixtureCorpusDir, LockFilename),
			"ReadOnlyDocCount must not acquire the exclusive directory lock")
	})

	t.Run("R3_malformed_copy_is_typed_and_bounded", func(t *testing.T) {
		for _, malformed := range []struct {
			mutate func(*testing.T, string)
			name   string
		}{
			{name: "segment_footer_truncated", mutate: truncateSegmentFooter},
			{name: "segment_root_offset_out_of_bounds", mutate: patchSegmentFieldsIndexOffsetBeyondFile},
			{name: "segment_version_unsupported", mutate: patchSegmentVersionToTwo},
			{name: "manifest_declares_oversized_deletion_bitmap", mutate: declareOversizedDeletionBitmap},
			{name: "manifest_truncated_mid_record", mutate: truncateOnlyManifest},
		} {
			t.Run(malformed.name, func(t *testing.T) {
				dir := copyCorpus(t)
				malformed.mutate(t, dir)

				outcome := callReadOnlyDocCount(t, dir)

				require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
				require.ErrorIs(t, outcome.err, icev3.ErrCorruptSegment)
				assert.Zero(t, outcome.count)
				assert.LessOrEqual(t, outcome.allocBytes, uint64(icev3.MaxSectionBytes),
					"ReadOnlyDocCount allocated %d bytes, beyond the configured %d-byte section limit",
					outcome.allocBytes, icev3.MaxSectionBytes)
			})
		}
	})

	t.Run("R4_reserved_crc32_is_ignored", func(t *testing.T) {
		for _, reserved := range []uint32{0xDEADBEEF, 0x0BADF00D, 0x00000000} {
			t.Run(fmt.Sprintf("healthy_copy_crc_%#08x", reserved), func(t *testing.T) {
				dir := copyCorpus(t)
				overwriteReservedCRC32(t, dir, reserved)

				outcome := callReadOnlyDocCount(t, dir)

				require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
				require.NoError(t, outcome.err)
				assert.Equal(t, fixtureVisibleCount, outcome.count)
			})
			t.Run(fmt.Sprintf("corrupt_copy_crc_%#08x", reserved), func(t *testing.T) {
				dir := copyCorpus(t)
				truncateSegmentFooter(t, dir)
				overwriteReservedCRC32(t, dir, reserved)

				outcome := callReadOnlyDocCount(t, dir)

				require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
				assert.ErrorIs(t, outcome.err, icev3.ErrCorruptSegment,
					"the reserved CRC32 value must not change the corruption classification")
			})
		}
	})

	t.Run("R6_corpus_matches_its_provenance_manifest", func(t *testing.T) {
		assertCorpusProvenance(t)
	})

	t.Run("R7_torn_newest_manifest_keeps_the_committed_count", func(t *testing.T) {
		dir := copyCorpus(t)
		tearNewestManifest(t, dir)

		outcome := callReadOnlyDocCount(t, dir)

		require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
		require.NoError(t, outcome.err,
			"a crash between publishing a manifest and completing it must not lose the previous generation")
		assert.Equal(t, fixtureVisibleCount, outcome.count)
	})

	t.Run("R8_no_committed_generation_reads_as_empty", func(t *testing.T) {
		t.Run("empty_directory", func(t *testing.T) {
			assertReadsAsEmpty(t, t.TempDir())
		})
		t.Run("segment_present_but_never_published", func(t *testing.T) {
			dir := copyCorpus(t)
			for _, manifest := range manifestPaths(t, dir) {
				require.NoError(t, os.Remove(manifest))
			}
			assertReadsAsEmpty(t, dir)
		})
		t.Run("absent_directory", func(t *testing.T) {
			assertReadsAsEmpty(t, filepath.Join(t.TempDir(), "never-created"))
		})
		t.Run("classification_distinguishes_empty_from_corrupt", func(t *testing.T) {
			corrupt := copyCorpus(t)
			truncateSegmentFooter(t, corrupt)

			corruptOutcome := callReadOnlyDocCount(t, corrupt)
			emptyOutcome := callReadOnlyDocCount(t, t.TempDir())

			require.ErrorIs(t, corruptOutcome.err, icev3.ErrCorruptSegment)
			require.NotErrorIs(t, emptyOutcome.err, icev3.ErrCorruptSegment,
				"a caller must be able to tell an index that was never flushed from one that is damaged")
		})
	})
}

// TestReadOnlyDocCountReachesTheNativeReader is the R5 dependency contract:
// ReadOnlyDocCount reaches the native ICE v3 reader and neither it nor that
// reader's package reaches Bluge or the legacy ICE segment library, directly or
// transitively. It holds from the cutover onward; a later revision that reopens
// a Bluge reader behind the seam fails here even though the counts still match.
func TestReadOnlyDocCountReachesTheNativeReader(t *testing.T) {
	t.Run("the_seam_body_names_no_legacy_package", func(t *testing.T) {
		decl, imports := findFuncDecl(t, ".", "ReadOnlyDocCount")
		legacy := legacyImportNames(imports)
		require.NotEmpty(t, legacy,
			"package inverted no longer imports Bluge at all, so this assertion has stopped testing anything")

		var offending []string
		nativeReferenced := false
		ast.Inspect(decl.Body, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if path, isLegacy := legacy[ident.Name]; isLegacy {
				offending = append(offending, ident.Name+" -> "+path)
			}
			if ident.Name == "icev3" {
				nativeReferenced = true
			}
			return true
		})
		assert.Empty(t, offending, "ReadOnlyDocCount must not call into the legacy libraries")
		assert.True(t, nativeReferenced, "ReadOnlyDocCount must reach the native ICE v3 reader")
	})

	t.Run("the_native_reader_package_never_depends_on_them", func(t *testing.T) {
		listed, listErr := exec.Command("go", "list", "-deps", "./internal/icev3").Output()
		require.NoError(t, listErr, "failed to resolve the native reader's dependencies")

		var offending []string
		for _, dep := range strings.Split(strings.TrimSpace(string(listed)), "\n") {
			if isLegacyIndexPackage(strings.TrimSpace(dep)) {
				offending = append(offending, dep)
			}
		}
		assert.Empty(t, offending, "the native ICE v3 reader must not depend on Bluge or the legacy ICE library")
	})
}

// assertReadsAsEmpty asserts that a directory with no committed generation
// reports a zero count together with an error that classifies the directory as
// empty rather than corrupt.
func assertReadsAsEmpty(t *testing.T, dir string) {
	t.Helper()
	outcome := callReadOnlyDocCount(t, dir)
	require.Nil(t, outcome.panicValue, "ReadOnlyDocCount panicked: %v", outcome.panicValue)
	require.Error(t, outcome.err, "an index with no committed generation must report why it is unreadable")
	assert.Zero(t, outcome.count, "callers treat a zero count as an empty index")
	assert.NotErrorIs(t, outcome.err, icev3.ErrCorruptSegment,
		"an index that was never flushed is empty, not corrupt")
}

// assertCorpusProvenance checks the checked-in corpus against its provenance
// manifest and against the shape the NIDX-01A specification calls for.
func assertCorpusProvenance(t *testing.T) {
	t.Helper()
	body, readErr := os.ReadFile(fixtureManifestPath)
	require.NoError(t, readErr)

	var manifest struct {
		Oracle      map[string]string `json:"oracle_versions"`
		FileSHA256  map[string]string `json:"file_sha256"`
		Command     string            `json:"generator_command"`
		Documents   []string          `json:"logical_documents"`
		Generations []struct {
			File           string `json:"file"`
			Version        uint64 `json:"version"`
			SegmentCount   uint64 `json:"segment_count"`
			VisibleCount   int64  `json:"declared_visible_count"`
			DeletionsBytes uint64 `json:"deletion_bitmap_entries"`
		} `json:"snapshot_generations"`
		Segments []struct {
			File    string `json:"file"`
			NumDocs uint64 `json:"num_docs"`
			Version uint32 `json:"version"`
		} `json:"segment_footers"`
		ExpectedVisibleCount int64 `json:"expected_visible_count"`
		CRCPatched           bool  `json:"reserved_crc32_patched"`
	}
	require.NoError(t, json.Unmarshal(body, &manifest))

	assert.Equal(t, fixtureVisibleCount, manifest.ExpectedVisibleCount,
		"the manifest must declare the visible count the specification pins")
	assert.Equal(t, logicalFixtureDocuments, manifest.Documents)
	assert.NotEmpty(t, manifest.Command, "the manifest must record how to regenerate the corpus")
	assert.NotEmpty(t, manifest.Oracle["github.com/blugelabs/bluge"], "the manifest must record the oracle writer version")
	assert.NotEmpty(t, manifest.Oracle["github.com/blugelabs/ice"], "the manifest must record the oracle segment version")
	assert.True(t, manifest.CRCPatched, "the corpus must ship arbitrary reserved CRC32 bytes")

	require.Len(t, manifest.Segments, 1, "NIDX-01A pins exactly one committed segment")
	assert.Equal(t, uint64(fixtureVisibleCount), manifest.Segments[0].NumDocs)
	assert.Equal(t, uint32(3), manifest.Segments[0].Version, "the corpus must be ICE v3")

	require.NotEmpty(t, manifest.Generations)
	for _, generation := range manifest.Generations {
		assert.Equal(t, uint64(3), generation.Version, "%s must be snapshot v3", generation.File)
		assert.Equal(t, uint64(1), generation.SegmentCount, "%s must reference one segment", generation.File)
		assert.Equal(t, fixtureVisibleCount, generation.VisibleCount, "%s must declare the pinned visible count", generation.File)
		assert.Zero(t, generation.DeletionsBytes, "%s must carry no deletion record", generation.File)
	}

	onDisk := map[string]string{}
	for _, path := range corpusFilePaths(t, fixtureCorpusDir) {
		content, contentErr := os.ReadFile(path)
		require.NoError(t, contentErr)
		sum := sha256.Sum256(content)
		onDisk[filepath.Base(path)] = hex.EncodeToString(sum[:])

		require.GreaterOrEqual(t, len(content), reservedCRCWidth)
		assert.NotZero(t, binary.BigEndian.Uint32(content[len(content)-reservedCRCWidth:]),
			"%s must carry non-zero bytes in its reserved CRC32 field", filepath.Base(path))
	}
	assert.Equal(t, manifest.FileSHA256, onDisk,
		"the checked-in corpus bytes drifted from the provenance manifest; a regenerated corpus needs a regenerated manifest, "+
			"and Git line-ending normalization on this path would show up here")
}

// countOutcome records one ReadOnlyDocCount call together with the resources it
// consumed, so a contract test can assert the call stayed bounded.
type countOutcome struct {
	err        error
	panicValue any
	count      int64
	allocBytes uint64
}

// callReadOnlyDocCount calls the production seam and reports its result, the
// panic it raised if any, and the number of bytes allocated while it ran. It
// fails the test if the call outlives callBudget.
func callReadOnlyDocCount(t *testing.T, dir string) countOutcome {
	t.Helper()
	done := make(chan countOutcome, 1)

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	//panicdiag:allow-rawgo the contract requires proving ReadOnlyDocCount neither panics nor hangs, which needs the call off the test goroutine.
	go func() {
		outcome := countOutcome{}
		defer func() {
			outcome.panicValue = recover()
			done <- outcome
		}()
		outcome.count, outcome.err = ReadOnlyDocCount(dir)
	}()

	select {
	case outcome := <-done:
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		outcome.allocBytes = after.TotalAlloc - before.TotalAlloc
		return outcome
	case <-time.After(callBudget):
		t.Fatalf("ReadOnlyDocCount(%q) did not return within %s", dir, callBudget)
		return countOutcome{}
	}
}

// fileFacts is the observable state of one corpus file: everything R1 forbids
// ReadOnlyDocCount from changing.
type fileFacts struct {
	modTime time.Time
	name    string
	sha256  string
	size    int64
	mode    os.FileMode
}

// inventoryOf returns the sorted observable state of every file directly under
// dir.
func inventoryOf(t *testing.T, dir string) []fileFacts {
	t.Helper()
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	facts := make([]fileFacts, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := entry.Info()
		require.NoError(t, infoErr)
		content, contentErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, contentErr)
		sum := sha256.Sum256(content)
		facts = append(facts, fileFacts{
			name:    entry.Name(),
			size:    info.Size(),
			mode:    info.Mode(),
			modTime: info.ModTime(),
			sha256:  hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].name < facts[j].name })
	return facts
}

// copyCorpus copies the checked-in corpus into a fresh temporary directory so a
// test can malform it without touching the oracle bytes.
func copyCorpus(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	for _, path := range corpusFilePaths(t, fixtureCorpusDir) {
		content, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.Base(path)), content, 0o600))
	}
	return dir
}

// corpusFilePaths returns the sorted paths of the files directly under dir.
func corpusFilePaths(t *testing.T, dir string) []string {
	t.Helper()
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		require.False(t, entry.IsDir(), "unexpected directory %s in %s", entry.Name(), dir)
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(paths)
	return paths
}

// segmentPath returns the single segment file of a corpus copy.
func segmentPath(t *testing.T, dir string) string {
	t.Helper()
	matches, globErr := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, globErr)
	require.Len(t, matches, 1, "NIDX-01A pins exactly one committed segment")
	return matches[0]
}

// manifestPaths returns the snapshot manifests of a corpus copy, ordered by
// ascending generation ID.
func manifestPaths(t *testing.T, dir string) []string {
	t.Helper()
	matches, globErr := filepath.Glob(filepath.Join(dir, "*.snp"))
	require.NoError(t, globErr)
	require.NotEmpty(t, matches, "the corpus carries no snapshot manifest")
	sort.Slice(matches, func(i, j int) bool {
		return manifestGeneration(t, matches[i]) < manifestGeneration(t, matches[j])
	})
	return matches
}

// manifestGeneration parses the numeric generation ID out of a manifest name.
// Names are lower-case hexadecimal per BDB-NIDX-SPEC-001 section 09.
func manifestGeneration(t *testing.T, path string) uint64 {
	t.Helper()
	base := strings.TrimSuffix(filepath.Base(path), ".snp")
	id, parseErr := strconv.ParseUint(base, 16, 64)
	require.NoError(t, parseErr, "manifest %s is not a hexadecimal generation ID", path)
	return id
}

// truncateSegmentFooter shortens the segment file to less than the fixed
// 60-byte footer it must end with.
func truncateSegmentFooter(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.Truncate(segmentPath(t, dir), segmentFooterLen-1))
}

// patchSegmentFieldsIndexOffsetBeyondFile moves the fields-index root offset
// far past the end of the segment file.
func patchSegmentFieldsIndexOffsetBeyondFile(t *testing.T, dir string) {
	t.Helper()
	patchSegmentFooter(t, dir, func(footer []byte) {
		binary.BigEndian.PutUint64(footer[16:24], 1<<40)
	})
}

// patchSegmentVersionToTwo rewrites the segment version to a value the ICE v3
// grammar does not describe.
func patchSegmentVersionToTwo(t *testing.T, dir string) {
	t.Helper()
	patchSegmentFooter(t, dir, func(footer []byte) {
		binary.BigEndian.PutUint32(footer[52:56], 2)
	})
}

// patchSegmentFooter rewrites the trailing 60-byte footer of the corpus copy's
// segment file. Field positions come from BDB-NIDX-SPEC-001 section 08:
// numDocs, storedIndex, fieldsIndex, docValues, chunkMode, timeMin, timeMax,
// version, reserved CRC32.
func patchSegmentFooter(t *testing.T, dir string, mutate func(footer []byte)) {
	t.Helper()
	path := segmentPath(t, dir)
	content, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.GreaterOrEqual(t, len(content), segmentFooterLen)
	mutate(content[len(content)-segmentFooterLen:])
	require.NoError(t, os.WriteFile(path, content, 0o600))
}

// declareOversizedDeletionBitmap rewrites the newest manifest so its final
// segment record declares a deletion bitmap far larger than the file, without
// supplying any of those bytes.
func declareOversizedDeletionBitmap(t *testing.T, dir string) {
	t.Helper()
	// The record ends with the deletion-bitmap length followed by the reserved
	// CRC32 field. The corpus carries no deletions, so that length is the
	// single byte zero: overwriting it and the four reserved bytes with a
	// five-byte unsigned LEB128 value keeps the file length identical while
	// declaring a section the file cannot hold.
	declared := make([]byte, reservedCRCWidth+1)
	written := binary.PutUvarint(declared, oversizedSectionLength)
	require.Len(t, declared, written, "the declared length must occupy the zero length byte and the reserved CRC32 field exactly")

	manifests := manifestPaths(t, dir)
	path := manifests[len(manifests)-1]
	content, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Greater(t, len(content), len(declared))
	copy(content[len(content)-len(declared):], declared)
	require.NoError(t, os.WriteFile(path, content, 0o600))
}

// truncateOnlyManifest cuts the corpus's single manifest short, mid segment
// record, leaving no other generation to fall back to.
func truncateOnlyManifest(t *testing.T, dir string) {
	t.Helper()
	manifests := manifestPaths(t, dir)
	require.Len(t, manifests, 1, "this case requires the corpus to hold exactly one generation")
	require.NoError(t, os.Truncate(manifests[0], 6))
}

// tearNewestManifest simulates a crash between publishing a new manifest and
// completing it: the directory gains a newer generation ID whose bytes stop
// mid segment record, while the previously committed generation stays intact.
// The legacy writer publishes manifests in place rather than by atomic rename,
// so this state is reachable on disk.
func tearNewestManifest(t *testing.T, dir string) {
	t.Helper()
	manifests := manifestPaths(t, dir)
	newest := manifests[len(manifests)-1]
	committed, readErr := os.ReadFile(newest)
	require.NoError(t, readErr)
	require.Greater(t, len(committed), 6)

	torn := filepath.Join(dir, fmt.Sprintf("%012x.snp", manifestGeneration(t, newest)+1))
	require.NoError(t, os.WriteFile(torn, committed[:6], 0o600))
}

// overwriteReservedCRC32 rewrites the reserved CRC32 field at the end of every
// file in a corpus copy.
func overwriteReservedCRC32(t *testing.T, dir string, reserved uint32) {
	t.Helper()
	for _, path := range corpusFilePaths(t, dir) {
		content, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		if len(content) < reservedCRCWidth {
			continue
		}
		binary.BigEndian.PutUint32(content[len(content)-reservedCRCWidth:], reserved)
		require.NoError(t, os.WriteFile(path, content, 0o600))
	}
}

// findFuncDecl locates a top-level function by name in the non-test sources of
// a package directory and returns it with that file's import specs.
func findFuncDecl(t *testing.T, dir, name string) (*ast.FuncDecl, []*ast.ImportSpec) {
	t.Helper()
	sources, globErr := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, globErr)
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		require.NoError(t, parseErr)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Name.Name == name && fn.Body != nil {
				return fn, file.Imports
			}
		}
	}
	t.Fatalf("no top-level func %s found in %s", name, dir)
	return nil, nil
}

// legacyImportNames maps the local name of every Bluge or legacy ICE import to
// its import path.
func legacyImportNames(imports []*ast.ImportSpec) map[string]string {
	names := map[string]string{}
	for _, spec := range imports {
		path := strings.Trim(spec.Path.Value, `"`)
		if !isLegacyIndexPackage(path) {
			continue
		}
		local := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			local = spec.Name.Name
		}
		names[local] = path
	}
	return names
}

// isLegacyIndexPackage reports whether an import path belongs to Bluge or to
// the legacy ICE segment library that this milestone replaces.
func isLegacyIndexPackage(path string) bool {
	return strings.Contains(path, "blugelabs") || strings.Contains(path, "bluge_segment_api") ||
		path == "github.com/blugelabs/ice" || strings.HasSuffix(path, "/ice")
}
