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
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/require"
)

// The NIDX-01A corpus is an ICE v3 / snapshot v3 directory produced by the
// pinned legacy writer and checked in as bytes. Tests read it; production code
// never generates it and never links the generator, which lives in this test
// file behind nidx01aGenerateEnv.
const (
	nidx01aRoot     = "testdata/nidx01a"
	nidx01aIndexDir = nidx01aRoot + "/index"
	nidx01aManifest = nidx01aRoot + "/provenance.json"

	// nidx01aGenerateEnv gates regeneration so an ordinary test run can never
	// overwrite the checked-in bytes the contract is pinned to.
	nidx01aGenerateEnv = "GENERATE_NIDX01A_FIXTURE"
	nidx01aGenerateCmd = "GENERATE_NIDX01A_FIXTURE=1 go test ./pkg/index/inverted/ -run TestGenerateNIDX01AFixture -count=1"

	// nidx01aVisibleCount is the declared visible document count of the corpus.
	// It is the literal from issue #14008, not a value any reader computes:
	// two documents are created, none is deleted, so two are visible.
	nidx01aVisibleCount = int64(2)

	segExt = ".seg"
	snpExt = ".snp"
)

// nidx01aDocIDs are the logical documents the corpus contains, in insertion
// order, as named by issue #14008.
var nidx01aDocIDs = []string{"doc-11", "doc-12"}

// reservedCRC32FillSegment overwrites the reserved four-byte CRC32 slot that
// BDB-NIDX-SPEC-001 section 08 places at the end of a segment footer. They are
// arbitrary non-zero bytes no writer computed, so the "reserved and ignored"
// contract is observable on the checked-in bytes themselves.
//
// The manifest's reserved slot, section 09, keeps the value the writer put
// there. The pinned legacy loader does validate that one, so overwriting it
// would leave a corpus the very oracle that produced it cannot open -- and a
// corpus no oracle can read is not evidence of anything. The native reader owes
// the same "carried, never checked" behavior on the manifest slot, and
// TestReadOnlyDocCountSingleSegment proves it by rewriting that slot at run
// time instead.
var reservedCRC32FillSegment = []byte{0xDE, 0xAD, 0xBE, 0xEF}

// nidx01aProvenance is the manifest checked in beside the corpus bytes. It
// records everything a reviewer needs to re-derive the corpus without reading
// the generator: which oracle produced it, how to run that oracle, which
// logical documents went in, what the bytes hash to, and what the declared
// visible count is.
type nidx01aProvenance struct {
	Oracle           map[string]string `json:"oracle"`
	FileSHA256       map[string]string `json:"file_sha256"`
	ReservedCRC32    map[string]string `json:"reserved_crc32"`
	GeneratorCommand string            `json:"generator_command"`
	Notes            string            `json:"notes"`
	LogicalDocuments []string          `json:"logical_documents"`
	VisibleCount     int64             `json:"visible_count"`
}

// TestGenerateNIDX01AFixture rebuilds the checked-in NIDX-01A corpus and its
// provenance manifest with the pinned legacy writer. It runs only when
// nidx01aGenerateEnv is set, so a normal test run reads the committed bytes
// instead of producing fresh ones.
func TestGenerateNIDX01AFixture(t *testing.T) {
	if os.Getenv(nidx01aGenerateEnv) != "1" {
		t.Skipf("set %s=1 to regenerate the NIDX-01A corpus", nidx01aGenerateEnv)
	}
	tester := require.New(t)

	staging := t.TempDir()
	writer, err := bluge.OpenWriter(bluge.DefaultConfig(staging))
	tester.NoError(err)
	batch := bluge.NewBatch()
	for _, docID := range nidx01aDocIDs {
		doc := bluge.NewDocument(docID)
		doc.AddField(bluge.NewKeywordField("kind", "nidx01a").StoreValue())
		batch.Insert(doc)
	}
	tester.NoError(writer.Batch(batch))
	tester.NoError(writer.Close())

	// The writer may leave older, empty generations behind; the corpus is the
	// newest committed generation alone, so a superseded manifest is dropped.
	// Identifiers are fixed-width lower-case hexadecimal, so the highest name
	// in lexical order is the highest numerically.
	segments, err := filepath.Glob(filepath.Join(staging, "*"+segExt))
	tester.NoError(err)
	tester.Len(segments, 1, "one batch must seal exactly one segment")
	snapshots, err := filepath.Glob(filepath.Join(staging, "*"+snpExt))
	tester.NoError(err)
	tester.NotEmpty(snapshots)
	sort.Strings(snapshots)

	tester.NoError(os.RemoveAll(nidx01aIndexDir))
	tester.NoError(os.MkdirAll(nidx01aIndexDir, 0o755))
	hashes := map[string]string{}
	for _, source := range []string{segments[0], snapshots[len(snapshots)-1]} {
		payload, readErr := os.ReadFile(source)
		tester.NoError(readErr)
		if filepath.Ext(source) == segExt {
			copy(payload[len(payload)-len(reservedCRC32FillSegment):], reservedCRC32FillSegment)
		}
		name := filepath.Base(source)
		tester.NoError(os.WriteFile(filepath.Join(nidx01aIndexDir, name), payload, 0o600))
		sum := sha256.Sum256(payload)
		hashes["index/"+name] = hex.EncodeToString(sum[:])
	}
	tester.Len(hashes, 2, "the corpus is exactly one committed segment and its manifest")

	manifest := nidx01aProvenance{
		Oracle:           readPinnedOracleVersions(t),
		GeneratorCommand: nidx01aGenerateCmd,
		LogicalDocuments: append([]string(nil), nidx01aDocIDs...),
		FileSHA256:       hashes,
		VisibleCount:     nidx01aVisibleCount,
		ReservedCRC32: map[string]string{
			segExt: hex.EncodeToString(reservedCRC32FillSegment) + " (arbitrary, no writer computed it)",
			snpExt: "as written by the oracle; the pinned legacy loader validates this slot, " +
				"so the corpus keeps it readable and the reserved-field contract is proved by run-time mutation",
		},
		Notes: "One committed segment, documents doc-11 and doc-12, no deletion record.",
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	tester.NoError(err)
	tester.NoError(os.WriteFile(nidx01aManifest, append(encoded, '\n'), 0o600))
}

// readPinnedOracleVersions returns the module replacements that pin the legacy
// writer used as the corpus oracle, read from go.mod so the manifest cannot
// drift away from the build.
func readPinnedOracleVersions(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "go.mod"))
	require.NoError(t, err)
	pinned := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 4 || fields[1] != "=>" {
			continue
		}
		if !strings.HasPrefix(fields[0], "github.com/blugelabs/") {
			continue
		}
		pinned[fields[0]] = fields[2] + " " + fields[3]
	}
	require.NotEmpty(t, pinned, "go.mod must pin the legacy writer used as the oracle")
	return pinned
}

// loadNIDX01AProvenance reads the checked-in manifest.
func loadNIDX01AProvenance(t *testing.T) nidx01aProvenance {
	t.Helper()
	raw, err := os.ReadFile(nidx01aManifest)
	require.NoError(t, err, "the NIDX-01A provenance manifest must be checked in")
	var manifest nidx01aProvenance
	require.NoError(t, json.Unmarshal(raw, &manifest))
	return manifest
}

// copyIndexDir copies the persisted index files in src into a fresh directory
// under the test's temporary space and returns it. Runtime files are outside
// the ICE grammar and are not part of a copied index generation.
func copyIndexDir(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "index")
	require.NoError(t, os.MkdirAll(dst, 0o755))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, entry := range entries {
		extension := filepath.Ext(entry.Name())
		if entry.IsDir() || extension != segExt && extension != snpExt {
			continue
		}
		payload, readErr := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(dst, entry.Name()), payload, 0o600))
	}
	return dst
}

// newestSegmentFile returns the path of the highest-numbered segment file in
// dir. Segment and snapshot identifiers are numbered independently, so tests
// that damage "the" segment locate it by scanning rather than by pairing names.
func newestSegmentFile(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*"+segExt))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "index directory %s holds no segment file", dir)
	sort.Strings(matches)
	return matches[len(matches)-1]
}

// dirInventory records every observable property of every entry in dir that a
// read-only call must leave alone: the set of names, and each entry's size,
// mode, modification time, and content hash. Access time is deliberately
// excluded -- reading a file is allowed to update it.
func dirInventory(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	inventory := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := entry.Info()
		require.NoError(t, infoErr)
		line := entry.Name() + " dir=" + strconv.FormatBool(entry.IsDir()) +
			" size=" + strconv.FormatInt(info.Size(), 10) +
			" mode=" + info.Mode().String() +
			" mtime=" + strconv.FormatInt(info.ModTime().UnixNano(), 10)
		if !entry.IsDir() {
			payload, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
			require.NoError(t, readErr)
			sum := sha256.Sum256(payload)
			line += " sha256=" + hex.EncodeToString(sum[:])
		}
		inventory = append(inventory, line)
	}
	sort.Strings(inventory)
	return inventory
}
