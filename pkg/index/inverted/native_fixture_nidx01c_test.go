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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// The NIDX-01C corpus extends the checked-in legacy corpus with the live series
// documents issue #14010 declares. It is two independent index directories:
// sourceA, holding the walk the milestone is specified against, and sourceB,
// which repeats one of sourceA's documents so the series-union case has a
// second source. Every byte is produced by BanyanDB's compatibility writer
// through the pkg/index/inverted store boundary and checked in; production code
// never generates it, and the generator lives in this test file behind
// nidx01cGenerateEnv.
const (
	nidx01cRoot       = "testdata/nidx01c"
	nidx01cSourceADir = nidx01cRoot + "/sourceA"
	nidx01cSourceBDir = nidx01cRoot + "/sourceB"
	nidx01cManifest   = nidx01cRoot + "/provenance.json"

	// nidx01cGenerateEnv gates regeneration so an ordinary test run can never
	// overwrite the checked-in bytes the contract is pinned to.
	nidx01cGenerateEnv = "GENERATE_NIDX01C_FIXTURE"
	nidx01cGenerateCmd = "GENERATE_NIDX01C_FIXTURE=1 go test ./pkg/index/inverted/ -run TestGenerateNIDX01CFixture -count=1"

	// nidx01cTagName is the one repeated tag name issue #14010 declares. It is
	// stored and not indexed, which is how the production write path records a
	// non-indexed tag.
	nidx01cTagName = "color"

	// The literals below are the documents issue #14010 declares, not values
	// any reader computes. Document labels 101, 202 and 303 are the issue's
	// names for the three documents; they are not series identifiers, because
	// pkg/pb/v1.Series derives its identifier by hashing the marshaled entity
	// buffer and a fixture cannot choose that hash. Each label's identity is
	// the raw byte string below, which the store records as the document's
	// identity field.
	nidx01cLabel101 = "101"
	nidx01cLabel202 = "202"
	nidx01cLabel303 = "303"

	// nidx01cSourceAPhysical is the physical document count of sourceA's
	// selected generation: 101, 202 and the deleted 303. One deletion leaves
	// two visible.
	nidx01cSourceAPhysical = uint64(3)
	nidx01cSourceADeleted  = 1
	nidx01cSourceAVisible  = int64(2)

	// nidx01cSourceBPhysical is sourceB's physical document count: the repeat
	// of 101 alone, with no deletion.
	nidx01cSourceBPhysical = uint64(1)
	nidx01cSourceBVisible  = int64(1)

	// The stored timestamp bytes below are the compatibility writer's encoding
	// of Unix nanoseconds 100, 200 and 300, pinned here as literals. Each is a
	// shift marker of 0x20 followed by the ten seven-bit groups, most
	// significant first, of the sortable form of the signed value -- 100 is
	// 0x8000000000000064, whose groups are 01 00 00 00 00 00 00 00 00 64 -- so
	// a reviewer can re-derive them by hand rather than by running a decoder.
	//
	// They are the compatibility claim this milestone makes: the native walk
	// must hand a caller the same bytes the retired reader hands it today, so a
	// caller that decodes them keeps working unchanged.
	nidx01cStoredTimestamp100Hex = "2001000000000000000064"
	nidx01cStoredTimestamp200Hex = "2001000000000000000148"
	nidx01cStoredTimestamp300Hex = "200100000000000000022c"
)

// nidx01cDocument is one declared document of the corpus: its issue label, the
// raw identity bytes the store records, its repeated tag values in the declared
// order, and its timestamp and version.
type nidx01cDocument struct {
	label string
	// storedTimestampHex is the compatibility writer's encoding of timestamp as
	// the segment records it. Unlike the identity, the tag values and the
	// version, a stored timestamp is not a BanyanDB encoding, so the corpus
	// pins the oracle's bytes rather than restating a BanyanDB one.
	storedTimestampHex string
	identity           []byte
	tagValues          []string
	timestamp          int64
	version            int64
	deleted            bool
}

// nidx01cSourceADocuments is sourceA's declared content, in the order issue
// #14010 lists it. 101 carries the binary identity 0x010203 the issue names,
// which is deliberately not valid UTF-8 and not a marshaled series buffer, so
// the walk's byte fidelity is observable rather than inferred from text.
var nidx01cSourceADocuments = []nidx01cDocument{
	{
		label:              nidx01cLabel101,
		identity:           []byte{0x01, 0x02, 0x03},
		tagValues:          []string{"blue", "green"},
		timestamp:          100,
		storedTimestampHex: nidx01cStoredTimestamp100Hex,
		version:            2,
	},
	{
		label:              nidx01cLabel202,
		identity:           []byte{0x04, 0x05, 0x06},
		tagValues:          []string{"red"},
		timestamp:          200,
		storedTimestampHex: nidx01cStoredTimestamp200Hex,
		version:            1,
	},
	{
		label:              nidx01cLabel303,
		identity:           []byte{0x07, 0x08, 0x09},
		tagValues:          []string{"gray"},
		timestamp:          300,
		storedTimestampHex: nidx01cStoredTimestamp300Hex,
		version:            3,
		deleted:            true,
	},
}

// nidx01cSourceBDocuments repeats 101 so the series-union case reads the same
// document from a second source.
var nidx01cSourceBDocuments = []nidx01cDocument{nidx01cSourceADocuments[0]}

// nidx01cProvenance is the manifest checked in beside the corpus bytes. It
// records which oracle produced the bytes, how to re-derive them, which
// documents each source declares, and what every file hashes to.
type nidx01cProvenance struct {
	Oracle           map[string]string         `json:"oracle"`
	FileSHA256       map[string]string         `json:"file_sha256"`
	ReservedCRC32    map[string]string         `json:"reserved_crc32"`
	GeneratorCommand string                    `json:"generator_command"`
	Notes            string                    `json:"notes"`
	Sources          []nidx01cProvenanceSource `json:"sources"`
}

// nidx01cProvenanceSource describes one source directory of the corpus.
type nidx01cProvenanceSource struct {
	Directory         string                      `json:"directory"`
	Documents         []nidx01cProvenanceDocument `json:"documents"`
	PhysicalDocuments uint64                      `json:"physical_documents"`
	Deletions         int                         `json:"deletions"`
	VisibleCount      int64                       `json:"visible_count"`
}

// nidx01cProvenanceDocument records one declared document as hexadecimal bytes,
// so a reviewer can compare the corpus against issue #14010 without decoding a
// segment.
type nidx01cProvenanceDocument struct {
	Label        string   `json:"label"`
	IdentityHex  string   `json:"identity_hex"`
	TagName      string   `json:"tag_name"`
	TimestampHex string   `json:"timestamp_stored_hex"`
	VersionHex   string   `json:"version_stored_hex"`
	TagValues    []string `json:"tag_values"`
	Timestamp    int64    `json:"timestamp"`
	Version      int64    `json:"version"`
	Deleted      bool     `json:"deleted"`
}

// TestGenerateNIDX01CFixture rebuilds the checked-in NIDX-01C corpus and its
// provenance manifest with BanyanDB's compatibility writer. It runs only when
// nidx01cGenerateEnv is set, so a normal test run reads the committed bytes
// instead of producing fresh ones.
//
// The writer reclaims deletions by merging, so the generation carrying 303's
// deletion is superseded shortly after the delete lands. The generator
// therefore lifts the generation out of the live directory as soon as the
// commit that produced it returns, rather than reading the directory's final
// state.
func TestGenerateNIDX01CFixture(t *testing.T) {
	if os.Getenv(nidx01cGenerateEnv) != "1" {
		t.Skipf("set %s=1 to regenerate the NIDX-01C corpus", nidx01cGenerateEnv)
	}
	tester := require.New(t)

	tester.NoError(os.RemoveAll(nidx01cRoot))
	tester.NoError(os.MkdirAll(nidx01cSourceADir, 0o755))
	tester.NoError(os.MkdirAll(nidx01cSourceBDir, 0o755))

	hashes := map[string]string{}
	record := func(relativeDir, name string, payload []byte) {
		tester.NoError(os.WriteFile(filepath.Join(nidx01cRoot, relativeDir, name), payload, 0o600))
		sum := sha256.Sum256(payload)
		hashes[relativeDir+"/"+name] = hex.EncodeToString(sum[:])
	}

	sourceA := generateNIDX01CSource(t, nidx01cSourceADocuments, nidx01cSourceAPhysical, nidx01cSourceADeleted)
	writeNIDX01CGeneration(t, record, "sourceA", sourceA)
	sourceB := generateNIDX01CSource(t, nidx01cSourceBDocuments, nidx01cSourceBPhysical, 0)
	writeNIDX01CGeneration(t, record, "sourceB", sourceB)

	manifest := nidx01cProvenance{
		Oracle:           readCompatibilityOracleIdentity(t),
		GeneratorCommand: nidx01cGenerateCmd,
		FileSHA256:       hashes,
		Sources: []nidx01cProvenanceSource{
			describeNIDX01CSource("sourceA", nidx01cSourceADocuments, nidx01cSourceAPhysical,
				nidx01cSourceADeleted, nidx01cSourceAVisible, sourceA),
			describeNIDX01CSource("sourceB", nidx01cSourceBDocuments, nidx01cSourceBPhysical,
				0, nidx01cSourceBVisible, sourceB),
		},
		ReservedCRC32: map[string]string{
			segExt: hex.EncodeToString(reservedCRC32FillSegment) + " (arbitrary, no writer computed it)",
			snpExt: "as written by the oracle; the compatibility loader validates this slot, " +
				"so the corpus keeps it readable and the reserved-field contract is proved by run-time mutation",
		},
		Notes: "sourceA holds documents 101, 202 and a deleted 303 in one committed generation, so two are visible " +
			"and the declared walk is 101 then 202. sourceB repeats 101 alone for the series-union case. " +
			"Document labels are issue #14010's names, not series identifiers: a series identifier is a hash of the " +
			"marshaled entity buffer and cannot be chosen by a fixture.",
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	tester.NoError(err)
	tester.NoError(os.WriteFile(nidx01cManifest, append(encoded, '\n'), 0o600))
}

// generateNIDX01CSource writes the given documents through the compatibility
// writer, deletes the ones declared deleted, and lifts out the generation
// holding the declared physical document and deletion counts.
func generateNIDX01CSource(t *testing.T, documents []nidx01cDocument, physical uint64, deletions int) capturedGeneration {
	t.Helper()
	tester := require.New(t)

	staging := t.TempDir()
	writer, err := NewStore(StoreOpts{Path: staging})
	tester.NoError(err)
	tester.NoError(writer.UpdateSeriesBatch(nidx01cBatch(documents)))
	var deletedIdentities [][]byte
	for _, document := range documents {
		if document.deleted {
			deletedIdentities = append(deletedIdentities, document.identity)
		}
	}
	if len(deletedIdentities) > 0 {
		tester.NoError(writer.Delete(deletedIdentities))
	}
	captured := captureGeneration(t, staging, "the NIDX-01C generation", func(records []snapshotSegmentRecord) bool {
		return totalPhysicalDocuments(records) == physical && totalDeletions(records) == deletions
	})
	tester.NoError(writer.Close())
	return captured
}

// writeNIDX01CGeneration checks a captured generation's manifest and segments
// into the corpus directory.
func writeNIDX01CGeneration(t *testing.T, record func(relativeDir, name string, payload []byte), directory string,
	captured capturedGeneration,
) {
	t.Helper()
	record(directory, filepath.Base(captured.source), captured.manifest)
	for _, segmentID := range sortedSegmentIDs(captured.segments) {
		record(directory, segmentFileName(segmentID), withReservedCRC32Fill(captured.segments[segmentID]))
	}
}

// nidx01cBatch turns the declared documents into one series commit. Each
// document's identity becomes the store's entity values, and every declared tag
// value becomes a separate stored, non-indexed field of the same name, which is
// how a repeated tag reaches the segment.
func nidx01cBatch(documents []nidx01cDocument) index.Batch {
	batch := index.Batch{Documents: make(index.Documents, 0, len(documents))}
	for _, document := range documents {
		fields := make([]index.Field, 0, len(document.tagValues))
		for _, tagValue := range document.tagValues {
			field := index.NewBytesField(index.FieldKey{TagName: nidx01cTagName}, []byte(tagValue))
			field.Store = true
			field.Index = false
			fields = append(fields, field)
		}
		batch.Documents = append(batch.Documents, index.Document{
			Fields:       fields,
			EntityValues: document.identity,
			Timestamp:    document.timestamp,
			Version:      document.version,
		})
	}
	return batch
}

// describeNIDX01CSource renders one source's declared content for the
// provenance manifest.
func describeNIDX01CSource(directory string, documents []nidx01cDocument, physical uint64, deletions int,
	visible int64, captured capturedGeneration,
) nidx01cProvenanceSource {
	described := make([]nidx01cProvenanceDocument, 0, len(documents))
	for _, document := range documents {
		described = append(described, nidx01cProvenanceDocument{
			Label:        document.label,
			IdentityHex:  hex.EncodeToString(document.identity),
			TagName:      nidx01cTagName,
			TagValues:    document.tagValues,
			Timestamp:    document.timestamp,
			TimestampHex: document.storedTimestampHex,
			Version:      document.version,
			VersionHex:   hex.EncodeToString(convert.Int64ToBytes(document.version)),
			Deleted:      document.deleted,
		})
	}
	return nidx01cProvenanceSource{
		Directory:         directory + "/" + filepath.Base(captured.source),
		Documents:         described,
		PhysicalDocuments: physical,
		Deletions:         deletions,
		VisibleCount:      visible,
	}
}
