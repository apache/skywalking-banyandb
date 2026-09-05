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

package reader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// The NIDX-01D corpus is the schema catalog issue #14011 declares: one shard of
// the property-backed `_schema` catalog holding five logical properties across
// seven physical revisions, with p5's stored record deliberately made
// unreadable. Every byte is produced by BanyanDB's compatibility writer through
// the pkg/index/inverted store boundary and checked in; production code never
// generates it, and the generator lives in this test file behind
// nidx01dGenerateEnv.
//
// It is an independent fixture in the sense the milestone needs: the expected
// results below are issue #14011's declaration, and the bytes are an oracle's
// output that no reader under test produced.
const (
	nidx01dRoot     = "testdata/nidx01d"
	nidx01dShardDir = nidx01dRoot + "/shard-0"
	nidx01dManifest = nidx01dRoot + "/provenance.json"

	// nidx01dGenerateEnv gates regeneration so an ordinary test run can never
	// overwrite the checked-in bytes the contract is pinned to.
	nidx01dGenerateEnv = "GENERATE_NIDX01D_FIXTURE"
	nidx01dGenerateCmd = "GENERATE_NIDX01D_FIXTURE=1 go test ./banyand/metadata/schema/reader/ " +
		"-run TestGenerateNIDX01DFixture -count=1"

	// nidx01dGroup is the resource group the group-scoped properties of the
	// corpus belong to; nidx01dGroupName is the group p4 itself declares.
	nidx01dGroup     = "g1"
	nidx01dGroupName = "g4"

	// The property identifiers below are the literals the catalog's own
	// identifier format yields for the corpus's five properties, pinned here so
	// an assertion compares against a declared string rather than against a
	// value recomputed the way the reader derives it.
	nidx01dPropID1 = "stream_g1/s1"
	nidx01dPropID2 = "measure_g1/m2"
	nidx01dPropID3 = "stream_g1/s3"
	nidx01dPropID4 = "group_g4"
	nidx01dPropID5 = "measure_g1/m5"

	// iceFooterLength and iceFooterStoredIndexStart locate the stored-document
	// offset index of a segment, whose width and position BDB-NIDX-SPEC-001
	// revision 0.2 section 08 fixes. They are read off the grammar rather than
	// off any reader, so the corpus cannot agree with a reader by construction.
	iceFooterLength           = 60
	iceFooterStoredIndexStart = 8
	iceFooterStoredIndexEnd   = 16
	storedChunkTableFooterLen = 8
	storedDocumentOffsetWidth = 8
	storedDocumentsPerChunk   = 128
)

// nidx01dDocument is one physical revision of the corpus: the logical property
// it belongs to, its kind, the group it is scoped to, the schema payload it
// carries, its modification revision, and whether it is a tombstone or the
// deliberately damaged one.
type nidx01dDocument struct {
	source  proto.Message
	label   string
	propID  string
	group   string
	kind    schema.Kind
	modRev  int64
	deleted bool
	damaged bool
}

// nidx01dDocuments is the corpus issue #14011 declares, in insertion order:
//
//	p1  kind Stream,  revisions 1 and 2;
//	p2  kind Measure, revision 1;
//	p3  kind Stream,  revision 1 followed by a tombstone at revision 2;
//	p4  kind Group,   revision 1;
//	p5  kind Measure, whose stored record is deliberately malformed.
//
// Group properties carry no group tag, matching how the schema server records
// them; every other property is scoped to g1.
var nidx01dDocuments = []nidx01dDocument{
	{
		label: "p1@1", propID: nidx01dPropID1, kind: schema.KindStream, group: nidx01dGroup, modRev: 1,
		source: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: nidx01dGroup, Name: "s1"}},
	},
	{
		label: "p1@2", propID: nidx01dPropID1, kind: schema.KindStream, group: nidx01dGroup, modRev: 2,
		source: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: nidx01dGroup, Name: "s1"}},
	},
	{
		label: "p2@1", propID: nidx01dPropID2, kind: schema.KindMeasure, group: nidx01dGroup, modRev: 1,
		source: &databasev1.Measure{Metadata: &commonv1.Metadata{Group: nidx01dGroup, Name: "m2"}},
	},
	{
		label: "p3@1", propID: nidx01dPropID3, kind: schema.KindStream, group: nidx01dGroup, modRev: 1,
		source: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: nidx01dGroup, Name: "s3"}},
	},
	{
		label: "p3@2", propID: nidx01dPropID3, kind: schema.KindStream, group: nidx01dGroup, modRev: 2, deleted: true,
		source: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: nidx01dGroup, Name: "s3"}},
	},
	{
		label: "p4@1", propID: nidx01dPropID4, kind: schema.KindGroup, modRev: 1,
		source: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: nidx01dGroupName},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
			},
		},
	},
	{
		label: "p5@1", propID: nidx01dPropID5, kind: schema.KindMeasure, group: nidx01dGroup, modRev: 1, damaged: true,
		source: &databasev1.Measure{Metadata: &commonv1.Metadata{Group: nidx01dGroup, Name: "m5"}},
	},
}

// nidx01dProvenance is the manifest checked in beside the corpus bytes. It
// records which oracle produced them, how to re-derive them, what every
// physical revision declares, which document was damaged and how, and what
// every file hashes to.
type nidx01dProvenance struct {
	Oracle           map[string]string           `json:"oracle"`
	FileSHA256       map[string]string           `json:"file_sha256"`
	Damage           map[string]string           `json:"damage"`
	GeneratorCommand string                      `json:"generator_command"`
	Notes            string                      `json:"notes"`
	Documents        []nidx01dProvenanceDocument `json:"documents"`
	DamagedOrdinal   uint64                      `json:"damaged_local_document_number"`
}

// nidx01dProvenanceDocument records one physical revision of the corpus so a
// reviewer can compare it against issue #14011 without decoding a segment.
type nidx01dProvenanceDocument struct {
	Label   string `json:"label"`
	PropID  string `json:"property_id"`
	Kind    string `json:"kind"`
	Group   string `json:"group"`
	ModRev  int64  `json:"mod_revision"`
	Deleted bool   `json:"deleted"`
	Damaged bool   `json:"damaged"`
}

// TestGenerateNIDX01DFixture rebuilds the checked-in NIDX-01D corpus and its
// provenance manifest with BanyanDB's compatibility writer. It runs only when
// nidx01dGenerateEnv is set, so a normal test run reads the committed bytes
// instead of producing fresh ones.
//
// The corpus is written as one commit so its seven revisions share a single
// segment, and p5's stored record is then made unreachable by pointing its
// entry in the stored-document offset index at the end of the chunk that holds
// it. That damage is invisible to a reader that filters before decoding and is
// a bounded, typed failure for one that decodes p5, which is exactly the
// distinction issue #14011 asks the schema walk to make. It leaves the reserved
// CRC32 slots as the oracle wrote them, so the oracle that produced the corpus
// can still open it.
func TestGenerateNIDX01DFixture(t *testing.T) {
	if os.Getenv(nidx01dGenerateEnv) != "1" {
		t.Skipf("set %s=1 to regenerate the NIDX-01D corpus", nidx01dGenerateEnv)
	}
	tester := require.New(t)

	staging := t.TempDir()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: staging})
	tester.NoError(err)
	tester.NoError(store.UpdateSeriesBatch(index.Batch{Documents: nidx01dBatch(t)}))
	tester.NoError(store.Close())

	tester.NoError(os.RemoveAll(nidx01dRoot))
	tester.NoError(os.MkdirAll(nidx01dShardDir, 0o755))
	entries, err := os.ReadDir(staging)
	tester.NoError(err)
	segments := 0
	for _, entry := range entries {
		extension := filepath.Ext(entry.Name())
		if extension != ".seg" && extension != ".snp" {
			continue
		}
		if extension == ".seg" {
			segments++
		}
		payload, readErr := os.ReadFile(filepath.Join(staging, entry.Name()))
		tester.NoError(readErr)
		tester.NoError(os.WriteFile(filepath.Join(nidx01dShardDir, entry.Name()), payload, 0o600))
	}
	tester.Equal(1, segments, "one commit must seal exactly one segment for the corpus")

	damagedOrdinal := nidx01dDamagedOrdinal(t)
	damageStoredDocumentRecord(t, nidx01dSegmentFile(t), damagedOrdinal)

	tester.NoError(os.WriteFile(nidx01dManifest, nidx01dProvenanceBytes(t, damagedOrdinal), 0o600))
}

// nidx01dBatch turns the declared revisions into one commit shaped like the
// property schema server's own writes: the whole property proto as a stored
// `_source`, the catalog group and the kind as indexed terms the kind pushdown
// resolves against, the property identifier as an indexed term, and a stored
// `_deleted` marker on tombstones. Each revision gets its own document
// identifier so every revision stays visible to a catalog walk.
func nidx01dBatch(t *testing.T) index.Documents {
	t.Helper()
	documents := make(index.Documents, 0, len(nidx01dDocuments))
	for _, declared := range nidx01dDocuments {
		fields := []index.Field{
			nidx01dStoredField(propSourceField, nidx01dSourceBytes(t, declared)),
			nidx01dIndexedField(index.IndexModeName, []byte(declared.kind.String())),
			nidx01dIndexedField(propGroupField, []byte(schema.SchemaGroup)),
			nidx01dIndexedField(propEntityIDField, []byte(declared.propID)),
		}
		if declared.deleted {
			fields = append(fields, nidx01dStoredField(propDeleteField, []byte("1")))
		}
		documents = append(documents, index.Document{
			EntityValues: []byte(declared.propID + "@" + strconv.FormatInt(declared.modRev, 10)),
			Fields:       fields,
		})
	}
	return documents
}

// nidx01dStoredField builds a field the segment stores but does not index.
func nidx01dStoredField(name string, value []byte) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, value)
	field.Store = true
	field.Index = false
	return field
}

// nidx01dIndexedField builds a field the segment indexes as one exact term.
func nidx01dIndexedField(name string, value []byte) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, value)
	field.Index = true
	field.NoSort = true
	return field
}

// nidx01dSourceBytes renders one declared revision as the stored `_source` the
// schema server writes: a property whose name is the schema kind, whose
// modification revision orders revisions of the same property, and whose tags
// carry the resource group and the embedded schema payload.
func nidx01dSourceBytes(t *testing.T, declared nidx01dDocument) []byte {
	t.Helper()
	payload, err := protojson.Marshal(declared.source)
	require.NoError(t, err)
	prop := &propertyv1.Property{
		Id: declared.propID,
		Metadata: &commonv1.Metadata{
			Name:        declared.kind.String(),
			ModRevision: declared.modRev,
		},
		Tags: []*modelv1.Tag{
			nidx01dTag(property.TagKeyGroup, declared.group),
			nidx01dTag(property.TagKeySource, string(payload)),
		},
	}
	encoded, err := protojson.Marshal(prop)
	require.NoError(t, err)
	return encoded
}

func nidx01dTag(key, value string) *modelv1.Tag {
	return &modelv1.Tag{
		Key:   key,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	}
}

// nidx01dDamagedOrdinal returns the local document number of the revision the
// corpus damages, found by streaming the freshly written corpus through the
// native match-all walk and matching the stored `_source` bytes. The ordinal is
// observed rather than assumed, and recorded in the provenance manifest.
func nidx01dDamagedOrdinal(t *testing.T) uint64 {
	t.Helper()
	var damaged nidx01dDocument
	for _, declared := range nidx01dDocuments {
		if declared.damaged {
			damaged = declared
		}
	}
	wanted := nidx01dSourceBytes(t, damaged)

	ordinal := uint64(0)
	found := false
	require.NoError(t, inverted.ReadOnlyWalkDocuments(context.Background(), nidx01dShardDir,
		func(doc inverted.StoredDocument) error {
			if found {
				return nil
			}
			matched := false
			if visitErr := doc.VisitStoredFields(func(name string, value []byte) bool {
				if name == propSourceField && bytes.Equal(value, wanted) {
					matched = true
				}
				return true
			}); visitErr != nil {
				return visitErr
			}
			if matched {
				found = true
				return nil
			}
			ordinal++
			return nil
		}))
	require.True(t, found, "the corpus must hold the revision it declares damaged")
	return ordinal
}

// damageStoredDocumentRecord makes one document's stored record unreachable by
// pointing its entry in the segment's stored-document offset index at the end
// of the chunk that holds it. Every other document's record, the chunk table,
// the fields index and the footer are left exactly as the oracle wrote them, so
// the damage is confined to the one document and only surfaces when that
// document is decoded.
func damageStoredDocumentRecord(t *testing.T, segmentPath string, documentNumber uint64) {
	t.Helper()
	payload, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Greater(t, len(payload), iceFooterLength)
	footer := payload[len(payload)-iceFooterLength:]
	storedIndex := binary.BigEndian.Uint64(footer[iceFooterStoredIndexStart:iceFooterStoredIndexEnd])
	require.Positive(t, storedIndex, "segment %s holds no stored document index", segmentPath)

	entry := storedIndex + documentNumber*storedDocumentOffsetWidth
	require.LessOrEqual(t, entry+storedDocumentOffsetWidth, uint64(len(payload)-iceFooterLength))
	chunkLength := decodedStoredChunkLength(t, payload, storedIndex, documentNumber/storedDocumentsPerChunk)
	binary.BigEndian.PutUint64(payload[entry:entry+storedDocumentOffsetWidth], chunkLength)
	require.NoError(t, os.WriteFile(segmentPath, payload, 0o600))
}

// decodedStoredChunkLength returns the decoded byte length of one stored chunk,
// read straight from the chunk table BDB-NIDX-SPEC-001 revision 0.2 section 08
// places below the stored-fields index root: a run of variable-length chunk
// offsets, then the table's byte length and chunk count as two big-endian
// 32-bit values.
func decodedStoredChunkLength(t *testing.T, payload []byte, storedIndex, chunkIndex uint64) uint64 {
	t.Helper()
	require.GreaterOrEqual(t, storedIndex, uint64(storedChunkTableFooterLen))
	tableFooter := payload[storedIndex-storedChunkTableFooterLen : storedIndex]
	offsetLength := uint64(binary.BigEndian.Uint32(tableFooter[0:4]))
	chunkCount := uint64(binary.BigEndian.Uint32(tableFooter[4:8]))
	require.Greater(t, chunkCount, chunkIndex+1, "the segment holds no stored chunk %d", chunkIndex)

	tableStart := storedIndex - storedChunkTableFooterLen - offsetLength
	table := payload[tableStart : tableStart+offsetLength]
	offsets := make([]uint64, 0, chunkCount)
	for cursor := 0; cursor < len(table); {
		value, width := binary.Uvarint(table[cursor:])
		require.Positive(t, width, "the stored chunk table holds an invalid offset")
		offsets = append(offsets, value)
		cursor += width
	}
	require.Len(t, offsets, int(chunkCount))

	decoded, err := s2.Decode(nil, payload[offsets[chunkIndex]:offsets[chunkIndex+1]])
	require.NoError(t, err)
	return uint64(len(decoded))
}

// nidx01dSegmentFile returns the corpus's single segment file.
func nidx01dSegmentFile(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(nidx01dShardDir)
	require.NoError(t, err)
	var segments []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".seg" {
			segments = append(segments, filepath.Join(nidx01dShardDir, entry.Name()))
		}
	}
	require.Len(t, segments, 1, "the corpus must hold exactly one segment")
	return segments[0]
}

// nidx01dProvenanceBytes renders the manifest checked in beside the corpus.
func nidx01dProvenanceBytes(t *testing.T, damagedOrdinal uint64) []byte {
	t.Helper()
	described := make([]nidx01dProvenanceDocument, 0, len(nidx01dDocuments))
	for _, declared := range nidx01dDocuments {
		described = append(described, nidx01dProvenanceDocument{
			Label:   declared.label,
			PropID:  declared.propID,
			Kind:    declared.kind.String(),
			Group:   declared.group,
			ModRev:  declared.modRev,
			Deleted: declared.deleted,
			Damaged: declared.damaged,
		})
	}
	manifest := nidx01dProvenance{
		Oracle: map[string]string{
			"writer":        "pkg/index/inverted.NewStore",
			"go_mod_sha256": nidx01dGoModSHA256(t),
		},
		GeneratorCommand: nidx01dGenerateCmd,
		FileSHA256:       nidx01dFileHashes(t),
		Documents:        described,
		DamagedOrdinal:   damagedOrdinal,
		Damage: map[string]string{
			"target": "the stored-document offset index entry of the damaged revision",
			"change": "the entry is set to the decoded length of the stored chunk holding it, " +
				"so the record starts at the chunk's end and no field can be read from it",
			"scope": "one document; the chunk table, the fields index, the footer, the reserved CRC32 slots " +
				"and every other document's record are exactly as the oracle wrote them",
		},
		Notes: "Seven physical revisions of five logical properties in one `_schema` shard. " +
			"For kinds {Stream} the catalog walk yields only p1@2; for {Stream, Group} it yields p1@2 and p4@1; " +
			"p3 is absent because its latest revision is a tombstone. A Stream walk never decodes p5, " +
			"so it succeeds; a Measure walk decodes p5 and fails with the native typed corruption error.",
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	return append(encoded, '\n')
}

// nidx01dGoModSHA256 pins the dependency set the oracle ran with, so the corpus
// is identified by an immutable content hash rather than by a module name.
func nidx01dGoModSHA256(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "go.mod"))
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// nidx01dFileHashes hashes every checked-in corpus file.
func nidx01dFileHashes(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(nidx01dShardDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	hashes := make(map[string]string, len(names))
	for _, name := range names {
		payload, readErr := os.ReadFile(filepath.Join(nidx01dShardDir, name))
		require.NoError(t, readErr)
		sum := sha256.Sum256(payload)
		hashes["shard-0/"+name] = hex.EncodeToString(sum[:])
	}
	return hashes
}
