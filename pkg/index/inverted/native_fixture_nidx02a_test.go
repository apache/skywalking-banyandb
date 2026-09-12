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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// The NIDX-02A corpus is one Property shard generation written by BanyanDB's
// compatibility writer through the pkg/index/inverted store boundary. It is the
// oracle the native encoder is measured against: the encoder is handed the same
// declared rows, and the generation it produces must yield the same visible
// count, stored walk, exact-term selection and repair page order as these
// checked-in bytes.
//
// The corpus is deliberately not a re-encoding of anything the encoder emits.
// Its bytes come from the compatibility writer, pinned by the content hash of
// the dependency set it ran with, and production code never generates them; the
// generator lives in this test file behind nidx02aGenerateEnv so an ordinary
// run reads the committed bytes instead of producing fresh ones.
const (
	nidx02aRoot     = "testdata/nidx02a"
	nidx02aShardDir = nidx02aRoot + "/shard-0"
	nidx02aManifest = nidx02aRoot + "/provenance.json"

	// nidx02aGenerateEnv gates regeneration so an ordinary test run can never
	// overwrite the checked-in bytes the contract is pinned to.
	nidx02aGenerateEnv = "GENERATE_NIDX02A_FIXTURE"
	nidx02aGenerateCmd = "GENERATE_NIDX02A_FIXTURE=1 go test ./pkg/index/inverted/ -run TestGenerateNIDX02AFixture -count=1"

	// The field names below are the Property shard's own, pinned here as
	// literals rather than imported: pkg/index/inverted sits underneath
	// banyand/property/db, so the corpus declares the names it was written with
	// instead of borrowing the constants the consumer happens to hold.
	nidx02aIDField        = "_id"
	nidx02aEntityField    = "_entity_id"
	nidx02aGroupField     = "_group"
	nidx02aNameField      = index.IndexModeName
	nidx02aSourceField    = "_source"
	nidx02aTimestampField = "_timestamp"
	nidx02aDeletedField   = "_deleted"
	nidx02aSHAField       = "_sha_value"

	// nidx02aTagKey is the Property tag the corpus carries. The Property write
	// path names a tag field by the hash of its key rather than by the key, so
	// the corpus records both: the key a reader recognizes and the field name
	// the bytes actually hold.
	nidx02aTagKey = "color"

	// nidx02aSnapshotID is the identifier of the committed generation the
	// corpus's newest snapshot manifest names. It is a property of the
	// checked-in bytes, recorded when they were produced.
	nidx02aSnapshotID = uint64(3)

	// nidx02aPhysicalRowCount and nidx02aVisibleRowCount are how many rows the
	// pinned generation holds and how many survive its deletion masks.
	nidx02aPhysicalRowCount = int64(6)
	nidx02aVisibleRowCount  = int64(4)
)

// nidx02aRow is one physical document of the corpus: the four ascending sort
// components the repair order is built from, the stored SHA a repair leaf
// carries, the stored payloads, the indexed tag, whether the row carries a
// deletion marker as a stored field, and whether the generation's deletion
// masks hide it.
//
// sources is a slice rather than a single value because one declared row
// records the same stored field name twice. An encoder that collapsed repeated
// stored values would still produce the right count and the right selection and
// would be caught only here.
type nidx02aRow struct {
	group     string
	name      string
	entityID  string
	sha       string
	tag       string
	sources   []string
	timestamp int64
	deletedAt int64
	masked    bool
}

// nidx02aRows is the corpus, in insertion order.
//
// Ascending by (_group, _im_name, _entity_id, _timestamp) the six rows order as
// e-1@10, e-1@15, e-1@20, e-2@5, e-3@7, e-4@30. Two of them are masked: e-1@15
// sorts between the first two visible rows, so a page built without the
// deletion masks is caught inside the very first page; e-4@30 sorts last, so a
// mask dropped only at the tail is caught too.
//
// Row e-1@10 records two _source values, and row e-2@5 carries the Property
// deletion marker as a stored field while staying visible -- a row marked
// deleted by the Property write path is still a live document of the
// generation, which is a different thing from a masked row.
var nidx02aRows = []nidx02aRow{
	{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 10, sha: "sha-a", sources: []string{"source-a1", "source-a2"}, tag: "red"},
	{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 15, sha: "sha-x", sources: []string{"source-x"}, tag: "red", masked: true},
	{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 20, sha: "sha-b", sources: []string{"source-b"}, tag: "blue"},
	{group: "g-a", name: "n-b", entityID: "e-2", timestamp: 5, sha: "sha-c", sources: []string{"source-c"}, tag: "red", deletedAt: 99},
	{group: "g-b", name: "n-a", entityID: "e-3", timestamp: 7, sha: "sha-d", sources: []string{"source-d"}, tag: "blue"},
	{group: "g-b", name: "n-b", entityID: "e-4", timestamp: 30, sha: "sha-e", sources: []string{"source-e"}, tag: "red", masked: true},
}

// nidx02aEncodedTimestamps are the exact doc values and stored values the
// corpus records for each declared revision. They are literals lifted from the
// checked-in generation, not a re-encoding: an eleven-byte historical
// prefix-coded signed int64 whose leading byte is the shift and whose sign bit
// is inverted so byte order matches numeric order.
var nidx02aEncodedTimestamps = map[int64][]byte{
	5:  {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
	7:  {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07},
	10: {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a},
	15: {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f},
	20: {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14},
	30: {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1e},
}

// nidx02aProvenance is the manifest checked in beside the corpus bytes. It
// records which oracle produced them, how to re-derive them, what every row
// declares, which rows the generation masks, which generation the bytes commit,
// and what every file hashes to.
type nidx02aProvenance struct {
	Oracle           map[string]string      `json:"oracle"`
	FileSHA256       map[string]string      `json:"file_sha256"`
	GeneratorCommand string                 `json:"generator_command"`
	Notes            string                 `json:"notes"`
	TagFieldName     string                 `json:"tag_field_name_hex"`
	Rows             []nidx02aProvenanceRow `json:"rows"`
	PhysicalRowCount int64                  `json:"physical_row_count"`
	SnapshotID       uint64                 `json:"snapshot_id"`
	VisibleRowCount  int64                  `json:"visible_row_count"`
}

// nidx02aProvenanceRow records one physical document of the corpus so a
// reviewer can compare it against the milestone's declaration without decoding
// a segment.
type nidx02aProvenanceRow struct {
	Group     string   `json:"group"`
	Name      string   `json:"name"`
	EntityID  string   `json:"entity_id"`
	SHA       string   `json:"sha_value"`
	DocID     string   `json:"doc_id"`
	Tag       string   `json:"tag"`
	Sources   []string `json:"sources"`
	Timestamp int64    `json:"timestamp"`
	DeletedAt int64    `json:"deleted_at"`
	Masked    bool     `json:"masked"`
}

// TestGenerateNIDX02AFixture rebuilds the checked-in NIDX-02A corpus and its
// provenance manifest with BanyanDB's compatibility writer. It runs only when
// nidx02aGenerateEnv is set, so a normal test run reads the committed bytes
// instead of producing fresh ones.
//
// The corpus is written as one commit followed by one deletion, which is how a
// Property shard reaches a generation carrying both live and masked rows for
// the same entity. Every row keeps its own document identifier -- the Property
// writer keys a document by entity and revision -- so the three revisions of
// e-1 are three physical rows rather than one replacing the others.
func TestGenerateNIDX02AFixture(t *testing.T) {
	if os.Getenv(nidx02aGenerateEnv) != "1" {
		t.Skipf("set %s=1 to regenerate the NIDX-02A corpus", nidx02aGenerateEnv)
	}
	tester := require.New(t)

	staging := t.TempDir()
	store, err := NewStore(StoreOpts{Path: staging})
	tester.NoError(err)
	tester.NoError(store.UpdateSeriesBatch(index.Batch{Documents: nidx02aBatch()}))
	masked := make([][]byte, 0, len(nidx02aRows))
	for _, row := range nidx02aRows {
		if row.masked {
			masked = append(masked, []byte(nidx02aDocID(row)))
		}
	}
	tester.NoError(store.Delete(masked))
	tester.NoError(store.Close())

	tester.NoError(os.RemoveAll(nidx02aRoot))
	tester.NoError(os.MkdirAll(nidx02aShardDir, 0o755))
	entries, err := os.ReadDir(staging)
	tester.NoError(err)
	segments, snapshots := 0, uint64(0)
	for _, entry := range entries {
		extension := filepath.Ext(entry.Name())
		if extension != segExt && extension != snpExt {
			continue
		}
		if extension == segExt {
			segments++
		} else if identifier, parsed := parseFixtureIdentifier(entry.Name(), snpExt); parsed && identifier > snapshots {
			snapshots = identifier
		}
		payload, readErr := os.ReadFile(filepath.Join(staging, entry.Name()))
		tester.NoError(readErr)
		tester.NoError(os.WriteFile(filepath.Join(nidx02aShardDir, entry.Name()), payload, 0o600))
	}
	tester.NotZero(segments, "the corpus must hold at least one sealed segment")

	// The corpus is only evidence if its own declaration holds on the bytes
	// just written, so the two facts every later assertion rests on -- which
	// generation is newest, and that exactly the declared rows survive its
	// deletion masks -- are checked here with operations already merged.
	tester.Equal(nidx02aSnapshotID, snapshots, "regenerating moved the corpus's newest generation; update nidx02aSnapshotID")
	visible, err := ReadOnlyDocCount(nidx02aShardDir)
	tester.NoError(err)
	tester.Equal(nidx02aVisibleRowCount, visible, "the corpus must leave exactly the declared visible rows undeleted")

	tester.NoError(os.WriteFile(nidx02aManifest, nidx02aProvenanceBytes(t), 0o600))
}

// nidx02aBatch turns the declared rows into one commit shaped like the Property
// shard's own writes: the entity identifier, the group, the name and the tag as
// indexed sortable fields, the payloads and the repair SHA as stored fields, the
// Property deletion marker as a stored field where the row declares one, and
// the property's modification revision as the document timestamp.
func nidx02aBatch() index.Documents {
	documents := make(index.Documents, 0, len(nidx02aRows))
	for _, row := range nidx02aRows {
		fields := []index.Field{
			nidx02aSortableField(index.FieldKey{TagName: nidx02aEntityField}, []byte(row.entityID)),
			nidx02aSortableField(index.FieldKey{TagName: nidx02aGroupField}, []byte(row.group)),
			nidx02aSortableField(index.FieldKey{TagName: nidx02aNameField}, []byte(row.name)),
			nidx02aSortableField(nidx02aTagFieldKey(), []byte(row.tag)),
		}
		for _, source := range row.sources {
			fields = append(fields, nidx02aStoredField(nidx02aSourceField, []byte(source)))
		}
		if row.deletedAt > 0 {
			fields = append(fields, nidx02aStoredField(nidx02aDeletedField, convert.Int64ToBytes(row.deletedAt)))
		}
		fields = append(fields, nidx02aStoredField(nidx02aSHAField, []byte(row.sha)))
		documents = append(documents, index.Document{
			EntityValues: []byte(nidx02aDocID(row)),
			Timestamp:    row.timestamp,
			Fields:       fields,
		})
	}
	return documents
}

// nidx02aTagFieldKey is the field key the Property write path gives the
// corpus's tag: the hash of the tag key rather than the key itself.
func nidx02aTagFieldKey() index.FieldKey {
	return index.FieldKey{IndexRuleID: uint32(convert.HashStr(nidx02aTagKey))}
}

// nidx02aTagFieldName renders the segment field name the corpus's tag is
// recorded under.
func nidx02aTagFieldName() string {
	return nidx02aTagFieldKey().Marshal()
}

// nidx02aDocID renders the document identifier the Property writer gives a
// revision: the entity it belongs to followed by that revision.
func nidx02aDocID(row nidx02aRow) string {
	return row.group + "/" + row.name + "/" + row.entityID + "/" + strconv.FormatInt(row.timestamp, 10)
}

// nidx02aSortableField builds a field the segment indexes and records a doc
// value for.
func nidx02aSortableField(key index.FieldKey, value []byte) index.Field {
	field := index.NewBytesField(key, value)
	field.Index = true
	return field
}

// nidx02aStoredField builds a field the segment stores but neither indexes nor
// records a doc value for.
func nidx02aStoredField(name string, value []byte) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, value)
	field.Store = true
	field.NoSort = true
	return field
}

// nidx02aVisibleRows lists the corpus's rows that survive its deletion masks,
// in the ascending order the four declared sort components put them in.
func nidx02aVisibleRows() []nidx02aRow {
	visible := make([]nidx02aRow, 0, len(nidx02aRows))
	for _, row := range nidx02aRows {
		if !row.masked {
			visible = append(visible, row)
		}
	}
	return visible
}

// nidx02aProvenanceBytes renders the manifest checked in beside the corpus.
func nidx02aProvenanceBytes(t *testing.T) []byte {
	t.Helper()
	described := make([]nidx02aProvenanceRow, 0, len(nidx02aRows))
	for _, row := range nidx02aRows {
		described = append(described, nidx02aDescribeRow(row))
	}
	manifest := nidx02aProvenance{
		Oracle: map[string]string{
			"writer":        "pkg/index/inverted.NewStore",
			"go_mod_sha256": nidx02aGoModSHA256(t),
		},
		GeneratorCommand: nidx02aGenerateCmd,
		FileSHA256:       nidx02aFileHashes(t),
		TagFieldName:     hex.EncodeToString([]byte(nidx02aTagFieldName())),
		Rows:             described,
		PhysicalRowCount: nidx02aPhysicalRowCount,
		SnapshotID:       nidx02aSnapshotID,
		VisibleRowCount:  nidx02aVisibleRowCount,
		Notes: "One Property shard generation holding six physical rows of four entities. " +
			"Ascending by (_group, _im_name, _entity_id, _timestamp) the rows order as " +
			"e-1@10, e-1@15, e-1@20, e-2@5, e-3@7, e-4@30. Rows e-1@15 and e-4@30 are masked by the " +
			"generation's deletion masks: the first sorts between the two leading visible rows so a page " +
			"built without the masks is caught inside the first page, and the second sorts last so a mask " +
			"dropped only at the tail is caught too. Row e-1@10 records two _source stored values, so an " +
			"encoder that collapsed repeated stored values is caught. Row e-2@5 carries the Property " +
			"_deleted marker as a stored field while staying a live document of the generation. Every row " +
			"also carries one indexed, sortable Property tag recorded under the hash of its key.",
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	return append(encoded, '\n')
}

func nidx02aDescribeRow(row nidx02aRow) nidx02aProvenanceRow {
	return nidx02aProvenanceRow{
		Group:     row.group,
		Name:      row.name,
		EntityID:  row.entityID,
		Timestamp: row.timestamp,
		SHA:       row.sha,
		Sources:   row.sources,
		Tag:       row.tag,
		DocID:     nidx02aDocID(row),
		DeletedAt: row.deletedAt,
		Masked:    row.masked,
	}
}

// nidx02aGoModSHA256 pins the dependency set the oracle ran with, so the corpus
// is identified by an immutable content hash rather than by a module name.
func nidx02aGoModSHA256(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "go.mod"))
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// nidx02aFileHashes hashes every checked-in corpus file.
func nidx02aFileHashes(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(nidx02aShardDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	hashes := make(map[string]string, len(names))
	for _, name := range names {
		payload, readErr := os.ReadFile(filepath.Join(nidx02aShardDir, name))
		require.NoError(t, readErr)
		sum := sha256.Sum256(payload)
		hashes["shard-0/"+name] = hex.EncodeToString(sum[:])
	}
	return hashes
}
