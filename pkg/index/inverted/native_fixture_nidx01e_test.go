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

	"github.com/apache/skywalking-banyandb/pkg/index"
)

// The NIDX-01E corpus is the Property shard issue #14012 declares: one shard
// directory whose newest committed generation holds the four visible repair
// rows the issue lists, in the ascending order it lists them, plus one deleted
// row that sorts between the first two so a page that ignored the deletion
// masks would be caught at the very first page boundary.
//
// Every byte is produced by BanyanDB's compatibility writer through the
// pkg/index/inverted store boundary and checked in; production code never
// generates it, and the generator lives in this test file behind
// nidx01eGenerateEnv.
//
// It is an independent fixture in the sense the milestone needs: the expected
// rows, cursor and repair-tree leaves are issue #14012's own declaration, and
// the bytes are an oracle's output that no reader under test produced.
const (
	nidx01eRoot     = "testdata/nidx01e"
	nidx01eShardDir = nidx01eRoot + "/shard-0"
	nidx01eManifest = nidx01eRoot + "/provenance.json"

	// nidx01eGenerateEnv gates regeneration so an ordinary test run can never
	// overwrite the checked-in bytes the contract is pinned to.
	nidx01eGenerateEnv = "GENERATE_NIDX01E_FIXTURE"
	nidx01eGenerateCmd = "GENERATE_NIDX01E_FIXTURE=1 go test ./pkg/index/inverted/ -run TestGenerateNIDX01EFixture -count=1"

	// The field names below are the Property shard's own, pinned here as
	// literals rather than imported: pkg/index/inverted sits underneath
	// banyand/property/db, so the corpus declares the names it was written with
	// instead of borrowing the constants the consumer happens to hold.
	nidx01eGroupField  = "_group"
	nidx01eNameField   = index.IndexModeName
	nidx01eEntityField = "_entity_id"
	nidx01eSourceField = "_source"
	nidx01eSHAField    = "_sha_value"

	// nidx01eSnapshotID is the identifier of the committed generation the
	// corpus's newest snapshot manifest names. It is a property of the
	// checked-in bytes, recorded when they were produced.
	nidx01eSnapshotID = uint64(3)

	// nidx01eVisibleRowCount is how many rows of the corpus survive the pinned
	// generation's deletion masks: the four the issue declares.
	nidx01eVisibleRowCount = int64(4)
)

// nidx01eRow is one physical document of the corpus: the four ascending sort
// components the repair order is built from, the stored SHA a repair leaf
// carries, an unrelated stored payload that must stay unread, and whether the
// row is deleted in the pinned generation.
type nidx01eRow struct {
	group     string
	name      string
	entityID  string
	sha       string
	source    string
	timestamp int64
	deleted   bool
}

// nidx01eRows is the corpus issue #14012 declares, in insertion order. The four
// visible rows are the issue's rows 1 to 4; row "e-1@15" is the extra deleted
// row, placed between visible rows 1 and 2 in the ascending order so its
// exclusion is observable inside the first page rather than only at the end.
var nidx01eRows = []nidx01eRow{
	{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 10, sha: "sha-a", source: "source-a"},
	{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 15, sha: "sha-x", source: "source-x", deleted: true},
	{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 20, sha: "sha-b", source: "source-b"},
	{group: "g-a", name: "n-b", entityID: "e-2", timestamp: 5, sha: "sha-c", source: "source-c"},
	{group: "g-b", name: "n-a", entityID: "e-3", timestamp: 7, sha: "sha-d", source: "source-d"},
}

// nidx01eProvenance is the manifest checked in beside the corpus bytes. It
// records which oracle produced them, how to re-derive them, what every row
// declares, which row is deleted, which generation the bytes commit, and what
// every file hashes to.
type nidx01eProvenance struct {
	Oracle           map[string]string        `json:"oracle"`
	FileSHA256       map[string]string        `json:"file_sha256"`
	GeneratorCommand string                   `json:"generator_command"`
	Notes            string                   `json:"notes"`
	Rows             []nidx01eProvenanceRow   `json:"rows"`
	DeclaredPages    [][]nidx01eProvenanceRow `json:"declared_pages"`
	SnapshotID       uint64                   `json:"snapshot_id"`
	VisibleRowCount  int64                    `json:"visible_row_count"`
}

// nidx01eProvenanceRow records one physical document of the corpus so a
// reviewer can compare it against issue #14012 without decoding a segment.
type nidx01eProvenanceRow struct {
	Group     string `json:"group"`
	Name      string `json:"name"`
	EntityID  string `json:"entity_id"`
	SHA       string `json:"sha_value"`
	DocID     string `json:"doc_id"`
	Timestamp int64  `json:"timestamp"`
	Deleted   bool   `json:"deleted"`
}

// TestGenerateNIDX01EFixture rebuilds the checked-in NIDX-01E corpus and its
// provenance manifest with BanyanDB's compatibility writer. It runs only when
// nidx01eGenerateEnv is set, so a normal test run reads the committed bytes
// instead of producing fresh ones.
//
// The corpus is written as one commit followed by one deletion, which is how a
// Property shard reaches a generation that carries both live and masked rows
// for the same entity. Every visible row keeps its own document identifier --
// the Property writer keys a document by entity and revision -- so the two
// revisions of e-1 are two live rows rather than one replacing the other, which
// is the case the repair build's own per-entity collapse exists to handle.
func TestGenerateNIDX01EFixture(t *testing.T) {
	if os.Getenv(nidx01eGenerateEnv) != "1" {
		t.Skipf("set %s=1 to regenerate the NIDX-01E corpus", nidx01eGenerateEnv)
	}
	tester := require.New(t)

	staging := t.TempDir()
	store, err := NewStore(StoreOpts{Path: staging})
	tester.NoError(err)
	tester.NoError(store.UpdateSeriesBatch(index.Batch{Documents: nidx01eBatch()}))
	for _, row := range nidx01eRows {
		if !row.deleted {
			continue
		}
		tester.NoError(store.Delete([][]byte{[]byte(nidx01eDocID(row))}))
	}
	tester.NoError(store.Close())

	tester.NoError(os.RemoveAll(nidx01eRoot))
	tester.NoError(os.MkdirAll(nidx01eShardDir, 0o755))
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
		tester.NoError(os.WriteFile(filepath.Join(nidx01eShardDir, entry.Name()), payload, 0o600))
	}
	tester.NotZero(segments, "the corpus must hold at least one sealed segment")

	// The corpus is only evidence if its own declaration holds on the bytes
	// just written, so the two facts every later assertion rests on -- which
	// generation is newest, and that exactly the four declared rows survive its
	// deletion masks -- are checked here with operations already merged.
	tester.Equal(nidx01eSnapshotID, snapshots, "regenerating moved the corpus's newest generation; update nidx01eSnapshotID")
	visible, err := ReadOnlyDocCount(nidx01eShardDir)
	tester.NoError(err)
	tester.Equal(nidx01eVisibleRowCount, visible, "the corpus must leave exactly the declared visible rows undeleted")

	tester.NoError(os.WriteFile(nidx01eManifest, nidx01eProvenanceBytes(t), 0o600))
}

// nidx01eBatch turns the declared rows into one commit shaped like the Property
// shard's own writes: the entity identifier, the group and the name as indexed,
// sortable fields, an unrelated stored payload, and the repair SHA as a stored
// field, with the property's modification revision as the document timestamp.
func nidx01eBatch() index.Documents {
	documents := make(index.Documents, 0, len(nidx01eRows))
	for _, row := range nidx01eRows {
		documents = append(documents, index.Document{
			EntityValues: []byte(nidx01eDocID(row)),
			Timestamp:    row.timestamp,
			Fields: []index.Field{
				nidx01eSortableField(nidx01eEntityField, []byte(row.entityID)),
				nidx01eSortableField(nidx01eGroupField, []byte(row.group)),
				nidx01eSortableField(nidx01eNameField, []byte(row.name)),
				nidx01eStoredField(nidx01eSourceField, []byte(row.source)),
				nidx01eStoredField(nidx01eSHAField, []byte(row.sha)),
			},
		})
	}
	return documents
}

// nidx01eDocID renders the document identifier the Property writer gives a
// revision: the entity it belongs to followed by that revision.
func nidx01eDocID(row nidx01eRow) string {
	return row.group + "/" + row.name + "/" + row.entityID + "/" + strconv.FormatInt(row.timestamp, 10)
}

// nidx01eSortableField builds a field the segment indexes and records a doc
// value for.
func nidx01eSortableField(name string, value []byte) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, value)
	field.Index = true
	return field
}

// nidx01eStoredField builds a field the segment stores but neither indexes nor
// records a doc value for.
func nidx01eStoredField(name string, value []byte) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, value)
	field.Store = true
	field.NoSort = true
	return field
}

// nidx01eVisibleRows lists the corpus's rows that survive its deletion masks,
// in the ascending order issue #14012 declares them.
func nidx01eVisibleRows() []nidx01eRow {
	visible := make([]nidx01eRow, 0, len(nidx01eRows))
	for _, row := range nidx01eRows {
		if !row.deleted {
			visible = append(visible, row)
		}
	}
	return visible
}

// parseFixtureIdentifier reads the numeric identifier out of a fixed-width
// hexadecimal index file name.
func parseFixtureIdentifier(name, extension string) (uint64, bool) {
	base := filepath.Base(name)
	if filepath.Ext(base) != extension {
		return 0, false
	}
	identifier, err := strconv.ParseUint(base[:len(base)-len(extension)], 16, 64)
	if err != nil {
		return 0, false
	}
	return identifier, true
}

// nidx01eProvenanceBytes renders the manifest checked in beside the corpus.
func nidx01eProvenanceBytes(t *testing.T) []byte {
	t.Helper()
	described := make([]nidx01eProvenanceRow, 0, len(nidx01eRows))
	for _, row := range nidx01eRows {
		described = append(described, nidx01eDescribeRow(row))
	}
	visible := nidx01eVisibleRows()
	manifest := nidx01eProvenance{
		Oracle: map[string]string{
			"writer":        "pkg/index/inverted.NewStore",
			"go_mod_sha256": nidx01eGoModSHA256(t),
		},
		GeneratorCommand: nidx01eGenerateCmd,
		FileSHA256:       nidx01eFileHashes(t),
		Rows:             described,
		DeclaredPages: [][]nidx01eProvenanceRow{
			{nidx01eDescribeRow(visible[0]), nidx01eDescribeRow(visible[1])},
			{nidx01eDescribeRow(visible[2]), nidx01eDescribeRow(visible[3])},
		},
		SnapshotID:      nidx01eSnapshotID,
		VisibleRowCount: nidx01eVisibleRowCount,
		Notes: "One Property shard generation holding five physical rows of three entities. " +
			"Ascending by (_group, _im_name, _entity_id, _timestamp) the visible rows are " +
			"(g-a,n-a,e-1,10,sha-a), (g-a,n-a,e-1,20,sha-b), (g-a,n-b,e-2,5,sha-c), (g-b,n-a,e-3,7,sha-d). " +
			"Row (g-a,n-a,e-1,15,sha-x) is deleted in this generation and sorts between the first two visible rows, " +
			"so a page that ignored the deletion masks is caught inside the first page rather than at the end. " +
			"At page size 2 the first page is the first two visible rows, the strict cursor is (g-a,n-a,e-1,20), " +
			"and the second page is the last two. The repair tree these rows complete maps " +
			"g-a/n-a/e-1 to sha-b, g-a/n-b/e-2 to sha-c and g-b/n-a/e-3 to sha-d. " +
			"Every row also stores a _source payload that no repair page decodes.",
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	return append(encoded, '\n')
}

func nidx01eDescribeRow(row nidx01eRow) nidx01eProvenanceRow {
	return nidx01eProvenanceRow{
		Group:     row.group,
		Name:      row.name,
		EntityID:  row.entityID,
		Timestamp: row.timestamp,
		SHA:       row.sha,
		DocID:     nidx01eDocID(row),
		Deleted:   row.deleted,
	}
}

// nidx01eGoModSHA256 pins the dependency set the oracle ran with, so the corpus
// is identified by an immutable content hash rather than by a module name.
func nidx01eGoModSHA256(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "go.mod"))
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// nidx01eFileHashes hashes every checked-in corpus file.
func nidx01eFileHashes(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(nidx01eShardDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	hashes := make(map[string]string, len(names))
	for _, name := range names {
		payload, readErr := os.ReadFile(filepath.Join(nidx01eShardDir, name))
		require.NoError(t, readErr)
		sum := sha256.Sum256(payload)
		hashes["shard-0/"+name] = hex.EncodeToString(sum[:])
	}
	return hashes
}
