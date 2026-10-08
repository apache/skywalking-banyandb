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

package native

import (
	"strconv"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// The NIDX-01E corpus is the Property shard issue #14012 declares: one shard
// directory whose newest committed generation holds the four visible repair
// rows the issue lists, in the ascending order it lists them, plus one deleted
// row that sorts between the first two so a page that ignored the deletion
// masks would be caught at the very first page boundary.
//
// Every byte was produced by the retired compatibility writer through the
// retired compatibility store boundary and checked in; the generator that
// produced it ran against the now-removed legacy engine and has been deleted
// along with it.
//
// It is an independent fixture in the sense the milestone needs: the expected
// rows, cursor and repair-tree leaves are issue #14012's own declaration, and
// the bytes are an oracle's output that no reader under test produced.
const (
	nidx01eRoot     = "../testdata/nidx01e"
	nidx01eShardDir = nidx01eRoot + "/shard-0"

	// The field names below are the Property shard's own, pinned here as
	// literals rather than imported: the retired compatibility store sat underneath
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

// nidx01eDocID renders the document identifier the Property writer gives a
// revision: the entity it belongs to followed by that revision.
func nidx01eDocID(row nidx01eRow) string {
	return row.group + "/" + row.name + "/" + row.entityID + "/" + strconv.FormatInt(row.timestamp, 10)
}

// nidx01eSortableField builds a field the segment indexes and records a doc
// value for.
func nidx01eSortableField(name string, value []byte) nativeice.EncodeField {
	return nativeice.EncodeField{Name: name, Value: value, Index: true, Sort: true}
}

// nidx01eStoredField builds a field the segment stores but neither indexes nor
// records a doc value for.
func nidx01eStoredField(name string, value []byte) nativeice.EncodeField {
	return nativeice.EncodeField{Name: name, Value: value, Store: true}
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
