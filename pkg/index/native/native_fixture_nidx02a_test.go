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

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// The NIDX-02A corpus is one Property shard generation written by the retired
// compatibility writer through the retired compatibility store boundary. It is the
// oracle the native encoder is measured against: the encoder is handed the same
// declared rows, and the generation it produces must yield the same visible
// count, stored walk, exact-term selection and repair page order as these
// checked-in bytes.
//
// The corpus is deliberately not a re-encoding of anything the encoder emits.
// Its bytes came from the compatibility writer, pinned by the content hash of
// the dependency set it ran with; the generator that produced it ran against
// the now-removed legacy engine and has been deleted along with it.
const (
	nidx02aRoot     = "testdata/nidx02a"
	nidx02aShardDir = nidx02aRoot + "/shard-0"

	// The field names below are the Property shard's own, pinned here as
	// literals rather than imported: the retired compatibility store sat underneath
	// banyand/property/db, so the corpus declares the names it was written with
	// instead of borrowing the constants the consumer happens to hold.
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

	// nidx02aVisibleRowCount is how many rows survive the pinned generation's
	// deletion masks.
	nidx02aVisibleRowCount = int64(4)
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
