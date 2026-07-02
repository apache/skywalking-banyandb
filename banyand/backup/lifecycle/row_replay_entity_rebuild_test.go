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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// seriesIDFor computes the real seriesID for (subject, entityValues) using the
// same pbv1.Series.Marshal the write path uses, so a part block carrying this id
// is exactly what the rebuild must reproduce.
func seriesIDFor(t *testing.T, subject string, ev ...*modelv1.TagValue) common.SeriesID {
	t.Helper()
	s := pbv1.Series{Subject: subject, EntityValues: ev}
	require.NoError(t, s.Marshal())
	return s.ID
}

// measureWithEntity builds a minimal measure schema: one tag family carrying the
// given tags, with the listed tag names forming the entity. buildEntityRebuildIndex
// keys candidates by the map subject, not Metadata, so no Metadata is needed.
func measureWithEntity(tags []*databasev1.TagSpec, entityNames []string) *databasev1.Measure {
	return &databasev1.Measure{
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: tags},
		},
		Entity: &databasev1.Entity{TagNames: entityNames},
	}
}

func strTagSpec(name string) *databasev1.TagSpec {
	return &databasev1.TagSpec{Name: name, Type: databasev1.TagType_TAG_TYPE_STRING}
}

func intTagSpec(name string) *databasev1.TagSpec {
	return &databasev1.TagSpec{Name: name, Type: databasev1.TagType_TAG_TYPE_INT}
}

// strCol / intCol render a single-row entity column the rebuild reads from a
// ColumnarBlock, in the on-disk raw-byte form dump.DecodeTagValue expects.
func strCol(v string) [][]byte { return [][]byte{[]byte(v)} }

func intCol(v int64) [][]byte { return [][]byte{convert.Int64ToBytes(v)} }

func newRebuildReplayer(schemas map[string]*databasev1.Measure) *measureRowReplayer {
	return &measureRowReplayer{rebuildIdx: buildEntityRebuildIndex(schemas)}
}

// TestRebuildEntity_StringEntity proves the core S4 fix: when the sidx cannot
// resolve a series, the entity is rebuilt byte-exactly from the part's own entity
// tag columns, recovering both subject and entity values.
func TestRebuildEntity_StringEntity(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"service_cpm": measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id"), strTagSpec("extra")},
			[]string{"entity_id"}),
	}
	r := newRebuildReplayer(schemas)

	want := stringTagValue("svc-A")
	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: seriesIDFor(t, "service_cpm", want),
		TagCols:  map[string][][]byte{"default.entity_id": strCol("svc-A")},
		TagTypes: map[string]pbv1.ValueType{"default.entity_id": pbv1.ValueTypeStr},
	}

	subject, ev, hadEntityCols, ok := r.rebuildEntity(cb.SeriesID, columnarBlockLookup(cb))
	require.True(t, ok, "must rebuild entity from columns")
	assert.True(t, hadEntityCols)
	assert.Equal(t, "service_cpm", subject)
	require.Len(t, ev, 1)
	assert.True(t, proto.Equal(want, ev[0]), "rebuilt entity value must equal the written value")
}

// TestRebuildEntity_IntEntity covers a non-string entity tag, exercising the
// int64 byte decode on the rebuild path.
func TestRebuildEntity_IntEntity(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"by_id": measureWithEntity([]*databasev1.TagSpec{intTagSpec("id")},
			[]string{"id"}),
	}
	r := newRebuildReplayer(schemas)

	want := intTagValue(987654321)
	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: seriesIDFor(t, "by_id", want),
		TagCols:  map[string][][]byte{"default.id": intCol(987654321)},
		TagTypes: map[string]pbv1.ValueType{"default.id": pbv1.ValueTypeInt64},
	}

	subject, ev, _, ok := r.rebuildEntity(cb.SeriesID, columnarBlockLookup(cb))
	require.True(t, ok)
	assert.Equal(t, "by_id", subject)
	require.Len(t, ev, 1)
	assert.True(t, proto.Equal(want, ev[0]))
}

// TestRebuildEntity_MultiTagEntity covers a composite entity (two ordered tags),
// proving the entity column order matches Entity.TagNames so the hash matches.
func TestRebuildEntity_MultiTagEntity(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"two": measureWithEntity([]*databasev1.TagSpec{strTagSpec("svc"), intTagSpec("inst")},
			[]string{"svc", "inst"}),
	}
	r := newRebuildReplayer(schemas)

	svc, inst := stringTagValue("checkout"), intTagValue(7)
	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: seriesIDFor(t, "two", svc, inst),
		TagCols: map[string][][]byte{
			"default.svc":  strCol("checkout"),
			"default.inst": intCol(7),
		},
		TagTypes: map[string]pbv1.ValueType{
			"default.svc":  pbv1.ValueTypeStr,
			"default.inst": pbv1.ValueTypeInt64,
		},
	}

	subject, ev, _, ok := r.rebuildEntity(cb.SeriesID, columnarBlockLookup(cb))
	require.True(t, ok)
	assert.Equal(t, "two", subject)
	require.Len(t, ev, 2)
	assert.True(t, proto.Equal(svc, ev[0]))
	assert.True(t, proto.Equal(inst, ev[1]))
}

// TestRebuildEntity_SameSignatureDisambiguatedByHash builds two measures sharing
// the identical entity column layout. Only the seriesID hash distinguishes them,
// so the rebuild must pick the measure whose (subject+entity) hashes to the
// block's seriesID — never a false match.
func TestRebuildEntity_SameSignatureDisambiguatedByHash(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"cpm":  measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id")}, []string{"entity_id"}),
		"resp": measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id")}, []string{"entity_id"}),
	}
	r := newRebuildReplayer(schemas)

	ent := stringTagValue("svc-Z")
	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: seriesIDFor(t, "resp", ent),
		TagCols:  map[string][][]byte{"default.entity_id": strCol("svc-Z")},
		TagTypes: map[string]pbv1.ValueType{"default.entity_id": pbv1.ValueTypeStr},
	}

	subject, _, _, ok := r.rebuildEntity(cb.SeriesID, columnarBlockLookup(cb))
	require.True(t, ok)
	assert.Equal(t, "resp", subject, "must select the measure whose hash matches, not the first bucket entry")
}

// TestRebuildEntity_HashMismatchSkips proves no false positives: a block whose
// seriesID does not match any candidate's recomputed hash is not rebuilt.
func TestRebuildEntity_HashMismatchSkips(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"cpm": measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id")}, []string{"entity_id"}),
	}
	r := newRebuildReplayer(schemas)

	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: 0xDEADBEEF, // not the hash of any (subject, entity) here
		TagCols:  map[string][][]byte{"default.entity_id": strCol("svc-A")},
		TagTypes: map[string]pbv1.ValueType{"default.entity_id": pbv1.ValueTypeStr},
	}

	_, _, hadEntityCols, ok := r.rebuildEntity(cb.SeriesID, columnarBlockLookup(cb))
	assert.False(t, ok, "hash mismatch must not rebuild")
	assert.True(t, hadEntityCols, "entity columns were present, so this is a rebuild miss, not an incomplete part")
}

// TestRebuildEntity_IncompletePart proves the S5 classification: a part missing
// the entity columns entirely reports hadEntityCols=false so the caller marks it
// incomplete-part rather than rebuild-failed.
func TestRebuildEntity_IncompletePart(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"cpm": measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id")}, []string{"entity_id"}),
	}
	r := newRebuildReplayer(schemas)

	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: seriesIDFor(t, "cpm", stringTagValue("svc-A")),
		TagCols:  map[string][][]byte{"default.other": strCol("x")}, // no entity column
		TagTypes: map[string]pbv1.ValueType{"default.other": pbv1.ValueTypeStr},
	}

	_, _, hadEntityCols, ok := r.rebuildEntity(cb.SeriesID, columnarBlockLookup(cb))
	assert.False(t, ok)
	assert.False(t, hadEntityCols, "no entity columns present => incomplete part")
}

// TestBuildEntityRebuildIndex_SkipsIndexOnlyEntity proves a measure whose entity
// tag is not column-backed (lives only in the index) is omitted from the rebuild
// index — it cannot be rebuilt from columns and must fall through to the skip path.
func TestBuildEntityRebuildIndex_SkipsIndexOnlyEntity(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		// entity references "ghost", which is not present in any tag family.
		"indexed_only": measureWithEntity([]*databasev1.TagSpec{strTagSpec("present")}, []string{"ghost"}),
		"column_ok":    measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id")}, []string{"entity_id"}),
	}
	idx := buildEntityRebuildIndex(schemas)

	var subjects []string
	for _, cands := range idx.bySignature {
		for i := range cands {
			subjects = append(subjects, cands[i].subject)
		}
	}
	assert.Equal(t, []string{"column_ok"}, subjects, "index-only-entity measure must be excluded")
}

// TestRebuildEntity_Allocs guards the per-series rebuild allocation budget: the
// reused Series/EntityValues buffers must keep the hot path from allocating
// proportional to the number of skipped series. The single small allocation is the
// returned entity copy handed to the caller.
func TestRebuildEntity_Allocs(t *testing.T) {
	schemas := map[string]*databasev1.Measure{
		"cpm": measureWithEntity([]*databasev1.TagSpec{strTagSpec("entity_id")}, []string{"entity_id"}),
	}
	r := newRebuildReplayer(schemas)
	cb := &dumpmeasure.ColumnarBlock{
		SeriesID: seriesIDFor(t, "cpm", stringTagValue("svc-A")),
		TagCols:  map[string][][]byte{"default.entity_id": strCol("svc-A")},
		TagTypes: map[string]pbv1.ValueType{"default.entity_id": pbv1.ValueTypeStr},
	}
	lookup := columnarBlockLookup(cb)

	avg := testing.AllocsPerRun(100, func() {
		_, _, _, ok := r.rebuildEntity(cb.SeriesID, lookup)
		if !ok {
			t.Fatal("rebuild must succeed")
		}
	})
	// One decode + the returned entity copy; assert it stays in single digits and
	// does not scale with candidate count.
	assert.LessOrEqualf(t, avg, float64(8), "rebuild allocated %.1f objects/run; expected a small constant", avg)
}
