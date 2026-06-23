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

package measure

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestClassifyIndexModeGroups(t *testing.T) {
	// Case 1: index-mode measures + only the internal _top_n_result -> index-mode group.
	isIndexMode, err := classifyGroup("sw_metadata", map[string]*measureSchemaInfo{
		"service_traffic":  {Name: "service_traffic", IndexMode: true},
		"endpoint_traffic": {Name: "endpoint_traffic", IndexMode: true},
		TopNSchemaName:     {Name: TopNSchemaName, IndexMode: false},
	})
	require.NoError(t, err)
	require.True(t, isIndexMode)

	// Case 2: index-mode + a real normal measure -> error naming the conflict.
	isIndexMode, err = classifyGroup("mixed", map[string]*measureSchemaInfo{
		"service_traffic":    {Name: "service_traffic", IndexMode: true},
		"service_cpm_minute": {Name: "service_cpm_minute", IndexMode: false},
		TopNSchemaName:       {Name: TopNSchemaName, IndexMode: false},
	})
	require.Error(t, err)
	require.False(t, isIndexMode)
	require.Contains(t, err.Error(), "service_cpm_minute")
	// The internal _top_n_result must never be listed among the conflicting
	// normal measures (it is auto-created for every group).
	require.Contains(t, err.Error(), "[service_cpm_minute]")

	// Case 3: pure normal group (including _top_n_result) -> not index-mode.
	isIndexMode, err = classifyGroup("sw_metricsMinute", map[string]*measureSchemaInfo{
		"a":            {Name: "a", IndexMode: false},
		"b":            {Name: "b", IndexMode: false},
		TopNSchemaName: {Name: TopNSchemaName, IndexMode: false},
	})
	require.NoError(t, err)
	require.False(t, isIndexMode)
}

func TestClassifyStoredField(t *testing.T) {
	const knownRuleID = uint32(201559343)
	ruleByID := map[uint32]indexRuleInfo{knownRuleID: {Analyzer: "", NoSort: true}}
	// The group's tag-name set includes a 4-CHARACTER tag name ("name") to prove
	// it is not misread as an IndexRuleID just because it is 4 bytes wide.
	tagNames := map[string]struct{}{"instance_traffic_name": {}, "name": {}}

	// A known tag name -> non-indexed TagName.
	k, indexed, missing := classifyStoredField("instance_traffic_name", tagNames, ruleByID)
	require.False(t, indexed)
	require.Equal(t, "instance_traffic_name", k.TagName)
	require.Zero(t, missing)

	// A 4-character tag name in the schema -> TagName, NOT an IndexRuleID.
	k, indexed, missing = classifyStoredField("name", tagNames, ruleByID)
	require.False(t, indexed, "a 4-char schema tag must not be treated as a rule id")
	require.Equal(t, "name", k.TagName)
	require.Zero(t, k.IndexRuleID)
	require.Zero(t, missing)

	// A 4-byte name matching a KNOWN rule -> indexed, no missing rule.
	known := string(convert.Uint32ToBytes(knownRuleID))
	k, indexed, missing = classifyStoredField(known, tagNames, ruleByID)
	require.True(t, indexed)
	require.Equal(t, knownRuleID, k.IndexRuleID)
	require.Zero(t, missing)

	// A 4-byte name that DECODES to a rule id absent from the catalog: kept indexed
	// (not demoted), and returned as missingRuleID so the caller warns.
	const goneRuleID = uint32(999999)
	gone := string(convert.Uint32ToBytes(goneRuleID))
	k, indexed, missing = classifyStoredField(gone, tagNames, ruleByID)
	require.True(t, indexed, "a field decoding to a rule id must stay indexed even if the rule is missing")
	require.Equal(t, goneRuleID, k.IndexRuleID)
	require.Equal(t, goneRuleID, missing, "missing rule id must be reported for the warning")
}

// indexModeDocSpec describes one production-equivalent index-mode document to
// seed into a source sidx for the read+rebuild tests.
type indexModeDocSpec struct {
	storedTags    map[string]string
	subject       string
	indexedValue  string
	entityTags    []string
	entityValues  []string
	timestamp     int64
	version       int64
	indexedRuleID uint32
	indexedNoSort bool
}

// buildIndexModeTestStore writes one document into a sidx at dir that mirrors
// what the production write path produces for an index-mode measure: stored
// regular tags + an indexed tag + index-only _im_name / _im_entity_tag_*.
func buildIndexModeTestStore(t *testing.T, dir string, spec indexModeDocSpec) {
	t.Helper()
	series := &pbv1.Series{Subject: spec.subject}
	for _, v := range spec.entityValues {
		series.EntityValues = append(series.EntityValues, strTagValue(v))
	}
	require.NoError(t, series.Marshal())

	var fields []index.Field
	// Stored-only regular tag(s) (handleIndexMode, non-indexed branch).
	for name, val := range spec.storedTags {
		f := index.NewBytesField(index.FieldKey{TagName: name}, convert.StringToBytes(val))
		f.Store = true
		f.Index = false
		fields = append(fields, f)
	}
	// One indexed stored tag (handleIndexMode, indexed branch).
	if spec.indexedRuleID != 0 {
		f := index.NewBytesField(index.FieldKey{IndexRuleID: spec.indexedRuleID}, convert.StringToBytes(spec.indexedValue))
		f.Store = true
		f.Index = true
		f.NoSort = spec.indexedNoSort
		fields = append(fields, f)
	}
	// Index-only entity-derived fields (appendEntityTagsToIndexFields).
	subj := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, spec.subject)
	subj.Index = true
	subj.NoSort = true
	fields = append(fields, subj)
	for i, tagName := range spec.entityTags {
		f := index.NewBytesField(index.FieldKey{TagName: index.IndexModeEntityTagPrefix + tagName},
			convert.StringToBytes(spec.entityValues[i]))
		f.Index = true
		f.NoSort = true
		fields = append(fields, f)
	}

	store, err := inverted.NewStore(inverted.StoreOpts{Path: dir, BatchWaitSec: 0})
	require.NoError(t, err)
	require.NoError(t, store.UpdateSeriesBatch(index.Batch{Documents: index.Documents{{
		Fields:       fields,
		EntityValues: series.Buffer,
		Timestamp:    spec.timestamp,
		DocID:        uint64(series.ID),
		Version:      spec.version,
	}}}))
	require.NoError(t, store.Close())
}

// fieldByMarshaledName returns the rebuilt field whose marshaled key matches.
func fieldByMarshaledName(d index.Document, name string) (index.Field, bool) {
	for i := range d.Fields {
		if d.Fields[i].Key.Marshal() == name {
			return d.Fields[i], true
		}
	}
	return index.Field{}, false
}

func TestReadAndRebuildIndexModeDoc(t *testing.T) {
	dir := t.TempDir()
	const ruleID = uint32(201559343)
	buildIndexModeTestStore(t, dir, indexModeDocSpec{
		subject:       "svc",
		entityTags:    []string{"id"},
		entityValues:  []string{"abc"},
		storedTags:    map[string]string{"properties": "v1"},
		indexedRuleID: ruleID,
		indexedValue:  "https://example.com/x",
		indexedNoSort: false,
		timestamp:     1000,
		version:       7,
	})

	schemasBySubject := map[string]*measureSchemaInfo{
		"svc": {
			Name:              "svc",
			EntityTagNames:    []string{"id"},
			TagType:           map[string]databasev1.TagType{"id": databasev1.TagType_TAG_TYPE_STRING},
			IndexedEntityTags: map[string]struct{}{},
		},
	}
	ruleByID := map[uint32]indexRuleInfo{ruleID: {Analyzer: index.AnalyzerURL, NoSort: false}}

	docs, err := readIndexModeDocs(context.Background(), dir, ruleByID, schemasBySubject)
	require.NoError(t, err)
	require.Len(t, docs, 1)
	d := docs[0]

	require.Equal(t, int64(1000), d.Timestamp)
	require.Equal(t, int64(7), d.Version)
	require.NotEmpty(t, d.EntityValues)

	// EntityValues must round-trip back to the original series.
	var series pbv1.Series
	require.NoError(t, series.Unmarshal(d.EntityValues))
	require.Equal(t, "svc", series.Subject)

	// Regular stored tag is recovered, not indexed.
	pf, ok := fieldByMarshaledName(d, "properties")
	require.True(t, ok, "stored tag 'properties' missing")
	require.False(t, pf.Index)
	require.True(t, pf.Store)
	require.Equal(t, "v1", string(pf.GetBytes()))

	// Indexed stored tag restores Analyzer/NoSort from the rule table.
	idxf, ok := fieldByMarshaledName(d, string(convert.Uint32ToBytes(ruleID)))
	require.True(t, ok, "indexed tag field missing")
	require.True(t, idxf.Index)
	require.True(t, idxf.Store)
	require.Equal(t, index.AnalyzerURL, idxf.Key.Analyzer)
	require.False(t, idxf.NoSort)

	// Index-only fields regenerated from _id.
	nameF, ok := fieldByMarshaledName(d, index.IndexModeName)
	require.True(t, ok, "_im_name not regenerated")
	require.True(t, nameF.Index)
	require.Equal(t, "svc", string(nameF.GetBytes()))

	entF, ok := fieldByMarshaledName(d, index.IndexModeEntityTagPrefix+"id")
	require.True(t, ok, "_im_entity_tag_id not regenerated")
	require.True(t, entF.Index)
	require.Equal(t, "abc", string(entF.GetBytes()))
}

// TestReadAndRebuildIndexModeDoc_SkipsIndexedEntityTag asserts that an entity
// tag already indexed by an index rule is NOT re-emitted as _im_entity_tag_*,
// replicating the production skip condition (write_standalone.go:417).
func TestReadAndRebuildIndexModeDoc_SkipsIndexedEntityTag(t *testing.T) {
	dir := t.TempDir()
	buildIndexModeTestStore(t, dir, indexModeDocSpec{
		subject:      "svc",
		entityTags:   []string{"id"},
		entityValues: []string{"abc"},
		storedTags:   map[string]string{"properties": "v1"},
		timestamp:    1000,
		version:      1,
	})

	schemasBySubject := map[string]*measureSchemaInfo{
		"svc": {
			Name:           "svc",
			EntityTagNames: []string{"id"},
			TagType:        map[string]databasev1.TagType{"id": databasev1.TagType_TAG_TYPE_STRING},
			// "id" is already indexed by an index rule -> must be skipped.
			IndexedEntityTags: map[string]struct{}{"id": {}},
		},
	}

	docs, err := readIndexModeDocs(context.Background(), dir, map[uint32]indexRuleInfo{}, schemasBySubject)
	require.NoError(t, err)
	require.Len(t, docs, 1)

	// _im_name is still regenerated.
	_, ok := fieldByMarshaledName(docs[0], index.IndexModeName)
	require.True(t, ok, "_im_name should always be regenerated")
	// _im_entity_tag_id must be absent because the tag is index-rule indexed.
	_, ok = fieldByMarshaledName(docs[0], index.IndexModeEntityTagPrefix+"id")
	require.False(t, ok, "_im_entity_tag_id must be skipped for index-rule-indexed entity tag")
}

func TestReadIndexModeDocs_EmptySidx(t *testing.T) {
	docs, err := readIndexModeDocs(context.Background(), t.TempDir(), map[uint32]indexRuleInfo{}, nil)
	require.NoError(t, err)
	require.Empty(t, docs)
}

// ── Phase 2: copy routing tests ──────────────────────────────────────────────.

// day20260621Nanos returns the local-time start-of-day nanos for 2026-06-21.
// Segment names are formatted in time.Local, so test timestamps are anchored to
// a local day boundary to stay aligned with formatDirectCopySegName.
func day20260621Nanos(t *testing.T) int64 {
	t.Helper()
	return time.Date(2026, time.June, 21, 0, 0, 0, 0, time.Local).UnixNano()
}

// seedSourceSeg writes one index-mode doc into "<root>/<segName>/sidx".
func seedSourceSeg(t *testing.T, root, segName string, spec indexModeDocSpec) {
	t.Helper()
	sidxDir := filepath.Join(root, segName, directCopySidxDirName)
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))
	buildIndexModeTestStore(t, sidxDir, spec)
}

// seedSourceSegDocs writes several docs into one source seg's sidx in a single
// committed batch, so a source segment can span more than one target seg (a
// "split" source) for the merge/eligibility tests.
func seedSourceSegDocs(t *testing.T, root, segName string, specs ...indexModeDocSpec) {
	t.Helper()
	sidxDir := filepath.Join(root, segName, directCopySidxDirName)
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))
	store, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	docs := make(index.Documents, 0, len(specs))
	for _, spec := range specs {
		series := &pbv1.Series{Subject: spec.subject}
		for _, v := range spec.entityValues {
			series.EntityValues = append(series.EntityValues, strTagValue(v))
		}
		require.NoError(t, series.Marshal())
		var fields []index.Field
		for name, val := range spec.storedTags {
			f := index.NewBytesField(index.FieldKey{TagName: name}, convert.StringToBytes(val))
			f.Store = true
			fields = append(fields, f)
		}
		subj := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, spec.subject)
		subj.Index = true
		subj.NoSort = true
		fields = append(fields, subj)
		for i, tagName := range spec.entityTags {
			f := index.NewBytesField(index.FieldKey{TagName: index.IndexModeEntityTagPrefix + tagName},
				convert.StringToBytes(spec.entityValues[i]))
			f.Index = true
			f.NoSort = true
			fields = append(fields, f)
		}
		docs = append(docs, index.Document{
			Fields: fields, EntityValues: series.Buffer,
			Timestamp: spec.timestamp, DocID: uint64(series.ID), Version: spec.version,
		})
	}
	require.NoError(t, store.UpdateSeriesBatch(index.Batch{Documents: docs}))
	require.NoError(t, store.Close())
}

func svcSchemas() map[string]*measureSchemaInfo {
	return map[string]*measureSchemaInfo{
		"svc": {
			Name:              "svc",
			EntityTagNames:    []string{"id"},
			TagType:           map[string]databasev1.TagType{"id": databasev1.TagType_TAG_TYPE_STRING},
			IndexedEntityTags: map[string]struct{}{},
		},
	}
}

// countSidxDocs opens a sidx read-only and returns the number of docs.
func countSidxDocs(t *testing.T, sidxDir string) int {
	t.Helper()
	docs, err := readIndexModeDocs(context.Background(), sidxDir, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	return len(docs)
}

func TestTargetIdxStore_GetSamePathSharesWriter(t *testing.T) {
	pool := newTargetIdxStore()
	path := filepath.Join(t.TempDir(), "sidx")

	s1, err := pool.get(path)
	require.NoError(t, err)
	s2, err := pool.get(path)
	require.NoError(t, err)
	require.Same(t, s1, s2, "same path must share one writer")

	require.NoError(t, pool.closeAll())
	require.Empty(t, pool.stores, "closeAll must clear the pool")
}

// readDocsByDir reads every dir's index-mode docs into the docsByDir map the
// slow path consumes, mirroring what copyIndexModeGroup's Pass 1 builds.
func readDocsByDir(t *testing.T, dirs []string) map[string][]index.Document {
	t.Helper()
	out := make(map[string][]index.Document, len(dirs))
	for _, dir := range dirs {
		docs, err := readIndexModeDocs(context.Background(), dir, map[uint32]indexRuleInfo{}, svcSchemas())
		require.NoError(t, err)
		out[dir] = docs
	}
	return out
}

func TestSlowPath_MergeKeepsMaxVersion(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(time.Hour)

	// Two source segs hold the SAME series (same subject+entity -> same DocID)
	// at different versions; both route to seg-20260621.
	seedSourceSeg(t, root, "seg-20260621a", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "old"}, timestamp: ts, version: 3,
	})
	seedSourceSeg(t, root, "seg-20260621b", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "new"}, timestamp: ts, version: 9,
	})

	stores := newTargetIdxStore()
	srcDirs := []string{
		filepath.Join(root, "seg-20260621a", directCopySidxDirName),
		filepath.Join(root, "seg-20260621b", directCopySidxDirName),
	}
	rows, err := copyIndexModeSlowDocs(context.Background(), srcDirs, readDocsByDir(t, srcDirs), dst, ir, stores)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows, "same series collapses to one row")
	require.NoError(t, stores.closeAll())

	targetSidx := filepath.Join(dst, "seg-20260621", directCopySidxDirName)
	docs, err := readIndexModeDocs(context.Background(), targetSidx, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Len(t, docs, 1)
	require.Equal(t, int64(9), docs[0].Version, "highest version must win")
	pf, ok := fieldByMarshaledName(docs[0], "properties")
	require.True(t, ok)
	require.Equal(t, "new", string(pf.GetBytes()))
	require.FileExists(t, filepath.Join(dst, "seg-20260621", storage.SegmentMetadataFilename))
}

func TestSlowPath_SplitRoutesByTimestamp(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.HOUR, Num: 1}
	base := day20260621Nanos(t)

	// One source seg holds two docs in different hours -> two target segs.
	src := filepath.Join(root, "seg-20260621", directCopySidxDirName)
	require.NoError(t, os.MkdirAll(src, storage.DirPerm))
	buildIndexModeTestStore(t, src, indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"h01"},
		storedTags: map[string]string{"properties": "a"}, timestamp: base + int64(time.Hour), version: 1,
	})
	buildIndexModeTestStore(t, src, indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"h05"},
		storedTags: map[string]string{"properties": "b"}, timestamp: base + int64(5*time.Hour), version: 1,
	})

	stores := newTargetIdxStore()
	srcDirs := []string{src}
	rows, err := copyIndexModeSlowDocs(context.Background(), srcDirs, readDocsByDir(t, srcDirs), dst, ir, stores)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows)
	require.NoError(t, stores.closeAll())

	require.Equal(t, 1, countSidxDocs(t, filepath.Join(dst, "seg-2026062101", directCopySidxDirName)))
	require.Equal(t, 1, countSidxDocs(t, filepath.Join(dst, "seg-2026062105", directCopySidxDirName)))
}

func TestCopyIndexModeGroup_FastPathByteCopy(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(2*time.Hour)

	src := filepath.Join(root, "seg-20260621", directCopySidxDirName)
	require.NoError(t, os.MkdirAll(src, storage.DirPerm))
	buildIndexModeTestStore(t, src, indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v1"}, timestamp: ts, version: 1,
	})

	res, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	require.Equal(t, 1, res.Segments)

	targetSidx := filepath.Join(dst, "seg-20260621", directCopySidxDirName)
	require.Equal(t, 1, countSidxDocs(t, targetSidx),
		"target sidx doc count must match source")
	require.FileExists(t, filepath.Join(dst, "seg-20260621", storage.SegmentMetadataFilename),
		"byte-copy must still write segment metadata")
}

// TestCopyIndexModeGroup_MergeAcrossSourcesKeepsMaxVersion drives the top-level
// copyIndexModeGroup (the production path) with two source sidx dirs holding the
// SAME series at different versions, both aligning to the same target seg. Since
// neither source exclusively owns the target seg, both must go through a single
// slow-path merge and the highest version must win. This reproduces the bug
// where copyIndexModeGroup looped one source per copyIndexModeSlow call, leaving
// cross-source dedup to bluge's last-writer-wins.
func TestCopyIndexModeGroup_MergeAcrossSourcesKeepsMaxVersion(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(time.Hour)

	// Two distinct source roots (e.g. two node replicas), same series, both
	// aligning to seg-20260621 at different versions.
	seedSourceSeg(t, rootA, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "old"}, timestamp: ts, version: 3,
	})
	seedSourceSeg(t, rootB, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "new"}, timestamp: ts, version: 9,
	})

	res, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{rootA, rootB},
		Interval:        ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Equal(t, 1, res.Segments)
	require.Equal(t, int64(1), res.Rows, "same series across sources collapses to one row")

	targetSidx := filepath.Join(dst, "seg-20260621", directCopySidxDirName)
	docs, err := readIndexModeDocs(context.Background(), targetSidx, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Len(t, docs, 1)
	require.Equal(t, int64(9), docs[0].Version, "highest version must win across sources")
	pf, ok := fieldByMarshaledName(docs[0], "properties")
	require.True(t, ok)
	require.Equal(t, "new", string(pf.GetBytes()), "the max-version doc's value must survive")
}

// TestCopyIndexModeGroup_ByteCopyTargetAlsoReceivesSplitSource_KeepsMaxVersion
// reproduces a real-data bug: a single-target source (byte-copy candidate) and a
// SPLIT source both route the same series into the same target seg. The split
// source was not counted as an owner of that target seg, so the single-target
// source byte-copied it and the split source's slow-path rebuild then upserted
// (last-writer-wins) — dropping the higher version. The target must keep the
// max version across BOTH sources.
func TestCopyIndexModeGroup_ByteCopyTargetAlsoReceivesSplitSource_KeepsMaxVersion(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.HOUR, Num: 1}
	base := day20260621Nanos(t)
	h1 := base + int64(time.Hour)
	h2 := base + int64(2*time.Hour)

	// Single-target source: all docs in hour-01. Higher version of "shared".
	seedSourceSeg(t, root, "seg-2026062101", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"shared"},
		storedTags: map[string]string{"properties": "new"}, timestamp: h1, version: 100,
	})
	// Split source: spans hour-01 (lower version of "shared") and hour-02 (filler),
	// so it routes a doc into hour-01 too — making hour-01 NOT exclusively owned.
	seedSourceSegDocs(t, root, "seg-2026062102",
		indexModeDocSpec{
			subject: "svc", entityTags: []string{"id"}, entityValues: []string{"shared"},
			storedTags: map[string]string{"properties": "old"}, timestamp: h1, version: 50,
		},
		indexModeDocSpec{
			subject: "svc", entityTags: []string{"id"}, entityValues: []string{"filler"},
			storedTags: map[string]string{"properties": "f"}, timestamp: h2, version: 1,
		})

	res, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Zero(t, res.Bytes, "no source exclusively owns a target seg, so nothing byte-copies")

	h1Sidx := filepath.Join(dst, "seg-2026062101", directCopySidxDirName)
	docs, err := readIndexModeDocs(context.Background(), h1Sidx, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Len(t, docs, 1, "hour-01 holds exactly the shared series")
	require.Equal(t, int64(100), docs[0].Version, "max version must survive across byte-copy + split source")
	pf, ok := fieldByMarshaledName(docs[0], "properties")
	require.True(t, ok)
	require.Equal(t, "new", string(pf.GetBytes()), "the max-version doc's value must survive")
	// The split source's filler doc must still land in hour-02 (not dropped).
	require.Equal(t, 1, countSidxDocs(t, filepath.Join(dst, "seg-2026062102", directCopySidxDirName)),
		"the split source's hour-02 doc must be preserved")
}

// TestCopyIndexModeGroup_ExclusiveTargetsByteCopy drives copyIndexModeGroup with
// two sources that each exclusively own a distinct target seg, so both must take
// the byte-copy fast path (no slow-path merge). res.Bytes>0 confirms byte-copy ran.
func TestCopyIndexModeGroup_ExclusiveTargetsByteCopy(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.HOUR, Num: 1}
	base := day20260621Nanos(t)

	// Two source segs, each landing wholly in a distinct hour-aligned target seg.
	seedSourceSeg(t, root, "seg-2026062101", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"h01"},
		storedTags: map[string]string{"properties": "a"}, timestamp: base + int64(time.Hour), version: 1,
	})
	seedSourceSeg(t, root, "seg-2026062105", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"h05"},
		storedTags: map[string]string{"properties": "b"}, timestamp: base + int64(5*time.Hour), version: 1,
	})

	res, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Rows)
	require.Equal(t, 2, res.Segments)
	require.Positive(t, res.Bytes, "exclusive single-target sources must byte-copy")

	require.Equal(t, 1, countSidxDocs(t, filepath.Join(dst, "seg-2026062101", directCopySidxDirName)))
	require.Equal(t, 1, countSidxDocs(t, filepath.Join(dst, "seg-2026062105", directCopySidxDirName)))
}

// TestBuildSegAligns_ByteCopyEligibility locks the alignment classification:
// a target is "aligned" (a byte-copy candidate) ONLY when its sole feeding source
// segment feeds no other target and no docs collapsed. A split source disqualifies
// every target it touches — even one it solely feeds — and a merge target (>1
// source) is never aligned.
func TestBuildSegAligns_ByteCopyEligibility(t *testing.T) {
	src := &segDigestResult{
		docsPerSeg: map[string]uint64{"seg-A": 10, "seg-B": 5, "seg-C": 5, "seg-D": 8},
		srcSegsPerTarget: map[string]map[string]struct{}{
			"seg-A": {"srcA": {}},             // exclusive 1:1
			"seg-B": {"srcS": {}},             // srcS also feeds seg-C => split
			"seg-C": {"srcS": {}},             // split source
			"seg-D": {"srcX": {}, "srcY": {}}, // merge: two sources
		},
	}
	tgt := &segDigestResult{docsPerSeg: map[string]uint64{"seg-A": 10, "seg-B": 5, "seg-C": 5, "seg-D": 8}}

	byName := map[string]IndexModeSegAlign{}
	for _, a := range buildSegAligns(src, tgt) {
		byName[a.Segment] = a
	}
	require.True(t, byName["seg-A"].Aligned, "exclusive 1:1 source is byte-copy aligned")
	require.False(t, byName["seg-B"].Aligned, "a target fed by a split source is not aligned")
	require.False(t, byName["seg-C"].Aligned, "the split source's other target is not aligned")
	require.False(t, byName["seg-D"].Aligned, "a merge target (>1 source) is not aligned")

	// A collapse (src docs > tgt docs) also disqualifies an otherwise-1:1 target.
	src2 := &segDigestResult{
		docsPerSeg:       map[string]uint64{"seg-A": 10},
		srcSegsPerTarget: map[string]map[string]struct{}{"seg-A": {"srcA": {}}},
	}
	tgt2 := &segDigestResult{docsPerSeg: map[string]uint64{"seg-A": 9}}
	require.False(t, buildSegAligns(src2, tgt2)[0].Aligned, "a collapsing target is not 1:1 aligned")
}

// TestCopyIndexModeGroup_TimestampZeroSurfacesError verifies that a Timestamp==0
// doc (no decodable _timestamp field) — which never occurs for valid index-mode
// data, since the write path always stamps a checked non-zero timestamp — fails
// the copy with an error naming the offending series, instead of being silently
// routed to a guessed segment boundary.
func TestCopyIndexModeGroup_TimestampZeroSurfacesError(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.HOUR, Num: 1}

	seedSourceSeg(t, root, "seg-2026062103", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"timeless"},
		storedTags: map[string]string{"properties": "v"}, timestamp: 0, version: 1,
	})

	_, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.Error(t, err, "a ts==0 doc must surface an error, not be silently routed")
	require.Contains(t, err.Error(), "ts==0")
	require.NoDirExists(t, dst, "no target segment may be written when the source has a ts==0 doc")
}

// ── Phase 3 / 4: value digest, verify, analyze tests ─────────────────────────.

// readOneDoc reads exactly one rebuilt doc from a sidx dir, failing otherwise.
func readOneDoc(t *testing.T, sidxDir string, schemas map[string]*measureSchemaInfo) index.Document {
	t.Helper()
	docs, err := readIndexModeDocs(context.Background(), sidxDir, map[uint32]indexRuleInfo{}, schemas)
	require.NoError(t, err)
	require.Len(t, docs, 1)
	return docs[0]
}

func TestDocValueDigest_StableAndOrderIndependent(t *testing.T) {
	dirA := filepath.Join(t.TempDir(), "sidx")
	dirB := filepath.Join(t.TempDir(), "sidx")
	require.NoError(t, os.MkdirAll(dirA, storage.DirPerm))
	require.NoError(t, os.MkdirAll(dirB, storage.DirPerm))
	spec := indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v1", "kind": "k1"}, timestamp: 1000, version: 5,
	}
	buildIndexModeTestStore(t, dirA, spec)
	buildIndexModeTestStore(t, dirB, spec)

	da := readOneDoc(t, dirA, svcSchemas())
	db := readOneDoc(t, dirB, svcSchemas())
	require.Equal(t, docValueDigest(da), docValueDigest(db),
		"identical content must hash equal regardless of field assembly order")
}

func TestDocValueDigest_ChangedTagValueDiffers(t *testing.T) {
	dirA := filepath.Join(t.TempDir(), "sidx")
	dirB := filepath.Join(t.TempDir(), "sidx")
	require.NoError(t, os.MkdirAll(dirA, storage.DirPerm))
	require.NoError(t, os.MkdirAll(dirB, storage.DirPerm))
	buildIndexModeTestStore(t, dirA, indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "old"}, timestamp: 1000, version: 5,
	})
	buildIndexModeTestStore(t, dirB, indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "NEW"}, timestamp: 1000, version: 5,
	})
	require.NotEqual(t, docValueDigest(readOneDoc(t, dirA, svcSchemas())),
		docValueDigest(readOneDoc(t, dirB, svcSchemas())),
		"a changed tag value must change the digest")
}

func TestDocValueDigest_MissingEntityFieldDiffers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	require.NoError(t, os.MkdirAll(dir, storage.DirPerm))
	buildIndexModeTestStore(t, dir, indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v1"}, timestamp: 1000, version: 5,
	})
	full := readOneDoc(t, dir, svcSchemas())

	// Drop the regenerated _im_entity_tag_id field to simulate a rebuild that
	// lost an index-only entity value: the digest MUST differ from the intact
	// doc (id present, value missing is still caught).
	stripped := index.Document{
		EntityValues: full.EntityValues,
		Timestamp:    full.Timestamp,
		DocID:        full.DocID,
		Version:      full.Version,
	}
	for i := range full.Fields {
		if full.Fields[i].Key.Marshal() == index.IndexModeEntityTagPrefix+"id" {
			continue
		}
		stripped.Fields = append(stripped.Fields, full.Fields[i])
	}
	require.NotEqual(t, docValueDigest(full), docValueDigest(stripped),
		"dropping a regenerated entity field must change the digest")
}

// runCopyVerify copies srcRoots into a fresh target group root and returns the
// resulting verify report. Used by the aligned-equal and merge tests so the
// target is produced by the real copy path.
func runCopyVerify(t *testing.T, srcRoots []string, ir storage.IntervalRule) EntryGroupReport {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "tgt")
	in := migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        srcRoots,
		Interval:        ir,
	}
	_, err := copyIndexModeGroup(context.Background(), in, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	report, err := verifyIndexModeGroup(context.Background(), in, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	return report
}

func TestVerifyIndexModeGroup_AlignedEqual(t *testing.T) {
	root := t.TempDir()
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(2*time.Hour)
	seedSourceSeg(t, root, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v1"}, timestamp: ts, version: 1,
	})
	seedSourceSeg(t, root, "seg-20260621", indexModeDocSpec{ // overwrites? no: same seg dir, second doc series id2
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"def"},
		storedTags: map[string]string{"properties": "v2"}, timestamp: ts, version: 1,
	})

	report := runCopyVerify(t, []string{root}, ir)
	require.True(t, report.IndexMode)
	require.Equal(t, report.SrcDistinctIDs, report.TgtDistinctIDs, "aligned copy keeps distinct doc-id equal")
	require.Equal(t, report.ExpectDistinctIDs, report.TgtDistinctIDs)
	require.Empty(t, report.ValueMismatches, "aligned copy must reconcile all value digests")
}

func TestVerifyIndexModeGroup_MergeDistinctIDUnion(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(time.Hour)

	// Same series in two source roots at different versions, both -> seg-20260621.
	// Target upsert collapses them to ONE doc; expected distinct doc-id == union(1).
	seedSourceSeg(t, rootA, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "old"}, timestamp: ts, version: 3,
	})
	seedSourceSeg(t, rootB, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "new"}, timestamp: ts, version: 9,
	})

	report := runCopyVerify(t, []string{rootA, rootB}, ir)
	require.Equal(t, uint64(2), report.SrcDocs, "two source docs before merge")
	require.Equal(t, uint64(1), report.ExpectDistinctIDs, "union of source doc-ids is 1")
	require.Equal(t, uint64(1), report.TgtDistinctIDs, "merge collapses to one upserted doc")
	require.Empty(t, report.ValueMismatches,
		"source reduced to max version must match the surviving target doc")
}

// TestVerifyIndexModeGroup_DetectsMissingAndExtra exercises the two reconcile
// branches the (segment, series) keying relies on: a series in source but not
// target is "missing"; a series in target but not source is "extra".
func TestVerifyIndexModeGroup_DetectsMissingAndExtra(t *testing.T) {
	srcRoot := t.TempDir()
	tgtGroup := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(time.Hour)

	// Source seg-20260621 holds series "aaa" (routes to target seg-20260621).
	seedSourceSeg(t, srcRoot, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"aaa"},
		storedTags: map[string]string{"properties": "v"}, timestamp: ts, version: 1,
	})
	// Target seg-20260621 holds a DIFFERENT series "bbb": "aaa" is missing from
	// the target and "bbb" is extra (not present in source).
	seedSourceSeg(t, tgtGroup, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"bbb"},
		storedTags: map[string]string{"properties": "v"}, timestamp: ts, version: 1,
	})

	report, err := verifyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		Group: "sw_metadata", SrcRoots: []string{srcRoot}, TargetGroupRoot: tgtGroup, Interval: ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	kinds := map[string]int{}
	for _, m := range report.ValueMismatches {
		kinds[m.Kind]++
		require.Equal(t, "seg-20260621", m.Segment)
	}
	require.Equal(t, 1, kinds["missing"], "source series aaa is missing from target")
	require.Equal(t, 1, kinds["extra"], "target series bbb is extra")
}

func TestVerifyIndexModeGroup_DetectsTamper(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	ts := day20260621Nanos(t) + int64(2*time.Hour)

	seedSourceSeg(t, root, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "correct"}, timestamp: ts, version: 1,
	})
	// Hand-craft a TAMPERED target: same series/timestamp/version but a changed
	// stored tag value. verify must flag it "tampered".
	seedSourceSeg(t, dst, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "TAMPERED"}, timestamp: ts, version: 1,
	})

	report, err := verifyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		Group: "sw_metadata", TargetGroupRoot: dst, SrcRoots: []string{root}, Interval: ir,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err)
	require.Len(t, report.ValueMismatches, 1)
	require.Equal(t, "tampered", report.ValueMismatches[0].Kind)
}

func TestVerifyIndexModeGroup_DetectsMissingEntityField(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	ts := day20260621Nanos(t) + int64(2*time.Hour)

	// Source: schema declares entity tag "id", so the source doc regenerates
	// _im_entity_tag_id from its entity value.
	seedSourceSeg(t, root, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v1"}, timestamp: ts, version: 1,
	})
	// Target: SAME doc on disk, but verified with a schema that has NO entity
	// tags, so the rebuild can't regenerate _im_entity_tag_id — the entity value
	// is effectively missing on the target side. The digest diverges and verify
	// flags "tampered" (id present, value missing).
	seedSourceSeg(t, dst, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v1"}, timestamp: ts, version: 1,
	})
	srcSchemas := svcSchemas()
	tgtSchemas := map[string]*measureSchemaInfo{
		"svc": {
			Name:              "svc",
			EntityTagNames:    nil, // no entity tags -> _im_entity_tag_id not regenerated
			TagType:           map[string]databasev1.TagType{},
			IndexedEntityTags: map[string]struct{}{},
		},
	}
	// Read both sides with their own schemas to simulate the lost entity field,
	// then diff the digests directly (verifyIndexModeGroup uses one schema map,
	// so we exercise the digest layer it relies on).
	srcDoc := readOneDoc(t, filepath.Join(root, "seg-20260621", directCopySidxDirName), srcSchemas)
	tgtDoc := readOneDoc(t, filepath.Join(dst, "seg-20260621", directCopySidxDirName), tgtSchemas)
	require.NotEqual(t, docValueDigest(srcDoc), docValueDigest(tgtDoc),
		"a target missing the regenerated entity field must not reconcile")
	srcMap := map[imSegKey]segSurvivor{{seg: "seg-20260621", sid: srcDoc.DocID}: {version: srcDoc.Version, digest: docValueDigest(srcDoc)}}
	tgtMap := map[imSegKey]segSurvivor{{seg: "seg-20260621", sid: tgtDoc.DocID}: {version: tgtDoc.Version, digest: docValueDigest(tgtDoc)}}
	mismatches := diffValueDigests(srcMap, tgtMap)
	require.Len(t, mismatches, 1)
	require.Equal(t, "tampered", mismatches[0].Kind)
}

func TestAnalyzeIndexModeGroup_ValueConflict(t *testing.T) {
	root := t.TempDir()
	ts := day20260621Nanos(t) + int64(2*time.Hour)

	// Two source segs hold the SAME series at the SAME timestamp but DIFFERENT
	// stored-tag values (and different versions). That is a value conflict: same
	// (sid, ts), diverging value digests.
	seedSourceSeg(t, root, "seg-20260621a", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "valA"}, timestamp: ts, version: 1,
	})
	seedSourceSeg(t, root, "seg-20260621b", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "valB"}, timestamp: ts, version: 2,
	})

	srcSidxDirs, err := collectSourceSidxDirs([]string{root})
	require.NoError(t, err)
	res, err := analyzeIndexModeGroup(context.Background(), srcSidxDirs, map[uint32]indexRuleInfo{}, svcSchemas(), 10)
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.TotalRows)
	require.Equal(t, uint64(1), res.UniqueKeys, "same (sid, ts) collapses to one key")
	require.Equal(t, uint64(1), res.KeysWithDuplicates)
	require.Equal(t, uint64(1), res.ValueConflictKeys, "differing values on the same key is a conflict")
	require.Len(t, res.ValueConflicts, 1)
	require.NotEqual(t, res.ValueConflicts[0].Digests[0], res.ValueConflicts[0].Digests[1])
}

func TestAnalyzeIndexModeGroup_NoConflictWhenIdentical(t *testing.T) {
	root := t.TempDir()
	ts := day20260621Nanos(t) + int64(2*time.Hour)
	// Same series, same ts, IDENTICAL value but different versions -> a version
	// duplicate but NOT a value conflict.
	seedSourceSeg(t, root, "seg-20260621a", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "same"}, timestamp: ts, version: 1,
	})
	seedSourceSeg(t, root, "seg-20260621b", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "same"}, timestamp: ts, version: 2,
	})
	srcSidxDirs, err := collectSourceSidxDirs([]string{root})
	require.NoError(t, err)
	res, err := analyzeIndexModeGroup(context.Background(), srcSidxDirs, map[uint32]indexRuleInfo{}, svcSchemas(), 10)
	require.NoError(t, err)
	require.Equal(t, uint64(1), res.KeysWithDuplicates)
	require.Zero(t, res.ValueConflictKeys, "identical values across versions are not a conflict")
}

func TestCopyIndexModeGroup_RejectsShardParts(t *testing.T) {
	root := t.TempDir()
	// A shard part on disk means a non-_top_n_result normal measure has data.
	partDir := filepath.Join(root, "seg-20260621", "shard-0", "0000000000000001")
	require.NoError(t, os.MkdirAll(partDir, storage.DirPerm))

	_, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		Group:           "sw_metadata",
		TargetGroupRoot: filepath.Join(t.TempDir(), "tgt"),
		SrcRoots:        []string{root},
		Interval:        storage.IntervalRule{Unit: storage.DAY, Num: 1},
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard part")
}

func TestCopyIndexModeGroup_EmptyShardDirAllowed(t *testing.T) {
	root := t.TempDir()
	// The segment controller can create shard-0 eagerly (or a dataless
	// _top_n_result leaves one behind) with NO part inside; that is not normal
	// measure data and must not block an index-mode copy.
	require.NoError(t, os.MkdirAll(filepath.Join(root, "seg-20260621", "shard-0"), storage.DirPerm))
	seedSourceSeg(t, root, "seg-20260621", indexModeDocSpec{
		subject: "svc", entityTags: []string{"id"}, entityValues: []string{"abc"},
		storedTags: map[string]string{"properties": "v"}, timestamp: time.Now().UnixNano(), version: 1,
	})

	_, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		Group:           "sw_metadata",
		TargetGroupRoot: filepath.Join(t.TempDir(), "tgt"),
		SrcRoots:        []string{root},
		Interval:        storage.IntervalRule{Unit: storage.DAY, Num: 1},
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	require.NoError(t, err, "an empty shard-* dir (no part) must not trip the guard")
}

// ── Phase 6: end-to-end (index rules + production write path + searchability) ──.
//
// These tests build a realistic index-mode measure schema that carries genuine
// index rules — including a "url"-analyzer rule and a noSort:false rule — write
// data points through the PRODUCTION field-construction path
// (handleIndexMode + appendEntityTagsToIndexFields, the exact functions
// processDataPoint calls, write_standalone.go:86-87), run the migration, and
// then prove that after migration every data value is preserved AND every
// index-only / analyzed / sorted field is still searchable on the target sidx.

const (
	e2eMeasureName = "service_traffic_minute"
	// e2eRuleIDShortName binds the default-flavored rule (empty analyzer,
	// noSort:true) and e2eRuleIDEndpointURL binds the url-analyzer, noSort:false
	// rule. The numeric ids are arbitrary but distinct.
	e2eRuleIDShortName   = uint32(1001)
	e2eRuleIDEndpointURL = uint32(1002)
)

// e2eIndexModeSchema returns a production-shaped index-mode measure plus its
// index rules. The searchable tag family carries:
//   - id           : entity tag (STRING), no index rule -> regenerated as
//     _im_entity_tag_id (index-only)
//   - short_name   : default rule (empty analyzer, noSort:true)
//   - endpoint_url : url-analyzer rule, noSort:false
//   - properties   : NO index rule -> stored-only tag
func e2eIndexModeSchema() (*databasev1.Measure, []*databasev1.IndexRule) {
	schema := &databasev1.Measure{
		Metadata:  &commonv1.Metadata{Name: e2eMeasureName, Group: "sw_metadata"},
		IndexMode: true,
		Entity:    &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "searchable", Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "short_name", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "endpoint_url", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "properties", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
	}
	rules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Id: e2eRuleIDShortName, Name: "short_name", Group: "sw_metadata"},
			Tags:     []string{"short_name"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Analyzer: index.AnalyzerUnspecified,
			NoSort:   true,
		},
		{
			Metadata: &commonv1.Metadata{Id: e2eRuleIDEndpointURL, Name: "endpoint_url", Group: "sw_metadata"},
			Tags:     []string{"endpoint_url"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Analyzer: index.AnalyzerURL,
			NoSort:   false,
		},
	}
	return schema, rules
}

// e2eSchemasBySubject is the migration-side lookup view of the e2e schema,
// mirroring what loadMeasureSchemas would produce from the backup catalog. It
// also injects an empty _top_n_result to mimic the real per-group internal
// measure (classifyGroup / copy ignore it, but its presence keeps the fixture
// faithful).
func e2eSchemasBySubject() map[string]*measureSchemaInfo {
	return map[string]*measureSchemaInfo{
		e2eMeasureName: {
			Name:           e2eMeasureName,
			EntityTagNames: []string{"id"},
			TagType: map[string]databasev1.TagType{
				"id":           databasev1.TagType_TAG_TYPE_STRING,
				"short_name":   databasev1.TagType_TAG_TYPE_STRING,
				"endpoint_url": databasev1.TagType_TAG_TYPE_STRING,
				"properties":   databasev1.TagType_TAG_TYPE_STRING,
			},
			// short_name + endpoint_url are index-rule indexed; id is an entity
			// tag with no rule -> it must regenerate _im_entity_tag_id.
			IndexedEntityTags: map[string]struct{}{},
		},
		TopNSchemaName: {Name: TopNSchemaName, IndexMode: false},
	}
}

// e2eRuleByID is the flat IndexRuleID -> {Analyzer, NoSort} table the slow-path
// rebuild needs to restore analyzer / sort flags FieldKey.Marshal drops.
func e2eRuleByID() map[uint32]indexRuleInfo {
	return map[uint32]indexRuleInfo{
		e2eRuleIDShortName:   {Analyzer: index.AnalyzerUnspecified, NoSort: true},
		e2eRuleIDEndpointURL: {Analyzer: index.AnalyzerURL, NoSort: false},
	}
}

// e2eDataPoint is one logical index-mode row to write through the production
// field-construction path.
type e2eDataPoint struct {
	id          string
	shortName   string
	endpointURL string
	properties  string
	timestamp   int64
	version     int64
}

// newE2EMeasure builds a minimal but real *measure for the e2e schema so the
// production appendEntityTagsToIndexFields (which needs stm.indexSchema +
// stm.schema) can run unchanged. OnIndexUpdate populates indexRuleLocators +
// indexTagMap exactly as the runtime does on schema load.
func newE2EMeasure(t *testing.T, schema *databasev1.Measure, rules []*databasev1.IndexRule) *measure {
	t.Helper()
	m := &measure{schema: schema}
	m.OnIndexUpdate(rules)
	return m
}

// e2eDocFor builds one index.Document via the PRODUCTION field-construction
// functions handleIndexMode + appendEntityTagsToIndexFields — the same two calls
// processDataPoint makes for an index-mode measure (write_standalone.go:86-87).
// The WriteRequest carries tags positionally (spec=nil), so the schema's
// tag-family order drives field assembly, just like the live write path.
func e2eDocFor(t *testing.T, m *measure, dp e2eDataPoint) index.Document {
	t.Helper()
	req := &measurev1.WriteRequest{
		Metadata: m.schema.Metadata,
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Unix(0, dp.timestamp)),
			Version:   dp.version,
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{
					strTagValue(dp.id),
					strTagValue(dp.shortName),
					strTagValue(dp.endpointURL),
					strTagValue(dp.properties),
				},
			}},
		},
	}
	series := &pbv1.Series{Subject: m.schema.Metadata.Name, EntityValues: []*modelv1.TagValue{strTagValue(dp.id)}}
	require.NoError(t, series.Marshal())

	is := m.indexSchema.Load().(indexSchema)
	fields := handleIndexMode(m.schema, req, is.indexRuleLocators, nil)
	fields = appendEntityTagsToIndexFields(fields, m, series)
	return index.Document{
		DocID:        uint64(series.ID),
		EntityValues: series.Buffer,
		Fields:       fields,
		Version:      dp.version,
		Timestamp:    dp.timestamp,
	}
}

// writeE2ESourceSeg writes the given data points into "<root>/<segName>/sidx"
// through a real inverted.Store, exactly as the production flusher does in
// writeCallback.Rev via segment.IndexDB().Update(indexModeDocs) (the Update path
// resolves to UpdateSeriesBatch on the underlying store). Closing the store
// commits the bluge snapshot so the migration can read it back.
func writeE2ESourceSeg(t *testing.T, m *measure, root, segName string, dps ...e2eDataPoint) {
	t.Helper()
	sidxDir := filepath.Join(root, segName, directCopySidxDirName)
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))
	store, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	batch := index.Batch{Documents: make(index.Documents, 0, len(dps))}
	for _, dp := range dps {
		batch.Documents = append(batch.Documents, e2eDocFor(t, m, dp))
	}
	require.NoError(t, store.UpdateSeriesBatch(batch))
	require.NoError(t, store.Close())
}

// countFieldMatches opens a sidx read-only with the raw bluge reader and counts
// how many committed documents match a single-term query on the given field.
// This is exactly how the runtime's index-mode query resolves a tag condition
// (a term query on the marshaled FieldKey), so a non-zero count proves the field
// is genuinely searchable — index-only fields included — after migration.
func countFieldMatches(t *testing.T, sidxDir, field, term string) int {
	t.Helper()
	r, err := bluge.OpenReader(bluge.DefaultConfig(sidxDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	q := bluge.NewTermQuery(term)
	q.SetField(field)
	dmi, err := r.Search(context.Background(), bluge.NewAllMatches(q))
	require.NoError(t, err)
	n := 0
	for {
		match, nextErr := dmi.Next()
		require.NoError(t, nextErr)
		if match == nil {
			break
		}
		n++
	}
	return n
}

// e2eSampleDataPoints returns a fixed set of index-mode rows spanning two hours,
// each entity carrying a distinct short_name + endpoint_url so per-field
// searchability is unambiguous.
func e2eSampleDataPoints(base int64) []e2eDataPoint {
	hour := int64(time.Hour)
	return []e2eDataPoint{
		{id: "svc-a", shortName: "alpha", endpointURL: "https://example.com/api/login", properties: "p-a", timestamp: base + hour, version: 1},
		{id: "svc-b", shortName: "beta", endpointURL: "https://example.com/api/logout", properties: "p-b", timestamp: base + hour + int64(time.Minute), version: 1},
		{id: "svc-c", shortName: "gamma", endpointURL: "https://shop.example.org/cart", properties: "p-c", timestamp: base + 5*hour, version: 1},
	}
}

// assertSourceSearchable sanity-checks that the SOURCE sidx already answers the
// per-field searchability queries, so a later target failure is unambiguously a
// migration regression and not a bad fixture.
func assertSourceSearchable(t *testing.T, sidxDir string) {
	t.Helper()
	// _im_name (measure subject) is index-only.
	require.Equal(t, 3, countFieldMatches(t, sidxDir, index.IndexModeName, e2eMeasureName),
		"source: all 3 docs must match by measure name (_im_name)")
	// _im_entity_tag_id is index-only, regenerated from the series _id.
	require.Equal(t, 1, countFieldMatches(t, sidxDir, index.IndexModeEntityTagPrefix+"id", "svc-a"),
		"source: entity-tag search must hit exactly one doc")
	// url-analyzer tag: "com" is a token only of the two https://example.com urls
	// (the third doc's host is shop.example.org -> token "org", not "com").
	require.Equal(t, 2, countFieldMatches(t, sidxDir, string(convert.Uint32ToBytes(e2eRuleIDEndpointURL)), "com"),
		"source: url-analyzer tokenizes the host -> 2 example.com docs")
	// default (noSort:true) keyword rule on short_name.
	require.Equal(t, 1, countFieldMatches(t, sidxDir, string(convert.Uint32ToBytes(e2eRuleIDShortName)), "alpha"),
		"source: keyword short_name search must hit exactly one doc")
}

func TestIndexModeMigration_AlignedByteCopy(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	schema, rules := e2eIndexModeSchema()
	m := newE2EMeasure(t, schema, rules)
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	base := day20260621Nanos(t)

	// All three docs fall in the same DAY bucket -> source seg interval ==
	// target interval -> byte-copy fast path.
	writeE2ESourceSeg(t, m, root, "seg-20260621", e2eSampleDataPoints(base)...)
	srcSidx := filepath.Join(root, "seg-20260621", directCopySidxDirName)
	assertSourceSearchable(t, srcSidx)

	in := migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}
	res, err := copyIndexModeGroup(context.Background(), in, e2eRuleByID(), e2eSchemasBySubject())
	require.NoError(t, err)
	require.Equal(t, int64(3), res.Rows)
	require.Equal(t, 1, res.Segments)
	require.Positive(t, res.Bytes, "aligned single-target source must take the byte-copy fast path")

	tgtSidx := filepath.Join(dst, "seg-20260621", directCopySidxDirName)
	require.Equal(t, 3, countSidxDocsWith(t, tgtSidx, e2eRuleByID(), e2eSchemasBySubject()),
		"target sidx doc count must match source")

	// Value reconciliation: every (series, timestamp) digest matches the source.
	report, err := verifyIndexModeGroup(context.Background(), in, e2eRuleByID(), e2eSchemasBySubject())
	require.NoError(t, err)
	require.Empty(t, report.ValueMismatches, "byte-copy must preserve every data value")
	require.Equal(t, uint64(3), report.SrcDistinctIDs)
	require.Equal(t, report.SrcDistinctIDs, report.TgtDistinctIDs)

	// Searchability preserved on the target (byte-copy keeps all index flags).
	assertSourceSearchable(t, tgtSidx)
}

func TestIndexModeMigration_SplitAndMergeRebuild(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	schema, rules := e2eIndexModeSchema()
	m := newE2EMeasure(t, schema, rules)
	// Source data lived on a DAY grid; target is HOUR. Two source segs each map
	// wholly into hour bucket 01 (so they BOTH route through the slow-path merge
	// for that seg — the supported max-version dedup path), while a third source
	// seg lands wholly in hour 05. This exercises split (01 vs 05) + merge
	// (two sources into 01 keeping max version).
	ir := storage.IntervalRule{Unit: storage.HOUR, Num: 1}
	base := day20260621Nanos(t)
	hour := int64(time.Hour)

	// Source seg #1: svc-a@v1 + svc-b, both in hour 01.
	writeE2ESourceSeg(
		t, m, root, "seg-2026062101a",
		e2eDataPoint{id: "svc-a", shortName: "alpha", endpointURL: "https://example.com/api/login", properties: "p-a", timestamp: base + hour, version: 1},
		e2eDataPoint{id: "svc-b", shortName: "beta", endpointURL: "https://example.com/api/logout", properties: "p-b", timestamp: base + hour + int64(time.Minute), version: 1},
	)
	// Source seg #2: svc-a re-emitted at a HIGHER version (changed stored
	// "properties"), also in hour 01 -> merges with seg #1's svc-a, max wins.
	writeE2ESourceSeg(
		t, m, root, "seg-2026062101b",
		e2eDataPoint{id: "svc-a", shortName: "alpha", endpointURL: "https://example.com/api/login", properties: "p-a-v2", timestamp: base + hour, version: 5},
	)
	// Source seg #3: svc-c in hour 05 (a distinct split target).
	writeE2ESourceSeg(
		t, m, root, "seg-2026062105",
		e2eDataPoint{id: "svc-c", shortName: "gamma", endpointURL: "https://shop.example.org/cart", properties: "p-c", timestamp: base + 5*hour, version: 1},
	)

	in := migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}
	res, err := copyIndexModeGroup(context.Background(), in, e2eRuleByID(), e2eSchemasBySubject())
	require.NoError(t, err)
	// hour 01: svc-a (merged) + svc-b; hour 05: svc-c -> 2 target segs.
	require.Equal(t, 2, res.Segments)

	hour01 := filepath.Join(dst, "seg-2026062101", directCopySidxDirName)
	hour05 := filepath.Join(dst, "seg-2026062105", directCopySidxDirName)
	require.Equal(t, 2, countSidxDocsWith(t, hour01, e2eRuleByID(), e2eSchemasBySubject()),
		"hour 01 holds svc-a (merged) + svc-b")
	require.Equal(t, 1, countSidxDocsWith(t, hour05, e2eRuleByID(), e2eSchemasBySubject()),
		"hour 05 holds svc-c")

	// Max-version merge: the surviving svc-a doc must carry version 5 + p-a-v2.
	docs, err := readIndexModeDocs(context.Background(), hour01, e2eRuleByID(), e2eSchemasBySubject())
	require.NoError(t, err)
	var svcADoc *index.Document
	for i := range docs {
		var series pbv1.Series
		require.NoError(t, series.Unmarshal(docs[i].EntityValues))
		if series.EntityValues[0].GetStr().GetValue() == "svc-a" {
			svcADoc = &docs[i]
		}
	}
	require.NotNil(t, svcADoc, "svc-a must be present in hour 01")
	require.Equal(t, int64(5), svcADoc.Version, "max version must win across merged sources")
	pf, ok := fieldByMarshaledName(*svcADoc, "properties")
	require.True(t, ok)
	require.Equal(t, "p-a-v2", string(pf.GetBytes()), "the max-version doc's stored value must survive")

	// Searchability across the split target segs: each analyzed / index-only /
	// sorted field still resolves on the rebuilt sidx.
	// _im_name on both hour segs.
	require.Equal(t, 2, countFieldMatches(t, hour01, index.IndexModeName, e2eMeasureName))
	require.Equal(t, 1, countFieldMatches(t, hour05, index.IndexModeName, e2eMeasureName))
	// url-analyzer: "com" hits svc-a + svc-b in hour 01, "shop" hits svc-c in hour 05.
	require.Equal(t, 2, countFieldMatches(t, hour01, string(convert.Uint32ToBytes(e2eRuleIDEndpointURL)), "com"))
	require.Equal(t, 1, countFieldMatches(t, hour05, string(convert.Uint32ToBytes(e2eRuleIDEndpointURL)), "shop"))
	// _im_entity_tag_id resolves the merged svc-a.
	require.Equal(t, 1, countFieldMatches(t, hour01, index.IndexModeEntityTagPrefix+"id", "svc-a"))
	// default keyword rule on short_name.
	require.Equal(t, 1, countFieldMatches(t, hour01, string(convert.Uint32ToBytes(e2eRuleIDShortName)), "alpha"))
}

// TestIndexModeMigration_SearchabilityPreserved is the core Phase 6 assertion:
// after a slow-path rebuild migration, the target sidx must reconcile every data
// value with the source AND keep every field searchable — by measure name
// (_im_name), by entity tag (_im_entity_tag_id, both index-only), by a
// url-analyzer tag, and by a default keyword (noSort:true) tag — with hit counts
// matching the source. This directly nails the "id present, searchability lost"
// failure the rebuild path guards against.
func TestIndexModeMigration_SearchabilityPreserved(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(t.TempDir(), "tgt")
	schema, rules := e2eIndexModeSchema()
	m := newE2EMeasure(t, schema, rules)
	// Target HOUR != implicit source DAY -> force the slow-path rebuild so the
	// index-only _im_* fields and analyzer/sort flags are reconstructed (not
	// byte-copied). If the rebuild dropped them, searchability would break here.
	ir := storage.IntervalRule{Unit: storage.HOUR, Num: 1}
	base := day20260621Nanos(t)
	hour := int64(time.Hour)

	// Split the three docs across TWO source segs that both map wholly into hour
	// bucket 01. Because neither source exclusively owns that target seg, BOTH are
	// routed through the slow-path rebuild (not byte-copy), forcing the index-only
	// _im_* fields and analyzer/sort flags to be reconstructed. They all land in
	// ONE target seg, so per-field source/target hit counts compare exactly.
	srcSegA := []e2eDataPoint{
		{id: "svc-a", shortName: "alpha", endpointURL: "https://example.com/api/login", properties: "p-a", timestamp: base + hour, version: 1},
		{id: "svc-b", shortName: "beta", endpointURL: "https://example.com/api/logout", properties: "p-b", timestamp: base + hour + int64(time.Minute), version: 1},
	}
	srcSegB := []e2eDataPoint{
		{id: "svc-c", shortName: "gamma", endpointURL: "https://shop.example.org/cart", properties: "p-c", timestamp: base + hour + int64(2*time.Minute), version: 1},
	}
	writeE2ESourceSeg(t, m, root, "seg-2026062101a", srcSegA...)
	writeE2ESourceSeg(t, m, root, "seg-2026062101b", srcSegB...)
	srcSidxA := filepath.Join(root, "seg-2026062101a", directCopySidxDirName)
	srcSidxB := filepath.Join(root, "seg-2026062101b", directCopySidxDirName)

	in := migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: dst,
		SrcRoots:        []string{root},
		Interval:        ir,
	}
	res, err := copyIndexModeGroup(context.Background(), in, e2eRuleByID(), e2eSchemasBySubject())
	require.NoError(t, err)
	require.Equal(t, 1, res.Segments, "all docs share one hour bucket")
	require.Equal(t, int64(3), res.Rows)
	require.Zero(t, res.Bytes, "two sources into one seg force a slow-path rebuild, not a byte-copy")

	tgtSidx := filepath.Join(dst, "seg-2026062101", directCopySidxDirName)

	// (1) Value reconciliation: each (series, timestamp) digest equals the source.
	report, err := verifyIndexModeGroup(context.Background(), in, e2eRuleByID(), e2eSchemasBySubject())
	require.NoError(t, err)
	require.Empty(t, report.ValueMismatches,
		"slow-path rebuild must reconcile every value (incl. entity values + version)")
	require.Equal(t, uint64(3), report.SrcDocs)
	require.Equal(t, uint64(3), report.TgtDocs)
	require.Equal(t, uint64(3), report.SrcDistinctIDs)
	require.Equal(t, report.SrcDistinctIDs, report.TgtDistinctIDs, "doc-id count preserved")

	// (2) Searchability: hit counts on the target equal the source per field. The
	// source side sums both source sidx dirs so the comparison spans all docs.
	fieldChecks := []struct {
		name  string
		field string
		term  string
		want  int
	}{
		{"_im_name (measure subject, index-only)", index.IndexModeName, e2eMeasureName, 3},
		{"_im_entity_tag_id (entity, index-only)", index.IndexModeEntityTagPrefix + "id", "svc-b", 1},
		{"url-analyzer endpoint_url (host token)", string(convert.Uint32ToBytes(e2eRuleIDEndpointURL)), "com", 2},
		{"url-analyzer endpoint_url (path token)", string(convert.Uint32ToBytes(e2eRuleIDEndpointURL)), "login", 1},
		{"default keyword short_name (noSort:true)", string(convert.Uint32ToBytes(e2eRuleIDShortName)), "gamma", 1},
	}
	for _, c := range fieldChecks {
		srcHits := countFieldMatches(t, srcSidxA, c.field, c.term) + countFieldMatches(t, srcSidxB, c.field, c.term)
		tgtHits := countFieldMatches(t, tgtSidx, c.field, c.term)
		require.Equal(t, c.want, srcHits, "source hit count mismatch for %s", c.name)
		require.Equal(t, srcHits, tgtHits,
			"target hit count for %s must equal source (searchability lost?)", c.name)
	}
}

// countSidxDocsWith opens a sidx read-only with explicit rule/schema tables and
// returns the rebuilt doc count.
func countSidxDocsWith(t *testing.T, sidxDir string, ruleByID map[uint32]indexRuleInfo,
	schemas map[string]*measureSchemaInfo,
) int {
	t.Helper()
	docs, err := readIndexModeDocs(context.Background(), sidxDir, ruleByID, schemas)
	require.NoError(t, err)
	return len(docs)
}
