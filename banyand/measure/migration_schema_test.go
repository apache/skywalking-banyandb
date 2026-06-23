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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/blugelabs/bluge"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// synthMeasure describes one measure to seed into the synthetic
// schema-property catalog.
type synthMeasure struct {
	group  string
	name   string
	modRev int64
}

// synthGroup describes one group to seed into the synthetic
// schema-property catalog. Opts is optional; nil yields a bare group
// metadata doc (sufficient for the measure-fetch tests).
type synthGroup struct {
	opts *commonv1.ResourceOpts
	name string
}

// synthBackup builds a minimal backup tree at root with one
// schema-property shard carrying the given groups and measures so the
// fetch/load helpers can exercise the full bluge → proto → info path
// without any real cluster snapshot.
func synthBackup(t *testing.T, root string, groups []synthGroup, measures []synthMeasure) {
	t.Helper()
	shardPath := filepath.Join(root, "node-0", "2026-05-21",
		backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup, "shard-0")
	if err := os.MkdirAll(shardPath, storage.DirPerm); err != nil {
		t.Fatalf("mkdir shard: %v", err)
	}
	w, err := bluge.OpenWriter(bluge.DefaultConfig(shardPath))
	if err != nil {
		t.Fatalf("open bluge writer: %v", err)
	}
	defer func() {
		if cErr := w.Close(); cErr != nil {
			t.Fatalf("close bluge writer: %v", cErr)
		}
	}()

	batch := bluge.NewBatch()
	for _, g := range groups {
		grp := &commonv1.Group{
			Metadata:     &commonv1.Metadata{Name: g.name, ModRevision: 1},
			ResourceOpts: g.opts,
		}
		grpJSON, mErr := protojson.Marshal(grp)
		if mErr != nil {
			t.Fatalf("marshal group %q: %v", g.name, mErr)
		}
		batch.Insert(synthSchemaDoc("group/"+g.name, schema.KindGroup.String(), "", 1, string(grpJSON)))
	}
	for _, m := range measures {
		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: m.name, Group: m.group, ModRevision: m.modRev},
			Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{
					{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "label", Type: databasev1.TagType_TAG_TYPE_STRING},
				}},
			},
			Fields: []*databasev1.FieldSpec{
				{Name: "total", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			},
		}
		mJSON, mErr := protojson.Marshal(measure)
		if mErr != nil {
			t.Fatalf("marshal measure %q/%q: %v", m.group, m.name, mErr)
		}
		docID := fmt.Sprintf("measure/%s/%s/rev-%d", m.group, m.name, m.modRev)
		// Use a stable propID (no revision suffix) so multiple revisions
		// of the same measure share one candidate slot at fetch time.
		propID := fmt.Sprintf("measure/%s/%s", m.group, m.name)
		batch.Insert(synthSchemaDocWithPropID(docID, propID,
			schema.KindMeasure.String(), m.group, m.modRev, string(mJSON)))
	}
	if bErr := w.Batch(batch); bErr != nil {
		t.Fatalf("write batch: %v", bErr)
	}
}

// synthSchemaDoc builds a single bluge doc whose `_source` field carries
// the inner-proto JSON wrapped in a propertyv1.Property, matching the
// shape that schema-property bluge indexes use in production.
func synthSchemaDoc(propID, kind, group string, modRev int64, sourceJSON string) *bluge.Document {
	return synthSchemaDocWithPropID(propID, propID, kind, group, modRev, sourceJSON)
}

func synthSchemaDocWithPropID(docID, propID, kind, group string, modRev int64, sourceJSON string) *bluge.Document {
	tags := []*modelv1.Tag{
		{Key: "source", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: sourceJSON}}}},
	}
	if group != "" {
		tags = append(tags, &modelv1.Tag{
			Key:   "group",
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}}},
		})
	}
	prop := &propertyv1.Property{
		Id:       propID,
		Metadata: &commonv1.Metadata{Name: kind, ModRevision: modRev},
		Tags:     tags,
	}
	propJSON, err := protojson.Marshal(prop)
	if err != nil {
		panic(fmt.Sprintf("marshal property %q: %v", propID, err))
	}
	return bluge.NewDocument(docID).
		AddField(bluge.NewStoredOnlyField("_source", propJSON)).
		AddField(bluge.NewKeywordFieldBytes(index.IndexModeName, []byte(kind))).
		AddField(bluge.NewKeywordFieldBytes("_group", []byte(schema.SchemaGroup)))
}

// synthSchemaRoot resolves the `_schema` root of the synthetic backup the
// same way the orchestrator does in production.
func synthSchemaRoot(t *testing.T, backupDir string) string {
	t.Helper()
	plan := &migration.CopyPlan{Source: migration.CopySource{Backup: &migration.BackupSource{Root: backupDir}}}
	root, err := plan.SchemaRoot()
	if err != nil {
		t.Fatalf("SchemaRoot: %v", err)
	}
	return root
}

// synthGroupsFromNames is a shorthand for callers that don't care
// about ResourceOpts.
func synthGroupsFromNames(names ...string) []synthGroup {
	out := make([]synthGroup, len(names))
	for i, n := range names {
		out[i] = synthGroup{name: n}
	}
	return out
}

func TestMeasureSchemaInfo_IndexModeFields(t *testing.T) {
	m := &databasev1.Measure{
		Metadata:  &commonv1.Metadata{Name: "svc", Group: "g"},
		IndexMode: true,
		Entity:    &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "searchable", Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "weight", Type: databasev1.TagType_TAG_TYPE_INT},
			}},
		},
	}
	info := measureSchemaInfoFromProto("g", m)
	if !info.IndexMode {
		t.Fatal("expected IndexMode true")
	}
	if len(info.EntityTagNames) != 1 || info.EntityTagNames[0] != "id" {
		t.Fatalf("EntityTagNames = %v, want [id]", info.EntityTagNames)
	}
	if info.TagType["id"] != databasev1.TagType_TAG_TYPE_STRING {
		t.Fatalf("TagType[id] = %v, want STRING", info.TagType["id"])
	}
	if info.TagType["weight"] != databasev1.TagType_TAG_TYPE_INT {
		t.Fatalf("TagType[weight] = %v, want INT", info.TagType["weight"])
	}
}

// synthIndexRule describes one index rule to seed into the synthetic catalog.
type synthIndexRule struct {
	group    string
	name     string
	analyzer string
	id       uint32
	noSort   bool
}

// synthIndexRules appends index-rule docs to an existing synthetic backup tree
// built by synthBackup, reusing the same schema-property shard layout.
func synthIndexRules(t *testing.T, root string, rules []synthIndexRule) {
	t.Helper()
	shardPath := filepath.Join(root, "node-0", "2026-05-21",
		backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup, "shard-0")
	w, err := bluge.OpenWriter(bluge.DefaultConfig(shardPath))
	if err != nil {
		t.Fatalf("open bluge writer: %v", err)
	}
	defer func() {
		if cErr := w.Close(); cErr != nil {
			t.Fatalf("close bluge writer: %v", cErr)
		}
	}()
	batch := bluge.NewBatch()
	for _, r := range rules {
		rule := &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: r.name, Group: r.group, Id: r.id, ModRevision: 1},
			Analyzer: r.analyzer,
			NoSort:   r.noSort,
		}
		rJSON, mErr := protojson.Marshal(rule)
		if mErr != nil {
			t.Fatalf("marshal index rule %q: %v", r.name, mErr)
		}
		propID := fmt.Sprintf("index-rule/%s/%s", r.group, r.name)
		batch.Insert(synthSchemaDocWithPropID(propID, propID,
			schema.KindIndexRule.String(), r.group, 1, string(rJSON)))
	}
	if bErr := w.Batch(batch); bErr != nil {
		t.Fatalf("write index rule batch: %v", bErr)
	}
}

func TestLoadIndexRuleInfoByID(t *testing.T) {
	dir := t.TempDir()
	synthBackup(t, dir, synthGroupsFromNames("g_a"), []synthMeasure{
		{group: "g_a", name: "m_a1", modRev: 1},
	})
	synthIndexRules(t, dir, []synthIndexRule{
		{group: "g_a", name: "r_default", id: 100, analyzer: "", noSort: true},
		{group: "g_a", name: "r_url", id: 201559343, analyzer: index.AnalyzerURL, noSort: false},
	})

	byID, err := loadIndexRuleInfoByID(synthSchemaRoot(t, dir), []string{"g_a"})
	if err != nil {
		t.Fatalf("loadIndexRuleInfoByID: %v", err)
	}
	if got := byID[100]; got.Analyzer != "" || !got.NoSort {
		t.Fatalf("rule 100 = %+v, want {Analyzer:\"\", NoSort:true}", got)
	}
	if got := byID[201559343]; got.Analyzer != index.AnalyzerURL || got.NoSort {
		t.Fatalf("rule 201559343 = %+v, want {Analyzer:url, NoSort:false}", got)
	}
}

func TestLoadMeasureSchemas_returnsRequestedGroups(t *testing.T) {
	dir := t.TempDir()
	groups := []string{"g_a", "g_b", "g_c"}
	measures := []synthMeasure{
		{group: "g_a", name: "m_a1", modRev: 1},
		{group: "g_a", name: "m_a2", modRev: 1},
		{group: "g_b", name: "m_b1", modRev: 1},
		{group: "g_c", name: "m_c1", modRev: 1},
		{group: "g_d", name: "m_d1", modRev: 1}, // outside the requested set
	}
	synthBackup(t, dir, synthGroupsFromNames(append(groups, "g_d")...), measures)

	byGroup, err := loadMeasureSchemas(synthSchemaRoot(t, dir), groups)
	if err != nil {
		t.Fatalf("loadMeasureSchemas: %v", err)
	}
	for _, g := range groups {
		if len(byGroup[g]) == 0 {
			t.Fatalf("group %q: expected schemas, got 0", g)
		}
	}
	for g := range byGroup {
		found := false
		for _, want := range groups {
			if g == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("group %q returned but not requested", g)
		}
	}
}

func TestLoadMeasureSchemas_schemaFieldsArePopulated(t *testing.T) {
	dir := t.TempDir()
	synthBackup(t, dir, synthGroupsFromNames("g_a"), []synthMeasure{
		{group: "g_a", name: "m_a1", modRev: 1},
	})

	byGroup, err := loadMeasureSchemas(synthSchemaRoot(t, dir), []string{"g_a"})
	if err != nil {
		t.Fatalf("loadMeasureSchemas: %v", err)
	}
	list := byGroup["g_a"]
	if len(list) == 0 {
		t.Fatal("expected at least one measure schema in g_a")
	}
	for _, s := range list {
		if s.Name == "" {
			t.Fatalf("measure with empty name (Group=%q)", s.Group)
		}
		if s.Group != "g_a" {
			t.Fatalf("measure %q: wrong group %q", s.Name, s.Group)
		}
		if len(s.TagFamilies) == 0 {
			t.Fatalf("measure %q: no tag families", s.Name)
		}
	}
}

func TestLoadMeasureSchemas_unknownGroupReturnsEmpty(t *testing.T) {
	dir := t.TempDir()
	synthBackup(t, dir, synthGroupsFromNames("g_a"), []synthMeasure{
		{group: "g_a", name: "m_a1", modRev: 1},
	})

	byGroup, err := loadMeasureSchemas(synthSchemaRoot(t, dir), []string{"this_group_does_not_exist"})
	if err != nil {
		t.Fatalf("loadMeasureSchemas: %v", err)
	}
	if got := len(byGroup["this_group_does_not_exist"]); got != 0 {
		t.Fatalf("expected 0 schemas for unknown group, got %d", got)
	}
}

func TestLoadMeasureSchemas_missingSchemaRoot(t *testing.T) {
	_, err := loadMeasureSchemas("/no/such/dir/should/exist", []string{"g_a"})
	if err == nil {
		t.Fatal("expected error for nonexistent schema root, got nil")
	}
}

// TestLoadMeasureSchemas_dedupesStaleRevisions asserts that the
// loader collapses historical revisions of the same schema (different
// mod_revisions on the same propID, retained across bluge segments) into a
// single entry. The live cluster's SchemaRegistry already dedupes by
// propID, and the backup loader must match that contract.
func TestLoadMeasureSchemas_dedupesStaleRevisions(t *testing.T) {
	dir := t.TempDir()
	// Same (group, name) appears at three different mod_revisions; the
	// loader must keep only the highest one.
	synthBackup(t, dir, synthGroupsFromNames("g_a"), []synthMeasure{
		{group: "g_a", name: "m_a1", modRev: 1},
		{group: "g_a", name: "m_a1", modRev: 2},
		{group: "g_a", name: "m_a1", modRev: 3},
	})

	byGroup, err := loadMeasureSchemas(synthSchemaRoot(t, dir), []string{"g_a"})
	if err != nil {
		t.Fatalf("loadMeasureSchemas: %v", err)
	}
	seen := map[string]int{}
	for _, s := range byGroup["g_a"] {
		seen[s.Name]++
	}
	for name, count := range seen {
		if count > 1 {
			t.Fatalf("group g_a: measure %q appears %d times (expected 1)", name, count)
		}
	}
}
