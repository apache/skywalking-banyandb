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

package migration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

const classifyTestDate = "2026-06-10"

// fakeExecutor is a classification-only CatalogExecutor stub: ClassifyGroups
// reads nothing but Catalog().
type fakeExecutor struct {
	cat commonv1.Catalog
}

func (f fakeExecutor) Catalog() commonv1.Catalog { return f.cat }
func (f fakeExecutor) LogPrefix() string         { return "[fake]" }
func (f fakeExecutor) Prepare(_ context.Context, _ string, _ []string) error {
	return nil
}

func (f fakeExecutor) CopyEntryGroup(_ context.Context, _ EntryGroupInput) (EntryGroupResult, error) {
	return EntryGroupResult{}, nil
}

func (f fakeExecutor) VerifyEntryGroup(_ context.Context, _ EntryGroupInput, _ func(report any)) error {
	return nil
}

// classifyTestExecutors registers the stream + measure catalogs, matching
// what the CLI wires up.
func classifyTestExecutors() []CatalogExecutor {
	return []CatalogExecutor{
		fakeExecutor{cat: commonv1.Catalog_CATALOG_STREAM},
		fakeExecutor{cat: commonv1.Catalog_CATALOG_MEASURE},
	}
}

// backupPlan builds a backup-mode plan over root with the given groups.
func backupPlan(root string, groups ...string) *CopyPlan {
	return &CopyPlan{
		Source: CopySource{Backup: &BackupSource{Root: root, Date: classifyTestDate}},
		Groups: groups,
		Entries: []CopyEntry{
			{Stage: "hot", Target: filepath.Join(root, "out"), Nodes: []string{"node-0"}},
		},
	}
}

// TestClassifyGroups_SchemaRequired: a backup tree without a schema-property
// catalog cannot classify at all, even when the group exists in the layout.
func TestClassifyGroups_SchemaRequired(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "node-0", classifyTestDate, "stream", "g-stream"), 0o755))

	_, err := backupPlan(root, "g-stream").ClassifyGroups(classifyTestExecutors())
	require.ErrorIs(t, err, errSchemaPropertyMissing)
}

func TestClassifyGroups_LiveModeNeedsSchema(t *testing.T) {
	plan := &CopyPlan{
		Source: CopySource{Live: &LiveSource{
			SchemaPropertyPath: filepath.Join(t.TempDir(), "no-such-schema"),
			Stages: map[string][]LiveStageNode{
				"hot": {{Node: "node-0", Root: t.TempDir()}},
			},
		}},
		Groups: []string{"g-any"},
		Entries: []CopyEntry{
			{Stage: "hot", Target: filepath.Join(t.TempDir(), "out"), Nodes: []string{"node-0"}},
		},
	}
	_, err := plan.ClassifyGroups(classifyTestExecutors())
	require.Error(t, err, "live mode requires a readable schema-property path")
}

// seedSchemaGroupDoc plants one KindGroup doc into the backup tree's
// schema-property shard, in the exact shape the property schema server
// persists (protojson(Property) under the `_source` stored field).
func seedSchemaGroupDoc(t *testing.T, root, group string, catalog commonv1.Catalog) {
	t.Helper()
	shardPath := filepath.Join(root, "node-0", classifyTestDate,
		backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup, "shard-0")
	require.NoError(t, os.MkdirAll(shardPath, 0o755))
	grp := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group, ModRevision: 1},
		Catalog:  catalog,
		ResourceOpts: &commonv1.ResourceOpts{
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
		},
	}
	grpJSON, err := protojson.Marshal(grp)
	require.NoError(t, err)
	prop := &propertyv1.Property{
		Id:       "group/" + group,
		Metadata: &commonv1.Metadata{Name: schema.KindGroup.String(), ModRevision: 1},
		Tags: []*modelv1.Tag{
			{Key: "source", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(grpJSON)}}}},
		},
	}
	propJSON, err := protojson.Marshal(prop)
	require.NoError(t, err)
	w, err := bluge.OpenWriter(bluge.DefaultConfig(shardPath))
	require.NoError(t, err)
	batch := bluge.NewBatch()
	batch.Insert(bluge.NewDocument(prop.Id).
		AddField(bluge.NewStoredOnlyField("_source", propJSON)).
		AddField(bluge.NewKeywordFieldBytes(index.IndexModeName, []byte(schema.KindGroup.String()))).
		AddField(bluge.NewKeywordFieldBytes("_group", []byte(schema.SchemaGroup))))
	require.NoError(t, w.Batch(batch))
	require.NoError(t, w.Close())
}

// TestClassifyGroups_SchemaAuthoritative covers the primary path: catalogs
// resolve from KindGroup docs in the schema-property catalog and the Group
// protos (incl. ResourceOpts) are retained.
func TestClassifyGroups_SchemaAuthoritative(t *testing.T) {
	root := t.TempDir()
	seedSchemaGroupDoc(t, root, "g-stream", commonv1.Catalog_CATALOG_STREAM)
	seedSchemaGroupDoc(t, root, "g-measure", commonv1.Catalog_CATALOG_MEASURE)

	cls, err := backupPlan(root, "g-stream", "g-measure").ClassifyGroups(classifyTestExecutors())
	require.NoError(t, err)
	require.NotEmpty(t, cls.SchemaRoot)
	require.Equal(t, []string{"g-stream"}, cls.Buckets[commonv1.Catalog_CATALOG_STREAM])
	require.Equal(t, []string{"g-measure"}, cls.Buckets[commonv1.Catalog_CATALOG_MEASURE])
	require.NotNil(t, cls.Groups["g-stream"].GetResourceOpts(), "schema-resolved group keeps its ResourceOpts")
}

// TestClassifyGroups_GroupMissingFromSchema: a plan group absent from the
// schema-property catalog is an error — there is no layout fallback.
func TestClassifyGroups_GroupMissingFromSchema(t *testing.T) {
	root := t.TempDir()
	seedSchemaGroupDoc(t, root, "g-stream", commonv1.Catalog_CATALOG_STREAM)

	_, err := backupPlan(root, "g-stream", "g-ghost").ClassifyGroups(classifyTestExecutors())
	require.ErrorContains(t, err, "not found in the source schema-property catalog")
}

// TestClassifyGroups_SchemaTraceRejected: a TRACE catalog from the schema
// must be rejected.
func TestClassifyGroups_SchemaTraceRejected(t *testing.T) {
	root := t.TempDir()
	seedSchemaGroupDoc(t, root, "g-trace", commonv1.Catalog_CATALOG_TRACE)

	_, err := backupPlan(root, "g-trace").ClassifyGroups(classifyTestExecutors())
	require.ErrorContains(t, err, "no executor registered")
}
