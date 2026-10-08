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
	"context"
	"path/filepath"
	"testing"

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
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// schemaDoc describes one property doc to plant into a test shard.
type schemaDoc struct {
	source  proto.Message // nil -> no source tag
	group   string
	name    string
	kind    schema.Kind
	modRev  int64
	deleted bool
}

func strTag(key, value string) *modelv1.Tag {
	return &modelv1.Tag{
		Key:   key,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	}
}

// readerTestLease is a minimal native.PathRootLease stub: these tests write
// directly into a throwaway shard directory with no surrounding database
// lock to validate against.
type readerTestLease struct{}

func (readerTestLease) Validate() error           { return nil }
func (readerTestLease) ValidatePath(string) error { return nil }

// writeShard persists docs into one native shard the same shape the property
// schema server writes: _source carries protojson(Property), _deleted is a
// non-empty stored field on tombstones, and the kind (Metadata.Name) is an
// indexed term that WalkShard's kind pushdown matches on.
func writeShard(t *testing.T, shardPath string, docs []schemaDoc) {
	t.Helper()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: readerTestLease{}, Path: shardPath})
	require.NoError(t, err)
	documents := make([]native.Document, 0, len(docs))
	for i, d := range docs {
		propID := property.BuildPropertyID(d.kind, &commonv1.Metadata{Group: d.group, Name: d.name})
		prop := &propertyv1.Property{
			Id: propID,
			Metadata: &commonv1.Metadata{
				Name:        d.kind.String(),
				ModRevision: d.modRev,
			},
			Tags: []*modelv1.Tag{strTag(property.TagKeyGroup, d.group)},
		}
		if d.source != nil {
			srcJSON, mErr := protojson.Marshal(d.source)
			require.NoError(t, mErr)
			prop.Tags = append(prop.Tags, strTag(property.TagKeySource, string(srcJSON)))
		}
		propJSON, mErr := protojson.Marshal(prop)
		require.NoError(t, mErr)
		fields := []native.Field{
			{Name: propSourceField, Value: propJSON, Store: true},
			{Name: index.IndexModeName, Value: []byte(d.kind.String()), Index: true},
			{Name: propGroupField, Value: []byte(schema.SchemaGroup), Index: true},
		}
		if d.deleted {
			fields = append(fields, native.Field{Name: propDeleteField, Value: []byte("1"), Store: true})
		}
		// Distinct identifiers keep every revision visible to the reader,
		// matching how the migration reader sees multiple revisions.
		documents = append(documents, native.Document{
			Identifier: []byte(propID + "@" + string(rune('a'+i))),
			Fields:     fields,
		})
	}
	done := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), native.Batch{
		Documents:          documents,
		PersistentCallback: func(batchErr error) { done <- batchErr },
	}))
	require.NoError(t, <-done)
	require.NoError(t, owner.Close())
}

func groupProto(name string, catalog commonv1.Catalog, segDays uint32) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  catalog,
		ResourceOpts: &commonv1.ResourceOpts{
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: segDays},
		},
	}
}

func TestLoadGroups(t *testing.T) {
	root := t.TempDir()
	writeShard(t, filepath.Join(root, "shard-0"), []schemaDoc{
		{kind: schema.KindGroup, name: "g-stream", source: groupProto("g-stream", commonv1.Catalog_CATALOG_STREAM, 1), modRev: 1},
		// A newer revision of the same group must win.
		{kind: schema.KindGroup, name: "g-stream", source: groupProto("g-stream", commonv1.Catalog_CATALOG_STREAM, 3), modRev: 2},
		{kind: schema.KindGroup, name: "g-measure", source: groupProto("g-measure", commonv1.Catalog_CATALOG_MEASURE, 1), modRev: 1},
		{kind: schema.KindGroup, name: "g-deleted", source: groupProto("g-deleted", commonv1.Catalog_CATALOG_STREAM, 1), modRev: 5, deleted: true},
		{kind: schema.KindGroup, name: "g-unwanted", source: groupProto("g-unwanted", commonv1.Catalog_CATALOG_STREAM, 1), modRev: 1},
	})

	groups, err := LoadGroups(root, []string{"g-stream", "g-measure", "g-deleted", "g-missing"})
	require.NoError(t, err)
	require.Len(t, groups, 2)
	require.Equal(t, commonv1.Catalog_CATALOG_STREAM, groups["g-stream"].GetCatalog())
	require.EqualValues(t, 3, groups["g-stream"].GetResourceOpts().GetSegmentInterval().GetNum())
	require.Equal(t, commonv1.Catalog_CATALOG_MEASURE, groups["g-measure"].GetCatalog())
	require.NotContains(t, groups, "g-deleted")
	require.NotContains(t, groups, "g-unwanted")
}

func TestLoadStreamsAndMeasures(t *testing.T) {
	root := t.TempDir()
	writeShard(t, filepath.Join(root, "shard-0"), []schemaDoc{
		{kind: schema.KindStream, group: "g1", name: "s1", modRev: 1, source: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "s1"},
		}},
		{kind: schema.KindStream, group: "g-other", name: "s2", modRev: 1, source: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: "g-other", Name: "s2"},
		}},
	})
	// Spread across a second shard to cover the multi-shard walk.
	writeShard(t, filepath.Join(root, "shard-1"), []schemaDoc{
		{kind: schema.KindMeasure, group: "g1", name: "m1", modRev: 1, source: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "m1"},
		}},
	})

	streams, err := LoadStreams(root, []string{"g1"})
	require.NoError(t, err)
	require.Len(t, streams["g1"], 1)
	require.Equal(t, "s1", streams["g1"][0].GetMetadata().GetName())
	require.NotContains(t, streams, "g-other")

	measures, err := LoadMeasures(root, []string{"g1"})
	require.NoError(t, err)
	require.Len(t, measures["g1"], 1)
	require.Equal(t, "m1", measures["g1"][0].GetMetadata().GetName())
}

func TestLoadStreamsMissingSourceFails(t *testing.T) {
	root := t.TempDir()
	writeShard(t, filepath.Join(root, "shard-0"), []schemaDoc{
		{kind: schema.KindStream, group: "g1", name: "s1", modRev: 1, source: nil},
	})
	_, err := LoadStreams(root, []string{"g1"})
	require.ErrorContains(t, err, "missing source tag")
}

func TestLoadStreamSchemaContextScoping(t *testing.T) {
	root := t.TempDir()
	writeShard(t, filepath.Join(root, "shard-0"), []schemaDoc{
		{kind: schema.KindStream, group: "g1", name: "s1", modRev: 1, source: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "s1"},
		}},
		{kind: schema.KindIndexRule, group: "g1", name: "r1", modRev: 1, source: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "r1"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		}},
		{kind: schema.KindIndexRule, group: "g1", name: "r-deleted", modRev: 1, deleted: true, source: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "r-deleted"},
		}},
		// A rule in an unwanted group must not load: rules resolve within
		// the subject's group only, mirroring metadata's IndexRules.
		{kind: schema.KindIndexRule, group: "g-other", name: "r-other", modRev: 1, source: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Group: "g-other", Name: "r-other"},
		}},
		{kind: schema.KindIndexRuleBinding, group: "g1", name: "b1", modRev: 1, source: &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "b1"},
			Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: "s1"},
			Rules:    []string{"r1"},
		}},
		// A measure-subject binding in a wanted group must be dropped.
		{kind: schema.KindIndexRuleBinding, group: "g1", name: "b-measure", modRev: 1, source: &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Group: "g1", Name: "b-measure"},
			Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m1"},
		}},
		// A binding in an unwanted group must be dropped.
		{kind: schema.KindIndexRuleBinding, group: "g-other", name: "b-other", modRev: 1, source: &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Group: "g-other", Name: "b-other"},
			Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: "s-other"},
		}},
	})

	sc, err := LoadStreamSchemaContext(root, []string{"g1"})
	require.NoError(t, err)
	require.Len(t, sc.Streams["g1"], 1)
	require.Len(t, sc.Rules, 1)
	require.Len(t, sc.Rules["g1"], 1)
	require.Equal(t, databasev1.IndexRule_TYPE_INVERTED, sc.Rules["g1"]["r1"].GetType())
	require.Len(t, sc.Bindings, 1)
	require.Equal(t, "g1", sc.Bindings[0].Group)
	require.Equal(t, "s1", sc.Bindings[0].Binding.GetSubject().GetName())
}
