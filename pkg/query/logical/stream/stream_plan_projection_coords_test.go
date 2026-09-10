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

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

// coordsStreamSchema declares TWO tag families so a family-ordering drift is
// observable at all: with one family every TagFamilyIdx is 0 and the invariant
// below holds vacuously.
func coordsStreamSchema() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "coords", Group: "test"},
		Entity:   &databasev1.Entity{TagNames: []string{"entity_tag"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "first",
				Tags: []*databasev1.TagSpec{
					{Name: "entity_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
			{
				Name: "second",
				Tags: []*databasev1.TagSpec{
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "state", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}
}

// TestProjectionCoordinatesAddressTheRightColumn pins the one invariant the
// pre-merge tag filter rests on and nothing else enforces.
//
// projectionBuilder.Model() becomes the scan's projectionTags, which the filter's
// row accessor walks to turn a (tagFamilyIdx, tagIdx) pair into a batch column.
// projectionBuilder.LogicalTags() becomes projTagsRefs, which CommonSchema.ProjTags
// numbers BY POSITION into the TagSpecs the filter indexes with. The two agree
// today only because they are the same loop written out twice.
//
// If they ever drift the filter silently reads the WRONG tag — no error, no panic,
// just wrong rows. The criteria here sits on the SECOND tag of the SECOND family,
// so a drift in either dimension moves it.
func TestProjectionCoordinatesAddressTheRightColumn(t *testing.T) {
	sch, err := BuildSchema(coordsStreamSchema(), nil)
	require.NoError(t, err)

	// Build the projection the way Analyze does: the client tags first, then the
	// criteria tag force-added on top.
	projBuilder := newProjectionBuilder([][]*logical.Tag{
		{logical.NewTag("first", "duration")},
		{logical.NewTag("second", "span_id")},
	})
	added, addErr := projBuilder.AddTagFromSchema(sch, "state")
	require.NoError(t, addErr)
	require.True(t, added, "the criteria tag must be force-added, or this test proves nothing")

	projection := projBuilder.Model()
	refs, refErr := sch.CreateTagRef(projBuilder.LogicalTags()...)
	require.NoError(t, refErr)
	projSchema := sch.ProjTags(refs...)
	require.NotNil(t, projSchema)

	batchSchema := vstream.BuildStreamBatchSchema(projection, "first", "duration")

	// The column each projected tag actually occupies, resolved independently of
	// the TagSpec coordinates under test.
	wantColIdx := make(map[string]int, 3)
	for _, proj := range projection {
		for _, tagName := range proj.Names {
			colIdx, ok := batchSchema.TagIndex(proj.Family, tagName)
			require.True(t, ok, "projected tag %q has no column", tagName)
			wantColIdx[tagName] = colIdx
		}
	}
	require.Len(t, wantColIdx, 3)

	for _, proj := range projection {
		for _, tagName := range proj.Names {
			spec := projSchema.FindTagSpecByName(tagName)
			require.NotNil(t, spec, "projected tag %q is missing from the projected schema", tagName)
			require.Less(t, spec.TagFamilyIdx, len(projection), "family index out of the projection's range")
			family := projection[spec.TagFamilyIdx]
			require.Less(t, spec.TagIdx, len(family.Names), "tag index out of family %q's range", family.Family)
			colIdx, ok := batchSchema.TagIndex(family.Family, family.Names[spec.TagIdx])
			require.True(t, ok)
			require.Equal(t, wantColIdx[tagName], colIdx,
				"the filter's (family,tag) coordinates must address the column that actually holds %q", tagName)
		}
	}
}
