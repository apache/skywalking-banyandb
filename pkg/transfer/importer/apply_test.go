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

package importer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

func TestBarrierKeySkipsOnlyPropertyGroups(t *testing.T) {
	group := func(catalog commonv1.Catalog) *SchemaAction {
		return &SchemaAction{Kind: schemasync.KindGroup, Group: "g", Spec: &commonv1.Group{Metadata: &commonv1.Metadata{Name: "g"}, Catalog: catalog}}
	}
	assert.Equal(t, &schemav1.SchemaKey{Kind: "group", Name: "g"}, barrierKey(group(commonv1.Catalog_CATALOG_MEASURE)))
	assert.Nil(t, barrierKey(group(commonv1.Catalog_CATALOG_PROPERTY)), "a property group")
	assert.Equal(t, &schemav1.SchemaKey{Kind: "index_rule_binding", Group: "g", Name: "b"},
		barrierKey(&SchemaAction{Kind: schemasync.KindIndexRuleBinding, Group: "g", Name: "b", Spec: &databasev1.IndexRuleBinding{}}))
	assert.Equal(t, &schemav1.SchemaKey{Kind: "index_rule", Group: "g", Name: "r"},
		barrierKey(&SchemaAction{Kind: schemasync.KindIndexRule, Group: "g", Name: "r", Spec: &databasev1.IndexRule{}}))
	assert.Equal(t, &schemav1.SchemaKey{Kind: "property", Group: "g", Name: "p"},
		barrierKey(&SchemaAction{Kind: schemasync.KindProperty, Group: "g", Name: "p", Spec: &databasev1.Property{}}))
}

func TestAwaitSchemaSplitsKeysUnderTheBarrierCap(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	n := 2*awaitSchemaMaxKeys + 1
	req := &schemav1.AwaitSchemaAppliedRequest{}
	for i := 0; i < n; i++ {
		req.Keys = append(req.Keys, &schemav1.SchemaKey{Kind: "measure", Group: "g", Name: fmt.Sprintf("m%d", i)})
		req.MinRevisions = append(req.MinRevisions, int64(i))
	}
	assert.NoError(t, AwaitSchema(context.Background(), li.Conn, req, time.Minute))
	if assert.Len(t, f.awaits, 3) {
		var seen int
		for _, a := range f.awaits {
			assert.LessOrEqual(t, len(a.GetKeys()), awaitSchemaMaxKeys)
			assert.Len(t, a.GetMinRevisions(), len(a.GetKeys()), "each key keeps its revision")
			assert.Equal(t, int64(seen), a.GetMinRevisions()[0], "the keys are awaited in order")
			seen += len(a.GetKeys())
		}
		assert.Equal(t, n, seen)
	}
}

func TestWriteSchemaKeepsIndexRuleIDsOnCreate(t *testing.T) {
	art := NewSchemas()
	src := testIndexRule("g", "level")
	src.Metadata.Id = 7
	art.IndexRules[ResourceKey{"g", "level"}] = src
	orig := proto.Clone(src)
	write := func(f *fakeLiaison, disposition Disposition, keep bool) {
		t.Helper()
		action := SchemaAction{
			Kind: schemasync.KindIndexRule, Group: "g", Name: "level", Disposition: disposition, Spec: PrepareForWrite(src),
		}
		_, err := WriteSchema(t.Context(), f.serve(t).Conn, []SchemaAction{action}, &art, keep)
		require.NoError(t, err)
	}
	stored := func(f *fakeLiaison) uint32 {
		return f.schema.IndexRules[ResourceKey{"g", "level"}].GetMetadata().GetId()
	}

	f := newFakeLiaison()
	write(f, DispositionCreate, true)
	assert.Equal(t, uint32(7), stored(f), "a native import creates the rule with the artifact's id")
	assert.True(t, proto.Equal(orig, src), "the artifact's rule is not changed")

	f = newFakeLiaison()
	write(f, DispositionCreate, false)
	assert.Equal(t, uint32(0), stored(f), "a CSV import leaves the id to the target")

	f = newFakeLiaison()
	target := testIndexRule("g", "level")
	target.Metadata.Id = 9
	f.schema.IndexRules[ResourceKey{"g", "level"}] = target
	write(f, DispositionOverwrite, true)
	assert.Equal(t, uint32(0), stored(f), "an update sends no artifact id; the registry keeps the target's")
}
