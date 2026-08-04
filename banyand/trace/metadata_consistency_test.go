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

package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	metadataschema "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// consistencyRepo injects controlled cache and runtime state. pkg/schema's
// resourceSpec is unexported, so a test cannot populate a real schemaRepo's
// resourceMap; substituting the embedded Repository is the pattern this package
// already uses (see fakeRepository in write_data_segmentref_test.go).
type consistencyRepo struct {
	resourceSchema.Repository
	indexRules map[string][]*databasev1.IndexRule
	group      resourceSchema.Group
	resources  []resourceSchema.Resource
	rules      []*databasev1.IndexRule
}

func (f *consistencyRepo) LoadAllResources(_ string) []resourceSchema.Resource { return f.resources }

func (f *consistencyRepo) LoadAllIndexRules(_ string) []*databasev1.IndexRule { return f.rules }

func (f *consistencyRepo) LoadGroup(_ string) (resourceSchema.Group, bool) {
	if f.group == nil {
		return nil, false
	}
	return f.group, true
}

func (f *consistencyRepo) IndexRules(s resourceSchema.ResourceSchema) []*databasev1.IndexRule {
	return f.indexRules[s.GetMetadata().GetName()]
}

type consistencyResource struct {
	schema    resourceSchema.ResourceSchema
	delegated resourceSchema.IndexListener
}

func (f consistencyResource) Schema() resourceSchema.ResourceSchema   { return f.schema }
func (f consistencyResource) Delegated() resourceSchema.IndexListener { return f.delegated }

func consistencyTraceSpec(group, name string) *databasev1.Trace {
	return &databasev1.Trace{
		Metadata: &commonv1.Metadata{Group: group, Name: name},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
		TraceIdTagName:   "trace_id",
		TimestampTagName: "timestamp",
	}
}

func newRuntimeTrace(t *testing.T, spec *databasev1.Trace, snapshot []*databasev1.IndexRule) *trace {
	t.Helper()
	tr := &trace{schema: spec}
	tr.parseSpec()
	tr.OnIndexUpdate(snapshot)
	return tr
}

func newConsistencyTestRepo(t *testing.T, group, name string) (*schemaRepo, *consistencyRepo) {
	t.Helper()
	spec := consistencyTraceSpec(group, name)
	fake := &consistencyRepo{
		indexRules: map[string][]*databasev1.IndexRule{name: nil},
		resources: []resourceSchema.Resource{consistencyResource{
			schema: spec, delegated: newRuntimeTrace(t, spec, nil),
		}},
	}
	return &schemaRepo{
		Repository: fake,
		l:          logger.GetLogger("test-trace-consistency"),
		nodeID:     "data-1",
	}, fake
}

func objectOf(objects []*databasev1.ObjectSchemaState, kind, name string) *databasev1.ObjectSchemaState {
	for _, o := range objects {
		if o.GetKind() == kind && o.GetName() == name {
			return o
		}
	}
	return nil
}

func TestCollectSchemaState_ReportsCacheAndRuntime(t *testing.T) {
	sr, _ := newConsistencyTestRepo(t, "g", "foo")

	got, err := sr.collectSchemaState("g")
	require.NoError(t, err)

	require.NotEmpty(t, got)
	found := objectOf(got, metadataschema.KindTrace.String(), "foo")
	require.NotNil(t, found, "the cached trace must be reported")
	assert.NotZero(t, found.GetCacheFingerprint())
	assert.Equal(t, found.GetCacheFingerprint(), found.GetRuntimeFingerprint(),
		"a healthy node must agree with itself across cache and runtime")
}

func TestCollectSchemaState_DetectsStaleRuntimeIndex(t *testing.T) {
	// Reproduce the reindexBoundSubjects early return: the cache gains a rule but
	// OnIndexUpdate is never called, so the runtime snapshot stays behind.
	sr, fake := newConsistencyTestRepo(t, "g", "foo")
	fake.indexRules["foo"] = []*databasev1.IndexRule{{
		Metadata: &commonv1.Metadata{Group: "g", Name: "r1"},
		Tags:     []string{"trace_id"},
	}}

	got, err := sr.collectSchemaState("g")
	require.NoError(t, err)

	found := objectOf(got, metadataschema.KindTrace.String(), "foo")
	require.NotNil(t, found)
	assert.NotEqual(t, found.GetCacheFingerprint(), found.GetRuntimeFingerprint(),
		"a rule present in cache but absent from the runtime snapshot must diverge")
}
