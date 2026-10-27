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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

func defaultOpts() SchemaOptions { return SchemaOptions{IncludeSchema: true} }

func actionOf(t *testing.T, d *SchemaDiff, id string) SchemaAction {
	t.Helper()
	for _, a := range d.Actions {
		if a.ID() == id {
			return a
		}
	}
	require.Failf(t, "action not found", "%s", id)
	return SchemaAction{}
}

func TestDiffSchemasIgnoresTimestampsAndRevisions(t *testing.T) {
	art := artifactSchemas()
	d := DiffSchemas(art, mirrorTarget(art), defaultOpts())
	require.Len(t, d.Actions, 11)
	for _, a := range d.Actions {
		assert.Equal(t, DispositionOK, a.Disposition, a.ID())
		assert.Empty(t, a.Diffs, a.ID())
	}
	assert.Empty(t, d.Writes())
	assert.False(t, (&Plan{Schema: d.Actions}).SchemaBlocked())
}

func TestDiffSchemasCreatesInDependencyOrder(t *testing.T) {
	art := artifactSchemas()
	d := DiffSchemas(art, NewTargetState(), defaultOpts())
	var kinds []string
	for _, a := range d.Actions {
		assert.Equal(t, DispositionCreate, a.Disposition, a.ID())
	}
	for _, w := range d.Writes() {
		assert.Equal(t, DispositionCreate, w.Disposition)
		kinds = append(kinds, w.Kind)
		r := w.Spec.ProtoReflect()
		assert.False(t, r.Has(r.Descriptor().Fields().ByName("created_at")), w.Name)
		assert.False(t, r.Has(r.Descriptor().Fields().ByName("updated_at")), w.Name)
	}
	assert.Equal(t, []string{
		schemasync.KindGroup, schemasync.KindGroup, schemasync.KindGroup, schemasync.KindGroup,
		schemasync.KindStream, schemasync.KindMeasure, schemasync.KindTrace, schemasync.KindProperty,
		schemasync.KindIndexRule, schemasync.KindIndexRuleBinding, schemasync.KindTopNAggregation,
	}, kinds)
	assert.Equal(t, "group/sw_metric", d.Actions[0].ID())
}

func TestPrepareForWrite(t *testing.T) {
	m := testMeasure("g", "m")
	out := PrepareForWrite(m).(*databasev1.Measure)
	assert.Nil(t, out.GetCreatedAt())
	assert.Nil(t, out.GetUpdatedAt())
	assert.Zero(t, out.GetMetadata().GetCreateRevision())
	assert.Zero(t, out.GetMetadata().GetModRevision())
	assert.Equal(t, uint32(1), out.GetMetadata().GetId(), "the id is kept")
	assert.NotNil(t, m.GetCreatedAt(), "the input is not changed")

	rule := artifactSchemas().IndexRules[ResourceKey{"sw_metric", "tier"}]
	rule.Metadata.Id = 7
	assert.Zero(t, PrepareForWrite(rule).(*databasev1.IndexRule).GetMetadata().GetId(),
		"an index rule id is left to the registry, which keeps the target's on an update")
	assert.Equal(t, uint32(7), rule.GetMetadata().GetId(), "the input is not changed")
}

func TestIndexRuleWithoutIDIsValid(t *testing.T) {
	art := artifactSchemas()
	art.IndexRules[ResourceKey{"sw_metric", "tier"}].Metadata.Id = 0
	a := actionOf(t, DiffSchemas(art, NewTargetState(), defaultOpts()), "indexRule/sw_metric/tier")
	assert.Equal(t, DispositionCreate, a.Disposition, a.Reason)
	assert.Zero(t, art.IndexRules[ResourceKey{"sw_metric", "tier"}].GetMetadata().GetId(), "the artifact spec is not changed")
}

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		kind, path string
		want       DiffClass
	}{
		{schemasync.KindGroup, "catalog", ClassIdentity},
		{schemasync.KindMeasure, "metadata.name", ClassIdentity},
		{schemasync.KindIndexRuleBinding, "subject.name", ClassIdentity},
		{schemasync.KindTopNAggregation, "source_measure.name", ClassIdentity},
		{schemasync.KindGroup, "resource_opts.shard_num", ClassTopology},
		{schemasync.KindGroup, "resource_opts.segment_interval.num", ClassTopology},
		{schemasync.KindGroup, "resource_opts.ttl.num", ClassTopology},
		{schemasync.KindGroup, "resource_opts.stages.ttl", ClassTopology},
		{schemasync.KindGroup, "resource_opts.default_stages", ClassTopology},
		{schemasync.KindGroup, "resource_opts.replicas", ClassTopology},
		{schemasync.KindMeasure, "tag_families.tags.type", ClassShape},
		{schemasync.KindMeasure, "interval", ClassShape},
		{schemasync.KindStream, "entity.tag_names", ClassShape},
		{schemasync.KindTrace, "timestamp_tag_name", ClassShape},
		{schemasync.KindIndexRule, "type", ClassShape},
		{schemasync.KindIndexRule, "analyzer", ClassShape},
		{schemasync.KindIndexRule, "no_sort", ClassShape},
		{schemasync.KindIndexRuleBinding, "rules", ClassShape},
		{schemasync.KindProperty, "tags.type", ClassShape},
		{schemasync.KindGroup, "pipeline.enabled", ClassBehavior},
		{schemasync.KindTopNAggregation, "counters_number", ClassBehavior},
		{schemasync.KindTopNAggregation, "lru_size", ClassBehavior},
		{schemasync.KindIndexRuleBinding, "expire_at.seconds", ClassBehavior},
		{schemasync.KindGroup, "resource_opts.something_new", ClassUnclassified},
		{schemasync.KindStream, "something_new", ClassUnclassified},
	} {
		assert.Equal(t, tc.want, Classify(tc.kind, tc.path), "%s %s", tc.kind, tc.path)
	}
}

func TestDiffFieldsPaths(t *testing.T) {
	a, b := testMeasure("g", "m"), testMeasure("g", "m")
	b.TagFamilies[0].Tags[1].Type = databasev1.TagType_TAG_TYPE_INT
	b.TagFamilies[0].Tags = append(b.TagFamilies[0].Tags, &databasev1.TagSpec{Name: "extra", Type: databasev1.TagType_TAG_TYPE_STRING})
	diffs := DiffFields(schemasync.KindMeasure, a, b)
	require.Len(t, diffs, 2)
	assert.Equal(t, FieldDiff{Path: "tagFamilies[default].tags[tier].type", Class: ClassShape, Artifact: "TAG_TYPE_STRING", Target: "TAG_TYPE_INT"}, diffs[0])
	assert.Equal(t, "tagFamilies[default].tags[extra]", diffs[1].Path)
	assert.Equal(t, noneValue, diffs[1].Artifact)
	assert.Equal(t, `{name:"extra" type:TAG_TYPE_STRING}`, diffs[1].Target)

	g1, g2 := testGroup("g", commonv1.Catalog_CATALOG_MEASURE), testGroup("g", commonv1.Catalog_CATALOG_MEASURE)
	g2.ResourceOpts.ShardNum = 4
	diffs = DiffFields(schemasync.KindGroup, g1, g2)
	require.Len(t, diffs, 1)
	assert.Equal(t, FieldDiff{Path: "resourceOpts.shardNum", Class: ClassTopology, Artifact: "2", Target: "4"}, diffs[0])
}

// conflictCase changes the target copy of one resource and expects a disposition.
type conflictCase struct {
	mutate  func(art *Schemas, target *TargetState)
	opts    func(o *SchemaOptions)
	name    string
	id      string
	reason  string
	want    Disposition
	diffLen int
}

// diffMutated diffs the artifact schemas against a mirrored target after mutate changes them.
func diffMutated(mutate func(*Schemas, *TargetState), opts func(*SchemaOptions)) *SchemaDiff {
	art := artifactSchemas()
	target := mirrorTarget(art)
	mutate(art, target)
	o := defaultOpts()
	if opts != nil {
		opts(&o)
	}
	return DiffSchemas(art, target, o)
}

func runConflictCases(t *testing.T, cases []conflictCase) {
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := actionOf(t, diffMutated(tc.mutate, tc.opts), tc.id)
			assert.Equal(t, tc.want, a.Disposition, a.Reason)
			assert.Contains(t, a.Reason, tc.reason)
			if tc.diffLen > 0 {
				assert.Len(t, a.Diffs, tc.diffLen)
			}
			assert.Equal(t, tc.id, a.Match)
		})
	}
}

func shardNum4(_ *Schemas, target *TargetState) { target.Groups["sw_metric"].ResourceOpts.ShardNum = 4 }

func TestPolicies(t *testing.T) {
	runConflictCases(t, []conflictCase{
		{name: "error by default", id: "group/sw_metric", mutate: shardNum4, want: DispositionConflict, reason: "policy=error (default)", diffLen: 1},
		{
			name: "ignore a topology difference", id: "group/sw_metric", mutate: shardNum4, want: DispositionIgnore,
			opts: func(o *SchemaOptions) { o.Default = config.ConflictPolicyIgnore }, reason: "policy=ignore (default)",
		},
		{
			name: "overwrite an empty group", id: "group/sw_metric", mutate: shardNum4, want: DispositionOverwrite,
			opts: func(o *SchemaOptions) { o.Default = config.ConflictPolicyOverwrite }, reason: "policy=overwrite (default)",
		},
		{
			name: "first matching rule wins", id: "measure/sw_metric/cpm", want: DispositionOverwrite,
			mutate: func(_ *Schemas, target *TargetState) {
				target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags = target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags[:1]
			},
			opts: func(o *SchemaOptions) {
				o.Rules = []config.ConflictRule{
					{Match: "group/sw_metric", Policy: config.ConflictPolicyIgnore},
					{Match: "measure/sw_metric/cpm", Policy: config.ConflictPolicyOverwrite},
					{Match: "measure/sw_metric", Policy: config.ConflictPolicyIgnore},
				}
			},
			reason: "rules[1] match: measure/sw_metric/cpm",
		},
		{
			name: "a two-segment rule matches every resource of the kind", id: "measure/sw_metric/cpm", want: DispositionIgnore,
			mutate: func(_ *Schemas, target *TargetState) {
				target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags = target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags[:1]
			},
			opts: func(o *SchemaOptions) {
				o.Rules = []config.ConflictRule{{Match: "measure/sw_metric", Policy: config.ConflictPolicyIgnore}}
			},
			reason: "rules[0] match: measure/sw_metric",
		},
		{
			name: "behavior difference", id: "topNAggregation/sw_metric/top", want: DispositionConflict, reason: "policy=error",
			mutate: func(_ *Schemas, target *TargetState) {
				target.TopNs[ResourceKey{"sw_metric", "top"}].CountersNumber = 7
			},
		},
	})
}

func TestGuards(t *testing.T) {
	overwrite := func(o *SchemaOptions) { o.Default = config.ConflictPolicyOverwrite }
	runConflictCases(t, []conflictCase{
		{
			name: "identity", id: "indexRuleBinding/sw_metric/cpm", want: DispositionConflict, reason: "an identity field differs", opts: overwrite,
			mutate: func(_ *Schemas, target *TargetState) {
				target.IndexRuleBindings[ResourceKey{"sw_metric", "cpm"}].Subject.Name = "other"
			},
		},
		{
			name: "group catalog", id: "group/sw_metric", want: DispositionConflict, reason: "an identity field differs", opts: overwrite,
			mutate: func(_ *Schemas, target *TargetState) {
				target.Groups["sw_metric"].Catalog = commonv1.Catalog_CATALOG_STREAM
			},
		},
		{
			name: "non-empty group", id: "group/sw_metric", want: DispositionConflict, reason: "holds data", opts: overwrite,
			mutate: func(art *Schemas, target *TargetState) {
				shardNum4(art, target)
				target.NonEmptyGroups["sw_metric"] = NonEmptyState{Known: true, NonEmpty: true}
			},
		},
		{
			name: "unknown state fails closed", id: "group/sw_metric", want: DispositionConflict, reason: "Plan is not available", opts: overwrite,
			mutate: func(art *Schemas, target *TargetState) {
				shardNum4(art, target)
				target.NonEmptyGroups["sw_metric"] = NonEmptyState{Reason: "Plan is not available"}
			},
		},
		{
			name: "missing state fails closed", id: "group/sw_metric", want: DispositionConflict, reason: "no Plan of it", opts: overwrite,
			mutate: func(art *Schemas, target *TargetState) {
				shardNum4(art, target)
				delete(target.NonEmptyGroups, "sw_metric")
			},
		},
		{
			name: "passes with --force", id: "group/sw_metric", want: DispositionOverwrite,
			opts: func(o *SchemaOptions) { o.Default, o.Force = config.ConflictPolicyOverwrite, true },
			mutate: func(art *Schemas, target *TargetState) {
				shardNum4(art, target)
				target.NonEmptyGroups["sw_metric"] = NonEmptyState{Known: true, NonEmpty: true}
			},
		},
		{
			name: "interval", id: "measure/sw_metric/cpm", want: DispositionConflict, reason: "measure index_mode or interval differs", opts: overwrite,
			mutate: func(_ *Schemas, target *TargetState) {
				target.Measures[ResourceKey{"sw_metric", "cpm"}].Interval = "5m"
			},
		},
		{
			name: "index mode under ignore", id: "measure/sw_metric/cpm", want: DispositionConflict, reason: "measure index_mode or interval differs",
			opts: func(o *SchemaOptions) { o.Default = config.ConflictPolicyIgnore },
			mutate: func(_ *Schemas, target *TargetState) {
				target.Measures[ResourceKey{"sw_metric", "cpm"}].IndexMode = true
			},
		},
		{
			name: "stream entity", id: "stream/sw_stream/log", want: DispositionConflict, opts: overwrite,
			reason: "the registry would reject the update: entity is different",
			mutate: func(_ *Schemas, target *TargetState) {
				target.Streams[ResourceKey{"sw_stream", "log"}].Entity.TagNames = []string{"svc", "latency"}
			},
		},
		{
			name: "deleted entity tag", id: "measure/sw_metric/cpm", want: DispositionConflict, reason: "cannot delete entity tag entity_id", opts: overwrite,
			mutate: func(art *Schemas, _ *TargetState) {
				m := art.Measures[ResourceKey{"sw_metric", "cpm"}]
				m.TagFamilies[0].Tags = m.TagFamilies[0].Tags[1:]
			},
		},
		{
			name: "deleted entity family", id: "measure/sw_metric/cpm", want: DispositionConflict, reason: "cannot delete tag family default", opts: overwrite,
			mutate: func(art *Schemas, _ *TargetState) {
				art.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Name = "renamed"
			},
		},
		{
			name: "field spec", id: "measure/sw_metric/cpm", want: DispositionConflict, reason: "field value is different", opts: overwrite,
			mutate: func(_ *Schemas, target *TargetState) {
				target.Measures[ResourceKey{"sw_metric", "cpm"}].Fields[0].CompressionMethod = databasev1.CompressionMethod_COMPRESSION_METHOD_UNSPECIFIED
			},
		},
		{
			name: "trace reserved tag", id: "trace/sw_trace/segment", want: DispositionConflict, reason: "cannot delete reserved tag ts", opts: overwrite,
			mutate: func(art *Schemas, _ *TargetState) {
				tr := art.Traces[ResourceKey{"sw_trace", "segment"}]
				tr.Tags = tr.Tags[:2]
			},
		},
		{
			name: "ignore with a type change", id: "measure/sw_metric/cpm", want: DispositionConflict,
			reason: "column default.tier is TAG_TYPE_STRING in the artifact and TAG_TYPE_INT",
			opts:   func(o *SchemaOptions) { o.Default = config.ConflictPolicyIgnore },
			mutate: func(_ *Schemas, target *TargetState) {
				target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags[1].Type = databasev1.TagType_TAG_TYPE_INT
			},
		},
	})
}

func TestIgnoreNotesDroppedAndFilledColumns(t *testing.T) {
	art := artifactSchemas()
	target := mirrorTarget(art)
	tm := target.Measures[ResourceKey{"sw_metric", "cpm"}]
	tm.TagFamilies[0].Tags[1] = &databasev1.TagSpec{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING}
	a := actionOf(t, DiffSchemas(art, target, SchemaOptions{IncludeSchema: true, Default: config.ConflictPolicyIgnore}), "measure/sw_metric/cpm")
	require.Equal(t, DispositionIgnore, a.Disposition)
	assert.Equal(t, []string{
		"column default.region: only on the target, filled with \\N",
		"column default.tier: only in the artifact, dropped",
	}, a.Notes)
}

func TestSamplerPipelineUpdateRejection(t *testing.T) {
	art := artifactSchemas()
	target := mirrorTarget(art)
	g := proto.Clone(art.Groups["sw_trace"]).(*commonv1.Group)
	g.Pipeline = samplerPipeline()
	r := resource{kind: schemasync.KindGroup, group: "sw_trace", artifact: g, target: target.Groups["sw_trace"]}
	target.Traces[ResourceKey{"sw_other", "other"}] = testTrace("sw_other", "other")
	assert.Empty(t, updateRejection(r, art, &target.Schemas, nil), "one trace is allowed; other groups do not count")
	target.Traces[ResourceKey{"sw_trace", "other"}] = testTrace("sw_trace", "other")
	assert.Contains(t, updateRejection(r, art, &target.Schemas, nil), "has an enabled sampler pipeline but would hold 2 traces (other, segment)")
}

func samplerPipeline() *commonv1.TracePipelineConfig {
	return &commonv1.TracePipelineConfig{Enabled: true, Plugins: []*commonv1.Plugin{
		{Name: "s", Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{Path: "s.so", AbiVersion: sdk.ABIVersion}}},
	}}
}

func TestSamplerPipelineTraceWrites(t *testing.T) {
	other := ResourceKey{"sw_trace", "other"}
	segment := ResourceKey{"sw_trace", "segment"}
	for _, tc := range []struct {
		mutate func(art *Schemas, target *TargetState)
		opts   func(o *SchemaOptions)
		want   map[string]Disposition
		name   string
	}{
		{
			name: "create beside an existing trace of an unchanged sampler group",
			mutate: func(art *Schemas, target *TargetState) {
				art.Groups["sw_trace"].Pipeline = samplerPipeline()
				target.Groups["sw_trace"].Pipeline = samplerPipeline()
				art.Traces[other] = testTrace("sw_trace", "other")
				delete(art.Traces, segment)
			},
			want: map[string]Disposition{"group/sw_trace": DispositionOK, "trace/sw_trace/other": DispositionConflict},
		},
		{
			name: "create into an ignored group whose target pipeline has a sampler",
			mutate: func(art *Schemas, target *TargetState) {
				target.Groups["sw_trace"].Pipeline = samplerPipeline()
				art.Traces[other] = testTrace("sw_trace", "other")
			},
			opts: func(o *SchemaOptions) {
				o.Rules = []config.ConflictRule{{Match: "group/sw_trace", Policy: config.ConflictPolicyIgnore}}
			},
			want: map[string]Disposition{
				"group/sw_trace": DispositionIgnore, "trace/sw_trace/other": DispositionConflict, "trace/sw_trace/segment": DispositionOK,
			},
		},
		{
			name: "overwrite in a sampler group that gains a second trace",
			mutate: func(art *Schemas, target *TargetState) {
				art.Groups["sw_trace"].Pipeline = samplerPipeline()
				target.Groups["sw_trace"].Pipeline = samplerPipeline()
				art.Traces[other] = testTrace("sw_trace", "other")
				art.Traces[segment].Tags = append(art.Traces[segment].Tags, &databasev1.TraceTagSpec{Name: "extra", Type: databasev1.TagType_TAG_TYPE_STRING})
			},
			opts: func(o *SchemaOptions) { o.Default = config.ConflictPolicyOverwrite },
			want: map[string]Disposition{"trace/sw_trace/other": DispositionConflict, "trace/sw_trace/segment": DispositionConflict},
		},
		{
			name: "a created sampler group with two traces",
			mutate: func(art *Schemas, target *TargetState) {
				art.Groups["sw_trace"].Pipeline = samplerPipeline()
				art.Traces[other] = testTrace("sw_trace", "other")
				delete(target.Groups, "sw_trace")
				delete(target.Traces, segment)
			},
			want: map[string]Disposition{
				"group/sw_trace": DispositionCreate, "trace/sw_trace/other": DispositionConflict, "trace/sw_trace/segment": DispositionConflict,
			},
		},
		{
			name: "an overwrite that removes the sampler allows the second trace",
			mutate: func(art *Schemas, target *TargetState) {
				target.Groups["sw_trace"].Pipeline = samplerPipeline()
				art.Traces[other] = testTrace("sw_trace", "other")
			},
			opts: func(o *SchemaOptions) { o.Default = config.ConflictPolicyOverwrite },
			want: map[string]Disposition{"group/sw_trace": DispositionOverwrite, "trace/sw_trace/other": DispositionCreate},
		},
		{
			name: "the same single trace stays allowed",
			mutate: func(art *Schemas, target *TargetState) {
				art.Groups["sw_trace"].Pipeline = samplerPipeline()
				target.Groups["sw_trace"].Pipeline = samplerPipeline()
				art.Traces[segment].Tags = append(art.Traces[segment].Tags, &databasev1.TraceTagSpec{Name: "extra", Type: databasev1.TagType_TAG_TYPE_STRING})
			},
			opts: func(o *SchemaOptions) { o.Default = config.ConflictPolicyOverwrite },
			want: map[string]Disposition{"group/sw_trace": DispositionOK, "trace/sw_trace/segment": DispositionOverwrite},
		},
		{
			name: "a single created trace in a sampler group",
			mutate: func(art *Schemas, target *TargetState) {
				art.Groups["sw_trace"].Pipeline = samplerPipeline()
				target.Groups["sw_trace"].Pipeline = samplerPipeline()
				delete(target.Traces, segment)
			},
			want: map[string]Disposition{"trace/sw_trace/segment": DispositionCreate},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := diffMutated(tc.mutate, tc.opts)
			for id, want := range tc.want {
				a := actionOf(t, d, id)
				assert.Equal(t, want, a.Disposition, "%s: %s", id, a.Reason)
				if a.Kind == schemasync.KindTrace && want == DispositionConflict {
					assert.Contains(t, a.Reason, "the registry would reject the")
					assert.Contains(t, a.Reason, "has an enabled sampler pipeline but would hold 2 traces")
					for _, w := range d.Writes() {
						assert.NotEqual(t, id, w.ID(), "a rejected trace is not written")
					}
				}
			}
		})
	}
}

func TestInvalidSpecUnderIgnoreIsNotChecked(t *testing.T) {
	const id = "stream/sw_stream/log"
	art := artifactSchemas()
	target := mirrorTarget(art)
	s := art.Streams[ResourceKey{"sw_stream", "log"}]
	s.TagFamilies[0].Tags = append(s.TagFamilies[0].Tags, s.TagFamilies[0].Tags[0])
	d := DiffSchemas(art, target, SchemaOptions{IncludeSchema: true, Default: config.ConflictPolicyIgnore})
	a := actionOf(t, d, id)
	assert.Equal(t, DispositionIgnore, a.Disposition, a.Reason)
	assert.False(t, (&Plan{Schema: d.Actions}).SchemaBlocked())

	d = DiffSchemas(art, target, SchemaOptions{IncludeSchema: true, Default: config.ConflictPolicyOverwrite})
	assert.Equal(t, DispositionInvalid, actionOf(t, d, id).Disposition)
}

func TestTopologyOnlyConflictSuggestsIgnore(t *testing.T) {
	art := artifactSchemas()
	target := mirrorTarget(art)
	shardNum4(art, target)
	a := actionOf(t, DiffSchemas(art, target, defaultOpts()), "group/sw_metric")
	assert.Equal(t, DispositionConflict, a.Disposition)
	assert.Contains(t, a.Reason, "set policy: ignore")

	target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags[1].Type = databasev1.TagType_TAG_TYPE_INT
	a = actionOf(t, DiffSchemas(art, target, defaultOpts()), "measure/sw_metric/cpm")
	assert.Equal(t, DispositionConflict, a.Disposition)
	assert.NotContains(t, a.Reason, "ignore", "a shape difference never suggests ignore")
}

func TestInvalidForEveryKind(t *testing.T) {
	for _, tc := range []struct {
		mutate func(s *Schemas)
		id     string
	}{
		{id: "group/sw_metric", mutate: func(s *Schemas) { s.Groups["sw_metric"].ResourceOpts.ShardNum = 0 }},
		{id: "stream/sw_stream/log", mutate: func(s *Schemas) { s.Streams[ResourceKey{"sw_stream", "log"}].Entity = nil }},
		{id: "measure/sw_metric/cpm", mutate: func(s *Schemas) { s.Measures[ResourceKey{"sw_metric", "cpm"}].Interval = "abc" }},
		{id: "trace/sw_trace/segment", mutate: func(s *Schemas) { s.Traces[ResourceKey{"sw_trace", "segment"}].TraceIdTagName = "" }},
		{id: "property/sw_prop/ui", mutate: func(s *Schemas) {
			s.Properties[ResourceKey{"sw_prop", "ui"}].Tags[0].Type = databasev1.TagType_TAG_TYPE_UNSPECIFIED
		}},
		{id: "indexRule/sw_metric/tier", mutate: func(s *Schemas) { s.IndexRules[ResourceKey{"sw_metric", "tier"}].Tags = nil }},
		{id: "indexRuleBinding/sw_metric/cpm", mutate: func(s *Schemas) { s.IndexRuleBindings[ResourceKey{"sw_metric", "cpm"}].Rules = nil }},
		{id: "topNAggregation/sw_metric/top", mutate: func(s *Schemas) { s.TopNs[ResourceKey{"sw_metric", "top"}].CountersNumber = 0 }},
		{id: "stream/sw_metric/misplaced", mutate: func(s *Schemas) {
			s.Streams[ResourceKey{"sw_metric", "misplaced"}] = testStream("sw_metric", "misplaced")
		}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			art := artifactSchemas()
			tc.mutate(art)
			d := DiffSchemas(art, NewTargetState(), defaultOpts())
			a := actionOf(t, d, tc.id)
			assert.Equal(t, DispositionInvalid, a.Disposition, a.Reason)
			assert.True(t, (&Plan{Schema: d.Actions}).SchemaBlocked())
			for _, w := range d.Writes() {
				assert.NotEqual(t, tc.id, w.ID())
			}
		})
	}
}

func TestIncludeSchemaFalse(t *testing.T) {
	art := artifactSchemas()
	target := mirrorTarget(art)
	delete(target.Streams, ResourceKey{"sw_stream", "log"})
	target.Measures[ResourceKey{"sw_metric", "cpm"}].Interval = "5m"
	d := DiffSchemas(art, target, SchemaOptions{})
	assert.Equal(t, DispositionMissing, actionOf(t, d, "stream/sw_stream/log").Disposition)
	cpm := actionOf(t, d, "measure/sw_metric/cpm")
	assert.Equal(t, DispositionOK, cpm.Disposition)
	assert.Contains(t, cpm.Reason, "not applied")
	assert.Empty(t, cpm.Notes)
	assert.Empty(t, d.Writes())
	assert.True(t, (&Plan{Schema: d.Actions}).SchemaBlocked())
}

func TestIncludeSchemaFalseNotesDroppedAndFilledColumns(t *testing.T) {
	art := artifactSchemas()
	target := mirrorTarget(art)
	target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags[1] = &databasev1.TagSpec{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING}
	a := actionOf(t, DiffSchemas(art, target, SchemaOptions{}), "measure/sw_metric/cpm")
	require.Equal(t, DispositionOK, a.Disposition)
	assert.Equal(t, []string{
		"column default.region: only on the target, filled with \\N",
		"column default.tier: only in the artifact, dropped",
	}, a.Notes)
	assert.Contains(t, schemaProblems([]SchemaAction{a}),
		"warning: measure/sw_metric/cpm is written under the target schema: "+
			"column default.region: only on the target, filled with \\N; column default.tier: only in the artifact, dropped")
}

func TestIncludeSchemaFalseRefusesDifferentRowShape(t *testing.T) {
	art := artifactSchemas()
	target := mirrorTarget(art)
	target.Measures[ResourceKey{"sw_metric", "cpm"}].TagFamilies[0].Tags[1].Type = databasev1.TagType_TAG_TYPE_INT
	a := actionOf(t, DiffSchemas(art, target, SchemaOptions{}), "measure/sw_metric/cpm")
	assert.Equal(t, DispositionConflict, a.Disposition)
	assert.Contains(t, a.Reason, "--include-schema=false, but column")

	target = mirrorTarget(art)
	target.Measures[ResourceKey{"sw_metric", "cpm"}].IndexMode = !art.Measures[ResourceKey{"sw_metric", "cpm"}].GetIndexMode()
	a = actionOf(t, DiffSchemas(art, target, SchemaOptions{}), "measure/sw_metric/cpm")
	assert.Equal(t, DispositionConflict, a.Disposition)
	assert.Contains(t, a.Reason, "index_mode")
}

func TestTopNResultIsOKOrConflicts(t *testing.T) {
	key := ResourceKey{"sw_metric", csvfmt.TopNResultMeasure}
	art := artifactSchemas()
	art.Measures[key] = testTopNResult("sw_metric")

	d := DiffSchemas(art, NewTargetState(), defaultOpts())
	a := actionOf(t, d, "measure/sw_metric/_top_n_result")
	assert.Equal(t, DispositionOK, a.Disposition)
	assert.Empty(t, a.Reason, "a compatible _top_n_result is not listed")
	for _, w := range d.Writes() {
		assert.NotEqual(t, csvfmt.TopNResultMeasure, w.Name, "never created by the import")
	}

	target := mirrorTarget(art)
	target.Measures[key].Interval = "1m" // other spec differences do not matter
	assert.Equal(t, DispositionOK, actionOf(t, DiffSchemas(art, target, defaultOpts()), a.ID()).Disposition)

	target.Measures[key].Fields[0].FieldType = databasev1.FieldType_FIELD_TYPE_INT
	a = actionOf(t, DiffSchemas(art, target, defaultOpts()), a.ID())
	assert.Equal(t, DispositionConflict, a.Disposition)
	assert.Equal(t, []FieldDiff{{Path: "_field.value", Class: ClassShape, Artifact: "FIELD_TYPE_DATA_BINARY", Target: "FIELD_TYPE_INT"}}, a.Diffs)
}

func TestPredictState(t *testing.T) {
	art := artifactSchemas()
	target := NewTargetState()
	target.Groups["sw_stream"] = restamp(art.Groups["sw_stream"]).(*commonv1.Group)
	target.NonEmptyGroups["sw_stream"] = NonEmptyState{Known: true, NonEmpty: true}
	d := DiffSchemas(art, target, defaultOpts())
	pred := PredictState(target, d.Actions)
	assert.Same(t, art.Groups["sw_metric"], pred.Groups["sw_metric"], "a created group takes the artifact spec")
	assert.Same(t, target.Groups["sw_stream"], pred.Groups["sw_stream"])
	assert.Equal(t, NonEmptyState{Known: true}, pred.NonEmptyGroups["sw_metric"])
	assert.True(t, pred.NonEmptyGroups["sw_stream"].NonEmpty)
	assert.Contains(t, pred.Measures, ResourceKey{"sw_metric", "cpm"})
	assert.NotContains(t, target.Measures, ResourceKey{"sw_metric", "cpm"}, "the input target is not changed")
}

func TestActionIDs(t *testing.T) {
	assert.Equal(t, "group/g", (&SchemaAction{Kind: schemasync.KindGroup, Group: "g"}).ID())
	assert.Equal(t, "measure/g/m", (&SchemaAction{Kind: schemasync.KindMeasure, Group: "g", Name: "m"}).ID())
	assert.True(t, strings.HasPrefix(string(ClassUnclassified), "shape"))
}
