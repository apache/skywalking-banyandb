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

package grpc

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
)

// entityRepoSchemaSource serves BydbQL schema lookups from the liaison's watch-fed
// entity repositories, the same view the native query handlers already read for
// gating and time-range clamping. Each kind has its own repository because each
// discovery service registers a separate handler.
type entityRepoSchemaSource struct {
	measures  *entityRepo
	streams   *entityRepo
	traces    *entityRepo
	revisions *registry.NodeRepoRegistry
}

var _ bydbql.SchemaSource = (*entityRepoSchemaSource)(nil)

func (s *entityRepoSchemaSource) Measure(group, name string) (*databasev1.Measure, bool) {
	return s.measures.getMeasure(identity{name: name, group: group})
}

func (s *entityRepoSchemaSource) Stream(group, name string) (*databasev1.Stream, bool) {
	return s.streams.getStream(identity{name: name, group: group})
}

func (s *entityRepoSchemaSource) Trace(group, name string) (*databasev1.Trace, bool) {
	return s.traces.getTrace(identity{name: name, group: group})
}

// measurePlanDependency describes the local schema view used by one compilation.
// Revisions alone are insufficient: same-revision replacements and deletion /
// recreation must invalidate a plan too.
type measurePlanDependency struct {
	source     bydbql.MeasureSource
	revision   int64
	generation uint64
}

func (s *entityRepoSchemaSource) captureMeasureDependencies(
	sources []bydbql.MeasureSource,
) ([]measurePlanDependency, map[bydbql.MeasureSource]*databasev1.Measure, bool) {
	if s == nil || s.measures == nil || len(sources) == 0 {
		return nil, nil, false
	}
	dependencies, snapshots, ok := s.captureLocalMeasures(sources)
	if !ok || !s.validateMeasureDependencies(dependencies) {
		return nil, nil, false
	}
	return dependencies, snapshots, true
}

// captureLocalMeasures borrows read-only watch values. entityRepo replaces whole
// schema pointers; an update never mutates a previously published value. The
// compiler clones these schemas only after acquiring its compile reservation.
func (s *entityRepoSchemaSource) captureLocalMeasures(
	sources []bydbql.MeasureSource,
) ([]measurePlanDependency, map[bydbql.MeasureSource]*databasev1.Measure, bool) {
	repo := s.measures
	repo.RLock()
	defer repo.RUnlock()
	dependencies := make([]measurePlanDependency, 0, len(sources))
	snapshots := make(map[bydbql.MeasureSource]*databasev1.Measure, len(sources))
	for _, source := range sources {
		if _, captured := snapshots[source]; captured {
			continue
		}
		id := identity{group: source.Group, name: source.Name}
		measure, found := repo.measureMap[id]
		locator, located := repo.entitiesMap[id]
		generation := repo.measureGenerations[id]
		revision := measure.GetMetadata().GetModRevision()
		if !found || !located || measure.GetMetadata().GetGroup() != source.Group || measure.GetMetadata().GetName() != source.Name ||
			revision <= 0 || generation == 0 || locator.ModRevision != revision {
			return nil, nil, false
		}
		dependencies = append(dependencies, measurePlanDependency{source: source, revision: revision, generation: generation})
		snapshots[source] = measure
	}
	return dependencies, snapshots, true
}

func (s *entityRepoSchemaSource) validateMeasureDependencies(dependencies []measurePlanDependency) bool {
	if s == nil || s.measures == nil || len(dependencies) == 0 || !s.localMeasureDependenciesCurrent(dependencies) {
		return false
	}
	// Do not hold the entity repository lock while calling another repository.
	// The final generation check closes a replacement during revision resolution.
	for _, dependency := range dependencies {
		revision, found := resolveQueryGateRevision(s.revisions, schema.KindMeasure,
			dependency.source.Group, dependency.source.Name, dependency.revision, true)
		if !found || revision != dependency.revision {
			return false
		}
	}
	return s.localMeasureDependenciesCurrent(dependencies)
}

func (s *entityRepoSchemaSource) localMeasureDependenciesCurrent(dependencies []measurePlanDependency) bool {
	repo := s.measures
	repo.RLock()
	defer repo.RUnlock()
	for _, dependency := range dependencies {
		id := identity{group: dependency.source.Group, name: dependency.source.Name}
		measure, found := repo.measureMap[id]
		locator, located := repo.entitiesMap[id]
		if !found || !located || dependency.revision <= 0 || dependency.generation == 0 ||
			repo.measureGenerations[id] != dependency.generation || locator.ModRevision != dependency.revision ||
			measure.GetMetadata().GetModRevision() != dependency.revision || measure.GetMetadata().GetGroup() != dependency.source.Group ||
			measure.GetMetadata().GetName() != dependency.source.Name {
			return false
		}
	}
	return true
}
