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
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
)

func TestMeasureDependenciesReplacementDeletionAndRecreation(t *testing.T) {
	repo := newEmptyEntityRepo()
	source := &entityRepoSchemaSource{measures: repo}
	requested := []bydbql.MeasureSource{{Group: "g", Name: "m"}, {Group: "other", Name: "m"}, {Group: "g", Name: "m"}}
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", 10)))
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("other", "m", 20)))
	dependencies, snapshots, ok := source.captureMeasureDependencies(requested)
	require.True(t, ok)
	require.Len(t, dependencies, 2)
	require.Len(t, snapshots, 2)
	require.True(t, source.validateMeasureDependencies(dependencies))
	local, found := source.Measure("g", "m")
	require.True(t, found)
	require.Same(t, local, snapshots[requested[0]], "capture borrows immutable watch values without cloning their payloads")

	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("unrelated", "m", 100)))
	require.True(t, source.validateMeasureDependencies(dependencies))
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("other", "m", 20)))
	require.False(t, source.validateMeasureDependencies(dependencies), "same revision replacement must invalidate")
	dependencies, _, ok = source.captureMeasureDependencies(requested)
	require.True(t, ok)
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("other", "m", 5)))
	require.False(t, source.validateMeasureDependencies(dependencies), "lower revision replacement must invalidate")
	dependencies, _, ok = source.captureMeasureDependencies(requested)
	require.True(t, ok)
	repo.OnDelete(measureAddEvent(minimalMeasure("other", "m", 5)))
	require.False(t, source.validateMeasureDependencies(dependencies))
	_, _, ok = source.captureMeasureDependencies(requested)
	require.False(t, ok)
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("other", "m", 5)))
	require.False(t, source.validateMeasureDependencies(dependencies), "delete and recreate at the same revision must invalidate")
}

func TestMeasureDependenciesRequireCoherentQueryGateRevision(t *testing.T) {
	repo := newEmptyEntityRepo()
	revisions := newFakeRevisionRepo()
	nodeRegistry := registry.NewNodeRepoRegistry()
	nodeRegistry.Register(schema.KindMeasure, revisions)
	source := &entityRepoSchemaSource{measures: repo, revisions: nodeRegistry}
	requested := []bydbql.MeasureSource{{Group: "g", Name: "m"}}
	_, _, ok := source.captureMeasureDependencies(requested)
	require.False(t, ok, "local miss may execute through registry fallback but cannot be cached")
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", 10)))
	_, _, ok = source.captureMeasureDependencies(requested)
	require.False(t, ok, "tracked registry has not observed the resource")
	revisions.set(schema.KindMeasure, "g", "m", 11)
	_, _, ok = source.captureMeasureDependencies(requested)
	require.False(t, ok, "registry must not label an older schema as current")
	revisions.set(schema.KindMeasure, "g", "m", 10)
	dependencies, _, ok := source.captureMeasureDependencies(requested)
	require.True(t, ok)
	revisions.set(schema.KindMeasure, "g", "m", 9)
	require.False(t, source.validateMeasureDependencies(dependencies))
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", 0)))
	revisions.set(schema.KindMeasure, "g", "m", 0)
	_, _, ok = source.captureMeasureDependencies(requested)
	require.False(t, ok, "zero revision cannot certify a reusable plan")
}

func TestMeasureDependenciesConcurrentWatchSnapshots(t *testing.T) {
	repo := newEmptyEntityRepo()
	source := &entityRepoSchemaSource{measures: repo}
	requested := []bydbql.MeasureSource{{Group: "g", Name: "m"}}
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", 10)))
	var updates sync.WaitGroup
	updates.Add(1)
	run.Go(t.Context(), "ql-schema-watch-test", logger.GetLogger("test"), func(context.Context) {
		defer updates.Done()
		for i := 0; i < 200; i++ {
			if i%5 == 0 {
				repo.OnDelete(measureAddEvent(minimalMeasure("g", "m", 10)))
			}
			repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", int64(10+i%2))))
		}
	})
	for i := 0; i < 200; i++ {
		dependencies, snapshots, ok := source.captureMeasureDependencies(requested)
		if !ok {
			continue
		}
		require.Len(t, dependencies, 1)
		require.Equal(t, snapshots[requested[0]].Metadata.ModRevision, dependencies[0].revision)
		require.NotZero(t, dependencies[0].generation)
	}
	updates.Wait()
	dependencies, _, ok := source.captureMeasureDependencies(requested)
	require.True(t, ok)
	require.True(t, source.validateMeasureDependencies(dependencies))
}

func TestMeasureDependenciesReadOnlySnapshotSurvivesReplacement(t *testing.T) {
	repo := newEmptyEntityRepo()
	source := &entityRepoSchemaSource{measures: repo}
	requested := []bydbql.MeasureSource{{Group: "g", Name: "watched"}}
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "watched", 10)))
	dependencies, snapshots, ok := source.captureMeasureDependencies(requested)
	require.True(t, ok)
	replacement := minimalMeasure("g", "watched", 11)
	replacement.TagFamilies[0].Name = "replacement"
	repo.OnAddOrUpdate(measureAddEvent(replacement))
	require.Equal(t, "default", snapshots[requested[0]].TagFamilies[0].Name)
	require.EqualValues(t, 10, snapshots[requested[0]].Metadata.ModRevision)
	current, found := source.Measure("g", "watched")
	require.True(t, found)
	require.Equal(t, "replacement", current.TagFamilies[0].Name)
	require.False(t, source.validateMeasureDependencies(dependencies))
}
