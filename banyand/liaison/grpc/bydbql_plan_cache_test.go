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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func planCacheFixture(t *testing.T, cfg planCacheConfig) (*compiledPlanCache, *entityRepoSchemaSource) {
	t.Helper()
	repo := newEmptyEntityRepo()
	repo.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", 100)))
	source := &entityRepoSchemaSource{measures: repo}
	cache := newCompiledPlanCache(cfg, source, bydbql.NewTransformerWithSource(nil, source), nil)
	t.Cleanup(cache.Close)
	return cache, source
}

func TestCompiledPlanReuseFreshBindings(t *testing.T) {
	cache, _ := planCacheFixture(t, planCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20, MaxCompiles: 2, MaxInFlight: 8})
	text := "SELECT svc AS node FROM g.m WHERE time >= ? LIMIT ?"
	statement, err := bydbql.Prepare(text)
	require.NoError(t, err)
	first, outcome, err := cache.acquire(t.Context(), text, statement)
	require.NoError(t, err)
	require.Equal(t, "miss", outcome)
	require.NotNil(t, first)
	template := first.template
	first.Release()
	for i := int64(0); i < 3; i++ {
		reparsed, parseErr := bydbql.Prepare(text)
		require.NoError(t, parseErr)
		handle, result, acquireErr := cache.acquire(t.Context(), text, reparsed)
		require.NoError(t, acquireErr)
		require.Equal(t, "hit", result)
		require.Same(t, template, handle.template)
		bound, bindErr := reparsed.Bind([]*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: time.UnixMilli(1000 + i*1000).UTC().Format(time.RFC3339Nano)}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: i}}},
		})
		require.NoError(t, bindErr)
		execution, bindErr := handle.template.Bind(bound, time.UnixMilli(0))
		require.NoError(t, bindErr)
		require.Equal(t, 1000+i*1000, execution.Request.TimeRange.Begin.AsTime().UnixMilli())
		require.Equal(t, uint32(i), *execution.Plan.Limit)
		handle.Release()
	}
	require.Equal(t, int64(0), cache.snapshot().InFlight)
}

func TestCompiledPlanCancellationBeforeLookup(t *testing.T) {
	cache, _ := planCacheFixture(t, planCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20, MaxCompiles: 2, MaxInFlight: 8})
	text := "SELECT svc AS node FROM g.m LIMIT ?"
	statement, err := bydbql.Prepare(text)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	handle, _, err := cache.acquire(ctx, text, statement)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, handle)
}

func TestCompiledPlanEvictionRetainsActiveExecution(t *testing.T) {
	cache, _ := planCacheFixture(t, planCacheConfig{MaxEntries: 1, MaxBytes: 1 << 20})
	firstSQL := "SELECT svc AS node FROM g.m LIMIT ?"
	firstStatement, err := bydbql.Prepare(firstSQL)
	require.NoError(t, err)
	first, _, err := cache.acquire(t.Context(), firstSQL, firstStatement)
	require.NoError(t, err)
	retained := cache.snapshot().RetainedBytes
	require.Positive(t, retained)
	secondSQL := "SELECT svc AS another FROM g.m LIMIT ?"
	secondStatement, err := bydbql.Prepare(secondSQL)
	require.NoError(t, err)
	second, _, err := cache.acquire(t.Context(), secondSQL, secondStatement)
	require.NoError(t, err)
	stats := cache.snapshot()
	require.Zero(t, stats.Entries)
	require.Equal(t, retained, stats.RetiredBytes)
	require.Positive(t, stats.UncachedBytes)
	require.Positive(t, stats.CompileReservedBytes)
	bound, err := firstStatement.Bind([]*modelv1.TagValue{{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 2}}}})
	require.NoError(t, err)
	execution, err := first.template.Bind(bound, time.UnixMilli(0))
	require.NoError(t, err)
	require.Equal(t, uint32(2), *execution.Plan.Limit)
	second.Release()
	first.Release()
	require.Zero(t, cache.snapshot().RetainedBytes)
	require.Zero(t, cache.snapshot().CompileReservedBytes)
	third, outcome, err := cache.acquire(t.Context(), secondSQL, secondStatement)
	require.NoError(t, err)
	require.Equal(t, "miss", outcome)
	require.Equal(t, int64(1), cache.snapshot().Entries)
	third.Release()
	cache.Close()
	require.Zero(t, cache.snapshot().RetainedBytes)
	require.Zero(t, cache.snapshot().UncachedBytes)
}

func TestCompiledPlanInvalidatesSameAndLowerRevision(t *testing.T) {
	cache, source := planCacheFixture(t, planCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20})
	query := "SELECT svc AS node FROM g.m LIMIT ?"
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	previous, _, err := cache.acquire(t.Context(), query, statement)
	require.NoError(t, err)
	for _, revision := range []int64{100, 99} {
		source.measures.OnAddOrUpdate(measureAddEvent(minimalMeasure("g", "m", revision)))
		current, outcome, acquireErr := cache.acquire(t.Context(), query, statement)
		require.NoError(t, acquireErr)
		require.Equal(t, "miss", outcome)
		require.NotSame(t, previous.template, current.template)
		require.False(t, previous.valid())
		require.True(t, current.valid())
		previous.Release()
		previous = current
	}
	previous.Release()
	require.Equal(t, uint64(3), cache.snapshot().Compiles)
}

func TestCompiledPlanDoesNotCacheCompilationErrors(t *testing.T) {
	cache, _ := planCacheFixture(t, planCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20})
	query := "SELECT missing AS node FROM g.m LIMIT ?"
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	for range 2 {
		handle, _, acquireErr := cache.acquire(t.Context(), query, statement)
		require.ErrorIs(t, acquireErr, bydbql.ErrInvalidMeasureQuery)
		require.Nil(t, handle)
		require.Zero(t, cache.snapshot().Entries)
		require.Zero(t, cache.snapshot().CompileReservedBytes)
	}
	require.Equal(t, uint64(2), cache.snapshot().Compiles)
}
