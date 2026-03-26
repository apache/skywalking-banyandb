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

package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	err := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, err)
	return logger.GetLogger("test", "lifecycle")
}

func TestNewCollector(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 10*time.Minute)
	require.NotNil(t, collector)
	assert.Nil(t, collector.currentData)
	assert.Equal(t, 10*time.Minute, collector.cacheTTL)
}

func TestCollector_ReadReportFiles_EmptyDir(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", t.TempDir(), 0)
	reports := collector.readReportFiles()
	assert.Empty(t, reports)
}

func TestCollector_Collect(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 0)
	ctx := t.Context()
	data, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data)
	assert.NotNil(t, collector.currentData)
}

func TestCollector_CacheTTL(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 5*time.Minute)
	ctx := t.Context()

	now := time.Now()
	collector.nowFunc = func() time.Time { return now }

	// First call should collect and cache
	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data1)

	// Second call within TTL should return cached data
	now = now.Add(2 * time.Minute)
	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	assert.Same(t, data1, data2)

	// After TTL expires, should re-collect
	now = now.Add(4 * time.Minute) // total 6 minutes > 5 minute TTL
	data3, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data3)
	assert.NotSame(t, data1, data3)
}

func TestCollector_GrpcUnimplemented_SkipsFutureCalls(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "localhost:0", "", 0)

	assert.False(t, collector.grpcUnimplemented.Load())

	groups := collector.collectGroups(t.Context())
	assert.Nil(t, groups)
	assert.False(t, collector.grpcUnimplemented.Load())

	collector.grpcUnimplemented.Store(true)
	groups = collector.collectGroups(t.Context())
	assert.Nil(t, groups)
	assert.True(t, collector.grpcUnimplemented.Load())
}

func TestCollector_ReadReportFiles_MaxFiles(t *testing.T) {
	log := initTestLogger(t)
	dir := t.TempDir()
	collector := NewCollector(log, "", dir, 0)

	// Create 7 files with timestamp-like names so sort order is predictable
	for idx, name := range []string{
		"2026-03-20.json",
		"2026-03-21.json",
		"2026-03-22.json",
		"2026-03-23.json",
		"2026-03-24.json",
		"2026-03-25.json",
		"2026-03-26.json",
	} {
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, name),
			[]byte(`{"idx":`+string(rune('0'+idx))+`}`),
			0o600,
		))
	}

	reports := collector.readReportFiles()
	require.Equal(t, maxReportFiles, len(reports))

	// Should be the 5 most recent (sorted descending by name)
	assert.Equal(t, "2026-03-26.json", reports[0].Filename)
	assert.Equal(t, "2026-03-25.json", reports[1].Filename)
	assert.Equal(t, "2026-03-24.json", reports[2].Filename)
	assert.Equal(t, "2026-03-23.json", reports[3].Filename)
	assert.Equal(t, "2026-03-22.json", reports[4].Filename)
}

func TestCollector_ReadReportFiles_SkipsNonJSON(t *testing.T) {
	log := initTestLogger(t)
	dir := t.TempDir()
	collector := NewCollector(log, "", dir, 0)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-03-26.json"), []byte(`{}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("text"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.csv"), []byte("a,b"), 0o600))

	reports := collector.readReportFiles()
	require.Equal(t, 1, len(reports))
	assert.Equal(t, "2026-03-26.json", reports[0].Filename)
}

func TestCollector_ReadReportFiles_OversizedFile(t *testing.T) {
	log := initTestLogger(t)
	dir := t.TempDir()
	collector := NewCollector(log, "", dir, 0)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-03-26.json"), []byte(`{"ok":true}`), 0o600))
	oversized := make([]byte, maxReportFileSize+1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "2026-03-25.json"), oversized, 0o600))

	reports := collector.readReportFiles()
	require.Equal(t, 1, len(reports))
	assert.Equal(t, "2026-03-26.json", reports[0].Filename)
}

func TestCollector_ZeroTTL_AlwaysRefreshes(t *testing.T) {
	log := initTestLogger(t)
	collector := NewCollector(log, "", "", 0)
	ctx := t.Context()

	data1, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data1)

	// With zero TTL, every call should re-collect
	data2, err := collector.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, data2)
	assert.NotSame(t, data1, data2)
}
