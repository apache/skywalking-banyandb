// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPhaseProfilerStopsAndClosesIdempotently(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "cpu.pprof")
	profiler, startErr := startPhaseProfiler(profilePath)
	require.NoError(t, startErr)
	require.NoError(t, profiler.stop())
	require.NoError(t, profiler.stop())
	require.FileExists(t, profilePath)
}

func TestPhaseProfilerClosesFileWhenStartFails(t *testing.T) {
	firstPath := filepath.Join(t.TempDir(), "first.pprof")
	first, firstErr := startPhaseProfiler(firstPath)
	require.NoError(t, firstErr)
	secondPath := filepath.Join(t.TempDir(), "second.pprof")
	_, secondErr := startPhaseProfiler(secondPath)
	require.ErrorContains(t, secondErr, "cannot start CPU profile")
	require.NoError(t, first.stop())
}
