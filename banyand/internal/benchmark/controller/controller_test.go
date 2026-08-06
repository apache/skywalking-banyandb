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

package controller

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
)

func TestAtomicPublishPreservesDirectoryManifest(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "staged", "part")
	destination := filepath.Join(root, "data", "part")
	require.NoError(t, os.MkdirAll(source, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "payload"), []byte("immutable"), 0o600))
	expected, manifestErr := benchmark.TreeManifest(source)
	require.NoError(t, manifestErr)

	report, publishErr := AtomicPublish([]Move{{Source: source, Destination: destination, SHA256: expected.SHA256}})
	require.NoError(t, publishErr)
	require.Len(t, report.Moves, 1)
	assert.Equal(t, expected, report.Moves[0].Manifest)
	_, sourceErr := os.Stat(source)
	assert.True(t, os.IsNotExist(sourceErr))
	after, afterErr := benchmark.TreeManifest(destination)
	require.NoError(t, afterErr)
	assert.Equal(t, expected, after)
}

func TestValidateResourceIsolation(t *testing.T) {
	dataNode := ResourceIdentity{PID: 10, Cgroup: "/benchmark/data", CPUs: []int{0, 1}}
	controller := ResourceIdentity{PID: 20, Cgroup: "/benchmark/controller", CPUs: []int{2, 3}}
	require.NoError(t, ValidateResourceIsolation(dataNode, controller))

	require.ErrorContains(t, ValidateResourceIsolation(dataNode, ResourceIdentity{PID: 20, Cgroup: dataNode.Cgroup, CPUs: []int{2}}), "cgroup")
	require.ErrorContains(t, ValidateResourceIsolation(dataNode, ResourceIdentity{PID: 20, Cgroup: controller.Cgroup, CPUs: []int{1, 2}}), "CPU")
	require.ErrorContains(t, ValidateResourceIsolation(dataNode, ResourceIdentity{PID: 10, Cgroup: controller.Cgroup, CPUs: []int{2}}), "process")
}
