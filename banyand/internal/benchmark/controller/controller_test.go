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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

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

func TestPinToCPUsAllThreads(t *testing.T) {
	const helperEnv = "BANYANDB_TEST_PIN_ALL_THREADS"
	if os.Getenv(helperEnv) != "1" {
		executable, executableErr := os.Executable()
		require.NoError(t, executableErr)
		command := exec.Command(executable, "-test.run=^TestPinToCPUsAllThreads$", "-test.count=1")
		command.Env = append(os.Environ(), helperEnv+"=1")
		output, commandErr := command.CombinedOutput()
		require.NoError(t, commandErr, "%s", output)
		return
	}

	identity, identityErr := CurrentResourceIdentity(os.Getpid())
	require.NoError(t, identityErr)
	if len(identity.CPUs) < 2 {
		t.Skip("requires at least two allowed CPUs")
	}
	// Keep several OS threads alive so pinning just the caller cannot pass.
	const workers = 4
	ready := make(chan struct{}, workers)
	release := make(chan struct{})
	defer close(release)
	for worker := 0; worker < workers; worker++ {
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			ready <- struct{}{}
			<-release
		}()
	}
	for worker := 0; worker < workers; worker++ {
		<-ready
	}
	selectedCPU := identity.CPUs[len(identity.CPUs)-1]
	require.NoError(t, PinToCPUs([]int{selectedCPU}))
	pinned, pinnedErr := CurrentResourceIdentity(os.Getpid())
	require.NoError(t, pinnedErr)
	assert.Equal(t, []int{selectedCPU}, pinned.CPUs)
	threads, threadsErr := os.ReadDir("/proc/self/task")
	require.NoError(t, threadsErr)
	for _, thread := range threads {
		threadID, parseErr := strconv.Atoi(thread.Name())
		require.NoError(t, parseErr)
		var affinity unix.CPUSet
		affinityErr := unix.SchedGetaffinity(threadID, &affinity)
		if errors.Is(affinityErr, unix.ESRCH) {
			continue
		}
		require.NoError(t, affinityErr)
		assert.Equal(t, 1, affinity.Count(), "thread %d", threadID)
		assert.True(t, affinity.IsSet(selectedCPU), "thread %d", threadID)
	}
}
