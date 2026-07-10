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

package sdktest_test

import (
	"bufio"
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// TestSDKTestImportGraph asserts that pkg/pipeline/sdk/sdktest — the offline
// dev-toolkit package plugin authors depend on for their own tests — has NO
// transitive dependency on banyand/* (the engine internals it exists to test
// against without a cluster) or on any logging/meter package. This is a
// dedicated denylist for sdktest: pkg/pipeline/sdk's own importgraph_test.go
// only covers pkg/pipeline/sdk itself, and sdktest's dependency-purity is a
// design property in its own right (it must stay usable from a plugin
// author's own _test.go, which has no access to banyand-internal packages).
func TestSDKTestImportGraph(t *testing.T) {
	goPath, lookErr := exec.LookPath("go")
	if lookErr != nil {
		t.Skip("go binary not on PATH; skipping import-graph check")
	}

	forbidden := []string{
		"github.com/apache/skywalking-banyandb/banyand",
		"github.com/rs/zerolog",
		"go.uber.org/zap",
		"github.com/apache/skywalking-banyandb/pkg/logger",
		"github.com/apache/skywalking-banyandb/pkg/meter",
		// pkg/pb/v1 is heavy (it imports pkg/logger); sdktest must reach
		// ValueType via pkg/pb/v1/valuetype (through pkg/pipeline/sdk) instead.
		"github.com/apache/skywalking-banyandb/pkg/pb/v1",
	}

	deps := listDeps(t, goPath, "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest")

	for _, banned := range forbidden {
		if deps[banned] {
			t.Errorf("pkg/pipeline/sdk/sdktest transitively imports forbidden package %q; "+
				"the offline dev toolkit must stay usable outside the banyand module tree", banned)
		}
		for dep := range deps {
			if dep == "github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype" {
				continue
			}
			if strings.HasPrefix(dep, banned+"/") {
				t.Errorf("pkg/pipeline/sdk/sdktest transitively imports forbidden package %q", dep)
			}
		}
	}
}

// listDeps returns the full transitive import set of pkg as a set (map to bool).
func listDeps(t *testing.T, goPath, pkg string) map[string]bool {
	t.Helper()
	cmd := exec.Command(goPath, "list", "-deps", pkg)
	out, runErr := cmd.Output()
	if runErr != nil {
		t.Fatalf("go list -deps %s failed: %v", pkg, runErr)
	}
	deps := make(map[string]bool)
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			deps[line] = true
		}
	}
	return deps
}
