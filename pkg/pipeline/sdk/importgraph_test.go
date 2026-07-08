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

package sdk_test

import (
	"bufio"
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// TestSDKImportGraph asserts that pkg/pipeline/sdk — the pinned surface plugin
// authors build against — has NO transitive dependency on any logging, meter, or
// heavy banyand-internal package. The SDK depends only on dependency-free leaves
// (pkg/convert, pkg/encoding/vararray, pkg/pb/v1/valuetype). This is an absolute
// check (not relative to pkg/pb/v1): a plugin built against this package must not
// link zerolog/zap, so a host zerolog bump can never break the plugin ABI.
func TestSDKImportGraph(t *testing.T) {
	goPath, lookErr := exec.LookPath("go")
	if lookErr != nil {
		t.Skip("go binary not on PATH; skipping import-graph check")
	}

	forbidden := []string{
		"github.com/rs/zerolog",
		"go.uber.org/zap",
		"github.com/apache/skywalking-banyandb/pkg/logger",
		"github.com/apache/skywalking-banyandb/pkg/meter",
		"github.com/apache/skywalking-banyandb/banyand/observability",
		// pkg/pb/v1 is heavy (it imports pkg/logger); the SDK must reach ValueType
		// via the pkg/pb/v1/valuetype leaf instead, so pbv1 itself is forbidden.
		"github.com/apache/skywalking-banyandb/pkg/pb/v1",
	}

	sdkDeps := listDeps(t, goPath, "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk")

	for _, banned := range forbidden {
		if sdkDeps[banned] {
			t.Errorf("pkg/pipeline/sdk transitively imports forbidden package %q; the plugin SDK must stay"+
				" dependency-pure (stdlib + leaf types only) so plugins are not version-locked to host internals", banned)
		}
		// Also check subpaths (e.g. github.com/rs/zerolog/internal/json), but do
		// not let a leaf subpackage (pkg/pb/v1/valuetype) trip the pkg/pb/v1 prefix.
		for dep := range sdkDeps {
			if dep == "github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype" {
				continue
			}
			if strings.HasPrefix(dep, banned+"/") {
				t.Errorf("pkg/pipeline/sdk transitively imports forbidden package %q; the plugin SDK must stay pure", dep)
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
