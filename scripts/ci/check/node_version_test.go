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

package check_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nodeVersionScript(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("node_version.sh requires a POSIX shell")
	}
	abs, err := filepath.Abs("node_version.sh")
	require.NoError(t, err)
	return abs
}

func runNodeVersion(t *testing.T, root string) (string, error) {
	t.Helper()
	return runNodeVersionMode(t, "check", root)
}

func runNodeVersionMode(t *testing.T, mode, root string) (string, error) {
	t.Helper()
	// The script path is this package's own file and mode/root come from the
	// tests below; nothing here is attacker-controlled.
	cmd := exec.Command("bash", nodeVersionScript(t), mode, root) //nolint:gosec // fixed local path, literal mode
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// nodeTree writes the mcp and canopy package.json files that declare the Node
// version, plus any further project manifests. There is no generated version
// file: actions/setup-node reads engines.node from a package.json directly.
func nodeTree(t *testing.T, mcpNode, canopyNode string, manifests map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for _, p := range []struct{ project, node string }{{"mcp", mcpNode}, {"canopy", canopyNode}} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, p.project), 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(root, p.project, "package.json"),
			[]byte("{\n  \"engines\": {\n    \"node\": \""+p.node+"\"\n  }\n}\n"), 0o600))
	}

	for rel, content := range manifests {
		path := filepath.Join(root, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	return root
}

func TestNodeVersionAgreesWhenBothProjectsPinTheSameVersion(t *testing.T) {
	// mcp and canopy are the sources; a third project declaring a lower floor is
	// still satisfied by that pin.
	root := nodeTree(t, "24.6.0", "24.6.0", map[string]string{
		"nested-js-project/package.json":     "{\n  \"engines\": {\n    \"node\": \">=23.0.0\"\n  }\n}\n",
		"nested-js-project/web/package.json": "{\n  \"name\": \"nested-web\"\n}\n",
	})
	out, err := runNodeVersion(t, root)
	require.NoError(t, err, out)
}

func TestNodeVersionRejectsExactMismatchInAnotherProject(t *testing.T) {
	root := nodeTree(t, "24.6.0", "24.6.0", map[string]string{
		"nested-js-project/package.json": "{\n  \"engines\": {\n    \"node\": \"24.5.0\"\n  }\n}\n",
	})
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "nested-js-project pins node 24.5.0")
}

func TestNodeVersionRejectsFloorAboveThePin(t *testing.T) {
	// A project outside the two sources needing something newer must fail here,
	// not surface later as a confusing npm engines error inside the container.
	root := nodeTree(t, "24.6.0", "24.6.0", map[string]string{
		"nested-js-project/package.json": "{\n  \"engines\": {\n    \"node\": \">=25.0.0\"\n  }\n}\n",
	})
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "nested-js-project requires node >=25.0.0 but mcp and canopy pin 24.6.0")
}

func TestNodeVersionRejectsSubprojectsDisagreeing(t *testing.T) {
	// The whole point of reading from both: an image built from a version only
	// one of them asked for would resolve a different license set for that one.
	root := nodeTree(t, "25.0.0", "24.6.0", nil)
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "the Node subprojects disagree")
	assert.Contains(t, out, "mcp pins 25.0.0")
	assert.Contains(t, out, "canopy pins 24.6.0")
}

func TestNodeVersionRejectsAFloorInsteadOfAPin(t *testing.T) {
	// A range does not identify one version, so the image would be built from
	// whatever the builder happened to choose.
	for _, project := range []string{"mcp", "canopy"} {
		mcpNode, canopyNode := "24.6.0", "24.6.0"
		if project == "mcp" {
			mcpNode = ">=24.6.0"
		} else {
			canopyNode = ">=24.6.0"
		}
		root := nodeTree(t, mcpNode, canopyNode, nil)
		out, err := runNodeVersion(t, root)
		require.Error(t, err, project)
		assert.Contains(t, out, project+"/package.json declares node '>=24.6.0'")
		assert.Contains(t, out, "must be an exact pin")
	}
}

func TestNodeVersionRejectsAMalformedPin(t *testing.T) {
	root := nodeTree(t, "lts/*", "lts/*", nil)
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "must be an exact pin")
}

func TestNodeVersionRejectsUnsupportedRangeInAnotherProject(t *testing.T) {
	// A check that quietly passes a range it does not understand is worse than
	// no check at all.
	root := nodeTree(t, "24.6.0", "24.6.0", map[string]string{
		"nested-js-project/package.json": "{\n  \"engines\": {\n    \"node\": \"^24.0.0\"\n  }\n}\n",
	})
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "this check does not")
}

func TestNodeVersionRejectsMissingSource(t *testing.T) {
	// A declaring project disappearing must be an error, not a silent pass.
	root := nodeTree(t, "24.6.0", "24.6.0", nil)
	require.NoError(t, os.Remove(filepath.Join(root, "canopy", "package.json")))
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "canopy/package.json not found")
}

func TestNodeVersionRejectsSourceWithoutEngines(t *testing.T) {
	root := nodeTree(t, "24.6.0", "24.6.0", nil)
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "canopy", "package.json"), []byte("{\n  \"name\": \"canopy\"\n}\n"), 0o600))
	out, err := runNodeVersion(t, root)
	require.Error(t, err)
	assert.Contains(t, out, "has no engines.node")
}

func TestResolvePrintsTheAgreedPin(t *testing.T) {
	// What make node-version-file and dockerize.mk both consume.
	root := nodeTree(t, "24.6.0", "24.6.0", nil)
	out, err := runNodeVersionMode(t, "resolve", root)
	require.NoError(t, err)
	assert.Equal(t, "24.6.0", strings.TrimSpace(out))
}

func TestResolveRefusesWhenTheSubprojectsDisagree(t *testing.T) {
	// The build must not silently pick one of them.
	root := nodeTree(t, "25.0.0", "24.6.0", nil)
	out, err := runNodeVersionMode(t, "resolve", root)
	require.Error(t, err)
	assert.Contains(t, out, "the Node subprojects disagree")
}
