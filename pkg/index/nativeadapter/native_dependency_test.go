// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nativeadapter

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdapterHasNoRetiredTransitiveDependencies(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	command := exec.Command("go", "list", "-deps", ".")
	command.Dir = filepath.Dir(sourceFile)
	output, err := command.Output()
	require.NoError(t, err)
	retired := retiredIndexModules(t, filepath.Join(filepath.Dir(sourceFile), "..", "..", ".."))
	for _, dependency := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		require.NotContains(t, dependency, "/pkg/index/inverted")
		for _, module := range retired {
			require.False(t, dependency == module || strings.HasPrefix(dependency, module+"/"),
				"%s reaches the retired module %s", dependency, module)
		}
	}
}

// retiredIndexModules returns the module paths go.mod redirects that the
// legacy index package imports directly. Reading both from the build keeps
// this guard free of the retired module names it enforces.
func retiredIndexModules(t *testing.T, repositoryRoot string) []string {
	t.Helper()
	goModule, err := os.ReadFile(filepath.Join(repositoryRoot, "go.mod"))
	require.NoError(t, err)
	var replaced []string
	for _, line := range strings.Split(string(goModule), "\n") {
		redirect := strings.Index(line, "=>")
		if redirect < 0 {
			continue
		}
		if fields := strings.Fields(line[:redirect]); len(fields) > 0 && fields[0] != "replace" {
			replaced = append(replaced, fields[0])
		}
	}
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, "./pkg/index/inverted")
	command.Dir = repositoryRoot
	output, err := command.Output()
	require.NoError(t, err)
	retired := map[string]struct{}{}
	for _, importPath := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		for _, module := range replaced {
			if importPath == module || strings.HasPrefix(importPath, module+"/") {
				retired[module] = struct{}{}
			}
		}
	}
	modules := make([]string, 0, len(retired))
	for module := range retired {
		modules = append(modules, module)
	}
	require.NotEmpty(t, modules, "the legacy index package must import at least one redirected module")
	return modules
}
