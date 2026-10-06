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
	for _, dependency := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		require.NotContains(t, dependency, "/pkg/index/inverted")
		require.NotContains(t, dependency, "github.com/blugelabs/bluge")
		require.NotContains(t, dependency, "github.com/blugelabs/bluge_segment_api")
	}
}
