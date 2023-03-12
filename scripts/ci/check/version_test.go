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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
)

const GoVersion = "1.20"

func TestGoVersion(t *testing.T) {
	goversion, err := exec.Command("go", "version").Output()
	require.NoError(t, err)

	currentVersion := strings.Split(string(goversion), " ")[2][2:]

	currentMajorMinor, currentPatch := splitVersion(currentVersion)
	expectedMajorMinor, expectedPatch := splitVersion(GoVersion)

	require.Equal(t, currentMajorMinor, expectedMajorMinor,
		"go version <mayor>.<minor> mismatch: current[%s], want[%s]",
		currentMajorMinor, expectedMajorMinor)

	require.True(t, currentPatch >= expectedPatch,
		"go version unsupported, current[%s.%s], minimum[%s.%s]",
		currentMajorMinor, currentPatch,
		currentMajorMinor, expectedPatch,
	)

	path, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	root := strings.TrimSpace(string(path))

	m := parseGoMod(t, root+"/go.mod")

	require.Equal(t, expectedMajorMinor, m.Go.Version,
		"go.mod version mismatch: current[%s], want[%s]",
		m.Go.Version, expectedMajorMinor)
}

func splitVersion(v string) (string, string) {
	versionParts := strings.SplitN(v, ".", 3)
	// if v only has two parts, it can be <major>.<minor>
	if len(versionParts) == 2 {
		return v, "0"
	}
	return strings.Join(versionParts[0:2], "."), versionParts[2]
}

func parseGoMod(t *testing.T, gomod string) *modfile.File {
	bytes, err := os.ReadFile(gomod)
	require.NoError(t, err)

	m, err := modfile.Parse(gomod, bytes, nil)
	require.NoError(t, err)

	return m
}
