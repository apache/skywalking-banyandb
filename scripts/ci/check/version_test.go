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

const GoVersion = "1.18.3"

func TestGoVersion(t *testing.T) {
	goversion, err := exec.Command("go", "version").Output()
	require.NoError(t, err)

	currentVersion := strings.Split(string(goversion), " ")[2][2:]

	currentMayorMinor, currentPatch := splitVersion(currentVersion)
	expectedMayorMinor, expectedPatch := splitVersion(GoVersion)

	require.Equal(t, currentMayorMinor, expectedMayorMinor,
		"go version <mayor>.<minor> mismatch: current[%s], want[%s]",
		currentMayorMinor, expectedMayorMinor)

	require.True(t, currentPatch >= expectedPatch,
		"go version unsupported, current[%s.%s], minimum[%s.%s]",
		currentMayorMinor, currentPatch,
		currentMayorMinor, expectedPatch,
	)

	path, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	root := strings.TrimSpace(string(path))

	m := parseGoMod(t, root+"/go.mod")

	require.Equal(t, expectedMayorMinor, m.Go.Version,
		"go.mod version mismatch: current[%s], want[%s]",
		m.Go.Version, expectedMayorMinor)
}

func splitVersion(v string) (string, string) {
	lastDotPos := strings.LastIndex(v, ".")
	return v[:lastDotPos], v[lastDotPos+1:]
}

func parseGoMod(t *testing.T, gomod string) *modfile.File {
	bytes, err := os.ReadFile(gomod)
	require.NoError(t, err)

	m, err := modfile.Parse(gomod, bytes, nil)
	require.NoError(t, err)

	return m
}
