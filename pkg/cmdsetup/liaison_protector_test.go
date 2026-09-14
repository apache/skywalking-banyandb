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

package cmdsetup

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Liaison must register the memory protector in its run group. Without PreRun,
// GetLimit stays 0 and query admission falls back to a 64MiB pool that rejects
// OAP property list-all during Cluster/Rover e2e.
func TestLiaisonSourceRegistersMemoryProtector(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	src, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "liaison.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	unitsIdx := strings.Index(text, "units = append(units,")
	if unitsIdx < 0 {
		t.Fatal("liaison units append not found")
	}
	// Bound the search to the first run-group registration after units append.
	registerIdx := strings.Index(text[unitsIdx:], "liaisonGroup.Register")
	if registerIdx < 0 {
		t.Fatal("liaisonGroup.Register not found after units append")
	}
	unitsBlock := text[unitsIdx : unitsIdx+registerIdx]
	if !strings.Contains(unitsBlock, "\tpm,") && !strings.Contains(unitsBlock, " pm,") {
		t.Fatal("liaison run units must include pm so memory-protector PreRun enables query budgets")
	}
}
