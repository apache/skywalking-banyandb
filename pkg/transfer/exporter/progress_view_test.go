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

package exporter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadProgress_MissingFileIsNotExist(t *testing.T) {
	if _, err := ReadProgress(filepath.Join(t.TempDir(), "none.json")); !os.IsNotExist(err) {
		t.Fatalf("a missing progress file is not exist: %v", err)
	}
}

// An interrupted export's progress file reads back with its session, its plan and the unit it finished.
func TestReadProgress_InterruptedExport(t *testing.T) {
	_, opts := interruptAfterOne(t)
	v, err := ReadProgress(ProgressPath(opts.Export))
	if err != nil {
		t.Fatal(err)
	}
	if v.SessionID == "" || len(v.Plan) == 0 {
		t.Fatalf("the session and the plan are recorded: %+v", v)
	}
	if len(v.Done) != 1 || len(v.Done[0].Files) == 0 {
		t.Fatalf("one finished unit with its files: %+v", v.Done)
	}
	for _, f := range v.Done[0].Files {
		if _, statErr := os.Stat(filepath.Join(opts.Output, filepath.FromSlash(f))); statErr != nil {
			t.Fatalf("a file of a finished unit is in the output directory: %v", statErr)
		}
	}
}
