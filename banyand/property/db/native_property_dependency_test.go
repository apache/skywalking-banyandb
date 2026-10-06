// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. Apache Software Foundation (ASF) licenses
// this file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
// language governing permissions and limitations under the License.

package db

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNativePropertyBackendHasNoRetiredQueryEngineImport(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate native property dependency test")
	}
	backendPaths, err := filepath.Glob(filepath.Join(filepath.Dir(sourceFile), "native_property*.go"))
	if err != nil {
		t.Fatalf("glob native property backend: %v", err)
	}
	for _, backendPath := range backendPaths {
		if strings.HasSuffix(backendPath, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), backendPath, nil, parser.ImportsOnly)
		if parseErr != nil {
			t.Fatalf("parse native property backend %s: %v", backendPath, parseErr)
		}
		for _, imported := range file.Imports {
			path := strings.Trim(imported.Path.Value, `"`)
			// An allowlist rather than a list of retired modules: the backend may
			// import only the standard library and this module, minus the
			// legacy index package, so no third-party index library can enter.
			standardLibrary := !strings.Contains(strings.SplitN(path, "/", 2)[0], ".")
			ownModule := strings.HasPrefix(path, "github.com/apache/skywalking-banyandb/")
			if strings.Contains(path, "/pkg/index/inverted") || (!standardLibrary && !ownModule) {
				t.Fatalf("native property backend %s imports %q, outside its dependency budget", backendPath, path)
			}
		}
	}
}
