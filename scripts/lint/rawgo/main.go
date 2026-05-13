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

// Command rawgo enforces the project's "no raw goroutines outside the
// recovery wrappers" rule. Invoke it like a linter:
//
//	go run ./scripts/lint/rawgo \
//	  -baseline=pkg/panicdiag/lintrawgo/baseline.txt ./...
//
// The driver loads packages in syntax-only mode so it works against a
// codebase that fails type-checking (e.g. an in-flight refactor where
// some test packages do not yet compile). Exit status:
//
//	0  no raw `go` violations found
//	1  one or more violations found
//	2  driver setup error (bad flag, missing baseline file, etc.)
//
// See pkg/panicdiag/lintrawgo for the rationale and rules.
package main

import (
	"flag"
	"fmt"
	"os"

	"golang.org/x/tools/go/packages"

	"github.com/apache/skywalking-banyandb/pkg/panicdiag/lintrawgo"
)

func main() {
	var (
		baseline     = flag.String("baseline", "", "baseline file of grandfathered raw `go` sites")
		includeTests = flag.Bool("include-tests", true, "check files whose names end in _test.go")
	)
	flag.Parse()

	patterns := flag.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	checker := lintrawgo.NewChecker(lintrawgo.CheckerOptions{
		BaselinePath: *baseline,
		IncludeTests: *includeTests,
	})

	cfg := &packages.Config{
		// Syntax-only: no NeedTypes / NeedTypesInfo. The analyzer does
		// not need type information, and excluding it lets the driver
		// run against packages that fail to type-check.
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedCompiledGoFiles,
		Tests: *includeTests,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rawgo: load packages:", err)
		os.Exit(2)
	}

	// packages.Load with Tests=true returns up to four synthetic packages
	// per directory (the package, its external test package, its
	// `.test` binary, and the in-package test variant). The same file
	// shows up across them; check each absolute filename only once.
	seen := map[string]bool{}
	violations := 0
	for _, pkg := range pkgs {
		// Parse errors prevent us from inspecting a file's syntax tree;
		// log them but continue. Type-check errors do not appear here
		// because we did not ask for type info.
		for _, e := range pkg.Errors {
			if e.Kind == packages.ParseError {
				fmt.Fprintln(os.Stderr, "rawgo: parse:", e)
			}
		}
		for _, f := range pkg.Syntax {
			fname := pkg.Fset.Position(f.Pos()).Filename
			if seen[fname] {
				continue
			}
			seen[fname] = true
			diags, err := checker.Check(pkg.Fset, pkg.PkgPath, f)
			if err != nil {
				fmt.Fprintln(os.Stderr, "rawgo:", err)
				os.Exit(2)
			}
			for _, d := range diags {
				pos := pkg.Fset.Position(d.Pos)
				fmt.Fprintf(os.Stderr, "%s:%d:%d: %s\n", pos.Filename, pos.Line, pos.Column, d.Message)
				violations++
			}
		}
	}

	if violations > 0 {
		fmt.Fprintf(os.Stderr, "\nrawgo: %d new raw-`go` violation(s); see pkg/panicdiag/lintrawgo for guidance\n", violations)
		os.Exit(1)
	}
}
