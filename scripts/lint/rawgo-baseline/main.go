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

// Command rawgo-baseline rewrites the raw-go baseline from the current tree.
// It keeps the comment header from the existing baseline file and replaces the
// entry list with every raw `go` statement that is not covered by an inline
// //panicdiag:allow-rawgo directive or an analyzer allowlist.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"

	"github.com/apache/skywalking-banyandb/pkg/panicdiag/lintrawgo"
)

type entry struct {
	path string
	line int
}

func main() {
	var (
		baselinePath = flag.String("baseline", "pkg/panicdiag/lintrawgo/baseline.txt", "baseline file to rewrite")
		includeTests = flag.Bool("include-tests", true, "check files whose names end in _test.go")
	)
	flag.Parse()

	patterns := flag.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}

	root, err := moduleRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "rawgo-baseline:", err)
		os.Exit(2)
	}
	entries, err := collectEntries(root, patterns, *includeTests)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rawgo-baseline:", err)
		os.Exit(2)
	}
	if err := rewriteBaseline(*baselinePath, entries); err != nil {
		fmt.Fprintln(os.Stderr, "rawgo-baseline:", err)
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "rawgo-baseline: wrote %d entries to %s\n", len(entries), *baselinePath)
}

func collectEntries(root string, patterns []string, includeTests bool) ([]entry, error) {
	checker := lintrawgo.NewChecker(lintrawgo.CheckerOptions{
		IncludeTests: includeTests,
	})
	cfg := &packages.Config{
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedCompiledGoFiles,
		Tests: includeTests,
	}
	pkgs, loadErr := packages.Load(cfg, patterns...)
	if loadErr != nil {
		return nil, fmt.Errorf("load packages: %w", loadErr)
	}

	seenFiles := map[string]bool{}
	seenEntries := map[string]bool{}
	var entries []entry
	for _, pkg := range pkgs {
		for _, pkgErr := range pkg.Errors {
			if pkgErr.Kind == packages.ParseError {
				fmt.Fprintln(os.Stderr, "rawgo-baseline: parse:", pkgErr)
			}
		}
		for _, file := range pkg.Syntax {
			filename := pkg.Fset.Position(file.Pos()).Filename
			if seenFiles[filename] {
				continue
			}
			seenFiles[filename] = true
			diags, checkErr := checker.Check(pkg.Fset, pkg.PkgPath, file)
			if checkErr != nil {
				return nil, checkErr
			}
			for _, diag := range diags {
				pos := pkg.Fset.Position(diag.Pos)
				rel, relErr := relativePath(root, pos.Filename)
				if relErr != nil {
					return nil, relErr
				}
				key := fmt.Sprintf("%s:%d", rel, pos.Line)
				if seenEntries[key] {
					continue
				}
				seenEntries[key] = true
				entries = append(entries, entry{path: rel, line: pos.Line})
			}
		}
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].path != entries[right].path {
			return entries[left].path < entries[right].path
		}
		return entries[left].line < entries[right].line
	})
	return entries, nil
}

func rewriteBaseline(path string, entries []entry) error {
	header, err := readHeader(path)
	if err != nil {
		return err
	}
	var b strings.Builder
	for _, line := range header {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if len(header) > 0 && header[len(header)-1] != "" {
		b.WriteByte('\n')
	}
	for _, entry := range entries {
		fmt.Fprintf(&b, "%s:%d\n", entry.path, entry.line)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func readHeader(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open baseline %q: %w", path, err)
	}
	defer file.Close()

	var header []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			break
		}
		header = append(header, line)
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, fmt.Errorf("read baseline %q: %w", path, scanErr)
	}
	return header, nil
}

func relativePath(root, filename string) (string, error) {
	abs, err := filepath.Abs(filename)
	if err != nil {
		return "", fmt.Errorf("resolve path %q: %w", filename, err)
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return "", fmt.Errorf("make %q relative to %q: %w", abs, root, err)
	}
	return filepath.ToSlash(rel), nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from %s", dir)
		}
		dir = parent
	}
}
