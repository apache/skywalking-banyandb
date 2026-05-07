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

// Package lintrawgo provides a go/analysis Analyzer that fails the build
// when production code launches a goroutine via a raw `go` statement
// instead of run.Go, run.GoOrDie, or run.GoWithSignal.
//
// The wrappers in pkg/run install panic recovery and breadcrumb
// propagation around each goroutine; raw `go` bypasses both, so a panic
// in the worker silently kills the process. The analyzer exists to make
// that mistake unrepresentable in code that ships.
//
// The check is wired into `make lint` and runs before every push and in
// CI. New violations fail the build. Existing sites are tracked in a
// checked-in baseline so the policy can be rolled out incrementally; a
// site cleared from the baseline cannot reappear without surfacing.
//
// Three escape hatches exist for legitimate raw `go` usage:
//
//   - Path allowlist: pkg/panicdiag/... and pkg/run/goroutine.go are
//     skipped because they implement the wrappers.
//   - Inline directive: a comment of the form
//     //panicdiag:allow-rawgo <reason> on the line above the `go`
//     statement suppresses the diagnostic for that one site. The reason
//     text after the directive is mandatory and is what reviewers will
//     read; "TODO" or empty justifications fail.
//   - Baseline file: a list of file:line pairs grandfathered in at
//     rollout. The format is documented at the top of the baseline file.
package lintrawgo

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/tools/go/analysis"
)

// directive is the trailing comment that suppresses the analyzer for a
// single goroutine launch. It must precede the `go` statement on its own
// line and carry a non-empty justification.
const directive = "//panicdiag:allow-rawgo"

// modulePrefix is the project's Go module path. Allowlist entries are
// expressed relative to this prefix so testdata fixtures (which load
// under synthetic import paths in analysistest) cannot accidentally
// match a real-package allowlist entry.
const modulePrefix = "github.com/apache/skywalking-banyandb/"

// allowedPackagePaths lists Go import paths (without the module prefix)
// whose entire package is exempt. These are the modules that *implement*
// panic recovery; raw `go` is unavoidable inside them.
var allowedPackagePaths = []string{
	"pkg/panicdiag",          // Recovery primitives.
	"pkg/panicdiag/lintrawgo", // This analyzer (uses raw `go` in tests).
}

// allowedFiles lists individual file basenames within named packages
// that are exempt. Used when the surrounding package contains many other
// functions that should still be checked. Match is `pkg/run`-package +
// basename, not whole-path, so testdata copies cannot satisfy it.
var allowedFiles = map[string]map[string]bool{
	"pkg/run": {"goroutine.go": true},
}

// Analyzer is the go/analysis hook. The flag set is exposed via Flags so
// callers (singlechecker, multichecker, golangci-lint) can surface them.
var Analyzer = newAnalyzer()

// Checker drives the analyzer outside the analysis.Pass machinery. The
// standalone driver uses it so packages that fail type-checking still
// receive raw-go diagnostics: it loads files in syntax-only mode and
// passes their AST through Checker.Check.
type Checker struct {
	s *state
}

// CheckerOptions configures a freshly constructed Checker.
type CheckerOptions struct {
	BaselinePath string
	IncludeTests bool
}

// NewChecker returns a Checker with options resolved at construction
// time. The baseline file (if any) is loaded lazily on first Check.
func NewChecker(opts CheckerOptions) *Checker {
	return &Checker{s: &state{
		baselinePath: opts.BaselinePath,
		includeTests: opts.IncludeTests,
	}}
}

// Check returns diagnostics for one parsed file. importPath is the
// canonical import path of the package the file belongs to and is used
// for allowlist matching; pass an empty string for files outside the
// module (they will be checked normally).
func (c *Checker) Check(fset *token.FileSet, importPath string, f *ast.File) ([]analysis.Diagnostic, error) {
	if err := c.s.ensureBaseline(); err != nil {
		return nil, err
	}
	return c.s.collect(fset, importPath, f), nil
}

// state is the per-Analyzer mutable configuration. It is private so the
// only mutators are the registered flags.
type state struct {
	baselinePath string
	includeTests bool

	once     sync.Once
	loadErr  error
	baseline map[string]map[int]bool // module-relative path → line → allowed
}

func newAnalyzer() *analysis.Analyzer {
	s := &state{includeTests: true}
	a := &analysis.Analyzer{
		Name: "rawgo",
		Doc: "rawgo flags raw `go` statements outside the panic-recovery " +
			"wrappers in pkg/run. Use run.Go, run.GoOrDie, or " +
			"run.GoWithSignal so panics surface through diagnostics.",
		Run: s.run,
	}
	a.Flags.StringVar(&s.baselinePath, "baseline", "",
		"path to a baseline file listing pre-existing raw `go` sites that "+
			"are grandfathered in (one `path:line` entry per line)")
	a.Flags.BoolVar(&s.includeTests, "include-tests", true,
		"check files whose names end in _test.go")
	return a
}

func (s *state) run(pass *analysis.Pass) (interface{}, error) {
	if err := s.ensureBaseline(); err != nil {
		return nil, err
	}
	for _, f := range pass.Files {
		s.checkFile(pass, f)
	}
	return nil, nil
}

func (s *state) ensureBaseline() error {
	s.once.Do(func() {
		if s.baselinePath == "" {
			s.baseline = map[string]map[int]bool{}
			return
		}
		// A missing baseline file is not an error: the file is
		// gitignored and generated locally on demand, so a fresh
		// checkout will not have one. Treat absence as "no baseline"
		// (every site is checked) so the linter still runs end-to-end
		// and the developer sees what would be reported.
		if _, err := os.Stat(s.baselinePath); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr,
				"rawgo: baseline %q not found; checking all sites. "+
					"Generate one locally to grandfather pre-existing "+
					"violations (see pkg/panicdiag/lintrawgo).\n",
				s.baselinePath)
			s.baseline = map[string]map[int]bool{}
			return
		}
		s.baseline, s.loadErr = loadBaseline(s.baselinePath)
	})
	return s.loadErr
}

func (s *state) checkFile(pass *analysis.Pass, f *ast.File) {
	for _, d := range s.collect(pass.Fset, pass.Pkg.Path(), f) {
		pass.Report(d)
	}
}

// collect is the file-level check shared by the Pass-backed Run path and
// the standalone driver. It returns diagnostics rather than calling
// pass.Report so callers control reporting and exit semantics.
func (s *state) collect(fset *token.FileSet, importPath string, f *ast.File) []analysis.Diagnostic {
	pos := fset.Position(f.Pos())
	if pos.Filename == "" {
		return nil
	}
	abs, err := filepath.Abs(pos.Filename)
	if err != nil {
		abs = pos.Filename
	}
	slash := filepath.ToSlash(abs)

	if !s.includeTests && strings.HasSuffix(slash, "_test.go") {
		return nil
	}
	if isGeneratedPath(slash) || isGeneratedHeader(f) {
		return nil
	}
	if isAllowedByImportPath(importPath) {
		return nil
	}
	if isAllowedByFileBase(importPath, slash) {
		return nil
	}
	rel := moduleRelative(abs)
	directives := collectDirectives(fset, f)

	var out []analysis.Diagnostic
	ast.Inspect(f, func(n ast.Node) bool {
		stmt, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		line := fset.Position(stmt.Pos()).Line
		if directives[line] {
			return true
		}
		if s.baseline[rel][line] {
			return true
		}
		out = append(out, analysis.Diagnostic{
			Pos: stmt.Pos(),
			Message: "raw `go` statement: launch goroutines through " +
				"run.Go, run.GoOrDie, or run.GoWithSignal so panics are " +
				"recovered. To allow this site, add `" + directive +
				" <reason>` on the line above, or extend the baseline.",
		})
		return true
	})
	return out
}

// generatedSuffixes lists path suffixes that the project treats as
// generated. Mirrors the exclusions in .golangci.yml plus the gateway
// flavor that the protobuf+grpc-gateway toolchain emits.
var generatedSuffixes = []string{
	".pb.go",
	".pb.gw.go",
	".pb.validate.go",
	".gen.go",
	"_mock.go",
}

// isGeneratedPath returns true when the filename matches a known
// generated-file naming convention.
func isGeneratedPath(slashPath string) bool {
	for _, suffix := range generatedSuffixes {
		if strings.HasSuffix(slashPath, suffix) {
			return true
		}
	}
	return false
}

// isGeneratedHeader returns true when the file begins with the standard
// `// Code generated ... DO NOT EDIT.` marker. This is the language the
// Go toolchain itself uses to recognize generated files, so honoring it
// makes the analyzer robust against project-specific naming drift.
func isGeneratedHeader(f *ast.File) bool {
	for _, group := range f.Comments {
		// Only look at comments that appear before the package clause.
		if group.End() >= f.Package {
			break
		}
		for _, c := range group.List {
			text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
			text = strings.TrimSpace(text)
			if strings.HasPrefix(text, "Code generated") &&
				strings.Contains(text, "DO NOT EDIT") {
				return true
			}
		}
	}
	return false
}

// isAllowedByImportPath matches the package import path against
// allowedPackagePaths. Only paths under the project's module prefix can
// satisfy the allowlist, so synthetic testdata packages (which load
// under bare names like "badcode") never short-circuit the check.
func isAllowedByImportPath(full string) bool {
	if !strings.HasPrefix(full, modulePrefix) {
		return false
	}
	rel := strings.TrimPrefix(full, modulePrefix)
	for _, allowed := range allowedPackagePaths {
		if rel == allowed {
			return true
		}
	}
	return false
}

// isAllowedByFileBase matches a specific file basename inside an
// otherwise checked package. Used for pkg/run/goroutine.go, which
// contains the only legitimate raw-go callsites in pkg/run.
func isAllowedByFileBase(full, slashPath string) bool {
	if !strings.HasPrefix(full, modulePrefix) {
		return false
	}
	rel := strings.TrimPrefix(full, modulePrefix)
	files, ok := allowedFiles[rel]
	if !ok {
		return false
	}
	base := slashPath
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	return files[base]
}

// collectDirectives returns the set of line numbers immediately
// following a `//panicdiag:allow-rawgo` comment with a non-empty reason.
// The directive must occupy its own line so review tools surface it.
func collectDirectives(fset *token.FileSet, f *ast.File) map[int]bool {
	out := map[int]bool{}
	for _, group := range f.Comments {
		for _, c := range group.List {
			text := strings.TrimSpace(c.Text)
			if !strings.HasPrefix(text, directive) {
				continue
			}
			rest := strings.TrimSpace(strings.TrimPrefix(text, directive))
			if rest == "" {
				// No reason: refuse to suppress. The diagnostic at the
				// `go` line will still fire and reviewers see why.
				continue
			}
			out[fset.Position(c.End()).Line+1] = true
		}
	}
	return out
}

// loadBaseline parses a baseline file. Lines beginning with `#` or empty
// lines are ignored. Each remaining line must be `path:line` where path
// is module-relative (forward-slash) and line is a positive integer.
func loadBaseline(path string) (map[string]map[int]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open baseline %q: %w", path, err)
	}
	defer f.Close()

	out := map[string]map[int]bool{}
	scan := bufio.NewScanner(f)
	for n := 1; scan.Scan(); n++ {
		raw := strings.TrimSpace(scan.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		// Strip optional trailing comment after `#`.
		if idx := strings.Index(raw, "#"); idx >= 0 {
			raw = strings.TrimSpace(raw[:idx])
		}
		colon := strings.LastIndex(raw, ":")
		if colon < 0 {
			return nil, fmt.Errorf("baseline %s: line %d: missing `:`", path, n)
		}
		file := raw[:colon]
		var line int
		if _, err := fmt.Sscanf(raw[colon+1:], "%d", &line); err != nil || line <= 0 {
			return nil, fmt.Errorf("baseline %s: line %d: bad line number %q", path, n, raw[colon+1:])
		}
		if out[file] == nil {
			out[file] = map[int]bool{}
		}
		out[file][line] = true
	}
	if err := scan.Err(); err != nil {
		return nil, fmt.Errorf("read baseline %q: %w", path, err)
	}
	return out, nil
}

// moduleRelative converts an absolute path into a module-relative path
// (forward-slash) by walking up to the nearest go.mod. If no go.mod is
// found, the absolute path is returned. Caching keeps the walk cheap.
func moduleRelative(abs string) string {
	root := findModuleRoot(abs)
	if root == "" {
		return filepath.ToSlash(abs)
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return filepath.ToSlash(abs)
	}
	return filepath.ToSlash(rel)
}

var (
	moduleRootCache   sync.Map // dir -> root (or "")
	moduleRootResolve sync.Mutex
)

func findModuleRoot(abs string) string {
	dir := filepath.Dir(abs)
	if v, ok := moduleRootCache.Load(dir); ok {
		return v.(string)
	}
	moduleRootResolve.Lock()
	defer moduleRootResolve.Unlock()
	if v, ok := moduleRootCache.Load(dir); ok {
		return v.(string)
	}
	cur := dir
	for {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			moduleRootCache.Store(dir, cur)
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			moduleRootCache.Store(dir, "")
			return ""
		}
		cur = parent
	}
}
