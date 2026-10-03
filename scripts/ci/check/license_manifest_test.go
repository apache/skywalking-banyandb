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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// script returns the absolute path of the license artifact verifier.
func script(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("license_manifest.sh requires a POSIX shell; the Windows path is exercised by the container")
	}
	abs, err := filepath.Abs("license_manifest.sh")
	require.NoError(t, err)
	return abs
}

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{script(t)}, args...)...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// tree writes files, keyed by path relative to the root, into a fresh temp dir.
func tree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	return root
}

func TestManifestIsSortedAndPathRelative(t *testing.T) {
	root := tree(t, map[string]string{
		"mcp/licenses/license-b.txt":     "bbb",
		"mcp/licenses/license-a.txt":     "aaa",
		"canopy/LICENSE":                 "canopy",
		"dist/licenses/license-d.txt":    "ddd",
		"unrelated/should-be-omitted.md": "not an artifact",
	})
	out, err := run(t, "manifest", root)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 4, "only the declared artifacts belong in the manifest: %s", out)

	var paths []string
	for _, line := range lines {
		fields := strings.SplitN(line, "  ", 2)
		require.Len(t, fields, 2)
		require.Regexp(t, `^[0-9a-f]{64}$`, fields[0], "raw sha256 of the file bytes")
		paths = append(paths, fields[1])
	}
	assert.Equal(t, []string{
		"canopy/LICENSE",
		"dist/licenses/license-d.txt",
		"mcp/licenses/license-a.txt",
		"mcp/licenses/license-b.txt",
	}, paths, "manifest is sorted by path and uses root-relative paths")
}

func TestManifestIsRepeatable(t *testing.T) {
	// Same tree, two runs: catches generator-side nondeterminism that a single
	// run cannot show. Distinct content per file so ordering cannot mask a swap.
	files := map[string]string{}
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		files["mcp/licenses/license-"+n+".txt"] = "content-" + n
	}
	root := tree(t, files)
	first, err := run(t, "manifest", root)
	require.NoError(t, err)
	second, err := run(t, "manifest", root)
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestManifestDetectsSingleByteChange(t *testing.T) {
	root := tree(t, map[string]string{"mcp/licenses/license-a.txt": "aaa"})
	before, err := run(t, "manifest", root)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(root, "mcp", "licenses", "license-a.txt"), []byte("aab"), 0o600))
	after, err := run(t, "manifest", root)
	require.NoError(t, err)
	assert.NotEqual(t, before, after, "a one-byte content change must change the manifest")
}

func TestManifestDetectsAddedAndRemovedFiles(t *testing.T) {
	// Comparing complete path sets is the reason this is not a per-file hash
	// loop: an added or removed file has to be visible too.
	base := tree(t, map[string]string{"mcp/licenses/license-a.txt": "aaa"})
	withExtra := tree(t, map[string]string{
		"mcp/licenses/license-a.txt": "aaa",
		"mcp/licenses/license-z.txt": "zzz",
	})
	one, err := run(t, "manifest", base)
	require.NoError(t, err)
	two, err := run(t, "manifest", withExtra)
	require.NoError(t, err)
	assert.NotEqual(t, one, two)
	assert.Len(t, strings.Split(strings.TrimSpace(two), "\n"), 2, "the added file is a second line")
}

func TestCheckEOlFlagsCRLF(t *testing.T) {
	clean := tree(t, map[string]string{"mcp/licenses/license-a.txt": "one\ntwo\n"})
	_, err := run(t, "check-eol", clean)
	require.NoError(t, err, "an LF artifact passes")

	crlf := tree(t, map[string]string{"mcp/licenses/license-a.txt": "one\r\ntwo\r\n"})
	out, err := run(t, "check-eol", crlf)
	require.Error(t, err, "a CR byte fails regardless of what Git would do with it")
	assert.Contains(t, out, "mcp/licenses/license-a.txt")
}

func TestCheckEOLFlagsCRWithoutCRLF(t *testing.T) {
	// A lone CR is invisible to any `grep $'\r\n'` style check and still
	// breaks a byte comparison.
	lone := tree(t, map[string]string{"canopy/LICENSE": "a\rb"})
	out, err := run(t, "check-eol", lone)
	require.Error(t, err)
	assert.Contains(t, out, "canopy/LICENSE")
}

func TestCheckCoverageAcceptsKnownProjects(t *testing.T) {
	root := tree(t, map[string]string{
		"mcp/Makefile": "license-dep:\n\techo hi\n",
		"mcp/go.mod":   "module x\n",
	})
	_, err := run(t, "check-coverage", root)
	require.NoError(t, err, "mcp is a known project")
}

func TestCheckCoverageRejectsUnknownProject(t *testing.T) {
	// A project that gains a license-dep target without an entry in
	// LICENSE_PATHS would otherwise have its licenses silently unverified.
	root := tree(t, map[string]string{
		"brandnew/Makefile": "license-dep:\n\techo hi\n",
	})
	out, err := run(t, "check-coverage", root)
	require.Error(t, err)
	assert.Contains(t, out, "brandnew")
}

// gitTree initialises a repository with one committed artifact.
func gitTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := tree(t, files)
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "git %v: %s", args, out)
	}
	git("init", "-q")
	git("config", "user.email", "test@example.invalid")
	git("config", "user.name", "test")
	git("add", "-A")
	git("commit", "-q", "-m", "initial")
	return root
}

func TestManifestCommittedMatchesTheWorkingTree(t *testing.T) {
	// This is the assertion the cross-host matrix ultimately makes: what a
	// generator produced on any host equals what is committed.
	root := gitTree(t, map[string]string{
		"mcp/LICENSE":                   "mcp license\n",
		"mcp/licenses/license-a.txt":    "aaa\n",
		"canopy/licenses/license-b.txt": "bbb\n",
	})
	fromDisk, err := run(t, "manifest", root)
	require.NoError(t, err)
	fromBlobs, err := run(t, "manifest-committed", root)
	require.NoError(t, err)
	assert.Equal(t, fromDisk, fromBlobs)
}

func TestManifestCommittedReflectsTheBlobNotTheWorktree(t *testing.T) {
	// With `eol=lf` a checkout can legitimately hold different bytes from the
	// blob. The committed manifest must describe the blob.
	root := gitTree(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	before, err := run(t, "manifest-committed", root)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(root, "mcp", "licenses", "license-a.txt"), []byte("zzz\n"), 0o600))
	after, err := run(t, "manifest-committed", root)
	require.NoError(t, err)
	assert.Equal(t, before, after, "editing the working tree must not change the committed manifest")
}

func TestNormalizeRewritesCRLFOnly(t *testing.T) {
	root := tree(t, map[string]string{
		"mcp/licenses/license-crlf.txt": "one\r\ntwo\r\n",
		"mcp/licenses/license-lf.txt":   "one\ntwo\n",
	})
	out, err := run(t, "normalize", root)
	require.NoError(t, err)
	assert.Contains(t, out, "license-crlf.txt")
	assert.NotContains(t, out, "license-lf.txt", "an LF file needs no work")

	after, err := run(t, "manifest", root)
	require.NoError(t, err)
	assert.Equal(t, 2, len(strings.Split(strings.TrimSpace(after), "\n")))
	_, err = run(t, "check-eol", root)
	require.NoError(t, err, "check-eol passes after normalize")

	// The LF file's bytes must be untouched, not merely end up LF.
	lf, err := os.ReadFile(filepath.Join(root, "mcp", "licenses", "license-lf.txt"))
	require.NoError(t, err)
	assert.Equal(t, "one\ntwo\n", string(lf))
}

// gitRepo initialises a repository, commits `files`, and returns its path.
func gitRepo(t *testing.T, files map[string]string) string {
	t.Helper()
	root := tree(t, files)
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "git %v: %s", args, out)
	}
	git("init", "-q")
	git("config", "user.email", "t@example.invalid")
	git("config", "user.name", "t")
	git("add", "-A")
	git("commit", "-q", "-m", "initial")
	return root
}

func gitIn(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)
}

func TestCheckCommittedRejectsAStagedAddition(t *testing.T) {
	// The regression this guards: reading `:$path` (the index) instead of a commit
	// made a staged file look already-committed, so an artifact the generator
	// never produced could pass verification.
	root := gitRepo(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "mcp", "licenses", "license-new.txt"), []byte("staged only\n"), 0o600))
	gitIn(t, root, "add", "mcp/licenses/license-new.txt")

	out, err := run(t, "check-committed", root)
	require.Error(t, err, "a staged addition is not committed")
	assert.Contains(t, out, "license-new.txt")
}

func TestCheckCommittedRejectsAStagedModification(t *testing.T) {
	// Same hole, other direction: a staged edit must not become the expectation.
	root := gitRepo(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "mcp", "licenses", "license-a.txt"), []byte("bbb\n"), 0o600))
	gitIn(t, root, "add", "mcp/licenses/license-a.txt")

	out, err := run(t, "check-committed", root)
	require.Error(t, err, "a staged modification is not committed")
	assert.Contains(t, out, "differs from committed blob")
}

func TestCheckCommittedDetectsAStagedDeletion(t *testing.T) {
	// `git rm` on a generated artifact is the silent-resolution-failure shape: it
	// leaves the index and the worktree agreeing with each other, so `git status`
	// looks clean, while the commit still promises the file. Comparing against a
	// commit rather than the index is what catches it.
	root := gitRepo(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	gitIn(t, root, "rm", "-q", "mcp/licenses/license-a.txt")

	// The index and the worktree now agree, so an index-based check would pass.
	_, err := runGitE(t, root, "status", "--porcelain")
	require.NoError(t, err)

	out, err := run(t, "check-committed", root)
	require.Error(t, err)
	assert.Contains(t, out, "missing from the tree")
}

func TestCheckCommittedReportsAFileDeletedFromTheTree(t *testing.T) {
	// The silent-npm-failure shape: committed, expected, gone from disk.
	root := gitRepo(t, map[string]string{
		"mcp/LICENSE":                "mcp\n",
		"mcp/licenses/license-a.txt": "aaa\n",
	})
	require.NoError(t, os.Remove(filepath.Join(root, "mcp", "licenses", "license-a.txt")))

	out, err := run(t, "check-committed", root)
	require.Error(t, err)
	assert.Contains(t, out, "missing from the tree")
}

func TestManifestCommittedIgnoresAStagedAddition(t *testing.T) {
	// The manifest must describe the commit, so a staged file is absent from it.
	root := gitRepo(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	before, err := run(t, "manifest-committed", root)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "mcp", "licenses", "license-new.txt"), []byte("staged only\n"), 0o600))
	gitIn(t, root, "add", "mcp/licenses/license-new.txt")

	after, err := run(t, "manifest-committed", root)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.NotContains(t, after, "license-new.txt")
}

func TestCheckCommittedHonoursAnExplicitRef(t *testing.T) {
	// A second commit moves HEAD, and the tree now matches it. Naming the
	// previous commit describes a different state, and must report the
	// difference rather than silently following HEAD.
	root := gitRepo(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	first := strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD"))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "mcp", "licenses", "license-a.txt"), []byte("bbb\n"), 0o600))
	gitIn(t, root, "add", "-A")
	gitIn(t, root, "commit", "-q", "-m", "second")

	out, err := run(t, "check-committed", root)
	require.NoError(t, err, "HEAD matches the tree after committing: "+out)

	out, err = run(t, "check-committed", root, first)
	require.Error(t, err, "the older commit does not match the tree")
	assert.Contains(t, out, "differs from committed blob")
}

func TestCheckCommittedRejectsAnUnknownRef(t *testing.T) {
	root := gitRepo(t, map[string]string{"mcp/licenses/license-a.txt": "aaa\n"})
	out, err := run(t, "check-committed", root, "no-such-ref")
	require.Error(t, err)
	assert.Contains(t, out, "cannot resolve the reference")
}

func runGitE(t *testing.T, root string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func runGit(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)
	return string(out)
}
