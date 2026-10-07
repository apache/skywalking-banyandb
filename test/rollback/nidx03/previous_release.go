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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

// Package nidx03rollback proves NIDX-03's §12 item 2 rollback claim: a
// directory the new (native-only) binary writes still opens, and returns the
// same query results, under the previous release. NIDX-03 §1 names "the
// previous release" as v0.11.1 specifically -- the last published release,
// which runs the series index, the Stream element index, and Property on the
// retired bluge engine, with none of #1383/#1390/this change's native
// writers. Unreleased main commits (for example 735e9ad2, which already
// carries native Property and a native Stream element index) are not
// rollback targets.
//
// This file builds that previous release from source on demand: a
// `git archive <tag>` export, patched only with what a bare export is
// missing to build at all (generated protobuf/mock code, a placeholder
// embedded UI asset), never with anything that changes its behavior.
package nidx03rollback

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

const (
	// envPreviousReleaseTag overrides the previous-release tag the rollback
	// proof builds and runs. Defaults to PreviousReleaseTag (v0.11.1, NIDX-03
	// §1's own answer to "the previous release").
	envPreviousReleaseTag = "NIDX03_ROLLBACK_PREVIOUS_RELEASE_TAG"
	// envWorkDir overrides where the previous-release source export and its
	// build output live. Defaults to a fresh t.TempDir() -- there is no
	// machine-specific default path; set this only to cache the export
	// across repeated local runs.
	envWorkDir = "NIDX03_ROLLBACK_WORKDIR"
	// PreviousReleaseTag is NIDX-03 §1's rollback target: the last published
	// release, predating #1383 (Property), #1390 (Stream element index) and
	// this change (the series index) going native.
	PreviousReleaseTag = "v0.11.1"
)

// PreviousReleaseBinary builds the previous release's `banyand` server
// binary from a fresh git archive export of previousReleaseTag (or
// envPreviousReleaseTag's override), inside the current repository (the
// worktree this test itself runs from -- git archive needs no network
// access and no second checkout). It returns the built binary's path.
//
// The export needs two patches a bare `git archive` tree is missing to
// build at all, neither of which changes runtime behavior:
//   - generated protobuf/gRPC and mock code (`make generate`, using the
//     export's own pinned toolchain, not the current tree's).
//   - a non-empty `ui/dist` directory, because `ui/embed.go` is a Go
//     `//go:embed dist` directive that fails to compile against an empty or
//     absent directory; the previous release's own build pipeline normally
//     populates it from a separate frontend build this test does not need.
func PreviousReleaseBinary(t *testing.T) string {
	t.Helper()
	exportDir := PreviousReleaseExport(t)
	workDir := filepath.Dir(exportDir)
	tag := previousReleaseTagFromEnv()
	binaryPath := filepath.Join(workDir, "banyand-"+tag)
	if info, statErr := os.Stat(binaryPath); statErr == nil && !info.IsDir() {
		// A cached workDir (envWorkDir reused across runs) already has a
		// built binary for this tag; reuse it rather than rebuild.
		return binaryPath
	}
	runIn(t, exportDir, "go", "build", "-o", binaryPath, "./banyand/cmd/server/")
	return binaryPath
}

// PreviousReleaseExport returns a buildable `git archive` export of the
// previous release, patched only as PreviousReleaseBinary's doc comment
// describes (generated code, a placeholder embedded UI asset) -- never with
// anything that changes runtime behavior. Reused by both
// PreviousReleaseBinary (the `banyand` server, for the gRPC-level proof) and
// the series-index-level proof (StagePreviousReleaseSeriesIndexTest), which
// needs the export's own `go test` toolchain, not a built binary.
func PreviousReleaseExport(t *testing.T) string {
	t.Helper()
	tag := previousReleaseTagFromEnv()
	workDir := os.Getenv(envWorkDir)
	if workDir == "" {
		workDir = t.TempDir()
	} else {
		require.NoError(t, os.MkdirAll(workDir, 0o755))
	}
	exportDir := filepath.Join(workDir, "export-"+tag)
	if info, statErr := os.Stat(filepath.Join(exportDir, "go.mod")); statErr == nil && !info.IsDir() {
		// A cached workDir already has this tag exported and generated.
		return exportDir
	}

	repoRoot := repositoryRoot(t)
	require.NoError(t, os.MkdirAll(exportDir, 0o755))
	runIn(t, repoRoot, "sh", "-c", fmt.Sprintf("git archive %s | tar -x -C %s", shellQuote(tag), shellQuote(exportDir)))

	// Generated code: the export's own Makefile, targeting its own pinned
	// tool versions. "not a git repository" warnings from sub-make targets
	// that try to stamp a version from git are expected and harmless here
	// (the export has no .git directory); generation itself still runs.
	runIn(t, exportDir, "make", "generate")

	// Placeholder embedded UI asset: never read by anything this test
	// exercises (the gRPC and storage paths under test), so its content is
	// irrelevant -- it only has to exist and be non-empty.
	distDir := filepath.Join(exportDir, "ui", "dist")
	require.NoError(t, os.MkdirAll(distDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(distDir, "index.html"), []byte("<!doctype html><html></html>"), 0o600))

	return exportDir
}

func previousReleaseTagFromEnv() string {
	if override := os.Getenv(envPreviousReleaseTag); override != "" {
		return override
	}
	return PreviousReleaseTag
}

// repositoryRoot locates the module root from this file's own path, so
// PreviousReleaseBinary works regardless of the working directory `go test`
// was invoked from.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	// This file lives at <root>/test/rollback/nidx03/previous_release.go.
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Join(wd, "..", "..", "..")
}

func runIn(t *testing.T, dir string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "%s %v (in %s):\n%s", name, args, dir, output)
}

func shellQuote(s string) string {
	return "'" + s + "'"
}

// startPreviousRelease starts the previous release's `banyand` server
// binary, built by PreviousReleaseBinary, as a real OS process -- it is a
// different binary than the one running this test, so it cannot run
// in-process the way pkg/test/setup's helpers run the current tree's
// standalone server. It points every root path at dataDir (a directory the
// current, native-only binary already wrote to and closed) and reuses the
// same ports the caller's own standalone server used, exactly as an
// operator's rollback (stop the new binary, start the old one on the same
// data directories and the same address) would.
//
// It returns the gRPC address once the previous release's own health check
// reports serving, and a function that sends it SIGTERM and waits for exit.
func startPreviousRelease(t *testing.T, binaryPath, dataDir, discoveryFilePath string, ports []int) (addr string, closeFn func()) {
	t.Helper()
	addr = fmt.Sprintf("127.0.0.1:%d", ports[0])
	httpAddr := fmt.Sprintf("127.0.0.1:%d", ports[1])
	// Mirrors pkg/test/setup's own standaloneServerWithAuth flag set exactly
	// (schema-registry-mode=property, node-discovery-mode=file, the schema
	// server's own grpc listener): without these, the previous release
	// defaults to a schema source that never sees the schemas the new code
	// registered as Property documents in the same dataDir, and every query
	// fails "measure doesn't exist" even though the data itself is present.
	//nolint:gosec // binaryPath is this process's own PreviousReleaseBinary
	// build output and every flag value is either a fixed literal or a
	// test.AllocateFreePorts/test.NewSpace-derived path, not external input.
	cmd := exec.Command(binaryPath, "standalone",
		"--logging-env=dev", "--logging-level=warn",
		"--grpc-host=127.0.0.1", fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--http-host=127.0.0.1", fmt.Sprintf("--http-port=%d", ports[1]),
		"--http-grpc-addr="+addr,
		"--stream-root-path="+dataDir, "--measure-root-path="+dataDir,
		"--property-root-path="+dataDir, "--trace-root-path="+dataDir,
		"--schema-server-root-path="+dataDir,
		"--schema-registry-mode=property",
		"--node-discovery-mode=file",
		"--node-discovery-file-path="+discoveryFilePath,
		"--node-host-provider=flag", "--node-host=127.0.0.1",
		"--schema-server-grpc-host=127.0.0.1", fmt.Sprintf("--schema-server-grpc-port=%d", ports[4]),
	)
	logPath := filepath.Join(t.TempDir(), "previous-release-server.log")
	logFile, openErr := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, openErr)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())

	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()

	require.Eventually(t, func() bool {
		return helpers.HealthCheck(addr, 2*time.Second, 2*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))() == nil
	}, 30*time.Second, 200*time.Millisecond, "previous release %s did not become healthy; see %s", binaryPath, logPath)
	_ = httpAddr // reserved for parity with standaloneServerWithAuth's flag set; not polled here.

	return addr, func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		_ = logFile.Close()
	}
}
