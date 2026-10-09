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

package cmd_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

// dataTree fingerprints the part files and segment metadata below root. The inverted index
// directories (sidx/, idx/) are skipped: their writers persist and merge on their own
// schedule, so their bytes move even when nobody reads them; parts are immutable.
func dataTree(root string) map[string]string {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "sidx" || d.Name() == "idx" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(d.Name()) == ".tmp" {
			return nil
		}
		body, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		sum := sha256.Sum256(body)
		rel, _ := filepath.Rel(root, p)
		out[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return out
}

var _ = Describe("Data Export Dry Run", func() {
	var grpcAddr, httpAddr, dataDir, configFile string

	BeforeEach(func() {
		var deferFunc func()
		var err error
		dataDir, deferFunc, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		ports, err := test.AllocateFreePorts(5)
		Expect(err).NotTo(HaveOccurred())
		var closeFn func()
		grpcAddr, httpAddr, closeFn = setup.ClosableStandalone(nil, dataDir, ports, "--stream-flush-timeout=500ms")
		// Stop the server before its directories go away, then forget this test's flags.
		DeferCleanup(func() {
			closeFn()
			deferFunc()
			cmd.ResetFlags()
		})
		httpAddr = httpSchema + httpAddr
		// Data commands never read the config file; point HOME at an empty directory so a
		// test that checks this cannot be fooled by the developer's own file.
		home := filepath.Join(dataDir, "home")
		Expect(os.MkdirAll(home, 0o700)).To(Succeed())
		savedHome := os.Getenv("HOME")
		Expect(os.Setenv("HOME", home)).To(Succeed())
		DeferCleanup(func() { _ = os.Setenv("HOME", savedHome) })
		configFile = filepath.Join(home, ".bydbctl.yaml")
		// The specs choose the BYDBCTL_* fallbacks themselves; none may come from the caller.
		for _, key := range []string{"BYDBCTL_NODES", "BYDBCTL_PARALLELISM"} {
			if saved, ok := os.LookupEnv(key); ok {
				Expect(os.Unsetenv(key)).To(Succeed())
				DeferCleanup(func() { _ = os.Setenv(key, saved) })
			}
		}
		conn, err := grpclib.NewClient(grpcAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = conn.Close() })
		casesstreamdata.Write(conn, "sw", timestamp.NowMilli(), 500*time.Millisecond)
	})
	// run executes one bydbctl invocation on a fresh root command.
	run := func(args ...string) (string, string, error) {
		rootCmd := &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		cmd.SetTestArgs(args)
		rootCmd.SetArgs(args)
		var out, errBuf bytes.Buffer
		rootCmd.SetOut(&out)
		rootCmd.SetErr(&errBuf)
		err := rootCmd.Execute()
		return out.String(), errBuf.String(), err
	}

	It("renders the inventory table and leaves the data directory untouched", func() {
		liveDir := filepath.Join(dataDir, "stream", "data")
		Eventually(func(g Gomega) {
			out, stderr, err := run("data", "export", "--dry-run", "--nodes", grpcAddr, "--selector", "catalog=stream")
			g.Expect(err).NotTo(HaveOccurred(), stderr)
			g.Expect(out).To(ContainSubstring("NODE"))
			g.Expect(out).To(MatchRegexp(`stream\s+default\s+hot`))
			g.Expect(out).To(ContainSubstring("SNAPSHOT"))
			g.Expect(stderr).To(ContainSubstring("standalone=true"))
			g.Expect(stderr).To(MatchRegexp(`parallelism: (max -> )?1`))
		}, flags.EventuallyTimeout, time.Second).Should(Succeed())
		var before map[string]string
		Eventually(func() bool {
			first := dataTree(liveDir)
			time.Sleep(time.Second)
			before = dataTree(liveDir)
			return len(first) == len(before) && len(first) > 0
		}, flags.EventuallyTimeout, time.Second).Should(BeTrue(), "the data directory never went quiet")
		_, _, err := run("data", "export", "--dry-run", "--nodes", grpcAddr)
		Expect(err).NotTo(HaveOccurred())
		Expect(dataTree(liveDir)).To(Equal(before))
	})

	It("emits machine-readable JSON on stdout only", func() {
		out, _, err := run("data", "export", "--dry-run", "--nodes", grpcAddr, "-o", "json", "--parallelism", "4")
		Expect(err).NotTo(HaveOccurred())
		var rep map[string]any
		Expect(json.Unmarshal([]byte(out), &rep)).To(Succeed(), out)
		Expect(rep).To(HaveKey("rows"))
		Expect(rep).To(HaveKeyWithValue("standalone", true))
	})

	It("takes nodes from plan.yaml and lets --nodes override it", func() {
		planFile := filepath.Join(dataDir, "plan.yaml")
		Expect(os.WriteFile(planFile, []byte("connection:\n  nodes: [127.0.0.1:1]\nexport:\n  selectors:\n    - catalog: stream\n"), 0o600)).To(Succeed())
		_, _, err := run("data", "export", "--dry-run", "--plan", planFile)
		var exitErr *exporter.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "unreachable plan.yaml nodes must exit 2: %v", err)
		Expect(exitErr.Code).To(Equal(exporter.ExitPreflight))
		_, _, err = run("data", "export", "--dry-run", "--plan", planFile, "--nodes", grpcAddr)
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects the root -a flag, an explicit -g flag, unknown selectors and bad parallelism", func() {
		_, _, err := run("data", "export", "--dry-run", "-a", httpAddr, "--nodes", grpcAddr)
		Expect(err).To(MatchError(ContainSubstring("--nodes")))
		_, _, err = run("data", "export", "--dry-run", "-g", "default", "--nodes", grpcAddr)
		Expect(err).To(MatchError(ContainSubstring("--selector")))
		_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr, "--selector", "catalog=stream,groups=nope")
		Expect(err).To(MatchError(ContainSubstring("does not exist")))
		_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr, "--parallelism", "0")
		Expect(err).To(MatchError(ContainSubstring("--parallelism")))
		planFile := filepath.Join(dataDir, "plan.yaml")
		Expect(os.WriteFile(planFile, []byte("export:\n  parallelism: 0\n"), 0o600)).To(Succeed())
		_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr, "--plan", planFile)
		Expect(err).To(MatchError(ContainSubstring("export.parallelism")))
		// A clean run right after the rejected ones: none of their flags carries over.
		_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr)
		Expect(err).NotTo(HaveOccurred())
	})

	It("ignores a group persisted by `bydbctl use` but rejects the explicit flag", func() {
		Expect(os.WriteFile(configFile, []byte("group: default\n"), 0o600)).To(Succeed())
		_, _, err := run("data", "export", "--dry-run", "--nodes", grpcAddr)
		Expect(err).NotTo(HaveOccurred(), "a configured group must not break data commands")
		_, _, err = run("data", "export", "--dry-run", "-g", "default", "--nodes", grpcAddr)
		Expect(err).To(MatchError(ContainSubstring("-g/--group")))
	})

	It("validates -o and --id before connecting", func() {
		// 127.0.0.1:1 never answers: reaching the preflight would fail with exit 2, not 1.
		_, _, err := run("data", "export", "--dry-run", "--nodes", "127.0.0.1:1", "-o", "xml")
		var exitErr *exporter.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
		Expect(exitErr.Code).To(Equal(exporter.ExitUsage))
		Expect(err).To(MatchError(ContainSubstring("--output-format")))
		// The request rule admits an empty id (ACTION_LIST ignores it); release-session must not.
		for _, id := range []string{"FEED-FEED", ""} {
			_, _, err = run("data", "release-session", "--id", id, "--nodes", "127.0.0.1:1")
			Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
			Expect(exitErr.Code).To(Equal(exporter.ExitUsage), "%q", id)
			Expect(err).To(MatchError(ContainSubstring("--id")), "%q", id)
		}
	})

	It("refuses a real export and the flags that only a real export reads", func() {
		_, _, err := run("data", "export", "--nodes", grpcAddr)
		Expect(err).To(MatchError(ContainSubstring("--dry-run")))
		var exitErr *exporter.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
		Expect(exitErr.Code).To(Equal(exporter.ExitUsage))
		for _, flag := range []string{"--output=" + filepath.Join(dataDir, "out"), "--preempt", "--format=csv", "--include-schema=false"} {
			_, _, err = run("data", "export", "--dry-run", flag, "--nodes", grpcAddr)
			Expect(err).To(MatchError(ContainSubstring("unknown flag")), flag)
		}
	})

	It("lets --selector override the plan.yaml selectors and checks the plan.yaml ones", func() {
		planFile := filepath.Join(dataDir, "plan.yaml")
		Expect(os.WriteFile(planFile, []byte("export:\n  selectors:\n    - catalog: stream\n      groups: [nope]\n"), 0o600)).To(Succeed())
		_, _, err := run("data", "export", "--dry-run", "--nodes", grpcAddr, "--plan", planFile)
		Expect(err).To(MatchError(ContainSubstring("does not exist")))
		_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr, "--plan", planFile, "--selector", "catalog=stream")
		Expect(err).NotTo(HaveOccurred(), "--selector replaces the plan.yaml selectors")
		for body, want := range map[string]string{
			"export:\n  selectors:\n    - catalog: stream\n      groups: [\"\"]\n":                        "plan.yaml export.selectors[0]: empty group name",
			"export:\n  selectors:\n    - catalog: stream\n    - catalog: stream\n      groups: [a, a]\n": "plan.yaml export.selectors[1]: group \"a\" given twice",
		} {
			Expect(os.WriteFile(planFile, []byte(body), 0o600)).To(Succeed())
			_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr, "--plan", planFile)
			var exitErr *exporter.ExitError
			Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
			Expect(exitErr.Code).To(Equal(exporter.ExitUsage))
			Expect(err).To(MatchError(ContainSubstring(want)))
		}
	})

	It("ignores the config file and takes nodes from BYDBCTL_NODES below plan.yaml", func() {
		// No address anywhere: there is no default liaison, and no config file is created.
		_, stderr, err := run("data", "export", "--dry-run")
		var exitErr *exporter.ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
		Expect(exitErr.Code).To(Equal(exporter.ExitUsage))
		Expect(err).To(MatchError(ContainSubstring("no liaison address")))
		Expect(configFile).NotTo(BeAnExistingFile(), "a data command must not create the config file")
		Expect(stderr).NotTo(ContainSubstring("Using config file"))
		// Nodes in $HOME/.bydbctl.yaml are not used either.
		Expect(os.WriteFile(configFile, []byte("nodes:\n  - "+grpcAddr+"\n"), 0o600)).To(Succeed())
		_, _, err = run("data", "export", "--dry-run")
		Expect(err).To(MatchError(ContainSubstring("no liaison address")), "config nodes must be ignored")
		// An explicit --config is refused rather than silently ignored.
		_, _, err = run("--config", configFile, "data", "export", "--dry-run", "--nodes", grpcAddr)
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
		Expect(exitErr.Code).To(Equal(exporter.ExitUsage))
		Expect(err).To(MatchError(ContainSubstring("--config does not apply to data commands")))
		Expect(os.Setenv("BYDBCTL_NODES", grpcAddr)).To(Succeed())
		DeferCleanup(func() { _ = os.Unsetenv("BYDBCTL_NODES") })
		_, _, err = run("data", "export", "--dry-run")
		Expect(err).NotTo(HaveOccurred(), "BYDBCTL_NODES must be used")
		// plan.yaml beats the environment: its unreachable node is the only candidate.
		planFile := filepath.Join(dataDir, "plan.yaml")
		Expect(os.WriteFile(planFile, []byte("connection:\n  nodes: [127.0.0.1:1]\n"), 0o600)).To(Succeed())
		_, _, err = run("data", "export", "--dry-run", "--plan", planFile)
		Expect(errors.As(err, &exitErr)).To(BeTrue(), "%v", err)
		Expect(exitErr.Code).To(Equal(exporter.ExitPreflight))
	})

	It("names the source of an invalid parallelism", func() {
		Expect(os.Setenv("BYDBCTL_PARALLELISM", "0")).To(Succeed())
		DeferCleanup(func() { _ = os.Unsetenv("BYDBCTL_PARALLELISM") })
		_, _, err := run("data", "export", "--dry-run", "--nodes", grpcAddr)
		Expect(err).To(MatchError(ContainSubstring("BYDBCTL_PARALLELISM")))
		_, _, err = run("data", "export", "--dry-run", "--nodes", grpcAddr, "--parallelism", "2")
		Expect(err).NotTo(HaveOccurred(), "the flag beats the environment")
	})

	It("releases an export session created through the liaison", func() {
		conn, err := grpclib.NewClient(grpcAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = conn.Close() }()
		create := &transferv1.PlanRequest{Session: &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{}}}
		stream, err := transferv1.NewExportServiceClient(conn).Plan(context.Background(), create)
		Expect(err).NotTo(HaveOccurred())
		first, err := stream.Recv()
		Expect(err).NotTo(HaveOccurred())
		id := first.GetCreated().GetSessionId()
		Expect(id).NotTo(BeEmpty())
		for {
			if _, recvErr := stream.Recv(); recvErr != nil {
				break
			}
		}
		lease := filepath.Join(dataDir, "stream", "export-snapshots", id, ".lease")
		Expect(lease).To(BeAnExistingFile())

		out, _, err := run("data", "release-session", "--id", id, "--nodes", grpcAddr)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("export session " + id + " released\n"))
		Expect(lease).NotTo(BeAnExistingFile())
		// Releasing again, or an id that never existed, deletes nothing: no data node holds it,
		// so the command says so and exits 2.
		for _, gone := range []string{id, strings.Repeat("ab", 8)} {
			_, _, err = run("data", "release-session", "--id", gone, "--nodes", grpcAddr)
			var exitErr *exporter.ExitError
			Expect(errors.As(err, &exitErr)).To(BeTrue(), "want an exit error, got %v", err)
			Expect(exitErr.Code).To(Equal(exporter.ExitPreflight))
			Expect(err).To(MatchError(ContainSubstring("export session " + gone + " is not held on any data node")))
		}
		_, _, err = run("data", "release-session", "extra", "--id", id, "--nodes", grpcAddr)
		Expect(err).To(MatchError(ContainSubstring("unknown command")))
	})
})
