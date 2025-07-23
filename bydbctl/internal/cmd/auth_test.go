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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/google/uuid"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"
	"sigs.k8s.io/yaml"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	serverAuth "github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/bydbctl/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var testUser serverAuth.User

// TODO check.
var _ = g.Describe("bydbctl test with authentication", func() {
	var deferFn func()
	var goods []gleak.Goroutine
	var httpAddr string
	var authCfgFile string
	var baseDir string
	var bydbctlCfgFile string
	var rootCmd *cobra.Command

	g.BeforeEach(func() {
		_, currentFile, _, _ := runtime.Caller(0)
		baseDir = filepath.Dir(currentFile)
		authCfgFile = filepath.Join(baseDir, "../../../test/integration/standalone/other/testdata/config.yaml")
		// load server config.yaml
		cfgBytes, err := os.ReadFile(authCfgFile)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		var cfg serverAuth.Config
		err = yaml.Unmarshal(cfgBytes, &cfg)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		// take the first user as the test user
		gm.Expect(len(cfg.Users)).Should(gm.BeNumerically(">", 0))
		testUser = cfg.Users[0]
		// Username and password must be provided because health checks require authentication when --enable-health-auth=true is enabled.
		_, httpAddr, deferFn = setup.EmptyStandaloneWithAuth(
			testUser.Username, testUser.Password,
			fmt.Sprintf("--auth-config-file=%s", authCfgFile),
			"--enable-health-auth=true",
		)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		httpAddr = httpSchema + httpAddr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		goods = gleak.Goroutines()
	})

	g.It("list groups and health check with correct username and password from command line", func() {
		originalPrompt := auth.PromptForPassword
		defer func() { auth.PromptForPassword = originalPrompt }() // Restore after the test
		// Mock the PromptForPassword function to return a predefined password without user input
		auth.PromptForPassword = func() (string, error) {
			return testUser.Password, nil
		}
		rootCmd.SetArgs([]string{"group", "list", "-a", httpAddr, "-u", testUser.Username, "-p", testUser.Password})
		err := rootCmd.Execute()
		gm.Expect(err).NotTo(gm.HaveOccurred())

		rootCmd.SetArgs([]string{"health", "-a", httpAddr, "-u", testUser.Username, "-p", testUser.Password})
		err = rootCmd.Execute()
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})

	g.It("list groups and health check with wrong username and password", func() {
		// it will send http request with username = "admin" and password = ""
		rootCmd.SetArgs([]string{"group", "list", "-a", httpAddr, "-u", "admin", "-p", testUser.Password + "wrong"})
		err := rootCmd.Execute()
		gm.Expect(err).To(gm.HaveOccurred())

		rootCmd.SetArgs([]string{"health", "-a", httpAddr, "-u", "admin", "-p", testUser.Password + "wrong"})
		err = rootCmd.Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	g.It("create and get a group and health check with correct username and password in bydbctlCfgFile", func() {
		bydbctlCfgFile = filepath.Join(baseDir, "../../../test/integration/standalone/other/testdata/.bydbctl.yaml")
		// Copy .bydbctl.yaml to /tmp directory to avoid permission modification failure under WSL
		tempBydbctl := filepath.Join(os.TempDir(), fmt.Sprintf(".bydbctl-%s.yaml", uuid.New().String()))
		input, err := os.ReadFile(bydbctlCfgFile)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = os.WriteFile(tempBydbctl, input, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		bydbctlCfgFile = tempBydbctl
		info, _ := os.Stat(bydbctlCfgFile)
		gm.Expect(info.Mode().Perm()).To(gm.Equal(os.FileMode(0o600)))

		rootCmd.SetArgs([]string{"--config", bydbctlCfgFile, "group", "create", "-a", httpAddr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1
catalog: CATALOG_STREAM
resource_opts:
  shard_num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7`))
		out := capturer.CaptureStdout(func() {
			err = rootCmd.Execute()
			gm.Expect(err).NotTo(gm.HaveOccurred())
		})
		gm.Expect(out).To(gm.ContainSubstring("group group1 is created"))

		rootCmd.SetArgs([]string{"--config", bydbctlCfgFile, "group", "-a", httpAddr, "get", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err = rootCmd.Execute()
			gm.Expect(err).NotTo(gm.HaveOccurred())
		})
		resp := new(databasev1.GroupRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		gm.Expect(resp.Group.Metadata.Name).To(gm.Equal("group1"))

		rootCmd.SetArgs([]string{"health", "-a", httpAddr})
		err = rootCmd.Execute()
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})

	g.It("list groups and health check with wrong username and password in bydbctlCfgFile", func() {
		bydbctlCfgFile = filepath.Join(baseDir, "../../../test/integration/standalone/other/testdata/.bydbctl1.yaml")
		// Copy .bydbctl.yaml to /tmp directory to avoid permission modification failure under WSL
		tempBydbctl := filepath.Join(os.TempDir(), fmt.Sprintf(".bydbctl-%s.yaml", uuid.New().String()))
		input, err := os.ReadFile(bydbctlCfgFile)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = os.WriteFile(tempBydbctl, input, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		bydbctlCfgFile = tempBydbctl
		info, _ := os.Stat(bydbctlCfgFile)
		gm.Expect(info.Mode().Perm()).To(gm.Equal(os.FileMode(0o600)))

		rootCmd.SetArgs([]string{"--config", bydbctlCfgFile, "group", "list", "-a", httpAddr})
		err = rootCmd.Execute()
		gm.Expect(err).To(gm.HaveOccurred())

		rootCmd.SetArgs([]string{"health", "-a", httpAddr})
		err = rootCmd.Execute()
		gm.Expect(err).To(gm.HaveOccurred())
	})

	g.AfterEach(func() {
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})
})
