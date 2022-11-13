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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"

	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = Describe("Group", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.SetUp()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		// extracting the operation of creating group
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1
catalog: CATALOG_STREAM
resource_opts:
  shard_num: 2
  block_interval:
    unit: UNIT_HOUR
    num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("group group1 is created"))
	})

	It("get group", func() {
		rootCmd.SetArgs([]string{"group", "get", "-g", "group1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.GroupRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Group.Metadata.Name).To(Equal("group1"))
	})

	It("update group", func() {
		rootCmd.SetArgs([]string{"group", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1
catalog: CATALOG_STREAM
resource_opts:
  shard_num: 1
  block_interval:
    unit: UNIT_HOUR
    num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("group group1 is updated"))
	})

	It("delete group", func() {
		rootCmd.SetArgs([]string{"group", "delete", "-g", "group1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("group group1 is deleted"))
	})

	It("list group", func() {
		// create another group for list operation
		rootCmd.SetArgs([]string{"group", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group2
catalog: CATALOG_STREAM
resource_opts:
  shard_num: 2
  block_interval:
    unit: UNIT_HOUR
    num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("group group2 is created"))
		// list
		rootCmd.SetArgs([]string{"group", "list"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.GroupRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Group).To(HaveLen(4))
	})

	AfterEach(func() {
		deferFunc()
	})
})
