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

var _ = Describe("IndexRuleBindingSchema Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.SetUp()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		// extracting the operation of creating indexRuleBinding schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		createGroup := func() string {
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
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createGroup, flags.EventuallyTimeout).Should(ContainSubstring("group group1 is created"))
		rootCmd.SetArgs([]string{"indexRuleBinding", "create", "-a", addr, "-f", "-"})
		createIndexRuleBinding := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
subject:
  catalog: CATALOG_STREAM
  name: stream1`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createIndexRuleBinding, flags.EventuallyTimeout).Should(ContainSubstring("indexRuleBinding group1.name1 is created"))
	})

	It("get indexRuleBinding schema", func() {
		rootCmd.SetArgs([]string{"indexRuleBinding", "get", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		resp := new(database_v1.IndexRuleBindingRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.IndexRuleBinding.Metadata.Group).To(Equal("group1"))
		Expect(resp.IndexRuleBinding.Metadata.Name).To(Equal("name1"))
		Expect(resp.IndexRuleBinding.Subject.Name).To(Equal("stream1"))
	})

	It("update indexRuleBinding schema", func() {
		rootCmd.SetArgs([]string{"indexRuleBinding", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
subject:
  catalog: CATALOG_STREAM
  name: stream2`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("indexRuleBinding group1.name1 is updated"))
		rootCmd.SetArgs([]string{"indexRuleBinding", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.IndexRuleBindingRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.IndexRuleBinding.Metadata.Group).To(Equal("group1"))
		Expect(resp.IndexRuleBinding.Metadata.Name).To(Equal("name1"))
		Expect(resp.IndexRuleBinding.Subject.Name).To(Equal("stream2"))
	})

	It("delete indexRuleBinding schema", func() {
		// delete
		rootCmd.SetArgs([]string{"indexRuleBinding", "delete", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("indexRuleBinding group1.name1 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"indexRuleBinding", "get", "-g", "group1", "-n", "name1"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})

	It("list indexRuleBinding schema", func() {
		// create another indexRuleBinding schema for list operation
		rootCmd.SetArgs([]string{"indexRuleBinding", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1
subject:
  catalog: CATALOG_STREAM
  name: stream2`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("indexRuleBinding group1.name2 is created"))
		// list
		rootCmd.SetArgs([]string{"indexRuleBinding", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.IndexRuleBindingRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.IndexRuleBinding).To(HaveLen(2))
	})

	AfterEach(func() {
		deferFunc()
	})
})
