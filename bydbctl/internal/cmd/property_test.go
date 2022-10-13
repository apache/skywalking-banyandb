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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"

	property_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = Describe("Property Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.SetUp()
		Eventually(helpers.HTTPHealthCheck(addr), 10*time.Second).Should(Succeed())
		addr = "http://" + addr
		// extracting the operation of creating property schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"property", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
 container:
	group: group1
	name: name1
 id: subgroup1`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property property1 is created"))

		It("get property", func() {
			rootCmd.SetArgs([]string{"property", "get", "-g", "group1", "-n", "property1"})
			out := capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
			GinkgoWriter.Println(out)
			resp := new(property_v1.ApplyRequest)
			helpers.UnmarshalYAML([]byte(out), resp)
			Expect(resp.Property.Metadata.Container.Group).To(Equal("group1"))
			Expect(resp.Property.Metadata.Container.Name).To(Equal("property1"))
			Expect(resp.Property.Metadata.Id).To(Equal("subgroup1"))
		})

		It("update property", func() {
			rootCmd.SetArgs([]string{"property", "update", "-f", "-"})
			rootCmd.SetIn(strings.NewReader(`
metadata:
container:
	group: group1
	name: name1
id: subgroup1`))
			out := capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
			Expect(out).To(ContainSubstring("property group1.property1 is updated"))
			rootCmd.SetArgs([]string{"property", "get", "-g", "group1", "-n", "property1"})
			out = capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
			resp := new(property_v1.ApplyRequest)
			helpers.UnmarshalYAML([]byte(out), resp)
			Expect(resp.Property.Metadata.Container.Group).To(Equal("group1"))
			Expect(resp.Property.Metadata.Id).To(Equal("property1"))
			Expect(resp.Property.Metadata.Container.Name).To(Equal("subgroup1"))
		})

		It("delete property", func() {
			// delete
			rootCmd.SetArgs([]string{"property", "delete", "-g", "group1", "-n", "property1"})
			out := capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
			Expect(out).To(ContainSubstring("property group1.property1 is deleted"))
			// get again
			rootCmd.SetArgs([]string{"property", "get", "-g", "group1", "-n", "property1"})
			err := rootCmd.Execute()
			Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
		})

		It("list property", func() {
			// create another property for list operation
			rootCmd.SetArgs([]string{"property", "create", "-f", "-"})
			rootCmd.SetIn(strings.NewReader(`
metadata:
 property: property2
 group: group1`))
			out := capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
			Expect(out).To(ContainSubstring("property group1.property2 is created"))
			// list
			rootCmd.SetArgs([]string{"property", "list", "-g", "group1"})
			out = capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
			resp := new(property_v1.ListResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			Expect(resp.Property).To(HaveLen(2))
		})

		AfterEach(func() {
			deferFunc()
		})
	})
})
