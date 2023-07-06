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

	"github.com/google/go-cmp/cmp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"
	"google.golang.org/protobuf/testing/protocmp"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = Describe("Property Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	p1YAML := `
metadata:
  container:
    group: ui-template 
    name: service
  id: kubernetes
tags:
  - key: content
    value:
      str:
        value: foo 
  - key: state
    value:
      int:
        value: 1
`
	p2YAML := `
metadata:
  container:
    group: ui-template 
    name: service
  id: kubernetes
tags:
  - key: content
    value:
      str:
        value: foo 
  - key: state
    value:
      int:
        value: 3
`
	p1Proto := new(propertyv1.Property)
	helpers.UnmarshalYAML([]byte(p1YAML), p1Proto)
	p2Proto := new(propertyv1.Property)
	helpers.UnmarshalYAML([]byte(p2YAML), p2Proto)
	BeforeEach(func() {
		_, addr, deferFunc = setup.Common()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		// extracting the operation of creating property schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		creatGroup := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: ui-template`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(creatGroup, flags.EventuallyTimeout).Should(ContainSubstring("group ui-template is created"))
		rootCmd.SetArgs([]string{"property", "apply", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(p1YAML))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		Expect(out).To(ContainSubstring("created: true"))
		Expect(out).To(ContainSubstring("tagsNum: 2"))
	})

	It("gets property", func() {
		rootCmd.SetArgs([]string{"property", "get", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		resp := new(propertyv1.GetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(cmp.Equal(resp.Property, p1Proto,
			protocmp.IgnoreUnknown(),
			protocmp.Transform())).To(BeTrue())
	})

	It("gets a tag", func() {
		rootCmd.SetArgs([]string{
			"property", "get", "-g", "ui-template", "-n",
			"service", "-i", "kubernetes", "-t", "state",
		})

		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		resp := new(propertyv1.GetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Property.Tags).To(HaveLen(1))
		Expect(resp.Property.Tags[0].Key).To(Equal("state"))
	})

	It("gets tags", func() {
		rootCmd.SetArgs([]string{
			"property", "get", "-g", "ui-template", "-n",
			"service", "-i", "kubernetes", "-t", "content,state",
		})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		resp := new(propertyv1.GetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(cmp.Equal(resp.Property, p1Proto,
			protocmp.IgnoreUnknown(),
			protocmp.Transform())).To(BeTrue())
	})

	It("update property", func() {
		rootCmd.SetArgs([]string{"property", "apply", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  container:
    group: ui-template 
    name: service
  id: kubernetes
tags:
- key: state
  value:
    int:
      value: 3
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("created: false"))
		Expect(out).To(ContainSubstring("tagsNum: 1"))
		rootCmd.SetArgs([]string{"property", "get", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(propertyv1.GetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)

		Expect(cmp.Equal(resp.Property, p2Proto,
			protocmp.IgnoreUnknown(),
			protocmp.Transform())).To(BeTrue())
	})

	It("delete property", func() {
		// delete
		rootCmd.SetArgs([]string{"property", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))
		Expect(out).To(ContainSubstring("tagsNum: 0"))
		// get again
		rootCmd.SetArgs([]string{"property", "get", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})

	It("list all properties", func() {
		// create another property for list operation
		rootCmd.SetArgs([]string{"property", "apply", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  container:
    group: ui-template 
    name: service
  id: spring
tags:
  - key: content
    value:
      str:
        value: bar
  - key: state
    value:
      int:
        value: 1
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("created: true"))
		Expect(out).To(ContainSubstring("tagsNum: 2"))
		// list
		rootCmd.SetArgs([]string{"property", "list", "-g", "ui-template"})
		out = capturer.CaptureStdout(func() {
			cmd.ResetFlags()
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(propertyv1.ListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Property).To(HaveLen(2))
	})

	It("list properties in a container", func() {
		// create another property for list operation
		rootCmd.SetArgs([]string{"property", "apply", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  container:
    group: ui-template 
    name: service
  id: spring
tags:
  - key: content
    value:
      str:
        value: bar
  - key: state
    value:
      int:
        value: 1
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("created: true"))
		Expect(out).To(ContainSubstring("tagsNum: 2"))
		// list
		rootCmd.SetArgs([]string{"property", "list", "-g", "ui-template", "-n", "service"})
		out = capturer.CaptureStdout(func() {
			cmd.ResetFlags()
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(propertyv1.ListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Property).To(HaveLen(2))
	})

	AfterEach(func() {
		deferFunc()
	})
})
