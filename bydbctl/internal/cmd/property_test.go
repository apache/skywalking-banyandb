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
		_, _, addr, _, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr
		// extracting the operation of creating property schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		creatGroup := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: ui-template
catalog: CATALOG_PROPERTY
resource_opts:
  shard_num: 2
`))
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

	It("query all properties", func() {
		rootCmd.SetArgs([]string{"property", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(`
groups: ["ui-template"]
`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(func() int {
			out := issue()
			resp := new(propertyv1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Properties)
		}, flags.EventuallyTimeout).Should(Equal(1))
	})

	It("delete property", func() {
		// delete
		rootCmd.SetArgs([]string{"property", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))
		rootCmd.SetArgs([]string{"property", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(`
groups: ["ui-template"]
`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(func() int {
			out := issue()
			resp := new(propertyv1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Properties)
		}, flags.EventuallyTimeout).Should(Equal(0))
	})

	AfterEach(func() {
		deferFunc()
	})
})
