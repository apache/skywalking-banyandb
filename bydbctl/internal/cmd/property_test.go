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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr
		// extracting the operation of creating property schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		createGroup := func() string {
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
		Eventually(createGroup, flags.EventuallyTimeout).Should(ContainSubstring("group ui-template is created"))
		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		createPropertySchema := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createPropertySchema, flags.EventuallyTimeout).Should(ContainSubstring("property schema ui-template.service is created"))
		rootCmd.SetArgs([]string{"property", "data", "apply", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(p1YAML))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		Expect(out).To(ContainSubstring("created: true"))
		Expect(out).To(ContainSubstring("tagsNum: 2"))
	})

	It("update property", func() {
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"property", "data", "apply", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(p2YAML))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		Expect(out).To(ContainSubstring("created: false"))
		Expect(out).To(ContainSubstring("tagsNum: 2"))
	})

	It("query all properties", func() {
		rootCmd.SetArgs([]string{"property", "data", "query", "-a", addr, "-f", "-"})
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
		rootCmd.SetArgs([]string{"property", "data", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))
		rootCmd.SetArgs([]string{"property", "data", "query", "-a", addr, "-f", "-"})
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

var _ = Describe("Property Schema Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		createGroup := func() string {
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
		Eventually(createGroup, flags.EventuallyTimeout).Should(ContainSubstring("group ui-template is created"))
	})

	It("create property schema", func() {
		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is created"))
	})

	It("get property schema", func() {
		// First create the schema
		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is created"))

		// Then get the schema
		rootCmd.SetArgs([]string{"property", "schema", "get", "-g", "ui-template", "-n", "service"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.PropertyRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Property.Metadata.Group).To(Equal("ui-template"))
		Expect(resp.Property.Metadata.Name).To(Equal("service"))
		Expect(resp.Property.Tags).To(HaveLen(2))
		Expect(resp.Property.Tags[0].Name).To(Equal("content"))
		Expect(resp.Property.Tags[1].Name).To(Equal("state"))
	})

	It("update property schema", func() {
		// First create the schema
		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is created"))

		// Then update the schema
		rootCmd.SetArgs([]string{"property", "schema", "update", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
  - name: version
    type: TAG_TYPE_STRING
`))
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is updated"))

		// Verify the update
		rootCmd.SetArgs([]string{"property", "schema", "get", "-g", "ui-template", "-n", "service"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.PropertyRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Property.Tags).To(HaveLen(3))
		Expect(resp.Property.Tags[2].Name).To(Equal("version"))
	})

	It("delete property schema", func() {
		// First create the schema
		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is created"))

		// Delete the schema
		rootCmd.SetArgs([]string{"property", "schema", "delete", "-g", "ui-template", "-n", "service"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is deleted"))

		// Verify deletion
		rootCmd.SetArgs([]string{"property", "schema", "get", "-g", "ui-template", "-n", "service"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError(ContainSubstring("not found")))
	})

	It("list property schema", func() {
		// Create multiple schemas
		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
  - name: state
    type: TAG_TYPE_INT
`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.service is created"))

		rootCmd.SetArgs([]string{"property", "schema", "create", "-a", addr, "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: endpoint
  group: ui-template
tags:
  - name: content
    type: TAG_TYPE_STRING
`))
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("property schema ui-template.endpoint is created"))

		// List all schemas
		rootCmd.SetArgs([]string{"property", "schema", "list", "-g", "ui-template"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.PropertyRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Properties).To(HaveLen(2))

		propertyNames := []string{resp.Properties[0].Metadata.Name, resp.Properties[1].Metadata.Name}
		Expect(propertyNames).To(ContainElements("service", "endpoint"))
	})

	AfterEach(func() {
		deferFunc()
	})
})
