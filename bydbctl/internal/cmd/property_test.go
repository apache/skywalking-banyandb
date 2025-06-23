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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var (
	propertyGroup    = "ui-template"
	propertyTagCount = 2
	p1YAML           = fmt.Sprintf(`
metadata:
  group: %s
  name: service
id: kubernetes
tags:
  - key: content
    value:
      str:
        value: foo111
  - key: state
    value:
      int:
        value: 1
`, propertyGroup)

	p2YAML = fmt.Sprintf(`
metadata:
  group: %s
  name: service
id: kubernetes
tags:
  - key: content
    value:
      str:
        value: foo333
  - key: state
    value:
      int:
        value: 3
`, propertyGroup)

	deletedFieldKey = index.FieldKey{TagName: "_deleted"}
)

var _ = Describe("Property Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command

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
		defUITemplateWithSchema(rootCmd, addr, 2, 0)
		applyData(rootCmd, addr, p1YAML, true, propertyTagCount)
	})

	It("update property", func() {
		// update the property
		applyData(rootCmd, addr, p2YAML, false, propertyTagCount)

		// check the property
		queryData(rootCmd, addr, propertyGroup, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).To(ContainSubstring("foo333"))
		})
	})

	It("apply same property after delete", func() {
		// delete
		rootCmd.SetArgs([]string{"property", "data", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))

		// apply property(created should be true)
		applyData(rootCmd, addr, p2YAML, true, propertyTagCount)
	})

	It("query all properties", func() {
		queryData(rootCmd, addr, propertyGroup, 1, nil)
	})

	It("delete property", func() {
		// delete
		rootCmd.SetArgs([]string{"property", "data", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))

		queryData(rootCmd, addr, propertyGroup, 0, nil)
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

var _ = Describe("Property Cluster Operation", func() {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())

	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	var node1Dir, node2Dir string
	var closeNode1, closeNode2 func()
	BeforeEach(func() {
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		var ports []int
		var err error
		var spaceDef1, spaceDef2 func()

		// first creating
		By("Starting node1 with data")
		node1Dir, spaceDef1, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		ports, err = test.AllocateFreePorts(4)
		Expect(err).NotTo(HaveOccurred())
		_, node1Addr, node1Defer := setup.ClosableStandalone(node1Dir, ports)
		node1Addr = httpSchema + node1Addr
		defUITemplateWithSchema(rootCmd, node1Addr, 1, 0)
		applyData(rootCmd, node1Addr, p1YAML, true, propertyTagCount)
		queryData(rootCmd, node1Addr, propertyGroup, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).To(ContainSubstring("foo111"))
		})
		node1Defer()

		By("Starting node2 with data")
		node2Dir, spaceDef2, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		ports, err = test.AllocateFreePorts(4)
		Expect(err).NotTo(HaveOccurred())
		_, node2Addr, node2Defer := setup.ClosableStandalone(node2Dir, ports)
		node2Addr = httpSchema + node2Addr
		defUITemplateWithSchema(rootCmd, node2Addr, 1, 0)
		applyData(rootCmd, node2Addr, p2YAML, true, propertyTagCount)
		queryData(rootCmd, node2Addr, propertyGroup, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).To(ContainSubstring("foo333"))
		})
		node2Defer()

		// setup cluster with two data nodes
		By("Starting etcd server")
		ports, err = test.AllocateFreePorts(2)
		Expect(err).NotTo(HaveOccurred())
		dir, spaceDef, err := test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
		server, err := embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
			embeddedetcd.RootDir(dir),
		)
		Expect(err).ShouldNot(HaveOccurred())
		<-server.ReadyNotify()
		By("Starting data node 0")
		closeNode1 = setup.DataNodeFromDataDir(ep, node1Dir)
		By("Starting data node 1")
		closeNode2 = setup.DataNodeFromDataDir(ep, node2Dir)
		By("Starting liaison node")
		_, liaisonHTTPAddr, closerLiaisonNode := setup.LiaisonNodeWithHTTP(ep)
		By("Initializing test cases")

		deferFunc = func() {
			closerLiaisonNode()
			closeNode1()
			closeNode2()
			_ = server.Close()
			<-server.StopNotify()
			spaceDef()
			spaceDef1()
			spaceDef2()
		}
		addr = httpSchema + liaisonHTTPAddr
		// creating schema
		defUITemplateWithSchema(rootCmd, addr, 1, 1)
	})

	AfterEach(func() {
		deferFunc()
	})

	It("query from difference version", func() {
		queryData(rootCmd, addr, propertyGroup, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).Should(ContainSubstring("foo333"))
		})
		closeNode1()
		closeNode2()

		// check there should have two real properties in the dest database
		// and one of them should be deleted (marked in the query phase)
		store1, err := generateInvertedStore(node1Dir)
		Expect(err).NotTo(HaveOccurred())
		store2, err := generateInvertedStore(node2Dir)
		Expect(err).NotTo(HaveOccurred())

		query, err := inverted.BuildPropertyQuery(&propertyv1.QueryRequest{
			Groups: []string{propertyGroup},
		}, "_group", "_entity_id")
		Expect(err).NotTo(HaveOccurred())
		node1Search, err := store1.Search(context.Background(), []index.FieldKey{deletedFieldKey}, query, 10)
		Expect(err).NotTo(HaveOccurred())
		node2Search, err := store2.Search(context.Background(), []index.FieldKey{deletedFieldKey}, query, 10)
		Expect(err).NotTo(HaveOccurred())

		totalProperties := append(node1Search, node2Search...)
		Expect(len(totalProperties)).Should(Equal(3)) // 1(deleted) + 2(applied replicas)
		deletedCount := 0
		for _, p := range totalProperties {
			if convert.BytesToBool(p.Fields[deletedFieldKey.TagName]) {
				deletedCount++
			}
		}
		Expect(deletedCount).Should(Equal(1))
	})

	It("delete property", func() {
		// delete properties
		rootCmd.SetArgs([]string{"property", "data", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))

		// should no properties after deletion
		queryData(rootCmd, addr, propertyGroup, 0, nil)

		// created again, the created should be true
		applyData(rootCmd, addr, p1YAML, true, propertyTagCount)
	})
})

func defUITemplateWithSchema(rootCmd *cobra.Command, addr string, shardCount int, replicas int) {
	rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
	createGroup := func() string {
		rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
metadata:
  name: ui-template
catalog: CATALOG_PROPERTY
resource_opts:
  shard_num: %d
  replicas: %d
`, shardCount, replicas)))
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
}

func applyData(rootCmd *cobra.Command, addr, data string, created bool, tagsNum int) {
	rootCmd.SetArgs([]string{"property", "data", "apply", "-a", addr, "-f", "-"})
	rootCmd.SetIn(strings.NewReader(data))
	out := capturer.CaptureStdout(func() {
		err := rootCmd.Execute()
		Expect(err).NotTo(HaveOccurred())
	})
	GinkgoWriter.Println(out)
	Expect(out).To(ContainSubstring(fmt.Sprintf("created: %t", created)))
	Expect(out).To(ContainSubstring(fmt.Sprintf("tagsNum: %d", tagsNum)))
}

func queryData(rootCmd *cobra.Command, addr, group string, dataCount int, verify func(data string, resp *propertyv1.QueryResponse)) {
	rootCmd.SetArgs([]string{"property", "data", "query", "-a", addr, "-f", "-"})
	issue := func() string {
		rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`groups: ["%s"]`, group)))
		return capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
	}
	Eventually(func() error {
		out := issue()
		resp := new(propertyv1.QueryResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		GinkgoWriter.Println(resp)

		failures := InterceptGomegaFailures(func() {
			Expect(len(resp.Properties)).To(Equal(dataCount))
			if verify != nil {
				verify(out, resp)
			}
		})

		if len(failures) > 0 {
			return errors.New(strings.Join(failures, "\n"))
		}
		return nil
	}, flags.EventuallyTimeout).Should(Succeed())
}

func generateInvertedStore(rootPath string) (index.SeriesStore, error) {
	shardParent := path.Join(rootPath, "property", "data")
	list, err := os.ReadDir(shardParent)
	if err != nil {
		return nil, fmt.Errorf("read dir %s error: %w", shardParent, err)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("no shard found in %s", shardParent)
	}
	for _, e := range list {
		if !e.Type().IsDir() {
			continue
		}
		_, found := strings.CutPrefix(e.Name(), "shard-")
		if !found {
			continue
		}
		return inverted.NewStore(
			inverted.StoreOpts{
				Path: path.Join(shardParent, e.Name()),
			})
	}
	return nil, fmt.Errorf("no shard found in %s", rootPath)
}
