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
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"
	"google.golang.org/protobuf/encoding/protojson"

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
	property1ID      = "kubernetes"
	property2ID      = "mesh"

	p1YAML = fmt.Sprintf(`
metadata:
  group: %s
  name: service
id: %s
tags:
  - key: content
    value:
      str:
        value: foo111
  - key: state
    value:
      int:
        value: 1
`, propertyGroup, property1ID)

	p2YAML = fmt.Sprintf(`
metadata:
  group: %s
  name: service
id: %s
tags:
  - key: content
    value:
      str:
        value: foo333
  - key: state
    value:
      int:
        value: 3
`, propertyGroup, property1ID)

	p3YAML = fmt.Sprintf(`
metadata:
  group: %s
  name: service
id: %s
tags:
  - key: content
    value:
      str:
        value: foo-mesh
  - key: state
    value:
      int:
        value: 22
`, propertyGroup, property2ID)

	deletedFieldKey = index.FieldKey{TagName: "_deleted"}
	sourceFieldKey  = index.FieldKey{TagName: "_source"}
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
		queryData(rootCmd, addr, propertyGroup, "", 1, func(data string, resp *propertyv1.QueryResponse) {
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
		queryData(rootCmd, addr, propertyGroup, "", 1, nil)
	})

	It("delete property", func() {
		// delete
		rootCmd.SetArgs([]string{"property", "data", "delete", "-g", "ui-template", "-n", "service", "-i", "kubernetes"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("deleted: true"))

		queryData(rootCmd, addr, propertyGroup, "", 0, nil)
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
		applyData(rootCmd, node1Addr, p3YAML, true, propertyTagCount)
		queryData(rootCmd, node1Addr, propertyGroup, property1ID, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).To(ContainSubstring("foo111"))
		})
		queryData(rootCmd, node1Addr, propertyGroup, property2ID, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).To(ContainSubstring("foo-mesh"))
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
		applyData(rootCmd, node2Addr, p3YAML, true, propertyTagCount)
		deleteData(rootCmd, node2Addr, propertyGroup, "service", property2ID, true)
		queryData(rootCmd, node2Addr, propertyGroup, property1ID, 1, func(data string, resp *propertyv1.QueryResponse) {
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
		// property 1 should have one older version in node1, and newer version in node2
		// after querying and repairing, it should contain three documents,
		// one deleted in node1, one in node1, and one in node2,
		// and not deleted documents(property) should have the same mod revision
		queryData(rootCmd, addr, propertyGroup, property1ID, 1, func(data string, resp *propertyv1.QueryResponse) {
			Expect(data).Should(ContainSubstring("foo333"))
		})
		// property 2 should have one older version in node1, and same version in node2, then deleted in node2
		// after querying and repairing, it should contain three documents,
		// one older deleted in node1, new deleted in node1, one deleted in node2
		queryData(rootCmd, addr, propertyGroup, property2ID, 0, nil)

		// wait for the repair to finish
		time.Sleep(1 * time.Second)

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
		node1Search, err := store1.Search(context.Background(), []index.FieldKey{sourceFieldKey, deletedFieldKey}, query, 10)
		Expect(err).NotTo(HaveOccurred())
		node2Search, err := store2.Search(context.Background(), []index.FieldKey{sourceFieldKey, deletedFieldKey}, query, 10)
		Expect(err).NotTo(HaveOccurred())

		totalProperties := append(node1Search, node2Search...)
		p1DeletedOnNode1 := filterProperties(node1Search, func(property *propertyv1.Property, deleted bool) bool {
			return deleted && property.Id == property1ID
		})
		p1NotDeletedOnNode1 := filterProperties(node1Search, func(property *propertyv1.Property, deleted bool) bool {
			return !deleted && property.Id == property1ID
		})
		p1NotDeletedInNode2 := filterProperties(node2Search, func(property *propertyv1.Property, deleted bool) bool {
			return !deleted && property.Id == property1ID
		})
		p1Total := filterProperties(totalProperties, func(property *propertyv1.Property, deleted bool) bool {
			return property.Id == property1ID
		})
		Expect(len(p1Total)).To(Equal(3))
		Expect(len(p1DeletedOnNode1)).To(Equal(1))
		Expect(len(p1NotDeletedOnNode1)).To(Equal(1))
		Expect(len(p1NotDeletedInNode2)).To(Equal(1))
		// mod time should be the same
		Expect(p1NotDeletedOnNode1[0].Metadata.ModRevision == p1NotDeletedInNode2[0].Metadata.ModRevision).
			To(BeTrue(), "the mod revision of not deleted property should be the same")

		p2Total := filterProperties(totalProperties, func(property *propertyv1.Property, deleted bool) bool {
			return property.Id == property2ID
		})
		p2DeletedOnNode1 := filterProperties(node1Search, func(property *propertyv1.Property, deleted bool) bool {
			return deleted && property.Id == property2ID
		})
		p2DeletedOnNode2 := filterProperties(node2Search, func(property *propertyv1.Property, deleted bool) bool {
			return deleted && property.Id == property2ID
		})
		sort.Sort(propertySlice(p2DeletedOnNode1))
		sort.Sort(propertySlice(p2DeletedOnNode2))
		Expect(len(p2Total)).To(Equal(3))
		Expect(len(p2DeletedOnNode1)).To(Equal(2))
		Expect(len(p2DeletedOnNode2)).To(Equal(1))
		Expect(p2DeletedOnNode1[1].Metadata.ModRevision == p2DeletedOnNode2[0].Metadata.ModRevision).
			To(BeTrue(), "the mod revision of not deleted property should be the same")
	})

	It("delete property", func() {
		// delete properties
		deleteData(rootCmd, addr, propertyGroup, "service", property1ID, true)

		// should no properties after deletion
		queryData(rootCmd, addr, propertyGroup, "", 0, nil)

		// created again, the created should be true
		applyData(rootCmd, addr, p1YAML, true, propertyTagCount)
	})
})

func filterProperties(doc []index.SeriesDocument, filter func(property *propertyv1.Property, deleted bool) bool) (res []*propertyv1.Property) {
	for _, p := range doc {
		deleted := convert.BytesToBool(p.Fields[deletedFieldKey.TagName])
		source := p.Fields[sourceFieldKey.TagName]
		Expect(source).NotTo(BeNil())
		var pt propertyv1.Property
		err := protojson.Unmarshal(source, &pt)
		Expect(err).NotTo(HaveOccurred())
		if filter(&pt, deleted) {
			res = append(res, &pt)
		}
	}
	return
}

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

func queryData(rootCmd *cobra.Command, addr, group, id string, dataCount int, verify func(data string, resp *propertyv1.QueryResponse)) {
	rootCmd.SetArgs([]string{"property", "data", "query", "-a", addr, "-f", "-"})
	issue := func() string {
		query := fmt.Sprintf(`groups: ["%s"]`, group)
		if id != "" {
			query += fmt.Sprintf("\nids: [\"%s\"]", id)
		}
		rootCmd.SetIn(strings.NewReader(query))
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

func deleteData(rootCmd *cobra.Command, addr, group, name, id string, success bool) {
	rootCmd.SetArgs([]string{"property", "data", "delete", "-a", addr, "-g", group, "-n", name, "-i", id})
	out := capturer.CaptureStdout(func() {
		err := rootCmd.Execute()
		Expect(err).NotTo(HaveOccurred())
	})
	Expect(out).To(ContainSubstring("deleted: %t", success))
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

type propertySlice []*propertyv1.Property

func (p propertySlice) Len() int {
	return len(p)
}

func (p propertySlice) Less(i, j int) bool {
	return p[i].Metadata.ModRevision < p[j].Metadata.ModRevision
}

func (p propertySlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
