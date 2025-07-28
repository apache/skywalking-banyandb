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
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = Describe("Trace Schema Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr
		// extracting the operation of creating trace schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		createGroup := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1
catalog: CATALOG_TRACE
resource_opts:
  shard_num: 2
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
		rootCmd.SetArgs([]string{"trace", "create", "-a", addr, "-f", "-"})
		createTrace := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
tags:
  - name: trace_id
    type: TAG_TYPE_STRING
  - name: timestamp
    type: TAG_TYPE_TIMESTAMP
trace_id_tag_name: trace_id
timestamp_tag_name: timestamp`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createTrace, flags.EventuallyTimeout).Should(ContainSubstring("trace group1.name1 is created"))
	})

	It("get trace schema", func() {
		rootCmd.SetArgs([]string{"trace", "get", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		resp := new(databasev1.TraceRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Trace.Metadata.Group).To(Equal("group1"))
		Expect(resp.Trace.Metadata.Name).To(Equal("name1"))
		Expect(resp.Trace.TraceIdTagName).To(Equal("trace_id"))
		Expect(resp.Trace.TimestampTagName).To(Equal("timestamp"))
		Expect(resp.Trace.Tags).To(HaveLen(2))
	})

	It("update trace schema", func() {
		rootCmd.SetArgs([]string{"trace", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
tags:
  - name: trace_id
    type: TAG_TYPE_STRING
  - name: timestamp
    type: TAG_TYPE_TIMESTAMP
  - name: service_name
    type: TAG_TYPE_STRING
trace_id_tag_name: trace_id
timestamp_tag_name: timestamp`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("trace group1.name1 is updated"))
		rootCmd.SetArgs([]string{"trace", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.TraceRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Trace.Metadata.Group).To(Equal("group1"))
		Expect(resp.Trace.Metadata.Name).To(Equal("name1"))
		Expect(resp.Trace.Tags).To(HaveLen(3))
		Expect(resp.Trace.Tags[2].Name).To(Equal("service_name"))
	})

	It("delete trace schema", func() {
		// delete
		rootCmd.SetArgs([]string{"trace", "delete", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("trace group1.name1 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"trace", "get", "-g", "group1", "-n", "name1"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})

	It("list trace schema", func() {
		// create another trace schema for list operation
		rootCmd.SetArgs([]string{"trace", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1
tags:
  - name: trace_id
    type: TAG_TYPE_STRING
  - name: timestamp
    type: TAG_TYPE_TIMESTAMP
trace_id_tag_name: trace_id
timestamp_tag_name: timestamp`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("trace group1.name2 is created"))
		// list
		rootCmd.SetArgs([]string{"trace", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.TraceRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Trace).To(HaveLen(2))
	})

	AfterEach(func() {
		deferFunc()
	})
})
