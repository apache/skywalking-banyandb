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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases_trace_data "github.com/apache/skywalking-banyandb/test/cases/trace/data"
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

var _ = Describe("Trace Data Query", func() {
	var addr, grpcAddr string
	var deferFunc func()
	var rootCmd *cobra.Command
	var now time.Time
	var interval time.Duration
	BeforeEach(func() {
		var err error
		now, err = time.ParseInLocation("2006-01-02T15:04:05", "2021-09-01T23:30:00", time.Local)
		Expect(err).NotTo(HaveOccurred())
		interval = 500 * time.Millisecond
		grpcAddr, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)

		// Create trace group
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		createGroup := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: test-trace-group
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
					GinkgoWriter.Printf("group creation fails: %v", err)
				}
			})
		}
		Eventually(createGroup, flags.EventuallyTimeout).Should(ContainSubstring("group test-trace-group is created"))

		// Create trace schema
		rootCmd.SetArgs([]string{"trace", "create", "-a", addr, "-f", "-"})
		createTrace := func() string {
			rootCmd.SetIn(strings.NewReader(`
 metadata:
   name: sw
   group: test-trace-group
 tags:
   - name: trace_id
     type: TAG_TYPE_STRING
   - name: state
     type: TAG_TYPE_INT
   - name: service_id
     type: TAG_TYPE_STRING
   - name: service_instance_id
     type: TAG_TYPE_STRING
   - name: endpoint_id
     type: TAG_TYPE_STRING
   - name: duration
     type: TAG_TYPE_INT
   - name: timestamp
     type: TAG_TYPE_TIMESTAMP
 trace_id_tag_name: trace_id
 timestamp_tag_name: timestamp`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("trace creation fails: %v", err)
				}
			})
		}
		Eventually(createTrace, flags.EventuallyTimeout).Should(ContainSubstring("trace test-trace-group.sw is created"))
	})

	It("query trace by trace id", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		cases_trace_data.Write(conn, "sw", now, interval)
		rootCmd.SetArgs([]string{"trace", "query", "-a", addr, "-f", "-"})
		for _, idx := range []int{1, 2, 3, 4, 5} {
			issue := func() string {
				rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
name: "sw"
groups: ["test-trace-group"]
tag_projection: ["trace_id"]
criteria:
  condition:
    name: "trace_id"
    op: "BINARY_OP_EQ"
    value:
      str:
        value: "%d"`, idx)))
				return capturer.CaptureStdout(func() {
					err := rootCmd.Execute()
					Expect(err).NotTo(HaveOccurred())
				})
			}
			Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
			Eventually(func() int {
				out := issue()
				GinkgoWriter.Println("Query output:", out)
				resp := new(tracev1.QueryResponse)
				helpers.UnmarshalYAML([]byte(out), resp)
				Expect(resp.Spans[0].Tags[0].Key).To(Equal("trace_id"))
				Expect(resp.Spans[0].Tags[0].Value.GetStr().Value).To(Equal(fmt.Sprintf("%d", idx)))
				return len(resp.Spans)
			}, flags.EventuallyTimeout).Should(Equal(1))
		}
	})

	AfterEach(func() {
		deferFunc()
	})
})
