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

	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	stream_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	cases_stream_data "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

var _ = Describe("Stream Schema Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.SetUp()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		// extracting the operation of creating stream schema
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
		rootCmd.SetArgs([]string{"stream", "create", "-a", addr, "-f", "-"})
		createStream := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
tagFamilies:
  - name: searchable
    tags: 
      - name: trace_id
        type: TAG_TYPE_STRING`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createStream, flags.EventuallyTimeout).Should(ContainSubstring("stream group1.name1 is created"))
	})

	It("get stream schema", func() {
		rootCmd.SetArgs([]string{"stream", "get", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		resp := new(database_v1.StreamRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Stream.Metadata.Group).To(Equal("group1"))
		Expect(resp.Stream.Metadata.Name).To(Equal("name1"))
	})

	It("update stream schema", func() {
		rootCmd.SetArgs([]string{"stream", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
entity:
  tagNames: ["tag1"]`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name1 is updated"))
		rootCmd.SetArgs([]string{"stream", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.StreamRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Stream.Metadata.Group).To(Equal("group1"))
		Expect(resp.Stream.Metadata.Name).To(Equal("name1"))
		Expect(resp.Stream.Entity.TagNames[0]).To(Equal("tag1"))
	})

	It("delete stream schema", func() {
		// delete
		rootCmd.SetArgs([]string{"stream", "delete", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name1 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"stream", "get", "-g", "group1", "-n", "name1"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})

	It("list stream schema", func() {
		// create another stream schema for list operation
		rootCmd.SetArgs([]string{"stream", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name2 is created"))
		// list
		rootCmd.SetArgs([]string{"stream", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.StreamRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Stream).To(HaveLen(2))
	})

	AfterEach(func() {
		deferFunc()
	})
})

var _ = Describe("Stream Data Query", func() {
	var addr, grpcAddr string
	var deferFunc func()
	var rootCmd *cobra.Command
	var now time.Time
	var nowStr, endStr string
	var interval time.Duration
	BeforeEach(func() {
		now = timestamp.NowMilli()
		nowStr = now.Format(time.RFC3339)
		interval = 500 * time.Millisecond
		endStr = now.Add(1 * time.Hour).Format(time.RFC3339)
		grpcAddr, addr, deferFunc = setup.SetUp()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("query stream all data", func() {
		conn, err := grpclib.Dial(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		cases_stream_data.Write(conn, "data.json", now, interval)
		rootCmd.SetArgs([]string{"stream", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
metadata:
  name: sw
  group: default
timeRange:
  begin: %s
  end: %s
projection:
  tagFamilies:
    - name: searchable
      tags:
        - trace_id`, nowStr, endStr)))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(stream_v1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Elements)
		}, flags.EventuallyTimeout).Should(Equal(5))
	})

	DescribeTable("query stream data with time range flags", func(timeArgs ...string) {
		conn, err := grpclib.Dial(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		now := timestamp.NowMilli()
		interval := -1 * time.Millisecond
		cases_stream_data.Write(conn, "data.json", now, interval)
		args := []string{"stream", "query", "-a", addr}
		args = append(args, timeArgs...)
		args = append(args, "-f", "-")
		rootCmd.SetArgs(args)
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: sw
  group: default
projection:
  tagFamilies:
    - name: searchable
      tags:
        - trace_id`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(stream_v1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Elements)
		}, flags.EventuallyTimeout).Should(Equal(5))
	},
		Entry("relative start", "--start", "-30m"),
		Entry("relative end", "--end", "0m"),
		Entry("absolute start", "--start", nowStr),
		Entry("absolute end", "--end", endStr),
		Entry("default"),
		Entry("all relative", "--start", "-30m", "--end", "0m"),
		Entry("all absolute", "--start", nowStr, "--end", endStr),
	)

	AfterEach(func() {
		deferFunc()
	})
})
