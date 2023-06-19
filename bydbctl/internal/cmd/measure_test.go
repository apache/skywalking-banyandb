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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	cases_measure_data "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = Describe("Measure Schema Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.Common()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		// extracting the operation of creating measure schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-a", addr, "-f", "-"})
		createGroup := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1
catalog: CATALOG_MEASURE
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
		rootCmd.SetArgs([]string{"measure", "create", "-a", addr, "-f", "-"})
		createMeasure := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
tag_families:
  - name: default
    tags:
      - name: id
        type: TAG_TYPE_STRING
entity:
  tagNames: ["id"]`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createMeasure, flags.EventuallyTimeout).Should(ContainSubstring("measure group1.name1 is created"))
	})

	It("get measure schema", func() {
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.MeasureRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Measure.Metadata.Group).To(Equal("group1"))
		Expect(resp.Measure.Metadata.Name).To(Equal("name1"))
	})

	It("update measure schema", func() {
		rootCmd.SetArgs([]string{"measure", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
tag_families:
  - name: default
    tags:
      - name: id
        type: TAG_TYPE_STRING
entity:
  tagNames: ["tag1"]`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name1 is updated"))
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.MeasureRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Measure.Metadata.Group).To(Equal("group1"))
		Expect(resp.Measure.Metadata.Name).To(Equal("name1"))
		Expect(resp.Measure.Entity.TagNames[0]).To(Equal("tag1"))
	})

	It("delete measure schema", func() {
		// delete
		rootCmd.SetArgs([]string{"measure", "delete", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name1 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name1"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})

	It("list measure schema", func() {
		// create another measure schema for list operation
		rootCmd.SetArgs([]string{"measure", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1
tag_families:
  - name: default
    tags:
      - name: id
        type: TAG_TYPE_STRING
entity:
  tagNames: ["tag1"]`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name2 is created"))
		// list
		rootCmd.SetArgs([]string{"measure", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.MeasureRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Measure).To(HaveLen(2))
	})

	AfterEach(func() {
		deferFunc()
	})
})

var _ = Describe("Measure Data Query", func() {
	var addr, grpcAddr string
	var deferFunc func()
	var rootCmd *cobra.Command
	var now time.Time
	var startStr, endStr string
	var interval time.Duration
	BeforeEach(func() {
		now = timestamp.NowMilli()
		startStr = now.Add(-20 * time.Minute).Format(time.RFC3339)
		interval = 1 * time.Millisecond
		endStr = now.Add(5 * time.Minute).Format(time.RFC3339)
		grpcAddr, addr, deferFunc = setup.Common()
		Eventually(helpers.HTTPHealthCheck(addr), flags.EventuallyTimeout).Should(Succeed())
		addr = "http://" + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("query all measure data", func() {
		conn, err := grpclib.Dial(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
		rootCmd.SetArgs([]string{"measure", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
metadata:
  name: service_cpm_minute
  group: sw_metric
timeRange:
  begin: %s
  end: %s
tagProjection:
  tagFamilies:
    - name: default
      tags:
        - id`, startStr, endStr)))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			GinkgoWriter.Println(out)
			resp := new(measurev1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.DataPoints)
		}, flags.EventuallyTimeout).Should(Equal(6))
	})

	DescribeTable("query measure data with time range flags", func(timeArgs ...string) {
		conn, err := grpclib.Dial(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		now := timestamp.NowMilli()
		interval := time.Minute
		cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
		args := []string{"measure", "query", "-a", addr}
		args = append(args, timeArgs...)
		args = append(args, "-f", "-")
		rootCmd.SetArgs(args)
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
 name: service_cpm_minute
 group: sw_metric
tagProjection:
 tagFamilies:
   - name: default
     tags:
       - id`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(measurev1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.DataPoints)
		}, flags.EventuallyTimeout).Should(Equal(6))
	},
		Entry("relative start", "--start", "-30m"),
		Entry("relative end", "--end", "0m"),
		Entry("absolute start", "--start", startStr),
		Entry("absolute end", "--end", endStr),
		Entry("default"),
		Entry("all relative", "--start", "-30m", "--end", "0m"),
		Entry("all absolute", "--start", startStr, "--end", endStr),
	)

	AfterEach(func() {
		deferFunc()
	})
})
