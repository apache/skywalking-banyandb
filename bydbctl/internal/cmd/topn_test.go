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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	cases_measure_data "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
	"time"
)

var _ = Describe("Topn Schema Operation", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command
	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr
		// extracting the operation of creating topn schema
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
    - name: entity_id
      type: TAG_TYPE_STRING
fields:
  - name: total
    field_type: FIELD_TYPE_INT
    encoding_method: ENCODING_METHOD_GORILLA
    compression_method: COMPRESSION_METHOD_ZSTD
  - name: value
    field_type: FIELD_TYPE_INT
    encoding_method: ENCODING_METHOD_GORILLA
    compression_method: COMPRESSION_METHOD_ZSTD
entity:
  tag_names:
  - id`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createMeasure, flags.EventuallyTimeout).Should(ContainSubstring("measure group1.name1 is created"))
		rootCmd.SetArgs([]string{"topn", "create", "-a", addr, "-f", "-"})
		createTopn := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1
source_measure:
  name: name1
  group: group1
field_name: value
field_value_sort: SORT_DESC
group_by_tag_names:
  - id
counters_number: 10000
lru_size: 10`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createTopn, flags.EventuallyTimeout).Should(ContainSubstring("topn group1.name2 is created"))
	})

	It("get topn schema", func() {
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name2"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.TopNAggregationRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.TopNAggregation.Metadata.Group).To(Equal("group1"))
		Expect(resp.TopNAggregation.Metadata.Name).To(Equal("name2"))
	})

	It("update topn schema", func() {
		rootCmd.SetArgs([]string{"topn", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1
source_measure:
  name: name1
  group: group1
field_name: value
field_value_sort: SORT_DESC
group_by_tag_names:
  - id
  - entity_id
counters_number: 10000
lru_size: 10`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("topn group1.name2 is updated"))
		rootCmd.SetArgs([]string{"topn", "get", "-g", "group1", "-n", "name2"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.TopNAggregationRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.TopNAggregation.Metadata.Group).To(Equal("group1"))
		Expect(resp.TopNAggregation.Metadata.Name).To(Equal("name2"))
		Expect(resp.TopNAggregation.GroupByTagNames[1]).To(Equal("entity_id"))
	})

	It("delete topn schema", func() {
		// delete
		rootCmd.SetArgs([]string{"topn", "delete", "-g", "group1", "-n", "name2"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("topn group1.name2 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"topn", "get", "-g", "group1", "-n", "name2"})
		err := rootCmd.Execute()
		Expect(err).To(MatchError("rpc error: code = NotFound desc = banyandb: resource not found"))
	})

	It("list topn schema", func() {
		// create another topn schema for list operation
		rootCmd.SetArgs([]string{"topn", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name3
  group: group1
source_measure:
  name: name1
  group: group1
field_name: value
field_value_sort: SORT_DESC
group_by_tag_names:
  - id
counters_number: 10000
lru_size: 10`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("topn group1.name3 is created"))
		// list
		rootCmd.SetArgs([]string{"topn", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(databasev1.TopNAggregationRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.TopNAggregation).To(HaveLen(2))
	})

	AfterEach(func() {
		deferFunc()
	})
})

var _ = Describe("Topn Data Query", func() {
	var addr, grpcAddr string
	var deferFunc func()
	var rootCmd *cobra.Command
	var now time.Time
	var startStr, endStr string
	var interval time.Duration
	BeforeEach(func() {
		var err error
		now, err = time.ParseInLocation("2006-01-02T15:04:05", "2021-09-01T23:30:00", time.Local)
		Expect(err).NotTo(HaveOccurred())
		startStr = now.Add(-20 * time.Minute).Format(time.RFC3339)
		interval = 1 * time.Millisecond
		endStr = now.Add(5 * time.Minute).Format(time.RFC3339)
		grpcAddr, addr, deferFunc = setup.Standalone()
		addr = httpSchema + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("query all topn data", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
		rootCmd.SetArgs([]string{"measure", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
name: service_cpm_minute
groups: ["sw_metric"]
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

		rootCmd.SetArgs([]string{"topn", "create", "-a", addr, "-f", "-"})
		createTopn := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service_instance_cpm_minute_top_bottom_100
  group: sw_metric
source_measure:
  name: service_cpm_minute
  group: sw_metric
field_name: value
field_value_sort: SORT_UNSPECIFIED
group_by_tag_names:
  - id
counters_number: 100
lru_size: 10`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createTopn, flags.EventuallyTimeout).Should(ContainSubstring("topn sw_metric.service_instance_cpm_minute_top_bottom_100 is created"))
		rootCmd.SetArgs([]string{"topn", "query", "-a", addr, "-f", "-"})
		issue1 := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
name: service_instance_cpm_minute_top_bottom_100
groups: ["sw_metric"]
timeRange:
  begin: %s
  end: %s
topN: 3
agg: 2
fieldValueSort: 1`, startStr, endStr)))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(issue1, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue1()
			GinkgoWriter.Println(out)
			resp := new(measurev1.TopNResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Lists)
		}, flags.EventuallyTimeout).Should(Equal(6))
	})

	DescribeTable("query topn data with time range flags", func(timeArgs ...string) {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		now := timestamp.NowMilli()
		interval := time.Minute
		cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
		rootCmd.SetArgs([]string{"topn", "create", "-a", addr, "-f", "-"})
		createTopn := func() string {
			rootCmd.SetIn(strings.NewReader(`
metadata:
  name: service_instance_cpm_minute_top_bottom_100
  group: sw_metric
source_measure:
  name: service_cpm_minute
  group: sw_metric
field_name: value
field_value_sort: SORT_UNSPECIFIED
group_by_tag_names:
  - id
counters_number: 100
lru_size: 10`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(createTopn, flags.EventuallyTimeout).Should(ContainSubstring("topn sw_metric.service_instance_cpm_minute_top_bottom_100 is created"))

		args := []string{"topn", "query", "-a", addr}
		args = append(args, timeArgs...)
		args = append(args, "-f", "-")
		rootCmd.SetArgs(args)
		issue1 := func() string {
			rootCmd.SetIn(strings.NewReader(`
name: service_instance_cpm_minute_top_bottom_100
groups: ["sw_metric"]
topN: 3
agg: 2
fieldValueSort: 1`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Eventually(issue1, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue1()
			GinkgoWriter.Println(out)
			resp := new(measurev1.TopNResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Lists)
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
