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

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases_stream_data "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

var _ = Describe("BydbQL Stream Query", func() {
	var addr, grpcAddr string
	var deferFunc func()
	var rootCmd *cobra.Command
	var now time.Time
	var nowStr, endStr string
	var interval time.Duration

	BeforeEach(func() {
		var err error
		now, err = time.ParseInLocation("2006-01-02T15:04:05", "2021-09-01T23:30:00", time.Local)
		Expect(err).NotTo(HaveOccurred())
		nowStr = now.Format(time.RFC3339)
		interval = 500 * time.Millisecond
		endStr = now.Add(1 * time.Hour).Format(time.RFC3339)
		grpcAddr, addr, deferFunc = setup.Standalone()
		addr = httpSchema + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("executes stream query with BydbQL", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		// Write test data
		cases_stream_data.Write(conn, "sw", now, interval)

		// Test BydbQL query execution
		rootCmd.SetArgs([]string{"bydbql", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
SELECT trace_id FROM STREAM sw in (default) TIME BETWEEN '%s' AND '%s'`, nowStr, endStr)))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(streamv1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Elements)
		}, flags.EventuallyTimeout).Should(Equal(5))
	})

	It("executes stream query with WHERE condition", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		// Write test data
		cases_stream_data.Write(conn, "sw", now, interval)

		// Test BydbQL query with WHERE condition
		rootCmd.SetArgs([]string{"bydbql", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
SELECT trace_id FROM STREAM sw in (default) WHERE trace_id = 'trace-1' TIME BETWEEN '%s' AND '%s' LIMIT 10`, nowStr, endStr)))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(streamv1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Elements)
		}, time.Microsecond).Should(Equal(1))
	})

	It("executes stream query with relative time range", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		// Write test data
		cases_stream_data.Write(conn, "sw", now, interval)

		// Test BydbQL query with relative time range
		rootCmd.SetArgs([]string{"bydbql", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(`
SELECT trace_id FROM STREAM sw TIME > '-30m' LIMIT 10`))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(streamv1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Elements)
		}, flags.EventuallyTimeout).Should(Equal(5))
	})

	It("executes stream query with multiple projections", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		// Write test data
		cases_stream_data.Write(conn, "sw", now, interval)

		// Test BydbQL query with multiple projections
		rootCmd.SetArgs([]string{"bydbql", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(fmt.Sprintf(`
SELECT trace_id, service_id FROM STREAM sw TIME BETWEEN '%s' AND '%s' LIMIT 10`, nowStr, endStr)))
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(issue, flags.EventuallyTimeout).ShouldNot(ContainSubstring("code:"))
		Eventually(func() int {
			out := issue()
			resp := new(streamv1.QueryResponse)
			helpers.UnmarshalYAML([]byte(out), resp)
			GinkgoWriter.Println(resp)
			return len(resp.Elements)
		}, flags.EventuallyTimeout).Should(Equal(5))
	})

	It("handles invalid BydbQL syntax", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		// Write test data
		cases_stream_data.Write(conn, "sw", now, interval)

		// Test invalid BydbQL query
		rootCmd.SetArgs([]string{"bydbql", "query", "-a", addr, "-f", "-"})
		issue := func() string {
			rootCmd.SetIn(strings.NewReader(`
SELECT FROM STREAM sw`))
			return capturer.CaptureStderr(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails:%v", err)
				}
			})
		}
		Eventually(issue, flags.EventuallyTimeout).Should(ContainSubstring("parsing errors"))
	})

	AfterEach(func() {
		deferFunc()
	})
})

var _ = Describe("BydbQL Command Structure", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command

	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr

		// Create root command with all subcommands
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	AfterEach(func() {
		deferFunc()
	})

	It("creates command with correct structure", func() {
		// Find bydbql command in root commands
		var bydbqlCmd *cobra.Command
		for _, cmd := range rootCmd.Commands() {
			if cmd.Use == "bydbql" {
				bydbqlCmd = cmd
				break
			}
		}

		Expect(bydbqlCmd).NotTo(BeNil())
		Expect(bydbqlCmd.Use).To(Equal("bydbql"))

		// Test subcommands
		subCommands := bydbqlCmd.Commands()
		Expect(subCommands).To(HaveLen(2))

		// Check that the expected subcommands exist (order may vary)
		subCommandNames := make([]string, len(subCommands))
		for i, cmd := range subCommands {
			subCommandNames[i] = cmd.Use
		}
		Expect(subCommandNames).To(ContainElements("interactive", "query"))
	})

	It("displays help information correctly", func() {
		// Capture help output
		var output strings.Builder
		rootCmd.SetOut(&output)

		// Execute help for bydbql command
		rootCmd.SetArgs([]string{"bydbql", "--help"})
		err := rootCmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		helpOutput := output.String()

		// Verify help content includes key information
		expectedContent := []string{
			"BanyanDB Query Language",
			"BydbQL",
			"interactive",
			"query",
			"SQL-like",
		}

		for _, expected := range expectedContent {
			Expect(helpOutput).To(ContainSubstring(expected))
		}
	})
})
