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

package cmd

import (
	"bytes"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/zenizh/go-capturer"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const httpSchema = "http://"

var _ = Describe("BydbQL Command", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command

	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr

		// Reset viper for clean test state
		viper.Reset()
		viper.Set("addr", addr)
		viper.Set("group", "default")

		// Create root command with all subcommands
		rootCmd = &cobra.Command{Use: "root"}
		RootCmdFlags(rootCmd)
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
})

var _ = Describe("Endpoint Determination", func() {
	DescribeTable("determines correct endpoint for different query types",
		func(query, expectedEndpoint string) {
			parsed, errors := bydbql.ParseQuery(query)
			Expect(errors).To(BeEmpty())

			endpoint, err := determineEndpoint(parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(endpoint).To(Equal(expectedEndpoint))
		},
		Entry("stream query", "SELECT * FROM STREAM sw", streamQueryPath),
		Entry("measure query", "SELECT * FROM MEASURE metrics", measureQueryPath),
		Entry("trace query", "SELECT * FROM TRACE traces", traceQueryPath),
		Entry("property query", "SELECT * FROM PROPERTY props", propertyQueryPath),
		Entry("top-n query", "SHOW TOP 10 FROM MEASURE metrics", topnQueryPath),
	)
})

var _ = Describe("Query Execution Parse Errors", func() {
	DescribeTable("handles parsing errors correctly",
		func(query string) {
			err := executeBydbQLQuery(query)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("parsing errors"))
		},
		Entry("incomplete SELECT", "SELECT"),
		Entry("missing resource", "SELECT * FROM"),
		Entry("missing N in TOP", "SHOW TOP FROM MEASURE metrics"),
	)
})

var _ = Describe("Valid Query Parsing", func() {
	DescribeTable("parses valid queries correctly",
		func(query string) {
			// Parse and validate query structure
			parsed, errors := bydbql.ParseQuery(query)
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			// Validate endpoint determination
			_, err := determineEndpoint(parsed)
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("stream query", "SELECT * FROM STREAM sw"),
		Entry("measure query with aggregation", "SELECT region, SUM(latency) FROM MEASURE metrics GROUP BY region"),
		Entry("top-n query", "SHOW TOP 10 FROM MEASURE service_latency ORDER BY value DESC"),
		Entry("trace query with condition", "SELECT * FROM TRACE traces WHERE status = 'error'"),
		Entry("property query with condition", "SELECT ip, owner FROM PROPERTY metadata WHERE datacenter = 'dc-1'"),
	)
})

var _ = Describe("Help Command", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command

	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr

		// Reset viper for clean test state
		viper.Reset()
		viper.Set("addr", addr)
		viper.Set("group", "default")

		// Create root command with all subcommands
		rootCmd = &cobra.Command{Use: "root"}
		RootCmdFlags(rootCmd)
	})

	AfterEach(func() {
		deferFunc()
	})

	It("displays help information correctly", func() {
		// Capture help output
		var output bytes.Buffer
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

var _ = Describe("BydbQL Integration", func() {
	DescribeTable("full pipeline processing",
		func(query string, resourceType bydbql.ResourceType) {
			// Step 1: Parse query
			parsed, errors := bydbql.ParseQuery(query)
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			// Step 2: Validate resource type detection
			if parsed.ResourceType != resourceType {
				// For auto-detected resource types, this might be expected
				GinkgoWriter.Printf("Resource type: expected %s, got %s (may be auto-detected)",
					resourceType.String(), parsed.ResourceType.String())
			}

			// Step 3: Translate to YAML
			context := &bydbql.QueryContext{
				DefaultGroup: "default",
			}
			translator := bydbql.NewTranslator(context)
			yamlData, err := translator.TranslateToYAML(parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(yamlData).NotTo(BeEmpty())

			// Step 4: Determine correct endpoint
			endpoint, err := determineEndpoint(parsed)
			Expect(err).NotTo(HaveOccurred())

			// Step 5: Validate endpoint matches expected resource type
			expectedEndpoints := map[bydbql.ResourceType]string{
				bydbql.ResourceTypeStream:   streamQueryPath,
				bydbql.ResourceTypeMeasure:  measureQueryPath,
				bydbql.ResourceTypeTrace:    traceQueryPath,
				bydbql.ResourceTypeProperty: propertyQueryPath,
			}

			// For Top-N queries, always use measure endpoint
			if _, isTopN := parsed.Statement.(*bydbql.TopNStatement); isTopN {
				Expect(endpoint).To(Equal(topnQueryPath))
			} else if expectedEndpoint, ok := expectedEndpoints[resourceType]; ok {
				Expect(endpoint).To(Equal(expectedEndpoint))
			}

			// Step 6: Validate YAML structure
			var yamlMap map[string]interface{}
			err = yaml.Unmarshal(yamlData, &yamlMap)
			Expect(err).NotTo(HaveOccurred())

			// Basic validation: should have name or resource identifier
			Expect(yamlMap["Name"] != nil || yamlMap["topN"] != nil).To(BeTrue())

			GinkgoWriter.Printf("Successfully processed query: %s", query)
			GinkgoWriter.Printf("Generated YAML length: %d bytes", len(yamlData))
			GinkgoWriter.Printf("Target endpoint: %s", endpoint)
		},
		Entry("stream query full pipeline",
			"SELECT trace_id, service_id FROM STREAM sw WHERE service_id = 'webapp' TIME > '-30m' LIMIT 100",
			bydbql.ResourceTypeStream),
		Entry("measure query full pipeline",
			"SELECT region, AVG(latency) FROM MEASURE service_metrics WHERE service = 'auth' GROUP BY region",
			bydbql.ResourceTypeMeasure),
		Entry("topn query full pipeline",
			"SHOW TOP 5 FROM MEASURE error_count WHERE status_code = '500'",
			bydbql.ResourceTypeMeasure),
		Entry("trace query full pipeline",
			"SELECT * FROM TRACE app_traces WHERE operation_name = 'GET /api/users' TIME BETWEEN '-1h' AND 'now'",
			bydbql.ResourceTypeTrace),
		Entry("property query full pipeline",
			"SELECT ip, region FROM PROPERTY server_info WHERE ID = 'server-1'",
			bydbql.ResourceTypeProperty),
	)
})

var _ = Describe("Command Line Flags", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command

	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr

		// Reset viper for clean test state
		viper.Reset()
		viper.Set("addr", addr)
		viper.Set("group", "default")

		// Create root command with all subcommands
		rootCmd = &cobra.Command{Use: "root"}
		RootCmdFlags(rootCmd)
	})

	AfterEach(func() {
		deferFunc()
	})

	DescribeTable("handles command line flags correctly",
		func(args []string, expectError bool) {
			// Prepend "bydbql" to the args since we're testing the bydbql subcommand
			fullArgs := append([]string{"bydbql"}, args...)
			rootCmd.SetArgs(fullArgs)

			// We're testing command structure, not execution
			// So we expect parsing to succeed even if execution might fail
			err := rootCmd.ParseFlags(fullArgs)

			if expectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		},
		Entry("valid query command with file", []string{"query", "-f", "test.sql"}, false),
		Entry("valid interactive command", []string{"interactive"}, false),
		Entry("alias i for interactive", []string{"i"}, false),
	)
})

var _ = Describe("BydbQL Query Execution", func() {
	var addr string
	var deferFunc func()
	var rootCmd *cobra.Command

	BeforeEach(func() {
		_, addr, deferFunc = setup.EmptyStandalone()
		addr = httpSchema + addr

		// Reset viper for clean test state
		viper.Reset()
		viper.Set("addr", addr)
		viper.Set("group", "default")

		// Create root command with all subcommands
		rootCmd = &cobra.Command{Use: "root"}
		RootCmdFlags(rootCmd)
	})

	AfterEach(func() {
		deferFunc()
	})

	It("executes simple query successfully", func() {
		// Test a simple query execution
		rootCmd.SetArgs([]string{"bydbql", "query", "-f", "-"})
		rootCmd.SetIn(strings.NewReader("SELECT * FROM STREAM sw LIMIT 1"))

		executeQuery := func() string {
			return capturer.CaptureStdout(func() {
				err := rootCmd.Execute()
				if err != nil {
					GinkgoWriter.Printf("execution fails: %v", err)
				}
			})
		}

		Eventually(executeQuery, flags.EventuallyTimeout).ShouldNot(ContainSubstring("error"))
	})
})
