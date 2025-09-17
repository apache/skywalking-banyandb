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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

// BydbQL endpoint paths
const (
	streamQueryPath   = "/api/v1/stream/query"
	measureQueryPath  = "/api/v1/measure/query"
	traceQueryPath    = "/api/v1/trace/query"
	propertyQueryPath = "/api/v1/property/query"
	topnQueryPath     = "/api/v1/measure/topn"
)

// newBydbQLCmd creates the BydbQL command
func newBydbQLCmd() *cobra.Command {
	bydbqlCmd := &cobra.Command{
		Use:     "bydbql",
		Version: version.Build(),
		Short:   "BanyanDB Query Language (BydbQL) interface",
		Long: `BanyanDB Query Language (BydbQL) provides a SQL-like interface for querying BanyanDB.
It supports querying streams, measures, traces, properties, and top-N operations with familiar SQL syntax.

Examples:
  # Interactive mode
  bydbctl bydbql

  # Execute query from file
  bydbctl bydbql query -f query.sql

  # Execute query from stdin
  echo "SELECT * FROM STREAM sw WHERE service_id = 'webapp'" | bydbctl bydbql query -f -`,
	}

	// Interactive command
	interactiveCmd := &cobra.Command{
		Use:     "interactive",
		Aliases: []string{"i"},
		Version: version.Build(),
		Short:   "Start interactive BydbQL shell",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInteractiveMode(cmd)
		},
	}

	// Query command
	queryCmd := &cobra.Command{
		Use:     "query [-f file]",
		Version: version.Build(),
		Short:   "Execute BydbQL query from file or stdin",
		Long: `Execute BydbQL query from a file or stdin. The query will be parsed and translated to YAML format,
then sent to the appropriate BanyanDB endpoint.

Supported query types:
  - SELECT queries for streams, measures, traces, and properties
  - SHOW TOP N queries for top-N operations

Examples:
  bydbctl bydbql query -f query.sql
  echo "SELECT * FROM STREAM sw" | bydbctl bydbql query -f -`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runQueryMode(cmd)
		},
	}

	bindFileFlag(queryCmd)
	bindTLSRelatedFlag(interactiveCmd, queryCmd)

	bydbqlCmd.AddCommand(interactiveCmd, queryCmd)

	// Default to interactive mode if no subcommand specified
	bydbqlCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return runInteractiveMode(cmd)
	}

	return bydbqlCmd
}

// runInteractiveMode starts the interactive BydbQL shell
func runInteractiveMode(cmd *cobra.Command) error {
	fmt.Println("BanyanDB Query Language (BydbQL) Interactive Shell")
	fmt.Println("Type 'help' for help, 'exit' or 'quit' to quit.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("bydbql> ")

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}

		// Handle special commands
		switch strings.ToLower(line) {
		case "help":
			printHelp()
			continue
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return nil
		case "\\q":
			fmt.Println("Goodbye!")
			return nil
		}

		// Check for multi-line queries (ending with semicolon)
		query := line
		for !strings.HasSuffix(strings.TrimSpace(query), ";") {
			fmt.Print("    -> ")
			if !scanner.Scan() {
				break
			}
			query += " " + scanner.Text()
		}

		// Remove trailing semicolon
		query = strings.TrimSuffix(strings.TrimSpace(query), ";")

		// Execute the query
		if err := executeBydbQLQuery(query); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		fmt.Println()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

// runQueryMode executes BydbQL query from file or stdin
func runQueryMode(cmd *cobra.Command) error {
	var reader io.Reader

	if filePath == "-" || filePath == "" {
		reader = cmd.InOrStdin()
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("error opening file %s: %w", filePath, err)
		}
		defer file.Close()
		reader = file
	}

	// Read the entire query
	content, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("error reading query: %w", err)
	}

	query := strings.TrimSpace(string(content))
	query = strings.TrimSuffix(query, ";")

	if query == "" {
		return fmt.Errorf("empty query")
	}

	return executeBydbQLQuery(query)
}

// executeBydbQLQuery parses and executes a BydbQL query
func executeBydbQLQuery(query string) error {
	// Create query context
	context := &bydbql.QueryContext{
		DefaultGroup:        viper.GetString("group"),
		DefaultResourceType: bydbql.ResourceTypeAuto,
	}

	// Parse the query
	parsed, parseErrors := bydbql.ParseQuery(query)
	if len(parseErrors) > 0 {
		return fmt.Errorf("parsing errors:\n  %s", strings.Join(parseErrors, "\n  "))
	}

	if parsed == nil {
		return fmt.Errorf("failed to parse query")
	}

	// Merge context
	if context.DefaultGroup != "" && len(parsed.Groups) == 0 {
		parsed.Groups = []string{context.DefaultGroup}
	}

	// Translate to YAML
	translator := bydbql.NewTranslator(context)
	yamlData, err := translator.TranslateToMap(parsed)
	if err != nil {
		return fmt.Errorf("translation error: %w", err)
	}
	fmt.Printf("Translated YAML:\n%s\n", yamlData)

	// Convert to JSON for REST API
	jsonData, err := json.Marshal(yamlData)
	if err != nil {
		return fmt.Errorf("JSON conversion error: %w", err)
	}

	// Determine endpoint based on query type
	endpoint, err := determineEndpoint(parsed)
	if err != nil {
		return err
	}

	// Execute the query via REST API
	return executeRESTQuery(endpoint, jsonData)
}

// determineEndpoint determines the REST endpoint based on query type
func determineEndpoint(parsed *bydbql.ParsedQuery) (string, error) {
	switch parsed.Statement.(type) {
	case *bydbql.SelectStatement:
		// Determine resource type
		resourceType := parsed.ResourceType
		if resourceType == bydbql.ResourceTypeAuto {
			// Default to stream if not specified
			resourceType = bydbql.ResourceTypeStream
		}

		switch resourceType {
		case bydbql.ResourceTypeStream:
			return streamQueryPath, nil
		case bydbql.ResourceTypeMeasure:
			return measureQueryPath, nil
		case bydbql.ResourceTypeTrace:
			return traceQueryPath, nil
		case bydbql.ResourceTypeProperty:
			return propertyQueryPath, nil
		default:
			return "", fmt.Errorf("unsupported resource type: %s", resourceType.String())
		}

	case *bydbql.TopNStatement:
		return topnQueryPath, nil

	default:
		return "", fmt.Errorf("unsupported statement type")
	}
}

// executeRESTQuery executes the query via REST API
func executeRESTQuery(endpoint string, jsonData []byte) error {
	// Create REST request using existing infrastructure
	reqBodyData := reqBody{
		data: jsonData,
	}

	// Use existing rest function with appropriate parameters
	return rest(
		func() ([]reqBody, error) {
			return []reqBody{reqBodyData}, nil
		},
		func(req request) (*resty.Response, error) {
			url := viper.GetString("addr") + endpoint
			resp, err := req.req.
				SetHeader("Content-Type", "application/json").
				SetBody(req.data).
				Post(url)
			return resp, err
		},
		yamlPrinter,
		enableTLS,
		insecure,
		cert,
	)
}

// printHelp prints help information for the interactive mode
func printHelp() {
	fmt.Println("BanyanDB Query Language (BydbQL) Help")
	fmt.Println("=====================================")
	fmt.Println()
	fmt.Println("BydbQL supports SQL-like syntax for querying BanyanDB resources:")
	fmt.Println()
	fmt.Println("STREAM QUERIES:")
	fmt.Println("  SELECT * FROM STREAM sw WHERE service_id = 'webapp';")
	fmt.Println("  SELECT trace_id, service_id FROM STREAM sw TIME > '-30m';")
	fmt.Println("  SELECT * FROM STREAM sw TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z';")
	fmt.Println()
	fmt.Println("MEASURE QUERIES:")
	fmt.Println("  SELECT region, SUM(latency) FROM MEASURE service_cpm GROUP BY region;")
	fmt.Println("  SELECT TOP 10 instance, cpu_usage FROM MEASURE instance_metrics ORDER BY cpu_usage DESC;")
	fmt.Println("  SELECT AVG(response_time) FROM MEASURE http_metrics WHERE region = 'us-west';")
	fmt.Println()
	fmt.Println("TRACE QUERIES:")
	fmt.Println("  SELECT trace_id, service_id FROM TRACE sw_trace WHERE status = 'error';")
	fmt.Println("  SELECT () FROM TRACE sw_trace WHERE service_id = 'webapp' LIMIT 100;")
	fmt.Println()
	fmt.Println("PROPERTY QUERIES:")
	fmt.Println("  SELECT ip, owner FROM PROPERTY server_metadata WHERE datacenter = 'dc-101';")
	fmt.Println("  SELECT * FROM PROPERTY server_metadata WHERE ID = 'server-123';")
	fmt.Println()
	fmt.Println("TOP-N QUERIES:")
	fmt.Println("  SHOW TOP 10 FROM MEASURE service_latency ORDER BY value DESC;")
	fmt.Println("  SHOW TOP 5 FROM MEASURE service_errors WHERE status_code = '500';")
	fmt.Println()
	fmt.Println("TIME FORMATS:")
	fmt.Println("  Absolute: '2023-01-01T00:00:00Z' (RFC3339)")
	fmt.Println("  Relative: '-30m', '2h', '-1d', '-1w', 'now'")
	fmt.Println()
	fmt.Println("OPERATORS:")
	fmt.Println("  =, !=, >, <, >=, <=, IN, NOT IN, HAVING, NOT HAVING, MATCH")
	fmt.Println("  AND, OR for combining conditions")
	fmt.Println()
	fmt.Println("SPECIAL COMMANDS:")
	fmt.Println("  help     - Show this help")
	fmt.Println("  exit     - Exit the shell")
	fmt.Println("  quit     - Exit the shell")
	fmt.Println("  \\q       - Exit the shell")
	fmt.Println()
	fmt.Println("QUERY TRACING:")
	fmt.Println("  Add 'WITH QUERY_TRACE' to any query for distributed tracing")
	fmt.Println()
	fmt.Println("NOTE: Queries can span multiple lines and should end with semicolon (;)")
}

// printQueryResult prints the query result in YAML format
func printQueryResult(data []byte) error {
	// The data is already in JSON format from the REST response
	// Convert it to YAML for better readability
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		// If JSON parsing fails, print raw data
		fmt.Println(string(data))
		return nil
	}

	yamlData, err := yaml.Marshal(jsonData)
	if err != nil {
		// If YAML conversion fails, print JSON data
		fmt.Println(string(data))
		return nil
	}

	fmt.Print(string(yamlData))
	return nil
}

