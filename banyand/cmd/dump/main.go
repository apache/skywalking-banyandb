// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package main provides a command-line tool to dump BanyanDB data.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/pkg/version"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     "dump",
		Short:   "Dump BanyanDB data from storage files",
		Version: version.Build(),
		Long: `dump is a command-line tool for dumping and inspecting BanyanDB storage data.
It provides subcommands for different data types (trace, stream, measure, etc.).`,
	}

	rootCmd.AddCommand(newTraceCmd())
	rootCmd.AddCommand(newStreamCmd())
	rootCmd.AddCommand(newSidxCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
