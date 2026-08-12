// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/sourcecatalog"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
)

func newTraceSourceCatalogCmd() *cobra.Command {
	var sourcePath, outputPath string
	command := &cobra.Command{
		Use:   "trace-source-catalog",
		Short: "Validate and catalog the frozen trace benchmark source shard",
		RunE: func(command *cobra.Command, _ []string) error {
			catalog, buildErr := sourcecatalog.Build(command.Context(), sourcecatalog.Options{
				SourcePath:   sourcePath,
				OutputPath:   outputPath,
				Format:       dumptrace.PartFormatLegacy,
				Expectations: sourcecatalog.DownloadedShardExpectations(),
			})
			if buildErr != nil {
				return fmt.Errorf("cannot build trace source catalog: %w", buildErr)
			}
			_, printErr := fmt.Fprintf(command.OutOrStdout(), "cataloged %d traces and %d rows at %s\n", catalog.Core.TraceCount, catalog.Core.RowCount, outputPath)
			if printErr != nil {
				return fmt.Errorf("cannot print trace source catalog summary: %w", printErr)
			}
			return nil
		},
	}
	command.Flags().StringVar(&sourcePath, "source-path", "", "Path to the immutable downloaded full-shard directory")
	command.Flags().StringVar(&outputPath, "output-path", "", "New directory for the catalog and ledgers")
	if requiredErr := command.MarkFlagRequired("source-path"); requiredErr != nil {
		panic(fmt.Sprintf("cannot require source-path flag: %v", requiredErr))
	}
	if requiredErr := command.MarkFlagRequired("output-path"); requiredErr != nil {
		panic(fmt.Sprintf("cannot require output-path flag: %v", requiredErr))
	}
	return command
}
