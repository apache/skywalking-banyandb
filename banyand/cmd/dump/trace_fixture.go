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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracefixture"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

const defaultFixtureDayStart = "2026-01-01T00:00:00Z"

func newTraceFixtureCmd() *cobra.Command {
	var sourcePath, catalogPath, outputPath, pluginPath, dayStartText string
	var writeIntensity int
	command := &cobra.Command{
		Use:   "trace-generate-fixture",
		Short: "Generate the production-written hybrid trace merge benchmark fixture",
		RunE: func(command *cobra.Command, _ []string) (runErr error) {
			dayStart, parseErr := time.Parse(time.RFC3339, dayStartText)
			if parseErr != nil {
				return fmt.Errorf("cannot parse fixture day start %q: %w", dayStartText, parseErr)
			}
			source, loadErr := tracefixture.LoadSource(command.Context(), tracefixture.LoadOptions{
				SourcePath: sourcePath, CatalogPath: catalogPath, Format: dumptrace.PartFormatLegacy,
			})
			if loadErr != nil {
				return fmt.Errorf("cannot load fixture source: %w", loadErr)
			}
			plan, planErr := tracefixture.BuildSourcePlan(source, tracefixture.Options{
				DayStart: dayStart, DayDuration: 24 * time.Hour, Shapes: tracefixture.DefaultShapes(), CopyCount: tracefixture.CopyTraceCount,
				WriteIntensity: writeIntensity,
			})
			if planErr != nil {
				return fmt.Errorf("cannot plan fixture: %w", planErr)
			}
			sampler, samplerErr := sdktest.LoadSO(pluginPath, "NewSampler", tracefixture.DefaultSkyWalkingSamplerConfig)
			if samplerErr != nil {
				return fmt.Errorf("cannot load default SkyWalking sampler: %w", samplerErr)
			}
			defer func() {
				runErr = errors.Join(runErr, sampler.Close())
			}()
			samplerArtifact, evaluateErr := tracefixture.EvaluateSampler(command.Context(), source, plan, sampler, pluginPath,
				tracefixture.DefaultSkyWalkingSamplerConfig)
			if evaluateErr != nil {
				return fmt.Errorf("cannot evaluate default SkyWalking sampler: %w", evaluateErr)
			}
			artifact, generateErr := tracefixture.Generate(command.Context(), source, plan, tracefixture.GenerateOptions{
				OutputPath: outputPath, DayStart: dayStart, DayDuration: 24 * time.Hour, MergeGrace: 2 * time.Hour,
			})
			if generateErr != nil {
				return fmt.Errorf("cannot generate fixture: %w", generateErr)
			}
			if writeErr := writeFixtureSamplerArtifact(outputPath, samplerArtifact); writeErr != nil {
				return fmt.Errorf("cannot persist sampler artifact: %w", writeErr)
			}
			_, printErr := fmt.Fprintf(command.OutOrStdout(),
				"generated %d traces, %d rows, and %d writes at %s; sampler deleted %.3f%%\n",
				artifact.TraceCount, artifact.RowCount, artifact.WriteCount, outputPath, samplerArtifact.DeletionRatio*100)
			if printErr != nil {
				return fmt.Errorf("cannot print fixture summary: %w", printErr)
			}
			return nil
		},
	}
	command.Flags().StringVar(&sourcePath, "source-path", "", "Path to the immutable downloaded full-shard directory")
	command.Flags().StringVar(&catalogPath, "catalog-path", "", "Path to the immutable source catalog JSON")
	command.Flags().StringVar(&outputPath, "output-path", "", "New directory for the generated fixture")
	command.Flags().StringVar(&pluginPath, "plugin-path", "", "Path to the built sw-trace-sampler shared object")
	command.Flags().StringVar(&dayStartText, "day-start", defaultFixtureDayStart, "UTC RFC3339 start of the logical day")
	command.Flags().IntVar(&writeIntensity, "write-intensity", 1, "Production-shaped write streams per logical day")
	for _, flagName := range []string{"source-path", "catalog-path", "output-path", "plugin-path"} {
		if requiredErr := command.MarkFlagRequired(flagName); requiredErr != nil {
			panic(fmt.Sprintf("cannot require %s flag: %v", flagName, requiredErr))
		}
	}
	return command
}

func writeFixtureSamplerArtifact(root string, artifact tracefixture.SamplerArtifact) error {
	data, marshalErr := json.MarshalIndent(artifact, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("cannot marshal sampler artifact: %w", marshalErr)
	}
	data = append(data, '\n')
	path := filepath.Join(root, "sampler.json")
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		return fmt.Errorf("cannot write sampler artifact %q: %w", path, writeErr)
	}
	return nil
}
