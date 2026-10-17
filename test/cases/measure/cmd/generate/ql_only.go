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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"flag"
	"fmt"

	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func runGenerateQL(arguments []string) error {
	options := flag.NewFlagSet("generate-ql", flag.ContinueOnError)
	check := options.Bool("check", false, "check committed generated cases without modifying them")
	if err := options.Parse(arguments); err != nil {
		return err
	}
	if options.NArg() > 1 {
		return fmt.Errorf("generate-ql accepts at most one output directory")
	}
	directory := "test/cases/measure/qlgen/generated"
	if options.NArg() == 1 {
		directory = options.Arg(0)
	}
	if *check {
		return qlgen.Check(directory)
	}
	if err := qlgen.Write(directory); err != nil {
		return err
	}
	suite, err := qlgen.Generate()
	if err != nil {
		return err
	}
	pending := 0
	for _, feature := range suite.Features {
		if feature.Status == "pending" {
			pending++
		}
	}
	fmt.Printf("Generated %d QL-only cases, %d reuse scenarios, %d aggregate scenarios, %d aggregate rejections, %d native aggregate scenarios, "+
		"%d expression scenarios, %d expression rejections, %d filter scenarios, %d filter rejections and %d legacy compatibility scenarios; "+
		"feasible pairs %d/%d; %d explicit pending features; output=%s\n",
		len(suite.Cases), len(suite.ReuseScenarios), len(suite.AggregateScenarios), len(suite.AggregateRejections), len(suite.NativeAggregateScenarios),
		len(suite.ExpressionScenarios), len(suite.ExpressionRejections), len(suite.FilterScenarios), len(suite.FilterRejections), len(suite.LegacyCompatibilityScenarios),
		suite.Pairs.Covered, suite.Pairs.Required, pending, directory)
	return nil
}
