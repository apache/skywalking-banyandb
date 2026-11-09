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

package main

import (
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/transfer/rowdiff"
)

// maxPrintedDiffs bounds the diffs printed under firstDiffs; all of them go to stderr.
const maxPrintedDiffs = 20

// runCompare compares the logical rows of a source artifact A with those of artifact B, the
// re-export of the cluster A was imported into, over the groups present in A (see
// pkg/transfer/rowdiff), and prints for expected/import-roundtrip.yaml:
//
//	diffs: <number of differences>
//	catalogs: [{catalog, rowsA, rowsB}]          deduplicated rows per catalog (property: entities)
//	stageCounts: [{key, rowsA, rowsB}]           deduplicated rows per catalog/group/stage (property: entities)
//	firstDiffs: ["<diff>", ...]                  rowdiff.Report.Summary(maxPrintedDiffs), quoted and clipped
//
// It exits 1 when the artifacts differ and 2 when either cannot be read.
func runCompare(args []string) int {
	if len(args) != 2 {
		usage()
	}
	rep, err := rowdiff.Compare(args[0], args[1], rowdiff.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "compare:", err)
		fmt.Println("diffs: -1")
		return 2
	}
	fmt.Printf("diffs: %d\n", len(rep.Diffs))
	fmt.Println("catalogs:")
	for _, st := range rep.Stats {
		fmt.Printf("- catalog: %s\n  rowsA: %d\n  rowsB: %d\n", st.Catalog, st.RowsA, st.RowsB)
	}
	if len(rep.StageCounts) == 0 {
		fmt.Println("stageCounts: []")
	} else {
		fmt.Println("stageCounts:")
		for _, key := range slices.Sorted(maps.Keys(rep.StageCounts)) {
			c := rep.StageCounts[key]
			fmt.Printf("- key: %s\n  rowsA: %d\n  rowsB: %d\n", key, c[0], c[1])
		}
	}
	if len(rep.Diffs) == 0 {
		fmt.Println("firstDiffs: []")
		return 0
	}
	for _, line := range rep.Summary(-1) {
		fmt.Fprintln(os.Stderr, line)
	}
	fmt.Println("firstDiffs:")
	for _, line := range rep.Summary(maxPrintedDiffs) {
		fmt.Printf("- %s\n", yamlScalar(line))
	}
	return 1
}
