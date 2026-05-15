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

// Package plan is the vectorized measure-query plan tree (G8).
//
// This package is a peer of pkg/query/logical/measure (deprecated, row
// path); the two share no plan-node types, executor wiring, or iterator
// machinery. Top-level dispatch (banyand/query/processor.go, G8d) routes
// requests to one OR the other based on VectorizedConfig.Enabled.
//
// A VecPlan node knows its output BatchSchema, its children, and how to
// append itself to a *vectorized.PipelineBuilder during Build. Build is
// bottom-up: a node calls Build on its child first, then attaches its own
// operator. The root's Build returns a fully-composed PipelineBuilder
// that the executor (G8c) finalizes via builder.Build() to produce a
// *vectorized.Pipeline.
package plan

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// VecPlan is the vectorized measure-query plan node interface.
//
// Schema returns the BatchSchema of rows this node emits — for fusible
// or schema-preserving breakers it matches the input schema; for
// schema-rewriting breakers (BatchAggregation) it is the operator's
// OutputSchema.
//
// Children returns the immediate child plan nodes (zero for leaves).
//
// Build appends this node's contribution to bc.Builder. The leaf (Scan)
// calls Builder.From; fusible nodes call Builder.Apply; breakers call
// Builder.Break. Build must call Build on its children before attaching
// itself so the source flows in tree order.
//
// String returns a single-line debug description ("Scan(measure=foo)",
// "GroupByAgg(keys=svc, fn=sum, field=value)", etc.) so plan trees can be
// pretty-printed with PrintTree.
type VecPlan interface {
	Schema() *vectorized.BatchSchema
	Children() []VecPlan
	Build(ctx context.Context, bc *BuildContext) error
	String() string
}

// BuildContext is the cross-cutting state threaded through every Build
// call. Builder accumulates operators; Tracker is the shared per-pipeline
// MemoryTracker (G7a) every memory-bookkeeping operator must use; Config
// supplies BatchSize and other runtime knobs.
type BuildContext struct {
	Builder *vectorized.PipelineBuilder
	Tracker *vectorized.MemoryTracker
	Config  measure.VectorizedConfig
}

// PrintTree renders a plan tree as multi-line text. Leaves first column,
// each parent indented two spaces deeper than its child. Useful for
// debugging analyzer output.
func PrintTree(root VecPlan) string {
	var sb stringBuilder
	printNode(&sb, root, 0)
	return sb.String()
}

func printNode(sb *stringBuilder, node VecPlan, depth int) {
	for range depth {
		sb.WriteString("  ")
	}
	sb.WriteString(node.String())
	sb.WriteString("\n")
	for _, child := range node.Children() {
		printNode(sb, child, depth+1)
	}
}

// stringBuilder is a tiny shim around strings.Builder to keep imports
// out of the package-level scope. Inline so test-only helpers don't pull
// strings into production builds.
type stringBuilder struct{ buf []byte }

func (s *stringBuilder) WriteString(str string) { s.buf = append(s.buf, str...) }
func (s *stringBuilder) String() string         { return string(s.buf) }
