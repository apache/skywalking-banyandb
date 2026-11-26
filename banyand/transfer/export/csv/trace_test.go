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

package csv

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

func TestTraceResolver_SupersetFallback(t *testing.T) {
	newTrace := func(name string, tags ...string) *databasev1.Trace {
		return &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: name, Group: "g"}, Tags: traceTagsOf(tags...), TraceIdTagName: "tid", SpanIdTagName: "sid",
		}
	}
	traces := []*databasev1.Trace{newTrace("segment", "a", "b", "c"), newTrace("zipkin", "a", "d")}
	layout, err := csvfmt.TraceLayout(traces)
	require.NoError(t, err)
	resolver, _, err := newTraceResolver("g", traces, layout)
	require.NoError(t, err)
	row := func(names ...string) map[string][]byte {
		out := map[string][]byte{}
		for _, n := range names {
			out[n] = nil
		}
		return out
	}
	nameOf := func(res *traceResource) string {
		if res == nil {
			return ""
		}
		return res.name
	}
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b", "c"))), "exact match")
	require.Equal(t, "zipkin", nameOf(resolver.resolve(row("a", "d"))), "exact match")
	// A row written before c was added to segment: only segment holds {a, b}.
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b"))), "unique superset")
	require.Equal(t, "segment", nameOf(resolver.resolve(row("b", "a"))), "cached superset")
	require.Equal(t, "zipkin", nameOf(resolver.resolve(row("d"))), "unique superset")
	// Both traces hold {a}; no trace holds x.
	require.Nil(t, resolver.resolve(row("a")), "ambiguous superset")
	require.Nil(t, resolver.resolve(row("b", "d")), "no superset")
	// x was deleted from the schema: it does not keep the row from its trace.
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b", "c", "x"))), "deleted tag ignored")
	require.Nil(t, resolver.resolve(row("a", "x")), "deleted tag ignored, ambiguous superset")
	// A merge renamed b to b#str and b#int: both still hold tag b.
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b#str", "b#int", "c"))), "typed columns")
}

func TestTraceSignature(t *testing.T) {
	tr := &databasev1.Trace{Tags: traceTagsOf("z", "a"), TraceIdTagName: "tid", SpanIdTagName: "sid"}
	declared := map[string]struct{}{"a": {}, "z": {}}
	sig, _ := rowSignature(map[string][]byte{"a": nil, "z": nil}, declared, nil)
	require.Equal(t, traceSignature(tr), sig)
	other, _ := rowSignature(map[string][]byte{"a": nil}, declared, nil)
	require.NotEqual(t, traceSignature(tr), other)
}
