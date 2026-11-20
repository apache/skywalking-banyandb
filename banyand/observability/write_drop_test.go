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

package observability

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// namingRegistry records the full name and labels of each counter created through it.
type namingRegistry struct {
	MetricsRegistry
	created map[string][]string
}

func (r *namingRegistry) With(scope meter.Scope) Factory {
	return &namingFactory{r: r, ns: scope.GetNamespace()}
}

type namingFactory struct {
	Factory
	r  *namingRegistry
	ns string
}

func (f *namingFactory) NewCounter(name string, labelNames ...string) meter.Counter {
	f.r.created[f.ns+"_"+name] = labelNames
	return nil
}

func TestWriteDropCounterNames(t *testing.T) {
	r := &namingRegistry{created: map[string][]string{}}
	for _, model := range []string{"stream", "measure", "trace"} {
		NewWriteDropCounter(r, model)
	}
	require.Equal(t, map[string][]string{
		"banyandb_stream_write_dropped_total":  {"group", "reason"},
		"banyandb_measure_write_dropped_total": {"group", "reason"},
		"banyandb_trace_write_dropped_total":   {"group", "reason"},
	}, r.created, "each model has its own name, which the native meter provider keys a measure by")
	require.Nil(t, NewWriteDropCounter(nil, "stream"), "a service without a registry counts nothing")
}
