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
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"strings"
)

// ABIVersion identifies the pinned host ABI.
var ABIVersion = sdk.ABIVersion

type sampler struct{}

func (*sampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (*sampler) Project() sdk.Projection { return sdk.Projection{Spans: true} }
func (*sampler) Close() error            { return nil }
func (*sampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for index, trace := range batch.Traces {
		// An incomplete per-part scan retains this trace, making assembly failures visible.
		keep[index] = (trace.TraceID != "sample-drop" && !strings.HasPrefix(trace.TraceID, "sample-drop-")) || len(trace.Spans) != 3
	}
	return sdk.Verdict{Keep: keep}, nil
}

// NewSampler constructs a deterministic whole-trace lifecycle test sampler.
func NewSampler([]byte) (sdk.Sampler, error) { return &sampler{}, nil }
