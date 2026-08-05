// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Command alwayskeepsampler is the metadata-only native plugin used to measure
// the minimum trace-pipeline framework cost.
package main

import "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"

// ABIVersion declares the SDK ABI implemented by this plugin.
var ABIVersion = sdk.ABIVersion

type alwaysKeepSampler struct{}

// NewSampler constructs a metadata-only sampler that retains every trace.
func NewSampler(_ []byte) (sdk.Sampler, error) {
	return alwaysKeepSampler{}, nil
}

func (alwaysKeepSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (alwaysKeepSampler) Project() sdk.Projection { return sdk.Projection{} }
func (alwaysKeepSampler) Close() error            { return nil }
func (alwaysKeepSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range keep {
		keep[traceIdx] = true
	}
	return sdk.Verdict{Keep: keep}, nil
}
