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

package sdktest

import "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"

// LoadSO drives a REAL, loaded .so in-process: it opens path, verifies its
// ABIVersion symbol, looks up symbol (the constructor), and calls it with
// cfg. It is a thin pass-through to the shared sdk.OpenSampler — the exact
// loader-contract implementation the host runs — NOT the unexported
// banyand/trace.newSamplerFromPlugin var, which lives in a different package
// tree and is unreachable from here. A plugin author can therefore drive
// their built .so through the identical open/ABI-check/construct sequence the
// server uses, offline, before ever deploying it.
func LoadSO(path, symbol string, cfg []byte) (sdk.Sampler, error) {
	return sdk.OpenSampler(path, symbol, cfg)
}
