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

package ql

// NativeAggregateExecutionCounters records successful keyed merge executions.
// Memory counters cover successful and failed lifecycles; production builds return zero.
type NativeAggregateExecutionCounters struct {
	Executions    uint64
	Branches      uint64
	InputRows     uint64
	Groups        uint64
	GroupCapacity uint64
	KeyBytes      uint64
	KeyCapacity   uint64
	StateBytes    uint64
	StateCapacity uint64
	StateLanes    uint64
	MergeVisits   uint64
	ReservedBytes uint64
	ReleasedBytes uint64
	LiveBytes     int64
}
