//go:build !querymetrics

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

package grpc

import (
	"context"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// BydbQLPlanCacheStats returns zero observations in production builds.
func BydbQLPlanCacheStats() PlanCacheStats                { return PlanCacheStats{} }
func recordPlanCacheStats(PlanCacheStats, PlanCacheStats) {}
func registerCompiledPlanCache(*compiledPlanCache)        {}
func unregisterCompiledPlanCache(*compiledPlanCache)      {}
func bydbQLPlanCacheHooks() PlanCacheTestHooks            { return PlanCacheTestHooks{} }

// SetBydbQLPlanCacheHooksForTest requires an instrumented test binary.
func SetBydbQLPlanCacheHooksForTest(PlanCacheTestHooks) func() {
	panic("plan cache hooks require -tags=querymetrics")
}

// ClearBydbQLPlanCachesForTest requires an instrumented test binary.
func ClearBydbQLPlanCachesForTest() { panic("plan cache hooks require -tags=querymetrics") }

// ContextWithPlanCachePrincipalForTest requires an instrumented test binary.
func ContextWithPlanCachePrincipalForTest(context.Context, auth.Principal) context.Context {
	panic("plan cache hooks require -tags=querymetrics")
}

// BumpBydbQLMeasureGenerationForTest requires an instrumented test binary.
func BumpBydbQLMeasureGenerationForTest(string, string, *int64) func() {
	panic("plan cache hooks require -tags=querymetrics")
}

// WithholdBydbQLMeasureSchemaForTest requires an instrumented test binary.
func WithholdBydbQLMeasureSchemaForTest(string, string) func() {
	panic("plan cache hooks require -tags=querymetrics")
}
