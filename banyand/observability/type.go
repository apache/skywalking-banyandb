// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package observability provides metrics, profiling, and etc.
package observability

import (
	"errors"

	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	errNoAddr         = errors.New("no address")
	errNoMode         = errors.New("no observability mode")
	errInvalidMode    = errors.New("invalid observability mode")
	errDuplicatedMode = errors.New("duplicated observability mode")
)

// MetricsRegistry is the interface for metrics registry.
type MetricsRegistry interface {
	run.Service
	// With returns a factory with the given scope.
	With(scope meter.Scope) *Factory
	// NativeEnabled returns whether the native mode is enabled.
	NativeEnabled() bool
}
