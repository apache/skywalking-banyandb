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

package api

import (
	"context"
)

//go:generate mockgen -destination=./inlet_mock.go -package=api github.com/apache/skywalking-banyandb/pkg/flow/streaming/api Inlet
// Inlet represents a type that exposes one open input.
type Inlet interface {
	In() chan<- interface{}
}

// Outlet represents a type that exposes one open output.
type Outlet interface {
	Out() <-chan interface{}
}

type Component interface {
	Setup(context.Context) error
	Teardown(context.Context) error
}

// Source represents a set of stream processing steps that has one open output.
type Source interface {
	Outlet
	Component
	Exec(downstream Inlet)
}

// Operator represents a set of stream processing steps that has one open input and one open output.
type Operator interface {
	Inlet
	Outlet
	Component
	Exec(downstream Inlet)
}

// Sink represents a set of stream processing steps that has one open input.
type Sink interface {
	Inlet
	Component
}
