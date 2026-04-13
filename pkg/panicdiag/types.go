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

// Package panicdiag provides panic recovery helpers and crash artifact writing.
package panicdiag

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// PanicRecord stores the structured panic information captured by recovery helpers.
type PanicRecord struct {
	OccurredAt      time.Time         `json:"occurredAt"`
	Component       string            `json:"component"`
	PanicValue      string            `json:"panicValue"`
	Recovered       bool              `json:"recovered"`
	GoroutineStack  string            `json:"goroutineStack"`
	Breadcrumbs     []Breadcrumb      `json:"breadcrumbs,omitempty"`
	StateDump       *StateDumpStatus  `json:"stateDump,omitempty"`
	ProcessMetadata map[string]string `json:"processMetadata,omitempty"`
}

// RecoveryOptions configures how panic recovery writes diagnostics.
type RecoveryOptions struct {
	Component       string
	ArtifactRoot    string
	Counter         meter.Counter
	Logger          *logger.Logger
	StateDumper     StateDumper
	StateLimitBytes int64
	ProcessMetadata map[string]string
}

// RecoveryResult contains the outcome of a recovered panic.
type RecoveryResult struct {
	Record      *PanicRecord
	ArtifactDir string
}

// Reporter receives the result of a recovered panic.
type Reporter func(context.Context, RecoveryResult)

// StateDumper returns a bounded diagnostic snapshot after a recovered panic.
type StateDumper interface {
	DumpState(context.Context) (any, error)
}

// Breadcrumb stores a semantic execution marker attached to a context.
type Breadcrumb struct {
	Time      time.Time         `json:"time"`
	Stage     string            `json:"stage"`
	Component string            `json:"component,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// StateDumpStatus describes the result of deep state serialization.
type StateDumpStatus struct {
	Path      string `json:"path,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
	Error     string `json:"error,omitempty"`
}
