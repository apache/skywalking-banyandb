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

//go:build !linux || !(amd64 || arm64 || 386)

package iomonitor

import (
	"github.com/rs/zerolog"

	fodcmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

type module struct {
	logger zerolog.Logger
	name   string
}

func newModule(log zerolog.Logger, _ EBPFConfig) (*module, error) {
	return &module{
		logger: log,
		name:   "iomonitor",
	}, nil
}

func (m *module) Name() string {
	return m.name
}

func (m *module) Start() error {
	m.logger.Info().Str("module", m.name).Msg("I/O monitor module runs in no-op mode on this platform")
	return nil
}

func (m *module) Stop() error {
	return nil
}

func (m *module) Degraded() bool {
	return false
}

func (m *module) Collect() ([]fodcmetrics.RawMetric, error) {
	return []fodcmetrics.RawMetric{}, nil
}
