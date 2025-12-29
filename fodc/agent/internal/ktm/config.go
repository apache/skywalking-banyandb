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

package ktm

import (
	"time"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor"
)

// Config represents the configuration for the KTM module.
type Config struct {
	Modules  []string             `mapstructure:"modules"`
	EBPF     iomonitor.EBPFConfig `mapstructure:"ebpf"`
	Interval time.Duration        `mapstructure:"interval"`
	Enabled  bool                 `mapstructure:"enabled"`
}

// DefaultConfig returns the default configuration for KTM.
func DefaultConfig() Config {
	return Config{
		Enabled:  false,
		Interval: 10 * time.Second,
		Modules:  []string{"iomonitor"},
		EBPF:     iomonitor.EBPFConfig{},
	}
}
