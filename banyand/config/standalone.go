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

package config

import (
	_ "embed"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/config"
)

//go:embed default/standalone.yaml
var standaloneDefault string

type Standalone struct {
	Logging config.Logging
}

func Load() (Standalone, error) {
	var c *config.Config
	var err error
	var s Standalone
	if c, err = config.NewConfig(standaloneDefault); err != nil {
		return s, fmt.Errorf("failed to initialize config system:%v", err)
	}
	if err = c.Unmarshal(&s); err != nil {
		return s, fmt.Errorf("failed to unmarshal config to standalone config")
	}
	return s, nil
}
