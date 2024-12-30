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

// Package cgroups provides a way to get the number of CPUs.
package cgroups

import (
	"os"
	"runtime"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// CPUs returns the number of CPUs.
func CPUs() int {
	return runtime.GOMAXPROCS(-1)
}

func init() {
	if maxProcs, exists := os.LookupEnv("GOMAXPROCS"); exists {
		logger.Infof("honoring GOMAXPROCS=%q as set in environment", maxProcs)
		return
	}
	_, err := maxprocs.Set(maxprocs.Logger(logger.Infof))
	if err != nil {
		logger.Warningf("failed to set GOMAXPROCS: %v", err)
	}
	gomaxprocs := runtime.GOMAXPROCS(-1)
	if gomaxprocs <= 0 {
		gomaxprocs = 1
	}
	numCPU := runtime.NumCPU()
	if gomaxprocs > numCPU {
		gomaxprocs = numCPU
	}
	runtime.GOMAXPROCS(gomaxprocs)
}
