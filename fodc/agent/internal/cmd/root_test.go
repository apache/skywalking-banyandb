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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateMetricsEndpoints_SinglePort(t *testing.T) {
	endpoints := generateMetricsEndpoints([]string{"2121"})

	assert.Len(t, endpoints, 1)
	assert.Equal(t, "http://localhost:2121/metrics", endpoints[0])
}

func TestGenerateMetricsEndpoints_MultiplePorts(t *testing.T) {
	endpoints := generateMetricsEndpoints([]string{"2121", "2122", "2123"})

	assert.Len(t, endpoints, 3)
	assert.Equal(t, "http://localhost:2121/metrics", endpoints[0])
	assert.Equal(t, "http://localhost:2122/metrics", endpoints[1])
	assert.Equal(t, "http://localhost:2123/metrics", endpoints[2])
}

func TestGenerateMetricsEndpoints_Empty(t *testing.T) {
	endpoints := generateMetricsEndpoints([]string{})

	assert.Empty(t, endpoints)
}

func TestValidateFlags_Valid(t *testing.T) {
	// Save original values
	origInterval := metricsPollInterval
	origPorts := pollMetricsPorts
	origContainers := containerNames
	origMemPercent := maxMetricsMemoryUsagePercent
	defer func() {
		metricsPollInterval = origInterval
		pollMetricsPorts = origPorts
		containerNames = origContainers
		maxMetricsMemoryUsagePercent = origMemPercent
	}()

	metricsPollInterval = defaultMetricsPollInterval
	pollMetricsPorts = []string{"2121"}
	containerNames = []string{"banyandb"}
	maxMetricsMemoryUsagePercent = 10

	err := validateFlags()
	assert.NoError(t, err)
}

func TestValidateFlags_ZeroInterval(t *testing.T) {
	origInterval := metricsPollInterval
	defer func() { metricsPollInterval = origInterval }()

	metricsPollInterval = 0

	err := validateFlags()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "poll-metrics-interval must be greater than 0")
}

func TestValidateFlags_EmptyPorts(t *testing.T) {
	origInterval := metricsPollInterval
	origPorts := pollMetricsPorts
	defer func() {
		metricsPollInterval = origInterval
		pollMetricsPorts = origPorts
	}()

	metricsPollInterval = defaultMetricsPollInterval
	pollMetricsPorts = []string{}

	err := validateFlags()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "poll-metrics-ports cannot be empty")
}

func TestValidateFlags_ContainerCountMismatch(t *testing.T) {
	origInterval := metricsPollInterval
	origPorts := pollMetricsPorts
	origContainers := containerNames
	defer func() {
		metricsPollInterval = origInterval
		pollMetricsPorts = origPorts
		containerNames = origContainers
	}()

	metricsPollInterval = defaultMetricsPollInterval
	pollMetricsPorts = []string{"2121", "2122"}
	containerNames = []string{"banyandb"}

	err := validateFlags()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "container-names count")
}

func TestValidateFlags_InvalidMemoryPercent(t *testing.T) {
	origInterval := metricsPollInterval
	origPorts := pollMetricsPorts
	origContainers := containerNames
	origMemPercent := maxMetricsMemoryUsagePercent
	defer func() {
		metricsPollInterval = origInterval
		pollMetricsPorts = origPorts
		containerNames = origContainers
		maxMetricsMemoryUsagePercent = origMemPercent
	}()

	metricsPollInterval = defaultMetricsPollInterval
	pollMetricsPorts = []string{"2121"}
	containerNames = []string{}
	maxMetricsMemoryUsagePercent = 150

	err := validateFlags()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max-metrics-memory-usage-percentage must be between 0 and 100")
}

func TestValidateFlags_NegativeMemoryPercent(t *testing.T) {
	origInterval := metricsPollInterval
	origPorts := pollMetricsPorts
	origContainers := containerNames
	origMemPercent := maxMetricsMemoryUsagePercent
	defer func() {
		metricsPollInterval = origInterval
		pollMetricsPorts = origPorts
		containerNames = origContainers
		maxMetricsMemoryUsagePercent = origMemPercent
	}()

	metricsPollInterval = defaultMetricsPollInterval
	pollMetricsPorts = []string{"2121"}
	containerNames = []string{}
	maxMetricsMemoryUsagePercent = -10

	err := validateFlags()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max-metrics-memory-usage-percentage must be between 0 and 100")
}
