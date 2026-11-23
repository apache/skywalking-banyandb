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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
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

func TestValidateFlags_PressureTriggerPercent(t *testing.T) {
	origInterval := metricsPollInterval
	origPorts := pollMetricsPorts
	origContainers := containerNames
	origMemPercent := maxMetricsMemoryUsagePercent
	origEnabled := pressureProfilerEnabled
	origTrigger := pressureTriggerPercent
	defer func() {
		metricsPollInterval = origInterval
		pollMetricsPorts = origPorts
		containerNames = origContainers
		maxMetricsMemoryUsagePercent = origMemPercent
		pressureProfilerEnabled = origEnabled
		pressureTriggerPercent = origTrigger
	}()

	metricsPollInterval = defaultMetricsPollInterval
	pollMetricsPorts = []string{"2121"}
	containerNames = []string{"banyandb"}
	maxMetricsMemoryUsagePercent = 10
	pressureProfilerEnabled = true

	for _, invalid := range []int{0, -1, 101} {
		pressureTriggerPercent = invalid
		err := validateFlags()
		assert.Error(t, err, "trigger=%d", invalid)
		assert.Contains(t, err.Error(), "pressure-profiler-trigger-percent must be between 1 and 100")
	}

	// A valid value passes.
	pressureTriggerPercent = 75
	assert.NoError(t, validateFlags())

	// When the profiler is disabled, an out-of-range value is tolerated.
	pressureProfilerEnabled = false
	pressureTriggerPercent = 0
	assert.NoError(t, validateFlags())
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

func TestNativeLogRouteDefaultsToCluster(t *testing.T) {
	route := rootCmd.Flags().Lookup("logging-native-route")
	if assert.NotNil(t, route) {
		assert.Equal(t, nativeLogRouteCluster, route.DefValue)
	}
	setFlag(t, "pod-name", "data-0")
	cluster, err := nativeLogConfig(nativeLogRouteCluster)
	assert.NoError(t, err)
	assert.False(t, cluster.Destination.Public)
	// The agent stores its logs as the pod it watches, not as its own host.
	assert.Equal(t, "data-0", cluster.NodeName)
	assert.Equal(t, "fodc-agent", cluster.NodeType)
	public, err := nativeLogConfig(nativeLogRoutePublic)
	assert.NoError(t, err)
	assert.True(t, public.Destination.Public)
	// A role name is not a route: the agent beside a liaison would otherwise
	// fall back to the cluster route, which the liaison refuses.
	_, err = nativeLogConfig("liaison")
	assert.ErrorContains(t, err, "logging-native-route must be cluster or public")
}

// setFlag sets a flag of the agent for the test, and its default again after.
func setFlag(t *testing.T, name, value string) {
	t.Helper()
	flag := rootCmd.Flags().Lookup(name)
	require.NotNil(t, flag, name)
	require.NoError(t, flag.Value.Set(value))
	t.Cleanup(func() { _ = flag.Value.Set(flag.DefValue) })
}

func nativeLogGauges(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	n := 0
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "banyandb_logging_native_log_") {
			n++
		}
	}
	return n
}

func TestStartLoggingWithNativeLogsOff(t *testing.T) {
	reg := prometheus.NewRegistry()
	stop, err := startLogging(rootCmd, reg)
	require.NoError(t, err)
	stop()
	assert.Zero(t, nativeLogGauges(t, reg), "the agent published native log counters with native logging off")
}

func TestStartLoggingRefusesAnUnknownRoute(t *testing.T) {
	setFlag(t, "logging-native-route", "liaison")
	_, err := startLogging(rootCmd, prometheus.NewRegistry())
	assert.ErrorContains(t, err, "logging-native-route must be cluster or public")
}

func TestStartLoggingNeedsAnAddressForNativeLogs(t *testing.T) {
	setFlag(t, "logging-native-enabled", "true")
	_, err := startLogging(rootCmd, prometheus.NewRegistry())
	assert.ErrorContains(t, err, "native logging")
}

// TestStartLoggingPublishesTheCountersInTheAgentRegistry checks that the
// agent's producer runs and reports through the registry the agent serves on
// /metrics. Nothing listens at the address, so its events are dropped, not
// stored; that is the producer's concern, tested in its own package.
func TestStartLoggingPublishesTheCountersInTheAgentRegistry(t *testing.T) {
	setFlag(t, "logging-native-enabled", "true")
	setFlag(t, "logging-native-addr", "127.0.0.1:1")
	t.Cleanup(func() { _ = logger.Init(logger.Logging{Env: "dev", Level: "info"}) })
	reg := prometheus.NewRegistry()
	stop, err := startLogging(rootCmd, reg)
	require.NoError(t, err)
	defer stop()
	assert.Eventually(t, func() bool { return nativeLogGauges(t, reg) > 0 }, 10*time.Second, 50*time.Millisecond,
		"the native log counters are not in the agent's registry")
}
