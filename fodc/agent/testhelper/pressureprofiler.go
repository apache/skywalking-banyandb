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

package testhelper

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/pressureprofiler"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/proxy"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/watchdog"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

// PressureAgentConfig configures a real agent stack wired for memory-pressure pprof capture.
type PressureAgentConfig struct {
	Logger         *logger.Logger
	ProxyAddr      string
	PodName        string
	Role           string
	MetricsURL     string
	PprofPort      string
	Dir            string
	Cooldown       time.Duration
	TriggerPercent int
}

// PressureAgent bundles a real watchdog + flight recorder + pressure profiler + proxy client,
// wired exactly as the agent does in production, for end-to-end integration tests. Only the
// monitored BanyanDB node is external (addressed by MetricsURL / PprofPort).
type PressureAgent struct {
	client *proxy.Client
	wd     *watchdog.Watchdog
}

// NewPressureAgent builds and starts the agent stack: the watchdog polls cfg.MetricsURL into a
// flight recorder, the profiler evaluates on each poll and captures from cfg.PprofPort, and the
// proxy client exposes the captured profiles to the proxy. The caller drives the connection with
// Connect/StartRegistrationStream/StartPressureProfilesStream and tears it down with Stop.
func NewPressureAgent(ctx context.Context, cfg PressureAgentConfig) (*PressureAgent, error) {
	fr := flightrecorder.NewFlightRecorder(10 * 1024 * 1024)
	wd := watchdog.NewWatchdogWithConfig(fr, []string{cfg.MetricsURL}, 200*time.Millisecond,
		cfg.Role, cfg.PodName, []string{cfg.Role})
	provider := prom.NewProvider(meter.NewHierarchicalScope("fodc_agent", "_"), prometheus.NewRegistry())
	pp, err := pressureprofiler.New(pressureprofiler.Config{
		Dir:            cfg.Dir,
		PprofPort:      cfg.PprofPort,
		PodName:        cfg.PodName,
		RoleProvider:   func() string { return cfg.Role },
		Cooldown:       cfg.Cooldown,
		TriggerPercent: cfg.TriggerPercent,
		MaxArtifacts:   16,
	}, fr, provider, cfg.Logger)
	if err != nil {
		return nil, err
	}
	wd.AddPostPollHook(pp.OnPollComplete)
	if preRunErr := wd.PreRun(ctx); preRunErr != nil {
		return nil, preRunErr
	}
	wd.Serve()

	client := proxy.NewClient(cfg.ProxyAddr, cfg.Role, cfg.PodName, []string{cfg.Role},
		map[string]string{"role": cfg.Role}, 2*time.Second, 1*time.Second, fr, nil, nil, nil, pp, cfg.Logger)
	return &PressureAgent{client: client, wd: wd}, nil
}

// Connect starts the connection manager and connects the proxy client.
func (a *PressureAgent) Connect(ctx context.Context) error {
	a.client.StartConnManager(ctx)
	return a.client.Connect(ctx)
}

// StartRegistrationStream starts the agent registration stream.
func (a *PressureAgent) StartRegistrationStream(ctx context.Context) error {
	return a.client.StartRegistrationStream(ctx)
}

// StartPressureProfilesStream starts the pressure profiles stream.
func (a *PressureAgent) StartPressureProfilesStream(ctx context.Context) error {
	return a.client.StartPressureProfilesStream(ctx)
}

// Stop tears the agent down. Disconnect runs first so the proxy client is marked disconnected
// before its streams error out, preventing a spurious reconnect goroutine (which would race the
// suite's goroutine-leak check); then the watchdog is stopped.
func (a *PressureAgent) Stop() {
	_ = a.client.Disconnect()
	a.wd.GracefulStop()
}
