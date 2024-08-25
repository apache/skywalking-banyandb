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

package observability

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/robfig/cron/v3"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/native"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	flagNativeMode    = "native"
	flagPromethusMode = "prometheus"
)

var (
	_ run.Service = (*metricService)(nil)
	_ run.Config  = (*metricService)(nil)

	obScope = RootScope.SubScope("observability")
)

// Service type for Metric Service.
type Service interface {
	run.PreRunner
	run.Service
}

// NewMetricService returns a metric service.
func NewMetricService(metadata metadata.Repo, pipeline queue.Client, nodeType string, nodeSelector native.NodeSelector) MetricsRegistry {
	return &metricService{
		closer:       run.NewCloser(1),
		metadata:     metadata,
		pipeline:     pipeline,
		nodeType:     nodeType,
		nodeSelector: nodeSelector,
	}
}

type metricService struct {
	metadata         metadata.Repo
	nodeSelector     native.NodeSelector
	pipeline         queue.Client
	svr              *http.Server
	l                *logger.Logger
	closer           *run.Closer
	scheduler        *timestamp.Scheduler
	nCollection      *native.MetricCollection
	promReg          *prometheus.Registry
	schedulerMetrics *SchedulerMetrics
	npf              nativeProviderFactory
	listenAddr       string
	nodeType         string
	modes            []string
	mutex            sync.Mutex
}

func (p *metricService) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("observability")
	flagSet.StringVar(&p.listenAddr, "observability-listener-addr", ":2121", "listen addr for observability")
	flagSet.StringSliceVar(&p.modes, "observability-modes", []string{"prometheus"}, "modes for observability")
	return flagSet
}

func (p *metricService) Validate() error {
	if p.listenAddr == "" {
		return errNoAddr
	}
	set := make(map[string]struct{})
	for _, mode := range p.modes {
		if mode != flagNativeMode && mode != flagPromethusMode {
			return errInvalidMode
		}
		if _, exists := set[mode]; exists {
			return errDuplicatedMode
		}
		set[mode] = struct{}{}
	}
	return nil
}

func (p *metricService) PreRun(ctx context.Context) error {
	p.l = logger.GetLogger(p.Name())
	if containsMode(p.modes, flagPromethusMode) {
		p.promReg = prometheus.NewRegistry()
		p.promReg.MustRegister(collectors.NewGoCollector())
		p.promReg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	}
	if containsMode(p.modes, flagNativeMode) {
		p.nCollection = native.NewMetricsCollection(p.pipeline, p.nodeSelector)
		val := ctx.Value(common.ContextNodeKey)
		if val == nil {
			return errors.New("metric service native mode, node id is empty")
		}
		node := val.(common.Node)
		nodeInfo := native.NodeInfo{
			Type:        p.nodeType,
			NodeID:      node.NodeID,
			GrpcAddress: node.GrpcAddress,
			HTTPAddress: node.HTTPAddress,
		}
		p.npf = nativeProviderFactory{
			metadata: p.metadata,
			nodeInfo: nodeInfo,
		}
	}
	return nil
}

func (p *metricService) Name() string {
	return "metric-service"
}

func (p *metricService) Serve() run.StopNotify {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.initMetrics()
	clock, _ := timestamp.GetClock(context.TODO())
	p.scheduler = timestamp.NewScheduler(p.l, clock)
	p.schedulerMetrics = NewSchedulerMetrics(p.With(obScope))
	err := p.scheduler.Register("metrics-collector", cron.Descriptor, "@every 15s", func(_ time.Time, _ *logger.Logger) bool {
		MetricsCollector.collect()
		metrics := p.scheduler.Metrics()
		for job, m := range metrics {
			p.schedulerMetrics.Collect(job, m)
		}
		return true
	})
	if err != nil {
		p.l.Fatal().Err(err).Msg("Failed to register metrics collector")
	}
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/_route", p.routeTableHandler)
	if containsMode(p.modes, flagPromethusMode) {
		registerMetricsEndpoint(p.promReg, metricsMux)
	}
	if containsMode(p.modes, flagNativeMode) {
		err = p.scheduler.Register("native-metric-collection", cron.Descriptor, "@every 5s", func(_ time.Time, _ *logger.Logger) bool {
			p.nCollection.FlushMetrics()
			return true
		})
		if err != nil {
			p.l.Fatal().Err(err).Msg("Failed to register native metric collection")
		}
	}
	p.svr = &http.Server{
		Addr:              p.listenAddr,
		ReadHeaderTimeout: 3 * time.Second,
		Handler:           metricsMux,
	}
	go func() {
		defer p.closer.Done()
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start metric server")
		_ = p.svr.ListenAndServe()
	}()
	return p.closer.CloseNotify()
}

func (p *metricService) GracefulStop() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.scheduler != nil {
		p.scheduler.Close()
	}
	if p.svr != nil {
		_ = p.svr.Close()
	}
	p.closer.CloseThenWait()
}

func (p *metricService) NativeEnabled() bool {
	return containsMode(p.modes, flagNativeMode)
}

func (p *metricService) routeTableHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(p.nodeSelector.String()))
}

func containsMode(modes []string, mode string) bool {
	for _, item := range modes {
		if item == mode {
			return true
		}
	}
	return false
}

// SchedulerMetrics is the metrics for scheduler.
type SchedulerMetrics struct {
	totalJobsStarted   meter.Gauge
	totalJobsFinished  meter.Gauge
	totalTasksStarted  meter.Gauge
	totalTasksFinished meter.Gauge
	totalTasksPanic    meter.Gauge
	totalTaskLatency   meter.Gauge
}

// NewSchedulerMetrics creates a new scheduler metrics.
func NewSchedulerMetrics(factory *Factory) *SchedulerMetrics {
	return &SchedulerMetrics{
		totalJobsStarted:   factory.NewGauge("scheduler_jobs_started", "job"),
		totalJobsFinished:  factory.NewGauge("scheduler_jobs_finished", "job"),
		totalTasksStarted:  factory.NewGauge("scheduler_tasks_started", "job"),
		totalTasksFinished: factory.NewGauge("scheduler_tasks_finished", "job"),
		totalTasksPanic:    factory.NewGauge("scheduler_tasks_panic", "job"),
		totalTaskLatency:   factory.NewGauge("scheduler_task_latency", "job"),
	}
}

// Collect collects the scheduler metrics.
func (sm *SchedulerMetrics) Collect(job string, m *timestamp.SchedulerMetrics) {
	sm.totalJobsStarted.Set(float64(m.TotalJobsStarted.Load()), job)
	sm.totalJobsFinished.Set(float64(m.TotalJobsFinished.Load()), job)
	sm.totalTasksStarted.Set(float64(m.TotalTasksStarted.Load()), job)
	sm.totalTasksFinished.Set(float64(m.TotalTasksFinished.Load()), job)
	sm.totalTasksPanic.Set(float64(m.TotalTasksPanic.Load()), job)
	sm.totalTaskLatency.Set(float64(m.TotalTaskLatencyInNanoseconds.Load())/float64(time.Second), job)
}
