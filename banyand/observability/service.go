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
	"net/http"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	flagNativeMode    = "native"
	flagPromethusMode = "prometheus"
)

var (
	_          run.Service = (*metricService)(nil)
	_          run.Config  = (*metricService)(nil)
	metricsMux             = http.NewServeMux()
	// MetricsServerInterceptor is the function to obtain grpc metrics interceptor.
	MetricsServerInterceptor func() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) = emptyMetricsServerInterceptor
)

// Service type for Metric Service.
type Service interface {
	run.PreRunner
	run.Service
}

// NewMetricService returns a metric service.
func NewMetricService(metadata metadata.Repo) Service {
	return &metricService{
		closer:   run.NewCloser(1),
		metadata: metadata,
	}
}

type metricService struct {
	l          *logger.Logger
	svr        *http.Server
	closer     *run.Closer
	scheduler  *timestamp.Scheduler
	metadata   metadata.Repo
	listenAddr string
	modes      []string
	mutex      sync.Mutex
}

func (p *metricService) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("observability")
	flagSet.StringVar(&p.listenAddr, "observability-listener-addr", ":2121", "listen addr for observability")
	flagSet.StringArrayVar(&p.modes, "observability-modes", []string{"prometheus"}, "modes for observability")
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
	p.mutex.Lock()
	defer p.mutex.Unlock()
	var providers []meter.Provider
	for _, mode := range p.modes {
		switch mode {
		case flagPromethusMode:
			MetricsServerInterceptor = promMetricsServerInterceptor
			providers = append(providers, newPromMeterProvider(SystemScope))
		case flagNativeMode:
			err := createNativeObservabilityGroup(ctx, p.metadata)
			if err != nil {
				p.l.Warn().Err(err).Msg("Failed to create native observability group")
			}
			providers = append(providers, newNativeMeterProvider(SystemScope))
		}
	}
	initMetrics(providers)
	return nil
}

func (p *metricService) Name() string {
	return "metric-service"
}

func (p *metricService) Serve() run.StopNotify {
	p.l = logger.GetLogger(p.Name())

	p.mutex.Lock()
	defer p.mutex.Unlock()
	clock, _ := timestamp.GetClock(context.TODO())
	p.scheduler = timestamp.NewScheduler(p.l, clock)
	err := p.scheduler.Register("metrics-collector", cron.Descriptor, "@every 15s", func(_ time.Time, _ *logger.Logger) bool {
		MetricsCollector.collect()
		return true
	})
	if err != nil {
		p.l.Fatal().Err(err).Msg("Failed to register metrics collector")
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
