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
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ run.Service = (*metricService)(nil)
	_ run.Config  = (*metricService)(nil)
)

// NewMetricService returns a metric service.
func NewMetricService() run.Service {
	return &metricService{
		closer: run.NewCloser(1),
	}
}

type metricService struct {
	l          *logger.Logger
	svr        *http.Server
	closer     *run.Closer
	listenAddr string
}

func (p *metricService) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("observability")
	flagSet.StringVar(&p.listenAddr, "observability-listener-addr", ":2121", "listen addr for observability")
	return flagSet
}

func (p *metricService) Validate() error {
	if p.listenAddr == "" {
		return errNoAddr
	}
	return nil
}

func (p *metricService) Name() string {
	return "metric-service"
}

func (p *metricService) Serve() run.StopNotify {
	p.l = logger.GetLogger(p.Name())
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	p.svr = &http.Server{
		Addr:              p.listenAddr,
		ReadHeaderTimeout: 3 * time.Second,
		Handler:           mux,
	}
	go func() {
		defer p.closer.Done()
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start metric server")
		_ = p.svr.ListenAndServe()
	}()
	return p.closer.CloseNotify()
}

func (p *metricService) GracefulStop() {
	_ = p.svr.Close()
	p.closer.CloseThenWait()
}
