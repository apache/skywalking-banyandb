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

package prof

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ run.Service = (*pprofService)(nil)
	_ run.Config  = (*pprofService)(nil)
)

func NewProfService() run.Service {
	return &pprofService{
		stopCh: make(chan struct{}),
	}
}

type pprofService struct {
	listenAddr string
	stopCh     chan struct{}
	l          *logger.Logger
}

func (p *pprofService) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("prof")
	flagSet.StringVar(&p.listenAddr, "pprof-listener-addr", "0.0.0.0:6060", "listen addr for pprof")
	return flagSet
}

func (p *pprofService) Validate() error {
	return nil
}

func (p *pprofService) Name() string {
	return "pprof-service"
}

func (p *pprofService) Serve() error {
	p.l = logger.GetLogger(p.Name())
	go func() {
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start pprof server")
		_ = http.ListenAndServe(p.listenAddr, nil)
	}()

	<-p.stopCh

	return nil
}

func (p *pprofService) GracefulStop() {
	close(p.stopCh)
}
