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

package observability

import (
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ run.Service = (*pprofService)(nil)
	_ run.Config  = (*pprofService)(nil)
)

// NewProfService returns a pprof service.
func NewProfService() run.Service {
	return &pprofService{
		closer: run.NewCloser(1),
	}
}

type pprofService struct {
	l          *logger.Logger
	svr        *http.Server
	closer     *run.Closer
	listenAddr string
	svrMux     sync.Mutex
}

func (p *pprofService) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("prof")
	flagSet.StringVar(&p.listenAddr, "pprof-listener-addr", ":6060", "listen addr for pprof")
	return flagSet
}

func (p *pprofService) Validate() error {
	if p.listenAddr == "" {
		return errNoAddr
	}
	return nil
}

func (p *pprofService) Name() string {
	return "pprof-service"
}

func (p *pprofService) Serve() run.StopNotify {
	p.l = logger.GetLogger(p.Name())
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	p.svrMux.Lock()
	defer p.svrMux.Unlock()
	p.svr = &http.Server{
		Addr:              p.listenAddr,
		ReadHeaderTimeout: 3 * time.Second,
		Handler:           mux,
	}
	go func() {
		defer p.closer.Done()
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start pprof server")
		_ = p.svr.ListenAndServe()
	}()
	return p.closer.CloseNotify()
}

func (p *pprofService) GracefulStop() {
	p.svrMux.Lock()
	defer p.svrMux.Unlock()
	if p.svr != nil {
		_ = p.svr.Close()
	}
	p.closer.CloseThenWait()
}
