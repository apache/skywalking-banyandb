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

// Package http implements the gRPC gateway.
package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/healthcheck"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

var (
	_ run.Config  = (*server)(nil)
	_ run.Service = (*server)(nil)

	errServerCert = errors.New("http: invalid server cert file")
	errServerKey  = errors.New("http: invalid server key file")
	errNoAddr     = errors.New("http: no address")
)

// NewServer return a http service.
func NewServer() Server {
	return &server{
		stopCh: make(chan struct{}),
	}
}

// Server is the http service.
type Server interface {
	run.Unit
	GetPort() *uint32
}

type server struct {
	creds        credentials.TransportCredentials
	tlsReloader  *pkgtls.Reloader
	l            *logger.Logger
	clientCloser context.CancelFunc
	mux          *chi.Mux
	srv          *http.Server
	stopCh       chan struct{}
	host         string
	listenAddr   string
	grpcAddr     string
	keyFile      string
	certFile     string
	grpcCert     string
	port         uint32
	tls          bool
}

func (p *server) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("http")
	flagSet.StringVar(&p.host, "http-host", "", "listen host for http")
	flagSet.Uint32Var(&p.port, "http-port", 17913, "listen port for http")
	flagSet.StringVar(&p.grpcAddr, "http-grpc-addr", "localhost:17912", "http server redirect grpc requests to this address")
	flagSet.StringVar(&p.certFile, "http-cert-file", "", "the TLS cert file of http server")
	flagSet.StringVar(&p.keyFile, "http-key-file", "", "the TLS key file of http server")
	flagSet.StringVar(&p.grpcCert, "http-grpc-cert-file", "", "the grpc TLS cert file if grpc server enables tls")
	flagSet.BoolVar(&p.tls, "http-tls", false, "connection uses TLS if true, else plain HTTP")
	return flagSet
}

func (p *server) Validate() error {
	p.listenAddr = net.JoinHostPort(p.host, strconv.FormatUint(uint64(p.port), 10))
	if p.listenAddr == ":" {
		return errNoAddr
	}
	if p.grpcCert != "" {
		creds, errTLS := credentials.NewClientTLSFromFile(p.grpcCert, "")
		if errTLS != nil {
			return errors.Wrap(errTLS, "failed to load the grpc cert")
		}
		p.creds = creds
	}
	if !p.tls {
		return nil
	}
	if p.certFile == "" {
		return errServerCert
	}
	if p.keyFile == "" {
		return errServerKey
	}
	return nil
}

func (p *server) Name() string {
	return "liaison-http"
}

func (p *server) Role() databasev1.Role {
	return databasev1.Role_ROLE_LIAISON
}

func (p *server) GetPort() *uint32 {
	return &p.port
}

func (p *server) PreRun(_ context.Context) error {
	p.l = logger.GetLogger(p.Name())
	p.l.Info().Str("level", p.l.GetLevel().String()).Msg("Logger initialized")

	// Log flag values after parsing
	p.l.Debug().Bool("tls", p.tls).Str("certFile", p.certFile).Str("keyFile", p.keyFile).Msg("Flag values after parsing")

	// Initialize TLSReloader if TLS is enabled
	p.l.Debug().Bool("tls", p.tls).Msg("HTTP TLS flag is set")
	if p.tls {
		p.l.Debug().Str("certFile", p.certFile).Str("keyFile", p.keyFile).Msg("Initializing TLSReloader for HTTP")
		var err error
		p.tlsReloader, err = pkgtls.NewReloader(p.certFile, p.keyFile, p.l)
		if err != nil {
			p.l.Error().Err(err).Msg("Failed to initialize TLSReloader for HTTP")
			return err
		}
	} else {
		p.l.Warn().Msg("HTTP TLS is disabled, skipping TLSReloader initialization")
	}

	p.mux = chi.NewRouter()

	if err := p.setRootPath(); err != nil {
		return err
	}

	// Configure the HTTP server with dynamic TLS if enabled
	p.srv = &http.Server{
		Addr:              p.listenAddr,
		Handler:           p.mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	if p.tls {
		p.srv.TLSConfig = p.tlsReloader.GetTLSConfig()
	}

	return nil
}

func (p *server) Serve() run.StopNotify {
	var ctx context.Context
	ctx, p.clientCloser = context.WithCancel(context.Background())
	opts := make([]grpc.DialOption, 0, 1)
	if p.creds == nil {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(p.creds))
	}
	client, err := healthcheck.NewClient(ctx, p.l, p.grpcAddr, opts)
	if err != nil {
		p.l.Error().Err(err).Msg("Failed to health check client")
		close(p.stopCh)
		return p.stopCh
	}
	gwMux := runtime.NewServeMux(runtime.WithHealthzEndpoint(client))
	err = multierr.Combine(
		commonv1.RegisterServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterStreamRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterMeasureRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterIndexRuleRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterIndexRuleBindingRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterGroupRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterTopNAggregationRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterSnapshotServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		databasev1.RegisterPropertyRegistryServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		streamv1.RegisterStreamServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		measurev1.RegisterMeasureServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
		propertyv1.RegisterPropertyServiceHandlerFromEndpoint(ctx, gwMux, p.grpcAddr, opts),
	)
	if err != nil {
		p.l.Error().Err(err).Msg("Failed to register endpoints")
		close(p.stopCh)
		return p.stopCh
	}
	p.mux.Mount("/api", http.StripPrefix("/api", gwMux))
	go func() {
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start liaison http server")
		var err error
		if p.tls {
			// Start the TLSReloader file monitoring
			if startErr := p.tlsReloader.Start(); startErr != nil {
				p.l.Error().Err(startErr).Msg("Failed to start TLSReloader for HTTP")
				close(p.stopCh)
				return
			}
			// Use TLS with dynamic certificate loading
			err = p.srv.ListenAndServeTLS("", "") // Empty strings because TLSConfig is set
		} else {
			err = p.srv.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			p.l.Error().Err(err)
		}

		close(p.stopCh)
	}()
	return p.stopCh
}

func (p *server) GracefulStop() {
	if p.tlsReloader != nil {
		p.tlsReloader.Stop()
	}
	if err := p.srv.Close(); err != nil {
		p.l.Error().Err(err)
	}
	p.clientCloser()
}

func intercept404(handler, on404 http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hookedWriter := &hookedResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(hookedWriter, r)

		if hookedWriter.got404 {
			on404.ServeHTTP(w, r)
		}
	}
}

type hookedResponseWriter struct {
	http.ResponseWriter
	got404 bool
}

func (hrw *hookedResponseWriter) WriteHeader(status int) {
	if status == http.StatusNotFound {
		hrw.got404 = true
	} else {
		hrw.ResponseWriter.WriteHeader(status)
	}
}

func (hrw *hookedResponseWriter) Write(p []byte) (int, error) {
	if hrw.got404 {
		return len(p), nil
	}

	return hrw.ResponseWriter.Write(p)
}

func serveFileContents(file string, files http.FileSystem) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept"), "text/html") {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "404 not found")

			return
		}
		index, err := files.Open(file)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}
		fi, err := index.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, fi.Name(), fi.ModTime(), index)
	}
}
