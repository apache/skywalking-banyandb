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
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
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
func NewServer(cfg *auth.Config) Server {
	return &server{
		stopCh: make(chan struct{}),
		cfg:    cfg,
	}
}

// Server is the http service.
type Server interface {
	run.Unit
	GetPort() *uint32
}

type server struct {
	creds           credentials.TransportCredentials
	grpcCtx         context.Context
	srv             *http.Server
	grpcCancel      context.CancelFunc
	handlerWrapper  *atomicHandler
	grpcTLSReloader *pkgtls.Reloader
	stopCh          chan struct{}
	gwMux           *runtime.ServeMux
	grpcClient      atomic.Pointer[healthcheck.Client]
	l               *logger.Logger
	tlsReloader     *pkgtls.Reloader
	host            string
	listenAddr      string
	grpcAddr        string
	keyFile         string
	certFile        string
	cfg             *auth.Config
	grpcCert        string
	grpcMu          sync.Mutex
	port            uint32
	tls             bool
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

	// Initialize gRPC client with cert file
	if p.grpcCert != "" {
		p.l.Debug().Str("grpcCert", p.grpcCert).Msg("Initializing TLS credentials for gRPC connection")

		// Create a client cert reloader that only watches the cert file
		var err error
		p.grpcTLSReloader, err = pkgtls.NewClientCertReloader(p.grpcCert, p.l)
		if err != nil {
			p.l.Error().Err(err).Msg("Failed to initialize gRPC TLS reloader")
			return err
		}

		// Start the reloader
		if err = p.grpcTLSReloader.Start(); err != nil {
			p.l.Error().Err(err).Msg("Failed to start gRPC TLS reloader")
			return err
		}

		// Get the update channel from the reloader
		certUpdateCh := p.grpcTLSReloader.GetUpdateChannel()

		p.l.Info().Msg("Starting certificate update notification listener")

		// Start a goroutine to watch for certificate update events
		go func() {
			p.l.Info().Msg("Certificate update notification goroutine started")

			for {
				select {
				case <-certUpdateCh:
					// Certificate was updated, let's debounce to handle potential multiple notifications
					p.l.Info().Msg("Received certificate update notification")

					// Debounce multiple notifications that might come in rapid succession
					func() {
						p.l.Info().Msg("Processing certificate update after debounce")

						// Safely handle the context and cancel function
						p.grpcMu.Lock()
						prevGRPCCancel := p.grpcCancel

						// Create a new context for the new connections
						p.grpcCtx, p.grpcCancel = context.WithCancel(context.Background())
						p.grpcMu.Unlock()

						// Cancel existing gRPC connections
						if prevGRPCCancel != nil {
							p.l.Info().Msg("Canceling existing gRPC connections")
							prevGRPCCancel()
						}

						// Force a short delay to ensure all resources are properly cleaned up
						time.Sleep(200 * time.Millisecond)

						// Re-create the gateway with updated credentials
						if p.gwMux != nil {
							p.l.Info().Msg("Re-creating gateway with updated credentials")
						}

						// Reinitialize the gRPC client (which will get fresh credentials from the reloader)
						if err := p.initGRPCClient(); err != nil {
							p.l.Error().Err(err).Msg("Failed to reinitialize gRPC client after credential update")
						} else {
							p.l.Info().Msg("Successfully reinitialized gRPC client with new credentials")
						}
					}()

				case <-p.stopCh:
					p.l.Info().Msg("Stopping certificate update notification listener")
					return
				}
			}
		}()
	}

	p.handlerWrapper = &atomicHandler{}

	// Configure the HTTP server with dynamic TLS if enabled
	p.srv = &http.Server{
		Addr:              p.listenAddr,
		Handler:           p.handlerWrapper,
		ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if p.tls {
		p.srv.TLSConfig = p.tlsReloader.GetTLSConfig()
	}

	return nil
}

func (p *server) Serve() run.StopNotify {
	p.grpcMu.Lock()
	p.grpcCtx, p.grpcCancel = context.WithCancel(context.Background())
	p.grpcMu.Unlock()

	// Start TLS reloader for HTTP server
	if p.tls && p.tlsReloader != nil {
		if err := p.tlsReloader.Start(); err != nil {
			p.l.Error().Err(err).Msg("Failed to start TLSReloader for HTTP")
			close(p.stopCh)
			return p.stopCh
		}
	}

	// Initialize gRPC client and gateway mux
	if err := p.initGRPCClient(); err != nil {
		p.l.Error().Err(err).Msg("Failed to initialize gRPC client")
		close(p.stopCh)
		return p.stopCh
	}

	go func() {
		defer close(p.stopCh)
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start liaison http server")
		var err error
		if p.tls {
			// Start the TLSReloader file monitoring already done above
			// Use TLS with dynamic certificate loading
			err = p.srv.ListenAndServeTLS("", "") // Empty strings because TLSConfig is set
		} else {
			err = p.srv.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			p.l.Error().Err(err).Msg("HTTP server failed")
		}
	}()
	return p.stopCh
}

// initGRPCClient initializes or reinitializes the gRPC client with current credentials.
func (p *server) initGRPCClient() error {
	// Clean up any existing client first
	if client := p.grpcClient.Load(); client != nil {
		p.l.Debug().Msg("Cleaning up existing gRPC client")
	}

	// Simplify the options slice initialization
	var opts []grpc.DialOption

	// Use switch statement instead of if-else chain
	switch {
	case p.grpcTLSReloader != nil:
		// Extract hostname from grpcAddr
		host, _, err := net.SplitHostPort(p.grpcAddr)
		if err != nil {
			p.l.Error().Err(err).Msg("Failed to split gRPC address")
			return errors.Wrap(err, "failed to split gRPC address")
		}
		if host == "" || host == "0.0.0.0" || host == "[::]" {
			host = "localhost"
		}

		// Get fresh TLS config from the reloader
		tlsConfig, err := p.grpcTLSReloader.GetClientTLSConfig(host)
		if err != nil {
			p.l.Error().Err(err).Msg("Failed to get TLS config from reloader")
			return errors.Wrap(err, "failed to get TLS config from reloader")
		}

		// Create new credentials from the TLS config
		p.creds = credentials.NewTLS(tlsConfig)
		p.l.Debug().Msg("Created fresh gRPC credentials from reloader")
		opts = append(opts, grpc.WithTransportCredentials(p.creds))
	case p.creds != nil:
		opts = append(opts, grpc.WithTransportCredentials(p.creds))
	default:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Create health check client
	p.grpcMu.Lock()
	ctx := p.grpcCtx
	p.grpcMu.Unlock()

	client, err := healthcheck.NewClient(ctx, p.l, p.grpcAddr, opts)
	if err != nil {
		return errors.Wrap(err, "failed to create health check client")
	}
	p.grpcClient.Store(client)

	// Create gateway mux with health endpoint
	p.gwMux = runtime.NewServeMux()

	// Register all service handlers
	err = multierr.Combine(
		commonv1.RegisterServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterStreamRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterMeasureRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterIndexRuleRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterIndexRuleBindingRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterGroupRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterTopNAggregationRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterSnapshotServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterPropertyRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		streamv1.RegisterStreamServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		measurev1.RegisterMeasureServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		propertyv1.RegisterPropertyServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
		databasev1.RegisterTraceRegistryServiceHandlerFromEndpoint(p.grpcCtx, p.gwMux, p.grpcAddr, opts),
	)
	if err != nil {
		return errors.Wrap(err, "failed to register endpoints")
	}

	// Create a new router to replace the existing one
	// This avoids the conflict when remounting to /api path
	newMux := chi.NewRouter()

	newMux.Use(authMiddleware(p.cfg))
	newMux.Handle("/api/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := buildGRPCContextForHealthCheck(p.cfg, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		resp, err := p.grpcClient.Load().Check(ctx, &grpc_health_v1.HealthCheckRequest{}) //nolint:contextcheck
		if err != nil {
			http.Error(w, "gRPC health check failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(resp.Status.String()))
		if err != nil {
			logger.Errorf("Failed to write health check response: %v", err)
		}
	}))

	// Mount the gateway mux to the HTTP server
	newMux.Mount("/api", http.StripPrefix("/api", p.gwMux))

	// Replace the old mux with the new one
	if err := p.setRootPath(newMux); err != nil {
		return err
	}
	p.handlerWrapper.Store(newMux)

	return nil
}

func (p *server) GracefulStop() {
	if p.tlsReloader != nil {
		p.tlsReloader.Stop()
	}
	if p.grpcTLSReloader != nil {
		p.grpcTLSReloader.Stop()
	}

	p.grpcMu.Lock()
	var cancel context.CancelFunc
	if p.grpcCancel != nil {
		cancel = p.grpcCancel
	}
	p.grpcMu.Unlock()
	if cancel != nil {
		cancel()
	}

	if err := p.srv.Close(); err != nil {
		p.l.Error().Err(err)
	}
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

type atomicHandler struct {
	value atomic.Value // stores http.Handler
}

func (h *atomicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler, ok := h.value.Load().(http.Handler)
	if ok && handler != nil {
		handler.ServeHTTP(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func (h *atomicHandler) Store(handler http.Handler) {
	h.value.Store(handler)
}
