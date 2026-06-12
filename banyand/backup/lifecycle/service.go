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

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-chi/chi/v5"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/healthcheck"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

type service interface {
	run.Config
	run.PreRunner
	run.Service
}

var _ service = (*lifecycleService)(nil)

const (
	// metricsLocalNodeName is the connMgr key for the co-located data node that
	// receives the lifecycle's native _monitoring measure writes.
	metricsLocalNodeName = "lifecycle-local"
	// metricsNodeKeeperInterval is how often the keeper re-checks that the local
	// data node connection is active and re-registers it if not.
	metricsNodeKeeperInterval = 10 * time.Second
	// lifecycleRoleName is the hard-coded role label the lifecycle stamps
	// on its own _monitoring series and on the wire-level SenderRole
	// for tier-migration traffic. Mirrors the liaison's "liaison"
	// pattern at pkg/cmdsetup/liaison.go:170-171.
	lifecycleRoleName = "lifecycle"
)

type lifecycleService struct {
	databasev1.UnimplementedClusterStateServiceServer
	databasev1.UnimplementedNodeQueryServiceServer
	metadata metadata.Repo
	omr      observability.MetricsRegistry
	pm       protector.Memory
	// cycleLabels is the label set shared by the three cycle-level
	// metrics (cyclesTotal, lastRunTimestamp, lastRunSuccess): the
	// SENDER identity of the lifecycle pod (remote_node = the
	// co-located data pod's BanyanDB NodeID, remote_role = "lifecycle",
	// remote_tier = the data pod's tier label) plus the GROUP being
	// processed. The label form mirrors the parallel
	// banyandb_lifecycle_migration_* family emitted by the queue/pub
	// lifecycle publisher, but the cycle-level series describe the
	// SENDER side (one tuple per cycle, the cycle's last-seen group)
	// while the per-message pub series describe the DESTINATION side
	// (one tuple per chunk, the destination's NodeID/role/tier resolved
	// from getNodeInfo). The two families are independently useful
	// (cycle health vs. per-message traffic) and share the same label
	// form so dashboard matchers and regexes apply to both.
	cyclesTotal      meter.Counter
	lastRunTimestamp meter.Gauge
	lastRunSuccess   meter.Gauge
	// Last-seen (group, remote_node, remote_role, remote_tier) tuple
	// from the most recent parseGroup call. Used by the deferred
	// recordLastRun to stamp the cycle-level last_run_* gauges when
	// the cycle ends. Reset to empty strings at the start of each
	// action() so an empty cycle (no parseGroup succeeded) doesn't
	// inherit the previous cycle's labels.
	lastRunGroup      string
	lastRunNode       string
	lastRunRole       string
	lastRunTier       string
	metricsClient     queue.Client
	grpcServer        *grpclib.Server
	httpSrv           *http.Server
	tlsReloader       *pkgtls.Reloader
	currentNode       *databasev1.Node
	clientCloser      context.CancelFunc
	stopCh            chan struct{}
	sch               *timestamp.Scheduler
	l                 *logger.Logger
	clusterStateMgr   *clusterStateManager
	metricsKeeperStop chan struct{}
	lifecycleHost     string
	lifecycleHTTPAddr string
	streamRoot        string
	traceRoot         string
	progressFilePath  string
	reportDir         string
	schedule          string
	cert              string
	gRPCAddr          string
	lifecycleKeyFile  string
	lifecycleGRPCAddr string
	measureRoot       string
	lifecycleCertFile string
	localNodeMD       schema.Metadata
	maxExecutionTimes int
	chunkSize         run.Bytes
	lifecycleGRPCPort uint32
	lifecycleHTTPPort uint32
	enableTLS         bool
	insecure          bool
	lifecycleTLS      bool
}

// NewService creates a new lifecycle service. metricsRegistry replaces the
// previous BypassRegistry, so the protector memory metrics and the lifecycle
// proof counter emit real series. metricsClient is the native-metrics pipeline to
// the co-located data node; it is only exercised when native mode is enabled.
func NewService(
	meta metadata.Repo,
	metricsRegistry observability.MetricsRegistry,
	metricsClient queue.Client,
) run.Unit {
	ls := &lifecycleService{
		metadata:        meta,
		omr:             metricsRegistry,
		metricsClient:   metricsClient,
		clusterStateMgr: &clusterStateManager{},
	}
	ls.pm = protector.NewMemory(ls.omr)
	return ls
}

// nativeNodeContext augments ctx with the lifecycle node identity that the metric
// service's native mode stamps onto every _monitoring series. The identity is the
// lifecycle's own (Type="lifecycle", set via the metric service nodeType); NodeID
// prefers POD_NAME (downward API) and falls back to the hostname (the pod name in
// k8s) so series from different pods stay distinct. The lifecycle's own gRPC/HTTP
// addresses are not computed until Validate (later, and only when scheduled), so
// the address tags are left empty.
func nativeNodeContext(ctx context.Context) context.Context {
	nodeID := os.Getenv("POD_NAME")
	if nodeID == "" {
		if hostname, hostErr := os.Hostname(); hostErr == nil {
			nodeID = hostname
		}
	}
	if nodeID == "" {
		nodeID = lifecycleRoleName
	}
	return context.WithValue(ctx, common.ContextNodeKey, common.Node{
		NodeID: nodeID,
		Labels: common.ParseNodeFlags(),
	})
}

func (l *lifecycleService) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet(l.Name())
	flagS.StringSliceVar(&common.FlagNodeLabels, "node-labels", nil, "the node labels. e.g. key1=value1,key2=value2")
	flagS.StringVar(&l.gRPCAddr, "grpc-addr", "127.0.0.1:17912", "gRPC address of the data node")
	flagS.BoolVar(&l.enableTLS, "enable-tls", false, "Enable TLS for gRPC connection")
	flagS.BoolVar(&l.insecure, "insecure", false, "Skip server certificate verification")
	flagS.StringVar(&l.cert, "cert", "", "Path to the gRPC server certificate")
	flagS.StringVar(&l.streamRoot, "stream-root-path", "/tmp", "Root directory for stream catalog")
	flagS.StringVar(&l.traceRoot, "trace-root-path", "/tmp", "Root directory for trace catalog")
	flagS.StringVar(&l.measureRoot, "measure-root-path", "/tmp", "Root directory for measure catalog")
	flagS.StringVar(&l.progressFilePath, "progress-file", "/tmp/lifecycle-progress.json", "Path to store progress for crash recovery")
	flagS.StringVar(&l.reportDir, "report-dir", "/tmp/lifecycle-reports", "Directory to store migration reports")
	flagS.StringVar(
		&l.schedule,
		"schedule",
		"",
		"Schedule expression for periodic backup. Options: @yearly, @monthly, @weekly, @daily, @hourly or @every <duration>",
	)
	flagS.IntVar(&l.maxExecutionTimes, "max-execution-times", 0, "Maximum number of times to execute the lifecycle migration. 0 means no limit.")
	l.chunkSize = run.Bytes(1024 * 1024)
	flagS.VarP(&l.chunkSize, "chunk-size", "", "Chunk size in bytes for streaming data during migration (default: 1MB)")
	flagS.IntVar(&rowReplayMaxBatchRows, "row-replay-max-batch-rows", rowReplayMaxBatchRows,
		"Maximum rows per row-replay batch (row-replay is the fallback for parts spanning multiple target segments)")
	flagS.VarP(&rowReplayMaxBatchBytes, "row-replay-max-batch-bytes", "",
		"Maximum in-flight marshaled bytes per row-replay batch; caps peak marshal memory for large bodies (default: 32MB)")

	// Lifecycle server flags
	flagS.BoolVar(&l.lifecycleTLS, "lifecycle-tls", false, "connection uses TLS if true, else plain TCP")
	flagS.StringVar(&l.lifecycleCertFile, "lifecycle-cert-file", "", "the TLS cert file")
	flagS.StringVar(&l.lifecycleKeyFile, "lifecycle-key-file", "", "the TLS key file")
	flagS.StringVar(&l.lifecycleHost, "lifecycle-grpc-host", "", "the host of lifecycle server listens")
	flagS.Uint32Var(&l.lifecycleGRPCPort, "lifecycle-grpc-port", 17914, "the port of lifecycle server listens")
	flagS.Uint32Var(&l.lifecycleHTTPPort, "lifecycle-http-port", 17915, "the port of lifecycle http api listens")

	return flagS
}

func (l *lifecycleService) Validate() error {
	if l.schedule != "" {
		l.lifecycleGRPCAddr = net.JoinHostPort(l.lifecycleHost, strconv.FormatUint(uint64(l.lifecycleGRPCPort), 10))
		if l.lifecycleGRPCAddr == ":" {
			return errors.New("no gRPC address")
		}
		l.lifecycleHTTPAddr = net.JoinHostPort(l.lifecycleHost, strconv.FormatUint(uint64(l.lifecycleHTTPPort), 10))
		if l.lifecycleHTTPAddr == ":" {
			return errors.New("no HTTP address")
		}
		if l.lifecycleTLS {
			if l.lifecycleCertFile == "" {
				return errors.New("missing cert file when TLS is enabled")
			}
			if l.lifecycleKeyFile == "" {
				return errors.New("missing key file when TLS is enabled")
			}
		}
		l.currentNode = &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: l.lifecycleGRPCAddr,
			},
			GrpcAddress: l.lifecycleGRPCAddr,
			HttpAddress: l.lifecycleHTTPAddr,
			Roles:       make([]databasev1.Role, 0),
			Labels:      common.ParseNodeFlags(),
			CreatedAt:   timestamppb.Now(),
		}
	}
	return nil
}

// PreRun initializes the lifecycle service and its embedded server.
func (l *lifecycleService) PreRun(_ context.Context) error {
	l.l = logger.GetLogger("lifecycle")

	// Safe to call With() here: the metrics registry is registered earlier in the
	// group, so its PreRun (which builds the provider) has already run.
	lifecycleScope := l.omr.With(observability.RootScope.SubScope("lifecycle"))
	cycleLabels := []string{"remote_node", "remote_role", "remote_tier", "group"}
	l.cyclesTotal = lifecycleScope.NewCounter("cycles_total", cycleLabels...)
	l.lastRunTimestamp = lifecycleScope.NewGauge("last_run_timestamp_seconds", cycleLabels...)
	l.lastRunSuccess = lifecycleScope.NewGauge("last_run_success", cycleLabels...)

	if l.schedule != "" && l.lifecycleTLS {
		var err error
		l.tlsReloader, err = pkgtls.NewReloader(l.lifecycleCertFile, l.lifecycleKeyFile, l.l)
		if err != nil {
			return errors.Wrap(err, "failed to initialize TLS reloader")
		}
	}

	return nil
}

func (l *lifecycleService) GracefulStop() {
	if l.sch != nil {
		l.sch.Close()
	}

	// Stop the native metrics node keeper, then close the local metrics pub
	// client. The metric service may still attempt a final flush concurrently
	// during shutdown; native metrics are best-effort, so a flush that loses the
	// race to the closing client simply logs and is dropped.
	if l.metricsKeeperStop != nil {
		close(l.metricsKeeperStop)
		l.metricsKeeperStop = nil
	}
	if l.metricsClient != nil {
		l.metricsClient.GracefulStop()
	}

	l.l.Info().Msg("Stopping lifecycle server")

	if l.tlsReloader != nil {
		l.tlsReloader.Stop()
	}

	if l.clientCloser != nil {
		l.clientCloser()
	}

	// Stop HTTP server
	if l.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := l.httpSrv.Shutdown(ctx); shutdownErr != nil {
			l.l.Warn().Err(shutdownErr).Msg("HTTP server shutdown error")
		}
	}

	// Stop gRPC server
	if l.grpcServer != nil {
		stopped := make(chan struct{})
		run.Go(context.Background(), "backup.lifecycle.graceful-stop", l.l, func(_ context.Context) {
			l.grpcServer.GracefulStop()
			close(stopped)
		})

		t := time.NewTimer(10 * time.Second)
		select {
		case <-t.C:
			l.grpcServer.Stop()
			l.l.Info().Msg("Lifecycle server force stopped")
		case <-stopped:
			t.Stop()
			l.l.Info().Msg("Lifecycle server stopped gracefully")
		}
	}
}

func (l *lifecycleService) Name() string {
	return "lifecycle"
}

// buildLocalNodeMD builds the schema metadata registered on the native metrics
// pub client for the co-located data node. The ROLE_DATA role is REQUIRED: the
// pub client's role gate (allowedRoles=[ROLE_DATA]) silently drops a node whose
// roles do not intersect, which would make native metrics a silent no-op.
func buildLocalNodeMD(grpcAddr string) schema.Metadata {
	return schema.Metadata{
		TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
		Spec: &databasev1.Node{
			Metadata:    &commonv1.Metadata{Name: metricsLocalNodeName},
			GrpcAddress: grpcAddr,
			Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
		},
	}
}

// buildHTTPRouter assembles the lifecycle's HTTP routes: the gRPC-gateway API
// under /api and, when the metrics registry exposes a Prometheus handler, the
// /metrics endpoint on the same (existing) HTTP server.
func (l *lifecycleService) buildHTTPRouter(apiHandler http.Handler) *chi.Mux {
	mux := chi.NewRouter()
	mux.Mount("/api", http.StripPrefix("/api", apiHandler))
	if reg, ok := l.omr.(observability.PrometheusHandlerProvider); ok {
		mux.Handle("/metrics", reg.PrometheusHandler())
	}
	return mux
}

// startMetricsNodeKeeper registers the co-located data node on the native
// metrics pub client and keeps the connection active. The pub connManager runs a
// synchronous health check at registration and does NOT auto-retry an evicted
// node (NewWithoutMetadata leaves healthCheckInterval==0), so the keeper
// re-registers whenever the node is not locatable — covering both an initial
// dial that failed (data node not ready yet) and a later failover eviction.
func (l *lifecycleService) startMetricsNodeKeeper() {
	l.localNodeMD = buildLocalNodeMD(l.gRPCAddr)
	l.metricsKeeperStop = make(chan struct{})
	// Initial registration dials the local data node so native flushes route there.
	l.metricsClient.OnAddOrUpdate(l.localNodeMD)
	run.Go(context.Background(), "backup.lifecycle.metrics-node-keeper", l.l, func(_ context.Context) {
		ticker := time.NewTicker(metricsNodeKeeperInterval)
		defer ticker.Stop()
		for {
			select {
			case <-l.metricsKeeperStop:
				return
			case <-ticker.C:
				// HealthyNodes reflects the pub's active-connection state directly.
				// connMgr.OnAddOrUpdate fans out OnActive synchronously (which seeds
				// the native selector), so an empty set means the local data node is
				// not connected and native flushes would drop; force a fresh dial.
				if len(l.metricsClient.HealthyNodes()) == 0 {
					// connMgr.OnAddOrUpdate short-circuits an evictable node, so drop
					// it first to force a fresh dial. queue.Client does not expose
					// OnDelete, hence the type assertion.
					if deleter, ok := l.metricsClient.(interface{ OnDelete(schema.Metadata) }); ok {
						deleter.OnDelete(l.localNodeMD)
					}
					l.metricsClient.OnAddOrUpdate(l.localNodeMD)
				}
			}
		}
	})
}

func (l *lifecycleService) Serve() run.StopNotify {
	l.l = logger.GetLogger("lifecycle")
	l.stopCh = make(chan struct{})

	// Register the co-located data node for native metrics and keep it active.
	// Native works in both scheduled and one-shot modes (it is a gRPC write that
	// does not depend on the lifecycle's own HTTP server).
	if l.omr.NativeEnabled() {
		l.startMetricsNodeKeeper()
	}

	// Start gRPC/HTTP servers when schedule is set
	if l.schedule != "" {
		l.startServers()
	}

	done := make(chan struct{})
	if l.schedule == "" {
		defer close(done)
		l.l.Info().Msg("starting lifecycle migration without schedule")
		if err := l.action(context.Background()); err != nil {
			logger.Panicf("failed to run lifecycle migration: %v", err)
		}
		return done
	}
	l.l.Info().Msgf("lifecycle migration will run with schedule: %s", l.schedule)
	clockInstance := clock.New()
	l.sch = timestamp.NewScheduler(l.l, clockInstance)
	var executionCount int
	err := l.sch.Register(context.Background(), "lifecycle", cron.Descriptor, l.schedule, func(ctx context.Context, triggerTime time.Time, _ *logger.Logger) bool {
		l.l.Info().Msgf("lifecycle migration triggered at %s", triggerTime)
		if err := l.action(ctx); err != nil {
			l.l.Error().Err(err).Msg("failed to run lifecycle migration action")
		}
		executionCount++
		if l.maxExecutionTimes > 0 && executionCount >= l.maxExecutionTimes {
			l.l.Info().Msgf("lifecycle migration reached max execution times: %d, stopping scheduler", l.maxExecutionTimes)
			close(done)
			return false
		}
		return true
	})
	if err != nil {
		l.l.Error().Err(err).Msg("failed to register lifecycle migration schedule")
		close(done)
		return done
	}

	// Wait for either migration completion or server stop
	run.Go(context.Background(), "backup.lifecycle.stop-watcher", l.l, func(_ context.Context) {
		select {
		case <-done:
			// Migration completed
		case <-l.stopCh:
			// Server stopped
			close(done)
		}
	})

	return done
}

func (l *lifecycleService) startServers() {
	// Setup gRPC server
	var opts []grpclib.ServerOption
	if l.lifecycleTLS && l.tlsReloader != nil {
		if startErr := l.tlsReloader.Start(context.Background()); startErr != nil {
			l.l.Error().Err(startErr).Msg("Failed to start TLS reloader")
			close(l.stopCh)
			return
		}
		tlsConfig := l.tlsReloader.GetTLSConfig()
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpclib.Creds(creds))
	}

	opts = append(opts,
		grpclib.ChainUnaryInterceptor(
			grpc_validator.UnaryServerInterceptor(),
		),
	)

	l.grpcServer = grpclib.NewServer(opts...)
	databasev1.RegisterClusterStateServiceServer(l.grpcServer, l)
	databasev1.RegisterNodeQueryServiceServer(l.grpcServer, l)
	grpc_health_v1.RegisterHealthServer(l.grpcServer, health.NewServer())

	// Setup HTTP server
	var ctx context.Context
	ctx, l.clientCloser = context.WithCancel(context.Background())

	clientOpts := make([]grpclib.DialOption, 0, 1)
	if l.lifecycleTLS && l.tlsReloader != nil {
		tlsConfig := l.tlsReloader.GetTLSConfig()
		creds := credentials.NewTLS(tlsConfig)
		clientOpts = append(clientOpts, grpclib.WithTransportCredentials(creds))
	} else {
		clientOpts = append(clientOpts, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	}

	client, err := healthcheck.NewClient(ctx, l.l, l.lifecycleGRPCAddr, clientOpts)
	if err != nil {
		l.l.Error().Err(err).Msg("Failed to create health check client")
		close(l.stopCh)
		return
	}

	gwMux := runtime.NewServeMux(runtime.WithHealthzEndpoint(client))
	if registerErr := databasev1.RegisterClusterStateServiceHandlerFromEndpoint(ctx, gwMux, l.lifecycleGRPCAddr, clientOpts); registerErr != nil {
		l.l.Error().Err(registerErr).Msg("Failed to register cluster state service")
		close(l.stopCh)
		return
	}

	// Reuse this existing HTTP server for the Prometheus endpoint instead of
	// opening a separate observability listener. The fodc-agent already scrapes
	// this port, so the lifecycle metrics appear with zero deployment change.
	mux := l.buildHTTPRouter(gwMux)

	l.httpSrv = &http.Server{
		Addr:              l.lifecycleHTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	// Start both servers
	var wg sync.WaitGroup
	wg.Add(2)

	// gRPC server goroutine
	run.Go(context.Background(), "backup.lifecycle.grpc-server", l.l, func(_ context.Context) {
		defer wg.Done()
		lis, listenErr := net.Listen("tcp", l.lifecycleGRPCAddr)
		if listenErr != nil {
			l.l.Error().Err(listenErr).Msg("Failed to listen on gRPC addr")
			return
		}
		l.l.Info().Str("addr", l.lifecycleGRPCAddr).Msg("Lifecycle gRPC server listening")
		if serveErr := l.grpcServer.Serve(lis); serveErr != nil {
			l.l.Error().Err(serveErr).Msg("gRPC server error")
		}
	})

	// HTTP server goroutine
	run.Go(context.Background(), "backup.lifecycle.http-server", l.l, func(_ context.Context) {
		defer wg.Done()
		l.l.Info().Str("addr", l.lifecycleHTTPAddr).Msg("Lifecycle HTTP server listening")
		var serveErr error
		if l.lifecycleTLS && l.tlsReloader != nil {
			l.httpSrv.TLSConfig = l.tlsReloader.GetTLSConfig()
			serveErr = l.httpSrv.ListenAndServeTLS("", "")
		} else {
			serveErr = l.httpSrv.ListenAndServe()
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
			l.l.Error().Err(serveErr).Msg("HTTP server error")
		}
	})

	// Wait for both servers to stop
	run.Go(context.Background(), "backup.lifecycle.shutdown-watcher", l.l, func(_ context.Context) {
		wg.Wait()
		close(l.stopCh)
	})
}

func (l *lifecycleService) action(ctx context.Context) (err error) {
	// Reset the cycle's last-seen (group, remote_*) tuple so an empty
	// cycle (no parseGroup succeeded) doesn't inherit the previous
	// cycle's labels. recordLastRun reads these at the end of the
	// cycle; without the reset, scheduler-driven consecutive cycles
	// could see a stale group label.
	l.lastRunGroup = ""
	l.lastRunNode = ""
	l.lastRunRole = ""
	l.lastRunTier = ""
	// Stamp last-run metrics at the end of this cycle regardless of outcome.
	// Using defer keeps the success/error bookkeeping in one place even as
	// the body grows new early returns; the metrics gauge Set()s observe
	// the same runStart and the success flag, so dashboards see consistent
	// (timestamp, success) pairs. The named return value lets the defer
	// observe whether the body succeeded.
	runStart := time.Now()
	defer func() {
		l.recordLastRun(runStart, err)
	}()
	progress := LoadProgress(l.progressFilePath, l.l)
	progress.ClearErrors()

	groups, err := l.getGroupsToProcess(ctx, progress)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to get groups to process")
		return err
	}

	l.l.Info().Msgf("starting migration for %d groups: %v", len(groups), getGroupNames(groups))

	if len(groups) == 0 {
		l.l.Info().Msg("no groups to process, all groups already completed")
		progress.Remove(l.progressFilePath, l.l)
		return err
	}

	// Pass progress to getSnapshots
	streamDir, measureDir, traceDir, err := l.getSnapshots(ctx, groups, progress)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to get snapshots")
		return err
	}
	if streamDir == "" && measureDir == "" && traceDir == "" {
		l.l.Warn().Msg("no snapshots found, skipping lifecycle migration")
		// Clear any GroupsToProcess persisted from a prior cycle so the
		// emitted report honestly reports total_groups=0 for this empty
		// cycle instead of inheriting a stale denominator.
		progress.SetGroupsToProcess(nil)
		l.generateReport(progress)
		return nil
	}
	l.l.Info().
		Str("stream_snapshot", streamDir).
		Str("measure_snapshot", measureDir).
		Str("trace_snapshot", traceDir).
		Msg("created snapshots")
	// Record the scheduled group set only after snapshots are confirmed so
	// the report distinguishes "no work this cycle" (total_groups=0) from a
	// real cycle that processed N groups (total_groups=N).
	progress.SetGroupsToProcess(getGroupNames(groups))
	progress.Save(l.progressFilePath, l.l)

	nodes, err := l.metadata.NodeRegistry().ListNode(ctx, databasev1.Role_ROLE_DATA)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to list data nodes")
		return err
	}
	// The lifecycle usually starts together with its co-located data node and
	// the property-based registry syncs asynchronously, so the node behind
	// --grpc-addr may not be visible in the first listing. deriveSelfIdentity
	// needs it for deterministic sender attribution (its address match is
	// pass 1), so wait briefly for it instead of mis-deriving the identity
	// from a label fallback.
	nodes = l.waitForCoLocatedNode(ctx, nodes)
	labels := common.ParseNodeFlags()

	for _, g := range groups {
		switch g.Catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			if streamDir == "" {
				l.l.Warn().Msgf("stream snapshot directory is not available, skipping group: %s", g.Metadata.Name)
				progress.MarkGroupCompleted(g.Metadata.Name)
				continue
			}
			l.processStreamGroup(ctx, g, streamDir, nodes, labels, progress)
		case commonv1.Catalog_CATALOG_MEASURE:
			if measureDir == "" {
				l.l.Warn().Msgf("measure snapshot directory is not available, skipping group: %s", g.Metadata.Name)
				progress.MarkGroupCompleted(g.Metadata.Name)
				continue
			}
			l.processMeasureGroup(ctx, g, measureDir, nodes, labels, progress)
		case commonv1.Catalog_CATALOG_TRACE:
			if traceDir == "" {
				l.l.Warn().Msgf("trace snapshot directory is not available, skipping group: %s", g.Metadata.Name)
				progress.MarkGroupCompleted(g.Metadata.Name)
				continue
			}
			l.processTraceGroup(ctx, g, traceDir, nodes, labels, progress)
		default:
			l.l.Info().Msgf("group catalog: %s doesn't support lifecycle management", g.Catalog)
		}
		progress.Save(l.progressFilePath, l.l)
	}

	// Only remove progress file if ALL groups are fully completed
	notCompleteGroups := progress.AllGroupsNotFullyCompleted(groups)
	if len(notCompleteGroups) == 0 {
		progress.Remove(l.progressFilePath, l.l)
		l.l.Info().Msg("lifecycle migration completed successfully")
		l.generateReport(progress)
		return nil
	}
	// Partial-failure path: also emit a report so operators can inspect
	// errors.* and per-resource completion_rate without parsing raw
	// progress.json. The report distinguishes itself from a clean cycle
	// by having errors.* non-empty and completion_rate < 100.
	l.l.Info().Msg("lifecycle migration partially completed, progress file retained")
	l.generateReport(progress)
	return fmt.Errorf("lifecycle migration partially completed, progress file retained; %v groups not fully completed", notCompleteGroups)
}

// recordCycleGroup stamps the per-group banyandb_lifecycle_cycles_total
// Inc with the (group, remote_node, remote_role, remote_tier) tuple
// returned by parseGroup. It also captures the (group, remote_*) tuple
// as the cycle's last-seen identity, which the deferred recordLastRun
// reads at the end of the cycle to stamp the cycle-level last_run_*
// gauges. lastRunTimestamp and lastRunSuccess are intentionally NOT
// touched here — they are stamped atomically at cycle end in
// recordLastRun so dashboards see consistent (timestamp, success) pairs
// for the same (group, remote_*) tuple, and so the success flag
// reflects the whole-cycle outcome (not the last group's parseGroup
// result).
//
// nil counters are skipped so a lifecycle run with a nil
// observability.MetricsRegistry (BypassRegistry) doesn't crash.
func (l *lifecycleService) recordCycleGroup(group, senderNode, senderRole, senderTier string) {
	if l.cyclesTotal != nil {
		l.cyclesTotal.Inc(1, senderNode, senderRole, senderTier, group)
	}
	l.lastRunGroup = group
	l.lastRunNode = senderNode
	l.lastRunRole = senderRole
	l.lastRunTier = senderTier
}

// recordLastRun stamps the banyandb_lifecycle_last_run_timestamp_seconds
// and banyandb_lifecycle_last_run_success gauges with the start time
// (epoch seconds) and a 0/1 success flag, both using the
// (group, remote_node, remote_role, remote_tier) tuple from the cycle's
// last processed group. Called from the deferred end-of-action block so
// every code path (success, error, panic-recovered) updates both
// gauges atomically.
//
// Prometheus' labeled gauges don't garbage-collect the previous tuple
// when Set is called with new labels — the old series lingers as
// "stale" until a scrape expires it. To prevent dashboards from
// reading a previous cycle's (group, remote_*) tuple as current,
// recordLastRun first Deletes the previous-tuple series (if any
// existed) before stamping the new one. The empty-cycle path (no
// group was processed) calls Delete on the previous tuple and then
// stamps a single series with all-empty labels, so dashboards always
// see exactly one current series. nil gauges are skipped so a lifecycle
// run with a nil observability.MetricsRegistry (BypassRegistry)
// doesn't crash.
func (l *lifecycleService) recordLastRun(start time.Time, err error) {
	success := 0.0
	if err == nil {
		success = 1.0
	}
	prevLabels := []string{l.lastRunNode, l.lastRunRole, l.lastRunTier, l.lastRunGroup}
	// If a previous tuple was set (and the cycle is not the empty one,
	// where the tuple was reset to empty strings at action start),
	// delete it first so the new stamp replaces rather than shadows.
	if l.lastRunGroup != "" || l.lastRunNode != "" || l.lastRunRole != "" || l.lastRunTier != "" {
		if l.lastRunTimestamp != nil {
			l.lastRunTimestamp.Delete(prevLabels...)
		}
		if l.lastRunSuccess != nil {
			l.lastRunSuccess.Delete(prevLabels...)
		}
	}
	if l.lastRunTimestamp != nil {
		l.lastRunTimestamp.Set(float64(start.Unix()), l.lastRunNode, l.lastRunRole, l.lastRunTier, l.lastRunGroup)
	}
	if l.lastRunSuccess != nil {
		l.lastRunSuccess.Set(success, l.lastRunNode, l.lastRunRole, l.lastRunTier, l.lastRunGroup)
	}
}

// waitForCoLocatedNode waits briefly for the data node behind --grpc-addr to
// appear in the registry listing so deriveSelfIdentity can resolve the sender
// identity through its deterministic address match (pass 1). The lifecycle
// typically starts alongside its co-located data node, whose registration
// propagates through the property-based registry asynchronously, so the first
// listing may not include it yet. Returns the freshest listing either way; on
// timeout the label fallbacks (or the empty identity) apply as before.
func (l *lifecycleService) waitForCoLocatedNode(ctx context.Context, nodes []*databasev1.Node) []*databasev1.Node {
	const (
		attempts = 20
		interval = 500 * time.Millisecond
	)
	if l.gRPCAddr == "" || hasGrpcAddress(nodes, l.gRPCAddr) {
		return nodes
	}
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			return nodes
		case <-time.After(interval):
		}
		refreshed, listErr := l.metadata.NodeRegistry().ListNode(ctx, databasev1.Role_ROLE_DATA)
		if listErr != nil {
			l.l.Warn().Err(listErr).Msg("failed to re-list data nodes while waiting for the co-located node")
			continue
		}
		nodes = refreshed
		if hasGrpcAddress(nodes, l.gRPCAddr) {
			return nodes
		}
	}
	l.l.Warn().Str("grpc_addr", l.gRPCAddr).Msg("co-located data node not visible in the registry; sender identity falls back to node labels")
	return nodes
}

// hasGrpcAddress reports whether any node advertises the given gRPC address
// (loopback host aliases are treated as equivalent, see grpcAddrEqual).
func hasGrpcAddress(nodes []*databasev1.Node, addr string) bool {
	for _, n := range nodes {
		if grpcAddrEqual(n.GrpcAddress, addr) {
			return true
		}
	}
	return false
}

// generateReport gathers detailed counts & errors from Progress, writes comprehensive JSON file per run, and keeps only 5 latest.
func (l *lifecycleService) generateReport(p *Progress) {
	reportDir := l.reportDir
	if err := os.MkdirAll(reportDir, 0o755); err != nil {
		l.l.Error().Err(err).Msgf("failed to create report dir %s", reportDir)
		return
	}

	// Build comprehensive migration report
	report := l.buildMigrationReport(p)

	// write file
	fname := time.Now().Format("20060102_150405") + ".json"
	fpath := filepath.Join(reportDir, fname)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		l.l.Error().Err(err).Msg("failed to marshal migration report")
		return
	}
	if err = os.WriteFile(fpath, data, 0o600); err != nil {
		l.l.Error().Err(err).Msgf("failed to write report file %s", fpath)
		return
	}
	l.l.Info().Msgf("wrote comprehensive migration report %s", fpath)

	// rotate: keep only 5 latest
	l.rotateReportFiles(reportDir)
}

// buildMigrationReport creates a comprehensive migration report from progress data.
func (l *lifecycleService) buildMigrationReport(p *Progress) map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	report := map[string]interface{}{
		"generated_at":   now,
		"report_version": "2.1",
		"summary":        l.buildSummaryStats(p),
		"errors":         l.buildErrorSummary(p),
		"snapshot_info": map[string]interface{}{
			"stream_dir":  p.SnapshotStreamDir,
			"measure_dir": p.SnapshotMeasureDir,
			"trace_dir":   p.SnapshotTraceDir,
		},
	}

	return report
}

// buildSummaryStats creates overall migration statistics.
func (l *lifecycleService) buildSummaryStats(p *Progress) map[string]interface{} {
	// total_groups reflects the cycle's scheduled set captured at the top of
	// action(); completed_groups counts groups that ran the full
	// processXxxGroup → MarkGroupCompleted path. The intersection guards
	// against resume from a partial-failure progress.json where
	// CompletedGroups carries entries from prior cycles that are not in
	// the current scheduled set, which would otherwise yield
	// completed_groups > total_groups.
	totalGroups := len(p.GroupsToProcess)
	completedGroups := 0
	for _, name := range p.GroupsToProcess {
		if p.CompletedGroups[name] {
			completedGroups++
		}
	}

	// Calculate total parts and series across all groups
	totalStreamParts, completedStreamParts := l.calculateTotalCounts(p.StreamPartCounts, p.StreamPartProgress)
	totalStreamSeries, completedStreamSeries := l.calculateTotalCounts(p.StreamSeriesCounts, p.StreamSeriesProgress)
	totalStreamElementIndex, completedStreamElementIndex := l.calculateTotalCounts(p.StreamElementIndexCounts, p.StreamElementIndexProgress)
	totalMeasureParts, completedMeasureParts := l.calculateTotalCounts(p.MeasurePartCounts, p.MeasurePartProgress)
	totalMeasureSeries, completedMeasureSeries := l.calculateTotalCounts(p.MeasureSeriesCounts, p.MeasureSeriesProgress)
	totalTraceShards, completedTraceShards := l.calculateTotalCounts(p.TraceShardCounts, p.TraceShardProgress)
	totalTraceSeries, completedTraceSeries := l.calculateTotalCounts(p.TraceSeriesCounts, p.TraceSeriesProgress)

	// Calculate error counts from the structured MigrationErrors (single source).
	streamPartErrors := countMigrationErrors(p, catalogStream, scopePart)
	streamSeriesErrors := countMigrationErrors(p, catalogStream, scopeSeries)
	streamElementIndexErrors := countMigrationErrors(p, catalogStream, scopeElementIndex)
	measurePartErrors := countMigrationErrors(p, catalogMeasure, scopePart)
	measureSeriesErrors := countMigrationErrors(p, catalogMeasure, scopeSeries)
	traceShardErrors := countMigrationErrors(p, catalogTrace, scopeShard)
	traceSeriesErrors := countMigrationErrors(p, catalogTrace, scopeSeries)

	return map[string]interface{}{
		"migration_status": map[string]interface{}{
			"total_groups":     totalGroups,
			"completed_groups": completedGroups,
			"completion_rate":  l.calculatePercentage(completedGroups, totalGroups),
		},
		"stream_migration": map[string]interface{}{
			"parts": map[string]interface{}{
				"total":           totalStreamParts,
				"completed":       completedStreamParts,
				"errors":          streamPartErrors,
				"completion_rate": l.calculatePercentage(completedStreamParts, totalStreamParts),
			},
			"series": map[string]interface{}{
				"total":           totalStreamSeries,
				"completed":       completedStreamSeries,
				"errors":          streamSeriesErrors,
				"completion_rate": l.calculatePercentage(completedStreamSeries, totalStreamSeries),
			},
			"element_index": map[string]interface{}{
				"total":           totalStreamElementIndex,
				"completed":       completedStreamElementIndex,
				"errors":          streamElementIndexErrors,
				"completion_rate": l.calculatePercentage(completedStreamElementIndex, totalStreamElementIndex),
			},
			"sync_breakdown": l.buildSyncBreakdown(p.StreamChunkSyncParts, p.StreamRowReplayParts, p.StreamRowReplayRows, "chunk_sync_parts"),
		},
		"measure_migration": map[string]interface{}{
			"parts": map[string]interface{}{
				"total":           totalMeasureParts,
				"completed":       completedMeasureParts,
				"errors":          measurePartErrors,
				"completion_rate": l.calculatePercentage(completedMeasureParts, totalMeasureParts),
			},
			"series": map[string]interface{}{
				"total":           totalMeasureSeries,
				"completed":       completedMeasureSeries,
				"errors":          measureSeriesErrors,
				"completion_rate": l.calculatePercentage(completedMeasureSeries, totalMeasureSeries),
			},
			"sync_breakdown": l.buildSyncBreakdown(p.MeasureChunkSyncParts, p.MeasureRowReplayParts, p.MeasureRowReplayRows, "chunk_sync_parts"),
		},
		// Field names parts/series mirror stream_migration / measure_migration.
		// "parts" is fed by the per-shard TraceShard counters because trace
		// migration is shard-batched (sidx + core parts streamed together).
		"trace_migration": map[string]interface{}{
			"parts": map[string]interface{}{
				"total":           totalTraceShards,
				"completed":       completedTraceShards,
				"errors":          traceShardErrors,
				"completion_rate": l.calculatePercentage(completedTraceShards, totalTraceShards),
			},
			"series": map[string]interface{}{
				"total":           totalTraceSeries,
				"completed":       completedTraceSeries,
				"errors":          traceSeriesErrors,
				"completion_rate": l.calculatePercentage(completedTraceSeries, totalTraceSeries),
			},
			// Trace chunk-sync is shard-batched, so the breakdown reports
			// chunk_sync_shards (not parts) alongside the row-replay part/row counts.
			"sync_breakdown": l.buildSyncBreakdown(p.TraceChunkSyncShards, p.TraceRowReplayParts, p.TraceRowReplayRows, "chunk_sync_shards"),
		},
	}
}

// buildSyncBreakdown reports, per group, how many parts/shards were migrated via
// chunk-sync vs row-replay and how many rows the row-replay path republished.
// The chunkKey is "chunk_sync_parts" for measure/stream and "chunk_sync_shards"
// for trace. A "_total" entry aggregates across groups.
func (l *lifecycleService) buildSyncBreakdown(chunk, replayParts, replayRows map[string]uint64, chunkKey string) map[string]interface{} {
	groups := make(map[string]struct{})
	for g := range chunk {
		groups[g] = struct{}{}
	}
	for g := range replayParts {
		groups[g] = struct{}{}
	}
	for g := range replayRows {
		groups[g] = struct{}{}
	}
	out := make(map[string]interface{}, len(groups)+1)
	var totalChunk, totalReplayParts, totalReplayRows uint64
	for g := range groups {
		c, rp, rr := chunk[g], replayParts[g], replayRows[g]
		out[g] = map[string]interface{}{
			chunkKey:           c,
			"row_replay_parts": rp,
			"row_replay_rows":  rr,
		}
		totalChunk += c
		totalReplayParts += rp
		totalReplayRows += rr
	}
	out["_total"] = map[string]interface{}{
		chunkKey:           totalChunk,
		"row_replay_parts": totalReplayParts,
		"row_replay_rows":  totalReplayRows,
	}
	return out
}

// buildErrorSummary creates detailed error information.
func (l *lifecycleService) buildErrorSummary(p *Progress) map[string]interface{} {
	// Project the flattened MigrationErrors into the 8 report buckets by their
	// (catalog, scope), each an array (empty when none) so the JSON shape is
	// stable.
	buckets := make(map[string][]MigrationError, len(errorBuckets)+1)
	for i := range errorBuckets {
		buckets[errorBuckets[i].name] = []MigrationError{}
	}
	buckets[nodeErrorBucket] = []MigrationError{}
	for i := range p.MigrationErrors {
		if key := errorBucketKey(p.MigrationErrors[i].Catalog, p.MigrationErrors[i].Scope); key != "" {
			buckets[key] = append(buckets[key], p.MigrationErrors[i])
		}
	}
	out := make(map[string]interface{}, len(buckets))
	for k := range buckets {
		out[k] = buckets[k]
	}
	return out
}

// errorBuckets is the single source of truth for the report's error buckets:
// each entry names a bucket and the (catalog, scope) it holds. Node-scoped
// errors share nodeErrorBucket across catalogs and are handled separately.
var errorBuckets = []struct {
	name, catalog, scope string
}{
	{"stream_parts", catalogStream, scopePart},
	{"stream_series", catalogStream, scopeSeries},
	{"stream_element_index", catalogStream, scopeElementIndex},
	{"measure_parts", catalogMeasure, scopePart},
	{"measure_series", catalogMeasure, scopeSeries},
	{"trace_parts", catalogTrace, scopeShard},
	{"trace_series", catalogTrace, scopeSeries},
}

const nodeErrorBucket = "row_replay_node_errors"

// errorBucketKey maps a migration error's catalog+scope to its report bucket
// name, or "" if it has none. Node-scoped errors share one bucket across catalogs.
func errorBucketKey(catalog, scope string) string {
	if scope == scopeNode {
		return nodeErrorBucket
	}
	for i := range errorBuckets {
		if errorBuckets[i].catalog == catalog && errorBuckets[i].scope == scope {
			return errorBuckets[i].name
		}
	}
	return ""
}

// Helper functions.
func (l *lifecycleService) calculateTotalCounts(counts, progress map[string]int) (int, int) {
	totalCount, totalProgress := 0, 0
	for _, count := range counts {
		totalCount += count
	}
	for _, prog := range progress {
		totalProgress += prog
	}
	return totalCount, totalProgress
}

// countMigrationErrors counts the structured migration errors matching the given
// catalog and scope, the single source for the report's per-resource error tally.
func countMigrationErrors(p *Progress, catalog, scope string) int {
	n := 0
	for i := range p.MigrationErrors {
		if p.MigrationErrors[i].Catalog == catalog && p.MigrationErrors[i].Scope == scope {
			n++
		}
	}
	return n
}

func (l *lifecycleService) calculatePercentage(completed, total int) float64 {
	if total == 0 {
		return 0.0
	}
	return float64(completed) / float64(total) * 100.0
}

// rotateReportFiles keeps only the 5 most recent report files.
func (l *lifecycleService) rotateReportFiles(reportDir string) {
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		return
	}

	type fileInfo struct {
		t    time.Time
		name string
	}

	var list []fileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		list = append(list, fileInfo{name: e.Name(), t: info.ModTime()})
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].t.After(list[j].t)
	})

	for i := 5; i < len(list); i++ {
		_ = os.Remove(filepath.Join(reportDir, list[i].name))
	}

	if len(list) > 5 {
		l.l.Info().Msgf("rotated %d old migration reports", len(list)-5)
	}
}

func getGroupNames(groups []*commonv1.Group) []string {
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Metadata.Name)
	}
	return names
}

func (l *lifecycleService) getGroupsToProcess(ctx context.Context, progress *Progress) ([]*commonv1.Group, error) {
	gg, err := l.metadata.GroupRegistry().ListGroup(ctx)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to list groups")
		return nil, err
	}
	allGroupNames := getGroupNames(gg)

	groups := make([]*commonv1.Group, 0, len(gg))
	for _, g := range gg {
		if g.ResourceOpts == nil {
			l.l.Debug().Msgf("skipping group %s because resource opts is nil", g.Metadata.Name)
			continue
		}
		if len(g.ResourceOpts.Stages) == 0 {
			l.l.Debug().Msgf("skipping group %s because stages is empty", g.Metadata.Name)
			continue
		}
		if progress.IsGroupCompleted(g.Metadata.Name) {
			l.l.Debug().Msgf("skipping already completed group: %s", g.Metadata.Name)
			continue
		}
		groups = append(groups, g)
	}
	l.l.Info().Msgf("found groups needs to do lifecycle processing: %v, all groups: %v", getGroupNames(groups), allGroupNames)

	return groups, nil
}

func (l *lifecycleService) processStreamGroup(ctx context.Context, g *commonv1.Group,
	streamDir string, nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	group, senderNode, senderRole, senderTier, err := parseGroup(g, labels, nodes, l.l, l.metadata, l.clusterStateMgr, l.omr)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to parse group %s", g.Metadata.Name)
		return
	}
	l.recordCycleGroup(g.Metadata.Name, senderNode, senderRole, senderTier)
	defer group.Close()
	tr := l.getRemovalSegmentsTimeRange(group)
	if tr.Start.IsZero() && tr.End.IsZero() {
		l.l.Info().Msgf("no removal segments time range for group %s, skipping stream migration", g.Metadata.Name)
		progress.MarkGroupCompleted(g.Metadata.Name)
		return
	}

	segmentSuffixes, err := l.processStreamGroupFileBased(ctx, group, streamDir, tr, progress)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to migrate stream group %s using file-based approach", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleting expired stream segments for group: %s, time range: %s", g.Metadata.Name, tr.String())
	l.deleteExpiredStreamSegments(ctx, g, segmentSuffixes, progress)
	progress.MarkGroupCompleted(g.Metadata.Name)
}

// processStreamGroupFileBased uses file-based migration instead of element-based queries.
func (l *lifecycleService) processStreamGroupFileBased(_ context.Context, g *GroupConfig, streamDir string,
	tr *timestamp.TimeRange, progress *Progress,
) ([]string, error) {
	if progress.IsStreamGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already completed file-based migration for group: %s", g.Metadata.Name)
		return nil, nil
	}

	l.l.Info().Msgf("starting file-based stream migration for group: %s, time range: %s", g.Metadata.Name, tr.String())

	rootDir := filepath.Join(streamDir, g.Metadata.Name)
	// skip the counting if the tsdb root path does not exist
	// may no data found in the snapshot
	if _, err := os.Stat(rootDir); err != nil && errors.Is(err, os.ErrNotExist) {
		l.l.Info().Msgf("skipping file-based stream migration for group because is empty in the snapshot dir: %s", g.Metadata.Name)
		return nil, nil
	}

	// Use the file-based migration with existing visitor pattern
	//nolint:contextcheck // migration drives its own context lifecycle for batch publish.
	segmentSuffixes, err := migrateStreamWithFileBasedAndProgress(rootDir, *tr, g, l.l, progress, int(l.chunkSize), l.metadata)
	if err != nil {
		return nil, fmt.Errorf("file-based stream migration failed: %w", err)
	}

	l.l.Info().Msgf("completed file-based stream migration for group: %s", g.Metadata.Name)
	return segmentSuffixes, nil
}

// getRemovalSegmentsTimeRange calculates the time range for segments that should be migrated
// based on the group's TTL configuration, similar to storage.segmentController.getExpiredSegmentsTimeRange.
func (l *lifecycleService) getRemovalSegmentsTimeRange(g *GroupConfig) *timestamp.TimeRange {
	// Convert TTL to storage.IntervalRule
	ttlRule := storage.MustToIntervalRule(g.AccumulatedTTL)

	// Calculate deadline based on TTL (same logic as segmentController.getExpiredSegmentsTimeRange)
	deadline := time.Now().Local().Add(-l.calculateTTLDuration(ttlRule))

	// Create time range for segments before the deadline
	timeRange := &timestamp.TimeRange{
		Start:        time.Time{}, // Will be set to earliest segment start time
		End:          deadline,    // All segments before this time should be migrated
		IncludeStart: true,
		IncludeEnd:   false,
	}

	l.l.Info().
		Str("group", g.Metadata.Name).
		Time("deadline", deadline).
		Str("ttl", fmt.Sprintf("%d %s", g.AccumulatedTTL.Num, g.AccumulatedTTL.Unit)).
		Msg("calculated removal segments time range based on TTL")

	return timeRange
}

// calculateTTLDuration calculates the duration for a TTL interval rule.
// This implements the same logic as storage.IntervalRule.estimatedDuration().
func (l *lifecycleService) calculateTTLDuration(ttl storage.IntervalRule) time.Duration {
	switch ttl.Unit {
	case storage.HOUR:
		return time.Hour * time.Duration(ttl.Num)
	case storage.DAY:
		return 24 * time.Hour * time.Duration(ttl.Num)
	default:
		l.l.Warn().Msgf("unknown TTL unit %v, defaulting to 1 day", ttl.Unit)
		return 24 * time.Hour
	}
}

func (l *lifecycleService) deleteExpiredStreamSegments(ctx context.Context, g *commonv1.Group, segmentSuffixes []string, progress *Progress) {
	if progress.IsStreamGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already deleted stream group segments: %s", g.Metadata.Name)
		return
	}

	resp, err := snapshot.Conn(l.gRPCAddr, l.enableTLS, l.insecure, l.cert, func(conn *grpclib.ClientConn) (*streamv1.DeleteExpiredSegmentsResponse, error) {
		client := streamv1.NewStreamServiceClient(conn)
		return client.DeleteExpiredSegments(ctx, &streamv1.DeleteExpiredSegmentsRequest{
			Group:           g.Metadata.Name,
			SegmentSuffixes: segmentSuffixes,
		})
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to delete expired segments in group %s, segments: %s", g.Metadata.Name, segmentSuffixes)
		return
	}

	l.l.Info().Msgf("deleted %d expired segments in group %s, segments: %s", resp.Deleted, g.Metadata.Name, segmentSuffixes)
	progress.MarkStreamGroupDeleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

func (l *lifecycleService) processMeasureGroup(ctx context.Context, g *commonv1.Group, measureDir string,
	nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	group, senderNode, senderRole, senderTier, err := parseGroup(g, labels, nodes, l.l, l.metadata, l.clusterStateMgr, l.omr)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to parse group %s", g.Metadata.Name)
		return
	}
	l.recordCycleGroup(g.Metadata.Name, senderNode, senderRole, senderTier)
	defer group.Close()

	tr := l.getRemovalSegmentsTimeRange(group)
	if tr.Start.IsZero() && tr.End.IsZero() {
		l.l.Info().Msgf("no removal segments time range for group %s, skipping measure migration", g.Metadata.Name)
		progress.MarkGroupCompleted(g.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
		return
	}

	// Try file-based migration first
	segmentSuffixes, err := l.processMeasureGroupFileBased(ctx, group, measureDir, tr, progress)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to migrate measure group %s using file-based approach", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleting expired measure segments for group: %s", g.Metadata.Name)
	l.deleteExpiredMeasureSegments(ctx, g, segmentSuffixes, progress)
	progress.MarkGroupCompleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

// processMeasureGroupFileBased uses file-based migration instead of query-based migration.
func (l *lifecycleService) processMeasureGroupFileBased(_ context.Context, g *GroupConfig, measureDir string,
	tr *timestamp.TimeRange, progress *Progress,
) ([]string, error) {
	if progress.IsMeasureGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already completed file-based measure migration for group: %s", g.Metadata.Name)
		return nil, nil
	}

	l.l.Info().Msgf("starting file-based measure migration for group: %s", g.Metadata.Name)

	rootDir := filepath.Join(measureDir, g.Metadata.Name)
	// skip the counting if the tsdb root path does not exist
	// may no data found in the snapshot
	if _, err := os.Stat(rootDir); err != nil && errors.Is(err, os.ErrNotExist) {
		l.l.Info().Msgf("skipping file-based measure migration for group because is empty in the snapshot dir: %s", g.Metadata.Name)
		return nil, nil
	}

	// Use the file-based migration with existing visitor pattern
	//nolint:contextcheck // migration drives its own context lifecycle for batch publish.
	segmentSuffixes, err := migrateMeasureWithFileBasedAndProgress(rootDir, *tr, g, l.l, progress, int(l.chunkSize), l.metadata)
	if err != nil {
		return nil, fmt.Errorf("file-based measure migration failed: %w", err)
	}

	l.l.Info().Msgf("completed file-based measure migration for group: %s", g.Metadata.Name)
	return segmentSuffixes, nil
}

func (l *lifecycleService) deleteExpiredMeasureSegments(ctx context.Context, g *commonv1.Group, segmentSuffixes []string, progress *Progress) {
	if progress.IsMeasureGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already deleted measure group segments: %s", g.Metadata.Name)
		return
	}

	resp, err := snapshot.Conn(l.gRPCAddr, l.enableTLS, l.insecure, l.cert, func(conn *grpclib.ClientConn) (*measurev1.DeleteExpiredSegmentsResponse, error) {
		client := measurev1.NewMeasureServiceClient(conn)
		return client.DeleteExpiredSegments(ctx, &measurev1.DeleteExpiredSegmentsRequest{
			Group:           g.Metadata.Name,
			SegmentSuffixes: segmentSuffixes,
		})
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to delete expired segments in group %s, suffixes: %s", g.Metadata.Name, segmentSuffixes)
		return
	}

	l.l.Info().Msgf("deleted %d expired segments in group %s, suffixes: %s", resp.Deleted, g.Metadata.Name, segmentSuffixes)
	progress.MarkMeasureGroupDeleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

func (l *lifecycleService) deleteExpiredTraceSegments(ctx context.Context, g *commonv1.Group, segmentSuffixes []string, progress *Progress) {
	if progress.IsTraceGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already deleted trace group segments: %s", g.Metadata.Name)
		return
	}

	resp, err := snapshot.Conn(l.gRPCAddr, l.enableTLS, l.insecure, l.cert, func(conn *grpclib.ClientConn) (*tracev1.DeleteExpiredSegmentsResponse, error) {
		client := tracev1.NewTraceServiceClient(conn)
		return client.DeleteExpiredSegments(ctx, &tracev1.DeleteExpiredSegmentsRequest{
			Group:           g.Metadata.Name,
			SegmentSuffixes: segmentSuffixes,
		})
	})
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to delete expired segments in group %s, suffixes: %s", g.Metadata.Name, segmentSuffixes)
		return
	}

	l.l.Warn().Msgf("deleted %d expired segments in group %s, suffixes: %s", resp.Deleted, g.Metadata.Name, segmentSuffixes)
	progress.MarkTraceGroupDeleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

func (l *lifecycleService) processTraceGroup(ctx context.Context, g *commonv1.Group, traceDir string,
	nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
	group, senderNode, senderRole, senderTier, err := parseGroup(g, labels, nodes, l.l, l.metadata, l.clusterStateMgr, l.omr)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to parse group %s", g.Metadata.Name)
		return
	}
	l.recordCycleGroup(g.Metadata.Name, senderNode, senderRole, senderTier)
	defer group.Close()

	tr := l.getRemovalSegmentsTimeRange(group)
	if tr.Start.IsZero() && tr.End.IsZero() {
		l.l.Info().Msgf("no removal segments time range for group %s, skipping trace migration", g.Metadata.Name)
		progress.MarkGroupCompleted(g.Metadata.Name)
		progress.Save(l.progressFilePath, l.l)
		return
	}

	// Try file-based migration first
	segmentSuffixes, err := l.processTraceGroupFileBased(ctx, group, traceDir, tr, progress)
	if err != nil {
		l.l.Error().Err(err).Msgf("failed to migrate trace group %s using file-based approach", g.Metadata.Name)
		return
	}

	l.l.Info().Msgf("deleting expired trace segments for group: %s", g.Metadata.Name)
	l.deleteExpiredTraceSegments(ctx, g, segmentSuffixes, progress)
	progress.MarkGroupCompleted(g.Metadata.Name)
	progress.Save(l.progressFilePath, l.l)
}

func (l *lifecycleService) processTraceGroupFileBased(_ context.Context, g *GroupConfig, traceDir string,
	tr *timestamp.TimeRange, progress *Progress,
) ([]string, error) {
	if progress.IsTraceGroupDeleted(g.Metadata.Name) {
		l.l.Info().Msgf("skipping already completed file-based trace migration for group: %s", g.Metadata.Name)
		return nil, nil
	}

	l.l.Info().Msgf("starting file-based trace migration for group: %s", g.Metadata.Name)

	rootDir := filepath.Join(traceDir, g.Metadata.Name)
	// skip the counting if the tsdb root path does not exist
	// may no data found in the snapshot
	if _, err := os.Stat(rootDir); err != nil && errors.Is(err, os.ErrNotExist) {
		l.l.Info().Msgf("skipping file-based trace migration for group because is empty in the snapshot dir: %s", g.Metadata.Name)
		return nil, nil
	}

	// Use the file-based migration with existing visitor pattern
	//nolint:contextcheck // migration drives its own context lifecycle for batch publish.
	segmentSuffixes, err := migrateTraceWithFileBasedAndProgress(rootDir, *tr, g, l.l, progress, int(l.chunkSize), l.metadata)
	if err != nil {
		return nil, fmt.Errorf("file-based trace migration failed: %w", err)
	}

	l.l.Info().Msgf("completed file-based trace migration for group: %s", g.Metadata.Name)
	return segmentSuffixes, nil
}
